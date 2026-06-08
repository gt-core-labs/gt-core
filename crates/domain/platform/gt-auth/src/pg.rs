//! Postgres-backed login store for the [`IdentityProvider`](crate::IdentityProvider) port
//! (`hq-auth-mint.2`). Compiled only under the `pg` feature.
//!
//! This is the *login* step that runs before a token is minted: a principal presents an email
//! and password ([`Credentials::EmailPassword`]); [`PgUsers`] looks the email up in the
//! per-workspace `users` table, verifies the password against the stored argon2 PHC hash, and
//! returns the [`VerifiedIdentity`] the minting tier folds into a fresh `JwtClaims`.
//!
//! ## Reuse, never duplicate
//!
//! The argon2 verification is the existing [`verify_password`](crate::password::verify_password)
//! from the `password-hash` adapter — `pg` turns that feature on (`pg = ["dep:sqlx",
//! "password-hash"]`) precisely so this adapter borrows it rather than re-implementing a hash
//! check. The same indistinguishability holds: an unknown email and a wrong password both
//! surface [`AuthError::InvalidCredentials`], so the login endpoint cannot be used to
//! enumerate accounts.
//!
//! ## Workspace isolation (docs/03 Rule 6, docs/04 §15)
//!
//! The `users` table is per-workspace projection data living in each tenant's `ws_<slug>`
//! schema (migration `migrations/auth/0001__create_users.sql`, defined once in the `ws_default`
//! template). This adapter issues an **unqualified** `users` query and relies on the
//! connection's `search_path` being scoped to the workspace schema — exactly what
//! [`gt_store_pg::WorkspacePool`] does on checkout. So a `PgUsers` built over a workspace-scoped
//! pool reads only that tenant's rows, with no `workspace` predicate.
//!
//! Because the row therefore does **not** carry a workspace, the workspace stamped onto the
//! returned [`VerifiedIdentity`] is the slug the *server* hands [`PgUsers::new`] (the tenant the
//! pool is scoped to) — never anything from the credentials payload. That is docs/04 §15: the
//! server injects the workspace, the client never supplies it, so a login cannot forge its
//! tenant.
//!
//! ## Async — and why it is an inherent method, not the trait
//!
//! [`IdentityProvider::authenticate`](crate::IdentityProvider::authenticate) is **synchronous**
//! (the in-memory and `password-hash` adapters are pure CPU work). A DB round-trip is I/O, so
//! `PgUsers` exposes an inherent `async fn authenticate` instead of implementing the sync trait
//! — blocking a runtime thread to satisfy a sync signature would be a footgun. This mirrors how
//! `gt-rig`'s `RigRepository` adapter is async-by-future while its in-memory sibling is sync.
//! The sync, PG-free seam ([`row_to_identity`]) is unit-tested without a database.

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::password::verify_password;
use crate::{AuthError, Credentials, VerifiedIdentity};

/// Postgres-backed login store: authenticate [`Credentials::EmailPassword`] against the
/// per-workspace `users` table.
///
/// Holds a [`PgPool`] whose connections are `search_path`-scoped to one workspace schema
/// (build it from `WorkspacePool::pool()`) plus the `workspace` slug that pool resolves to —
/// the slug stamped onto every [`VerifiedIdentity`] this store returns (docs/04 §15, injected
/// server-side). Cloning is cheap: `PgPool` is an `Arc` over the connection pool.
#[derive(Clone, Debug)]
pub struct PgUsers {
    pool: PgPool,
    workspace: String,
}

impl PgUsers {
    /// Wrap a workspace-scoped connection `pool` and the `workspace` slug it resolves to.
    ///
    /// The `users` table is expected to already exist in the pool's schema (provisioned via
    /// the module migration + `gt_create_workspace_schema`). `workspace` is the tenant the
    /// pool's `search_path` points at; it is what every returned [`VerifiedIdentity`] carries,
    /// so it MUST come from the server's workspace context, never from request input.
    pub fn new(pool: PgPool, workspace: impl Into<String>) -> Self {
        PgUsers {
            pool,
            workspace: workspace.into(),
        }
    }

    /// Authenticate `creds` against the store, returning the established identity or the reason
    /// it was rejected.
    ///
    /// Only [`Credentials::EmailPassword`] is served — OAuth/OIDC are
    /// [`AuthError::UnsupportedProvider`], matching the other adapters. An unknown email and a
    /// wrong password are the same [`AuthError::InvalidCredentials`] (no user enumeration); a
    /// query/connection fault is [`AuthError::Backend`] (an outage, not a denied login); a
    /// stored hash that will not parse is [`AuthError::HashFailure`].
    pub async fn authenticate(&self, creds: &Credentials) -> Result<VerifiedIdentity, AuthError> {
        let Credentials::EmailPassword { email, password } = creds else {
            return Err(AuthError::UnsupportedProvider(creds.kind()));
        };

        let row = sqlx::query("SELECT id, password_hash, scopes, roles FROM users WHERE email = $1")
            .bind(email)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;

        // Unknown email is indistinguishable from a wrong password — same error, no enumeration.
        let Some(row) = row else {
            return Err(AuthError::InvalidCredentials);
        };

        let hash: String = row
            .try_get("password_hash")
            .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
        // Reuse the `password-hash` adapter's argon2 check — never a second implementation.
        // A mismatch is InvalidCredentials; a corrupt stored hash is HashFailure.
        verify_password(password, &hash)?;

        let mut identity = row_to_identity(&row, &self.workspace)?;
        // hq-rbac.3: expand the user's assigned roles → their scope bundles and union them with
        // the row's direct scopes. The union is what `into_claims` folds into the JWT, so editing
        // a role re-shapes every assignee's next token without rewriting user rows.
        let roles: Vec<String> = row
            .try_get("roles")
            .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
        let role_scopes = self.expand_roles(&roles).await?;
        identity.scopes = merge_scopes(identity.scopes, role_scopes);
        Ok(identity)
    }

    /// Expand a set of role names into the union of their scope bundles, looked up in the
    /// per-workspace `roles` table (hq-rbac.3). Empty input short-circuits without a query (the
    /// common no-roles case). Order is the table's row order; [`merge_scopes`] de-dups.
    async fn expand_roles(&self, roles: &[String]) -> Result<Vec<String>, AuthError> {
        if roles.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT scopes FROM roles WHERE name = ANY($1)")
            .bind(roles)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
        let mut out = Vec::new();
        for row in &rows {
            let scopes: Vec<String> = row
                .try_get("scopes")
                .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
            out.extend(scopes);
        }
        Ok(out)
    }
}

/// A user's membership in a workspace (hq-identity.1): one `public.user_workspaces` row, naming
/// the workspace slug the user can reach and the role they hold there. Login (hq-identity.2)
/// resolves these to choose the JWT's active workspace + role; switching (hq-identity.3) re-mints
/// against another one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    /// The workspace slug (the `ws_<slug>` tenant the membership grants reach into).
    pub workspace: String,
    /// The role held in that workspace — expanded to scopes against `ws_<slug>.roles` at login.
    /// Empty string ⇒ a member with no role-granted scopes.
    pub role: String,
}

/// Global identity administration over `public.users` + `public.user_workspaces` (hq-identity.1).
///
/// These methods address the GLOBAL, cross-tenant tables by their fully-qualified `public.` name,
/// so — unlike the per-workspace login queries above (unqualified, `search_path`-resolved) — they
/// read/write the same identity rows regardless of which workspace the pool is scoped to. That is
/// the point: identity is shared across tenants, membership is the N:N bridge into them.
impl PgUsers {
    /// Insert a global identity row in `public.users` (hq-identity.1). Idempotent on the unique
    /// email: `ON CONFLICT (email) DO NOTHING`, so a re-run (e.g. the boot admin seed) is a no-op
    /// rather than an error. The password is already argon2-hashed by the caller — never plaintext.
    pub async fn create_global_user(
        &self,
        id: &str,
        email: &str,
        password_hash: &str,
        now: u64,
    ) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO public.users (id, email, password_hash, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $4) ON CONFLICT (email) DO NOTHING",
        )
        .bind(id)
        .bind(email)
        .bind(password_hash)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("global users postgres: {e}")))?;
        Ok(())
    }

    /// Grant `user_id` membership of `workspace` with `role` (hq-identity.1). Idempotent:
    /// `ON CONFLICT (user_id, workspace_slug) DO UPDATE SET role`, so re-assigning updates the
    /// role in place rather than failing the unique pair. The `user_id` must reference an existing
    /// `public.users` row (the FK rejects a dangling membership as [`AuthError::Backend`]).
    pub async fn add_membership(
        &self,
        user_id: &str,
        workspace: &str,
        role: &str,
        now: u64,
    ) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO public.user_workspaces (user_id, workspace_slug, role, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (user_id, workspace_slug) DO UPDATE SET role = EXCLUDED.role",
        )
        .bind(user_id)
        .bind(workspace)
        .bind(role)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("user_workspaces postgres: {e}")))?;
        Ok(())
    }

    /// List a user's workspace memberships, oldest first (hq-identity.1). Empty ⇒ the user exists
    /// but reaches no workspace (login must then reject rather than mint a tenantless token).
    pub async fn memberships(&self, user_id: &str) -> Result<Vec<Membership>, AuthError> {
        let rows = sqlx::query(
            "SELECT workspace_slug, role FROM public.user_workspaces \
             WHERE user_id = $1 ORDER BY created_at, workspace_slug",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("user_workspaces postgres: {e}")))?;
        rows.iter().map(row_to_membership).collect()
    }

    /// Every ACTIVE workspace slug, for a system admin's cross-tenant picker (hq-identity.3
    /// system-admin extension). Sourced from the `public.workspaces` registry written by the
    /// `gt-workspace` domain (a co-located table in the SAME Postgres, read by raw SQL — this crate
    /// keeps NO `gt-workspace` dependency). `public.user_workspaces` is the wrong source: it only
    /// holds GRANTED memberships, so a workspace with no member would be invisible to the admin who
    /// must still reach it. Suspended/archived workspaces are excluded — the admin cannot switch
    /// into a tenant that is no longer live.
    pub async fn all_workspaces(&self) -> Result<Vec<String>, AuthError> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT slug FROM public.workspaces WHERE status = 'active' ORDER BY slug",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("workspaces registry postgres: {e}")))?;
        Ok(rows)
    }

    /// Global login (hq-identity.2): authenticate against `public.users`, resolve the user's
    /// workspace memberships, and stamp the ACTIVE workspace + that membership's role-expanded
    /// scopes onto the returned identity.
    ///
    /// Unlike [`authenticate`](Self::authenticate) — per-workspace, where the slug is the pool's
    /// injected schema — the workspace here comes from the chosen MEMBERSHIP: one global
    /// credential reaches whichever tenants the user belongs to, and the active tenant is still
    /// resolved server-side (never from the payload, docs/04 §15). `preferred_workspace` requests
    /// a specific active tenant (the switch path, hq-identity.3); it is honoured only when the
    /// user actually holds that membership ([`select_active_membership`]), else the default (first)
    /// membership wins.
    ///
    /// A user with NO membership cannot establish a session: [`AuthError::InvalidCredentials`].
    /// They authenticate, but a token must name a tenant — there is none to mint, and returning
    /// the same error as a bad password leaks nothing about which accounts exist but are placeless.
    pub async fn authenticate_global(
        &self,
        creds: &Credentials,
        preferred_workspace: Option<&str>,
    ) -> Result<VerifiedIdentity, AuthError> {
        let Credentials::EmailPassword { email, password } = creds else {
            return Err(AuthError::UnsupportedProvider(creds.kind()));
        };

        let row = sqlx::query("SELECT id, password_hash FROM public.users WHERE email = $1")
            .bind(email)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("global users postgres: {e}")))?;
        // Unknown email is indistinguishable from a wrong password — same error, no enumeration.
        let Some(row) = row else {
            return Err(AuthError::InvalidCredentials);
        };
        let sub: String = row
            .try_get("id")
            .map_err(|e| AuthError::Backend(format!("global users postgres: {e}")))?;
        let hash: Option<String> = row
            .try_get("password_hash")
            .map_err(|e| AuthError::Backend(format!("global users postgres: {e}")))?;
        // SSO-only users have NULL password_hash — they cannot log in with a password.
        let Some(hash) = hash else {
            return Err(AuthError::InvalidCredentials);
        };
        verify_password(password, &hash)?;

        let memberships = self.memberships(&sub).await?;
        let active = select_active_membership(&memberships, preferred_workspace)
            .ok_or(AuthError::InvalidCredentials)?;
        // The membership's role expands to scopes against the ACTIVE workspace's own `roles`
        // catalog (hq-rbac), so the very same role name can grant different scopes per tenant.
        let scopes = self
            .expand_roles_in_workspace(&active.workspace, std::slice::from_ref(&active.role))
            .await?;
        Ok(VerifiedIdentity {
            sub,
            workspace: active.workspace.clone(),
            scopes,
            email: None,
        })
    }

    /// Expand role names into the union of their scope bundles against a SPECIFIC workspace's
    /// `roles` table (`ws_<slug>.roles`) — the variant global login (.2) / switch (.3) need, where
    /// the active workspace changes per request (so the `search_path`-resolved [`expand_roles`] of
    /// the per-ws path does not apply). Empty input — including an empty role name (a member with
    /// no role) — short-circuits without a query. The schema name is derived + validated by
    /// [`workspace_schema`] before interpolation (identifiers cannot be bound), so a malformed slug
    /// is a [`AuthError::Backend`], never injected SQL.
    async fn expand_roles_in_workspace(
        &self,
        workspace: &str,
        roles: &[String],
    ) -> Result<Vec<String>, AuthError> {
        let names: Vec<String> = roles.iter().filter(|r| !r.is_empty()).cloned().collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let schema = workspace_schema(workspace)?;
        let sql = format!("SELECT scopes FROM {schema}.roles WHERE name = ANY($1)");
        let rows = sqlx::query(&sql)
            .bind(&names)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
        let mut out = Vec::new();
        for row in &rows {
            let scopes: Vec<String> = row
                .try_get("scopes")
                .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
            out.extend(scopes);
        }
        // De-dup while preserving first-seen order (two roles may share a scope).
        Ok(merge_scopes(out, Vec::new()))
    }

    /// Resolve `{sub, workspace}` into the identity a switch should re-mint (hq-identity.3): the
    /// user's role in `workspace`, expanded to scopes against that workspace's catalog. `None` when
    /// the user holds NO membership of `workspace` — the switch endpoint maps that to `403`, so a
    /// caller can never re-mint into a tenant they do not belong to. No password is checked here:
    /// the caller already proved identity with a valid bearer; this only re-targets the workspace.
    pub async fn resolve_membership(
        &self,
        sub: &str,
        workspace: &str,
    ) -> Result<Option<VerifiedIdentity>, AuthError> {
        let memberships = self.memberships(sub).await?;
        let Some(membership) = memberships.iter().find(|m| m.workspace == workspace) else {
            return Ok(None);
        };
        let scopes = self
            .expand_roles_in_workspace(workspace, std::slice::from_ref(&membership.role))
            .await?;
        Ok(Some(VerifiedIdentity {
            sub: sub.to_string(),
            workspace: workspace.to_string(),
            scopes,
            email: None,
        }))
    }

    /// Add a global user (looked up by `email`) as a member of `workspace`, holding `role`
    /// (hq-platform-hardening.2). Mirrors the `seed_workspace_rbac` shape that provisions a
    /// workspace's CREATOR, but for a SECOND user an admin attaches afterwards:
    ///
    /// 1. mirror the global identity into the tenant's per-ws login store
    ///    (`ws_<slug>.users`), holding `role` — so the per-tenant `authenticate` agrees the user
    ///    exists in that schema;
    /// 2. insert the `public.user_workspaces` membership — what `/auth/switch` resolves before
    ///    expanding `role` to scopes.
    ///
    /// Returns `Ok(true)` when the email named a known global user (membership written);
    /// `Ok(false)` when no `public.users` row has that email — the caller maps that to `404`
    /// rather than minting a dangling membership. Both writes are idempotent: re-adding updates
    /// the role in place (mirror via `DO UPDATE`, membership via the unique-pair `DO UPDATE`).
    /// The `ws_<slug>` schema must already exist (every provisioned workspace has it).
    pub async fn add_workspace_member(
        &self,
        email: &str,
        workspace: &str,
        role: &str,
        now: u64,
    ) -> Result<bool, AuthError> {
        let schema = workspace_schema(workspace)?;
        // Resolve the global identity by email — only a known user may be granted a membership.
        let global: Option<(String, String)> =
            sqlx::query_as("SELECT id, password_hash FROM public.users WHERE email = $1")
                .bind(email)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("lookup global identity: {e}")))?;
        let Some((user_id, password_hash)) = global else {
            return Ok(false);
        };

        // Mirror into the tenant's per-ws login store, holding `role` (schema validated above,
        // values bound). Re-add updates the role in place rather than failing the PK.
        sqlx::query(&format!(
            "INSERT INTO {schema}.users \
                (id, email, password_hash, scopes, roles, created_at, updated_at) \
             SELECT id, email, $2, ARRAY[]::text[], ARRAY[$3]::text[], $4, $4 \
             FROM public.users WHERE id = $1 \
             ON CONFLICT (id) DO UPDATE SET roles = EXCLUDED.roles, updated_at = EXCLUDED.updated_at"
        ))
        .bind(&user_id)
        .bind(&password_hash)
        .bind(role)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("mirror {schema}.users member: {e}")))?;

        // The cross-tenant membership: FK-safe (user_id was just read from public.users).
        self.add_membership(&user_id, workspace, role, now).await?;
        Ok(true)
    }

    /// Remove a user (by `email`) from `workspace` (hq-platform-hardening.2): delete the
    /// `public.user_workspaces` membership AND the `ws_<slug>.users` mirror, so the user can no
    /// longer `/auth/switch` into the tenant nor authenticate against its per-ws store.
    ///
    /// Returns `Ok(true)` when a membership existed and was deleted; `Ok(false)` when the email
    /// named no global user OR the user held no membership of `workspace` — the caller maps that
    /// to `404` (idempotent remove). The mirror delete is best-effort to the same end: a user
    /// without a membership cannot reach the tenant regardless of a leftover login row.
    pub async fn remove_workspace_member(
        &self,
        email: &str,
        workspace: &str,
    ) -> Result<bool, AuthError> {
        let schema = workspace_schema(workspace)?;
        let user_id: Option<String> =
            sqlx::query_scalar("SELECT id FROM public.users WHERE email = $1")
                .bind(email)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("lookup global identity: {e}")))?;
        let Some(user_id) = user_id else {
            return Ok(false);
        };

        let deleted = sqlx::query(
            "DELETE FROM public.user_workspaces WHERE user_id = $1 AND workspace_slug = $2",
        )
        .bind(&user_id)
        .bind(workspace)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("user_workspaces postgres: {e}")))?
        .rows_affected()
            > 0;

        // Drop the per-ws login mirror too (schema validated above). Harmless if absent.
        sqlx::query(&format!("DELETE FROM {schema}.users WHERE id = $1"))
            .bind(&user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("delete {schema}.users member: {e}")))?;

        Ok(deleted)
    }

    /// JIT (just-in-time) user provisioning for SSO logins (hq-epic.auth-refactor.4): ensure a
    /// global identity row + workspace membership exist for an OAuth/OIDC principal before minting
    /// their first token. Idempotent — re-calling on subsequent logins is a no-op.
    ///
    /// Two writes, both `ON CONFLICT DO NOTHING`:
    /// 1. `public.users (id, email, password_hash = NULL)` — the SSO stub, no password.
    /// 2. `public.user_workspaces (user_id, workspace_slug, role = '')` — a placeless membership
    ///    (no role-granted scopes by default; an admin can promote the user later).
    ///
    /// `email` is taken from the IdP's userinfo; a fallback `{sub}@sso` is used when the IdP
    /// omits the claim so the `NOT NULL UNIQUE` constraint is still satisfied. The `email` value
    /// of an EXISTING row is NOT overwritten — `ON CONFLICT … DO NOTHING` leaves it untouched.
    ///
    /// Migration `0012` must have run first (makes `public.users.password_hash` nullable).
    pub async fn find_or_create_sso_user(
        &self,
        sub: &str,
        email: Option<&str>,
        workspace: &str,
        now: u64,
    ) -> Result<(), AuthError> {
        let resolved_email = email
            .filter(|e| !e.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{sub}@sso"));
        // Upsert the global identity row. ON CONFLICT on `id` (PK) — re-login is a no-op.
        // A second conflict path on `email` (UNIQUE) can arise if the same SSO account was
        // previously registered with a password; we DO NOTHING to avoid clobbering the hash.
        sqlx::query(
            "INSERT INTO public.users (id, email, password_hash, created_at, updated_at) \
             VALUES ($1, $2, NULL, $3, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(sub)
        .bind(&resolved_email)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("sso user upsert: {e}")))?;
        // Ensure a workspace membership exists (no role — admin can promote later).
        sqlx::query(
            "INSERT INTO public.user_workspaces (user_id, workspace_slug, role, created_at) \
             VALUES ($1, $2, '', $3) \
             ON CONFLICT (user_id, workspace_slug) DO NOTHING",
        )
        .bind(sub)
        .bind(workspace)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("sso membership upsert: {e}")))?;
        Ok(())
    }
}

/// Build a [`Membership`] from a `public.user_workspaces` row. A column-read fault is an
/// [`AuthError::Backend`] (an outage, not a denied login).
fn row_to_membership(row: &PgRow) -> Result<Membership, AuthError> {
    Ok(Membership {
        workspace: row
            .try_get("workspace_slug")
            .map_err(|e| AuthError::Backend(format!("user_workspaces postgres: {e}")))?,
        role: row
            .try_get("role")
            .map_err(|e| AuthError::Backend(format!("user_workspaces postgres: {e}")))?,
    })
}

/// Choose a user's active workspace from their memberships (hq-identity.1 seam, reused by login
/// .2 and switch .3): honour `preferred` when the user actually holds a membership of it, else
/// fall back to the first (oldest) membership. Returns `None` only when the user has NO
/// membership at all — the caller maps that to a rejected login, never a tenantless token. Pure,
/// so it is unit-tested without a database.
pub(crate) fn select_active_membership<'a>(
    memberships: &'a [Membership],
    preferred: Option<&str>,
) -> Option<&'a Membership> {
    if let Some(want) = preferred {
        if let Some(found) = memberships.iter().find(|m| m.workspace == want) {
            return Some(found);
        }
    }
    memberships.first()
}

/// Map a workspace slug to its `ws_<slug>` Postgres schema name for a qualified `roles` lookup
/// (hq-identity.2), validating it is a safe unquoted identifier first. A schema name cannot be a
/// bound parameter, so it is interpolated — and therefore MUST be proven to hold only the
/// lowercase DNS-label grammar `WorkspaceId` enforces (`[a-z0-9-]`, ≤ the schema-length cap)
/// before it touches the SQL string. Anything else is an [`AuthError::Backend`], never injected
/// SQL. Mirrors `gt_store_pg::schema_for` (gt-auth must not depend on the kernel store — platform
/// keeps its own one-liner rather than take that dep, docs/03 Rule 4); the `-` → `_` mapping and
/// `ws_` prefix match it exactly so both resolve the same physical schema.
fn workspace_schema(slug: &str) -> Result<String, AuthError> {
    // 60 = 63-byte Postgres identifier limit minus the 3-char `ws_` prefix (gt_store_pg's cap).
    let valid = !slug.is_empty()
        && slug.len() <= 60
        && slug
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !valid {
        return Err(AuthError::Backend(format!("invalid workspace slug: {slug:?}")));
    }
    let mut name = String::with_capacity(3 + slug.len());
    name.push_str("ws_");
    for c in slug.chars() {
        name.push(if c == '-' { '_' } else { c });
    }
    Ok(name)
}

/// Union a user's direct scopes with the scopes expanded from their roles, de-duplicating while
/// preserving first-seen order (direct scopes first, then role scopes). The PG-free seam of the
/// role expansion in [`PgUsers::authenticate`] — pure, unit-tested without a database.
fn merge_scopes(direct: Vec<String>, role_scopes: Vec<String>) -> Vec<String> {
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(direct.len() + role_scopes.len());
    for scope in direct.into_iter().chain(role_scopes) {
        if seen.insert(scope.clone()) {
            out.push(scope);
        }
    }
    out
}

/// Build a [`VerifiedIdentity`] from an authenticated `users` row and the server-injected
/// `workspace` slug. The PG-free seam of [`PgUsers::authenticate`] — pure mapping, unit-tested
/// without a database.
///
/// `sub` is the row's `id`; `scopes` is the row's `TEXT[]`; `workspace` is the tenant the pool
/// is scoped to (not read from the row — it carries none, see the module docs). A column read
/// failure is an [`AuthError::Backend`] fault.
fn row_to_identity(row: &PgRow, workspace: &str) -> Result<VerifiedIdentity, AuthError> {
    let sub: String = row
        .try_get("id")
        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
    let scopes: Vec<String> = row
        .try_get("scopes")
        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
    Ok(VerifiedIdentity {
        sub,
        workspace: workspace.to_string(),
        scopes,
        email: None,
    })
}

/// User administration over the same `users` table (`hq-web-extras.5`). Available when the
/// `axum` admin surface is also on; storage-only — the handler hashes the password first.
#[cfg(feature = "axum")]
#[async_trait::async_trait]
impl crate::http::UserStore for PgUsers {
    async fn create_user(
        &self,
        id: &str,
        email: &str,
        password_hash: &str,
        scopes: &[String],
        now: u64,
    ) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, scopes, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(id)
        .bind(email)
        .bind(password_hash)
        .bind(scopes)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
        Ok(())
    }

    async fn list_users(&self) -> Result<Vec<crate::http::UserSummary>, AuthError> {
        let rows =
            sqlx::query("SELECT id, email, scopes, created_at FROM users ORDER BY created_at")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
        rows.iter()
            .map(|row| {
                Ok(crate::http::UserSummary {
                    sub: row
                        .try_get("id")
                        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?,
                    email: row
                        .try_get("email")
                        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?,
                    scopes: row
                        .try_get("scopes")
                        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?,
                    created_at: row
                        .try_get("created_at")
                        .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?,
                })
            })
            .collect()
    }
}

/// JIT SSO user provisioner (hq-epic.auth-refactor.4): wraps [`PgUsers::find_or_create_sso_user`]
/// behind the [`SsoProvisioner`](crate::http::SsoProvisioner) port so the OAuth callback can call
/// it without knowing about the `PgUsers` type.
#[cfg(all(feature = "axum", feature = "oauth"))]
#[async_trait::async_trait]
impl crate::http::SsoProvisioner for PgUsers {
    async fn provision(
        &self,
        sub: &str,
        email: Option<&str>,
        workspace: &str,
        now: u64,
    ) -> Result<(), AuthError> {
        self.find_or_create_sso_user(sub, email, workspace, now).await
    }
}

/// Role administration + assignment over the per-workspace `roles` table and the `users.roles`
/// column (hq-rbac.4). Available with the `axum` admin surface; the same `PgUsers` adapter backs
/// the login, user-admin, and role-admin ports over one workspace pool.
#[cfg(feature = "axum")]
#[async_trait::async_trait]
impl crate::http::RoleStore for PgUsers {
    async fn upsert_role(&self, name: &str, scopes: &[String], now: u64) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO roles (name, scopes, created_at, updated_at) VALUES ($1, $2, $3, $3) \
             ON CONFLICT (name) DO UPDATE SET scopes = EXCLUDED.scopes, updated_at = EXCLUDED.updated_at",
        )
        .bind(name)
        .bind(scopes)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
        Ok(())
    }

    async fn list_roles(&self) -> Result<Vec<crate::http::RoleSummary>, AuthError> {
        let rows = sqlx::query("SELECT name, scopes, created_at FROM roles ORDER BY created_at")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
        rows.iter()
            .map(|row| {
                Ok(crate::http::RoleSummary {
                    name: row
                        .try_get("name")
                        .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?,
                    scopes: row
                        .try_get("scopes")
                        .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?,
                    created_at: row
                        .try_get("created_at")
                        .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?,
                })
            })
            .collect()
    }

    async fn delete_role(&self, name: &str) -> Result<bool, AuthError> {
        let res = sqlx::query("DELETE FROM roles WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("roles postgres: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    async fn assign_user_roles(
        &self,
        email: &str,
        roles: &[String],
        now: u64,
    ) -> Result<bool, AuthError> {
        let res = sqlx::query("UPDATE users SET roles = $1, updated_at = $2 WHERE email = $3")
            .bind(roles)
            .bind(now as i64)
            .bind(email)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("users postgres: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::password::hash_password;
    use crate::ProviderKind;

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset so the suite
    /// is a no-op off a developer box / CI without PG (same gate as the other `pg` adapters).
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(
            PgPool::connect(&url)
                .await
                .expect("GT_PG_URL must point at a reachable Postgres"),
        )
    }

    /// Provision the `users` table for the test in the connection's current schema. The
    /// migration targets `ws_default`; the contract tests run against the default `search_path`
    /// (`public`/whatever the URL selects), so create an unqualified mirror to exercise the
    /// adapter's unqualified query. Idempotent.
    async fn ensure_users_table(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS users ( \
                id TEXT PRIMARY KEY, \
                email TEXT NOT NULL UNIQUE, \
                password_hash TEXT NOT NULL, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                roles TEXT[] NOT NULL DEFAULT '{}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )",
        )
        .execute(pool)
        .await
        .expect("create users table");
    }

    /// Provision the `roles` catalog (hq-rbac.3) in the connection's current schema, mirroring
    /// migration 0003 unqualified for the contract tests. Idempotent.
    async fn ensure_roles_table(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS roles ( \
                name TEXT PRIMARY KEY, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )",
        )
        .execute(pool)
        .await
        .expect("create roles table");
    }

    /// Provision the global identity tables (hq-identity.1) for the contract test.
    ///
    /// All `pg` contract tests share the connection's default schema (`public`), and the per-ws
    /// login tests above already define `public.users` (via [`ensure_users_table`], the full
    /// per-ws shape WITH `scopes`/`roles`). Migration 0005's production `public.users` is leaner
    /// (identity only — scopes come from membership roles), but the global adapter only ever
    /// touches the identity *subset* of the columns, so reusing the sibling helper here keeps one
    /// agreed `public.users` shape and avoids a clash under parallel test execution. Only
    /// `user_workspaces` is exclusive to this epic.
    async fn ensure_global_identity_tables(pool: &PgPool) {
        ensure_users_table(pool).await;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS public.user_workspaces ( \
                user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE, \
                workspace_slug TEXT NOT NULL, \
                role TEXT NOT NULL DEFAULT '', \
                created_at BIGINT NOT NULL, \
                UNIQUE (user_id, workspace_slug) )",
        )
        .execute(pool)
        .await
        .expect("create public.user_workspaces table");
    }

    /// Provision a per-workspace `roles` catalog in the `ws_<slug>` schema (hq-identity.2), so the
    /// global-login test can prove the active membership's role expands against the RIGHT tenant's
    /// table. `schema` is the already-mapped schema name (e.g. `ws_ida`). Idempotent.
    async fn ensure_ws_roles(pool: &PgPool, schema: &str) {
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(pool)
            .await
            .expect("create ws schema");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.roles ( \
                name TEXT PRIMARY KEY, \
                scopes TEXT[] NOT NULL DEFAULT '{{}}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )"
        ))
        .execute(pool)
        .await
        .expect("create ws roles table");
    }

    /// Provision a per-workspace `users` login mirror in the `ws_<slug>` schema
    /// (hq-platform-hardening.2), the table `add_workspace_member` mirrors into. `schema` is the
    /// already-mapped schema name (e.g. `ws_phm`). Idempotent.
    async fn ensure_ws_users(pool: &PgPool, schema: &str) {
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(pool)
            .await
            .expect("create ws schema");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.users ( \
                id TEXT PRIMARY KEY, \
                email TEXT NOT NULL UNIQUE, \
                password_hash TEXT NOT NULL, \
                scopes TEXT[] NOT NULL DEFAULT '{{}}', \
                roles TEXT[] NOT NULL DEFAULT '{{}}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )"
        ))
        .execute(pool)
        .await
        .expect("create ws users table");
    }

    /// Upsert a role into a specific `ws_<slug>.roles` catalog (hq-identity.2 test helper).
    async fn seed_role_in(pool: &PgPool, schema: &str, name: &str, scopes: &[&str]) {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        sqlx::query(&format!(
            "INSERT INTO {schema}.roles (name, scopes, created_at, updated_at) VALUES ($1, $2, 0, 0) \
             ON CONFLICT (name) DO UPDATE SET scopes = EXCLUDED.scopes"
        ))
        .bind(name)
        .bind(&scopes)
        .execute(pool)
        .await
        .expect("insert ws role");
    }

    async fn seed_user(pool: &PgPool, id: &str, email: &str, hash: &str, scopes: &[&str]) {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        sqlx::query("DELETE FROM users WHERE email = $1")
            .bind(email)
            .execute(pool)
            .await
            .expect("clear seed user");
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, scopes, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, 0, 0)",
        )
        .bind(id)
        .bind(email)
        .bind(hash)
        .bind(&scopes)
        .execute(pool)
        .await
        .expect("insert seed user");
    }

    async fn seed_role(pool: &PgPool, name: &str, scopes: &[&str]) {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        sqlx::query(
            "INSERT INTO roles (name, scopes, created_at, updated_at) VALUES ($1, $2, 0, 0) \
             ON CONFLICT (name) DO UPDATE SET scopes = EXCLUDED.scopes",
        )
        .bind(name)
        .bind(&scopes)
        .execute(pool)
        .await
        .expect("insert seed role");
    }

    // --- PG-free test: the scope-union seam is pure, so no live DB needed. ---

    #[test]
    fn merge_scopes_unions_dedups_and_keeps_direct_first() {
        // Direct scopes come first; role scopes follow; duplicates are dropped on first sight.
        let merged = merge_scopes(
            vec!["beads.read".into(), "merge.read".into()],
            vec!["merge.read".into(), "issues.write".into()],
        );
        assert_eq!(
            merged,
            vec![
                "beads.read".to_string(),
                "merge.read".to_string(),
                "issues.write".to_string()
            ]
        );
        // No roles → the direct scopes pass through unchanged.
        assert_eq!(
            merge_scopes(vec!["rig.read".into()], vec![]),
            vec!["rig.read".to_string()]
        );
    }

    // --- PG-free test: the active-workspace selection seam (hq-identity.1) is pure. ---

    #[test]
    fn select_active_membership_honours_preferred_then_falls_back_to_first() {
        let memberships = vec![
            Membership {
                workspace: "acme".into(),
                role: "admin".into(),
            },
            Membership {
                workspace: "globex".into(),
                role: "member".into(),
            },
        ];
        // A preferred slug the user is a member of wins.
        assert_eq!(
            select_active_membership(&memberships, Some("globex")),
            Some(&memberships[1])
        );
        // A preferred slug the user does NOT hold falls back to the first (oldest) membership —
        // never silently grants the requested tenant.
        assert_eq!(
            select_active_membership(&memberships, Some("ghost")),
            Some(&memberships[0])
        );
        // No preference → the first membership is the default active workspace.
        assert_eq!(
            select_active_membership(&memberships, None),
            Some(&memberships[0])
        );
        // No memberships → None, so the caller rejects rather than minting a tenantless token.
        assert_eq!(select_active_membership(&[], Some("acme")), None);
        assert_eq!(select_active_membership(&[], None), None);
    }

    // --- PG-free test: the slug→schema mapping (hq-identity.2) is pure and rejects unsafe slugs. ---

    #[test]
    fn workspace_schema_maps_valid_slugs_and_rejects_unsafe_ones() {
        // `-` → `_`, `ws_` prefix — matching gt_store_pg::schema_for so both hit one schema.
        assert_eq!(workspace_schema("acme").unwrap(), "ws_acme");
        assert_eq!(workspace_schema("team-1").unwrap(), "ws_team_1");
        assert_eq!(workspace_schema("default").unwrap(), "ws_default");
        // Anything outside the lowercase DNS-label grammar is rejected BEFORE it can reach the
        // interpolated SQL — no quotes, spaces, semicolons, uppercase, or over-long slugs.
        for bad in ["", "Acme", "a b", "drop;", "a\"b", "a'b", "x.y", &"a".repeat(61)] {
            assert!(
                matches!(workspace_schema(bad), Err(AuthError::Backend(_))),
                "slug {bad:?} must be rejected, not interpolated"
            );
        }
    }

    // --- PG-free test: provider-kind gating returns before any I/O, so no live DB needed. ---

    #[tokio::test]
    async fn oauth_credentials_are_unsupported_without_touching_the_db() {
        // A lazily-connected pool never dials until the first query; the OAuth branch returns
        // first, so this exercises the kind gate with no Postgres.
        let pool = PgPool::connect_lazy("postgres://unused/unused").expect("lazy pool");
        let users = PgUsers::new(pool, "acme");
        let got = users
            .authenticate(&Credentials::OAuth {
                provider: "github".into(),
                code: "c".into(),
            })
            .await;
        assert_eq!(
            got,
            Err(AuthError::UnsupportedProvider(ProviderKind::OAuth))
        );
    }

    // --- GT_PG_URL-gated contract tests against a live Postgres. ---

    #[tokio::test]
    async fn authenticates_a_correct_password_and_stamps_the_injected_workspace() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgUsers contract test");
            return;
        };
        ensure_users_table(&pool).await;
        let hash = hash_password("hunter2").unwrap();
        seed_user(&pool, "u-alice", "alice@acme.test", &hash, &["rig.read"]).await;

        let users = PgUsers::new(pool, "acme");
        let got = users
            .authenticate(&Credentials::EmailPassword {
                email: "alice@acme.test".into(),
                password: "hunter2".into(),
            })
            .await
            .expect("login succeeds");

        assert_eq!(
            got,
            VerifiedIdentity {
                sub: "u-alice".into(),
                // Stamped from the server-injected slug, NOT from the row (which has none).
                workspace: "acme".into(),
                scopes: vec!["rig.read".into()],
                email: None,
            }
        );
    }

    #[tokio::test]
    async fn wrong_password_is_invalid_credentials() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgUsers contract test");
            return;
        };
        ensure_users_table(&pool).await;
        let hash = hash_password("hunter2").unwrap();
        seed_user(&pool, "u-bob", "bob@acme.test", &hash, &[]).await;

        let users = PgUsers::new(pool, "acme");
        let err = users
            .authenticate(&Credentials::EmailPassword {
                email: "bob@acme.test".into(),
                password: "nope".into(),
            })
            .await;
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    #[tokio::test]
    async fn unknown_email_is_the_same_invalid_credentials() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgUsers contract test");
            return;
        };
        ensure_users_table(&pool).await;

        let users = PgUsers::new(pool, "acme");
        // No row for this email — must be indistinguishable from a wrong password.
        let err = users
            .authenticate(&Credentials::EmailPassword {
                email: "ghost@acme.test".into(),
                password: "hunter2".into(),
            })
            .await;
        assert_eq!(err, Err(AuthError::InvalidCredentials));
    }

    /// hq-identity.1: the global-identity adapter creates a `public.users` row and attaches N
    /// workspace memberships, which `memberships()` lists oldest-first. Both writes are idempotent
    /// — re-running create is a no-op and re-adding a membership updates its role in place.
    #[tokio::test]
    async fn global_identity_create_and_memberships_round_trip() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping global-identity contract test");
            return;
        };
        ensure_global_identity_tables(&pool).await;
        // Isolate from other runs: drop this id's rows first (cascade clears its memberships).
        sqlx::query("DELETE FROM public.users WHERE id = $1")
            .bind("u-frank")
            .execute(&pool)
            .await
            .expect("clear seed global user");

        let users = PgUsers::new(pool.clone(), "default");
        let hash = hash_password("hunter2").unwrap();
        users
            .create_global_user("u-frank", "frank@global.test", &hash, 10)
            .await
            .expect("create global user");
        // Idempotent: a second create on the same email is a no-op, not an error.
        users
            .create_global_user("u-frank", "frank@global.test", &hash, 11)
            .await
            .expect("re-create global user is idempotent");

        // No memberships yet → the user reaches nothing.
        assert!(users.memberships("u-frank").await.unwrap().is_empty());

        users
            .add_membership("u-frank", "acme", "admin", 12)
            .await
            .unwrap();
        users
            .add_membership("u-frank", "globex", "member", 13)
            .await
            .unwrap();
        let got = users.memberships("u-frank").await.unwrap();
        assert_eq!(
            got,
            vec![
                Membership {
                    workspace: "acme".into(),
                    role: "admin".into()
                },
                Membership {
                    workspace: "globex".into(),
                    role: "member".into()
                },
            ],
            "memberships list oldest-first"
        );

        // Re-adding an existing membership updates the role in place (no duplicate pair).
        users
            .add_membership("u-frank", "acme", "viewer", 14)
            .await
            .unwrap();
        let got = users.memberships("u-frank").await.unwrap();
        assert_eq!(got.len(), 2, "re-add does not duplicate the pair");
        let acme = got.iter().find(|m| m.workspace == "acme").unwrap();
        assert_eq!(acme.role, "viewer", "re-add updates the role");
    }

    /// hq-identity.2: global login authenticates against `public.users`, picks the active
    /// workspace from the user's memberships (default = first, or a held `preferred` one), and
    /// expands THAT membership's role against the active workspace's own `roles` table — so the
    /// same credential yields different scopes depending on which tenant it lands in.
    #[tokio::test]
    async fn global_login_resolves_active_workspace_and_expands_its_role() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping global-login contract test");
            return;
        };
        ensure_global_identity_tables(&pool).await;
        // Two distinct per-workspace role catalogs: the SAME role name grants different scopes.
        ensure_ws_roles(&pool, "ws_ida").await;
        ensure_ws_roles(&pool, "ws_idb").await;
        sqlx::query("DELETE FROM public.users WHERE id = $1")
            .bind("u-gwen")
            .execute(&pool)
            .await
            .expect("clear seed global user");

        let users = PgUsers::new(pool.clone(), "default");
        let hash = hash_password("hunter2").unwrap();
        users
            .create_global_user("u-gwen", "gwen@global.test", &hash, 20)
            .await
            .unwrap();
        // Member of two workspaces, holding role "lead" in each — but the catalogs differ.
        users.add_membership("u-gwen", "ida", "lead", 21).await.unwrap();
        users.add_membership("u-gwen", "idb", "lead", 22).await.unwrap();
        seed_role_in(&pool, "ws_ida", "lead", &["beads.read", "merge.submit"]).await;
        seed_role_in(&pool, "ws_idb", "lead", &["rig.read"]).await;

        let creds = Credentials::EmailPassword {
            email: "gwen@global.test".into(),
            password: "hunter2".into(),
        };

        // No preference → the first membership ("ida") is active; its "lead" → ida's scopes.
        let got = users.authenticate_global(&creds, None).await.expect("login");
        assert_eq!(got.sub, "u-gwen");
        assert_eq!(got.workspace, "ida");
        let set: std::collections::BTreeSet<&str> = got.scopes.iter().map(String::as_str).collect();
        assert_eq!(set, ["beads.read", "merge.submit"].into_iter().collect());

        // Preferring a HELD membership switches the active tenant; the role re-expands in idb.
        let got = users
            .authenticate_global(&creds, Some("idb"))
            .await
            .expect("login preferring idb");
        assert_eq!(got.workspace, "idb");
        assert_eq!(got.scopes, vec!["rig.read".to_string()]);

        // Preferring a tenant the user does NOT hold falls back to the default (ida), never grants
        // the unheld one.
        let got = users
            .authenticate_global(&creds, Some("ghost"))
            .await
            .expect("login preferring an unheld ws");
        assert_eq!(got.workspace, "ida");

        // Wrong password is rejected before any membership lookup.
        let denied = users
            .authenticate_global(
                &Credentials::EmailPassword {
                    email: "gwen@global.test".into(),
                    password: "nope".into(),
                },
                None,
            )
            .await;
        assert_eq!(denied, Err(AuthError::InvalidCredentials));

        // A correct credential with NO membership cannot establish a session.
        sqlx::query("DELETE FROM public.user_workspaces WHERE user_id = $1")
            .bind("u-gwen")
            .execute(&pool)
            .await
            .unwrap();
        let placeless = users.authenticate_global(&creds, None).await;
        assert_eq!(placeless, Err(AuthError::InvalidCredentials));
    }

    /// hq-platform-hardening.2: a workspace admin adds a SECOND user, who can then switch into the
    /// workspace; removing them revokes that reach. Proves the full membership-admin seam end to
    /// end against a live PG — `add_workspace_member` mirrors into `ws_<slug>.users` AND inserts
    /// the `public.user_workspaces` bridge `resolve_membership` (the switch path) reads, and
    /// `remove_workspace_member` undoes both.
    #[tokio::test]
    async fn membership_admin_add_lets_a_second_user_switch_then_remove_revokes() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping membership-admin contract test");
            return;
        };
        ensure_global_identity_tables(&pool).await;
        // The target tenant's per-ws catalogs: a `member` role + the login-mirror table.
        ensure_ws_roles(&pool, "ws_phm").await;
        ensure_ws_users(&pool, "ws_phm").await;
        seed_role_in(&pool, "ws_phm", "member", &["beads.read"]).await;
        // Isolate from prior runs (cascade clears memberships); drop the mirror rows too.
        for id in ["u-phm-admin", "u-phm-bob"] {
            sqlx::query("DELETE FROM public.users WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("clear seed global user");
            sqlx::query("DELETE FROM ws_phm.users WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("clear ws mirror");
        }

        let users = PgUsers::new(pool.clone(), "default");
        let hash = hash_password("hunter2").unwrap();
        // Two distinct global identities — the workspace admin and the user they will add.
        users
            .create_global_user("u-phm-admin", "admin@phm.test", &hash, 1)
            .await
            .unwrap();
        users
            .create_global_user("u-phm-bob", "bob@phm.test", &hash, 2)
            .await
            .unwrap();

        // Before the add, Bob holds NO membership of `phm`, so a switch would resolve to None.
        assert!(users.resolve_membership("u-phm-bob", "phm").await.unwrap().is_none());

        // The admin adds Bob as a `member` of `phm`.
        assert!(
            users.add_workspace_member("bob@phm.test", "phm", "member", 3).await.unwrap(),
            "adding a known global user writes the membership"
        );

        // Bob can now switch into `phm`: the membership resolves and `member` expands to its scopes.
        let switched = users
            .resolve_membership("u-phm-bob", "phm")
            .await
            .unwrap()
            .expect("the added user can switch into the workspace");
        assert_eq!(switched.sub, "u-phm-bob");
        assert_eq!(switched.workspace, "phm");
        assert_eq!(switched.scopes, vec!["beads.read".to_string()]);

        // The mirror landed in the tenant's login store with the role, so the per-ws store agrees.
        let (mid, mroles): (String, Vec<String>) =
            sqlx::query_as("SELECT id, roles FROM ws_phm.users WHERE id = $1")
                .bind("u-phm-bob")
                .fetch_one(&pool)
                .await
                .expect("the added user is mirrored into ws_phm.users");
        assert_eq!(mid, "u-phm-bob");
        assert_eq!(mroles, vec!["member".to_string()]);

        // Adding an UNKNOWN email is a no-op the handler maps to 404 — no dangling membership.
        assert!(
            !users.add_workspace_member("ghost@phm.test", "phm", "member", 4).await.unwrap(),
            "an unknown email writes nothing"
        );

        // Removing Bob revokes the membership AND the mirror — he can no longer switch in.
        assert!(
            users.remove_workspace_member("bob@phm.test", "phm").await.unwrap(),
            "removing a member returns true"
        );
        assert!(
            users.resolve_membership("u-phm-bob", "phm").await.unwrap().is_none(),
            "the removed user can no longer switch into the workspace"
        );
        let mirror_left: i64 = sqlx::query_scalar("SELECT count(*) FROM ws_phm.users WHERE id = $1")
            .bind("u-phm-bob")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(mirror_left, 0, "the per-ws mirror is dropped on remove");

        // Removing again (or an unknown user) is an idempotent 404 (no membership left).
        assert!(!users.remove_workspace_member("bob@phm.test", "phm").await.unwrap());
        assert!(!users.remove_workspace_member("ghost@phm.test", "phm").await.unwrap());
    }

    /// hq-rbac.3: a user's assigned roles expand to their scope bundles and union with the row's
    /// direct scopes at login — the union is what rides the minted token.
    #[tokio::test]
    async fn login_expands_assigned_roles_into_the_identity_scopes() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping role-expansion contract test");
            return;
        };
        ensure_users_table(&pool).await;
        ensure_roles_table(&pool).await;
        let hash = hash_password("hunter2").unwrap();
        // A direct scope on the user, plus two roles whose bundles overlap (`merge.read`).
        seed_user(&pool, "u-dana", "dana@acme.test", &hash, &["beads.read"]).await;
        seed_role(&pool, "reviewer", &["merge.read", "merge.submit"]).await;
        seed_role(&pool, "reader", &["merge.read", "issues.read"]).await;
        sqlx::query("UPDATE users SET roles = $1 WHERE email = $2")
            .bind(vec!["reviewer".to_string(), "reader".to_string()])
            .bind("dana@acme.test")
            .execute(&pool)
            .await
            .expect("assign roles");

        let users = PgUsers::new(pool, "acme");
        let got = users
            .authenticate(&Credentials::EmailPassword {
                email: "dana@acme.test".into(),
                password: "hunter2".into(),
            })
            .await
            .expect("login succeeds");

        // Direct scope first, then the (deduped) union of both roles' scopes. Role row order is
        // not guaranteed, so assert membership + the direct-first invariant rather than an order.
        assert_eq!(got.scopes[0], "beads.read", "direct scope leads");
        let set: std::collections::BTreeSet<&str> = got.scopes.iter().map(String::as_str).collect();
        assert_eq!(
            set,
            ["beads.read", "merge.read", "merge.submit", "issues.read"]
                .into_iter()
                .collect(),
        );
        // `merge.read` is granted by both roles but appears once.
        assert_eq!(got.scopes.iter().filter(|s| *s == "merge.read").count(), 1);
    }

    /// hq-rbac.4: the role-admin adapter round-trips — upsert, list, delete — and an assignment
    /// written through it feeds login-time expansion (the .3 path) end to end.
    #[cfg(feature = "axum")]
    #[tokio::test]
    async fn role_store_crud_and_assignment_feeds_expansion() {
        use crate::http::RoleStore;
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping role-store contract test");
            return;
        };
        ensure_users_table(&pool).await;
        ensure_roles_table(&pool).await;
        let users = PgUsers::new(pool.clone(), "acme");

        // Upsert creates, then replaces the scope bundle for the same name.
        RoleStore::upsert_role(&users, "crud-role", &["beads.read".into()], 1)
            .await
            .unwrap();
        RoleStore::upsert_role(
            &users,
            "crud-role",
            &["beads.read".into(), "merge.submit".into()],
            2,
        )
        .await
        .unwrap();
        let listed = RoleStore::list_roles(&users).await.unwrap();
        let role = listed
            .iter()
            .find(|r| r.name == "crud-role")
            .expect("role listed");
        assert_eq!(
            role.scopes,
            vec!["beads.read".to_string(), "merge.submit".to_string()]
        );

        // Assign the role to a user, then log in: the role's scopes ride the identity (.3).
        let hash = hash_password("hunter2").unwrap();
        seed_user(&pool, "u-erin", "erin@acme.test", &hash, &["rig.read"]).await;
        assert!(
            RoleStore::assign_user_roles(&users, "erin@acme.test", &["crud-role".into()], 3)
                .await
                .unwrap(),
            "assignment to a known user returns true"
        );
        assert!(
            !RoleStore::assign_user_roles(&users, "ghost@acme.test", &["crud-role".into()], 3)
                .await
                .unwrap(),
            "assignment to an unknown user returns false"
        );
        let identity = users
            .authenticate(&Credentials::EmailPassword {
                email: "erin@acme.test".into(),
                password: "hunter2".into(),
            })
            .await
            .expect("login");
        let set: std::collections::BTreeSet<&str> =
            identity.scopes.iter().map(String::as_str).collect();
        assert!(
            set.contains("rig.read") && set.contains("beads.read") && set.contains("merge.submit")
        );

        // Delete is idempotent: true once, false thereafter.
        assert!(RoleStore::delete_role(&users, "crud-role").await.unwrap());
        assert!(!RoleStore::delete_role(&users, "crud-role").await.unwrap());
    }

    // --- mint.4: the full login flow against the REAL stack -------------------------------------
    //
    // Where the tests above stop at `PgUsers -> VerifiedIdentity`, this drives the whole login
    // pipeline end to end with the production adapters: credentials -> `PgUsers` (argon2 against
    // PG) -> `VerifiedIdentity` -> `into_claims` -> `JwtMinter` (access JWT) -> `JwtAuthenticator`
    // verifies that very token back into the same claims, plus a `RefreshStore` issuing the
    // long-lived counterpart. It is gated on the `jsonwebtoken` feature *as well* (the mint/verify
    // adapters live behind it): with only `pg` on it compiles out, so the contract tests above
    // still run on a pg-only build. Like them, it is a no-op without `GT_PG_URL`.
    #[cfg(feature = "jsonwebtoken")]
    #[tokio::test]
    async fn login_e2e_mints_an_access_token_the_verifier_accepts_and_issues_a_refresh() {
        use crate::{
            Authenticator, InMemoryRefreshStore, JwtAuthenticator, JwtMinter, RefreshStore,
        };

        // The same throwaway 2048-bit RSA keypair the mint/verify slices use: sign with the
        // private half, verify against its public half — a real RS256 round-trip, no shared secret.
        const PRIV_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
        const PUB_PEM: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");

        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping login e2e test");
            return;
        };
        ensure_users_table(&pool).await;
        let hash = hash_password("hunter2").unwrap();
        seed_user(
            &pool,
            "u-carol",
            "carol@acme.test",
            &hash,
            &["rig.read", "rig.write"],
        )
        .await;

        let users = PgUsers::new(pool, "acme");

        // 1. credentials -> PgUsers (argon2 verified against PG) -> VerifiedIdentity.
        let identity = users
            .authenticate(&Credentials::EmailPassword {
                email: "carol@acme.test".into(),
                password: "hunter2".into(),
            })
            .await
            .expect("correct password authenticates");
        assert_eq!(identity.sub, "u-carol");
        // Workspace is the server-injected slug, never anything from the payload (docs/04 §15).
        assert_eq!(identity.workspace, "acme");
        assert_eq!(
            identity.scopes,
            vec!["rig.read".to_string(), "rig.write".to_string()]
        );

        // 2. VerifiedIdentity.into_claims(exp, iat) -> JwtMinter.mint -> the access JWT.
        let (iat, exp) = (1_000u64, 1_900u64);
        let claims = identity.clone().into_claims(exp, iat);
        let minter = JwtMinter::from_rsa_pem(PRIV_PEM).unwrap().with_kid("k1");
        let access = minter.mint(&claims).expect("mint access token");

        // 3. JwtAuthenticator (the public half of the pair) verifies the access token back into
        //    the SAME claims — proving the mint -> verify round-trip end to end.
        let verifier = JwtAuthenticator::from_kid_pems([("k1", PUB_PEM)]).unwrap();
        let verified = verifier.authenticate(&access).expect("verify access token");
        assert_eq!(verified, claims);
        assert_eq!(verified.sub, "u-carol");
        assert_eq!(verified.workspace, "acme");
        assert_eq!(
            verified.scopes,
            vec!["rig.read".to_string(), "rig.write".to_string()]
        );
        // The signature-verified token still passes the semantic clock/workspace gate in-window.
        assert_eq!(verified.validate(iat + 1, false), Ok(()));

        // 4. RefreshStore issues the long-lived counterpart for the same session, carrying the
        //    identity's sub/workspace/scopes.
        let store = InMemoryRefreshStore::new();
        let (refresh, record) = store.issue(
            &identity.sub,
            &identity.workspace,
            &identity.scopes,
            iat,
            exp,
        );
        assert!(!refresh.as_str().is_empty());
        assert_eq!(record.sub, "u-carol");
        assert_eq!(record.workspace, "acme");
        assert_eq!(record.scopes, identity.scopes);
        assert_eq!(record.status, crate::RefreshStatus::Active);

        // Negative: a wrong password is rejected at the login step, so the flow never reaches the
        // minter — there is no token to mint for a denied login.
        let denied = users
            .authenticate(&Credentials::EmailPassword {
                email: "carol@acme.test".into(),
                password: "wrong".into(),
            })
            .await;
        assert_eq!(denied, Err(AuthError::InvalidCredentials));
    }

    // --- JIT provisioning contract test (hq-epic.auth-refactor.4) ---

    #[tokio::test]
    async fn find_or_create_sso_user_provisions_and_is_idempotent() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping JIT-provisioning contract test");
            return;
        };
        ensure_users_table(&pool).await;
        // Ensure the workspace membership bridge exists so the FK is satisfied.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS public.user_workspaces ( \
               user_id TEXT NOT NULL, \
               workspace_slug TEXT NOT NULL, \
               role TEXT NOT NULL DEFAULT '', \
               created_at BIGINT NOT NULL, \
               PRIMARY KEY (user_id, workspace_slug) \
             )",
        )
        .execute(&pool)
        .await
        .expect("ensure user_workspaces table");

        let users = PgUsers::new(pool.clone(), "sso-ws");

        // First call: provisions a new SSO user row + membership.
        users
            .find_or_create_sso_user("sso-sub-1", Some("sso@idp.test"), "sso-ws", 1_000_000)
            .await
            .expect("first provision succeeds");

        let email: String =
            sqlx::query_scalar("SELECT email FROM public.users WHERE id = 'sso-sub-1'")
                .fetch_one(&pool)
                .await
                .expect("user row exists after provision");
        assert_eq!(email, "sso@idp.test");

        let ws_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.user_workspaces WHERE user_id = 'sso-sub-1' AND workspace_slug = 'sso-ws'")
                .fetch_one(&pool)
                .await
                .expect("membership row query");
        assert_eq!(ws_count, 1, "exactly one membership row");

        // Second call (re-login): idempotent — no error, no duplicate rows.
        users
            .find_or_create_sso_user("sso-sub-1", Some("sso@idp.test"), "sso-ws", 1_000_001)
            .await
            .expect("second provision is a no-op");

        let ws_count_after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.user_workspaces WHERE user_id = 'sso-sub-1' AND workspace_slug = 'sso-ws'")
                .fetch_one(&pool)
                .await
                .expect("membership row query after re-login");
        assert_eq!(ws_count_after, 1, "still exactly one membership row");

        // Fallback email: IdP omits the claim → synthetic `{sub}@sso` address is used.
        users
            .find_or_create_sso_user("sso-sub-no-email", None, "sso-ws", 1_000_002)
            .await
            .expect("provision with no email falls back to synthetic address");

        let fallback_email: String =
            sqlx::query_scalar("SELECT email FROM public.users WHERE id = 'sso-sub-no-email'")
                .fetch_one(&pool)
                .await
                .expect("user row with synthetic email exists");
        assert_eq!(fallback_email, "sso-sub-no-email@sso");
    }
}
