//! `workspace.*` domain dispatch (`hq-mcp-dispatch.2`).
//!
//! Routes the `workspace.create` / `workspace.list` / `workspace.info` tools onto
//! the [`WorkspaceCommand`] decide/apply layer over the PG-backed
//! [`PgWorkspaces`] adapter. `create` runs the full
//! [`WorkspaceActor`](gt_workspace::WorkspaceActor) cycle (decide → apply →
//! persist → log) so the catalog is mutated only through replayable events;
//! `list` / `info` are pure reads straight off the repository.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::RwLock;

use gt_mcp_server::{DomainCtx, DomainHandler, GateStatus, WorkspaceStatusGate};
use gt_module::McpTool;
use gt_store_dolt::{AppError, WorkspacePools};
use gt_store_pg::schema_for;
use gt_workspace::{
    ActorError, PgWorkspaces, WorkspaceActor, WorkspaceCommand, WorkspaceEntry, WorkspaceError,
    WorkspaceId, WorkspaceRepository, WorkspaceStatus,
};

use super::util::{descriptor, req};

/// PG-backed handler for the `workspace.*` tool namespace.
///
/// Holds a [`PgPool`]; every call builds a [`PgWorkspaces`] over a clone (cheap —
/// `PgPool` is an `Arc`) so the workspace catalog is the durable Postgres table,
/// shared across requests.
pub struct WorkspaceHandler {
    pool: PgPool,
    /// Per-workspace Dolt pools, when multi-tenant Dolt routing is configured
    /// (`GT_DOLT_BASE_URL`). `workspace.create` uses this to `CREATE DATABASE
    /// hq_<slug>` and seed its `issues` schema so the new tenant's tracker works
    /// end to end. `None` ⇒ single-tenant Dolt; create provisions only the PG
    /// side (the Dolt DB is the shared `hq`).
    dolt: Option<Arc<WorkspacePools>>,
}

impl WorkspaceHandler {
    /// Wrap a connection pool. The `workspaces` table is expected to already
    /// exist (applied via the `gt-store-pg` migration at boot).
    pub fn new(pool: PgPool) -> Self {
        Self { pool, dolt: None }
    }

    /// Attach the per-workspace Dolt pools so `workspace.create` also provisions
    /// the tenant's `hq_<slug>` database + issues schema (hq-gap-workspace-provision-full).
    pub fn with_dolt(mut self, dolt: Arc<WorkspacePools>) -> Self {
        self.dolt = Some(dolt);
        self
    }

    /// A repository over the shared pool.
    fn repo(&self) -> PgWorkspaces {
        PgWorkspaces::new(self.pool.clone())
    }

    /// The global-identity adapter over the shared pool, backing the membership tools
    /// (hq-platform-hardening.2). Its add/remove write fully-qualified `public.`/`ws_<slug>.`
    /// tables, so the pool's default schema is irrelevant; the slug is passed per call.
    fn users(&self) -> gt_auth::PgUsers {
        gt_auth::PgUsers::new(self.pool.clone(), "default")
    }

    /// The GLOBAL OAuth/OIDC provider store over the shared pool, backing the system-admin
    /// provider tools (hq-idp-db.4). CRUD writes the `public.oauth_providers` table, sealing the
    /// client secret at rest; the slug is irrelevant (providers are deploy-global). Behind the
    /// `oauth` feature (the provider store + secret crypto).
    #[cfg(feature = "oauth")]
    fn providers(&self) -> gt_auth::PgProviderRepo {
        gt_auth::PgProviderRepo::new(self.pool.clone())
    }
}

#[async_trait]
impl DomainHandler for WorkspaceHandler {
    fn namespace(&self) -> &'static str {
        "workspace"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        #[allow(unused_mut)]
        let mut tools = vec![
            descriptor(
                "workspace.create",
                "Provision a new workspace (tenant) in the catalog.",
                &[req("id", "string"), req("name", "string")],
            ),
            descriptor(
                "workspace.suspend",
                "Reversibly disable an active workspace.",
                &[req("id", "string")],
            ),
            descriptor(
                "workspace.resume",
                "Restore a suspended workspace to active.",
                &[req("id", "string")],
            ),
            descriptor(
                "workspace.archive",
                "Archive a workspace (terminal transition from active or suspended).",
                &[req("id", "string")],
            ),
            descriptor("workspace.list", "List every workspace in the catalog.", &[]),
            descriptor("workspace.info", "Show one workspace's id, name + status.", &[req("id", "string")]),
            // Membership management (hq-platform-hardening.2): a ws admin adds/removes ANOTHER
            // user. Not in the `workspace_member` preset's allow-list, so the MCP scope gate
            // reserves these for `workspace.admin` (the same ws-admin guard the REST surface uses).
            descriptor(
                "workspace.member-add",
                "Add another user (by email) to a workspace, holding a role.",
                &[req("id", "string"), req("email", "string"), req("role", "string")],
            ),
            descriptor(
                "workspace.member-remove",
                "Remove a user (by email) from a workspace.",
                &[req("id", "string"), req("email", "string")],
            ),
        ];
        // OAuth/OIDC provider administration (hq-idp-db.4): a SYSTEM admin manages the GLOBAL login
        // providers. Reserved for `*`/admin by the scope gate (absent from the member preset), the
        // same shape as the membership tools. The REST sibling lives on `/auth/providers`, so the
        // MCP↔HTTP parity test exempts these tools. The `client_secret` is write-only — accepted on
        // create/update, sealed at rest, and never in any read/list output.
        #[cfg(feature = "oauth")]
        tools.extend([
            descriptor(
                "workspace.provider-list",
                "List the GLOBAL OAuth/OIDC login providers (never the client secret).",
                &[],
            ),
            descriptor(
                "workspace.provider-create",
                "Register a GLOBAL OAuth/OIDC login provider (secret write-only, sealed at rest).",
                &[req("id", "string"), req("kind", "string"), req("client_id", "string"), req("client_secret", "string")],
            ),
            descriptor(
                "workspace.provider-update",
                "Update a GLOBAL OAuth/OIDC provider (toggle enabled, rotate secret, edit endpoints).",
                &[req("id", "string")],
            ),
            descriptor(
                "workspace.provider-delete",
                "Delete a GLOBAL OAuth/OIDC login provider.",
                &[req("id", "string")],
            ),
        ]);
        tools
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "workspace.create" => {
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let name = str_arg(&ctx.args, "name")?.to_string();
                // Hydrate from Postgres, then run the command through the actor so
                // the change is an applied event (replay-authoritative), persisted
                // back to the `workspaces` table.
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Create {
                        id: id.clone(),
                        name: name.clone(),
                    })
                    .await
                    .map_err(actor_err)?;
                // Provision the tenant's PG schema + RBAC and (when multi-tenant) its Dolt DB +
                // issues schema, so the workspace is usable the moment it is created. Shared with
                // the REST `POST /api/v1/workspace` path via `provision_tenant`
                // (hq-gap-workspace-rest-create-provision).
                provision_tenant(&self.pool, self.dolt.as_ref(), id.as_str(), ctx.actor).await?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "name": name,
                    "status": status_str(WorkspaceStatus::Active),
                }))
            }
            "workspace.suspend" => {
                // Reversibly disable an active workspace. The decide layer rejects a
                // non-active source (IllegalTransition) as a validation fault, so a
                // suspended/archived workspace cannot be re-suspended.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Suspend { id: id.clone() })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "status": status_str(WorkspaceStatus::Suspended),
                }))
            }
            "workspace.resume" => {
                // Restore a suspended workspace to active (the suspend counterpart,
                // hq-mt-bootstrap.4). The decide layer rejects a non-suspended source
                // (IllegalTransition) as a validation fault. The on-disk restore —
                // untar the snapshot, pg_restore, replay the event log, verify the
                // checksum — is a deploy-edge step (like archive's teardown); this
                // tool records only the orchestrator's status change back to active.
                // The suspend/archive gate (hq-mt-bootstrap.8) whitelists this tool,
                // so a suspended tenant can be brought back.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Resume { id: id.clone() })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "status": status_str(WorkspaceStatus::Active),
                }))
            }
            "workspace.archive" => {
                // Terminal transition from Active or Suspended. The decide layer rejects an
                // already-archived workspace (IllegalTransition) as a validation fault. The
                // on-disk teardown (Dolt snapshot + tar the event log) is a deploy-edge step;
                // this tool records only the orchestrator's status change.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let mut actor = WorkspaceActor::hydrate(self.repo()).await.map_err(actor_err)?;
                actor
                    .handle(WorkspaceCommand::Archive { id: id.clone() })
                    .await
                    .map_err(actor_err)?;
                Ok(json!({
                    "ok": true,
                    "id": id.as_str(),
                    "status": status_str(WorkspaceStatus::Archived),
                }))
            }
            "workspace.list" => {
                let entries = self.repo().list().await.map_err(repo_err)?;
                Ok(json!({
                    "workspaces": entries.iter().map(entry_json).collect::<Vec<_>>(),
                }))
            }
            "workspace.info" => {
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                match self.repo().load(&id).await.map_err(repo_err)? {
                    Some(entry) => Ok(entry_json(&entry)),
                    None => Err(AppError::NotFound(format!("workspace {id}"))),
                }
            }
            "workspace.member-add" => {
                // Add another user to the workspace (hq-platform-hardening.2). The MCP scope gate
                // already reserves this tool for `workspace.admin` (it is absent from the member
                // preset), so this is the ws admin attaching a second user — mirroring them into
                // `ws_<slug>.users` + inserting the `public.user_workspaces` membership the
                // `/auth/switch` path resolves.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let email = str_arg(&ctx.args, "email")?;
                let role = str_arg(&ctx.args, "role")?;
                let added = self
                    .users()
                    .add_workspace_member(email, id.as_str(), role, now_secs())
                    .await
                    .map_err(auth_err)?;
                if !added {
                    return Err(AppError::NotFound(format!("user {email}")));
                }
                Ok(json!({ "ok": true, "id": id.as_str(), "email": email, "role": role }))
            }
            "workspace.member-remove" => {
                // Revoke another user's membership (hq-platform-hardening.2), the member-add
                // counterpart — drops both the membership and the per-ws login mirror.
                let id = workspace_id(str_arg(&ctx.args, "id")?)?;
                let email = str_arg(&ctx.args, "email")?;
                let removed = self
                    .users()
                    .remove_workspace_member(email, id.as_str())
                    .await
                    .map_err(auth_err)?;
                if !removed {
                    return Err(AppError::NotFound(format!("member {email} of {}", id.as_str())));
                }
                Ok(json!({ "ok": true, "id": id.as_str(), "email": email }))
            }
            #[cfg(feature = "oauth")]
            "workspace.provider-list" => {
                // System-admin read of the GLOBAL providers. The scope gate already reserved this
                // for `*`/admin. The projection drops every secret — only `provider_view` fields.
                let records = gt_auth::ProviderRepo::list(&self.providers())
                    .await
                    .map_err(provider_err)?;
                Ok(json!({
                    "providers": records.iter().map(provider_view).collect::<Vec<_>>(),
                }))
            }
            #[cfg(feature = "oauth")]
            "workspace.provider-create" => {
                let new = new_provider_from_args(&ctx.args)?;
                let stored = gt_auth::ProviderRepo::create(&self.providers(), new)
                    .await
                    .map_err(provider_err)?;
                Ok(provider_view(&stored))
            }
            #[cfg(feature = "oauth")]
            "workspace.provider-update" => {
                let id = str_arg(&ctx.args, "id")?.to_string();
                let patch = patch_provider_from_args(&ctx.args);
                match gt_auth::ProviderRepo::patch(&self.providers(), &id, patch)
                    .await
                    .map_err(provider_err)?
                {
                    Some(updated) => Ok(provider_view(&updated)),
                    None => Err(AppError::NotFound(format!("provider {id}"))),
                }
            }
            #[cfg(feature = "oauth")]
            "workspace.provider-delete" => {
                let id = str_arg(&ctx.args, "id")?.to_string();
                let removed = gt_auth::ProviderRepo::delete(&self.providers(), &id)
                    .await
                    .map_err(provider_err)?;
                if !removed {
                    return Err(AppError::NotFound(format!("provider {id}")));
                }
                Ok(json!({ "ok": true, "id": id }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Project a [`ProviderRecord`](gt_auth::ProviderRecord) onto the MCP dispatch payload, OMITTING
/// the `client_secret` (sealed or plain) — the write-only secret never appears in any read/echo
/// (hq-idp-db.4), mirroring the REST `ProviderView`.
#[cfg(feature = "oauth")]
fn provider_view(r: &gt_auth::ProviderRecord) -> Value {
    json!({
        "id": r.id,
        "kind": r.kind.as_str(),
        "display_name": r.display_name,
        "client_id": r.client_id,
        "issuer": r.issuer,
        "authorize_endpoint": r.authorize_endpoint,
        "token_endpoint": r.token_endpoint,
        "userinfo_endpoint": r.userinfo_endpoint,
        "scopes": r.scopes,
        "enabled": r.enabled,
    })
}

/// Build a [`NewProvider`](gt_auth::NewProvider) from the `workspace.provider-create` args, filling
/// a preset kind's baked endpoints/scopes when absent and requiring every endpoint for `generic`.
/// An unknown kind / a generic provider missing an endpoint is a validation fault.
#[cfg(feature = "oauth")]
fn new_provider_from_args(args: &Value) -> Result<gt_auth::NewProvider, AppError> {
    let kind = gt_auth::OauthProviderKind::parse(str_arg(args, "kind")?)
        .map_err(|e| AppError::Validation(e.to_string()))?;
    let preset = gt_auth::preset_for(kind);
    let opt = |key: &str| args.get(key).and_then(Value::as_str).map(str::to_owned);
    let need = |key: &str, from_preset: Option<&str>| {
        opt(key)
            .or_else(|| from_preset.map(str::to_owned))
            .ok_or_else(|| AppError::Validation(format!("missing `{key}` for a generic provider")))
    };
    let id = str_arg(args, "id")?.to_string();
    let display_name = opt("display_name")
        .or_else(|| preset.as_ref().map(|p| p.display_name.to_owned()))
        .unwrap_or_else(|| id.clone());
    Ok(gt_auth::NewProvider {
        id,
        kind,
        display_name,
        client_id: str_arg(args, "client_id")?.to_string(),
        client_secret: str_arg(args, "client_secret")?.to_string(),
        issuer: need("issuer", preset.as_ref().map(|p| p.issuer))?,
        authorize_endpoint: need("authorize_endpoint", preset.as_ref().map(|p| p.authorize_endpoint))?,
        token_endpoint: need("token_endpoint", preset.as_ref().map(|p| p.token_endpoint))?,
        userinfo_endpoint: need("userinfo_endpoint", preset.as_ref().map(|p| p.userinfo_endpoint))?,
        scopes: need("scopes", preset.as_ref().map(|p| p.default_scopes))?,
        enabled: args.get("enabled").and_then(Value::as_bool).unwrap_or(true),
    })
}

/// Build a [`PatchProvider`](gt_auth::PatchProvider) from the `workspace.provider-update` args —
/// each present field overwrites, each absent one leaves the column. `client_secret` is write-only
/// (supply to rotate, omit to keep).
#[cfg(feature = "oauth")]
fn patch_provider_from_args(args: &Value) -> gt_auth::PatchProvider {
    let opt = |key: &str| args.get(key).and_then(Value::as_str).map(str::to_owned);
    gt_auth::PatchProvider {
        display_name: opt("display_name"),
        client_id: opt("client_id"),
        client_secret: opt("client_secret"),
        issuer: opt("issuer"),
        authorize_endpoint: opt("authorize_endpoint"),
        token_endpoint: opt("token_endpoint"),
        userinfo_endpoint: opt("userinfo_endpoint"),
        scopes: opt("scopes"),
        enabled: args.get("enabled").and_then(Value::as_bool),
    }
}

/// Map a `gt-auth` provider-store failure onto the MCP error space (hq-idp-db.4): an invalid kind
/// is a caller-side validation fault, any other (sealing/backend) is an internal fault.
#[cfg(feature = "oauth")]
fn provider_err(e: gt_auth::AuthError) -> AppError {
    match e {
        gt_auth::AuthError::Backend(msg) if msg.starts_with("unknown provider kind") => {
            AppError::Validation(msg)
        }
        other => AppError::Other(other.to_string()),
    }
}

/// Provision a freshly-created workspace's per-tenant backends: clone the `ws_<slug>` PG schema
/// from `ws_default`, seed its RBAC (so `/auth/switch` yields a usable token), and — when
/// multi-tenant Dolt routing is configured — `CREATE DATABASE hq_<slug>` + apply its issues
/// schema. Shared by the MCP `workspace.create` tool and the REST `POST /api/v1/workspace`
/// handler (`hq-gap-workspace-rest-create-provision`) so both transports provision identically.
/// Idempotent throughout. `actor` is the creator's verified id (membership lands for them; `""`
/// ⇒ schema/role only, no membership).
pub(crate) async fn provision_tenant(
    pool: &PgPool,
    dolt: Option<&Arc<WorkspacePools>>,
    slug: &str,
    actor: &str,
) -> Result<(), AppError> {
    // Clone the `ws_default` template (CREATE TABLE LIKE) into `ws_<slug>` — idempotent, and
    // re-running repairs a tenant created before this fix.
    sqlx::query("SELECT gt_create_workspace_schema($1)")
        .bind(slug)
        .execute(pool)
        .await
        .map_err(|e| AppError::Other(format!("provision schema for {slug}: {e}")))?;
    // The clone copies STRUCTURE only, so the new roles/users tables are empty; seed the RBAC.
    seed_workspace_rbac(pool, slug, actor).await?;
    // Dolt side, only under multi-tenant routing: CREATE DATABASE over a server-scoped pool (a
    // tenant pool can't connect to a not-yet-existing DB), then `ensured_pool` applies the issues
    // schema once per slug — the same self-healing path the read/write routes take.
    if let Some(dolt) = dolt {
        let server = dolt.server_pool();
        gt_store_dolt::create_workspace_dolt(&server, slug).await?;
        dolt.ensured_pool(slug).await?;
    }
    Ok(())
}

/// The composition-tier [`gt_workspace::TenantProvisioner`] backing `POST /api/v1/workspace`:
/// provisions a new tenant's PG schema/RBAC + Dolt exactly as the MCP `workspace.create` tool
/// does, over the same shared pools (`hq-gap-workspace-rest-create-provision`).
pub struct CompositionTenantProvisioner {
    pool: PgPool,
    dolt: Option<Arc<WorkspacePools>>,
}

impl CompositionTenantProvisioner {
    /// Wrap the shared PG pool + (optional) per-workspace Dolt pools.
    pub fn new(pool: PgPool, dolt: Option<Arc<WorkspacePools>>) -> Self {
        Self { pool, dolt }
    }
}

#[async_trait]
impl gt_workspace::TenantProvisioner for CompositionTenantProvisioner {
    async fn provision(
        &self,
        slug: &str,
        actor: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        provision_tenant(&self.pool, self.dolt.as_ref(), slug, actor)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })
    }
}

/// Seed the freshly-cloned `ws_<slug>` schema's RBAC so the creator can use the
/// workspace immediately (hq-gap-workspace-provision-full).
///
/// `gt_create_workspace_schema` clones the `ws_default` template STRUCTURE only,
/// so the new `roles`/`users` tables are empty — `/auth/switch` into the tenant
/// would expand no scopes (no usable token). This seeds the minimum that makes a
/// switch succeed:
/// - an `admin` role granting the `*` wildcard in `ws_<slug>.roles`;
/// - the creating user (mirrored from `public.users`) into `ws_<slug>.users`,
///   holding that `admin` role, so the per-tenant login store agrees with the
///   global identity;
/// - a `(user_id, slug, 'admin')` row in `public.user_workspaces` so the creator
///   is a member — what `/auth/switch` resolves before expanding the role.
///
/// All writes are idempotent (`ON CONFLICT DO NOTHING`), so a re-create / retry
/// is a no-op. The membership + the per-ws user mirror are skipped when the actor
/// is not a known global identity (e.g. the boot/system actor): the role still
/// lands so an operator can attach a membership later. The schema name is derived
/// by [`schema_for`] (the same hyphen→underscore mapping the SQL function uses)
/// and validated as a plain `ws_*` identifier before interpolation, since a
/// schema name cannot be a bound parameter.
async fn seed_workspace_rbac(pool: &PgPool, slug: &str, actor: &str) -> Result<(), AppError> {
    let schema = schema_for(slug);
    if !is_safe_ws_schema(&schema) {
        return Err(AppError::Other(format!(
            "refusing to seed unsafe schema ident {schema:?}"
        )));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // The `admin` role granting the wildcard, mirroring the boot admin-seed shape
    // (ws_default.roles). Identifier interpolated (validated above); values bound.
    sqlx::query(&format!(
        "INSERT INTO {schema}.roles (name, scopes, created_at, updated_at) \
         VALUES ('admin', ARRAY['*'], $1, $1) ON CONFLICT (name) DO NOTHING"
    ))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|e| AppError::Other(format!("seed {schema}.roles admin: {e}")))?;

    // Mirror the creating user's global identity into the tenant's per-ws login
    // store, holding the `admin` role. Only when the actor is a known global
    // identity — the boot/system actor has no `public.users` row, and the empty
    // role assignment / absent membership is harmless (operator attaches later).
    let global: Option<(String, String)> = sqlx::query_as(
        "SELECT id, password_hash FROM public.users WHERE id = $1",
    )
    .bind(actor)
    .fetch_optional(pool)
    .await
    .map_err(|e| AppError::Other(format!("lookup global identity {actor}: {e}")))?;

    if let Some((user_id, password_hash)) = global {
        sqlx::query(&format!(
            "INSERT INTO {schema}.users \
                (id, email, password_hash, scopes, roles, created_at, updated_at) \
             SELECT id, email, $2, ARRAY[]::text[], ARRAY['admin'], $3, $3 \
             FROM public.users WHERE id = $1 \
             ON CONFLICT (id) DO NOTHING"
        ))
        .bind(&user_id)
        .bind(&password_hash)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|e| AppError::Other(format!("seed {schema}.users creator: {e}")))?;

        // Membership: the creator holds `admin` in this workspace, so `/auth/switch`
        // resolves it and expands `admin` → `[*]`. FK-safe: user_id exists in
        // public.users (we just read it).
        sqlx::query(
            "INSERT INTO public.user_workspaces (user_id, workspace_slug, role, created_at) \
             VALUES ($1, $2, 'admin', $3) ON CONFLICT (user_id, workspace_slug) DO NOTHING",
        )
        .bind(&user_id)
        .bind(slug)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|e| AppError::Other(format!("seed membership {user_id}@{slug}: {e}")))?;
    }
    Ok(())
}

/// True when `s` is a plain `ws_<a-z0-9_>+` schema identifier — the only shape
/// [`schema_for`] yields — so it is safe to interpolate into DDL (a schema name
/// cannot be a bound parameter). Mirrors the server's boot-time guard.
fn is_safe_ws_schema(s: &str) -> bool {
    s.strip_prefix("ws_")
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
}

/// Pull a required string argument, rejecting a missing/non-string value as a
/// validation fault (not a 500).
fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, AppError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation(format!("missing string argument `{key}`")))
}

/// Parse a workspace slug, surfacing a malformed id as a validation fault.
fn workspace_id(raw: &str) -> Result<WorkspaceId, AppError> {
    WorkspaceId::new(raw).map_err(|e| AppError::Validation(e.to_string()))
}

/// The snake_case spelling of a status, matching the serde representation.
fn status_str(status: WorkspaceStatus) -> &'static str {
    match status {
        WorkspaceStatus::Active => "active",
        WorkspaceStatus::Suspended => "suspended",
        WorkspaceStatus::Archived => "archived",
    }
}

/// Shape one catalog entry as the dispatch payload.
fn entry_json(entry: &WorkspaceEntry) -> Value {
    json!({
        "id": entry.id.as_str(),
        "name": entry.name,
        "status": status_str(entry.status),
    })
}

/// Map an actor failure onto the MCP error space: a missing target is a
/// not-found, any other rejection is a caller-side validation fault, and an
/// apply/persist failure is internal.
fn actor_err(e: ActorError) -> AppError {
    match e {
        ActorError::Rejected(WorkspaceError::NotFound(id)) => {
            AppError::NotFound(format!("workspace {id}"))
        }
        ActorError::Rejected(rejected) => AppError::Validation(rejected.to_string()),
        other => AppError::Other(other.to_string()),
    }
}

/// Map a repository failure onto an internal error (a backend/consistency fault,
/// not a caller fault).
fn repo_err(e: gt_workspace::RepoError) -> AppError {
    AppError::Other(e.to_string())
}

/// Map a `gt-auth` failure from the membership tools onto the MCP error space: an invalid slug is
/// a caller-side validation fault, any other is an internal backend fault.
fn auth_err(e: gt_auth::AuthError) -> AppError {
    match e {
        gt_auth::AuthError::Backend(msg) if msg.starts_with("invalid workspace slug") => {
            AppError::Validation(msg)
        }
        other => AppError::Other(other.to_string()),
    }
}

/// Now, in whole seconds since the Unix epoch — the timestamp the membership writes stamp.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Map a `gt-workspace` lifecycle status onto the orchestration-tier
/// [`GateStatus`] the server speaks (it owns no platform-domain type).
fn gate_status(status: WorkspaceStatus) -> GateStatus {
    match status {
        WorkspaceStatus::Active => GateStatus::Active,
        WorkspaceStatus::Suspended => GateStatus::Suspended,
        WorkspaceStatus::Archived => GateStatus::Archived,
    }
}

/// How long a looked-up status is trusted before it is re-read from Postgres. Keeps
/// the suspend/archive gate off the per-call hot path (one PG read per tenant per
/// window, not per request) at the cost of a bounded lag: a just-suspended tenant
/// may still mutate for up to this long. A few seconds is the right trade — fast
/// enough that "suspend" feels immediate, slow enough that a burst of calls is one
/// read.
const STATUS_TTL: Duration = Duration::from_secs(5);

/// PG-backed [`WorkspaceStatusGate`] for the suspend/archive enforcement
/// (hq-mt-bootstrap.8). Reads the workspace catalog status from the shared `public`
/// pool — the same catalog [`WorkspaceHandler`] mutates — behind a small TTL cache
/// so a mutating call does not pay a Postgres round trip every time.
pub struct PgWorkspaceStatus {
    pool: PgPool,
    /// Per-slug cached status + when it was read. `None` caches a known-absent slug
    /// too, so a flood of calls for an unknown tenant is one read, not many.
    cache: RwLock<HashMap<String, (Option<GateStatus>, Instant)>>,
}

impl PgWorkspaceStatus {
    /// Wrap the shared catalog pool (the `public`-schema `workspaces` table).
    pub fn new(pool: PgPool) -> Self {
        Self { pool, cache: RwLock::new(HashMap::new()) }
    }

    /// Read a fresh (within [`STATUS_TTL`]) cached entry for `ws`, if any.
    async fn cached(&self, ws: &str) -> Option<Option<GateStatus>> {
        let cache = self.cache.read().await;
        cache
            .get(ws)
            .filter(|(_, at)| at.elapsed() < STATUS_TTL)
            .map(|(s, _)| *s)
    }
}

#[async_trait]
impl WorkspaceStatusGate for PgWorkspaceStatus {
    async fn status(&self, ws: &str) -> Result<Option<GateStatus>, AppError> {
        if let Some(hit) = self.cached(ws).await {
            return Ok(hit);
        }
        // A malformed slug is not this gate's to reject — it passes here (as unknown)
        // and the server's `resolve_store` surfaces the real validation error.
        let Ok(id) = WorkspaceId::new(ws) else {
            return Ok(None);
        };
        let status = PgWorkspaces::new(self.pool.clone())
            .load(&id)
            .await
            .map_err(repo_err)?
            .map(|entry| gate_status(entry.status));
        self.cache.write().await.insert(ws.to_string(), (status, Instant::now()));
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `workspace.create` rejects a payload without an `id`.
    #[tokio::test]
    async fn create_requires_id() {
        // A lazy pool URL parses without connecting; the missing-arg check fires
        // before any I/O, so no Postgres is needed.
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx {
            workspace: None,
            actor: "tester",
            args: json!({ "name": "Acme" }),
        };
        let err = handler.dispatch("workspace.create", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// `workspace.suspend` rejects a payload without an `id` before any I/O.
    #[tokio::test]
    async fn suspend_requires_id() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx { workspace: None, actor: "tester", args: json!({}) };
        let err = handler.dispatch("workspace.suspend", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// `workspace.resume` rejects a payload without an `id` before any I/O.
    #[tokio::test]
    async fn resume_requires_id() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx { workspace: None, actor: "tester", args: json!({}) };
        let err = handler.dispatch("workspace.resume", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// `workspace.archive` rejects a payload without an `id` before any I/O.
    #[tokio::test]
    async fn archive_requires_id() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx { workspace: None, actor: "tester", args: json!({}) };
        let err = handler.dispatch("workspace.archive", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// An unknown verb in the namespace is a validation fault, not a panic.
    #[tokio::test]
    async fn unknown_verb_is_validation() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let handler = WorkspaceHandler::new(pool);
        let ctx = DomainCtx {
            workspace: None,
            actor: "tester",
            args: json!({}),
        };
        let err = handler.dispatch("workspace.frobnicate", ctx).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset
    /// so the suite is a no-op off a developer box / CI without PG.
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(PgPool::connect(&url).await.expect("GT_PG_URL must point at a reachable Postgres"))
    }

    /// Seed the public-schema catalog the way the server's `apply_pg_catalog` does:
    /// the `workspaces` table (migration 0), the `gt_create_workspace_schema`
    /// provisioning function (migration 1), and the `ws_default` template the
    /// function clones from. `workspace.create` now calls that function, so a
    /// contract test that only created the table would fail at the clone step.
    /// All idempotent (`raw_sql` simple-query protocol for the multi-statement
    /// migrations), so every test can call this on entry.
    async fn apply_workspace_migrations(pool: &PgPool) {
        for m in gt_store_pg::workspace_migrations() {
            sqlx::raw_sql(&m.sql).execute(pool).await.expect("apply workspaces migration");
        }
        sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS ws_default")
            .execute(pool)
            .await
            .expect("ensure ws_default template");
    }

    /// Seed the `ws_default` template's RBAC tables (the `roles`/`users` shapes the
    /// boot auth migrations define) plus the global-identity tables (`public.users`,
    /// `public.user_workspaces`), so a `gt_create_workspace_schema` clone carries the
    /// `roles`/`users` structure and `seed_workspace_rbac` has somewhere to land. All
    /// idempotent — mirrors the gt-auth migrations without depending on that crate's
    /// migration runner here.
    async fn apply_rbac_template(pool: &PgPool) {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS ws_default.roles ( \
                name TEXT PRIMARY KEY, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL ); \
             CREATE TABLE IF NOT EXISTS ws_default.users ( \
                id TEXT PRIMARY KEY, \
                email TEXT NOT NULL UNIQUE, \
                password_hash TEXT NOT NULL, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                roles TEXT[] NOT NULL DEFAULT '{}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL ); \
             CREATE TABLE IF NOT EXISTS public.users ( \
                id TEXT PRIMARY KEY, \
                email TEXT NOT NULL UNIQUE, \
                password_hash TEXT NOT NULL, \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL ); \
             CREATE TABLE IF NOT EXISTS public.user_workspaces ( \
                user_id TEXT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE, \
                workspace_slug TEXT NOT NULL, \
                role TEXT NOT NULL DEFAULT '', \
                created_at BIGINT NOT NULL, \
                UNIQUE (user_id, workspace_slug) );",
        )
        .execute(pool)
        .await
        .expect("seed rbac template + global identity tables");
    }

    /// PG-backed round trip: `workspace.create` persists through the actor, then
    /// `workspace.info` and `workspace.list` read it back from Postgres — proving
    /// the handler shares durable state across calls (the bead's whole point).
    #[tokio::test]
    async fn create_then_read_back_round_trip() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler contract test");
            return;
        };
        // The `workspaces` table is the one the gt-store-pg migration creates.
        // `raw_sql` uses the simple-query protocol so the multi-statement migration
        // (CREATE TABLE + seed INSERT) runs in one call.
        apply_workspace_migrations(&pool).await;
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("dispatch-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        let created = handler
            .dispatch("workspace.create", ctx(json!({ "id": "dispatch-test", "name": "Dispatch" })))
            .await
            .unwrap();
        assert_eq!(created["status"], "active");
        assert_eq!(created["id"], "dispatch-test");

        // create must also provision the tenant's PG schema (`ws_dispatch_test`),
        // not just the catalog row — the gap that left `rig.*` failing with
        // `relation "rigs" does not exist` for freshly-created workspaces.
        let schema_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind("ws_dispatch_test")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(schema_exists, "workspace.create must clone ws_default into ws_dispatch_test");

        // Re-create is rejected (the actor decides AlreadyExists) as a validation fault.
        let dup = handler
            .dispatch("workspace.create", ctx(json!({ "id": "dispatch-test", "name": "X" })))
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        // info reads it back from Postgres.
        let info = handler
            .dispatch("workspace.info", ctx(json!({ "id": "dispatch-test" })))
            .await
            .unwrap();
        assert_eq!(info["name"], "Dispatch");

        // list includes it.
        let list = handler.dispatch("workspace.list", ctx(json!({}))).await.unwrap();
        let ids: Vec<&str> = list["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"dispatch-test"));

        // info on an absent id is a not-found.
        let missing = handler
            .dispatch("workspace.info", ctx(json!({ "id": "no-such-ws" })))
            .await
            .unwrap_err();
        assert!(matches!(missing, AppError::NotFound(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("dispatch-test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql("DROP SCHEMA IF EXISTS ws_dispatch_test CASCADE")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// PG-backed: `workspace.suspend` transitions Active → Suspended and persists it;
    /// a second suspend is an illegal transition (validation fault).
    #[tokio::test]
    async fn suspend_transitions_active_then_rejects_resuspend() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler suspend test");
            return;
        };
        apply_workspace_migrations(&pool).await;
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("suspend-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        handler
            .dispatch("workspace.create", ctx(json!({ "id": "suspend-test", "name": "S" })))
            .await
            .unwrap();

        let suspended = handler
            .dispatch("workspace.suspend", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap();
        assert_eq!(suspended["status"], "suspended");

        // Persisted: info reads back the suspended status.
        let info = handler
            .dispatch("workspace.info", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap();
        assert_eq!(info["status"], "suspended");

        // Re-suspend is an illegal transition (Suspended is not Active) → validation.
        let again = handler
            .dispatch("workspace.suspend", ctx(json!({ "id": "suspend-test" })))
            .await
            .unwrap_err();
        assert!(matches!(again, AppError::Validation(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("suspend-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// PG-backed: `workspace.resume` transitions Suspended → Active and persists it;
    /// resuming an already-active workspace is an illegal transition (validation fault).
    #[tokio::test]
    async fn resume_transitions_suspended_back_to_active() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler resume test");
            return;
        };
        apply_workspace_migrations(&pool).await;
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("resume-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        handler
            .dispatch("workspace.create", ctx(json!({ "id": "resume-test", "name": "R" })))
            .await
            .unwrap();
        // Resuming an active workspace is illegal (nothing to restore).
        let illegal = handler
            .dispatch("workspace.resume", ctx(json!({ "id": "resume-test" })))
            .await
            .unwrap_err();
        assert!(matches!(illegal, AppError::Validation(_)));

        handler.dispatch("workspace.suspend", ctx(json!({ "id": "resume-test" }))).await.unwrap();
        let resumed = handler
            .dispatch("workspace.resume", ctx(json!({ "id": "resume-test" })))
            .await
            .unwrap();
        assert_eq!(resumed["status"], "active");

        // Persisted: info reads back active.
        let info = handler.dispatch("workspace.info", ctx(json!({ "id": "resume-test" }))).await.unwrap();
        assert_eq!(info["status"], "active");

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("resume-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// PG-backed: `workspace.archive` transitions Suspended → Archived (terminal) and
    /// persists it; a second archive is an illegal transition (validation fault).
    #[tokio::test]
    async fn archive_is_terminal_and_rejects_rearchive() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping WorkspaceHandler archive test");
            return;
        };
        apply_workspace_migrations(&pool).await;
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("archive-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };

        handler
            .dispatch("workspace.create", ctx(json!({ "id": "archive-test", "name": "A" })))
            .await
            .unwrap();
        // Archive straight from Active is allowed too, but go via Suspended to prove both.
        handler.dispatch("workspace.suspend", ctx(json!({ "id": "archive-test" }))).await.unwrap();

        let archived = handler
            .dispatch("workspace.archive", ctx(json!({ "id": "archive-test" })))
            .await
            .unwrap();
        assert_eq!(archived["status"], "archived");

        // Persisted.
        let info = handler.dispatch("workspace.info", ctx(json!({ "id": "archive-test" }))).await.unwrap();
        assert_eq!(info["status"], "archived");

        // Re-archive rejected (Archived is terminal) → validation.
        let again = handler
            .dispatch("workspace.archive", ctx(json!({ "id": "archive-test" })))
            .await
            .unwrap_err();
        assert!(matches!(again, AppError::Validation(_)));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("archive-test")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[test]
    fn status_strings_match_serde() {
        for s in [
            WorkspaceStatus::Active,
            WorkspaceStatus::Suspended,
            WorkspaceStatus::Archived,
        ] {
            let serde = serde_json::to_value(s).unwrap();
            assert_eq!(serde, json!(status_str(s)));
        }
    }

    #[test]
    fn safe_ws_schema_accepts_ws_idents_and_rejects_the_rest() {
        assert!(is_safe_ws_schema("ws_acme"));
        assert!(is_safe_ws_schema("ws_team_1"));
        assert!(is_safe_ws_schema(&schema_for("team-1")));
        // Rejected: missing prefix, empty tail, uppercase, separators, injection.
        assert!(!is_safe_ws_schema("ws_"));
        assert!(!is_safe_ws_schema("public"));
        assert!(!is_safe_ws_schema("ws_Acme"));
        assert!(!is_safe_ws_schema("ws_a-b"));
        assert!(!is_safe_ws_schema("ws_a; DROP"));
    }

    /// PG-backed: `workspace.create` makes a freshly-created tenant usable end to
    /// end (hq-gap-workspace-provision-full). After create, the new `ws_<slug>`
    /// schema holds an `admin` role granting `[*]`, the creating user mirrored from
    /// `public.users` with that role, and a `public.user_workspaces` membership —
    /// exactly what `/auth/switch` resolves before expanding the role. Re-create is
    /// a no-op (idempotent seed), not a duplicate-key error.
    #[tokio::test]
    async fn create_provisions_usable_tenant_rbac_and_membership() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping provision-full contract test");
            return;
        };
        apply_workspace_migrations(&pool).await;
        apply_rbac_template(&pool).await;

        // A clean slate for this slug.
        let now = 1_700_000_000i64;
        sqlx::query("DELETE FROM workspaces WHERE id = $1").bind("prov-test").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM public.user_workspaces WHERE workspace_slug = $1")
            .bind("prov-test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql("DROP SCHEMA IF EXISTS ws_prov_test CASCADE").execute(&pool).await.unwrap();
        // The creating user must be a known global identity (membership FK).
        sqlx::query(
            "INSERT INTO public.users (id, email, password_hash, created_at, updated_at) \
             VALUES ('user-prov', 'prov@gt.local', 'argon2-hash', $1, $1) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "user-prov", args };

        let created = handler
            .dispatch("workspace.create", ctx(json!({ "id": "prov-test", "name": "Prov" })))
            .await
            .unwrap();
        assert_eq!(created["status"], "active");

        // The admin role exists in the tenant schema, granting the wildcard.
        let role_scopes: Vec<String> =
            sqlx::query_scalar("SELECT scopes FROM ws_prov_test.roles WHERE name = 'admin'")
                .fetch_one(&pool)
                .await
                .expect("admin role seeded into ws_prov_test.roles");
        assert_eq!(role_scopes, vec!["*".to_string()]);

        // The creator is mirrored into the tenant's users, holding `admin`.
        let (uid, uroles): (String, Vec<String>) =
            sqlx::query_as("SELECT id, roles FROM ws_prov_test.users WHERE id = 'user-prov'")
                .fetch_one(&pool)
                .await
                .expect("creator seeded into ws_prov_test.users");
        assert_eq!(uid, "user-prov");
        assert_eq!(uroles, vec!["admin".to_string()]);

        // The membership ties the creator to the workspace with the admin role.
        let mrole: String = sqlx::query_scalar(
            "SELECT role FROM public.user_workspaces WHERE user_id = 'user-prov' AND workspace_slug = 'prov-test'",
        )
        .fetch_one(&pool)
        .await
        .expect("membership row seeded");
        assert_eq!(mrole, "admin");

        // Idempotent: deleting the catalog row and re-creating re-runs the seed as a
        // no-op (ON CONFLICT DO NOTHING), never a duplicate-key error.
        sqlx::query("DELETE FROM workspaces WHERE id = $1").bind("prov-test").execute(&pool).await.unwrap();
        handler
            .dispatch("workspace.create", ctx(json!({ "id": "prov-test", "name": "Prov" })))
            .await
            .expect("re-create with already-seeded RBAC is idempotent");

        // Cleanup.
        sqlx::query("DELETE FROM public.user_workspaces WHERE workspace_slug = $1")
            .bind("prov-test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM workspaces WHERE id = $1").bind("prov-test").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM public.users WHERE id = 'user-prov'").execute(&pool).await.unwrap();
        sqlx::raw_sql("DROP SCHEMA IF EXISTS ws_prov_test CASCADE").execute(&pool).await.unwrap();
    }

    /// PG-backed: the `workspace.provider-*` system-admin tools (hq-idp-db.4) round-trip through
    /// `public.oauth_providers` — create (preset endpoints filled), list/get show NO secret, patch
    /// `enabled`, delete. The scope gate (system admin only) is enforced one tier up at the MCP
    /// server boundary; this proves the handler + projection. An unknown kind is a validation fault.
    #[cfg(feature = "oauth")]
    #[tokio::test]
    async fn provider_tools_round_trip_without_leaking_the_secret() {
        std::env::set_var(
            "GT_SECRET_KEY",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping provider tools contract test");
            return;
        };
        // Provision the GLOBAL provider table (the same DDL the boot migration applies).
        sqlx::raw_sql(gt_auth::migrations::CREATE_OAUTH_PROVIDERS)
            .execute(&pool)
            .await
            .expect("create public.oauth_providers");
        let id = format!("prov-tool-{}", now_secs());
        sqlx::query("DELETE FROM public.oauth_providers WHERE id = $1")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "sysadmin", args };

        // Create a Google preset: only client credentials supplied, endpoints baked.
        let created = handler
            .dispatch(
                "workspace.provider-create",
                ctx(json!({ "id": id, "kind": "google", "client_id": "cid", "client_secret": "top-secret-xyz" })),
            )
            .await
            .unwrap();
        assert_eq!(created["id"], id);
        assert_eq!(created["token_endpoint"], "https://oauth2.googleapis.com/token");
        let created_str = created.to_string();
        assert!(!created_str.contains("client_secret"), "create echo names no secret: {created_str}");
        assert!(!created_str.contains("top-secret-xyz"), "create echo carries no secret: {created_str}");

        // List shows it, still no secret.
        let list = handler.dispatch("workspace.provider-list", ctx(json!({}))).await.unwrap();
        let list_str = list.to_string();
        assert!(list_str.contains(&id), "list includes the provider: {list_str}");
        assert!(!list_str.contains("client_secret"), "list names no secret");
        assert!(!list_str.contains("top-secret-xyz"), "list carries no secret");

        // Patch `enabled` (no secret re-supplied).
        let patched = handler
            .dispatch("workspace.provider-update", ctx(json!({ "id": id, "enabled": false })))
            .await
            .unwrap();
        assert_eq!(patched["enabled"], false);
        assert!(!patched.to_string().contains("top-secret-xyz"));

        // Unknown kind is a validation fault, not a 500.
        let bad = handler
            .dispatch(
                "workspace.provider-create",
                ctx(json!({ "id": "x", "kind": "nope", "client_id": "c", "client_secret": "s" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(bad, AppError::Validation(_)));

        // Delete: present → ok; absent → not found.
        let del = handler
            .dispatch("workspace.provider-delete", ctx(json!({ "id": id })))
            .await
            .unwrap();
        assert_eq!(del["ok"], true);
        let gone = handler
            .dispatch("workspace.provider-delete", ctx(json!({ "id": id })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
    }

    #[test]
    fn gate_status_maps_every_variant() {
        assert_eq!(gate_status(WorkspaceStatus::Active), GateStatus::Active);
        assert_eq!(gate_status(WorkspaceStatus::Suspended), GateStatus::Suspended);
        assert_eq!(gate_status(WorkspaceStatus::Archived), GateStatus::Archived);
    }

    /// An unknown slug is `None` (passes the gate); a malformed slug is also `None`,
    /// offline — no Postgres needed, since both return before any I/O via the cache
    /// miss + a lazy pool that is never hit for the malformed case.
    #[tokio::test]
    async fn status_unknown_and_malformed_are_none() {
        let pool = PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").unwrap();
        let gate = PgWorkspaceStatus::new(pool);
        // Malformed slug rejected by WorkspaceId::new ⇒ None, no I/O attempted.
        assert_eq!(gate.status("Not A Slug!").await.unwrap(), None);
    }

    /// PG-backed: the gate reflects the catalog status — Active right after create,
    /// then Suspended once the actor transitions it (read by a fresh gate so the TTL
    /// cache does not mask the change).
    #[tokio::test]
    async fn pg_status_gate_reflects_lifecycle() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgWorkspaceStatus contract test");
            return;
        };
        apply_workspace_migrations(&pool).await;
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("gate-test")
            .execute(&pool)
            .await
            .unwrap();

        let handler = WorkspaceHandler::new(pool.clone());
        let ctx = |args| DomainCtx { workspace: None, actor: "tester", args };
        handler
            .dispatch("workspace.create", ctx(json!({ "id": "gate-test", "name": "G" })))
            .await
            .unwrap();

        // Active right after create; an unknown slug is None.
        let gate = PgWorkspaceStatus::new(pool.clone());
        assert_eq!(gate.status("gate-test").await.unwrap(), Some(GateStatus::Active));
        assert_eq!(gate.status("no-such-ws").await.unwrap(), None);

        // Suspend it, then read with a *fresh* gate (the first gate cached Active).
        handler.dispatch("workspace.suspend", ctx(json!({ "id": "gate-test" }))).await.unwrap();
        let fresh = PgWorkspaceStatus::new(pool.clone());
        assert_eq!(fresh.status("gate-test").await.unwrap(), Some(GateStatus::Suspended));
        // The stale gate still serves its cached Active within the TTL — the
        // documented bounded lag, proving the cache is in force.
        assert_eq!(gate.status("gate-test").await.unwrap(), Some(GateStatus::Active));

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind("gate-test")
            .execute(&pool)
            .await
            .unwrap();
    }
}
