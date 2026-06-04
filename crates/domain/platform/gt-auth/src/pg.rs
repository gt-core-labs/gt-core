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

        let row = sqlx::query("SELECT id, password_hash, scopes FROM users WHERE email = $1")
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

        row_to_identity(&row, &self.workspace)
    }
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
    })
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
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )",
        )
        .execute(pool)
        .await
        .expect("create users table");
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
        assert_eq!(got, Err(AuthError::UnsupportedProvider(ProviderKind::OAuth)));
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
}
