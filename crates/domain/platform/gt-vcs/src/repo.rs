//! The VCS-connection model + store (hq-vcs-connections.1).
//!
//! A VCS connection is how a workspace lets the server reach its repos: either a GitHub App
//! installation (the server mints ephemeral installation tokens JIT — only the `installation_id`
//! is stored, never a long-lived credential) or a Personal Access Token (the fallback — sealed at
//! rest). It MIRRORS the OAuth provider store ([`gt_auth::ProviderRepo`]): a single GLOBAL
//! `public.vcs_connections` table with an optional `workspace_id`, the PAT secret AES-GCM-sealed
//! with `GT_SECRET_KEY` by REUSING [`gt_auth::seal`] / [`gt_auth::unseal`] — never a new cipher.
//!
//! This module owns:
//! - [`ConnectionKind`] — the variant (`github_app` vs `pat`).
//! - [`ConnectionStatus`] — the lifecycle gate (`active` / `disabled` / `revoked`).
//! - [`VcsConnection`] / [`NewConnection`] / [`PatchConnection`] — a row read back / created / patched.
//! - [`VcsConnectionRepo`] — the async CRUD port.
//! - [`PgVcsConnections`] — the Postgres adapter (gated by `pg`), sealing the PAT on write.

use async_trait::async_trait;
use gt_events::AppError;

/// The variant of a stored VCS connection.
///
/// [`GithubApp`](Self::GithubApp) stores NO secret — the server mints an installation token JIT
/// from the GitHub App's private key + the `installation_id`. [`Pat`](Self::Pat) is the fallback:
/// a Personal Access Token sealed at rest. The wire spelling (the TEXT in `vcs_connections.kind`)
/// is the lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionKind {
    /// A GitHub App installation — ephemeral JIT tokens, no stored credential.
    GithubApp,
    /// A Personal Access Token fallback — the token is sealed at rest.
    Pat,
}

impl ConnectionKind {
    /// The wire (column) spelling stored in `vcs_connections.kind`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionKind::GithubApp => "github_app",
            ConnectionKind::Pat => "pat",
        }
    }

    /// Parse the wire spelling back to a kind; an unknown value is [`AppError::Validation`] when it
    /// comes off the wire, but a corrupt stored row surfaces as [`AppError::Other`] via
    /// [`parse_stored`](Self::parse_stored).
    pub fn parse(s: &str) -> Result<Self, AppError> {
        match s {
            "github_app" => Ok(ConnectionKind::GithubApp),
            "pat" => Ok(ConnectionKind::Pat),
            other => Err(AppError::Validation(format!(
                "unknown connection kind: {other} (expected github_app|pat)"
            ))),
        }
    }

    /// Parse a kind read back from the database; an unknown value is a corrupt/forward-incompatible
    /// row ([`AppError::Other`]), never a client validation error.
    fn parse_stored(s: &str) -> Result<Self, AppError> {
        Self::parse(s).map_err(|_| AppError::Other(format!("corrupt vcs_connections.kind: {s}")))
    }
}

/// The lifecycle state of a connection. `active` is usable; `disabled` is an admin pause;
/// `revoked` records that the upstream credential was withdrawn (a connection is never deleted out
/// from under in-flight references — it is marked instead).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Usable for cloning.
    Active,
    /// Paused by an admin; not used until re-enabled.
    Disabled,
    /// The upstream credential was withdrawn.
    Revoked,
}

impl ConnectionStatus {
    /// The wire (column) spelling stored in `vcs_connections.status`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionStatus::Active => "active",
            ConnectionStatus::Disabled => "disabled",
            ConnectionStatus::Revoked => "revoked",
        }
    }

    /// Parse the wire spelling; an unknown value off the wire is [`AppError::Validation`].
    pub fn parse(s: &str) -> Result<Self, AppError> {
        match s {
            "active" => Ok(ConnectionStatus::Active),
            "disabled" => Ok(ConnectionStatus::Disabled),
            "revoked" => Ok(ConnectionStatus::Revoked),
            other => Err(AppError::Validation(format!(
                "unknown connection status: {other} (expected active|disabled|revoked)"
            ))),
        }
    }

    /// Parse a status read back from the database; an unknown value is a corrupt row.
    fn parse_stored(s: &str) -> Result<Self, AppError> {
        Self::parse(s).map_err(|_| AppError::Other(format!("corrupt vcs_connections.status: {s}")))
    }
}

/// A connection to create. The `secret` (a PAT) is cleartext here — the repo SEALS it
/// ([`gt_auth::seal`]) on write, so it never reaches the database in clear. A `github_app`
/// connection MUST NOT carry a secret (the store enforces it); a `pat` connection MUST.
#[derive(Clone, Debug)]
pub struct NewConnection {
    /// The stable id / primary key.
    pub id: String,
    /// The owning workspace (`None` = a global connection visible to every workspace).
    pub workspace_id: Option<String>,
    /// The connection variant.
    pub kind: ConnectionKind,
    /// The GitHub App installation id (`Some` for `github_app`, `None` for `pat`).
    pub installation_id: Option<String>,
    /// The GitHub account/org login the installation lives under (`github_app` only).
    pub account_login: Option<String>,
    /// The Personal Access Token in cleartext — sealed by the repo before storage. `Some` ONLY for
    /// `kind = Pat`; a `github_app` connection leaves this `None`.
    pub secret: Option<String>,
    /// The initial lifecycle state.
    pub status: ConnectionStatus,
}

/// A partial update to an existing connection. Every field is optional: `None` leaves the stored
/// column untouched, `Some(_)` overwrites it. A `Some` secret is RE-SEALED; a `None` secret leaves
/// the stored sealed blob in place (so a metadata edit, e.g. disabling, keeps the token).
#[derive(Clone, Debug, Default)]
pub struct PatchConnection {
    /// New owning workspace: `Some(None)` clears to global, `Some(Some(slug))` scopes it, `None`
    /// leaves it.
    pub workspace_id: Option<Option<String>>,
    /// New installation id, or `None` to leave it.
    pub installation_id: Option<Option<String>>,
    /// New account login, or `None` to leave it.
    pub account_login: Option<Option<String>>,
    /// New PAT in cleartext (re-sealed), `Some(None)` clears the secret, `None` keeps the stored
    /// blob.
    pub secret: Option<Option<String>>,
    /// New lifecycle state, or `None` to leave it.
    pub status: Option<ConnectionStatus>,
}

/// A connection read back from the store. `secret_sealed` is the SEALED blob — the cleartext PAT is
/// recovered only in memory via [`unseal_secret`](Self::unseal_secret). The HTTP read projection
/// NEVER carries it (see `http::ConnectionView`).
#[derive(Clone, Debug)]
pub struct VcsConnection {
    /// The id / primary key.
    pub id: String,
    /// The owning workspace (`None` = global).
    pub workspace_id: Option<String>,
    /// The connection variant.
    pub kind: ConnectionKind,
    /// The GitHub App installation id.
    pub installation_id: Option<String>,
    /// The GitHub account/org login.
    pub account_login: Option<String>,
    /// The SEALED PAT (`nonce || ciphertext+tag`, [`gt_auth::seal`]), or `None` for a `github_app`
    /// connection. Never the cleartext.
    pub secret_sealed: Option<Vec<u8>>,
    /// The lifecycle state.
    pub status: ConnectionStatus,
    /// Creation time (epoch seconds).
    pub created_at: i64,
}

impl VcsConnection {
    /// Whether this connection stores a sealed secret (a PAT). `github_app` connections do not.
    pub fn has_secret(&self) -> bool {
        self.secret_sealed.is_some()
    }

    /// Unseal the stored PAT into cleartext, in memory only (`gt_auth::unseal`). `Ok(None)` when the
    /// connection carries no secret (a `github_app`); an unseal failure (wrong/rotated key,
    /// corruption) is [`AppError::Other`]. A leaked blob is useless without `GT_SECRET_KEY`.
    pub fn unseal_secret(&self) -> Result<Option<String>, AppError> {
        match &self.secret_sealed {
            None => Ok(None),
            Some(blob) => {
                let bytes = gt_auth::unseal(blob)
                    .map_err(|e| AppError::Other(format!("vcs_connections unseal: {e}")))?;
                let s = String::from_utf8(bytes)
                    .map_err(|_| AppError::Other("unsealed PAT is not utf-8".into()))?;
                Ok(Some(s))
            }
        }
    }
}

/// Test-only shim so the `http` router contract test can exercise the kind/secret `422` path
/// through an in-memory store without a database. Delegates to the private [`check_kind_secret`].
#[cfg(test)]
pub(crate) fn check_kind_secret_for_test(
    kind: ConnectionKind,
    has_secret: bool,
) -> Result<(), AppError> {
    check_kind_secret(kind, has_secret)
}

/// Validate the kind/secret invariant common to create + patch: a `pat` connection MUST carry a
/// secret, a `github_app` connection MUST NOT. Returns [`AppError::Validation`] on a mismatch so
/// the REST surface maps it to `422`.
fn check_kind_secret(kind: ConnectionKind, has_secret: bool) -> Result<(), AppError> {
    match (kind, has_secret) {
        (ConnectionKind::Pat, false) => Err(AppError::Validation(
            "kind=pat requires a secret (the Personal Access Token)".into(),
        )),
        (ConnectionKind::GithubApp, true) => Err(AppError::Validation(
            "kind=github_app must not carry a secret (tokens are minted JIT)".into(),
        )),
        _ => Ok(()),
    }
}

/// The async CRUD port over the VCS-connection store.
///
/// An abstract port (docs/03 Rule 4) so the REST surface depends on the contract, not the Postgres
/// adapter. [`create`](Self::create) takes a cleartext PAT and is responsible for sealing it; reads
/// return the sealed blob (cleartext is recovered only via [`VcsConnection::unseal_secret`]).
#[async_trait]
pub trait VcsConnectionRepo: Send + Sync {
    /// Connections visible to `workspace`: its own scoped rows plus the global ones
    /// (`workspace_id IS NULL`), ordered by `created_at`. This is the self-auth list a workspace
    /// member sees, mirroring `oauth_providers.list_for_workspace`.
    async fn list_for_workspace(&self, workspace: &str)
        -> Result<Vec<VcsConnection>, AppError>;
    /// One connection by id, scoped to `workspace` (its own or a global row). `None` when no such
    /// id is visible to the workspace — so a caller can never read another tenant's connection by
    /// guessing an id.
    async fn get_for_workspace(
        &self,
        workspace: &str,
        id: &str,
    ) -> Result<Option<VcsConnection>, AppError>;
    /// Register `conn`, sealing its PAT (if any) before storage. The kind/secret invariant is
    /// enforced ([`check_kind_secret`]).
    async fn create(&self, conn: NewConnection) -> Result<VcsConnection, AppError>;
    /// Apply a partial update to connection `id` scoped to `workspace`: each `Some` field
    /// overwrites, each `None` leaves the column. A `Some` secret is RE-SEALED. Returns the updated
    /// record, or `None` if no such id is visible to the workspace.
    async fn patch(
        &self,
        workspace: &str,
        id: &str,
        patch: PatchConnection,
    ) -> Result<Option<VcsConnection>, AppError>;
    /// Remove connection `id` scoped to `workspace`; `true` if a row was deleted, `false` if none
    /// matched (or it belonged to another tenant).
    async fn delete(&self, workspace: &str, id: &str) -> Result<bool, AppError>;
}

#[cfg(feature = "pg")]
pub use pg_impl::PgVcsConnections;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`VcsConnectionRepo`] over the GLOBAL `public.vcs_connections` table.
    ///
    /// Statements are `public`-qualified (the table is cross-tenant, carrying `workspace_id`), the
    /// same shape as [`gt_auth::PgProviderRepo`] over `public.oauth_providers`. The PAT is SEALED on
    /// write and only ever stored/returned as the sealed blob. Cloning is cheap: `PgPool` is an
    /// `Arc`.
    #[derive(Clone, Debug)]
    pub struct PgVcsConnections {
        pool: PgPool,
    }

    impl PgVcsConnections {
        /// Wrap a connection `pool`. `public.vcs_connections` is expected to already exist
        /// (provisioned via `migrations/vcs/0001__create_vcs_connections.sql`, applied at boot).
        pub fn new(pool: PgPool) -> Self {
            PgVcsConnections { pool }
        }
    }

    const COLS: &str =
        "id, workspace_id, kind, installation_id, account_login, secret, status, \
         EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at";

    /// Decode a `public.vcs_connections` row into a [`VcsConnection`]. A column read fault is
    /// [`AppError::Other`] (an outage/corruption, never a denied request).
    fn row_to_record(row: &PgRow) -> Result<VcsConnection, AppError> {
        let kind: String = row
            .try_get("kind")
            .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?;
        let status: String = row
            .try_get("status")
            .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?;
        Ok(VcsConnection {
            id: row
                .try_get("id")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
            workspace_id: row
                .try_get("workspace_id")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
            kind: ConnectionKind::parse_stored(&kind)?,
            installation_id: row
                .try_get("installation_id")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
            account_login: row
                .try_get("account_login")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
            secret_sealed: row
                .try_get("secret")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
            status: ConnectionStatus::parse_stored(&status)?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| AppError::Other(format!("vcs_connections postgres: {e}")))?,
        })
    }

    #[async_trait]
    impl VcsConnectionRepo for PgVcsConnections {
        async fn list_for_workspace(
            &self,
            workspace: &str,
        ) -> Result<Vec<VcsConnection>, AppError> {
            let rows = sqlx::query(&format!(
                "SELECT {COLS} FROM public.vcs_connections \
                 WHERE workspace_id IS NULL OR workspace_id = $1 \
                 ORDER BY created_at"
            ))
            .bind(workspace)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("vcs_connections list: {e}")))?;
            rows.iter().map(row_to_record).collect()
        }

        async fn get_for_workspace(
            &self,
            workspace: &str,
            id: &str,
        ) -> Result<Option<VcsConnection>, AppError> {
            let row = sqlx::query(&format!(
                "SELECT {COLS} FROM public.vcs_connections \
                 WHERE id = $1 AND (workspace_id IS NULL OR workspace_id = $2)"
            ))
            .bind(id)
            .bind(workspace)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("vcs_connections get: {e}")))?;
            row.as_ref().map(row_to_record).transpose()
        }

        async fn create(&self, conn: NewConnection) -> Result<VcsConnection, AppError> {
            check_kind_secret(conn.kind, conn.secret.is_some())?;
            // Seal the cleartext PAT BEFORE it touches the database — it is never stored in clear.
            let sealed = match &conn.secret {
                Some(pat) => Some(
                    gt_auth::seal(pat.as_bytes())
                        .map_err(|e| AppError::Other(format!("vcs_connections seal: {e}")))?,
                ),
                None => None,
            };
            let row = sqlx::query(&format!(
                "INSERT INTO public.vcs_connections \
                 (id, workspace_id, kind, installation_id, account_login, secret, status) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 RETURNING {COLS}"
            ))
            .bind(&conn.id)
            .bind(&conn.workspace_id)
            .bind(conn.kind.as_str())
            .bind(&conn.installation_id)
            .bind(&conn.account_login)
            .bind(&sealed)
            .bind(conn.status.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("vcs_connections create: {e}")))?;
            row_to_record(&row)
        }

        async fn patch(
            &self,
            workspace: &str,
            id: &str,
            patch: PatchConnection,
        ) -> Result<Option<VcsConnection>, AppError> {
            // Read-modify-write within the caller's visibility: load the current row, fold the
            // `Some` fields over it, then write it back. The secret is re-sealed only when the patch
            // carries a new one; a `None` patch leaves the stored sealed blob untouched.
            let Some(current) = self.get_for_workspace(workspace, id).await? else {
                return Ok(None);
            };
            let kind = current.kind; // kind is immutable — a connection never changes variant.
            let secret_sealed = match patch.secret {
                Some(Some(pat)) => Some(
                    gt_auth::seal(pat.as_bytes())
                        .map_err(|e| AppError::Other(format!("vcs_connections seal: {e}")))?,
                ),
                Some(None) => None,
                None => current.secret_sealed.clone(),
            };
            check_kind_secret(kind, secret_sealed.is_some())?;
            let workspace_id = match patch.workspace_id {
                Some(ws) => ws,
                None => current.workspace_id.clone(),
            };
            let installation_id = match patch.installation_id {
                Some(v) => v,
                None => current.installation_id.clone(),
            };
            let account_login = match patch.account_login {
                Some(v) => v,
                None => current.account_login.clone(),
            };
            let status = patch.status.unwrap_or(current.status);
            let row = sqlx::query(&format!(
                "UPDATE public.vcs_connections SET \
                 workspace_id = $2, installation_id = $3, account_login = $4, secret = $5, \
                 status = $6 WHERE id = $1 RETURNING {COLS}"
            ))
            .bind(id)
            .bind(&workspace_id)
            .bind(&installation_id)
            .bind(&account_login)
            .bind(&secret_sealed)
            .bind(status.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("vcs_connections patch: {e}")))?;
            Ok(Some(row_to_record(&row)?))
        }

        async fn delete(&self, workspace: &str, id: &str) -> Result<bool, AppError> {
            let res = sqlx::query(
                "DELETE FROM public.vcs_connections \
                 WHERE id = $1 AND (workspace_id IS NULL OR workspace_id = $2)",
            )
            .bind(id)
            .bind(workspace)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::Other(format!("vcs_connections delete: {e}")))?;
            Ok(res.rows_affected() > 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_wire_round_trips() {
        for k in [ConnectionKind::GithubApp, ConnectionKind::Pat] {
            assert_eq!(ConnectionKind::parse(k.as_str()).unwrap(), k);
        }
        assert!(ConnectionKind::parse("gitlab").is_err());
    }

    #[test]
    fn status_wire_round_trips() {
        for s in [
            ConnectionStatus::Active,
            ConnectionStatus::Disabled,
            ConnectionStatus::Revoked,
        ] {
            assert_eq!(ConnectionStatus::parse(s.as_str()).unwrap(), s);
        }
        assert!(ConnectionStatus::parse("nope").is_err());
    }

    #[test]
    fn pat_requires_a_secret_github_app_forbids_one() {
        // pat without a secret is rejected.
        assert!(check_kind_secret(ConnectionKind::Pat, false).is_err());
        // pat with a secret is fine.
        assert!(check_kind_secret(ConnectionKind::Pat, true).is_ok());
        // github_app with a secret is rejected.
        assert!(check_kind_secret(ConnectionKind::GithubApp, true).is_err());
        // github_app without a secret is fine.
        assert!(check_kind_secret(ConnectionKind::GithubApp, false).is_ok());
    }

    // --- PG-gated contract tests: round-trip against a real `public.vcs_connections` table. ---
    //
    // No-ops when `GT_PG_URL` is unset (same gate as the other `pg` adapters), so a developer box /
    // CI without Postgres still passes. Run with `--test-threads=1` and a `GT_SECRET_KEY` set.
    #[cfg(feature = "pg")]
    mod pg_contract {
        use super::*;
        use sqlx::PgPool;

        async fn pool_or_skip() -> Option<PgPool> {
            let url = std::env::var("GT_PG_URL").ok()?;
            Some(
                PgPool::connect(&url)
                    .await
                    .expect("GT_PG_URL must point at a reachable Postgres"),
            )
        }

        /// Provision `public.vcs_connections` for the test under a transaction-scoped advisory lock
        /// (a `CREATE TABLE IF NOT EXISTS` race between two sessions can throw 23505), the same
        /// guard the OAuth provider contract test uses.
        async fn ensure_table(pool: &PgPool) {
            let mut tx = pool.begin().await.expect("begin ddl tx");
            sqlx::query("SELECT pg_advisory_xact_lock(8423)")
                .execute(&mut *tx)
                .await
                .expect("advisory lock");
            sqlx::query(crate::migrations::CREATE_VCS_CONNECTIONS)
                .execute(&mut *tx)
                .await
                .expect("create vcs_connections table");
            tx.commit().await.expect("commit ddl tx");
        }

        fn unique_id(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("test-vcs-{tag}-{n}")
        }

        #[tokio::test]
        async fn pat_seals_and_round_trips() {
            std::env::set_var(
                gt_auth::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgVcsConnections::new(pool.clone());

            let id = unique_id("pat");
            let cleartext = "ghp_super_secret_token";
            let stored = repo
                .create(NewConnection {
                    id: id.clone(),
                    workspace_id: Some("acme".into()),
                    kind: ConnectionKind::Pat,
                    installation_id: None,
                    account_login: None,
                    secret: Some(cleartext.into()),
                    status: ConnectionStatus::Active,
                })
                .await
                .unwrap();

            // The stored blob is NOT the cleartext.
            let blob = stored.secret_sealed.clone().expect("pat has a sealed secret");
            assert!(!blob.windows(cleartext.len()).any(|w| w == cleartext.as_bytes()));
            // It unseals back to the original PAT.
            assert_eq!(stored.unseal_secret().unwrap().as_deref(), Some(cleartext));

            // The raw BYTEA in PG (read directly, not via the adapter) is not the cleartext.
            let raw: Vec<u8> =
                sqlx::query_scalar("SELECT secret FROM public.vcs_connections WHERE id = $1")
                    .bind(&id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(!raw.windows(cleartext.len()).any(|w| w == cleartext.as_bytes()));

            // Visible to acme; delete removes it.
            assert!(repo
                .list_for_workspace("acme")
                .await
                .unwrap()
                .iter()
                .any(|c| c.id == id));
            assert!(repo.delete("acme", &id).await.unwrap());
            assert!(repo.get_for_workspace("acme", &id).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn github_app_stores_no_secret() {
            std::env::set_var(
                gt_auth::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgVcsConnections::new(pool.clone());

            let id = unique_id("gh");
            let stored = repo
                .create(NewConnection {
                    id: id.clone(),
                    workspace_id: Some("acme".into()),
                    kind: ConnectionKind::GithubApp,
                    installation_id: Some("12345".into()),
                    account_login: Some("codecsrayo".into()),
                    secret: None,
                    status: ConnectionStatus::Active,
                })
                .await
                .unwrap();
            assert!(!stored.has_secret(), "github_app stores no secret");
            assert_eq!(stored.installation_id.as_deref(), Some("12345"));
            assert_eq!(stored.unseal_secret().unwrap(), None);

            // A github_app with a secret is rejected (the kind/secret invariant).
            let bad = repo
                .create(NewConnection {
                    id: unique_id("gh-bad"),
                    workspace_id: Some("acme".into()),
                    kind: ConnectionKind::GithubApp,
                    installation_id: Some("9".into()),
                    account_login: None,
                    secret: Some("nope".into()),
                    status: ConnectionStatus::Active,
                })
                .await;
            assert!(matches!(bad, Err(AppError::Validation(_))));

            repo.delete("acme", &id).await.unwrap();
        }

        #[tokio::test]
        async fn tenant_isolation_and_global_visibility() {
            std::env::set_var(
                gt_auth::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgVcsConnections::new(pool.clone());

            let id_acme = unique_id("iso-acme");
            let id_other = unique_id("iso-other");
            let id_global = unique_id("iso-global");
            let make = |id: &str, ws: Option<&str>| NewConnection {
                id: id.into(),
                workspace_id: ws.map(str::to_owned),
                kind: ConnectionKind::GithubApp,
                installation_id: Some("1".into()),
                account_login: None,
                secret: None,
                status: ConnectionStatus::Active,
            };
            repo.create(make(&id_acme, Some("acme"))).await.unwrap();
            repo.create(make(&id_other, Some("other"))).await.unwrap();
            repo.create(make(&id_global, None)).await.unwrap();

            // acme sees its own + the global, never `other`'s.
            let for_acme: Vec<String> = repo
                .list_for_workspace("acme")
                .await
                .unwrap()
                .into_iter()
                .map(|c| c.id)
                .collect();
            assert!(for_acme.contains(&id_acme));
            assert!(for_acme.contains(&id_global));
            assert!(!for_acme.contains(&id_other));

            // get/delete are tenant-scoped: acme cannot reach `other`'s row by id.
            assert!(repo.get_for_workspace("acme", &id_other).await.unwrap().is_none());
            assert!(!repo.delete("acme", &id_other).await.unwrap());

            repo.delete("acme", &id_acme).await.unwrap();
            repo.delete("other", &id_other).await.unwrap();
            repo.delete("acme", &id_global).await.unwrap();
        }

        #[tokio::test]
        async fn patch_disables_keeping_the_sealed_pat() {
            std::env::set_var(
                gt_auth::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgVcsConnections::new(pool.clone());

            let id = unique_id("patch");
            let original = repo
                .create(NewConnection {
                    id: id.clone(),
                    workspace_id: Some("acme".into()),
                    kind: ConnectionKind::Pat,
                    installation_id: None,
                    account_login: None,
                    secret: Some("ghp_orig".into()),
                    status: ConnectionStatus::Active,
                })
                .await
                .unwrap();
            let original_blob = original.secret_sealed.clone().unwrap();

            // A metadata-only patch (disable) LEAVES the sealed secret intact.
            let disabled = repo
                .patch(
                    "acme",
                    &id,
                    PatchConnection {
                        status: Some(ConnectionStatus::Disabled),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .expect("present");
            assert_eq!(disabled.status, ConnectionStatus::Disabled);
            assert_eq!(disabled.secret_sealed.as_ref().unwrap(), &original_blob);

            // A patch WITH a secret re-seals it.
            let rotated = repo
                .patch(
                    "acme",
                    &id,
                    PatchConnection {
                        secret: Some(Some("ghp_rotated".into())),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_ne!(rotated.secret_sealed.as_ref().unwrap(), &original_blob);
            assert_eq!(rotated.unseal_secret().unwrap().as_deref(), Some("ghp_rotated"));

            // Patching another tenant's id is None.
            assert!(repo
                .patch("other", &id, PatchConnection::default())
                .await
                .unwrap()
                .is_none());

            repo.delete("acme", &id).await.unwrap();
        }
    }
}
