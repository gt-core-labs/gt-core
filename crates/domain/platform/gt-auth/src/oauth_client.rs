//! OAuth 2.0 client registry (Authorization Server surface, hq-oauth-as.1).
//!
//! An **OAuth client** is a downstream application — like Claude.ai's remote MCP connector —
//! that authenticates users THROUGH gt's `/oauth/authorize` → `/oauth/token` flow.  This is
//! the inverse of [`oauth_providers`](crate::provider_repo) (upstream IdPs gt logs into).
//!
//! Each registered client carries:
//! - `client_id` — the public identifier sent on `/oauth/authorize`.
//! - `client_secret_enc` — AES-256-GCM sealed at rest (same [`crypto`](crate::crypto) infra
//!   as `oauth_providers`), verified on `/oauth/token`.
//! - `redirect_uris` — strict allowlist (exact match, no wildcards).
//! - `allowed_scopes` — comma-separated ceiling; the issued token's scopes are the intersection
//!   of the user's grants and this ceiling.
//!
//! The [`OAuthClientRepo`] port + [`PgOAuthClientRepo`] adapter follow the same hexagonal
//! pattern as [`ProviderRepo`](crate::ProviderRepo) (docs/03 Rule 4).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::AuthError;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A registered OAuth client read from the store (sealed secret, never cleartext).
#[derive(Clone, Debug)]
pub struct OAuthClientRecord {
    /// The public client identifier.
    pub client_id: String,
    /// AES-256-GCM sealed client secret (nonce ‖ ciphertext ‖ tag).
    pub client_secret_enc: Vec<u8>,
    /// Human-readable name shown on the consent screen.
    pub display_name: String,
    /// Strict redirect-URI allowlist (exact match on `/oauth/authorize`).
    pub redirect_uris: Vec<String>,
    /// Comma-separated scope ceiling.  The issued token's scopes are the intersection of the
    /// user's grants and this ceiling.  Empty = no scopes.
    pub allowed_scopes: String,
    /// Whether the client is active.  Disabled clients are rejected on `/oauth/authorize`.
    pub enabled: bool,
}

impl OAuthClientRecord {
    /// Unseal the client secret in memory.  Returns `AuthError::Backend` when the master key
    /// is wrong or the blob is corrupt.
    pub fn unseal_secret(&self) -> Result<String, AuthError> {
        let bytes = crate::crypto::unseal(&self.client_secret_enc)?;
        String::from_utf8(bytes)
            .map_err(|e| AuthError::Backend(format!("oauth_client secret decode: {e}")))
    }

    /// Check whether `uri` is in the registered [`redirect_uris`](Self::redirect_uris)
    /// (exact match, RFC 6749 §3.1.2.3).
    pub fn redirect_uri_allowed(&self, uri: &str) -> bool {
        self.redirect_uris.iter().any(|r| r == uri)
    }

    /// Parse [`allowed_scopes`](Self::allowed_scopes) into individual scope strings.
    pub fn scope_list(&self) -> Vec<&str> {
        self.allowed_scopes
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// Payload for creating a new OAuth client.  The `client_secret` is cleartext here — the
/// repo seals it before writing.
#[derive(Clone, Debug)]
pub struct NewOAuthClient {
    pub client_id: String,
    /// Cleartext secret — sealed by the repo on [`create`](OAuthClientRepo::create).
    pub client_secret: String,
    pub display_name: String,
    pub redirect_uris: Vec<String>,
    pub allowed_scopes: String,
    pub enabled: bool,
}

/// Partial update for an existing OAuth client.  `None` fields are left untouched.  If
/// `client_secret` is `Some`, the repo re-seals it.
#[derive(Clone, Debug, Default)]
pub struct PatchOAuthClient {
    pub display_name: Option<String>,
    pub client_secret: Option<String>,
    pub redirect_uris: Option<Vec<String>>,
    pub allowed_scopes: Option<String>,
    pub enabled: Option<bool>,
}

// ---------------------------------------------------------------------------
// View (secret-omitting projection for admin responses)
// ---------------------------------------------------------------------------

/// Secret-omitting projection returned by admin endpoints — never exposes the sealed secret.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuthClientView {
    pub client_id: String,
    pub display_name: String,
    pub redirect_uris: Vec<String>,
    pub allowed_scopes: String,
    pub enabled: bool,
}

impl From<OAuthClientRecord> for OAuthClientView {
    fn from(r: OAuthClientRecord) -> Self {
        Self {
            client_id: r.client_id,
            display_name: r.display_name,
            redirect_uris: r.redirect_uris,
            allowed_scopes: r.allowed_scopes,
            enabled: r.enabled,
        }
    }
}

// ---------------------------------------------------------------------------
// Repository port
// ---------------------------------------------------------------------------

/// The CRUD port over `public.oauth_clients`.  Same contract style as
/// [`ProviderRepo`](crate::ProviderRepo).
#[async_trait]
pub trait OAuthClientRepo: Send + Sync {
    /// List all registered clients, ordered by `created_at` (oldest first).
    async fn list(&self) -> Result<Vec<OAuthClientRecord>, AuthError>;
    /// Get a single client by `client_id`, or `None` if absent.
    async fn get(&self, client_id: &str) -> Result<Option<OAuthClientRecord>, AuthError>;
    /// Register a new client.  The cleartext `client_secret` is sealed before writing.
    /// Duplicate `client_id` is `AuthError::Backend`.
    async fn create(&self, client: NewOAuthClient) -> Result<OAuthClientRecord, AuthError>;
    /// Partial update.  Returns `None` if no row matched.  When `client_secret` is `Some`,
    /// re-seals the new value.
    async fn patch(
        &self,
        client_id: &str,
        patch: PatchOAuthClient,
    ) -> Result<Option<OAuthClientRecord>, AuthError>;
    /// Delete a client.  Returns `true` if a row was removed, `false` if none matched.
    async fn delete(&self, client_id: &str) -> Result<bool, AuthError>;
}

// ---------------------------------------------------------------------------
// Postgres adapter (gated behind `pg`)
// ---------------------------------------------------------------------------

#[cfg(feature = "pg")]
pub use pg_impl::PgOAuthClientRepo;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`OAuthClientRepo`] over the GLOBAL `public.oauth_clients` table.
    #[derive(Clone, Debug)]
    pub struct PgOAuthClientRepo {
        pool: PgPool,
    }

    impl PgOAuthClientRepo {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    const COLUMNS: &str =
        "client_id, client_secret_enc, display_name, redirect_uris, allowed_scopes, enabled";

    fn row_to_record(row: &PgRow) -> Result<OAuthClientRecord, AuthError> {
        Ok(OAuthClientRecord {
            client_id: row
                .try_get("client_id")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
            client_secret_enc: row
                .try_get("client_secret_enc")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
            display_name: row
                .try_get("display_name")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
            redirect_uris: row
                .try_get::<Vec<String>, _>("redirect_uris")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
            allowed_scopes: row
                .try_get("allowed_scopes")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| AuthError::Backend(format!("oauth_clients pg: {e}")))?,
        })
    }

    #[async_trait]
    impl OAuthClientRepo for PgOAuthClientRepo {
        async fn list(&self) -> Result<Vec<OAuthClientRecord>, AuthError> {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM public.oauth_clients ORDER BY created_at"
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients list: {e}")))?;
            rows.iter().map(row_to_record).collect()
        }

        async fn get(&self, client_id: &str) -> Result<Option<OAuthClientRecord>, AuthError> {
            let row = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM public.oauth_clients WHERE client_id = $1"
            ))
            .bind(client_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients get: {e}")))?;
            row.as_ref().map(row_to_record).transpose()
        }

        async fn create(&self, client: NewOAuthClient) -> Result<OAuthClientRecord, AuthError> {
            let sealed = crate::crypto::seal(client.client_secret.as_bytes())?;
            let row = sqlx::query(&format!(
                "INSERT INTO public.oauth_clients \
                 (client_id, client_secret_enc, display_name, redirect_uris, allowed_scopes, enabled) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 RETURNING {COLUMNS}"
            ))
            .bind(&client.client_id)
            .bind(&sealed)
            .bind(&client.display_name)
            .bind(&client.redirect_uris)
            .bind(&client.allowed_scopes)
            .bind(client.enabled)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients create: {e}")))?;
            row_to_record(&row)
        }

        async fn patch(
            &self,
            client_id: &str,
            patch: PatchOAuthClient,
        ) -> Result<Option<OAuthClientRecord>, AuthError> {
            // Read-modify-write inside a transaction so a concurrent patch does not lose a field.
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| AuthError::Backend(format!("oauth_clients patch begin: {e}")))?;

            let existing = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM public.oauth_clients WHERE client_id = $1 FOR UPDATE"
            ))
            .bind(client_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients patch select: {e}")))?;

            let Some(existing) = existing else {
                return Ok(None);
            };
            let mut rec = row_to_record(&existing)?;

            if let Some(name) = patch.display_name {
                rec.display_name = name;
            }
            if let Some(secret) = patch.client_secret {
                rec.client_secret_enc = crate::crypto::seal(secret.as_bytes())?;
            }
            if let Some(uris) = patch.redirect_uris {
                rec.redirect_uris = uris;
            }
            if let Some(scopes) = patch.allowed_scopes {
                rec.allowed_scopes = scopes;
            }
            if let Some(enabled) = patch.enabled {
                rec.enabled = enabled;
            }

            sqlx::query(
                "UPDATE public.oauth_clients SET \
                 display_name = $2, client_secret_enc = $3, redirect_uris = $4, \
                 allowed_scopes = $5, enabled = $6 \
                 WHERE client_id = $1",
            )
            .bind(client_id)
            .bind(&rec.display_name)
            .bind(&rec.client_secret_enc)
            .bind(&rec.redirect_uris)
            .bind(&rec.allowed_scopes)
            .bind(rec.enabled)
            .execute(&mut *tx)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_clients patch update: {e}")))?;

            tx.commit()
                .await
                .map_err(|e| AuthError::Backend(format!("oauth_clients patch commit: {e}")))?;

            Ok(Some(rec))
        }

        async fn delete(&self, client_id: &str) -> Result<bool, AuthError> {
            let res =
                sqlx::query("DELETE FROM public.oauth_clients WHERE client_id = $1")
                    .bind(client_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| AuthError::Backend(format!("oauth_clients delete: {e}")))?;
            Ok(res.rows_affected() > 0)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_uri_allowed_is_exact_match() {
        let rec = OAuthClientRecord {
            client_id: "test".into(),
            client_secret_enc: vec![],
            display_name: "Test".into(),
            redirect_uris: vec![
                "https://claude.ai/oauth/callback".into(),
                "http://localhost:3000/callback".into(),
            ],
            allowed_scopes: String::new(),
            enabled: true,
        };
        assert!(rec.redirect_uri_allowed("https://claude.ai/oauth/callback"));
        assert!(rec.redirect_uri_allowed("http://localhost:3000/callback"));
        assert!(!rec.redirect_uri_allowed("https://claude.ai/oauth/callback/"));
        assert!(!rec.redirect_uri_allowed("https://evil.test/callback"));
    }

    #[test]
    fn scope_list_splits_and_trims() {
        let rec = OAuthClientRecord {
            client_id: "test".into(),
            client_secret_enc: vec![],
            display_name: "Test".into(),
            redirect_uris: vec![],
            allowed_scopes: "issues.read, agent.read , memory.read".into(),
            enabled: true,
        };
        assert_eq!(rec.scope_list(), vec!["issues.read", "agent.read", "memory.read"]);
    }

    #[test]
    fn empty_scopes_yields_empty_list() {
        let rec = OAuthClientRecord {
            client_id: "test".into(),
            client_secret_enc: vec![],
            display_name: "Test".into(),
            redirect_uris: vec![],
            allowed_scopes: String::new(),
            enabled: true,
        };
        assert!(rec.scope_list().is_empty());
    }

    #[test]
    fn view_omits_secret() {
        let rec = OAuthClientRecord {
            client_id: "c1".into(),
            client_secret_enc: vec![1, 2, 3],
            display_name: "App".into(),
            redirect_uris: vec!["https://example.com/cb".into()],
            allowed_scopes: "a,b".into(),
            enabled: true,
        };
        let view: OAuthClientView = rec.into();
        assert_eq!(view.client_id, "c1");
        assert_eq!(view.display_name, "App");
        // No `client_secret_enc` field on OAuthClientView — omitted by design.
    }

    // --- PG-gated contract tests ---
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

        async fn ensure_table(pool: &PgPool) {
            let mut tx = pool.begin().await.expect("begin ddl tx");
            sqlx::query("SELECT pg_advisory_xact_lock(8425)")
                .execute(&mut *tx)
                .await
                .expect("advisory lock");
            sqlx::query(crate::migrations::CREATE_OAUTH_CLIENTS)
                .execute(&mut *tx)
                .await
                .expect("create oauth_clients table");
            tx.commit().await.expect("commit ddl tx");
        }

        fn unique_id(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            format!("test-{tag}-{n}")
        }

        #[tokio::test]
        async fn crud_lifecycle() {
            std::env::set_var(
                crate::ENV_SECRET_KEY,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            let Some(pool) = pool_or_skip().await else { return };
            ensure_table(&pool).await;
            let repo = PgOAuthClientRepo::new(pool);

            let id = unique_id("crud");

            // Create
            let rec = repo
                .create(NewOAuthClient {
                    client_id: id.clone(),
                    client_secret: "super-secret".into(),
                    display_name: "Claude.ai".into(),
                    redirect_uris: vec!["https://claude.ai/cb".into()],
                    allowed_scopes: "issues.read,memory.read".into(),
                    enabled: true,
                })
                .await
                .unwrap();
            assert_eq!(rec.client_id, id);
            assert_eq!(rec.display_name, "Claude.ai");
            assert!(rec.redirect_uri_allowed("https://claude.ai/cb"));
            // Secret round-trips through seal/unseal.
            assert_eq!(rec.unseal_secret().unwrap(), "super-secret");

            // Get
            let got = repo.get(&id).await.unwrap().expect("present");
            assert_eq!(got.client_id, id);

            // List
            let all = repo.list().await.unwrap();
            assert!(all.iter().any(|r| r.client_id == id));

            // Patch
            let patched = repo
                .patch(
                    &id,
                    PatchOAuthClient {
                        display_name: Some("Claude Remote".into()),
                        enabled: Some(false),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .expect("present");
            assert_eq!(patched.display_name, "Claude Remote");
            assert!(!patched.enabled);
            // Secret unchanged by a metadata-only patch.
            assert_eq!(patched.unseal_secret().unwrap(), "super-secret");

            // Delete
            assert!(repo.delete(&id).await.unwrap());
            assert!(repo.get(&id).await.unwrap().is_none());
            // Idempotent second delete.
            assert!(!repo.delete(&id).await.unwrap());
        }

        #[tokio::test]
        async fn patch_nonexistent_returns_none() {
            let Some(pool) = pool_or_skip().await else { return };
            ensure_table(&pool).await;
            let repo = PgOAuthClientRepo::new(pool);
            let result = repo
                .patch("nonexistent", PatchOAuthClient::default())
                .await
                .unwrap();
            assert!(result.is_none());
        }
    }
}
