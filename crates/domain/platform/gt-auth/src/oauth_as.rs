//! OAuth 2.0 Authorization Server — authorization code store (hq-oauth-as.2).
//!
//! When gt acts as the Authorization Server (the inverse of the IdP-client flow in
//! [`authz_state`](crate::authz_state)), `GET /oauth/authorize` stores a short-lived,
//! one-shot authorization code here after the user consents.  `POST /oauth/token` then
//! consumes it (DELETE-on-read) to mint access + refresh tokens for the downstream OAuth
//! client (e.g. Claude.ai remote MCP connector).
//!
//! Same one-shot + TTL pattern as [`AuthzStateRepo`](crate::AuthzStateRepo).

use async_trait::async_trait;

use crate::AuthError;

/// TTL for authorization codes (seconds).  RFC 6749 §4.1.2 recommends a maximum of 10 min;
/// we use 5 min, matching the upstream PKCE flow.
pub const CODE_TTL_SECS: u64 = 300;

/// A pending authorization code to persist (the `/oauth/authorize` leg writes this).
#[derive(Clone, Debug)]
pub struct NewAsCode {
    /// The opaque authorization code (primary key).
    pub code: String,
    /// The `oauth_clients.client_id` this code was issued to.
    pub client_id: String,
    /// The authenticated user's subject.
    pub user_sub: String,
    /// The authenticated user's workspace.
    pub user_workspace: String,
    /// Comma-separated scopes granted (intersection of user's scopes and client's ceiling).
    pub user_scopes: String,
    /// The `redirect_uri` echoed on this authorize request (must match on `/oauth/token`).
    pub redirect_uri: String,
    /// The PKCE `code_challenge` (S256) — verified against `code_verifier` at `/oauth/token`.
    pub code_challenge: String,
    /// Creation time, epoch seconds.
    pub created_at: u64,
    /// Expiry, epoch seconds.
    pub expires_at: u64,
}

/// An authorization code read back by [`AsCodeStore::consume`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAsCode {
    pub code: String,
    pub client_id: String,
    pub user_sub: String,
    pub user_workspace: String,
    pub user_scopes: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub expires_at: u64,
}

/// The async store port over authorization codes.
#[async_trait]
pub trait AsCodeStore: Send + Sync {
    /// Persist a pending authorization code.
    async fn insert(&self, code: NewAsCode) -> Result<(), AuthError>;
    /// One-shot read: atomically DELETE the row for `code` and return it, or `None` if
    /// absent (unknown or already-consumed).
    async fn consume(&self, code: &str) -> Result<Option<PendingAsCode>, AuthError>;
    /// Best-effort sweep of rows past `now` (epoch seconds).
    async fn purge_expired(&self, now: u64) -> Result<u64, AuthError>;
}

/// Generate a high-entropy authorization code (32 bytes, hex-encoded = 64 chars).
pub fn generate_code() -> Result<String, AuthError> {
    let mut raw = [0u8; 32];
    getrandom::getrandom(&mut raw)
        .map_err(|e| AuthError::Backend(format!("OS CSPRNG for authz code: {e}")))?;
    Ok(hex::encode(raw))
}

// ---------------------------------------------------------------------------
// Postgres adapter
// ---------------------------------------------------------------------------

#[cfg(feature = "pg")]
pub use pg_impl::PgAsCodeStore;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    #[derive(Clone, Debug)]
    pub struct PgAsCodeStore {
        pool: PgPool,
    }

    impl PgAsCodeStore {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    fn row_to_pending(row: &PgRow) -> Result<PendingAsCode, AuthError> {
        let expires_at: f64 = row
            .try_get("expires_at_epoch")
            .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?;
        Ok(PendingAsCode {
            code: row
                .try_get("code")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            client_id: row
                .try_get("client_id")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            user_sub: row
                .try_get("user_sub")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            user_workspace: row
                .try_get("user_workspace")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            user_scopes: row
                .try_get("user_scopes")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            redirect_uri: row
                .try_get("redirect_uri")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            code_challenge: row
                .try_get("code_challenge")
                .map_err(|e| AuthError::Backend(format!("oauth_as_codes pg: {e}")))?,
            expires_at: expires_at as u64,
        })
    }

    #[async_trait]
    impl AsCodeStore for PgAsCodeStore {
        async fn insert(&self, ac: NewAsCode) -> Result<(), AuthError> {
            sqlx::query(
                "INSERT INTO public.oauth_as_codes \
                 (code, client_id, user_sub, user_workspace, user_scopes, \
                  redirect_uri, code_challenge, created_at, expires_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8), to_timestamp($9))",
            )
            .bind(&ac.code)
            .bind(&ac.client_id)
            .bind(&ac.user_sub)
            .bind(&ac.user_workspace)
            .bind(&ac.user_scopes)
            .bind(&ac.redirect_uri)
            .bind(&ac.code_challenge)
            .bind(ac.created_at as f64)
            .bind(ac.expires_at as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_as_codes insert: {e}")))?;
            Ok(())
        }

        async fn consume(&self, code: &str) -> Result<Option<PendingAsCode>, AuthError> {
            let row = sqlx::query(
                "DELETE FROM public.oauth_as_codes WHERE code = $1 \
                 RETURNING code, client_id, user_sub, user_workspace, user_scopes, \
                 redirect_uri, code_challenge, \
                 extract(epoch from expires_at)::float8 AS expires_at_epoch",
            )
            .bind(code)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_as_codes consume: {e}")))?;
            row.as_ref().map(row_to_pending).transpose()
        }

        async fn purge_expired(&self, now: u64) -> Result<u64, AuthError> {
            let res = sqlx::query(
                "DELETE FROM public.oauth_as_codes WHERE expires_at < to_timestamp($1)",
            )
            .bind(now as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_as_codes purge: {e}")))?;
            Ok(res.rows_affected())
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
    fn generate_code_produces_64_hex_chars() {
        let code = generate_code().unwrap();
        assert_eq!(code.len(), 64);
        assert!(code.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn each_code_is_unique() {
        let a = generate_code().unwrap();
        let b = generate_code().unwrap();
        assert_ne!(a, b);
    }

    #[cfg(feature = "pg")]
    mod pg_contract {
        use super::*;
        use sqlx::PgPool;

        async fn pool_or_skip() -> Option<PgPool> {
            let url = std::env::var("GT_PG_URL").ok()?;
            Some(PgPool::connect(&url).await.expect("GT_PG_URL reachable"))
        }

        async fn ensure_table(pool: &PgPool) {
            let mut tx = pool.begin().await.expect("begin");
            sqlx::query("SELECT pg_advisory_xact_lock(8426)")
                .execute(&mut *tx)
                .await
                .expect("lock");
            sqlx::query(crate::migrations::CREATE_OAUTH_AS_CODES)
                .execute(&mut *tx)
                .await
                .expect("create table");
            tx.commit().await.expect("commit");
        }

        #[tokio::test]
        async fn insert_then_consume_is_one_shot() {
            let Some(pool) = pool_or_skip().await else { return };
            ensure_table(&pool).await;
            let store = PgAsCodeStore::new(pool);

            let code = generate_code().unwrap();
            store
                .insert(NewAsCode {
                    code: code.clone(),
                    client_id: "claude-ai".into(),
                    user_sub: "alice".into(),
                    user_workspace: "default".into(),
                    user_scopes: "issues.read,memory.read".into(),
                    redirect_uri: "https://claude.ai/cb".into(),
                    code_challenge: "challenge-xyz".into(),
                    created_at: 1000,
                    expires_at: 1300,
                })
                .await
                .unwrap();

            let got = store.consume(&code).await.unwrap().expect("present");
            assert_eq!(got.client_id, "claude-ai");
            assert_eq!(got.user_sub, "alice");
            assert_eq!(got.user_workspace, "default");
            assert_eq!(got.user_scopes, "issues.read,memory.read");
            assert_eq!(got.redirect_uri, "https://claude.ai/cb");
            assert_eq!(got.code_challenge, "challenge-xyz");
            assert_eq!(got.expires_at, 1300);

            // Second consume finds nothing (one-shot).
            assert!(store.consume(&code).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn purge_expired_removes_stale_codes() {
            let Some(pool) = pool_or_skip().await else { return };
            ensure_table(&pool).await;
            let store = PgAsCodeStore::new(pool);

            let stale = generate_code().unwrap();
            let fresh = generate_code().unwrap();
            for (c, exp) in [(&stale, 100u64), (&fresh, 9_999_999_999)] {
                store
                    .insert(NewAsCode {
                        code: c.clone(),
                        client_id: "c".into(),
                        user_sub: "u".into(),
                        user_workspace: "w".into(),
                        user_scopes: String::new(),
                        redirect_uri: "https://x.test/cb".into(),
                        code_challenge: "ch".into(),
                        created_at: 10,
                        expires_at: exp,
                    })
                    .await
                    .unwrap();
            }

            assert!(store.purge_expired(200).await.unwrap() >= 1);
            assert!(store.consume(&stale).await.unwrap().is_none());
            assert!(store.consume(&fresh).await.unwrap().is_some());
        }
    }
}
