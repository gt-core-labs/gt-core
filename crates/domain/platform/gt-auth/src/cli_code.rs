//! The one-shot CLI hand-off code store (hq-gt-login-oauth.2).
//!
//! The `gt login` browser flow finishes at `/auth/callback`, which mints the session — but a CLI
//! loopback server cannot read a URL `#fragment` (the browser never sends it) and the token pair is
//! too sensitive to drop in a loopback query string. So the callback parks the freshly minted pair
//! here under a short opaque `code` and 302s only that `code` to the loopback; the CLI redeems it
//! ONCE at `POST /auth/cli/exchange`.
//!
//! The row is short-lived (~60s) and ONE-SHOT — [`consume`](CliCodeRepo::consume) DELETEs it on read
//! (DELETE … RETURNING, same atomic one-shot as [`crate::authz_state`]), so a replayed/captured
//! loopback URL is useless after the first redemption. Durable (Postgres) so the redemption survives
//! a redeploy and works across replicas.

use async_trait::async_trait;

use crate::error::AuthError;

/// A minted CLI hand-off code to persist (the callback writes this). `code` is the opaque secret the
/// loopback receives; the rest is the token pair the exchange returns verbatim.
#[derive(Clone, Debug)]
pub struct NewCliCode {
    /// The opaque one-shot code (primary key) handed to the loopback.
    pub code: String,
    /// The minted access JWT.
    pub access_token: String,
    /// The minted opaque refresh token.
    pub refresh_token: String,
    /// Always `"Bearer"` today — stored verbatim so the exchange echoes the callback's value.
    pub token_type: String,
    /// Access-token lifetime in seconds.
    pub expires_in: u64,
    /// Creation time, epoch seconds.
    pub created_at: u64,
    /// Expiry, epoch seconds (~60s after `created_at`); a row read past this is rejected.
    pub expires_at: u64,
}

/// A CLI hand-off code read back by [`CliCodeRepo::consume`] — the token pair the exchange returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCliCode {
    /// The minted access JWT.
    pub access_token: String,
    /// The minted opaque refresh token.
    pub refresh_token: String,
    /// The token type (`"Bearer"`).
    pub token_type: String,
    /// Access-token lifetime in seconds.
    pub expires_in: u64,
    /// Expiry, epoch seconds — the exchange rejects a row read past this instant.
    pub expires_at: u64,
}

/// The async store port over the one-shot CLI hand-off codes.
///
/// An abstract port (docs/03 Rule 4) so the callback/exchange handlers depend on the contract, not
/// the Postgres adapter. [`consume`](Self::consume) is ONE-SHOT — it deletes the row as it reads it,
/// so a replayed `code` finds nothing.
#[async_trait]
pub trait CliCodeRepo: Send + Sync {
    /// Persist a minted code. A duplicate `code` (astronomically unlikely with 256-bit entropy) is
    /// [`AuthError::Backend`] — never a silent overwrite.
    async fn insert(&self, code: NewCliCode) -> Result<(), AuthError>;
    /// One-shot read: atomically DELETE the row for `code` and return it, or `None` if absent
    /// (unknown or already-consumed). The handler still checks `expires_at` on the result.
    async fn consume(&self, code: &str) -> Result<Option<PendingCliCode>, AuthError>;
    /// Best-effort sweep of rows past `now` (epoch seconds). Returns the number removed.
    async fn purge_expired(&self, now: u64) -> Result<u64, AuthError>;
}

#[cfg(feature = "pg")]
pub use pg_impl::PgCliCodeRepo;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`CliCodeRepo`] over the GLOBAL `public.oauth_cli_code` table. Statements are
    /// `public`-qualified (cross-tenant, like `oauth_authz_state`). Cloning is cheap (`PgPool` is an
    /// `Arc`).
    #[derive(Clone, Debug)]
    pub struct PgCliCodeRepo {
        pool: PgPool,
    }

    impl PgCliCodeRepo {
        /// Wrap a connection `pool`. The `public.oauth_cli_code` table is expected to already exist
        /// (provisioned via `migrations/auth/0010__create_oauth_cli_code.sql`).
        pub fn new(pool: PgPool) -> Self {
            PgCliCodeRepo { pool }
        }
    }

    /// Decode an `oauth_cli_code` row into a [`PendingCliCode`]. A column read fault is
    /// [`AuthError::Backend`] (an outage/corruption, never a denied request).
    fn row_to_pending(row: &PgRow) -> Result<PendingCliCode, AuthError> {
        let expires_at: f64 = row
            .try_get("expires_at_epoch")
            .map_err(|e| AuthError::Backend(format!("oauth_cli_code postgres: {e}")))?;
        let expires_in: i64 = row
            .try_get("expires_in")
            .map_err(|e| AuthError::Backend(format!("oauth_cli_code postgres: {e}")))?;
        Ok(PendingCliCode {
            access_token: row
                .try_get("access_token")
                .map_err(|e| AuthError::Backend(format!("oauth_cli_code postgres: {e}")))?,
            refresh_token: row
                .try_get("refresh_token")
                .map_err(|e| AuthError::Backend(format!("oauth_cli_code postgres: {e}")))?,
            token_type: row
                .try_get("token_type")
                .map_err(|e| AuthError::Backend(format!("oauth_cli_code postgres: {e}")))?,
            expires_in: expires_in as u64,
            expires_at: expires_at as u64,
        })
    }

    #[async_trait]
    impl CliCodeRepo for PgCliCodeRepo {
        async fn insert(&self, code: NewCliCode) -> Result<(), AuthError> {
            sqlx::query(
                "INSERT INTO public.oauth_cli_code \
                 (code, access_token, refresh_token, token_type, expires_in, created_at, expires_at) \
                 VALUES ($1, $2, $3, $4, $5, to_timestamp($6), to_timestamp($7))",
            )
            .bind(&code.code)
            .bind(&code.access_token)
            .bind(&code.refresh_token)
            .bind(&code.token_type)
            .bind(code.expires_in as i64)
            .bind(code.created_at as f64)
            .bind(code.expires_at as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_cli_code insert: {e}")))?;
            Ok(())
        }

        async fn consume(&self, code: &str) -> Result<Option<PendingCliCode>, AuthError> {
            // DELETE ... RETURNING is the atomic one-shot: a replayed `code` returns no row.
            let row = sqlx::query(
                "DELETE FROM public.oauth_cli_code WHERE code = $1 \
                 RETURNING access_token, refresh_token, token_type, expires_in, \
                 extract(epoch from expires_at)::float8 AS expires_at_epoch",
            )
            .bind(code)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_cli_code consume: {e}")))?;
            row.as_ref().map(row_to_pending).transpose()
        }

        async fn purge_expired(&self, now: u64) -> Result<u64, AuthError> {
            let res = sqlx::query(
                "DELETE FROM public.oauth_cli_code WHERE expires_at < to_timestamp($1)",
            )
            .bind(now as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_cli_code purge: {e}")))?;
            Ok(res.rows_affected())
        }
    }
}
