//! Ephemeral OAuth authorize-state + PKCE verifier store (hq-idp-db.3).
//!
//! The public authorization-code flow has two legs: `GET /auth/providers/{id}/authorize` starts it
//! (mint a random anti-CSRF `state` + a PKCE `code_verifier`, redirect the browser to the IdP with
//! `state` and the S256 `code_challenge`), and `GET /auth/callback?code&state` finishes it (validate
//! + consume the `state`, recover the `code_verifier`, exchange the `code`). Between the two legs the
//! pending handshake must be REMEMBERED somewhere durable — that is this module.
//!
//! It owns:
//! - [`Pkce`] / [`new_pkce`] — a fresh PKCE pair: a high-entropy `code_verifier` and its `S256`
//!   `code_challenge` (`BASE64URL(SHA256(verifier))`, no padding — RFC 7636).
//! - [`NewAuthz`] / [`PendingAuthz`] — a pending row to persist / one read back.
//! - [`AuthzStateRepo`] — the async port: `insert`, one-shot `consume` (delete-on-read), and a
//!   best-effort `purge_expired` sweep.
//! - [`PgAuthzStateRepo`] — the Postgres adapter over `public.oauth_authz_state`, gated by `pg`.
//!
//! Scope is GLOBAL (the flow is keyed by the opaque `state`, not a tenant), the row is one-shot
//! (the callback DELETEs it on read, so a replayed `state` is rejected), and it is durable (Postgres,
//! not in-memory) so an in-flight login survives a `gt-mcp-server` redeploy — consistent with the
//! durable refresh store (hq-platform-hardening.1).

use async_trait::async_trait;

use crate::AuthError;

/// A freshly minted PKCE pair (RFC 7636): the secret `code_verifier` kept server-side and the
/// `code_challenge` (its `S256` transform) sent to the IdP on `/authorize`. The IdP later binds the
/// authorization `code` to this challenge, and the token exchange proves possession by replaying the
/// verifier — so an intercepted `code` is useless without it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pkce {
    /// The high-entropy secret, persisted with the pending row and replayed on the token exchange.
    pub verifier: String,
    /// `BASE64URL-NOPAD(SHA256(verifier))` — the `code_challenge` sent to the IdP (`S256` method).
    pub challenge: String,
}

/// Mint a fresh PKCE pair: a 32-byte CSPRNG `code_verifier` (base64url, no padding — a valid
/// 43-char verifier per RFC 7636) and its `S256` `code_challenge`. The OS CSPRNG (`getrandom`) is
/// the same entropy source the refresh-token minter and the AES-GCM nonce draw from.
pub fn new_pkce() -> Result<Pkce, AuthError> {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    let mut raw = [0u8; 32];
    getrandom::getrandom(&mut raw)
        .map_err(|e| AuthError::Backend(format!("OS CSPRNG for PKCE verifier: {e}")))?;
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    let digest = Sha256::digest(verifier.as_bytes());
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    Ok(Pkce { verifier, challenge })
}

/// A pending authorize handshake to persist (the `/authorize` leg writes this). `state` is the
/// opaque anti-CSRF token echoed back on the callback; `code_verifier` is the PKCE secret;
/// `provider_id` selects which stored provider runs the exchange; `redirect_uri` is the app's own
/// callback URL echoed on the token exchange. `expires_at`/`created_at` are epoch seconds.
#[derive(Clone, Debug)]
pub struct NewAuthz {
    /// The opaque anti-CSRF `state` (primary key) sent to the IdP and matched on the callback.
    pub state: String,
    /// The PKCE `code_verifier`, replayed on the token exchange.
    pub code_verifier: String,
    /// The `oauth_providers` id this handshake targets.
    pub provider_id: String,
    /// The app's own callback URL echoed on the exchange (the OAuth spec requires it match).
    pub redirect_uri: String,
    /// The CLI loopback URL to hand the session back to (`gt login`), or `None` for the ordinary
    /// web login. STRICTLY a `127.0.0.1`/`localhost` URL — allowlisted at `/authorize` before it
    /// reaches here. Distinct from [`redirect_uri`](Self::redirect_uri) (the IdP's callback).
    pub cli_redirect: Option<String>,
    /// Creation time, epoch seconds.
    pub created_at: u64,
    /// Expiry, epoch seconds (~10 min after `created_at`); a consumed-after-expiry row is rejected.
    pub expires_at: u64,
}

/// A pending authorize handshake read back by [`AuthzStateRepo::consume`] — everything the callback
/// needs to finish the exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAuthz {
    /// The opaque anti-CSRF `state` (primary key).
    pub state: String,
    /// The PKCE `code_verifier` to replay on the token exchange.
    pub code_verifier: String,
    /// The `oauth_providers` id this handshake targets.
    pub provider_id: String,
    /// The app's own callback URL echoed on the exchange.
    pub redirect_uri: String,
    /// The CLI loopback URL to 302 the one-shot code to (`gt login`), or `None` for the ordinary
    /// web login (the callback then redirects to the FE as before).
    pub cli_redirect: Option<String>,
    /// Expiry, epoch seconds — the callback rejects a row read past this instant.
    pub expires_at: u64,
}

/// The async store port over the ephemeral authorize-state rows.
///
/// An abstract port (docs/03 Rule 4) so the `/authorize`→`/callback` handlers depend on the contract,
/// not the Postgres adapter. [`consume`](Self::consume) is ONE-SHOT — it deletes the row as it reads
/// it, so a replayed `state` finds nothing (anti-CSRF + replay defence).
#[async_trait]
pub trait AuthzStateRepo: Send + Sync {
    /// Persist a pending handshake. A duplicate `state` (astronomically unlikely with 256-bit
    /// entropy) is [`AuthError::Backend`] — never a silent overwrite of another flow.
    async fn insert(&self, authz: NewAuthz) -> Result<(), AuthError>;
    /// One-shot read: atomically DELETE the row for `state` and return it, or `None` if absent
    /// (unknown or already-consumed `state`). The handler still checks `expires_at` on the result.
    async fn consume(&self, state: &str) -> Result<Option<PendingAuthz>, AuthError>;
    /// Best-effort sweep of rows past `now` (epoch seconds). Returns the number removed; a fault is
    /// logged-and-dropped by the caller, never surfaced to the login.
    async fn purge_expired(&self, now: u64) -> Result<u64, AuthError>;
}

#[cfg(feature = "pg")]
pub use pg_impl::PgAuthzStateRepo;

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};

    /// Postgres-backed [`AuthzStateRepo`] over the GLOBAL `public.oauth_authz_state` table.
    ///
    /// Statements are `public`-qualified (the table is cross-tenant, like `oauth_providers`). Epoch
    /// seconds cross the boundary as `TIMESTAMPTZ` via `to_timestamp` on write and
    /// `extract(epoch ...)` on read, so the port stays a plain `u64` while the column self-describes.
    /// Cloning is cheap: `PgPool` is an `Arc`.
    #[derive(Clone, Debug)]
    pub struct PgAuthzStateRepo {
        pool: PgPool,
    }

    impl PgAuthzStateRepo {
        /// Wrap a connection `pool`. The `public.oauth_authz_state` table is expected to already
        /// exist (provisioned via `migrations/auth/0007__create_oauth_authz_state.sql`).
        pub fn new(pool: PgPool) -> Self {
            PgAuthzStateRepo { pool }
        }
    }

    /// Decode an `oauth_authz_state` row into a [`PendingAuthz`]. A column read fault is
    /// [`AuthError::Backend`] (an outage/corruption, never a denied request).
    fn row_to_pending(row: &PgRow) -> Result<PendingAuthz, AuthError> {
        let expires_at: f64 = row
            .try_get("expires_at_epoch")
            .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?;
        Ok(PendingAuthz {
            state: row
                .try_get("state")
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?,
            code_verifier: row
                .try_get("code_verifier")
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?,
            provider_id: row
                .try_get("provider_id")
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?,
            redirect_uri: row
                .try_get("redirect_uri")
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?,
            // Nullable column: a web-login row stores NULL → `None`.
            cli_redirect: row
                .try_get("cli_redirect")
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state postgres: {e}")))?,
            expires_at: expires_at as u64,
        })
    }

    #[async_trait]
    impl AuthzStateRepo for PgAuthzStateRepo {
        async fn insert(&self, authz: NewAuthz) -> Result<(), AuthError> {
            sqlx::query(
                "INSERT INTO public.oauth_authz_state \
                 (state, code_verifier, provider_id, redirect_uri, cli_redirect, created_at, expires_at) \
                 VALUES ($1, $2, $3, $4, $5, to_timestamp($6), to_timestamp($7))",
            )
            .bind(&authz.state)
            .bind(&authz.code_verifier)
            .bind(&authz.provider_id)
            .bind(&authz.redirect_uri)
            .bind(&authz.cli_redirect)
            .bind(authz.created_at as f64)
            .bind(authz.expires_at as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_authz_state insert: {e}")))?;
            Ok(())
        }

        async fn consume(&self, state: &str) -> Result<Option<PendingAuthz>, AuthError> {
            // DELETE ... RETURNING is the atomic one-shot: the row is gone the instant it is read,
            // so two concurrent callbacks for the same `state` cannot both succeed (the second
            // RETURNS no row). A replayed `state` therefore always finds nothing.
            let row = sqlx::query(
                "DELETE FROM public.oauth_authz_state WHERE state = $1 \
                 RETURNING state, code_verifier, provider_id, redirect_uri, cli_redirect, \
                 extract(epoch from expires_at)::float8 AS expires_at_epoch",
            )
            .bind(state)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AuthError::Backend(format!("oauth_authz_state consume: {e}")))?;
            row.as_ref().map(row_to_pending).transpose()
        }

        async fn purge_expired(&self, now: u64) -> Result<u64, AuthError> {
            let res = sqlx::query("DELETE FROM public.oauth_authz_state WHERE expires_at < to_timestamp($1)")
                .bind(now as f64)
                .execute(&self.pool)
                .await
                .map_err(|e| AuthError::Backend(format!("oauth_authz_state purge: {e}")))?;
            Ok(res.rows_affected())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        use base64::Engine as _;
        use sha2::{Digest, Sha256};
        let p = new_pkce().unwrap();
        // The verifier is a non-empty base64url string (43 chars for 32 raw bytes, no padding).
        assert_eq!(p.verifier.len(), 43);
        assert!(!p.verifier.contains('=') && !p.verifier.contains('+') && !p.verifier.contains('/'));
        // The challenge is exactly BASE64URL-NOPAD(SHA256(verifier)).
        let expect = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(p.verifier.as_bytes()));
        assert_eq!(p.challenge, expect);
    }

    #[test]
    fn each_pkce_pair_is_fresh() {
        let a = new_pkce().unwrap();
        let b = new_pkce().unwrap();
        assert_ne!(a.verifier, b.verifier);
        assert_ne!(a.challenge, b.challenge);
    }

    // --- PG-gated contract tests against a real `public.oauth_authz_state` table. ---
    //
    // No-ops when `GT_PG_URL` is unset (same gate as the other `pg` adapters). Run with
    // `--test-threads=1`.
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
            sqlx::query("SELECT pg_advisory_xact_lock(8423)")
                .execute(&mut *tx)
                .await
                .expect("advisory lock");
            sqlx::query(crate::migrations::CREATE_OAUTH_AUTHZ_STATE)
                .execute(&mut *tx)
                .await
                .expect("create oauth_authz_state table");
            tx.commit().await.expect("commit ddl tx");
        }

        fn unique(tag: &str) -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            format!("st-{tag}-{n}")
        }

        #[tokio::test]
        async fn insert_then_consume_is_one_shot() {
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgAuthzStateRepo::new(pool.clone());

            let state = unique("oneshot");
            repo.insert(NewAuthz {
                state: state.clone(),
                code_verifier: "verifier-xyz".into(),
                provider_id: "corp".into(),
                redirect_uri: "https://gt.test/auth/callback".into(),
                cli_redirect: Some("http://127.0.0.1:8976/callback".into()),
                created_at: 1_000,
                expires_at: 1_600,
            })
            .await
            .unwrap();

            // First consume returns the row with the verifier intact.
            let got = repo.consume(&state).await.unwrap().expect("present");
            assert_eq!(got.code_verifier, "verifier-xyz");
            assert_eq!(got.provider_id, "corp");
            assert_eq!(got.redirect_uri, "https://gt.test/auth/callback");
            assert_eq!(got.cli_redirect.as_deref(), Some("http://127.0.0.1:8976/callback"));
            assert_eq!(got.expires_at, 1_600);

            // Second consume finds nothing — the row was deleted on read (replay rejected).
            assert!(repo.consume(&state).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn purge_expired_removes_only_stale_rows() {
            let Some(pool) = pool_or_skip().await else {
                return;
            };
            ensure_table(&pool).await;
            let repo = PgAuthzStateRepo::new(pool.clone());

            let stale = unique("stale");
            let fresh = unique("fresh");
            repo.insert(NewAuthz {
                state: stale.clone(),
                code_verifier: "v".into(),
                provider_id: "p".into(),
                redirect_uri: "https://gt.test/cb".into(),
                cli_redirect: None,
                created_at: 10,
                expires_at: 100,
            })
            .await
            .unwrap();
            repo.insert(NewAuthz {
                state: fresh.clone(),
                code_verifier: "v".into(),
                provider_id: "p".into(),
                redirect_uri: "https://gt.test/cb".into(),
                cli_redirect: None,
                created_at: 10,
                expires_at: 9_999_999_999,
            })
            .await
            .unwrap();

            // Sweep at t=200: the stale row (exp 100) goes, the fresh one stays.
            assert!(repo.purge_expired(200).await.unwrap() >= 1);
            assert!(repo.consume(&stale).await.unwrap().is_none());
            assert!(repo.consume(&fresh).await.unwrap().is_some());
        }
    }
}
