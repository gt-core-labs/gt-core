//! Postgres-backed [`PatStore`](crate::pat) for Personal Access Tokens (`hq-security-pat.1`).
//! Compiled only under the `pg` feature.
//!
//! This is the durable store the PAT verifier and the self-service `/auth/tokens` surface sit on:
//! mint a clamped token, list a user's own tokens, revoke one, and verify a presented token into a
//! [`JwtClaims`]. It mirrors [`PgRefreshStore`](crate::PgRefreshStore) in shape and reuses the same
//! security posture — the opaque secret is stored only as its SHA-256 hash, and the adapter exposes
//! inherent `async` methods (not a sync trait) because every call is a Postgres round-trip.
//!
//! ## The secret is never stored
//!
//! The opaque token is high-entropy (256 bits), so a fast deterministic **SHA-256** of it is the
//! stored key (`token_hash`, the primary key) — no salt, because there is nothing to brute-force
//! and a deterministic hash is what an index lookup needs. A database leak yields no usable bearer
//! tokens.
//!
//! ## Workspace isolation (docs/03 Rule 6, docs/04 §15)
//!
//! `personal_access_tokens` is per-workspace projection data in each tenant's `ws_<slug>` schema
//! (migration `0008`, defined once in the `ws_default` template). This adapter issues UNQUALIFIED
//! statements and relies on the connection's `search_path` being scoped to the workspace schema —
//! exactly what [`WorkspacePool`](gt_store_pg) does on checkout — so a store built over a
//! workspace-scoped pool touches only that tenant's rows.
//!
//! ## Clamp at mint — no privilege escalation
//!
//! [`mint`](PgPatStore::mint) takes both the *requested* scopes and the minter's *granted* scopes
//! and stores their intersection ([`clamp_scopes`](crate::pat::clamp_scopes)), so a PAT is always a
//! subset of the authority that created it. The store is the chokepoint: the REST handler passes
//! the caller's own claim scopes as `granted`, and the store guarantees the row can hold nothing
//! more.

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::pat::{clamp_scopes, PatError, PatId, PatRecord, PatStatus, PatToken};
use crate::JwtClaims;

/// Postgres-backed Personal Access Token store: mint / list / revoke / verify against the
/// per-workspace `personal_access_tokens` table.
///
/// Holds a [`PgPool`] whose connections are `search_path`-scoped to one workspace schema (build it
/// from `WorkspacePool::pool()`). Cloning is cheap: `PgPool` is an `Arc` over the connection pool.
#[derive(Clone, Debug)]
pub struct PgPatStore {
    pool: PgPool,
}

impl PgPatStore {
    /// Wrap a workspace-scoped connection `pool`. The `personal_access_tokens` table is expected to
    /// already exist in the pool's schema (provisioned via migration `0008` +
    /// `gt_create_workspace_schema`).
    pub fn new(pool: PgPool) -> Self {
        PgPatStore { pool }
    }

    /// Mint a new PAT for `sub`/`workspace` labelled `name`, granting the intersection of
    /// `requested` scopes and the minter's `granted` scopes (so it can never escalate), valid until
    /// `expires_at` (`None` ⇒ never) and created at `now`. Returns the opaque secret to hand the
    /// user **once** plus its server-side [`PatRecord`]. A backend fault is [`PatError::Backend`].
    #[allow(clippy::too_many_arguments)]
    pub async fn mint(
        &self,
        sub: &str,
        workspace: &str,
        name: &str,
        requested: &[String],
        granted: &[String],
        now: u64,
        expires_at: Option<u64>,
    ) -> Result<(PatToken, PatRecord), PatError> {
        let record = PatRecord {
            id: PatId::generate(),
            sub: sub.to_owned(),
            workspace: workspace.to_owned(),
            name: name.to_owned(),
            scopes: clamp_scopes(requested, granted),
            created_at: now,
            expires_at,
            last_used_at: None,
            status: PatStatus::Active,
        };
        let token = PatToken::generate();
        sqlx::query(
            "INSERT INTO personal_access_tokens \
                 (token_hash, id, sub, workspace, name, scopes, created_at, expires_at, last_used_at, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9)",
        )
        .bind(token_hash(&token))
        .bind(record.id.as_str())
        .bind(&record.sub)
        .bind(&record.workspace)
        .bind(&record.name)
        .bind(&record.scopes)
        .bind(record.created_at as i64)
        .bind(record.expires_at.map(|e| e as i64))
        .bind(record.status.as_str())
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok((token, record))
    }

    /// List `sub`'s own tokens (self-only — keyed by subject), newest first, **without** any
    /// secret (the hash is never returned). Empty ⇒ the user has minted none.
    pub async fn list(&self, sub: &str) -> Result<Vec<PatRecord>, PatError> {
        let rows = sqlx::query(
            "SELECT id, sub, workspace, name, scopes, created_at, expires_at, last_used_at, status \
             FROM personal_access_tokens WHERE sub = $1 ORDER BY created_at DESC, id",
        )
        .bind(sub)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(row_to_record).collect()
    }

    /// Revoke `sub`'s token addressed by `id` (self-only: the `sub` predicate means a caller can
    /// only revoke their OWN tokens, never another user's by guessing an id). Idempotent in spirit:
    /// returns `Ok(true)` when an active row was flipped to revoked, `Ok(false)` when no such
    /// active token exists for that `sub` (unknown id, or already revoked) — the handler maps that
    /// to `404`.
    pub async fn revoke(&self, sub: &str, id: &str) -> Result<bool, PatError> {
        let res = sqlx::query(
            "UPDATE personal_access_tokens SET status = 'revoked' \
             WHERE id = $1 AND sub = $2 AND status = 'active'",
        )
        .bind(id)
        .bind(sub)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(res.rows_affected() > 0)
    }

    /// Verify a presented `token` against the injected clock `now`, resolving it to the
    /// [`JwtClaims`] the auth middleware injects — so a PAT authenticates a request exactly as a
    /// session JWT does, carrying the token's clamped scopes.
    ///
    /// - active & unexpired → stamp `last_used_at = now` and return claims (`sub`/`workspace`/
    ///   `scopes` from the row; `exp` = the PAT's own expiry, or a far-future sentinel for a
    ///   never-expiring token, so the downstream [`JwtClaims::validate`] clock gate passes).
    /// - revoked → [`PatError::Revoked`].
    /// - expired → [`PatError::Expired`].
    /// - unknown → [`PatError::Unknown`].
    pub async fn verify(&self, token: &PatToken, now: u64) -> Result<JwtClaims, PatError> {
        let hash = token_hash(token);
        let row = sqlx::query(
            "SELECT sub, workspace, scopes, expires_at, status \
             FROM personal_access_tokens WHERE token_hash = $1",
        )
        .bind(&hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;

        let Some(row) = row else {
            return Err(PatError::Unknown);
        };
        let status: String = try_col(&row, "status")?;
        match PatStatus::from_wire(&status) {
            Some(PatStatus::Revoked) => return Err(PatError::Revoked),
            Some(PatStatus::Active) => {}
            None => {
                return Err(PatError::Backend(format!(
                    "personal_access_tokens row has unknown status {status:?}"
                )))
            }
        }
        let expires_at: Option<i64> = try_col(&row, "expires_at")?;
        if let Some(exp) = expires_at {
            if now >= exp as u64 {
                return Err(PatError::Expired);
            }
        }
        let sub: String = try_col(&row, "sub")?;
        let workspace: String = try_col(&row, "workspace")?;
        let scopes: Vec<String> = try_col(&row, "scopes")?;

        // Best-effort usage stamp: a write fault here must not deny an otherwise-valid token, so a
        // failure is swallowed (the verdict is already decided). Touch only the matched row.
        let _ = sqlx::query(
            "UPDATE personal_access_tokens SET last_used_at = $1 WHERE token_hash = $2",
        )
        .bind(now as i64)
        .bind(&hash)
        .execute(&self.pool)
        .await;

        // Synthesize the claims. `exp` is the PAT's own expiry; a never-expiring PAT gets a
        // far-future sentinel so the stateless clock gate (`JwtClaims::validate`) never trips on it
        // — revocation, not expiry, is what kills such a token, and that is checked above by lookup.
        let exp = expires_at.map(|e| e as u64).unwrap_or(NEVER_EXPIRES);
        Ok(JwtClaims {
            sub,
            workspace,
            scopes,
            exp,
            nbf: None,
            iat: now,
        })
    }
}

/// The `exp` stamped on the synthesized claims of a never-expiring PAT: far enough out that the
/// downstream clock gate always passes, so such a token is killed only by revocation (a store
/// lookup), never by the stateless expiry check. ~ year 2286.
const NEVER_EXPIRES: u64 = 10_000_000_000;

/// The stored key for a token: SHA-256 of the opaque secret, lowercase hex. Deterministic so an
/// index lookup resolves it; unsalted because the token is already high-entropy (mirrors
/// `refresh_pg::token_hash`).
fn token_hash(token: &PatToken) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(token.as_str().as_bytes());
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Build a [`PatRecord`] from a listed row (no secret column is selected). A column-read or
/// unknown-status fault is a [`PatError::Backend`] (an outage / corrupt row, not a token verdict).
fn row_to_record(row: &PgRow) -> Result<PatRecord, PatError> {
    let status_str: String = try_col(row, "status")?;
    let status = PatStatus::from_wire(&status_str).ok_or_else(|| {
        PatError::Backend(format!(
            "personal_access_tokens row has unknown status {status_str:?}"
        ))
    })?;
    let expires_at: Option<i64> = try_col(row, "expires_at")?;
    let last_used_at: Option<i64> = try_col(row, "last_used_at")?;
    let created_at: i64 = try_col(row, "created_at")?;
    Ok(PatRecord {
        id: PatId::new(try_col::<String>(row, "id")?),
        sub: try_col(row, "sub")?,
        workspace: try_col(row, "workspace")?,
        name: try_col(row, "name")?,
        scopes: try_col(row, "scopes")?,
        created_at: created_at as u64,
        expires_at: expires_at.map(|e| e as u64),
        last_used_at: last_used_at.map(|e| e as u64),
        status,
    })
}

/// Read a column, mapping a decode/IO failure to [`PatError::Backend`].
fn try_col<'r, T>(row: &'r PgRow, col: &str) -> Result<T, PatError>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(col).map_err(backend)
}

/// Map a sqlx error to the backend fault variant.
fn backend(e: sqlx::Error) -> PatError {
    PatError::Backend(format!("personal_access_tokens postgres: {e}"))
}

/// The self-service `/auth/tokens` port (hq-security-pat.2): the same `PgPatStore` that backs the
/// PAT verifier also drives the mint/list/revoke surface, so one adapter over one workspace pool
/// serves both the request-time verify and the user-facing administration. Gated with the `axum`
/// HTTP surface that defines the port.
#[cfg(feature = "axum")]
#[async_trait::async_trait]
impl crate::http::PatAdmin for PgPatStore {
    #[allow(clippy::too_many_arguments)]
    async fn mint(
        &self,
        sub: &str,
        workspace: &str,
        name: &str,
        requested: &[String],
        granted: &[String],
        now: u64,
        expires_at: Option<u64>,
    ) -> Result<(PatToken, PatRecord), PatError> {
        PgPatStore::mint(
            self, sub, workspace, name, requested, granted, now, expires_at,
        )
        .await
    }

    async fn list(&self, sub: &str) -> Result<Vec<PatRecord>, PatError> {
        PgPatStore::list(self, sub).await
    }

    async fn revoke(&self, sub: &str, id: &str) -> Result<bool, PatError> {
        PgPatStore::revoke(self, sub, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset so the suite is a
    /// no-op off a developer box / CI without PG (same gate as the other `pg` adapters).
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(
            PgPool::connect(&url)
                .await
                .expect("GT_PG_URL must point at a reachable Postgres"),
        )
    }

    /// Provision an unqualified `personal_access_tokens` mirror in the connection's current schema.
    /// The migration targets `ws_default`; the contract tests run against the default `search_path`,
    /// so create an unqualified copy to exercise the adapter's unqualified statements. Serialized by
    /// a transaction-scoped advisory lock so two concurrent first-runs can't race the DDL (the same
    /// guard `PgRefreshStore`'s tests use, a distinct lock key).
    async fn ensure_table(pool: &PgPool) {
        let mut tx = pool.begin().await.expect("begin ddl tx");
        sqlx::query("SELECT pg_advisory_xact_lock(8422)")
            .execute(&mut *tx)
            .await
            .expect("advisory lock");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS personal_access_tokens ( \
                token_hash TEXT PRIMARY KEY, \
                id TEXT NOT NULL, \
                sub TEXT NOT NULL, \
                workspace TEXT NOT NULL, \
                name TEXT NOT NULL, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                created_at BIGINT NOT NULL, \
                expires_at BIGINT, \
                last_used_at BIGINT, \
                status TEXT NOT NULL )",
        )
        .execute(&mut *tx)
        .await
        .expect("create personal_access_tokens table");
        tx.commit().await.expect("commit ddl tx");
    }

    // --- PG-free test: hashing is the one piece of store logic that runs without a database. ---

    #[test]
    fn token_hash_is_deterministic_64_hex_and_hides_the_secret() {
        let tok = PatToken::new("gtpat_super-secret-opaque-value");
        let a = token_hash(&tok);
        let b = token_hash(&tok);
        assert_eq!(a, b, "hash must be deterministic for index lookups");
        assert_eq!(a.len(), 64, "sha-256 hex is 64 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(
            !a.contains("super-secret"),
            "the secret must not leak into its hash"
        );
    }

    // --- GT_PG_URL-gated contract tests against a live Postgres. ---

    #[tokio::test]
    async fn mint_clamps_scopes_and_verify_resolves_claims() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgPatStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgPatStore::new(pool);

        // Granted = the minter's own scopes; requested asks for one they DON'T hold → it is dropped.
        let granted = vec!["tokens.read".to_string(), "issues.read".to_string()];
        let (token, rec) = store
            .mint(
                "alice",
                "acme",
                "ci-deploy",
                &["tokens.read".into(), "tokens.write".into()],
                &granted,
                100,
                Some(100 + HOUR),
            )
            .await
            .unwrap();
        assert!(token.as_str().starts_with("gtpat_"));
        assert_eq!(
            rec.scopes,
            vec!["tokens.read".to_string()],
            "escalation clamped away"
        );
        assert_eq!(rec.workspace, "acme");

        // Verify the very token mint handed out → claims carrying the clamped scopes.
        let claims = store
            .verify(&token, 200)
            .await
            .expect("verify a fresh token");
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.workspace, "acme");
        assert_eq!(claims.scopes, vec!["tokens.read".to_string()]);
        assert_eq!(claims.exp, 100 + HOUR, "exp is the PAT's own expiry");
    }

    #[tokio::test]
    async fn an_unknown_token_is_rejected() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgPatStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgPatStore::new(pool);
        let stranger = PatToken::from_bytes(&[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(store.verify(&stranger, 0).await, Err(PatError::Unknown));
    }

    #[tokio::test]
    async fn an_expired_token_is_rejected() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgPatStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgPatStore::new(pool);
        let (token, _) = store
            .mint(
                "bob",
                "acme",
                "short",
                &[],
                &["tokens.read".into()],
                0,
                Some(100),
            )
            .await
            .unwrap();
        // exp is exclusive: now == exp is already expired.
        assert_eq!(store.verify(&token, 100).await, Err(PatError::Expired));
        assert_eq!(store.verify(&token, 101).await, Err(PatError::Expired));
        // Inside the window it still verifies.
        assert!(store.verify(&token, 99).await.is_ok());
    }

    #[tokio::test]
    async fn a_never_expiring_token_verifies_far_into_the_future() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgPatStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgPatStore::new(pool);
        let (token, _) = store
            .mint(
                "eve",
                "acme",
                "forever",
                &[],
                &["tokens.read".into()],
                0,
                None,
            )
            .await
            .unwrap();
        let claims = store
            .verify(&token, 9_000_000_000)
            .await
            .expect("no expiry → always valid");
        // The synthesized claims still pass the downstream stateless clock gate.
        assert_eq!(claims.validate(9_000_000_000, false), Ok(()));
    }

    #[tokio::test]
    async fn list_is_self_only_and_revoke_kills_a_token() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgPatStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgPatStore::new(pool);

        // Two users mint tokens; a list keyed by `sub` shows only that user's own.
        let (carol_tok, carol_rec) = store
            .mint(
                "carol",
                "acme",
                "one",
                &[],
                &["tokens.read".into()],
                10,
                None,
            )
            .await
            .unwrap();
        let (_dave_tok, _) = store
            .mint(
                "dave",
                "acme",
                "two",
                &[],
                &["tokens.read".into()],
                11,
                None,
            )
            .await
            .unwrap();

        let carol_list = store.list("carol").await.unwrap();
        assert!(carol_list.iter().any(|r| r.id == carol_rec.id));
        assert!(
            carol_list.iter().all(|r| r.sub == "carol"),
            "list is self-only — never another user's tokens"
        );

        // Revoke is self-only: dave cannot revoke carol's token by id (no row for that sub).
        assert!(
            !store.revoke("dave", carol_rec.id.as_str()).await.unwrap(),
            "a user cannot revoke another's token"
        );
        assert!(
            store.verify(&carol_tok, 20).await.is_ok(),
            "still active after a foreign revoke"
        );

        // The owner revokes it → verify now rejects, and a second revoke is a no-op 404.
        assert!(store.revoke("carol", carol_rec.id.as_str()).await.unwrap());
        assert_eq!(store.verify(&carol_tok, 30).await, Err(PatError::Revoked));
        assert!(!store.revoke("carol", carol_rec.id.as_str()).await.unwrap());
    }
}
