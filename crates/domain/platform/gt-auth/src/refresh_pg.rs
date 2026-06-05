//! Postgres-backed [`RefreshStore`] (`hq-auth-session.1`). Compiled only under the `pg` feature.
//!
//! This is the **durable** counterpart of [`InMemoryRefreshStore`](crate::InMemoryRefreshStore):
//! the same issue / rotate / revoke contract, but persisted so logout and reuse-revocation
//! survive a process restart. The short-lived access JWT stays stateless (it simply expires);
//! only the long-lived refresh token needs a store it can be revoked in, which is exactly what
//! Postgres gives us here.
//!
//! ## The reuse race is settled by the database (RFC 6819 §5.2.2.3)
//!
//! [`InMemoryRefreshStore`](crate::InMemoryRefreshStore) closes the reuse-detection race with a
//! `Mutex`. The durable store cannot hold a process lock across tenants, so it pushes the
//! atomicity down to a single compare-and-swap statement:
//!
//! ```sql
//! UPDATE refresh_tokens SET status = 'rotated'
//!  WHERE token_hash = $1 AND status = 'active' AND expires_at > $now
//!  RETURNING family, sub, workspace, scopes, expires_at
//! ```
//!
//! Two concurrent presentations of the *same* active token both issue this `UPDATE`; the first
//! takes the row lock and flips `active → rotated`, and the second — re-evaluating its `WHERE`
//! against the now-committed row — matches nothing. So exactly one rotate succeeds. The loser
//! finds the row `rotated` and reports [`RefreshError::Reused`], revoking the whole family. The
//! security-critical decision (who gets to rotate) is made *by the CAS*, never by a
//! read-then-write gap, so there is no window for a double-spend to slip through. A failed CAS
//! is only **classified** by a follow-up `SELECT`, and that is race-free: once a row leaves
//! `active` its status is terminal-ish (`rotated`/`revoked` never return to `active`, and an
//! expired-but-active row never changes), so the rejection reason it reads is stable.
//!
//! ## The secret is never stored
//!
//! The opaque token is high-entropy (256 bits), so a fast deterministic **SHA-256** of it is the
//! stored key (column `token_hash`, the primary key) — no salt, because there is nothing to
//! brute-force and a deterministic hash is what an index lookup needs (unlike the *salted* argon2
//! used for low-entropy passwords). A database leak therefore yields no usable bearer tokens.
//!
//! ## Async — inherent methods, not the sync trait (the [`PgUsers`](crate::PgUsers) precedent)
//!
//! [`RefreshStore`] is **synchronous**: the reference adapter does only in-memory map work, so a
//! sync signature keeps callers off a runtime. A Postgres round-trip is I/O, so — exactly as
//! [`PgUsers`](crate::PgUsers) does for [`IdentityProvider`](crate::IdentityProvider) — this
//! adapter does **not** implement the sync `RefreshStore` trait; it exposes inherent `async`
//! methods (`issue` / `rotate` / `revoke_family` / `revoke_by_token`) with the same contracts.
//! Implementing the sync trait would force a `block_on` that parks a runtime thread on every DB
//! call — a footgun. The verdict vocabulary is the same [`RefreshError`], with one durable-only
//! addition: a backend/I/O fault surfaces as [`RefreshError::Backend`] (an outage, not a denied
//! token).

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::refresh::{RefreshError, RefreshId, RefreshRecord, RefreshStatus, RefreshToken};

/// Postgres-backed durable [`RefreshStore`](crate::RefreshStore): issue / rotate / revoke opaque
/// refresh tokens against the per-workspace `refresh_tokens` table.
///
/// Holds a [`PgPool`] whose connections are `search_path`-scoped to one workspace schema (build
/// it from `WorkspacePool::pool()`), so every statement is unqualified and resolves to that
/// tenant's own rows — the same isolation contract as [`PgUsers`](crate::PgUsers). Cloning is
/// cheap: `PgPool` is an `Arc` over the connection pool.
#[derive(Clone, Debug)]
pub struct PgRefreshStore {
    pool: PgPool,
}

impl PgRefreshStore {
    /// Wrap a workspace-scoped connection `pool`. The `refresh_tokens` table is expected to
    /// already exist in the pool's schema (provisioned via the module migration
    /// `migrations/auth/0002__create_refresh_tokens.sql` + `gt_create_workspace_schema`).
    pub fn new(pool: PgPool) -> Self {
        PgRefreshStore { pool }
    }

    /// Mint a new token in a **new** family for `sub`/`workspace` carrying `scopes`, valid until
    /// `exp` and issued at `issued_at` (both injected — the store reads no clock). The
    /// inherent-async counterpart of [`RefreshStore::issue`](crate::RefreshStore::issue). A
    /// backend fault is [`RefreshError::Backend`].
    pub async fn issue(
        &self,
        sub: &str,
        workspace: &str,
        scopes: &[String],
        issued_at: u64,
        exp: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        let record = RefreshRecord {
            id: RefreshId::generate(),
            // A fresh login opens a fresh family.
            family: RefreshId::generate(),
            sub: sub.to_owned(),
            workspace: workspace.to_owned(),
            scopes: scopes.to_vec(),
            issued_at,
            expires_at: exp,
            status: RefreshStatus::Active,
        };
        let token = RefreshToken::generate();
        self.insert(&token, &record).await?;
        Ok((token, record))
    }

    /// Exchange an **active** `token` for a fresh successor in the same family, against the
    /// injected clock `now`. The inherent-async counterpart of
    /// [`RefreshStore::rotate`](crate::RefreshStore::rotate), with identical verdicts:
    ///
    /// - active & unexpired → mark it `rotated`, mint a successor (same family, inheriting
    ///   `sub`/`workspace`, `issued_at = now`, same `expires_at`).
    /// - already rotated → [`RefreshError::Reused`] **and revoke the whole family** (theft).
    /// - family already revoked → [`RefreshError::Revoked`].
    /// - expired → [`RefreshError::Expired`].
    /// - unknown → [`RefreshError::Unknown`].
    pub async fn rotate(
        &self,
        token: &RefreshToken,
        now: u64,
    ) -> Result<(RefreshToken, RefreshRecord), RefreshError> {
        let hash = token_hash(token);

        // The atomic compare-and-swap: flip active → rotated only if still active AND unexpired,
        // in one statement, so the reuse race is settled by the row lock (see module docs).
        let won = sqlx::query(
            "UPDATE refresh_tokens SET status = 'rotated' \
             WHERE token_hash = $1 AND status = 'active' AND expires_at > $2 \
             RETURNING family, sub, workspace, scopes, expires_at",
        )
        .bind(&hash)
        .bind(now as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;

        if let Some(row) = won {
            // We won the CAS: this token is spent. Mint the successor in the same family,
            // inheriting the granted scopes unchanged so a refresh re-mints a faithful access.
            let family: String = try_col(&row, "family")?;
            let sub: String = try_col(&row, "sub")?;
            let workspace: String = try_col(&row, "workspace")?;
            let scopes: Vec<String> = try_col(&row, "scopes")?;
            let expires_at: i64 = try_col(&row, "expires_at")?;

            let successor = RefreshRecord {
                id: RefreshId::generate(),
                family: RefreshId::new(family),
                sub,
                workspace,
                scopes,
                issued_at: now,
                expires_at: expires_at as u64,
                status: RefreshStatus::Active,
            };
            let new_token = RefreshToken::generate();
            self.insert(&new_token, &successor).await?;
            return Ok((new_token, successor));
        }

        // The CAS matched nothing — classify WHY (race-free: a non-active status is stable).
        let existing = sqlx::query("SELECT status, family FROM refresh_tokens WHERE token_hash = $1")
            .bind(&hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;

        let Some(row) = existing else {
            // No such token: never issued, or pruned.
            return Err(RefreshError::Unknown);
        };
        let status: String = try_col(&row, "status")?;
        match status.as_str() {
            // A dead family stays dead even past exp.
            "revoked" => Err(RefreshError::Revoked),
            // Reuse of a single-use token: the theft signal. Burn the whole family down.
            "rotated" => {
                let family: String = try_col(&row, "family")?;
                self.revoke_family_str(&family).await?;
                Err(RefreshError::Reused)
            }
            // Still active but the CAS rejected it → its `expires_at > now` guard failed.
            "active" => Err(RefreshError::Expired),
            other => Err(RefreshError::Backend(format!(
                "refresh_tokens row has unknown status {other:?}"
            ))),
        }
    }

    /// Kill every token in `family` (logout / reuse response). Idempotent — revoking an
    /// already-dead or unknown family is a no-op. Inherent-async counterpart of
    /// [`RefreshStore::revoke_family`](crate::RefreshStore::revoke_family).
    pub async fn revoke_family(&self, family: &RefreshId) -> Result<(), RefreshError> {
        self.revoke_family_str(family.as_str()).await
    }

    /// Revoke the family of the session `token` belongs to — the durable **logout** path: a
    /// client presents its current refresh token and the whole session dies. Idempotent: an
    /// unknown token is a silent no-op (logout of an already-dead session is success). The
    /// in-memory store has no such helper; here it is a single indexed lookup + family sweep, and
    /// revoking the *family* (not just the one row) is what makes logout kill the session rather
    /// than leave rotated siblings alive.
    pub async fn revoke_by_token(&self, token: &RefreshToken) -> Result<(), RefreshError> {
        let hash = token_hash(token);
        let row = sqlx::query("SELECT family FROM refresh_tokens WHERE token_hash = $1")
            .bind(&hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        if let Some(row) = row {
            let family: String = try_col(&row, "family")?;
            self.revoke_family_str(&family).await?;
        }
        Ok(())
    }

    /// Insert a freshly-minted token+record pair (the secret is stored only as its hash).
    async fn insert(&self, token: &RefreshToken, record: &RefreshRecord) -> Result<(), RefreshError> {
        sqlx::query(
            "INSERT INTO refresh_tokens \
                 (token_hash, id, family, sub, workspace, scopes, issued_at, expires_at, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(token_hash(token))
        .bind(record.id.as_str())
        .bind(record.family.as_str())
        .bind(&record.sub)
        .bind(&record.workspace)
        .bind(&record.scopes)
        .bind(record.issued_at as i64)
        .bind(record.expires_at as i64)
        .bind(status_str(record.status))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    /// Mark every row sharing `family` as revoked. Idempotent.
    async fn revoke_family_str(&self, family: &str) -> Result<(), RefreshError> {
        sqlx::query("UPDATE refresh_tokens SET status = 'revoked' WHERE family = $1")
            .bind(family)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }
}

/// The stored key for a token: SHA-256 of the opaque secret, lowercase hex. Deterministic so an
/// index lookup resolves it; unsalted because the token is already high-entropy (see module docs).
fn token_hash(token: &RefreshToken) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(token.as_str().as_bytes());
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// The wire (column) spelling of a [`RefreshStatus`], matching the migration's CHECK-free TEXT.
fn status_str(status: RefreshStatus) -> &'static str {
    match status {
        RefreshStatus::Active => "active",
        RefreshStatus::Rotated => "rotated",
        RefreshStatus::Revoked => "revoked",
    }
}

/// Read a column, mapping a decode/IO failure to [`RefreshError::Backend`] (a storage fault, not
/// a token verdict).
fn try_col<'r, T>(row: &'r PgRow, col: &str) -> Result<T, RefreshError>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(col).map_err(backend)
}

/// Map a sqlx error to the durable-store fault variant.
fn backend(e: sqlx::Error) -> RefreshError {
    RefreshError::Backend(format!("refresh_tokens postgres: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset so the suite is
    /// a no-op off a developer box / CI without PG (same gate as the other `pg` adapters).
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(
            PgPool::connect(&url)
                .await
                .expect("GT_PG_URL must point at a reachable Postgres"),
        )
    }

    /// Provision an unqualified `refresh_tokens` mirror in the connection's current schema. The
    /// migration targets `ws_default`; the contract tests run against the default `search_path`,
    /// so create an unqualified copy to exercise the adapter's unqualified statements.
    ///
    /// No `TRUNCATE`: cargo runs these tests in parallel and they share one table, so wiping it
    /// would clobber a sibling mid-flight. Instead each test is isolated by its own random
    /// token/family ids — rows never collide — so the table is purely additive across the suite.
    ///
    /// `CREATE TABLE IF NOT EXISTS` is itself NOT safe against two sessions creating the same
    /// table at once (a duplicate `pg_type` row, 23505), so the create runs under a
    /// transaction-scoped advisory lock that serializes concurrent first-runs.
    async fn ensure_table(pool: &PgPool) {
        let mut tx = pool.begin().await.expect("begin ddl tx");
        sqlx::query("SELECT pg_advisory_xact_lock(8421)")
            .execute(&mut *tx)
            .await
            .expect("advisory lock");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS refresh_tokens ( \
                token_hash TEXT PRIMARY KEY, \
                id TEXT NOT NULL, \
                family TEXT NOT NULL, \
                sub TEXT NOT NULL, \
                workspace TEXT NOT NULL, \
                scopes TEXT[] NOT NULL DEFAULT '{}', \
                issued_at BIGINT NOT NULL, \
                expires_at BIGINT NOT NULL, \
                status TEXT NOT NULL )",
        )
        .execute(&mut *tx)
        .await
        .expect("create refresh_tokens table");
        tx.commit().await.expect("commit ddl tx");
    }

    // --- PG-free test: hashing is the one piece of logic that runs without a database. ---

    #[test]
    fn token_hash_is_deterministic_64_hex_and_hides_the_secret() {
        let tok = RefreshToken::new("super-secret-opaque-value");
        let a = token_hash(&tok);
        let b = token_hash(&tok);
        assert_eq!(a, b, "hash must be deterministic for index lookups");
        assert_eq!(a.len(), 64, "sha-256 hex is 64 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(!a.contains("super-secret"), "the secret must not leak into its hash");
    }

    // --- GT_PG_URL-gated contract tests against a live Postgres. ---

    #[tokio::test]
    async fn issue_then_rotate_swaps_the_token_and_invalidates_the_old_one() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let scopes = vec!["rig.read".to_string(), "rig.write".to_string()];
        let (tok, rec) = store.issue("alice", "acme", &scopes, 0, HOUR).await.unwrap();
        assert_eq!(rec.sub, "alice");
        assert_eq!(rec.workspace, "acme");
        assert_eq!(rec.scopes, scopes);
        assert_eq!(rec.status, RefreshStatus::Active);

        let (next, next_rec) = store.rotate(&tok, 10).await.unwrap();
        assert_ne!(tok, next);
        assert_eq!(next_rec.family, rec.family);
        assert_eq!(next_rec.sub, "alice");
        assert_eq!(next_rec.workspace, "acme");
        // Scopes are inherited unchanged so the re-minted access token is faithful.
        assert_eq!(next_rec.scopes, scopes);
        assert_eq!(next_rec.issued_at, 10);
        assert_eq!(next_rec.expires_at, HOUR);

        // The successor rotates fine; the chain is healthy.
        assert!(store.rotate(&next, 20).await.is_ok());
    }

    #[tokio::test]
    async fn rotating_the_same_token_twice_is_reuse_and_revokes_the_family() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let (tok, _) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();
        let (successor, _) = store.rotate(&tok, 10).await.unwrap();
        // Presenting the spent token again → reuse detected.
        assert_eq!(store.rotate(&tok, 20).await, Err(RefreshError::Reused));
        // The family is now revoked, so even the legitimate latest successor is dead.
        assert_eq!(store.rotate(&successor, 30).await, Err(RefreshError::Revoked));
    }

    #[tokio::test]
    async fn an_expired_token_cannot_be_rotated() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let (tok, _) = store.issue("alice", "acme", &[], 0, 100).await.unwrap();
        // exp is exclusive: now == exp is already expired.
        assert_eq!(store.rotate(&tok, 100).await, Err(RefreshError::Expired));
        assert_eq!(store.rotate(&tok, 101).await, Err(RefreshError::Expired));
        // Never spent (expiry short-circuits the CAS), so back inside the window it still works.
        assert!(store.rotate(&tok, 99).await.is_ok());
    }

    #[tokio::test]
    async fn an_unknown_token_is_rejected() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let stranger = RefreshToken::from_bytes(&[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(store.rotate(&stranger, 0).await, Err(RefreshError::Unknown));
    }

    #[tokio::test]
    async fn revoke_family_then_rotate_is_revoked() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let (tok, rec) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();
        store.revoke_family(&rec.family).await.unwrap();
        assert_eq!(store.rotate(&tok, 10).await, Err(RefreshError::Revoked));
        // Idempotent: a second revoke is a no-op, not an error.
        store.revoke_family(&rec.family).await.unwrap();
    }

    #[tokio::test]
    async fn revoke_by_token_kills_the_whole_session() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let (tok, _) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();
        let (successor, _) = store.rotate(&tok, 10).await.unwrap();
        // Logout by presenting the current token revokes the family — the successor dies too.
        store.revoke_by_token(&successor).await.unwrap();
        assert_eq!(store.rotate(&successor, 20).await, Err(RefreshError::Revoked));
        // Logging out an unknown token is a silent no-op.
        let stranger = RefreshToken::from_bytes(&[0x01, 0x02, 0x03]);
        store.revoke_by_token(&stranger).await.unwrap();
    }

    #[tokio::test]
    async fn a_refresh_token_survives_a_simulated_restart_new_store_same_pg() {
        // hq-platform-hardening.1: the whole point of the durable store — a refresh token issued
        // before a gt-mcp-server redeploy is STILL redeemable after it. We model the redeploy by
        // dropping the issuing store and building a brand-new `PgRefreshStore` over the same
        // Postgres (a fresh process holds no in-memory session state), then rotating the token the
        // "old" store handed out. The in-memory MVP would have lost it with the process.
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore restart-survival test");
            return;
        };
        ensure_table(&pool).await;

        // --- before the "restart": issue a token, then drop the store that minted it. ---
        let token = {
            let store = PgRefreshStore::new(pool.clone());
            let (tok, _) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();
            tok
            // `store` drops here — the issuing process is gone.
        };

        // --- after the "restart": a fresh store over the same PG, no shared in-memory state. ---
        let restarted = PgRefreshStore::new(pool);
        let (next, rec) = restarted
            .rotate(&token, 10)
            .await
            .expect("a pre-restart refresh token must still be redeemable after a new store");
        assert_eq!(rec.sub, "alice");
        assert_eq!(rec.workspace, "acme");
        assert_ne!(token, next, "the survived token rotates to a fresh successor");
        // And reuse detection still holds across the restart: the now-spent token is reuse.
        assert_eq!(restarted.rotate(&token, 20).await, Err(RefreshError::Reused));
    }

    #[tokio::test]
    async fn a_fresh_issue_opens_an_independent_family() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgRefreshStore contract test");
            return;
        };
        ensure_table(&pool).await;
        let store = PgRefreshStore::new(pool);

        let (tok_a, _) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();
        let (tok_b, rec_b) = store.issue("alice", "acme", &[], 0, HOUR).await.unwrap();

        // Trip reuse detection on session A.
        let _ = store.rotate(&tok_a, 10).await.unwrap();
        assert_eq!(store.rotate(&tok_a, 20).await, Err(RefreshError::Reused));

        // Session B is untouched and rotates cleanly.
        let (next_b, next_rec_b) = store.rotate(&tok_b, 10).await.unwrap();
        assert_eq!(next_rec_b.family, rec_b.family);
        assert_ne!(tok_b, next_b);
    }
}
