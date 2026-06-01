//! Postgres adapter for the [`FeatureFlags`](crate::FeatureFlags) port
//! (hq-mod-flags.4). Compiled only under the `pg` feature.
//!
//! Backs the override-only store with the `flag_overrides` table created by the
//! `gt-store-pg` migration (`hq-mod-flags.7`). It sits beside
//! [`InMemoryFeatureFlags`](crate::InMemoryFeatureFlags) as another adapter of the
//! same port; the heavy `sqlx` dependency is feature-gated so the default
//! `gt-feature-flags` build stays pure.
//!
//! The table is override-only: a row is the deviation from a flag's build
//! default, so [`set_override`](FeatureFlags::set_override) upserts and
//! [`clear_override`](FeatureFlags::clear_override) deletes. The `set_by` audit
//! column defaults to empty here — attributing the toggle to an actor is a
//! follow-up that does not change this port.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::key::FlagKey;
use crate::repo::{FeatureFlags, FlagError};

/// Postgres-backed [`FeatureFlags`](crate::FeatureFlags).
///
/// Holds a [`PgPool`]; every method runs one statement against `flag_overrides`.
/// Cloning is cheap — `PgPool` is an `Arc` over the connection pool.
#[derive(Clone)]
pub struct PgFeatureFlags {
    pool: PgPool,
}

impl PgFeatureFlags {
    /// Wrap a connection pool. The `flag_overrides` table is expected to already
    /// exist (applied via the `gt-store-pg` migration at boot).
    pub fn new(pool: PgPool) -> Self {
        PgFeatureFlags { pool }
    }
}

/// Map a `sqlx` error to a [`FlagError::Backend`].
fn backend(e: sqlx::Error) -> FlagError {
    FlagError::Backend(e.to_string())
}

#[async_trait]
impl FeatureFlags for PgFeatureFlags {
    async fn get_override(
        &self,
        workspace: &str,
        key: &FlagKey,
    ) -> Result<Option<bool>, FlagError> {
        let row = sqlx::query(
            "SELECT enabled FROM flag_overrides WHERE workspace_id = $1 AND flag_key = $2",
        )
        .bind(workspace)
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        row.map(|r| r.try_get::<bool, _>("enabled").map_err(backend))
            .transpose()
    }

    async fn set_override(
        &self,
        workspace: &str,
        key: &FlagKey,
        enabled: bool,
    ) -> Result<(), FlagError> {
        // Upsert on the (workspace, key) PK: one override per pair. `since` is
        // refreshed on every write so it tracks the last toggle.
        sqlx::query(
            "INSERT INTO flag_overrides (workspace_id, flag_key, enabled) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (workspace_id, flag_key) DO UPDATE \
                SET enabled = EXCLUDED.enabled, since = now()",
        )
        .bind(workspace)
        .bind(key.as_str())
        .bind(enabled)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn clear_override(&self, workspace: &str, key: &FlagKey) -> Result<(), FlagError> {
        // Idempotent: deleting an absent override affects zero rows, no error.
        sqlx::query("DELETE FROM flag_overrides WHERE workspace_id = $1 AND flag_key = $2")
            .bind(workspace)
            .bind(key.as_str())
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn list_overrides(&self, workspace: &str) -> Result<Vec<(FlagKey, bool)>, FlagError> {
        let rows = sqlx::query(
            "SELECT flag_key, enabled FROM flag_overrides WHERE workspace_id = $1 ORDER BY flag_key",
        )
        .bind(workspace)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter()
            .map(|r| {
                let raw: String = r.try_get("flag_key").map_err(backend)?;
                let enabled: bool = r.try_get("enabled").map_err(backend)?;
                // A stored key that no longer parses means the row was written by
                // something not sharing this contract — surface it as a backend
                // inconsistency rather than silently dropping the override.
                let key = FlagKey::new(raw.clone()).map_err(|e| {
                    FlagError::Backend(format!("stored flag_key {raw:?} is invalid: {e}"))
                })?;
                Ok((key, enabled))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset
    /// so the suite is a no-op off a developer box / CI without PG.
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(PgPool::connect(&url).await.expect("GT_PG_URL must point at a reachable Postgres"))
    }

    /// Process-wide lock serializing schema setup. Two `#[tokio::test]`s in this
    /// module run in parallel; concurrent `CREATE TABLE/FUNCTION IF NOT EXISTS`
    /// from separate sessions races in the Postgres catalog, so the DDL is applied
    /// under this lock (the per-test data ops afterwards stay parallel-safe via
    /// distinct workspace ids).
    fn schema_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    /// Apply the workspaces + flag_overrides migrations (idempotent) and ensure
    /// the FK target workspace row exists.
    async fn ensure_schema(pool: &PgPool, ws: &str) {
        let _guard = schema_lock().lock().await;
        // Migrations are multi-statement (table + seed, or a function body), so
        // run them unprepared — a prepared statement rejects multiple commands.
        for m in gt_store_pg::workspace_migrations() {
            sqlx::raw_sql(&m.sql).execute(pool).await.expect("apply workspaces migration");
        }
        for m in gt_store_pg::feature_flags_migrations() {
            sqlx::raw_sql(&m.sql).execute(pool).await.expect("apply flag_overrides migration");
        }
        // The override table FKs workspaces.id; seed the test tenant.
        sqlx::query(
            "INSERT INTO workspaces (id, slug, name, status) VALUES ($1, $1, $1, 'active') \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(ws)
        .execute(pool)
        .await
        .expect("seed test workspace");
    }

    fn key(s: &str) -> FlagKey {
        FlagKey::new(s).unwrap()
    }

    #[tokio::test]
    async fn set_get_clear_round_trip() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgFeatureFlags contract test");
            return;
        };
        let ws = "flags-pg-test";
        ensure_schema(&pool, ws).await;
        let repo = PgFeatureFlags::new(pool.clone());

        // Clean slate for a repeatable run.
        sqlx::query("DELETE FROM flag_overrides WHERE workspace_id = $1")
            .bind(ws)
            .execute(&pool)
            .await
            .unwrap();

        // Absent → None.
        assert_eq!(repo.get_override(ws, &key("module.beads")).await.unwrap(), None);

        // Set false, read it back, then upsert to true.
        repo.set_override(ws, &key("module.beads"), false).await.unwrap();
        assert_eq!(repo.get_override(ws, &key("module.beads")).await.unwrap(), Some(false));
        repo.set_override(ws, &key("module.beads"), true).await.unwrap();
        assert_eq!(repo.get_override(ws, &key("module.beads")).await.unwrap(), Some(true));

        // List is scoped + ordered.
        repo.set_override(ws, &key("module.rigs"), true).await.unwrap();
        let listed = repo.list_overrides(ws).await.unwrap();
        assert_eq!(
            listed,
            vec![(key("module.beads"), true), (key("module.rigs"), true)]
        );

        // Clear reverts (deletes) the override; clearing again is a no-op.
        repo.clear_override(ws, &key("module.beads")).await.unwrap();
        assert_eq!(repo.get_override(ws, &key("module.beads")).await.unwrap(), None);
        repo.clear_override(ws, &key("module.beads")).await.unwrap();

        // Cleanup.
        sqlx::query("DELETE FROM flag_overrides WHERE workspace_id = $1")
            .bind(ws)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn overrides_are_workspace_scoped() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgFeatureFlags contract test");
            return;
        };
        // Distinct workspace from the round-trip test so the two can run in
        // parallel without clobbering each other's overrides.
        let ws = "flags-pg-scope-test";
        ensure_schema(&pool, ws).await;
        let repo = PgFeatureFlags::new(pool.clone());
        sqlx::query("DELETE FROM flag_overrides WHERE workspace_id = $1")
            .bind(ws)
            .execute(&pool)
            .await
            .unwrap();

        repo.set_override(ws, &key("module.beads"), false).await.unwrap();
        // The bootstrap `default` workspace sees no override for the same key.
        assert_eq!(repo.get_override("default", &key("module.beads")).await.unwrap(), None);

        sqlx::query("DELETE FROM flag_overrides WHERE workspace_id = $1")
            .bind(ws)
            .execute(&pool)
            .await
            .unwrap();
    }
}
