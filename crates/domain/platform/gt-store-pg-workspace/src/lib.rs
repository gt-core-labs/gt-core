//! Postgres adapter for the `gt-workspace` [`WorkspaceRepository`] port
//! (`hq-mt-core.6`).
//!
//! The hexagonal outer adapter: it depends on the `gt-workspace` domain crate
//! (port + types) and on `sqlx`, and backs the port with the `workspaces` table
//! created by the `gt-store-pg` migration (`hq-mt-core.5`). Because it references
//! domain types it lives in `domain/platform`, not `kernel` — a kernel crate may
//! not depend on a domain crate (docs/03 Rule 4). The bead's `surface_json`
//! pointed at `crates/kernel/gt-store-pg`; that placement is unreachable under
//! the dependency rule (see gap `arch.hq-mt-core.6.store-pg-tier-placement`).
//!
//! `WorkspaceEntry` carries no slug of its own, but the table's `slug` column is
//! `NOT NULL UNIQUE`; since [`WorkspaceId`] is itself a validated DNS-label slug,
//! the adapter writes `slug = id`.

#![forbid(unsafe_code)]

use async_trait::async_trait;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use gt_workspace::{RepoError, WorkspaceEntry, WorkspaceId, WorkspaceRepository, WorkspaceStatus};

/// Postgres-backed [`WorkspaceRepository`].
///
/// Holds a [`PgPool`]; every method runs one statement against the `workspaces`
/// table. Cloning is cheap — `PgPool` is an `Arc` over the connection pool.
#[derive(Clone)]
pub struct PgWorkspaces {
    pool: PgPool,
}

impl PgWorkspaces {
    /// Wrap a connection pool. The `workspaces` table is expected to already
    /// exist (applied via the `gt-store-pg` migration at boot).
    pub fn new(pool: PgPool) -> Self {
        PgWorkspaces { pool }
    }
}

/// Map a `sqlx` error to a [`RepoError::Backend`].
fn backend(e: sqlx::Error) -> RepoError {
    RepoError::Backend(e.to_string())
}

/// The persisted spelling of a [`WorkspaceStatus`] — matches the snake_case
/// serde representation and the migration's `CHECK` constraint.
fn status_to_db(status: WorkspaceStatus) -> &'static str {
    match status {
        WorkspaceStatus::Active => "active",
        WorkspaceStatus::Suspended => "suspended",
        WorkspaceStatus::Archived => "archived",
    }
}

/// Parse a persisted status string back into a [`WorkspaceStatus`].
///
/// An unrecognized value means the row was written by something that does not
/// share this contract — a store inconsistency, not a transient backend fault.
fn status_from_db(raw: &str) -> Result<WorkspaceStatus, RepoError> {
    match raw {
        "active" => Ok(WorkspaceStatus::Active),
        "suspended" => Ok(WorkspaceStatus::Suspended),
        "archived" => Ok(WorkspaceStatus::Archived),
        other => Err(RepoError::Inconsistent(format!("unknown workspace status {other:?}"))),
    }
}

/// Build a [`WorkspaceEntry`] from a `(id, name, status)` row.
fn row_to_entry(row: &PgRow) -> Result<WorkspaceEntry, RepoError> {
    let id: String = row.try_get("id").map_err(backend)?;
    let name: String = row.try_get("name").map_err(backend)?;
    let status: String = row.try_get("status").map_err(backend)?;
    Ok(WorkspaceEntry {
        id: WorkspaceId::new(id).map_err(|e| RepoError::Inconsistent(e.to_string()))?,
        name,
        status: status_from_db(&status)?,
    })
}

#[async_trait]
impl WorkspaceRepository for PgWorkspaces {
    async fn save(&self, entry: &WorkspaceEntry) -> Result<(), RepoError> {
        // Upsert by id. `slug = id` (WorkspaceId is the validated slug); on a
        // re-save only the mutable columns change.
        sqlx::query(
            "INSERT INTO workspaces (id, slug, name, status, updated_at) \
             VALUES ($1, $1, $2, $3, now()) \
             ON CONFLICT (id) DO UPDATE \
                SET name = EXCLUDED.name, status = EXCLUDED.status, updated_at = now()",
        )
        .bind(entry.id.as_str())
        .bind(&entry.name)
        .bind(status_to_db(entry.status))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn load(&self, id: &WorkspaceId) -> Result<Option<WorkspaceEntry>, RepoError> {
        let row = sqlx::query("SELECT id, name, status FROM workspaces WHERE id = $1")
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        row.as_ref().map(row_to_entry).transpose()
    }

    async fn list(&self) -> Result<Vec<WorkspaceEntry>, RepoError> {
        let rows = sqlx::query("SELECT id, name, status FROM workspaces ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        rows.iter().map(row_to_entry).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is
    /// unset so the suite is a no-op off a developer box / CI without PG.
    async fn pool_or_skip() -> Option<PgPool> {
        let url = std::env::var("GT_PG_URL").ok()?;
        Some(PgPool::connect(&url).await.expect("GT_PG_URL must point at a reachable Postgres"))
    }

    /// Apply the workspaces migration (idempotent: `CREATE TABLE IF NOT EXISTS`
    /// + `ON CONFLICT DO NOTHING` seed) so the table exists for the test.
    async fn ensure_schema(pool: &PgPool) {
        let sql = &gt_store_pg::workspace_migrations()[0].sql;
        sqlx::query(sql).execute(pool).await.expect("apply workspaces migration");
    }

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    #[tokio::test]
    async fn save_load_round_trip_and_upsert() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgWorkspaces contract test");
            return;
        };
        ensure_schema(&pool).await;
        let repo = PgWorkspaces::new(pool.clone());

        let wid = id("acme-pg-test");
        // Clean any leftover from a prior run.
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(wid.as_str())
            .execute(&pool)
            .await
            .unwrap();

        let entry = WorkspaceEntry {
            id: wid.clone(),
            name: "Acme".to_string(),
            status: WorkspaceStatus::Active,
        };
        repo.save(&entry).await.unwrap();
        assert_eq!(repo.load(&wid).await.unwrap(), Some(entry.clone()));

        // Upsert: same id, new name + status.
        let updated = WorkspaceEntry {
            name: "Acme Corp".to_string(),
            status: WorkspaceStatus::Suspended,
            ..entry
        };
        repo.save(&updated).await.unwrap();
        let loaded = repo.load(&wid).await.unwrap().unwrap();
        assert_eq!(loaded.name, "Acme Corp");
        assert_eq!(loaded.status, WorkspaceStatus::Suspended);

        // The bootstrap default row from the migration is visible via list.
        let ids: Vec<String> =
            repo.list().await.unwrap().into_iter().map(|e| e.id.as_str().to_string()).collect();
        assert!(ids.contains(&"default".to_string()), "bootstrap default present");
        assert!(ids.contains(&"acme-pg-test".to_string()));

        // Cleanup.
        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(wid.as_str())
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_absent_is_none() {
        let Some(pool) = pool_or_skip().await else {
            eprintln!("GT_PG_URL unset; skipping PgWorkspaces contract test");
            return;
        };
        ensure_schema(&pool).await;
        let repo = PgWorkspaces::new(pool);
        assert_eq!(repo.load(&id("nonexistent-ws")).await.unwrap(), None);
    }

    #[test]
    fn status_db_mapping_round_trips() {
        for s in [WorkspaceStatus::Active, WorkspaceStatus::Suspended, WorkspaceStatus::Archived] {
            assert_eq!(status_from_db(status_to_db(s)).unwrap(), s);
        }
        assert!(matches!(status_from_db("bogus"), Err(RepoError::Inconsistent(_))));
    }
}
