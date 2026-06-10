//! Postgres adapter for the [`RigRepository`](crate::RigRepository) port
//! (`hq-mcp-dispatch.3`). Compiled only under the `pg` feature.
//!
//! Backs the port with the per-workspace `rigs` table from the module migration
//! (`migrations/rig/0001__create_rigs.sql`). The table lives in each tenant's
//! `ws_<slug>` schema (docs/04 §15, schema-per-workspace): this adapter issues
//! **unqualified** `rigs` statements and relies on the connection's `search_path`
//! being set to the workspace schema — exactly what
//! [`gt_store_pg::WorkspacePool`] does on checkout. So a `PgRigs` built over a
//! workspace-scoped pool reads and writes that tenant's own copy, with no
//! `workspace_id` predicate.
//!
//! It sits beside [`InMemoryRigs`](crate::InMemoryRigs) as another adapter of the
//! same port; the heavy `sqlx` dependency is feature-gated so the default
//! `gt-rig` build stays pure.

use std::future::Future;
use std::path::PathBuf;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use gt_events::AppError;

use crate::state::RigEntry;
use crate::RigRepository;

/// Postgres-backed [`RigRepository`](crate::RigRepository).
///
/// Holds a [`PgPool`] whose connections are `search_path`-scoped to one
/// workspace schema (build it from `WorkspacePool::pool()`). Cloning is cheap —
/// `PgPool` is an `Arc` over the connection pool.
#[derive(Clone)]
pub struct PgRigs {
    pool: PgPool,
}

impl PgRigs {
    /// Wrap a workspace-scoped connection pool. The `rigs` table is expected to
    /// already exist in the pool's schema (provisioned via the module migration +
    /// `gt_create_workspace_schema`).
    pub fn new(pool: PgPool) -> Self {
        PgRigs { pool }
    }
}

/// Map a `sqlx` error onto the catch-all domain error (a backend fault, not a
/// caller fault).
fn backend(e: sqlx::Error) -> AppError {
    AppError::Other(format!("rigs postgres: {e}"))
}

/// Build a [`RigEntry`] from a `rigs` row.
fn row_to_entry(row: &PgRow) -> Result<RigEntry, AppError> {
    let registered_at: i64 = row.try_get("registered_at").map_err(backend)?;
    let worktree_root: Option<String> = row.try_get("worktree_root").map_err(backend)?;
    Ok(RigEntry {
        name: row.try_get("name").map_err(backend)?,
        prefix: row.try_get("prefix").map_err(backend)?,
        git_url: row.try_get("git_url").map_err(backend)?,
        push_url: row.try_get("push_url").map_err(backend)?,
        upstream_url: row.try_get("upstream_url").map_err(backend)?,
        default_branch: row.try_get("default_branch").map_err(backend)?,
        registered_at_secs: registered_at as u64,
        worktree_root: worktree_root.map(PathBuf::from),
        git_connection_ref: row.try_get("git_connection_ref").map_err(backend)?,
    })
}

impl RigRepository for PgRigs {
    fn upsert(&self, entry: &RigEntry) -> impl Future<Output = Result<(), AppError>> + Send {
        let pool = self.pool.clone();
        let entry = entry.clone();
        async move {
            // Upsert by name (the catalog identity). On re-save the mutable
            // columns change; `registered_at` is preserved from the first insert.
            // Path → TEXT. A worktree root always arrives as a UTF-8 JSON string, so `to_str`
            // is `Some`; a (pathological) non-UTF-8 path stores NULL rather than corrupting.
            let worktree_root = entry
                .worktree_root
                .as_ref()
                .and_then(|p| p.to_str())
                .map(str::to_string);
            sqlx::query(
                "INSERT INTO rigs \
                   (name, prefix, git_url, push_url, upstream_url, default_branch, registered_at, \
                    worktree_root, git_connection_ref) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (name) DO UPDATE SET \
                   prefix = EXCLUDED.prefix, \
                   git_url = EXCLUDED.git_url, \
                   push_url = EXCLUDED.push_url, \
                   upstream_url = EXCLUDED.upstream_url, \
                   default_branch = EXCLUDED.default_branch, \
                   worktree_root = EXCLUDED.worktree_root, \
                   git_connection_ref = EXCLUDED.git_connection_ref",
            )
            .bind(&entry.name)
            .bind(&entry.prefix)
            .bind(&entry.git_url)
            .bind(&entry.push_url)
            .bind(&entry.upstream_url)
            .bind(&entry.default_branch)
            .bind(entry.registered_at_secs as i64)
            .bind(&worktree_root)
            .bind(&entry.git_connection_ref)
            .execute(&pool)
            .await
            .map_err(backend)?;
            Ok(())
        }
    }

    fn remove(&self, name: &str) -> impl Future<Output = Result<bool, AppError>> + Send {
        let pool = self.pool.clone();
        let name = name.to_string();
        async move {
            let res = sqlx::query("DELETE FROM rigs WHERE name = $1")
                .bind(&name)
                .execute(&pool)
                .await
                .map_err(backend)?;
            Ok(res.rows_affected() > 0)
        }
    }

    fn get(&self, name: &str) -> impl Future<Output = Result<Option<RigEntry>, AppError>> + Send {
        let pool = self.pool.clone();
        let name = name.to_string();
        async move {
            let row = sqlx::query(
                "SELECT name, prefix, git_url, push_url, upstream_url, default_branch, \
                        registered_at, worktree_root, git_connection_ref \
                 FROM rigs WHERE name = $1",
            )
            .bind(&name)
            .fetch_optional(&pool)
            .await
            .map_err(backend)?;
            row.as_ref().map(row_to_entry).transpose()
        }
    }

    fn prefix_owner(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Option<String>, AppError>> + Send {
        let pool = self.pool.clone();
        let prefix = prefix.to_string();
        async move {
            let row = sqlx::query("SELECT name FROM rigs WHERE prefix = $1")
                .bind(&prefix)
                .fetch_optional(&pool)
                .await
                .map_err(backend)?;
            row.map(|r| r.try_get::<String, _>("name").map_err(backend))
                .transpose()
        }
    }

    fn list(&self) -> impl Future<Output = Result<Vec<RigEntry>, AppError>> + Send {
        let pool = self.pool.clone();
        async move {
            let rows = sqlx::query(
                "SELECT name, prefix, git_url, push_url, upstream_url, default_branch, \
                        registered_at, worktree_root, git_connection_ref \
                 FROM rigs ORDER BY name",
            )
            .fetch_all(&pool)
            .await
            .map_err(backend)?;
            rows.iter().map(row_to_entry).collect()
        }
    }
}
