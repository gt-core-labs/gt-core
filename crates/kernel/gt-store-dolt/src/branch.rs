//! Dolt branch deletion (epic hq-branch-gc) — drop a delivered branch ref once its bead has
//! merged to `main`. A pure SQL helper over a per-workspace pool: no domain types, kernel-tier,
//! the same grain as [`commit`](crate::commit). The decision of *when* to reap (the
//! `merge.merged.v1` reaction) lives up in the composition root; this is only the effect.

use mysql_async::prelude::*;
use mysql_async::{params, Pool};

use crate::conn::map_err;
use crate::error::AppError;

/// Drop branch `branch` from the workspace's Dolt database. Returns `true` if a branch was
/// deleted, `false` if it was already absent — so a replayed delivery (boot hydration
/// re-emitting `Merged`) or a second reaper pass is a harmless no-op, never a hard error.
///
/// Uses the safe `DOLT_BRANCH('-d', ..)` form, which *refuses* a branch not fully merged into
/// its base. A delivered branch is merged to `main` by definition, so the delete succeeds;
/// guarding against the unmerged case keeps un-landed work from being force-dropped. Existence
/// is pre-checked against the `dolt_branches` system table so an already-reaped name returns
/// `false` instead of surfacing Dolt's "branch not found" error.
pub async fn delete_branch(pool: &Pool, branch: &str) -> Result<bool, AppError> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let exists: Option<i64> = conn
        .exec_first(
            "SELECT 1 FROM dolt_branches WHERE name = :b",
            params! { "b" => branch },
        )
        .await
        .map_err(map_err)?;
    if exists.is_none() {
        return Ok(false);
    }
    conn.exec_drop("CALL DOLT_BRANCH('-d', :b)", params! { "b" => branch })
        .await
        .map_err(map_err)?;
    Ok(true)
}
