//! Per-workspace Dolt connection pools (hq-mt-data.7).
//!
//! The storage model is **DB-per-workspace**: tenant `ws` lives in Dolt
//! database `hq_<ws>` (see [`crate::dolt_db_name`]). Each workspace therefore
//! needs its own `mysql_async::Pool` whose connections default to that
//! database. [`WorkspacePools`] is the lock-free registry of those pools: one
//! shared Dolt server endpoint in, one pool per workspace out, created lazily
//! on first access and capped per workspace.
//!
//! Like the rest of this kernel crate it is **slug-typed, not
//! [`WorkspaceId`](../../gt-workspace)-typed**: `gt-store-dolt` may not depend
//! on the platform tier (docs/03 Rule 4). The host holds the `WorkspaceId` and
//! passes its validated slug down; the slug is re-validated here before it is
//! used to build a database name, mirroring [`crate::create_workspace_dolt`].

use dashmap::DashMap;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};

use crate::conn::map_err;
use crate::error::AppError;
use crate::workspace::{dolt_db_name, validate_slug};

/// Default ceiling on simultaneously-open connections per workspace pool. Dolt
/// is single-process and optimistic; a modest per-tenant cap keeps one noisy
/// workspace from starving the server's connection budget.
pub const DEFAULT_MAX_CONNS_PER_WS: usize = 10;

/// Lock-free registry of per-workspace Dolt connection pools.
///
/// Holds the shared server endpoint (host/port/credentials, parsed once) and a
/// [`DashMap`] of `slug -> Pool`. Pools are created lazily on first
/// [`pool_for`](WorkspacePools::pool_for) and reused thereafter. `Pool` is
/// itself an `Arc`-backed handle, so cloning one out of the map is cheap.
pub struct WorkspacePools {
    /// Server coordinates parsed from the base URL. The `db_name` carried here
    /// is ignored — each per-workspace pool overrides it with `hq_<ws>`.
    base: Opts,
    /// Per-workspace ceiling on open connections.
    max_conns: usize,
    /// `slug -> Pool`, populated lazily.
    pools: DashMap<String, Pool>,
}

impl WorkspacePools {
    /// Build a registry from a base Dolt URL, e.g.
    /// `mysql://gastown@127.0.0.1:3307/`. Only the server coordinates and
    /// credentials are taken from the URL; any database component is replaced
    /// per workspace. Uses [`DEFAULT_MAX_CONNS_PER_WS`].
    pub fn from_url(base_url: &str) -> Result<Self, AppError> {
        Self::with_max_conns(base_url, DEFAULT_MAX_CONNS_PER_WS)
    }

    /// Like [`from_url`](WorkspacePools::from_url) with an explicit per-workspace
    /// connection ceiling. `max_conns` is clamped to at least 1.
    pub fn with_max_conns(base_url: &str, max_conns: usize) -> Result<Self, AppError> {
        let base = Opts::from_url(base_url)
            .map_err(|e| AppError::Other(format!("dolt base url: {e}")))?;
        Ok(Self {
            base,
            max_conns: max_conns.max(1),
            pools: DashMap::new(),
        })
    }

    /// Get (or lazily create) the connection pool for workspace `ws`.
    ///
    /// The first call for a given slug parses nothing new beyond the database
    /// name and inserts a fresh pool; later calls clone the cached handle. The
    /// slug is validated first so a malformed tenant id can never select a
    /// surprising database. Concurrent first-callers race on the [`DashMap`]
    /// entry; the loser drops its just-built pool and takes the winner's — at
    /// most a wasted (un-connected) pool, never two live pools for one slug.
    pub fn pool_for(&self, ws: &str) -> Result<Pool, AppError> {
        validate_slug(ws)?;
        if let Some(p) = self.pools.get(ws) {
            return Ok(p.clone());
        }
        let pool = self.build_pool(ws);
        let entry = self
            .pools
            .entry(ws.to_owned())
            .or_insert(pool);
        Ok(entry.clone())
    }

    /// Construct (but do not connect) a pool whose connections default to the
    /// `hq_<ws>` database, capped at `max_conns`. `mysql_async` pools are lazy,
    /// so no socket opens until the pool is first used.
    fn build_pool(&self, ws: &str) -> Pool {
        let db = dolt_db_name(ws);
        // `new(0, max)` is always valid (min <= max), so the unwrap cannot fire.
        let constraints = PoolConstraints::new(0, self.max_conns)
            .expect("0 <= max_conns after clamp");
        let opts = OptsBuilder::from_opts(self.base.clone())
            .db_name(Some(db))
            .pool_opts(PoolOpts::default().with_constraints(constraints));
        Pool::new(opts)
    }

    /// Health-probe a workspace's pool with a `SELECT 1`. Lazily creates the
    /// pool if absent, then forces a real connection round-trip, surfacing a
    /// dead/unreachable Dolt server or a missing `hq_<ws>` database as an error.
    pub async fn health(&self, ws: &str) -> Result<(), AppError> {
        use mysql_async::prelude::Queryable;
        let pool = self.pool_for(ws)?;
        let mut conn = pool.get_conn().await.map_err(map_err)?;
        let one: Option<i64> = conn.query_first("SELECT 1").await.map_err(map_err)?;
        match one {
            Some(1) => Ok(()),
            other => Err(AppError::Other(format!(
                "health probe for workspace {ws:?} returned {other:?}"
            ))),
        }
    }

    /// Number of workspaces with a live pool.
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Whether no pools have been created yet.
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    /// Drop a workspace's pool from the registry, disconnecting it in the
    /// background. Next access lazily recreates it. Returns `true` if a pool was
    /// present. Use when a tenant is deprovisioned or a pool is known stale.
    pub fn evict(&self, ws: &str) -> bool {
        match self.pools.remove(ws) {
            Some((_, pool)) => {
                // Disconnect is async; spawn-free fire-and-forget is not
                // available in a kernel crate, so we drop the handle — the
                // pool's own Drop tears down idle connections.
                drop(pool);
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pools() -> WorkspacePools {
        WorkspacePools::from_url("mysql://root@127.0.0.1:3307/").unwrap()
    }

    #[test]
    fn rejects_bad_base_url() {
        assert!(WorkspacePools::from_url("not a url").is_err());
    }

    #[test]
    fn max_conns_clamped_to_one() {
        let p = WorkspacePools::with_max_conns("mysql://root@127.0.0.1:3307/", 0).unwrap();
        assert_eq!(p.max_conns, 1);
    }

    #[test]
    fn pool_for_validates_slug() {
        let p = pools();
        assert!(p.pool_for("Bad_Slug").is_err());
        assert!(p.pool_for("").is_err());
        assert!(p.is_empty(), "rejected slugs must not create pools");
    }

    #[test]
    fn pool_for_is_lazy_and_cached() {
        let p = pools();
        assert!(p.is_empty());
        // Building a pool opens no socket, so this is offline-safe.
        let _ = p.pool_for("acme").unwrap();
        assert_eq!(p.len(), 1);
        // Second access reuses the same entry.
        let _ = p.pool_for("acme").unwrap();
        assert_eq!(p.len(), 1);
        let _ = p.pool_for("beta").unwrap();
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn evict_removes_pool() {
        let p = pools();
        let _ = p.pool_for("acme").unwrap();
        assert!(p.evict("acme"));
        assert!(!p.evict("acme"));
        assert!(p.is_empty());
    }
}
