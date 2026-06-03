//! Per-workspace Postgres pool cache for the schema-isolated domain handlers
//! (`hq-mcp-dispatch.3..7`).
//!
//! The workspace catalog (`workspace.*`) lives in the shared `public` schema, but
//! every other domain's projection table is **per-workspace**: it exists once in
//! each tenant's `ws_<slug>` schema (docs/04 §15). A handler for such a domain
//! must run its queries against the caller's schema, which
//! [`gt_store_pg::WorkspacePool`] arranges by setting `search_path` on every
//! physical connection.
//!
//! [`WsPools`] caches one [`WorkspacePool`] per workspace slug so a handler does
//! not reconnect on every call. The schema + its tables are a **deploy
//! precondition** (provisioned by the migration runner + `gt_create_workspace_schema`);
//! this cache only opens scoped pools, it does not create schemas.

use std::collections::HashMap;
use std::sync::Mutex;

use gt_store_dolt::AppError;
use gt_store_pg::WorkspacePool;

/// The workspace a request resolves to when it carries no `X-Workspace` header —
/// the single-tenant default, schema `ws_default`.
const DEFAULT_WORKSPACE: &str = "default";

/// A lazily-populated cache of workspace-scoped [`WorkspacePool`]s keyed by slug.
///
/// Cheap to share: a [`WorkspacePool`] is an `Arc` over its pool, so a cached
/// clone is handed back without reconnecting.
pub struct WsPools {
    url: String,
    cache: Mutex<HashMap<String, WorkspacePool>>,
}

impl WsPools {
    /// Build the cache over a base Postgres URL (the server coordinates; the
    /// schema is selected per workspace on checkout).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// A pool scoped to `workspace` (or the default workspace when `None`).
    ///
    /// Returns a cached pool when present, otherwise connects one and caches it.
    /// A connection failure surfaces as [`AppError::Other`].
    pub async fn get(&self, workspace: Option<&str>) -> Result<WorkspacePool, AppError> {
        let slug = workspace.unwrap_or(DEFAULT_WORKSPACE);
        if let Some(pool) = self.cache.lock().unwrap().get(slug).cloned() {
            return Ok(pool);
        }
        let pool = WorkspacePool::connect(&self.url, slug)
            .await
            .map_err(|e| AppError::Other(format!("workspace pool connect ({slug}): {e}")))?;
        self.cache
            .lock()
            .unwrap()
            .insert(slug.to_string(), pool.clone());
        Ok(pool)
    }
}
