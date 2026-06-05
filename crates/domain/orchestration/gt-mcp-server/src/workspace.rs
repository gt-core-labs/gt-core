//! Per-request workspace resolution for the multi-tenant MCP server
//! (hq-mt-routing.5).
//!
//! ## Reframe note
//!
//! The bead was written against the upstream `gt-mcp` bin
//! (`McpService::with_workspace_registry(Arc<RootRegistry>)`, resolve a `Root`
//! per request). That bin was retired when gt-core took over the canonical MCP
//! endpoint (hq-core-host cutover), and gt-core's [`IssuesServer`](crate::server)
//! is not `Root`-composed — it dispatches the `issues.*` tools directly against a
//! [`DoltIssues`] store. The faithful intent of the bead, adapted to the live
//! architecture, is therefore: **resolve the per-workspace store per request
//! instead of pinning the server to one store**, mirroring how the server already
//! resolves a per-connection [`Scope`](gt_rbac::Scope) from the `X-Actor` header.
//!
//! ## Storage model
//!
//! Tenant `ws` lives in Dolt database `hq_<ws>` (`gt_store_dolt::dolt_db_name`),
//! reached through a per-workspace connection pool ([`WorkspacePools`]). This
//! resolver wraps that registry: given a workspace slug it returns a
//! [`DoltIssues`] bound to that tenant's pool. Construction is cheap — a
//! `mysql_async::Pool` is an `Arc` handle and is created lazily on first use, so
//! [`store_for`](WorkspaceStores::store_for) opens no socket until a query runs.
//!
//! Schema provisioning (`create_workspace_dolt`) is a separate concern: this
//! resolver assumes the tenant database already exists. An unknown/malformed slug
//! is rejected by [`WorkspacePools::pool_for`] before any database is selected.

use gt_store_dolt::{AppError, DoltIssues, WorkspacePools};
use rmcp::model::Extensions;

/// The transport header carrying the server-injected tenant for a request.
///
/// Per docs/04 §15 the workspace is resolved from the connection context, NEVER
/// from the MCP tool payload/body/URL — exactly like [`X-Actor`](crate::server)
/// for scope. A caller cannot reach another tenant's beads by spoofing a tool
/// argument because there is no such argument; the header is the only channel.
pub const WORKSPACE_HEADER: &str = "x-workspace";

/// Resolver from a workspace slug to that tenant's [`DoltIssues`] store.
///
/// Holds the lock-free [`WorkspacePools`] registry (one shared Dolt endpoint, one
/// lazily-created pool per workspace). Cheap to share behind an `Arc`.
pub struct WorkspaceStores {
    pools: WorkspacePools,
}

impl WorkspaceStores {
    /// Build the resolver from a base Dolt URL, e.g.
    /// `mysql://gastown@127.0.0.1:3307/`. Only the server coordinates and
    /// credentials are used; the per-workspace database (`hq_<ws>`) is selected
    /// per tenant. Fails only if the base URL is malformed.
    pub fn from_base_url(base_url: &str) -> Result<Self, AppError> {
        Ok(Self { pools: WorkspacePools::from_url(base_url)? })
    }

    /// The [`DoltIssues`] store for workspace `ws`.
    ///
    /// Validates the slug (a malformed tenant id can never select a surprising
    /// database), then wraps the tenant's lazily-created pool. Returns an owned
    /// store — `DoltIssues::new` only stores the `Arc`-backed pool handle, so this
    /// is allocation-light and opens no connection.
    pub fn store_for(&self, ws: &str) -> Result<DoltIssues, AppError> {
        Ok(DoltIssues::new(self.pools.pool_for(ws)?))
    }

    /// Number of workspaces with a live pool — the `workspaces_loaded` gauge the
    /// `/health` endpoint reports (hq-mt-routing.8).
    pub fn live_pools(&self) -> usize {
        self.pools.len()
    }

    /// Slugs of the workspaces with a live pool, sorted for a stable `/readyz`
    /// listing (hq-mt-routing.8).
    pub fn loaded_workspaces(&self) -> Vec<String> {
        let mut ws = self.pools.workspaces();
        ws.sort();
        ws
    }

    /// Health-probe a workspace's pool (`SELECT 1`), surfacing a dead Dolt server
    /// or a missing `hq_<ws>` database. Backs the per-workspace `dolt_ok` in
    /// `/readyz` (hq-mt-routing.8).
    pub async fn health(&self, ws: &str) -> Result<(), AppError> {
        self.pools.health(ws).await
    }
}

/// Extract the server-injected workspace slug from a request's extensions.
///
/// rmcp injects the HTTP [`Parts`](axum::http::request::Parts) into each call's
/// extensions, so the trimmed, non-empty [`WORKSPACE_HEADER`] value names the
/// connection's tenant. `None` when no Parts, no header, or an empty/garbled
/// value — the caller then falls back to the server's default-workspace store, so
/// a request with no header behaves exactly as the single-tenant server did.
pub fn workspace_from_ext(ext: &Extensions) -> Option<&str> {
    ext.get::<axum::http::request::Parts>()
        .and_then(|p| p.headers.get(WORKSPACE_HEADER))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext_with_header(name: &str, value: &str) -> Extensions {
        let parts = axum::http::Request::builder()
            .header(name, value)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut ext = Extensions::new();
        ext.insert(parts);
        ext
    }

    #[test]
    fn reads_x_workspace_header() {
        let ext = ext_with_header("x-workspace", "acme");
        assert_eq!(workspace_from_ext(&ext), Some("acme"));
    }

    #[test]
    fn trims_and_rejects_blank() {
        assert_eq!(workspace_from_ext(&ext_with_header("x-workspace", "  beta ")), Some("beta"));
        assert_eq!(workspace_from_ext(&ext_with_header("x-workspace", "   ")), None);
    }

    #[test]
    fn none_without_parts_or_header() {
        assert_eq!(workspace_from_ext(&Extensions::new()), None);
        assert_eq!(workspace_from_ext(&ext_with_header("x-other", "acme")), None);
    }

    #[test]
    fn from_base_url_rejects_malformed() {
        assert!(WorkspaceStores::from_base_url("not a url").is_err());
    }

    #[test]
    fn store_for_validates_slug_and_is_offline_safe() {
        let stores = WorkspaceStores::from_base_url("mysql://root@127.0.0.1:3307/").unwrap();
        // A malformed slug is rejected before any database is selected.
        assert!(stores.store_for("Bad_Slug").is_err());
        assert!(stores.store_for("").is_err());
        // A well-formed slug builds a store without opening a socket (pool lazy),
        // and is idempotent across calls (the pool is cached, reused).
        assert!(stores.store_for("acme").is_ok());
        assert!(stores.store_for("acme").is_ok());
    }
}
