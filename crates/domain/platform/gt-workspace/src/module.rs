//! `WorkspaceModule` — the [`GtModule`] facade carrying the `workspace.*` REST
//! surface (`hq-fe-api-platform.1`). Compiled only under the `axum` feature.
//!
//! This module exists to route the workspace catalog's REST routes through the
//! kernel module seam: the binary builds it with [`with_http`](WorkspaceModule::with_http)
//! and the `RootBuilder` harvests its [`register_routes`](GtModule::register_routes)
//! / [`openapi`](GtModule::openapi) instead of the composition root hand-wiring an
//! axum router (docs/03 Rule 3).
//!
//! It contributes **only** the HTTP surface — the `workspace.*` MCP tools stay wired
//! through the orchestration-tier `WorkspaceHandler` (`gt-composition`), which the
//! catalog already serves over MCP; this facade adds the REST transport without
//! duplicating that dispatch. So `register_mcp_tools` is left at its default no-op and
//! the whole module is gated behind `axum`, keeping the dependency-light default
//! build (and the `pg`-only build) free of the module/web stack.
//!
//! ## Capability: admin-level catalog scopes (`workspace.read` / `workspace.write`)
//!
//! Unlike a per-tenant module, `workspace.*` administers the **catalog of workspaces
//! itself** — the cross-workspace lifecycle (docs/03 Rule 6), not data inside one
//! tenant — so its routes resolve no [`WorkspaceContext`](crate::WorkspaceContext)
//! (see [`crate::http`]). The two scopes it claims are therefore **operator/admin**
//! authorities over that catalog: the builder's guard maps `GET` → `workspace.read`
//! and the mutating verbs → `workspace.write`, and only platform operators should
//! ever be granted `workspace.write`. (The guard recognizes the `read`/`write` verb
//! segments specifically; a lone `workspace.admin` scope would leave the surface
//! ungated, so the admin boundary is expressed as *who holds these scopes*.)

use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the workspace catalog's REST surface.
///
/// Identity, capability, and the routes are described here; the live catalog
/// repository is baked into the router only when the binary constructs the module
/// with [`with_http`](Self::with_http). Without the `axum` feature this type does not
/// exist — the catalog still functions as a plain domain crate (commands, actor, PG
/// adapter); the module facade is purely the opt-in HTTP transport.
#[derive(Clone, Default)]
pub struct WorkspaceModule {
    /// The REST state, present only when the binary opts the module into its HTTP
    /// surface via [`with_http`](Self::with_http). `None` (the default) keeps the
    /// empty-router default.
    http: Option<crate::http::WorkspaceApiState>,
}

impl WorkspaceModule {
    /// The module's stable id (`workspace`). Matches the `workspace.*` MCP-tool and
    /// `workspace.<verb>` scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("workspace").expect("`workspace` is a valid module id")
    }

    /// Build a module that serves the REST surface (`hq-fe-api-platform.1`), baking
    /// `state` (the live catalog repository) into the router
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this;
    /// the default-constructed module contributes no routes.
    pub fn with_http(state: crate::http::WorkspaceApiState) -> Self {
        Self { http: Some(state) }
    }
}

impl GtModule for WorkspaceModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Workspace",
            Version::new(1, 0, 0),
            "Workspace catalog — the cross-workspace tenant lifecycle \
             (create/info/list/suspend/resume/archive). Admin-level: these routes \
             administer the catalog itself, not data inside a tenant.",
        )
    }

    fn capability(&self) -> Capability {
        // `workspace.read` / `workspace.write` — admin-level authorities over the
        // tenant catalog (see the module docs). The builder's guard derives the
        // required scope per verb-class from these. No event kinds: the catalog
        // commands persist through the repository, not the bus.
        Capability::empty().claiming_all([
            Scope::new("workspace.read").expect("valid scope"),
            Scope::new("workspace.write").expect("valid scope"),
        ])
    }

    /// The workspace REST routes (`hq-fe-api-platform.1`), relative — the builder
    /// nests them under `/api/v1/workspace` and applies the
    /// `workspace.read`/`workspace.write` scope guard. Present only when the module
    /// was built with [`with_http`](Self::with_http); otherwise the empty default.
    fn register_routes(&self) -> axum::Router {
        match &self.http {
            Some(state) => crate::http::workspace_router(state.clone()),
            None => axum::Router::new(),
        }
    }

    /// The OpenAPI spec for the workspace REST routes, contributed only when the
    /// module carries the HTTP state (so the combined document documents exactly the
    /// routes actually mounted).
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        self.http.as_ref().map(|_| crate::http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::WorkspaceApiState;
    use crate::InMemoryWorkspaces;
    use std::sync::Arc;

    #[test]
    fn meta_identity_is_workspace() {
        let m = WorkspaceModule::default().meta();
        assert_eq!(m.id.as_str(), "workspace");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_workspace_scopes_and_emits_nothing() {
        let cap = WorkspaceModule::default().capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["workspace.read", "workspace.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn default_module_contributes_no_routes_or_openapi() {
        // The default module carries no HTTP state ⇒ the empty-router / no-OpenAPI
        // defaults stand. The REST surface is opt-in via `with_http`.
        assert!(WorkspaceModule::default().openapi().is_none());
        assert!(WorkspaceModule::default().migrations().is_empty());
    }

    #[test]
    fn with_http_contributes_routes_and_openapi() {
        // Built with HTTP state, the module documents its REST routes and returns a
        // non-empty OpenAPI spec the builder mounts under `/api/v1/workspace`.
        let repo: Arc<dyn crate::WorkspaceRepository> = Arc::new(InMemoryWorkspaces::new());
        let m = WorkspaceModule::with_http(WorkspaceApiState::new(repo));
        assert!(m.openapi().is_some());
    }
}
