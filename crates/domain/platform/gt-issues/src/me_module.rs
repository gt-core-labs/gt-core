//! `MeModule` — the [`GtModule`] facade over the cross-workspace `me.*` REST surface
//! (`hq-web-extras.15`).
//!
//! The per-workspace stats endpoint (`hq-web-extras.12`, on [`IssuesModule`](crate::IssuesModule))
//! only aggregates the *active* workspace. This module mounts the complementary cross-workspace
//! roll-up at `/api/v1/me/stats`: the `Usuario -> Workspace[] -> Rig[]` tree spanning every
//! workspace the caller is a member of. It is a thin caller-scoped read surface — there is no MCP
//! tool sibling — so it lives as its own module (`me`) rather than folding into `issues` (whose id
//! fixes its routes under `/api/v1/issues`).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `me`, so the builder mounts the router under
//!   `/api/v1/me`.
//! - **Capability** ([`GtModule::capability`]) — the `me.read` scope. The kernel route guard
//!   derives the required scope from the module id (`<id>.read` for a GET), so a `me`-mounted
//!   surface is guarded by `me.read`; it cannot reuse `issues.read` (the `IssuesModule` owns that,
//!   and two modules claiming one scope is a build conflict). `me.read` is the caller's
//!   self-view grant (the cross-workspace identity dashboard), distinct from per-workspace
//!   `issues.read`.
//! - **No MCP tools / migrations** — the surface is REST-only and reads existing trackers, so it
//!   owns no tool and no table.
//! - **HTTP routes + OpenAPI** — under the `axum` feature, the `GET /stats` route ([`crate::me`])
//!   the builder mounts at `/api/v1/me` behind the `me.read` guard. The live cross-workspace
//!   source is the one the binary supplies via [`with_http`](MeModule::with_http).

use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

#[cfg(feature = "axum")]
use gt_module::McpRegistry;

/// The [`GtModule`] facade over the cross-workspace `me.*` surface. Field-less without the `axum`
/// feature; the HTTP-enabled instance carries the REST state (see [`with_http`](Self::with_http)).
#[derive(Clone, Default)]
pub struct MeModule {
    /// The REST state, present only when the binary opts the module into its HTTP surface via
    /// [`with_http`](Self::with_http). `None` (the default) keeps the empty-router default.
    #[cfg(feature = "axum")]
    http: Option<crate::me::MeApiState>,
}

impl MeModule {
    /// The module's stable id (`me`). Mounts the router under `/api/v1/me`; the route guard
    /// derives `me.read` from it.
    pub fn id() -> ModuleId {
        ModuleId::new("me").expect("`me` is a valid module id")
    }

    /// Build a module that also serves the REST surface (`hq-web-extras.15`), baking `state` (the
    /// cross-workspace stats source) into the router
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this; the MCP
    /// harvest path uses [`default`](Self::default).
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::me::MeApiState) -> Self {
        Self { http: Some(state) }
    }
}

impl GtModule for MeModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Me",
            Version::new(1, 0, 0),
            "Cross-workspace self-view: aggregate issue progress across every workspace the \
             calling user is a member of, without switching tenant.",
        )
    }

    fn capability(&self) -> Capability {
        // `me.read` — the caller's cross-workspace self-view grant. The route guard derives it
        // from the module id; it is distinct from per-workspace `issues.read` (which `IssuesModule`
        // owns), so the two modules never conflict on a claimed scope.
        Capability::empty().claiming(Scope::new("me.read").expect("valid scope"))
    }

    /// The `me.*` REST routes (`hq-web-extras.15`), relative — the builder nests them under
    /// `/api/v1/me` and applies the `me.read` scope guard (the route is a GET). Present only when
    /// the module was built with [`with_http`](Self::with_http); otherwise the empty default.
    #[cfg(feature = "axum")]
    fn register_routes(&self) -> axum::Router {
        match &self.http {
            Some(state) => crate::me::me_router(state.clone()),
            None => axum::Router::new(),
        }
    }

    /// The OpenAPI spec for the `me.*` REST routes, contributed only when the module carries the
    /// HTTP state (so the combined document documents exactly the routes actually mounted).
    #[cfg(feature = "axum")]
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        self.http.as_ref().map(|_| crate::me::ApiDoc::openapi())
    }

    /// No MCP tools — the cross-workspace stats surface is REST-only (the `me.read` self-view has
    /// no model-facing tool sibling).
    #[cfg(feature = "axum")]
    fn register_mcp_tools(&self, _registry: &mut McpRegistry) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_me_and_mounts_under_api_v1_me() {
        let m = MeModule::default().meta();
        assert_eq!(m.id.as_str(), "me");
        assert_eq!(gt_module::module_prefix(&m.id), "/api/v1/me");
    }

    #[test]
    fn capability_owns_only_me_read() {
        let cap = MeModule::default().capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["me.read"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn default_module_contributes_no_routes_or_openapi() {
        assert!(MeModule::default().openapi().is_none());
        assert!(MeModule::default().migrations().is_empty());
    }

    #[cfg(feature = "axum")]
    #[test]
    fn registers_no_mcp_tools() {
        let mut reg = gt_module::McpRegistry::new();
        MeModule::default().register_mcp_tools(&mut reg);
        assert!(
            reg.tools().iter().next().is_none(),
            "the me surface is REST, not MCP"
        );
    }
}
