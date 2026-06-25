//! [`MetaModule`] — the cross-cutting `meta.*` tools as a [`GtModule`].

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::commands::ReportGap;

/// The meta module. The default-constructed `MetaModule` is **descriptor-only**: it
/// declares the `meta.*` tool DESCRIPTORS + the `meta.read`/`meta.write` scopes, but
/// contributes no HTTP surface — the MCP server bin owns the tools' execution
/// (meta.help over the built Root's tool list, meta.report-gap over the issues
/// store), keeping the descriptor-only seam the kernel mandates.
///
/// To also serve the REST surface (`hq-fe-api-platform.4`), the binary builds an
/// HTTP-bearing module with [`with_http`](Self::with_http). `MetaModule` itself
/// stays a unit struct so `RootBuilder::module(MetaModule)` keeps compiling as-is;
/// the live store + tool list ride in a separate [`MetaHttpModule`] that delegates
/// its descriptors here, so the two never drift.
pub struct MetaModule;

impl MetaModule {
    /// The module's stable id (`meta`). Matches the `meta.*` tool namespace and the
    /// `meta.<verb>` scope namespace.
    pub fn id() -> ModuleId {
        ModuleId::new("meta").expect("`meta` is a valid module id")
    }

    /// Build a module that also serves the REST surface (`hq-fe-api-platform.4`),
    /// baking `state` (the live store + attribution actor + harvested tool list)
    /// into the router [`GtModule::register_routes`] returns. The binary calls this;
    /// the descriptor-only path uses `MetaModule` directly.
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::MetaApiState) -> MetaHttpModule {
        MetaHttpModule { http: state }
    }

    /// The `meta.read` / `meta.write` scopes the module owns (same
    /// `<resource>.<verb>` convention `issues`/`rig`/`workspace` follow). No event
    /// kinds: the meta tools touch the issues store directly, not the bus.
    fn meta_capability() -> Capability {
        Capability::empty().claiming_all([
            Scope::new("meta.read").expect("valid scope"),
            Scope::new("meta.write").expect("valid scope"),
        ])
    }

    /// The shared `meta.*` tool descriptors. Declared once and reused by both the
    /// descriptor-only [`MetaModule`] and the HTTP-bearing [`MetaHttpModule`] so the
    /// MCP surface never diverges from the REST surface's scope/tooling.
    fn register_meta_tools(registry: &mut McpRegistry) {
        registry
            .tool(
                "meta.help.execute",
                "Server self-description: the full tools/list (names, descriptions, input \
                 schemas) the server serves. Single-call discovery. No state change.",
            )
            .tool_with_schema(
                "meta.report-gap.execute",
                "Surface a missing MCP operation: the server mints a hq-gap-<slug>-<ts> bead \
                 so the gap enters the routine catalog. Input: { operation (required), notes?, \
                 priority? (0=P0..2=P2, default 2), external_ref? (sub-epic; defaults to the \
                 hq-gaps catalog so the bead is NN-16-sound, never orphaned), surface? \
                 (crate/path strings), domain? (domain discriminators), depends_on? (blocker \
                 bead ids) }. The surface/domain/depends_on seeds make the gap visible to \
                 ?ready=true filtering + reconciler edge derivation without a manual update.",
                schema_for::<ReportGap>(),
            );
    }
}

impl GtModule for MetaModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Meta",
            Version::new(1, 0, 0),
            "Cross-cutting server tools: meta.help (tools/list) and meta.report-gap \
             (mint a hq-gap bead for a missing operation).",
        )
    }

    fn capability(&self) -> Capability {
        Self::meta_capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        Self::register_meta_tools(registry);
    }
}

/// [`MetaModule`] built with its REST state (`hq-fe-api-platform.4`).
///
/// Carries the live store + attribution actor + harvested tool list, so its
/// [`register_routes`](GtModule::register_routes) mounts the `meta.*` REST surface
/// the builder nests under `/api/v1/meta` behind the `meta.read`/`meta.write` scope
/// guard. Its identity, capability, and MCP tools delegate to [`MetaModule`] so the
/// descriptor surface is identical whether or not the REST routes are wired.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct MetaHttpModule {
    /// The REST state baked into the router by [`register_routes`](GtModule::register_routes).
    http: crate::http::MetaApiState,
}

#[cfg(feature = "axum")]
impl GtModule for MetaHttpModule {
    fn meta(&self) -> ModuleMeta {
        MetaModule.meta()
    }

    fn capability(&self) -> Capability {
        MetaModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        MetaModule.register_mcp_tools(registry);
    }

    /// The `meta.*` REST routes, relative — the builder nests them under
    /// `/api/v1/meta` and applies the `meta.read`/`meta.write` scope guard.
    fn register_routes(&self) -> axum::Router {
        crate::http::meta_router(self.http.clone())
    }

    /// The OpenAPI spec for the meta REST routes, so the combined document documents
    /// exactly the routes the HTTP-bearing module mounts.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

/// The per-workspace domain-catalog REST module (gtcore-b37400 H4).
///
/// A standalone module (id `domain`, distinct from `meta`) so its routes are guarded
/// by `domain.read`/`domain.write` — admin scopes the agent presets never carry —
/// rather than the `meta.read`/`meta.write` the `report-gap` surface shares. It
/// contributes NO MCP tools: the `domain.catalog.*` tools are served by the
/// composition-tier `DomainRouter` handler, which also advertises their descriptors,
/// so the `/scopes` catalog surfaces `domain.read`/`domain.write` from the tool names
/// alone. This module only mounts the REST twin of those tools.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct DomainCatalogHttpModule {
    /// The REST state baked into the router by [`register_routes`](GtModule::register_routes).
    http: crate::domain_http::DomainApiState,
}

#[cfg(feature = "axum")]
impl DomainCatalogHttpModule {
    /// The module's stable id (`domain`) — matches the `domain.*` tool namespace and
    /// the `domain.<verb>` scope namespace.
    pub fn id() -> ModuleId {
        ModuleId::new("domain").expect("`domain` is a valid module id")
    }

    /// Build the module over its REST state (the live store + per-workspace pools).
    pub fn with_http(state: crate::domain_http::DomainApiState) -> Self {
        Self { http: state }
    }

    /// The `domain.read` / `domain.write` scopes this module owns — the same
    /// `<resource>.<verb>` convention the rest follow. The route guard derives
    /// `domain.read` for `GET` and `domain.write` for the mutating methods.
    fn domain_capability() -> Capability {
        Capability::empty().claiming_all([
            Scope::new("domain.read").expect("valid scope"),
            Scope::new("domain.write").expect("valid scope"),
        ])
    }
}

#[cfg(feature = "axum")]
impl GtModule for DomainCatalogHttpModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Domain catalog",
            Version::new(1, 0, 0),
            "Per-workspace domain catalog editing: list/add/rename/set-enabled/remove \
             the domains a workspace's beads may carry (gtcore-b37400).",
        )
    }

    fn capability(&self) -> Capability {
        Self::domain_capability()
    }

    /// The `domain.catalog.*` REST routes, relative — the builder nests them under
    /// `/api/v1/domain` and applies the `domain.read`/`domain.write` scope guard.
    fn register_routes(&self) -> axum::Router {
        crate::domain_http::domain_router(self.http.clone())
    }

    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::domain_http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_meta() {
        let m = MetaModule.meta();
        assert_eq!(m.id.as_str(), "meta");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_meta_scopes_and_emits_nothing() {
        let cap = MetaModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["meta.read", "meta.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_the_meta_tools() {
        let mut reg = McpRegistry::new();
        MetaModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["meta.help.execute", "meta.report-gap.execute"]);
    }

    #[test]
    fn report_gap_tool_carries_an_object_input_schema() {
        let mut reg = McpRegistry::new();
        MetaModule.register_mcp_tools(&mut reg);
        let gap = reg
            .tools()
            .iter()
            .find(|t| t.name == "meta.report-gap.execute")
            .expect("report-gap present");
        assert_eq!(gap.input_schema["type"], "object");
        assert!(gap.input_schema["properties"]["operation"].is_object());
    }

    #[test]
    fn default_module_contributes_no_routes_or_openapi() {
        // The descriptor-only path uses `MetaModule`: it owns scopes + tools but no HTTP
        // surface, so the empty-router / no-OpenAPI defaults stand. The REST surface is
        // opt-in via `with_http`.
        assert!(MetaModule.openapi().is_none());
        assert!(MetaModule.migrations().is_empty());
    }

    #[cfg(feature = "axum")]
    #[test]
    fn with_http_contributes_routes_and_openapi_and_same_descriptors() {
        // Built with HTTP state, the module documents its REST routes and returns a non-empty
        // OpenAPI spec the builder mounts under `/api/v1/meta`, while its descriptors stay
        // identical to the descriptor-only module.
        use gt_store_dolt::DoltIssues;
        use std::sync::Arc;
        // A never-connected store is fine: `openapi()` reads no store, and `register_routes`
        // only clones the state into the router without dispatching.
        let store = Arc::new(DoltIssues::connect("mysql://x@127.0.0.1:1/none").unwrap());
        let tools: Vec<gt_module::McpTool> = Vec::new();
        let m = MetaModule::with_http(crate::http::MetaApiState::new(store, "test", tools));
        assert!(m.openapi().is_some());
        assert_eq!(m.meta().id.as_str(), "meta");
        let cap = m.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["meta.read", "meta.write"]);
    }
}
