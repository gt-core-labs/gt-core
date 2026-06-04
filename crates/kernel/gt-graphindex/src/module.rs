//! [`GraphModule`] — the `graph.*` knowledge-graph surface as a [`GtModule`]
//! (`hq-fe-api-kernel.1`).
//!
//! The graph subsystem had no module declaration: the MCP `graph.*` tools were registered as a
//! bare `DomainHandler` at the composition edge. This file gives the subsystem a proper module
//! seam so the `RootBuilder` can harvest its identity, capability, and tool descriptors — and,
//! under the `axum` feature, mount its read-only REST surface — exactly as `gt-quota`/`gt-merge`
//! declare theirs.
//!
//! `GraphModule` is **descriptor-only**: it declares the `graph.*` tool descriptors + the
//! `graph.read`/`graph.write` scopes, but contributes no HTTP surface — the MCP server bin owns
//! the tools' execution (the warden-state replay + indexer dispatch), keeping the descriptor-only
//! seam the kernel mandates. To also serve the REST surface, the binary builds an HTTP-bearing
//! module with [`with_http`](GraphModule::with_http); the live read provider + indexer ride in a
//! separate [`GraphHttpModule`] that delegates its descriptors here, so the two never drift.

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The descriptor-only [`GtModule`] facade over the graph subsystem. Zero-sized: the live
/// indexer + per-workspace warden custody live at the composition edge, so the unit struct is all
/// the MCP harvest path registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct GraphModule;

impl GraphModule {
    /// The module's stable id (`graph`). Matches the `graph.*` MCP-tool namespace and the
    /// `graph.<verb>` scope namespace. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("graph").expect("`graph` is a valid module id")
    }

    /// Build the HTTP-enabled graph module (`hq-fe-api-kernel.1`), baking `state` (the
    /// per-workspace read provider + the active indexer) into the router its
    /// [`register_routes`](GtModule::register_routes) returns. The binary calls this to opt the
    /// module into its REST surface; the MCP harvest path keeps the plain unit [`GraphModule`].
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::GraphApiState) -> GraphHttpModule {
        GraphHttpModule { http: state }
    }

    /// The `graph.read` / `graph.write` scopes the module owns (same `<resource>.<verb>`
    /// convention `rig`/`merge`/`quota` follow). No event kinds: the graph tools read the index
    /// and the warden writes freshness through its own command path, not this module's bus.
    fn graph_capability() -> Capability {
        Capability::empty().claiming_all([
            Scope::new("graph.read").expect("valid scope"),
            Scope::new("graph.write").expect("valid scope"),
        ])
    }

    /// The shared `graph.*` tool descriptors, names + descriptions verbatim from the live MCP
    /// `GraphHandler` (`hq-graphrig.10`). Declared once and reused by both the descriptor-only
    /// [`GraphModule`] and the HTTP-bearing [`GraphHttpModule`] so the MCP surface never diverges.
    /// The four reads (`query`/`explain`/`status`/`list`) are the surface the REST adapter mirrors;
    /// the two writes (`refresh`/`refresh-stale`) stay an MCP-only custodian concern (they shell
    /// out to `git` + append warden events, which the domain tier skips — the issues-S2/S3
    /// precedent).
    fn register_graph_tools(registry: &mut McpRegistry) {
        registry
            .tool(
                "graph.query",
                "Ask a natural-language question against a rig's codebase knowledge graph.",
            )
            .tool(
                "graph.explain",
                "Explain one node (crate/concept) in a rig's knowledge graph.",
            )
            .tool("graph.status", "Report a rig's graph freshness + index stats.")
            .tool(
                "graph.refresh",
                "Rebuild a rig's knowledge graph; optionally over an explicit repo_dir.",
            )
            .tool(
                "graph.refresh-stale",
                "Rebuild every rig whose graph the warden marked stale.",
            )
            .tool(
                "graph.list",
                "List the rigs under warden custody with their freshness.",
            );
    }
}

impl GtModule for GraphModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Graph",
            Version::new(1, 0, 0),
            "Codebase knowledge-graph queries — the surface other agents use to consult a rig's \
             graph for context (query/explain/status/list); the warden owns refresh.",
        )
    }

    fn capability(&self) -> Capability {
        Self::graph_capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        Self::register_graph_tools(registry);
    }
}

/// The HTTP-enabled graph module (`hq-fe-api-kernel.1`): the same `GtModule` contract as
/// [`GraphModule`] plus the read-only `graph.*` REST routes + OpenAPI spec.
///
/// Built by [`GraphModule::with_http`]. Identity, capability, and MCP tools delegate verbatim to
/// [`GraphModule`] (one source of truth for the contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the per-workspace [`WorkspaceGraph`](crate::WorkspaceGraph) read provider
/// + indexer the handlers dispatch through.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct GraphHttpModule {
    /// The REST state baked into the router by [`register_routes`](GtModule::register_routes).
    http: crate::http::GraphApiState,
}

#[cfg(feature = "axum")]
impl GtModule for GraphHttpModule {
    fn meta(&self) -> ModuleMeta {
        GraphModule.meta()
    }

    fn capability(&self) -> Capability {
        GraphModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        GraphModule.register_mcp_tools(registry);
    }

    /// The read-only graph REST routes (`hq-fe-api-kernel.1`), relative — the builder nests them
    /// under `/api/v1/graph` and applies the `graph.read` scope guard (every route is a GET).
    fn register_routes(&self) -> axum::Router {
        crate::http::graph_router(self.http.clone())
    }

    /// The OpenAPI spec for the graph REST routes, so the combined document documents exactly the
    /// routes mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_graph() {
        let m = GraphModule.meta();
        assert_eq!(m.id.as_str(), "graph");
        assert_eq!(m.id, GraphModule::id());
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_graph_scopes_and_emits_nothing() {
        let cap = GraphModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["graph.read", "graph.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_the_six_graph_tools_namespaced() {
        let mut reg = McpRegistry::new();
        GraphModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "graph.query",
                "graph.explain",
                "graph.status",
                "graph.refresh",
                "graph.refresh-stale",
                "graph.list",
            ]
        );
        for t in reg.tools() {
            let ns = t.name.split('.').next().unwrap();
            assert_eq!(ns, GraphModule::id().as_str(), "tool {} must be in the graph namespace", t.name);
        }
    }

    #[test]
    fn descriptor_only_module_contributes_no_routes_or_openapi() {
        // The MCP harvest path uses the unit module: no HTTP state ⇒ the empty-router /
        // no-OpenAPI defaults stand. The REST surface is opt-in via `with_http`.
        assert!(GraphModule.openapi().is_none());
        assert!(GraphModule.migrations().is_empty());
    }
}
