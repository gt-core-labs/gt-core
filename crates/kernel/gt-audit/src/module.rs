//! [`AuditModule`] — the `audit.*` trail surface as a [`GtModule`] (`hq-fe-api-kernel.2`).
//!
//! The audit subsystem had no module declaration: the MCP `audit.tail` tool was registered as a
//! bare `DomainHandler` at the composition edge. This file gives the subsystem a proper module
//! seam so the `RootBuilder` can harvest its identity, capability, and the `audit.tail` descriptor
//! — and, under the `axum` feature, mount its read-only REST surface — exactly as `gt-quota` /
//! `gt-graphindex` declare theirs.
//!
//! `AuditModule` is **descriptor-only**: it declares the `audit.tail` descriptor + the `audit.read`
//! scope, but contributes no HTTP surface — the MCP server bin owns the tool's execution (the tail
//! over the shared sink), keeping the descriptor-only seam the kernel mandates. To also serve the
//! REST surface, the binary builds an HTTP-bearing module with [`with_http`](AuditModule::with_http);
//! the live sink rides in a separate [`AuditHttpModule`] that delegates its descriptor here, so the
//! two never drift.

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The descriptor-only [`GtModule`] facade over the audit trail. Zero-sized: the live sink is the
/// `Arc` the server records into, so the unit struct is all the MCP harvest path registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuditModule;

impl AuditModule {
    /// The module's stable id (`audit`). Matches the `audit.*` MCP-tool namespace and the
    /// `audit.<verb>` scope namespace. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("audit").expect("`audit` is a valid module id")
    }

    /// Build the HTTP-enabled audit module (`hq-fe-api-kernel.2`), baking `state` (the shared
    /// audit sink) into the router its [`register_routes`](GtModule::register_routes) returns. The
    /// binary calls this to opt the module into its REST surface; the MCP harvest path keeps the
    /// plain unit [`AuditModule`].
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::AuditApiState) -> AuditHttpModule {
        AuditHttpModule { http: state }
    }

    /// The `audit.read` scope the module owns (the trail is read-only — only the dispatch path
    /// writes records, through [`AuditSink::record`](crate::AuditSink), never an `audit.*` tool).
    fn audit_capability() -> Capability {
        Capability::empty()
            .claiming_all([Scope::new("audit.read").expect("valid scope")])
    }

    /// The shared `audit.tail` descriptor, name + description matching the live MCP `AuditHandler`.
    /// Declared once and reused by both the descriptor-only [`AuditModule`] and the HTTP-bearing
    /// [`AuditHttpModule`] so the MCP surface never diverges from the REST surface's scope/tooling.
    fn register_audit_tools(registry: &mut McpRegistry) {
        registry.tool(
            "audit.tail",
            "Tail the MCP audit trail for the caller's workspace, most recent first. Optional \
             actor/tool/outcome exact-match filters, an RFC3339 `since` lower bound, and a `limit` \
             (default 20).",
        );
    }
}

impl GtModule for AuditModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Audit",
            Version::new(1, 0, 0),
            "MCP audit trail — tail the per-tenant record of who called which tool with what \
             args and whether it was invoked or rejected unauthorized.",
        )
    }

    fn capability(&self) -> Capability {
        Self::audit_capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        Self::register_audit_tools(registry);
    }
}

/// The HTTP-enabled audit module (`hq-fe-api-kernel.2`): the same `GtModule` contract as
/// [`AuditModule`] plus the read-only `audit.tail` REST route + OpenAPI spec.
///
/// Built by [`AuditModule::with_http`]. Identity, capability, and the MCP descriptor delegate
/// verbatim to [`AuditModule`] (one source of truth for the contract); only
/// [`register_routes`](GtModule::register_routes) and [`openapi`](GtModule::openapi) are
/// overridden, carrying the shared [`AuditSink`](crate::AuditSink) the handler tails.
#[cfg(feature = "axum")]
#[derive(Clone)]
pub struct AuditHttpModule {
    /// The REST state baked into the router by [`register_routes`](GtModule::register_routes).
    http: crate::http::AuditApiState,
}

#[cfg(feature = "axum")]
impl GtModule for AuditHttpModule {
    fn meta(&self) -> ModuleMeta {
        AuditModule.meta()
    }

    fn capability(&self) -> Capability {
        AuditModule.capability()
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        AuditModule.register_mcp_tools(registry);
    }

    /// The read-only audit REST route (`hq-fe-api-kernel.2`), relative — the builder nests it under
    /// `/api/v1/audit` and applies the `audit.read` scope guard (the single route is a GET).
    fn register_routes(&self) -> axum::Router {
        crate::http::audit_router(self.http.clone())
    }

    /// The OpenAPI spec for the audit REST route, so the combined document documents exactly the
    /// route mounted under the HTTP-enabled module.
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        Some(crate::http::ApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_audit() {
        let m = AuditModule.meta();
        assert_eq!(m.id.as_str(), "audit");
        assert_eq!(m.id, AuditModule::id());
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_audit_read_and_emits_nothing() {
        let cap = AuditModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["audit.read"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_audit_tail_namespaced() {
        let mut reg = McpRegistry::new();
        AuditModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["audit.tail"]);
        let ns = reg.tools()[0].name.split('.').next().unwrap();
        assert_eq!(ns, AuditModule::id().as_str());
    }

    #[test]
    fn descriptor_only_module_contributes_no_routes_or_openapi() {
        assert!(AuditModule.openapi().is_none());
        assert!(AuditModule.migrations().is_empty());
    }
}
