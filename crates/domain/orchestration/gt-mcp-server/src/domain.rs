//! Domain tool dispatch contract (`hq-mcp-dispatch.1`).
//!
//! The server's `call_tool` routes by the tool's namespace (the segment before the first dot):
//! `meta.*` → the self-description tools, `issues.*` → the tracker dispatch over a `DoltIssues`
//! store, and **everything else** → this [`DomainRouter`]. Each ported domain (workspace, rig,
//! quota, merge, convoy, sessions — the `hq-mcp-dispatch.2..7` slices) registers a
//! [`DomainHandler`] for its namespace, so the server can operate every domain function, not
//! just issue tracking.
//!
//! This module is the **contract only**: the routing seam + the per-request [`DomainCtx`]. It
//! owns no domain logic. A handler receives the resolved workspace slug (a plain `&str`, so the
//! server never depends on a `WorkspaceId` type), the attribution actor, and the parsed args;
//! it composes / caches whatever per-workspace actors or stores it needs internally and returns
//! a JSON payload (or an [`AppError`] the server maps to an MCP error). `dyn` + `async_trait`
//! are allowed here — this is the orchestration tier, not the kernel.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gt_module::McpTool;
use gt_store_dolt::AppError;
use serde_json::Value;

/// Everything one domain tool call needs, resolved by the server before dispatch.
pub struct DomainCtx<'a> {
    /// The resolved workspace slug, or `None` for the default workspace. A plain string at this
    /// boundary so the handler — not the server — owns the `WorkspaceId` parse.
    pub workspace: Option<&'a str>,
    /// The attribution actor (the resolved scope actor), for audit + ownership.
    pub actor: &'a str,
    /// The parsed tool arguments.
    pub args: Value,
}

/// A handler for one tool namespace (e.g. `workspace`, `rig`).
///
/// `namespace` is the dotted prefix this handler owns (`"workspace"` handles `workspace.*`).
/// `dispatch` runs one fully-qualified tool (`"workspace.create"`); returning `Err` surfaces as
/// an MCP error, exactly like the issues dispatch.
#[async_trait]
pub trait DomainHandler: Send + Sync {
    /// The namespace (first dot-segment) this handler is registered under.
    fn namespace(&self) -> &'static str;

    /// Run one `<namespace>.<verb>` tool call.
    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError>;

    /// The tool descriptors (name + description + input schema) this namespace
    /// advertises (hq-mcp-native.3). The server folds these into `self.tools` so
    /// `tools/list` + `meta.help` surface every dispatchable domain tool — without
    /// this, a tool routes but is undiscoverable to a native client. Each name is
    /// the two-segment `<namespace>.<verb>` the handler's `dispatch` matches.
    ///
    /// Default empty: a handler surfaces nothing until it overrides this, so adding
    /// the method is additive (a not-yet-described handler still dispatches).
    fn descriptors(&self) -> Vec<McpTool> {
        Vec::new()
    }
}

/// Registry of [`DomainHandler`]s keyed by namespace. Built once at composition time, handed to
/// the server via `IssuesServer::with_domains`. Empty by default — the server then behaves
/// exactly as the issues-only build.
#[derive(Clone, Default)]
pub struct DomainRouter {
    handlers: HashMap<&'static str, Arc<dyn DomainHandler>>,
}

impl DomainRouter {
    /// An empty router (no domain handlers).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler under its [`namespace`](DomainHandler::namespace). Builder-style; a
    /// second handler for the same namespace replaces the first (last wins).
    pub fn register(mut self, handler: Arc<dyn DomainHandler>) -> Self {
        self.handlers.insert(handler.namespace(), handler);
        self
    }

    /// The registered namespaces, for diagnostics / tests.
    pub fn namespaces(&self) -> Vec<&'static str> {
        let mut ns: Vec<&'static str> = self.handlers.keys().copied().collect();
        ns.sort_unstable();
        ns
    }

    /// Every registered handler's tool descriptors (hq-mcp-native.3), sorted by
    /// name for a stable `tools/list` order. The server folds these into the
    /// advertised tool set so the domain namespaces are discoverable, not just
    /// dispatchable.
    pub fn descriptors(&self) -> Vec<McpTool> {
        let mut tools: Vec<McpTool> = self
            .handlers
            .values()
            .flat_map(|h| h.descriptors())
            .collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }

    /// Dispatch `tool` to the handler owning its namespace.
    ///
    /// `Ok(Some(value))` — a handler ran. `Ok(None)` — no handler owns this namespace, so the
    /// server reports an unknown tool (a domain `Err` is the handler's own failure, returned as
    /// `Err`). The namespace is the segment before the first `.`.
    pub async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Option<Value>, AppError> {
        let ns = tool.split('.').next().unwrap_or("");
        match self.handlers.get(ns) {
            Some(handler) => handler.dispatch(tool, ctx).await.map(Some),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Echoes the tool + workspace it was dispatched for, and fails a designated verb.
    struct DemoHandler;

    #[async_trait]
    impl DomainHandler for DemoHandler {
        fn namespace(&self) -> &'static str {
            "demo"
        }
        async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
            if tool == "demo.boom" {
                return Err(AppError::Validation("boom".into()));
            }
            Ok(json!({ "tool": tool, "workspace": ctx.workspace, "actor": ctx.actor }))
        }
    }

    fn ctx() -> DomainCtx<'static> {
        DomainCtx { workspace: Some("acme"), actor: "tester", args: json!({}) }
    }

    #[tokio::test]
    async fn dispatches_to_the_namespace_handler() {
        let router = DomainRouter::new().register(Arc::new(DemoHandler));
        let out = router.dispatch("demo.create", ctx()).await.unwrap();
        assert_eq!(out, Some(json!({ "tool": "demo.create", "workspace": "acme", "actor": "tester" })));
    }

    #[tokio::test]
    async fn unknown_namespace_returns_none() {
        let router = DomainRouter::new().register(Arc::new(DemoHandler));
        assert_eq!(router.dispatch("rig.add", ctx()).await.unwrap(), None);
        // empty router: every namespace is unknown.
        assert_eq!(DomainRouter::new().dispatch("demo.create", ctx()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn handler_error_propagates() {
        let router = DomainRouter::new().register(Arc::new(DemoHandler));
        assert!(matches!(
            router.dispatch("demo.boom", ctx()).await,
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn namespaces_are_listed_sorted() {
        let r = DomainRouter::new().register(Arc::new(DemoHandler));
        assert_eq!(r.namespaces(), vec!["demo"]);
    }
}
