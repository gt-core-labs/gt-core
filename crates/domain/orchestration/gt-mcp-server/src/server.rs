//! The rmcp [`ServerHandler`] for the gt-core MCP server (hq-core-host.3).
//!
//! This is the transport pillar: it wraps the issues module's tool DESCRIPTORS
//! (harvested from the kernel [`McpRegistry`](gt_module::McpRegistry) into a
//! built [`Root`](gt_module::Root)) and the lifted Dolt store, adding the parts
//! the kernel deliberately keeps out of itself — the rmcp `call_tool` dispatch,
//! the `gt-rbac` scope check, and the `gt-audit` record per call.

use std::borrow::Cow;
use std::sync::Arc;

use gt_audit::{AuditRecord, AuditSink};
use gt_module::McpTool;
use gt_rbac::{RbacConfig, Scope};
use gt_store_dolt::{AppError, DoltIssues};

use rmcp::model::{
    AnnotateAble, CallToolRequestParams, CallToolResult, Content, Extensions, Implementation,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, RawResource,
    ReadResourceRequestParams, ReadResourceResult, ResourceContents, ServerCapabilities,
    ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};

use crate::dispatch::{dispatch, dispatch_meta, parse_issue_filter};

/// The shared MCP service. Cheap to clone (every field is `Arc`-backed) so the
/// streamable-HTTP session manager hands each connection its own handle over the
/// one store + scope + audit + descriptor set.
#[derive(Clone)]
pub struct IssuesServer {
    store: Arc<DoltIssues>,
    /// Scope used when a request carries no `X-Actor` header (the boot
    /// `GT_MCP_ACTOR`, or a closed deny-all when no RBAC config is wired).
    default_scope: Arc<Scope>,
    /// RBAC config for per-connection resolution (hq-core-mcp.6). `Some` when
    /// `GT_MCP_SCOPE_CONFIG` is set: a request's `X-Actor` header then resolves
    /// its own scope, so multiple actors share one server with distinct
    /// allow-lists. `None` keeps every request on `default_scope`.
    rbac: Option<Arc<RbacConfig>>,
    audit: Arc<dyn AuditSink + Send + Sync>,
    tools: Arc<Vec<McpTool>>,
}

impl IssuesServer {
    /// Build the service from the composed pieces. `tools` are the descriptors
    /// the built `Root` exposes (`root.mcp_tools()`), already namespaced + NN-16
    /// shaped by the kernel builder. `rbac` enables per-connection `X-Actor`
    /// scope resolution; `None` pins every call to `default_scope`.
    pub fn new(
        store: Arc<DoltIssues>,
        default_scope: Scope,
        rbac: Option<Arc<RbacConfig>>,
        audit: Arc<dyn AuditSink + Send + Sync>,
        tools: Vec<McpTool>,
    ) -> Self {
        Self {
            store,
            default_scope: Arc::new(default_scope),
            rbac,
            audit,
            tools: Arc::new(tools),
        }
    }

    /// Resolve the scope for one request from its extensions. When an RBAC config
    /// is wired and the request carries an `X-Actor` header, the actor gets its
    /// own allow-list; otherwise the boot `default_scope` applies.
    fn resolve_scope(&self, ext: &Extensions) -> Cow<'_, Scope> {
        match (&self.rbac, actor_from_ext(ext)) {
            (Some(rbac), Some(actor)) => Cow::Owned(Scope::from_rbac(rbac, actor)),
            _ => Cow::Borrowed(&self.default_scope),
        }
    }

    /// Map a domain error onto an MCP error. Validation / not-found are caller
    /// faults (invalid_request); everything else is an internal error.
    fn to_mcp_error(err: &AppError) -> McpError {
        match err {
            AppError::Validation(_) | AppError::NotFound(_) | AppError::InvalidTransition(_) => {
                McpError::invalid_request(err.to_string(), None)
            }
            _ => McpError::internal_error(err.to_string(), None),
        }
    }
}

impl ServerHandler for IssuesServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools: Vec<Tool> = self
            .tools
            .iter()
            .map(|t| {
                let schema = t
                    .input_schema
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                Tool::new(t.name.clone(), t.description.clone(), Arc::new(schema))
            })
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let tool = request.name.to_string();
        let args = serde_json::Value::Object(request.arguments.clone().unwrap_or_default());

        // Per-connection scope (hq-core-mcp.6): the X-Actor header picks the
        // allow-list + the attribution actor, falling back to the boot default.
        let scope = self.resolve_scope(&context.extensions);

        // Scope check FIRST — deny-by-default. A rejection is audited as
        // Unauthorized and surfaces as an invalid_request the caller cannot retry.
        if let Err(e) = scope.check(&tool) {
            let _ = self
                .audit
                .record(AuditRecord::unauthorized(&scope.actor, &tool, args));
            return Err(McpError::invalid_request(e.to_string(), None));
        }

        let result = if tool.starts_with("meta.") {
            dispatch_meta(&self.store, &tool, args.clone(), &scope.actor, &self.tools).await
        } else {
            dispatch(&self.store, &tool, args.clone(), &scope.actor).await
        };
        let _ = self
            .audit
            .record(AuditRecord::invoked(&scope.actor, &tool, args));

        match result {
            Ok(payload) => Ok(CallToolResult::success(vec![Content::text(
                payload.to_string(),
            )])),
            Err(err) => Err(Self::to_mcp_error(&err)),
        }
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        // Advertise the two issues resources so clients discover them (hq-core-mcp.1)
        // instead of having to know the URIs out of band.
        let mk = |uri: &str, name: &str, desc: &str| {
            let mut r = RawResource::new(uri, name);
            r.description = Some(desc.to_string());
            r.mime_type = Some("application/json".to_string());
            r.no_annotation()
        };
        let resources = vec![
            mk(
                "gt://issues",
                "issues",
                "Canonical issues snapshot from Dolt. Filters via querystring: \
                 status=open[,working], priority_max=2, assignee=X, external_ref=Y, \
                 issue_type=epic, limit=N. Pass full=1 to inline the text bodies \
                 (description/design/acceptance_criteria/notes) on every row.",
            ),
            mk(
                "gt://issue/{id}",
                "issue.detail",
                "Single bead WITH full text bodies + taxonomy columns + version. Read \
                 after claiming a bead to see the actual spec.",
            ),
        ];
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = request.uri.as_str();
        let value = self
            .read_resource_json(uri)
            .await
            .map_err(|e| Self::to_mcp_error(&e))?;
        let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            text, uri,
        )]))
    }
}

impl IssuesServer {
    /// Resolve a `gt://issues...` / `gt://issue/{id}` URI to its JSON payload.
    async fn read_resource_json(&self, uri: &str) -> Result<serde_json::Value, AppError> {
        // gt://issues[?filter] — the snapshot (optionally ?full=1).
        let issues_qs = match uri.strip_prefix("gt://issues") {
            Some("") => Some(""),
            Some(rest) if rest.starts_with('?') => Some(&rest[1..]),
            _ => None,
        };
        if let Some(qs) = issues_qs {
            let filter = parse_issue_filter(qs)?;
            let rows = gt_issues::resources::read_issues(&self.store, &filter).await?;
            return serde_json::to_value(&rows)
                .map_err(|e| AppError::Other(format!("encode issues: {e}")));
        }

        // gt://issue/{id} — one bead WITH the heavy bodies.
        if let Some(id) = uri.strip_prefix("gt://issue/") {
            if id.is_empty() || id.contains('/') {
                return Err(AppError::Validation(
                    "gt://issue/<id> expects a single non-empty path segment".into(),
                ));
            }
            return match gt_issues::resources::read_issue(&self.store, id).await? {
                Some(detail) => serde_json::to_value(&detail)
                    .map_err(|e| AppError::Other(format!("encode issue: {e}"))),
                None => Err(AppError::NotFound(format!("issue {id}"))),
            };
        }

        Err(AppError::NotFound(format!("resource {uri}")))
    }
}

/// Extract the `X-Actor` header from a request's extensions. rmcp injects the
/// HTTP `Parts` into each call's extensions, so the trimmed, non-empty header
/// value identifies the connection's actor (hq-core-mcp.6). `None` when no Parts,
/// no header, or an empty/garbled value — the caller then uses the default scope.
fn actor_from_ext(ext: &Extensions) -> Option<&str> {
    ext.get::<axum::http::request::Parts>()
        .and_then(|p| p.headers.get("x-actor"))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::actor_from_ext;
    use rmcp::model::Extensions;

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
    fn reads_x_actor_header() {
        let ext = ext_with_header("x-actor", "alice");
        assert_eq!(actor_from_ext(&ext), Some("alice"));
    }

    #[test]
    fn trims_and_rejects_blank() {
        assert_eq!(actor_from_ext(&ext_with_header("x-actor", "  bob ")), Some("bob"));
        assert_eq!(actor_from_ext(&ext_with_header("x-actor", "   ")), None);
    }

    #[test]
    fn none_without_parts_or_header() {
        assert_eq!(actor_from_ext(&Extensions::new()), None);
        assert_eq!(actor_from_ext(&ext_with_header("x-other", "alice")), None);
    }
}
