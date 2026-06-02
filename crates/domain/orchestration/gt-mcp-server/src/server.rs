//! The rmcp [`ServerHandler`] for the gt-core MCP server (hq-core-host.3).
//!
//! This is the transport pillar: it wraps the issues module's tool DESCRIPTORS
//! (harvested from the kernel [`McpRegistry`](gt_module::McpRegistry) into a
//! built [`Root`](gt_module::Root)) and the lifted Dolt store, adding the parts
//! the kernel deliberately keeps out of itself — the rmcp `call_tool` dispatch,
//! the `gt-rbac` scope check, and the `gt-audit` record per call.

use std::borrow::Cow;
use std::path::PathBuf;
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
use crate::workspace::{workspace_from_ext, WorkspaceStores};

/// The shared MCP service. Cheap to clone (every field is `Arc`-backed) so the
/// streamable-HTTP session manager hands each connection its own handle over the
/// one store + scope + audit + descriptor set.
#[derive(Clone)]
pub struct IssuesServer {
    /// The default-workspace store: the tenant a request resolves to when it
    /// carries no `X-Workspace` header (the single-tenant `hq` database today).
    store: Arc<DoltIssues>,
    /// Multi-tenant resolver (hq-mt-routing.5). `Some` when a base Dolt URL is
    /// wired: a request's `X-Workspace` header then resolves that tenant's own
    /// `hq_<ws>` store per call. `None` pins every request to [`store`](Self::store),
    /// so the server behaves exactly as the single-tenant build did.
    workspaces: Option<Arc<WorkspaceStores>>,
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
    /// Git repo whose `main` tree backs surface referential integrity (S3,
    /// hq-core-mcp.9). `Some` when `GT_REPO_DIR` is set: `issues.create`/`.update`
    /// reject a `planned:false` surface path absent from `main`. `None` (no
    /// checkout, e.g. the live container) degrades to accept-all, no validation.
    repo_dir: Option<Arc<PathBuf>>,
}

impl IssuesServer {
    /// Build the service from the composed pieces. `tools` are the descriptors
    /// the built `Root` exposes (`root.mcp_tools()`), already namespaced + NN-16
    /// shaped by the kernel builder. `rbac` enables per-connection `X-Actor`
    /// scope resolution; `None` pins every call to `default_scope`. `repo_dir` is
    /// the gt-core checkout backing surface validation; `None` disables it.
    pub fn new(
        store: Arc<DoltIssues>,
        default_scope: Scope,
        rbac: Option<Arc<RbacConfig>>,
        audit: Arc<dyn AuditSink + Send + Sync>,
        tools: Vec<McpTool>,
        repo_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            store,
            workspaces: None,
            default_scope: Arc::new(default_scope),
            rbac,
            audit,
            tools: Arc::new(tools),
            repo_dir: repo_dir.map(Arc::new),
        }
    }

    /// Enable per-request workspace resolution (hq-mt-routing.5). With a resolver
    /// wired, a request carrying an `X-Workspace` header dispatches against that
    /// tenant's `hq_<ws>` store; a request without the header still uses the
    /// default-workspace [`store`](Self::store), so the change is additive.
    pub fn with_workspaces(mut self, workspaces: Arc<WorkspaceStores>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }

    /// The workspace resolver, if multi-tenant routing is wired. The health
    /// endpoints (hq-mt-routing.8) share it to report `workspaces_loaded` and
    /// per-workspace Dolt readiness.
    pub fn workspaces(&self) -> Option<Arc<WorkspaceStores>> {
        self.workspaces.clone()
    }

    /// Resolve the store for one request from its extensions. When a workspace
    /// resolver is wired and the request carries an `X-Workspace` header, the
    /// tenant's own store is built (cheap: a lazily-pooled `DoltIssues`);
    /// otherwise the default-workspace store is used. A malformed/unknown slug is
    /// rejected here, before any dispatch.
    fn resolve_store(&self, ext: &Extensions) -> Result<Arc<DoltIssues>, AppError> {
        match (&self.workspaces, workspace_from_ext(ext)) {
            (Some(ws), Some(slug)) => Ok(Arc::new(ws.store_for(slug)?)),
            _ => Ok(self.store.clone()),
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

    /// Authorize one tool call: resolve the caller's scope from its extensions
    /// (the `X-Actor` header when RBAC is wired, else the boot default) and run
    /// the deny-by-default [`Scope::check`]. A rejection is recorded as an
    /// `Unauthorized` audit row and surfaces as an `invalid_request` the caller
    /// cannot retry; on success the resolved scope is returned so the caller
    /// reuses its `actor` for store dispatch + the `Invoked` audit attribution.
    ///
    /// Carved out of [`call_tool`](Self::call_tool) so the scope→audit→error
    /// boundary is testable end to end: the rmcp [`RequestContext`] that
    /// `call_tool` receives wraps a `Peer` whose constructor is crate-private, so
    /// the wired gate cannot be exercised through `call_tool` itself — only
    /// through this seam (`tests::*` build the `Extensions` directly).
    fn authorize(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ext: &Extensions,
    ) -> Result<Cow<'_, Scope>, McpError> {
        let scope = self.resolve_scope(ext);
        if let Err(e) = scope.check(tool) {
            let _ = self
                .audit
                .record(AuditRecord::unauthorized(&scope.actor, tool, args.clone()));
            return Err(McpError::invalid_request(e.to_string(), None));
        }
        Ok(scope)
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
        // Deny-by-default — a rejection is audited as Unauthorized here and
        // surfaces as an invalid_request the caller cannot retry (hq-core-mcp.15
        // covers this gate end to end through `authorize`).
        let scope = self.authorize(&tool, &args, &context.extensions)?;

        // Per-request workspace (hq-mt-routing.5): the X-Workspace header selects
        // the tenant's store; absent it, the default-workspace store. Resolved
        // after the scope check so a malformed slug from an unauthorized caller is
        // never built.
        let store = match self.resolve_store(&context.extensions) {
            Ok(store) => store,
            Err(e) => return Err(Self::to_mcp_error(&e)),
        };

        let result = if tool.starts_with("meta.") {
            dispatch_meta(&store, &tool, args.clone(), &scope.actor, &self.tools).await
        } else {
            let repo_dir = self.repo_dir.as_deref().map(|p| p.as_path());
            dispatch(&store, &tool, args.clone(), &scope.actor, repo_dir).await
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
                "Canonical issues snapshot from Dolt, returned as a less-style PAGE: \
                 {rows, total, next_offset, has_more} in stable order — walk the \
                 corpus by advancing offset=next_offset until has_more=false (no \
                 full dump, no silent cap). Filters via querystring: \
                 status=open[,working], priority_max=2, assignee=X, external_ref=Y, \
                 issue_type=epic, limit=N, offset=M (page size default \
                 GT_ISSUES_DEFAULT_LIMIT, ceiling GT_ISSUES_MAX_LIMIT). Pass full=1 \
                 to inline the text bodies (description/design/acceptance_criteria/\
                 notes) on every row. Pass ready=1 for the UNBOUNDED frontier — a \
                 bare array of only SOUND beads (docs/10 §S4): deps delivered, phase \
                 open, own non-planned surfaces exist, status=open — claim straight \
                 from this list without re-checking the graph.",
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
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = request.uri.as_str();
        // Resources are tenant data too: resolve the per-request workspace store
        // so a read reflects the caller's workspace, not just the default one.
        let store = self
            .resolve_store(&context.extensions)
            .map_err(|e| Self::to_mcp_error(&e))?;
        let value = self
            .read_resource_json(&store, uri)
            .await
            .map_err(|e| Self::to_mcp_error(&e))?;
        let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            text, uri,
        )]))
    }
}

impl IssuesServer {
    /// Resolve a `gt://issues...` / `gt://issue/{id}` URI to its JSON payload
    /// against `store` (the per-request workspace store, hq-mt-routing.5).
    async fn read_resource_json(
        &self,
        store: &DoltIssues,
        uri: &str,
    ) -> Result<serde_json::Value, AppError> {
        // gt://issues[?filter] — the snapshot (optionally ?full=1).
        let issues_qs = match uri.strip_prefix("gt://issues") {
            Some("") => Some(""),
            Some(rest) if rest.starts_with('?') => Some(&rest[1..]),
            _ => None,
        };
        if let Some(qs) = issues_qs {
            let filter = parse_issue_filter(qs)?;
            // hq-core-mcp.11 (docs/10 §S4): `?ready=true` is the unbounded
            // actionable frontier — pull every row, then narrow to the sound
            // beads. The predicate needs the phase frontier, the (whole-table)
            // delivered index, and the `main` git tree, gathered here and applied
            // by the module's `filter_ready`. It returns a bare array (small,
            // self-complete) — no pager envelope.
            if filter.ready {
                let rows = gt_issues::resources::read_issues(store, &filter).await?;
                let open_phase = store.open_phase().await?;
                let deps = store.dep_index().await?;
                let repo_dir = self.repo_dir.as_deref().map(|p| p.as_path());
                let tree = crate::git_tree::surface_tree(repo_dir);
                let rows =
                    gt_issues::resources::filter_ready(rows, open_phase, &deps, tree.as_ref());
                return serde_json::to_value(&rows)
                    .map_err(|e| AppError::Other(format!("encode issues: {e}")));
            }
            // hq-core-mcp.13: the default snapshot is a less-style PAGE — rows plus
            // `total`/`next_offset`/`has_more` so no caller mistakes one page for
            // the whole corpus. `?limit=N&offset=M` positions it.
            let page = gt_issues::resources::read_issues_page(store, &filter).await?;
            return serde_json::to_value(&page)
                .map_err(|e| AppError::Other(format!("encode issues: {e}")));
        }

        // gt://issue/{id} — one bead WITH the heavy bodies.
        if let Some(id) = uri.strip_prefix("gt://issue/") {
            if id.is_empty() || id.contains('/') {
                return Err(AppError::Validation(
                    "gt://issue/<id> expects a single non-empty path segment".into(),
                ));
            }
            return match gt_issues::resources::read_issue(store, id).await? {
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
    use super::{actor_from_ext, IssuesServer};
    use gt_audit::{AuditSink, InMemoryAudit, Outcome};
    use gt_rbac::{RbacConfig, Scope};
    use gt_store_dolt::DoltIssues;
    use rmcp::model::{ErrorCode, Extensions};
    use serde_json::json;
    use std::sync::Arc;

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

    // ----- hq-core-mcp.15: the wired authorization gate, end to end ----------
    //
    // `call_tool` cannot be driven directly (its rmcp `RequestContext` wraps a
    // `Peer` with a crate-private constructor), so these exercise the same
    // `authorize` seam `call_tool` calls: resolve_scope (from the X-Actor header
    // or the boot default) -> Scope::check -> Unauthorized audit + invalid_request.

    /// Build a server over a shared in-memory audit sink. The Dolt URL is
    /// well-formed but unreachable: every authorization assertion here is a deny
    /// (which returns before any store I/O), and `mysql_async` pools are lazy, so
    /// no connection is ever attempted.
    fn server(
        default_scope: Scope,
        rbac: Option<Arc<RbacConfig>>,
        audit: Arc<InMemoryAudit>,
    ) -> IssuesServer {
        let store = Arc::new(
            DoltIssues::connect("mysql://gt@127.0.0.1:1/none").expect("lazy pool url parses"),
        );
        IssuesServer::new(store, default_scope, rbac, audit, vec![], None)
    }

    fn rbac(toml: &str) -> Arc<RbacConfig> {
        Arc::new(RbacConfig::from_toml(toml).expect("rbac toml parses"))
    }

    #[test]
    fn denied_default_scope_rejects_and_audits_unauthorized() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = server(Scope::denied("boot"), None, audit.clone());

        let err = srv
            .authorize("issues.create.execute", &json!({}), &Extensions::new())
            .expect_err("a closed scope must deny every tool");
        // invalid_request, not internal — a caller-side fault it cannot retry.
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1, "exactly one rejection is audited");
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].actor, "boot");
        assert_eq!(rows[0].tool, "issues.create.execute");
    }

    #[test]
    fn wrong_scope_actor_is_denied_with_its_own_attribution() {
        // `watcher` may touch issues.update.*, nothing else; the X-Actor header
        // selects that allow-list per connection (hq-core-mcp.6).
        let cfg = rbac(
            r#"
[actors.watcher]
allow = ["issues.update.*"]
"#,
        );
        let audit = Arc::new(InMemoryAudit::new());
        // Boot default is admin: the deny must come from the *resolved* per-actor
        // scope, not the fallback — proving the header drives the gate.
        let srv = server(Scope::admin("boot"), Some(cfg), audit.clone());

        let err = srv
            .authorize(
                "issues.create.execute",
                &json!({}),
                &ext_with_header("x-actor", "watcher"),
            )
            .expect_err("create is outside watcher's update-only allow-list");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        // The rejection is attributed to the resolved actor, not the boot default.
        assert_eq!(rows[0].actor, "watcher");
    }

    #[test]
    fn validate_only_actor_is_denied_execute_but_allowed_validate() {
        let cfg = rbac(
            r#"
[actors.readonly]
allow = ["*"]
validate_only = true
"#,
        );
        let audit = Arc::new(InMemoryAudit::new());
        let srv = server(Scope::denied("boot"), Some(cfg), audit.clone());
        let ext = ext_with_header("x-actor", "readonly");

        // `.execute` is barred even with a `*` allow-list...
        let err = srv
            .authorize("issues.create.execute", &json!({}), &ext)
            .expect_err("validate_only must block execute");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        // ...while the matching `.validate` passes the gate.
        let scope = srv
            .authorize("issues.create.validate", &json!({}), &ext)
            .expect("validate is permitted for a validate_only scope");
        assert_eq!(scope.actor, "readonly");

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1, "only the execute denial is audited");
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].tool, "issues.create.execute");
    }

    #[test]
    fn allowed_call_passes_the_gate_without_an_unauthorized_row() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = server(Scope::admin("boot"), None, audit.clone());

        let scope = srv
            .authorize("issues.close.execute", &json!({}), &Extensions::new())
            .expect("admin scope permits every tool");
        // The resolved scope flows out for store dispatch + Invoked attribution.
        assert_eq!(scope.actor, "boot");
        // `authorize` only records the deny path; the Invoked row is the caller's
        // to write after dispatch, so nothing is audited on the allow path here.
        assert!(audit.read_all().unwrap().is_empty());
    }
}
