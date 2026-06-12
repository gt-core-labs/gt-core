//! The rmcp [`ServerHandler`] for the gt-core MCP server (hq-core-host.3).
//!
//! This is the transport pillar: it wraps the issues module's tool DESCRIPTORS
//! (harvested from the kernel [`McpRegistry`](gt_module::McpRegistry) into a
//! built [`Root`](gt_module::Root)) and the lifted Dolt store, adding the parts
//! the kernel deliberately keeps out of itself — the rmcp `call_tool` dispatch,
//! the `gt-rbac` scope check, and the `gt-audit` record per call.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use gt_audit::{AuditRecord, AuditSink};
use gt_auth::{has_pat_prefix, Authenticator};
use gt_issues::IssueEventSink;
use gt_module::McpTool;
use gt_rbac::{RbacConfig, Scope};
use gt_store_dolt::{AppError, DoltIssues};

use rmcp::model::{
    AnnotateAble, CallToolRequestParams, CallToolResult, Content, Extensions, Implementation,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, RawResource,
    ReadResourceRequestParams, ReadResourceResult, ResourceContents, ServerCapabilities,
    ServerInfo, Tool,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};
use tracing::Instrument;

use crate::dispatch::{dispatch, dispatch_meta, parse_issue_filter, QuotaBlockSignal};
use crate::documents::DocumentsResource;
use crate::domain::{DomainCtx, DomainRouter};
use crate::prefixes::WorkspaceRigPrefixes;
use crate::workspace::{rig_from_ext, workspace_from_ext, WorkspaceStores};
use crate::ws_status::WorkspaceStatusGate;

/// The async port that resolves an opaque Personal Access Token (`gtpat_…`) to verified
/// [`JwtClaims`](gt_auth::JwtClaims) for the `/mcp` transport (gt-core#6). The sync
/// [`Authenticator`] verifies an RS256 JWT signature in-process; a PAT is verified by a
/// Postgres lookup (I/O), so this port is `async`. The composition root supplies the
/// production adapter (`gt_auth::PgPatStore`); without it a `gtpat_` bearer is rejected
/// rather than RS256-decoded — a PAT can never be a valid JWT, so it must never reach the
/// JWT verifier (mirrors the REST `authenticate` chain).
#[async_trait::async_trait]
pub trait PatAuthenticator: Send + Sync {
    /// Verify `token` (a `gtpat_…` bearer) against the injected clock `now` (Unix seconds),
    /// resolving it to the claims the transport authorizes against — or `Err(())` when the
    /// token is unknown / expired / revoked / the backend faulted (every rejection is one
    /// deny, like a bad bearer).
    async fn verify(&self, token: &str, now: u64) -> Result<gt_auth::JwtClaims, ()>;
}

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
    /// Map from a sanitized tool name (dots → underscores) back to its canonical
    /// dotted name (hq-mcp-native.1). The kernel namespaces tools with dots
    /// (`issues.create.execute`), but an MCP function name an Anthropic client can
    /// surface must match `^[a-zA-Z0-9_-]+$` — a dotted name is silently dropped, so
    /// native clients never see the tools. `list_tools` therefore advertises the
    /// sanitized form, and `call_tool` translates the inbound name back through this
    /// map before authorize / dispatch (which key on the canonical dotted name).
    /// Built once from `tools`; a call whose name is absent (a legacy client invoking
    /// the canonical dotted name directly, or a domain tool) passes through unchanged.
    tool_aliases: Arc<HashMap<String, String>>,
    /// Git repo whose `main` tree backs surface referential integrity (S3,
    /// hq-core-mcp.9). `Some` when `GT_REPO_DIR` is set: `issues.create`/`.update`
    /// reject a `planned:false` surface path absent from `main`. `None` (no
    /// checkout, e.g. the live container) degrades to accept-all, no validation.
    repo_dir: Option<Arc<PathBuf>>,
    /// Handlers for non-issues/non-meta tool namespaces (workspace.*/rig.*/…),
    /// the `hq-mcp-dispatch` seam. Empty by default, so a server with no domain
    /// handlers wired behaves exactly as the issues-only build.
    domains: Arc<DomainRouter>,
    /// Per-workspace JWT auth (`hq-mcp-dispatch.9`). `Some` when a bearer-token
    /// verifier is wired: a request's `Authorization: Bearer` token is verified,
    /// its `workspace` claim becomes the authoritative tenant (a spoofed
    /// `X-Workspace` for another tenant is rejected — the cross-workspace leak
    /// gate), and the scope is derived from the claim's workspace role. `None`
    /// keeps the legacy `X-Actor`/`X-Workspace` header behaviour exactly.
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    /// Personal Access Token verifier for the `/mcp` transport (gt-core#6). `Some` when the
    /// composition root wires the Postgres-backed [`PatAuthenticator`]: a `gtpat_…` bearer is
    /// then verified through this async port instead of being RS256-decoded by
    /// [`authenticator`](Self::authenticator) (which would reject it as `malformed token`).
    /// `None` ⇒ a presented PAT is denied (no JWT fall-through), mirroring the REST chain.
    pat: Option<Arc<dyn PatAuthenticator>>,
    /// Per-workspace rig-prefix policy for `issues.create` (hq-mt-rigs.6). `Some`
    /// when the composition root wires a rig catalog: a new bead whose id prefix is
    /// neither a registered rig prefix in the caller's workspace nor a reserved
    /// prefix is rejected. `None` keeps the legacy accept-all create behaviour, so
    /// single-tenant / no-Postgres builds are unaffected.
    rig_prefixes: Option<Arc<dyn WorkspaceRigPrefixes>>,
    /// Per-workspace lifecycle gate for the suspend/archive enforcement
    /// (hq-mt-bootstrap.8). `Some` when the composition root wires the workspace
    /// catalog: a *mutating* tool call whose resolved tenant is suspended/archived
    /// is rejected (reads + `workspace.resume` still pass). `None` keeps the legacy
    /// accept-all behaviour, so single-tenant / no-Postgres builds are unaffected.
    workspace_status: Option<Arc<dyn WorkspaceStatusGate>>,
    /// Per-workspace document reader for the `gt://doc/{id}` + `gt://issue/{id}` resource
    /// reads (hq-docs-api.3). `Some` when the composition root wires the document store:
    /// `gt://issue/{id}` then inlines a `documents` array, and `gt://doc/{id}` resolves a
    /// single document. `None` keeps the issues-only resource surface unchanged.
    documents: Option<Arc<dyn DocumentsResource>>,
    /// SSE-feed event sink for `issues.*` mutations (`hq-issues-sse`). `Some` when the
    /// composition root wires the event-log-backed sink: a successful `issues.*.execute`
    /// over MCP then appears on `GET /stream?channel=issues` — the agent-driven
    /// natural-movement source the REST surface alone would miss. `None` emits nothing,
    /// so the issues-only build is unchanged.
    issue_sink: Option<Arc<dyn IssueEventSink>>,
    /// Freshly-confirmed quota-block signal for the claim-context custodian
    /// (`hq-context-custodian.2`). `Some` when the composition root wires the workspace
    /// quota log: a `working` transition with no context is then *deferred* (not rejected)
    /// only when the pool is confirmed out of capacity. `None` keeps context mandatory,
    /// so the issues-only build is unchanged.
    quota_signal: Option<Arc<dyn QuotaBlockSignal>>,
}

/// The auth-free readiness probe tool (hq-mcp-ready-probe.1). No input schema,
/// no auth required — a caller invokes `ping` at startup; if the call resolves,
/// the server is live and action tools are safe to invoke.
fn ping_tool() -> McpTool {
    McpTool::new(
        "ping",
        "Readiness probe. Returns {ok:true,version,ts} without auth — \
         call this at startup to confirm the MCP server is live before \
         invoking any action tool.",
    )
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
        // Prepend the auth-free ping probe so it is always first in tools/list.
        let mut tools = tools;
        tools.insert(0, ping_tool());
        let tool_aliases = tools
            .iter()
            .map(|t| (sanitize_tool_name(&t.name), t.name.clone()))
            .collect();
        Self {
            store,
            workspaces: None,
            default_scope: Arc::new(default_scope),
            rbac,
            audit,
            tools: Arc::new(tools),
            tool_aliases: Arc::new(tool_aliases),
            repo_dir: repo_dir.map(Arc::new),
            domains: Arc::new(DomainRouter::new()),
            authenticator: None,
            pat: None,
            rig_prefixes: None,
            workspace_status: None,
            documents: None,
            issue_sink: None,
            quota_signal: None,
        }
    }

    /// Wire the SSE-feed event sink (`hq-issues-sse`): every successful `issues.*.execute`
    /// then publishes its [`IssueEvent`](gt_issues::IssueEvent) to `sink`, keyed to the
    /// resolved tenant, so an agent moving a bead over MCP shows up on
    /// `GET /stream?channel=issues`. Additive — without it the MCP path emits nothing.
    pub fn with_issue_sink(mut self, sink: Arc<dyn IssueEventSink>) -> Self {
        self.issue_sink = Some(sink);
        self
    }

    /// Wire the claim-context custodian's quota-block signal (`hq-context-custodian.2`):
    /// a `working` transition with no context is then deferred (with a debt note) instead
    /// of stop-the-line ONLY when `signal` confirms the workspace pool is freshly out of
    /// capacity. Additive — without it, context stays mandatory for every `working` claim.
    pub fn with_quota_signal(mut self, signal: Arc<dyn QuotaBlockSignal>) -> Self {
        self.quota_signal = Some(signal);
        self
    }

    /// Wire the per-workspace document reader (hq-docs-api.3): `gt://issue/{id}` then inlines
    /// the bead's `documents` array and `gt://doc/{id}` resolves one document. Additive —
    /// without it the resource surface is issues-only.
    pub fn with_documents(mut self, documents: Arc<dyn DocumentsResource>) -> Self {
        self.documents = Some(documents);
        self
    }

    /// Enable per-workspace JWT auth (`hq-mcp-dispatch.9`). With a verifier wired,
    /// every call must carry a valid `Authorization: Bearer` token; its `workspace`
    /// claim is the authoritative tenant (an `X-Workspace` header naming a different
    /// tenant is rejected) and its workspace-role scopes drive the per-tool check.
    /// Additive — without it the server keeps the `X-Actor`/`X-Workspace` behaviour.
    pub fn with_authenticator(
        mut self,
        authenticator: Arc<dyn Authenticator + Send + Sync>,
    ) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// Wire the Personal Access Token verifier (gt-core#6). With it, a `gtpat_…` bearer on the
    /// `/mcp` transport is verified through the async PAT port — a Postgres lookup — instead of
    /// being RS256-decoded by the JWT [`authenticator`](Self::with_authenticator), which would
    /// reject the opaque token as `malformed token: InvalidToken`. Its workspace-claim + scopes
    /// then drive the same leak gate + per-tool check a JWT does. Additive — without it a PAT is
    /// denied (never tried as a JWT), matching the REST chain.
    pub fn with_pat_authenticator(mut self, pat: Arc<dyn PatAuthenticator>) -> Self {
        self.pat = Some(pat);
        self
    }

    /// Wire the domain dispatch router (`hq-mcp-dispatch.1`): tool namespaces
    /// beyond `issues.*`/`meta.*` (workspace.*/rig.*/…) route to their registered
    /// [`DomainHandler`](crate::domain::DomainHandler). Additive — without it the
    /// server serves issues + meta only.
    pub fn with_domains(mut self, domains: Arc<DomainRouter>) -> Self {
        // Surface the handlers' tool descriptors (hq-mcp-native.3): fold them into
        // the advertised set so `tools/list` + `meta.help` list workspace.*/rig.*/…
        // alongside issues.*/meta.*, and extend the sanitized→canonical alias map so
        // a native client can invoke them by the sanitized name. Without this the
        // domain tools dispatch but stay undiscoverable. Additive — an empty router
        // (no `GT_PG_URL`) contributes nothing, so the issues-only build is unchanged.
        let domain_tools = domains.descriptors();
        if !domain_tools.is_empty() {
            let mut tools = (*self.tools).clone();
            let mut aliases = (*self.tool_aliases).clone();
            for tool in &domain_tools {
                aliases.insert(sanitize_tool_name(&tool.name), tool.name.clone());
            }
            tools.extend(domain_tools);
            self.tools = Arc::new(tools);
            self.tool_aliases = Arc::new(aliases);
        }
        self.domains = domains;
        self
    }

    /// Enable per-request workspace resolution (hq-mt-routing.5). With a resolver
    /// wired, a request carrying an `X-Workspace` header dispatches against that
    /// tenant's `hq_<ws>` store; a request without the header still uses the
    /// default-workspace [`store`](Self::store), so the change is additive.
    pub fn with_workspaces(mut self, workspaces: Arc<WorkspaceStores>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }

    /// Wire the per-workspace rig-prefix policy (hq-mt-rigs.6). With it, an
    /// `issues.create` whose bead-id prefix is not a registered rig prefix in the
    /// caller's workspace (nor a reserved prefix) is rejected. Additive — without it
    /// the server accepts any prefix, exactly as before.
    pub fn with_rig_prefixes(mut self, rig_prefixes: Arc<dyn WorkspaceRigPrefixes>) -> Self {
        self.rig_prefixes = Some(rig_prefixes);
        self
    }

    /// Wire the workspace lifecycle gate (hq-mt-bootstrap.8). With it, a *mutating*
    /// tool call whose resolved tenant is suspended/archived is rejected — reads and
    /// `workspace.resume` still pass. Additive — without it the server accepts
    /// mutations regardless of workspace status, exactly as before.
    pub fn with_workspace_status(mut self, gate: Arc<dyn WorkspaceStatusGate>) -> Self {
        self.workspace_status = Some(gate);
        self
    }

    /// The workspace resolver, if multi-tenant routing is wired. The health
    /// endpoints (hq-mt-routing.8) share it to report `workspaces_loaded` and
    /// per-workspace Dolt readiness.
    pub fn workspaces(&self) -> Option<Arc<WorkspaceStores>> {
        self.workspaces.clone()
    }

    /// Resolve the store for one request, given its authoritative workspace slug
    /// (the JWT claim's tenant when auth is wired, else the `X-Workspace` header).
    /// When a workspace resolver is wired and a slug is present, the tenant's own
    /// store is built (cheap: a lazily-pooled `DoltIssues`); otherwise the
    /// default-workspace store is used. A malformed/unknown slug is rejected here,
    /// before any dispatch.
    async fn resolve_store(&self, workspace: Option<&str>) -> Result<Arc<DoltIssues>, AppError> {
        match (&self.workspaces, workspace) {
            (Some(ws), Some(slug)) => Ok(Arc::new(ws.store_for(slug).await?)),
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

    /// Authorize one tool call and resolve its authoritative workspace.
    ///
    /// Two modes, selected by whether a JWT [`Authenticator`] is wired:
    ///
    /// - **JWT mode** (`hq-mcp-dispatch.9`): the `Authorization: Bearer` token is
    ///   verified, its claim validated (expiry + workspace presence), and the
    ///   request's `X-Workspace` header — if present — is reconciled against the
    ///   claim's `workspace`. A mismatch is the **cross-workspace leak gate**: a
    ///   token minted for tenant A cannot operate tenant B. The scope is derived
    ///   from the claim's workspace role ([`Scope::from_workspace_claim`]), and the
    ///   authoritative workspace returned is the claim's — never a spoofable header.
    /// - **Legacy mode** (no authenticator): the `X-Actor` header resolves the
    ///   scope (or the boot default) and the `X-Workspace` header the tenant,
    ///   exactly as before.
    ///
    /// Either way the deny-by-default [`Scope::check`] runs; a rejection (failed
    /// auth, leak, or out-of-scope tool) is recorded as an `Unauthorized` audit row
    /// and surfaces as an `invalid_request` the caller cannot retry. On success the
    /// resolved scope + authoritative workspace flow out for store dispatch + the
    /// `Invoked` audit attribution.
    ///
    /// Carved out of [`call_tool`](Self::call_tool) so the auth→scope→audit→error
    /// boundary is testable end to end: the rmcp [`RequestContext`] that
    /// `call_tool` receives wraps a `Peer` whose constructor is crate-private, so
    /// the wired gate cannot be exercised through `call_tool` itself — only
    /// through this seam (`tests::*` build the `Extensions` directly).
    async fn authorize(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ext: &Extensions,
    ) -> Result<(Cow<'_, Scope>, Option<String>), McpError> {
        let (scope, workspace) = match self.authenticate_claims(tool, args, ext).await? {
            Some(claims) => (
                Cow::Owned(Scope::from_workspace_claim(&claims.sub, &claims.scopes)),
                Some(claims.workspace),
            ),
            None => (
                self.resolve_scope(ext),
                workspace_from_ext(ext).map(str::to_string),
            ),
        };

        if let Err(e) = scope.check(tool) {
            // The tenant is already resolved here (the token was verified), so an
            // out-of-scope denial is attributed to its workspace — not "default".
            let _ = self.audit.record(
                AuditRecord::unauthorized(&scope.actor, tool, args.clone())
                    .in_workspace(workspace_or_default(&workspace)),
            );
            return Err(McpError::invalid_request(e.to_string(), None));
        }
        Ok((scope, workspace))
    }

    /// Reject a *mutating* tool call against a suspended / archived workspace
    /// (hq-mt-bootstrap.8) — the enforcement half of the suspend/archive
    /// transitions. Runs after [`authorize`](Self::authorize) (so the tenant is
    /// resolved + authenticated) and before any store I/O.
    ///
    /// A no-op when no [`WorkspaceStatusGate`] is wired, when the call resolved no
    /// workspace (the default tenant), when the tool is [non-mutating](is_mutation),
    /// or when the status is [`Active`](crate::GateStatus) / unknown. Only a known
    /// suspended/archived status blocks, and only for a mutation; `workspace.resume`
    /// is the recovery exception (classified as a read by [`is_mutation`]) so a
    /// suspended tenant can be brought back. A block is recorded as an
    /// `Unauthorized` audit row and surfaces as an `invalid_request`.
    async fn enforce_workspace_active(
        &self,
        tool: &str,
        args: &serde_json::Value,
        scope: &Scope,
        workspace: Option<&str>,
    ) -> Result<(), McpError> {
        let (Some(gate), Some(ws)) = (&self.workspace_status, workspace) else {
            return Ok(());
        };
        if !is_mutation(tool) {
            return Ok(());
        }
        // A lookup failure is the backend's fault, not the caller's: surface it as
        // an internal error rather than silently letting a mutation through.
        let status = gate
            .status(ws)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        match status {
            Some(s) if !s.allows_mutation() => {
                let _ = self.audit.record(
                    AuditRecord::unauthorized(&scope.actor, tool, args.clone()).in_workspace(ws),
                );
                Err(McpError::invalid_request(
                    format!(
                        "workspace `{ws}` is {} — mutating tool `{tool}` is rejected; \
                         reads still work, and `workspace.resume` restores it",
                        s.label()
                    ),
                    None,
                ))
            }
            // Active, or unknown (not in the catalog — the legacy/default tenant or a
            // not-yet-provisioned id, neither of which this gate governs).
            _ => Ok(()),
        }
    }

    /// Verify the request's bearer token and run the cross-workspace leak gate,
    /// returning the validated [`JwtClaims`] (whose `workspace` is the authoritative
    /// tenant). `None` when no authenticator is wired — the legacy header path. Used
    /// by both [`authorize`](Self::authorize) and `read_resource` so a resource read
    /// cannot dodge the tenant binding a tool call is held to. `op` labels an audit
    /// denial (the tool name or resource URI).
    async fn authenticate_claims(
        &self,
        op: &str,
        args: &serde_json::Value,
        ext: &Extensions,
    ) -> Result<Option<gt_auth::JwtClaims>, McpError> {
        let Some(auth) = &self.authenticator else {
            return Ok(None);
        };
        // A credential is mandatory in JWT mode: Authorization: Bearer, or the gt_web_token cookie.
        let token = token_from_ext(ext).ok_or_else(|| {
            self.deny(
                op,
                args,
                "<anonymous>",
                "missing bearer token or gt_web_token cookie",
            )
        })?;
        // A bearer shaped like a Personal Access Token (`gtpat_…`, gt-core#6) is verified through
        // the async PAT port — a Postgres lookup, not a signature check — and is NEVER RS256-decoded
        // (it could never be a valid JWT, so the JWT verifier would reject it as
        // `malformed token: InvalidToken`). A PAT with no verifier wired is a hard deny, never a
        // silent JWT fall-through. This mirrors the REST `authenticate` chain.
        let claims = if has_pat_prefix(token) {
            let pat = self.pat.as_ref().ok_or_else(|| {
                self.deny(
                    op,
                    args,
                    "<unverified>",
                    "personal access tokens are not configured",
                )
            })?;
            pat.verify(token, now_secs())
                .await
                .map_err(|()| self.deny(op, args, "<unverified>", "invalid personal access token"))?
        } else {
            auth.authenticate(token)
                .map_err(|e| self.deny(op, args, "<unverified>", &e.to_string()))?
        };
        // Cheap semantic gate: expiry + workspace-claim presence.
        claims
            .validate(now_secs(), JWT_WORKSPACE_OPTIONAL)
            .map_err(|e| self.deny(op, args, &claims.sub, &e.to_string()))?;
        // Cross-workspace leak gate: an X-Workspace naming a different tenant than
        // the token's claim is rejected outright.
        if let Some(requested) = workspace_from_ext(ext) {
            if requested != claims.workspace {
                return Err(self.deny(
                    op,
                    args,
                    &claims.sub,
                    &format!(
                        "token for workspace `{}` cannot operate workspace `{requested}`",
                        claims.workspace
                    ),
                ));
            }
        }
        Ok(Some(claims))
    }

    /// Record an `Unauthorized` audit row and build the matching `invalid_request`
    /// — the single deny path for every auth failure (bad token, leak, expiry).
    fn deny(&self, op: &str, args: &serde_json::Value, actor: &str, reason: &str) -> McpError {
        let _ = self
            .audit
            .record(AuditRecord::unauthorized(actor, op, args.clone()));
        McpError::invalid_request(reason.to_string(), None)
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
                // Declare the tools `listChanged` capability so a connected client
                // refetches `tools/list` when we emit the notification, rather than
                // requiring a manual reconnect (hq-mcp-native.4). Paired with the
                // `on_initialized` emit below.
                .enable_tool_list_changed()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
    }

    /// Fires once the client completes the initialize handshake (sends
    /// `notifications/initialized`). The served tool set is fixed for the process
    /// lifetime, but a client can connect and cache `tools/list` before the PG
    /// catalog finishes seeding on boot (gt-composition self-seeds the catalog),
    /// harvesting an incomplete set. Emitting `tools/list_changed` here nudges every
    /// client to refetch the full, sanitized tool list deterministically on
    /// (re)connect (hq-mcp-native.4).
    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        if let Err(err) = context.peer.notify_tool_list_changed().await {
            tracing::warn!(%err, "failed to emit tools/list_changed after initialize");
        }
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
                let mut schema = t.input_schema.as_object().cloned().unwrap_or_default();
                // MCP clients validate that inputSchema.type == "object" (strict).
                // Tools with no arguments default to {} — inject the required field.
                schema.entry("type").or_insert_with(|| serde_json::json!("object"));
                // Advertise the sanitized name (dots → underscores) so an Anthropic
                // client surfaces the tool; `call_tool` maps it back (hq-mcp-native.1).
                Tool::new(
                    sanitize_tool_name(&t.name),
                    t.description.clone(),
                    Arc::new(schema),
                )
            })
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // Translate the inbound name back to its canonical dotted form
        // (hq-mcp-native.1): a native Anthropic client invokes the sanitized name
        // `list_tools` advertised. Absent from the map ⇒ pass through unchanged (a
        // legacy client calling the dotted name directly, or a domain-router tool).
        // Everything below — authorize, the suspend gate, `is_mutation`, dispatch,
        // audit — keys on this canonical name.
        let tool = self
            .tool_aliases
            .get(request.name.as_ref())
            .cloned()
            .unwrap_or_else(|| request.name.to_string());
        let args = serde_json::Value::Object(request.arguments.clone().unwrap_or_default());

        // Auth-free readiness probe (hq-mcp-ready-probe.1): short-circuit before
        // authorize so the caller can confirm the server is live without a token.
        if tool == "ping" {
            let payload = serde_json::json!({
                "ok": true,
                "version": env!("CARGO_PKG_VERSION"),
                "ts": now_secs(),
            });
            return Ok(CallToolResult::success(vec![Content::text(
                payload.to_string(),
            )]));
        }

        // Per-connection scope (hq-core-mcp.6): the X-Actor header picks the
        // allow-list + the attribution actor, falling back to the boot default.
        // Deny-by-default — a rejection is audited as Unauthorized here and
        // surfaces as an invalid_request the caller cannot retry (hq-core-mcp.15
        // covers this gate end to end through `authorize`).
        // In JWT mode this also verifies the bearer token + closes the cross-
        // workspace leak; the authoritative workspace (the claim's tenant, or the
        // X-Workspace header in legacy mode) flows out for store + domain dispatch.
        let (scope, workspace) = self.authorize(&tool, &args, &context.extensions).await?;

        // Suspend/archive enforcement (hq-mt-bootstrap.8): a mutating call against a
        // suspended/archived tenant is rejected here, after the tenant is resolved
        // and before any store I/O. Reads + `workspace.resume` pass through.
        self.enforce_workspace_active(&tool, &args, &scope, workspace.as_deref())
            .await?;

        // Per-request workspace (hq-mt-routing.5): the resolved slug selects the
        // tenant's store; absent it, the default-workspace store. Resolved after
        // the scope check so a malformed slug from an unauthorized caller is never
        // built.
        let store = match self.resolve_store(workspace.as_deref()).await {
            Ok(store) => store,
            Err(e) => return Err(Self::to_mcp_error(&e)),
        };

        // Per-call request-root span (hq-mt-ops.2): the authoritative tenant is
        // recorded as the `workspace.id` attribute so every exported Tempo trace
        // is searchable per tenant. Every span opened deeper in the dispatch path
        // (domain handlers, `record_envelope`) hangs under this root, so the whole
        // trace tree carries the tenant.
        let span = call_span(&tool, workspace.as_deref());

        // Per-workspace request counter (hq-mt-deploy.8): one bump per dispatched
        // call, labelled by the resolved tenant (or `"default"` in legacy header
        // mode). Counted after authorize + the suspend gate so a denied call — which
        // returned above — is not a cost-attributable request. No-op until the bin's
        // `gt_telemetry::init` registers the counter.
        gt_telemetry::metrics::observe_workspace_request(workspace_or_default(&workspace));

        // Route by namespace (the segment before the first dot): meta.* self-
        // description, issues.*/board.* the tracker dispatch (the board is a
        // projection over the same Dolt store, hq-62130a), everything else the
        // domain router (hq-mcp-dispatch). An unowned namespace is an unknown tool.
        let result = async {
            match tool.split('.').next().unwrap_or("") {
                "meta" => {
                    dispatch_meta(&store, &tool, args.clone(), &scope.actor, &self.tools).await
                }
                "issues" | "board" => {
                    let repo_dir = self.repo_dir.as_deref().map(|p| p.as_path());
                    // hq-rig-isolation.7: request-level rig from X-Rig header; used as
                    // fallback in dispatch when the agent didn't pass rig in the args.
                    let request_rig = rig_from_ext(&context.extensions);
                    dispatch(
                        &store,
                        &tool,
                        args.clone(),
                        &scope.actor,
                        repo_dir,
                        workspace.as_deref(),
                        request_rig,
                        self.rig_prefixes.as_deref(),
                        self.issue_sink.as_deref(),
                        self.quota_signal.as_deref(),
                    )
                    .await
                }
                _ => {
                    let ctx = DomainCtx {
                        workspace: workspace.as_deref(),
                        actor: &scope.actor,
                        args: args.clone(),
                    };
                    match self.domains.dispatch(&tool, ctx).await {
                        Ok(Some(value)) => Ok(value),
                        Ok(None) => Err(AppError::NotFound(format!("unknown tool `{tool}`"))),
                        Err(e) => Err(e),
                    }
                }
            }
        }
        .instrument(span)
        .await;
        // Attribute the invocation to the authoritative tenant (the JWT claim's
        // workspace, hq-mcp-dispatch.9) so the audit trail is per-tenant filterable
        // (hq-mt-auth.7 SOC2 dump). Absent a resolved workspace (legacy header mode
        // with no X-Workspace) it falls back to the default tenant.
        let _ = self.audit.record(
            AuditRecord::invoked(&scope.actor, &tool, args)
                .in_workspace(workspace_or_default(&workspace)),
        );

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
                 from this list without re-checking the graph. Pass format=tsv for a \
                 dense tabular snapshot (header + one tab-separated row per bead, \
                 cheap scalar columns only) instead of JSON — fewer tokens on a \
                 list-heavy read; the JSON default still carries the full row shape.",
            ),
            mk(
                "gt://issue/{id}",
                "issue.detail",
                "Single bead WITH full text bodies + taxonomy columns + version. Read \
                 after claiming a bead to see the actual spec. When the document store is \
                 wired, also carries a `documents` array — the bead's attached .md / \
                 extracted-text supporting material (docs/11).",
            ),
        ];
        // gt://doc/{id} is discoverable only when a document reader is wired (hq-docs-api.3).
        let mut resources = resources;
        if self.documents.is_some() {
            resources.push(mk(
                "gt://doc/{id}",
                "document.detail",
                "Single document by id: .md body or a binary's extracted text + metadata \
                 (the supporting material a model reads when resolving a bead, docs/11).",
            ));
        }
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = request.uri.as_str();
        // hq-mcp-output-format: pull the optional `format=json|tsv` selector off the
        // querystring and run everything downstream against the cleaned URI, so the
        // unknown key never reaches `parse_issue_filter` (which rejects it).
        let (clean_uri, format) = split_format(uri).map_err(|e| Self::to_mcp_error(&e))?;
        // Resources are tenant data too: run the same JWT/leak gate as a tool call
        // (a token for ws A must not read ws B's resources), then resolve that
        // tenant's store so a read reflects the caller's workspace.
        let workspace = match self
            .authenticate_claims(&clean_uri, &serde_json::json!({}), &context.extensions)
            .await?
        {
            Some(claims) => Some(claims.workspace),
            None => workspace_from_ext(&context.extensions).map(str::to_string),
        };
        let store = self
            .resolve_store(workspace.as_deref())
            .await
            .map_err(|e| Self::to_mcp_error(&e))?;
        let payload = self
            .read_resource_json(&store, workspace.as_deref(), &clean_uri, format)
            .await
            .map_err(|e| Self::to_mcp_error(&e))?;
        // hq-mcp-output-format: compact JSON always (no pretty-print whitespace); a
        // `format=tsv` snapshot arrives already rendered as a dense table.
        let text = match payload {
            ResourcePayload::Json(value) => {
                serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
            }
            ResourcePayload::Text(text) => text,
        };
        // Echo the URI the client asked for (format selector intact) so it correlates.
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
        workspace: Option<&str>,
        uri: &str,
        format: OutputFormat,
    ) -> Result<ResourcePayload, AppError> {
        // gt://doc/{id} — one document by id (hq-docs-api.3). Only when a document reader is
        // wired; otherwise it falls through to the not-found at the end.
        if let Some(id) = uri.strip_prefix("gt://doc/") {
            if id.is_empty() || id.contains('/') {
                return Err(AppError::Validation(
                    "gt://doc/<id> expects a single non-empty path segment".into(),
                ));
            }
            if let Some(docs) = &self.documents {
                return match docs.get(workspace, id).await? {
                    Some(doc) => Ok(ResourcePayload::Json(doc)),
                    None => Err(AppError::NotFound(format!("document {id}"))),
                };
            }
        }

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
                let deps_map = store.depends_on_edges(&filter).await?;
                let deps = store.dep_index().await?;
                let repo_dir = self.repo_dir.as_deref().map(|p| p.as_path());
                let tree = crate::git_tree::surface_tree(repo_dir);
                let rows = gt_issues::resources::filter_ready(rows, &deps_map, open_phase, &deps, tree.as_ref());
                // hq-mcp-output-format: the frontier is list-heavy too — honour tsv.
                if format == OutputFormat::Tsv {
                    let parent_map = store.parent_map(
                        filter.rig.as_deref().unwrap_or(""),
                        filter.workspace.as_deref().unwrap_or(""),
                    ).await?;
                    return Ok(ResourcePayload::Text(gt_issues::resources::rows_to_tsv(
                        &rows,
                        &parent_map,
                    )));
                }
                return serde_json::to_value(&rows)
                    .map(ResourcePayload::Json)
                    .map_err(|e| AppError::Other(format!("encode issues: {e}")));
            }
            // hq-core-mcp.13: the default snapshot is a less-style PAGE — rows plus
            // `total`/`next_offset`/`has_more` so no caller mistakes one page for
            // the whole corpus. `?limit=N&offset=M` positions it.
            let page = gt_issues::resources::read_issues_page(store, &filter).await?;
            // hq-mcp-output-format: the dense tabular shape for the largest read.
            if format == OutputFormat::Tsv {
                let parent_map = store.parent_map(
                    filter.rig.as_deref().unwrap_or(""),
                    filter.workspace.as_deref().unwrap_or(""),
                ).await?;
                return Ok(ResourcePayload::Text(gt_issues::resources::page_to_tsv(
                    &page,
                    &parent_map,
                )));
            }
            return serde_json::to_value(&page)
                .map(ResourcePayload::Json)
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
                Some(detail) => {
                    let mut value = serde_json::to_value(&detail)
                        .map_err(|e| AppError::Other(format!("encode issue: {e}")))?;
                    // hq-docs-api.3: inline the bead's attached documents so a model reading
                    // the issue gets its supporting .md / extracted text in the same call.
                    if let (Some(docs), Some(obj)) = (&self.documents, value.as_object_mut()) {
                        let attached = docs.list_for_owner(workspace, id).await?;
                        obj.insert("documents".into(), serde_json::Value::Array(attached));
                    }
                    // Detail carries nested heavy bodies — always structured JSON
                    // (the bead's rule: tabular for lists, JSON when re-parsed).
                    Ok(ResourcePayload::Json(value))
                }
                None => Err(AppError::NotFound(format!("issue {id}"))),
            };
        }

        Err(AppError::NotFound(format!("resource {uri}")))
    }
}

/// Output selector parsed from a `gt://` URI's `format=` query param
/// (hq-mcp-output-format). `Json` (default) is compact JSON; `Tsv` is the dense
/// tabular shape, honoured only by the list-heavy `gt://issues` snapshot — detail
/// and `?full=1` reads stay JSON because their nested bodies must round-trip as
/// structured data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OutputFormat {
    Json,
    Tsv,
}

/// The resolved body of a resource read: a JSON value the caller serializes
/// compactly, or an already-rendered text block (the `format=tsv` table).
enum ResourcePayload {
    Json(serde_json::Value),
    Text(String),
}

/// Split an optional `format=json|tsv` selector out of a `gt://` URI's querystring,
/// returning the URI with that pair removed (so `parse_issue_filter` never sees the
/// unknown key) and the chosen [`OutputFormat`]. Absent ⇒ compact JSON. An
/// unrecognised value is a `Validation` error rather than a silent fallback.
fn split_format(uri: &str) -> Result<(String, OutputFormat), AppError> {
    let (base, qs) = match uri.split_once('?') {
        Some((b, q)) => (b, q),
        None => return Ok((uri.to_string(), OutputFormat::Json)),
    };
    let mut format = OutputFormat::Json;
    let mut kept: Vec<&str> = Vec::new();
    for pair in qs.split('&').filter(|p| !p.is_empty()) {
        match pair.split_once('=') {
            Some(("format", value)) => {
                format = match value {
                    "json" => OutputFormat::Json,
                    "tsv" => OutputFormat::Tsv,
                    other => {
                        return Err(AppError::Validation(format!(
                            "unknown format `{other}` (expected json|tsv)"
                        )))
                    }
                };
            }
            _ => kept.push(pair),
        }
    }
    let cleaned = if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok((cleaned, format))
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

/// The MCP boundary requires the workspace claim — a token without it cannot pin a
/// tenant, so the grace flag stays off here. (`gt-auth`'s `GT_JWT_WS_OPTIONAL` grace
/// is for issuers mid-rollout; an enforcement point that *binds* the workspace can't
/// honour a blank one.)
const JWT_WORKSPACE_OPTIONAL: bool = false;

/// Extract the bearer token from a request's `Authorization` header. rmcp injects
/// the HTTP `Parts` into each call's extensions, so the `Bearer <token>` value (the
/// scheme matched case-insensitively per RFC 6750) names the credential. `None` when
/// there is no Parts, no header, or no `Bearer` scheme.
fn bearer_from_ext(ext: &Extensions) -> Option<&str> {
    let raw = ext
        .get::<axum::http::request::Parts>()
        .and_then(|p| p.headers.get(axum::http::header::AUTHORIZATION))
        .and_then(|v| v.to_str().ok())?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?;
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Extract the `gt_web_token` JWT from the request's `Cookie` header — the same cookie the
/// SSE feed authenticates by (hq-mcp-cookie-auth). Lets a client that logged in once
/// authenticate `/mcp` calls by persisted cookie instead of resending `Authorization` each
/// run. Borrows the value out of the `Cookie` header; `None` when absent or empty.
fn cookie_token_from_ext(ext: &Extensions) -> Option<&str> {
    let raw = ext
        .get::<axum::http::request::Parts>()
        .and_then(|p| p.headers.get(axum::http::header::COOKIE))
        .and_then(|v| v.to_str().ok())?;
    raw.split(';')
        .filter_map(|kv| kv.trim().strip_prefix("gt_web_token="))
        .map(str::trim)
        .find(|t| !t.is_empty())
}

/// The MCP request credential: an `Authorization: Bearer` token first, else the
/// `gt_web_token` cookie (hq-mcp-cookie-auth). Both carry the same RS256 access JWT, so a
/// CLI/browser can present either.
fn token_from_ext(ext: &Extensions) -> Option<&str> {
    bearer_from_ext(ext).or_else(|| cookie_token_from_ext(ext))
}

/// Server-side wall clock (epoch seconds) for JWT expiry checks.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The resolved tenant for an audit record, or `"default"` when no workspace was
/// resolved (legacy header mode with no `X-Workspace`). Mirrors the `mcp_audit`
/// column default so the trail never carries a NULL tenant.
fn workspace_or_default(workspace: &Option<String>) -> &str {
    workspace.as_deref().unwrap_or("default")
}

/// Build the per-call request-root span (hq-mt-ops.2). It always carries the
/// `tool` and declares a `workspace.id` field, which is recorded only when a
/// tenant resolved — so a legacy header-mode call (no `X-Workspace`) leaves it
/// empty rather than guessing `default`, while a tenant-scoped call makes the
/// whole exported trace searchable by `workspace.id` in Tempo.
fn call_span(tool: &str, workspace: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        "mcp.call_tool",
        tool = %tool,
        workspace.id = tracing::field::Empty,
    );
    if let Some(ws) = workspace {
        span.record("workspace.id", ws);
    }
    span
}

/// Whether a tool call mutates tenant state, for the suspend/archive gate
/// (hq-mt-bootstrap.8). The classification is by the tool's *verb* (its last
/// dot-segment), so it holds across the two name shapes the server dispatches:
/// `issues.<action>.<verb>` (e.g. `issues.create.execute`) and the domain
/// `<namespace>.<verb>` (e.g. `workspace.list`).
///
/// A call is a **read** (never blocked) when:
/// - its verb is `validate` — a dry-run that by construction changes nothing;
/// - its verb is a known read (`list`/`info`/`get`/`status`/`query`/`explain`/
///   `owner`/`prefix`/`read`);
/// - it is `meta.help.execute` — a pure descriptor read despite the `execute` verb;
/// - it is `workspace.resume` — the recovery exception: a suspended tenant must be
///   able to come back, so its resume is allowed even though it mutates.
///
/// Everything else (every `.execute`, `meta.report-gap`, `workspace.create/suspend/
/// archive`, `rig.add`, …) is treated as a **mutation**. Defaulting unknown verbs to
/// "mutation" is the safe bias: a new write tool is gated until proven a read,
/// rather than silently slipping past a suspended workspace.
fn is_mutation(tool: &str) -> bool {
    // Recovery + pure-read exceptions that the verb rule alone would misclassify.
    if tool == "workspace.resume" {
        return false;
    }
    if tool == "meta.help.execute" {
        return false;
    }
    let verb = tool.rsplit('.').next().unwrap_or(tool);
    !matches!(
        verb,
        "validate"
            | "list"
            | "info"
            | "get"
            | "status"
            | "query"
            | "explain"
            | "owner"
            | "prefix"
            | "read"
    )
}

/// Sanitize a canonical dotted tool name (`issues.create.execute`) into an MCP
/// function name an Anthropic client accepts (hq-mcp-native.1). The kernel
/// namespaces tools with dots, but a surfaced `mcp__<server>__<tool>` name must
/// match `^[a-zA-Z0-9_-]+$` — a dot makes the whole name invalid and the client
/// drops the tool. Dots become underscores; dashes (e.g. `report-gap`) are already
/// valid and kept. The kernel's name segments never contain underscores, so the map
/// keyed on this output stays collision-free across the descriptor set.
fn sanitize_tool_name(name: &str) -> String {
    name.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use super::{
        actor_from_ext, call_span, cookie_token_from_ext, split_format, token_from_ext,
        IssuesServer, OutputFormat, PatAuthenticator,
    };
    use gt_audit::{AuditSink, InMemoryAudit, Outcome};
    use gt_auth::{InMemoryAuthenticator, JwtClaims};
    use gt_rbac::{RbacConfig, Scope, WORKSPACE_ADMIN, WORKSPACE_MEMBER};
    use gt_store_dolt::DoltIssues;
    use rmcp::model::{ErrorCode, Extensions};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn split_format_defaults_to_json_and_leaves_uri_untouched() {
        let (uri, fmt) = split_format("gt://issues?status=open").unwrap();
        assert_eq!(uri, "gt://issues?status=open");
        assert_eq!(fmt, OutputFormat::Json);
        // No querystring at all.
        let (uri, fmt) = split_format("gt://issue/a-1").unwrap();
        assert_eq!(uri, "gt://issue/a-1");
        assert_eq!(fmt, OutputFormat::Json);
    }

    #[test]
    fn split_format_strips_the_selector_from_the_filter_querystring() {
        // format= is pulled out so parse_issue_filter never sees the unknown key,
        // and the surviving filter pairs keep their order + join.
        let (uri, fmt) = split_format("gt://issues?status=open&format=tsv&priority_max=2").unwrap();
        assert_eq!(uri, "gt://issues?status=open&priority_max=2");
        assert_eq!(fmt, OutputFormat::Tsv);
        // Sole param: the cleaned URI drops the now-empty querystring entirely.
        let (uri, fmt) = split_format("gt://issues?format=tsv").unwrap();
        assert_eq!(uri, "gt://issues");
        assert_eq!(fmt, OutputFormat::Tsv);
    }

    #[test]
    fn split_format_rejects_an_unknown_format_value() {
        assert!(split_format("gt://issues?format=yaml").is_err());
    }

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

    /// Build extensions carrying an `Authorization: Bearer <token>` plus an optional
    /// `X-Workspace` header — the shape a JWT-mode request arrives with.
    fn ext_bearer(token: &str, workspace: Option<&str>) -> Extensions {
        let mut builder =
            axum::http::Request::builder().header("authorization", format!("Bearer {token}"));
        if let Some(ws) = workspace {
            builder = builder.header("x-workspace", ws);
        }
        let parts = builder.body(()).unwrap().into_parts().0;
        let mut ext = Extensions::new();
        ext.insert(parts);
        ext
    }

    /// A token whose claim is scoped to `workspace` with the given role scope, well
    /// clear of expiry (year 2286).
    fn token(sub: &str, workspace: &str, scope: &str) -> JwtClaims {
        JwtClaims {
            sub: sub.into(),
            workspace: workspace.into(),
            scopes: vec![scope.into()],
            exp: 9_999_999_999,
            nbf: None,
            iat: 0,
        }
    }

    #[test]
    fn reads_x_actor_header() {
        let ext = ext_with_header("x-actor", "alice");
        assert_eq!(actor_from_ext(&ext), Some("alice"));
    }

    #[test]
    fn reads_gt_web_token_cookie() {
        // The token is picked out of a multi-pair Cookie header, trimmed.
        let ext = ext_with_header("cookie", "foo=bar; gt_web_token=jwt.abc.def ; baz=qux");
        assert_eq!(cookie_token_from_ext(&ext), Some("jwt.abc.def"));
        // Absent cookie ⇒ None; an empty value ⇒ None.
        assert_eq!(
            cookie_token_from_ext(&ext_with_header("cookie", "foo=bar")),
            None
        );
        assert_eq!(
            cookie_token_from_ext(&ext_with_header("cookie", "gt_web_token=")),
            None
        );
    }

    #[test]
    fn token_prefers_bearer_then_falls_back_to_cookie() {
        // Bearer header wins when present.
        assert_eq!(
            token_from_ext(&ext_with_header("authorization", "Bearer hdr.tok")),
            Some("hdr.tok")
        );
        // No Authorization ⇒ the cookie is used.
        assert_eq!(
            token_from_ext(&ext_with_header("cookie", "gt_web_token=cookie.tok")),
            Some("cookie.tok")
        );
    }

    // ----- hq-mcp-native.1: dotted-name sanitization for native MCP clients ----

    use super::sanitize_tool_name;
    use gt_module::McpTool;

    /// Dots become underscores; dashes (`report-gap`) survive — the output matches
    /// the Anthropic function-name charset `^[a-zA-Z0-9_-]+$`.
    #[test]
    fn sanitize_replaces_dots_keeps_dashes() {
        assert_eq!(
            sanitize_tool_name("issues.create.execute"),
            "issues_create_execute"
        );
        assert_eq!(
            sanitize_tool_name("meta.report-gap.execute"),
            "meta_report-gap_execute"
        );
        // No dots ⇒ unchanged (a domain tool already underscore-free stays put).
        assert_eq!(sanitize_tool_name("workspace_list"), "workspace_list");
    }

    /// `get_info` advertises the tools `listChanged` capability (hq-mcp-native.4) so a
    /// client refetches `tools/list` when `on_initialized` emits the notification,
    /// instead of being stuck with a stale set until a manual reconnect.
    #[test]
    fn get_info_advertises_tool_list_changed_capability() {
        let srv = IssuesServer::new(
            Arc::new(
                DoltIssues::connect("mysql://gt@127.0.0.1:1/none").expect("lazy pool url parses"),
            ),
            Scope::admin("boot"),
            None,
            Arc::new(InMemoryAudit::new()),
            vec![],
            None,
        );
        let tools = rmcp::ServerHandler::get_info(&srv)
            .capabilities
            .tools
            .expect("tools capability is enabled");
        assert_eq!(
            tools.list_changed,
            Some(true),
            "listChanged must be advertised for the on_initialized emit to take effect"
        );
    }

    /// `new` builds the alias map keyed on the sanitized name, valued by the canonical
    /// dotted name — the exact lookup `call_tool` does to translate an inbound native
    /// call back before authorize/dispatch.
    #[test]
    fn new_builds_sanitized_to_canonical_alias_map() {
        let tool = |name: &str| McpTool::new(name, "");
        let srv = IssuesServer::new(
            Arc::new(
                DoltIssues::connect("mysql://gt@127.0.0.1:1/none").expect("lazy pool url parses"),
            ),
            Scope::admin("boot"),
            None,
            Arc::new(InMemoryAudit::new()),
            vec![
                tool("issues.create.execute"),
                tool("meta.report-gap.execute"),
            ],
            None,
        );
        assert_eq!(
            srv.tool_aliases
                .get("issues_create_execute")
                .map(String::as_str),
            Some("issues.create.execute"),
            "sanitized name maps back to the canonical dotted name"
        );
        assert_eq!(
            srv.tool_aliases
                .get("meta_report-gap_execute")
                .map(String::as_str),
            Some("meta.report-gap.execute")
        );
        // A name never advertised (canonical dotted, or a domain tool) is absent, so
        // `call_tool` passes it through unchanged.
        assert!(srv.tool_aliases.get("issues.create.execute").is_none());
    }

    // ----- hq-mcp-native.3: domain descriptors surface in the served tool set ----

    use crate::domain::{DomainCtx, DomainHandler, DomainRouter};

    /// Advertises one two-segment domain tool, to prove `with_domains` folds a
    /// handler's descriptors into the served set + the sanitized alias map.
    struct DescribedHandler;

    #[async_trait::async_trait]
    impl DomainHandler for DescribedHandler {
        fn namespace(&self) -> &'static str {
            "workspace"
        }
        async fn dispatch(
            &self,
            _tool: &str,
            _ctx: DomainCtx<'_>,
        ) -> Result<serde_json::Value, gt_store_dolt::AppError> {
            Ok(serde_json::json!({}))
        }
        fn descriptors(&self) -> Vec<McpTool> {
            vec![McpTool::with_schema(
                "workspace.create",
                "Provision a workspace.",
                serde_json::json!({
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"],
                }),
            )]
        }
    }

    /// `with_domains` surfaces every registered handler's descriptors in the served
    /// tool set and registers the sanitized→canonical alias — without this the tool
    /// dispatches but is undiscoverable (the hq-mcp-native.3 gap). The served set and
    /// the router come from one wiring, so this is not a reconstructed-harvest
    /// tautology: it asserts the fold the binary actually performs.
    #[test]
    fn with_domains_folds_handler_descriptors_into_the_served_set() {
        let tool = |name: &str| McpTool::new(name, "");
        let srv = IssuesServer::new(
            Arc::new(
                DoltIssues::connect("mysql://gt@127.0.0.1:1/none").expect("lazy pool url parses"),
            ),
            Scope::admin("boot"),
            None,
            Arc::new(InMemoryAudit::new()),
            vec![tool("issues.create.execute")],
            None,
        )
        .with_domains(Arc::new(
            DomainRouter::new().register(Arc::new(DescribedHandler)),
        ));

        // The domain tool now rides alongside the issues tool in the advertised set,
        let wc = srv
            .tools
            .iter()
            .find(|t| t.name == "workspace.create")
            .expect("domain descriptor folded into the served tool set");
        // carries a complete entry (non-empty description + object input schema),
        assert!(!wc.description.is_empty());
        assert_eq!(wc.input_schema["type"], "object");
        // the issues tool is still served,
        assert!(srv.tools.iter().any(|t| t.name == "issues.create.execute"));
        // and the sanitized name maps back so a native client can invoke it.
        assert_eq!(
            srv.tool_aliases.get("workspace_create").map(String::as_str),
            Some("workspace.create"),
        );
    }

    #[test]
    fn trims_and_rejects_blank() {
        assert_eq!(
            actor_from_ext(&ext_with_header("x-actor", "  bob ")),
            Some("bob")
        );
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

    #[tokio::test]
    async fn denied_default_scope_rejects_and_audits_unauthorized() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = server(Scope::denied("boot"), None, audit.clone());

        let err = srv
            .authorize("issues.create.execute", &json!({}), &Extensions::new())
            .await
            .expect_err("a closed scope must deny every tool");
        // invalid_request, not internal — a caller-side fault it cannot retry.
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1, "exactly one rejection is audited");
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].actor, "boot");
        assert_eq!(rows[0].tool, "issues.create.execute");
    }

    #[tokio::test]
    async fn wrong_scope_actor_is_denied_with_its_own_attribution() {
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
            .await
            .expect_err("create is outside watcher's update-only allow-list");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        // The rejection is attributed to the resolved actor, not the boot default.
        assert_eq!(rows[0].actor, "watcher");
    }

    #[tokio::test]
    async fn validate_only_actor_is_denied_execute_but_allowed_validate() {
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
            .await
            .expect_err("validate_only must block execute");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        // ...while the matching `.validate` passes the gate.
        let (scope, _ws) = srv
            .authorize("issues.create.validate", &json!({}), &ext)
            .await
            .expect("validate is permitted for a validate_only scope");
        assert_eq!(scope.actor, "readonly");

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1, "only the execute denial is audited");
        assert_eq!(rows[0].outcome, Outcome::Unauthorized);
        assert_eq!(rows[0].tool, "issues.create.execute");
    }

    #[tokio::test]
    async fn allowed_call_passes_the_gate_without_an_unauthorized_row() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = server(Scope::admin("boot"), None, audit.clone());

        let (scope, _ws) = srv
            .authorize("issues.close.execute", &json!({}), &Extensions::new())
            .await
            .expect("admin scope permits every tool");
        // The resolved scope flows out for store dispatch + Invoked attribution.
        assert_eq!(scope.actor, "boot");
        // `authorize` only records the deny path; the Invoked row is the caller's
        // to write after dispatch, so nothing is audited on the allow path here.
        assert!(audit.read_all().unwrap().is_empty());
    }

    // ----- hq-mcp-dispatch.9: per-workspace JWT auth + the leak gate ----------

    /// Build a server with a JWT verifier wired. The boot default is intentionally
    /// admin so a deny can only come from the JWT/workspace gate, not the fallback.
    fn auth_server(auth: InMemoryAuthenticator, audit: Arc<InMemoryAudit>) -> IssuesServer {
        server(Scope::admin("boot"), None, audit).with_authenticator(Arc::new(auth))
    }

    /// The headline guarantee: a token minted for workspace A cannot operate
    /// workspace B. Same-workspace passes (and pins the tenant to the claim);
    /// a mismatched `X-Workspace` is denied + audited; an absent header pins to the
    /// claim's workspace rather than trusting any header.
    #[tokio::test]
    async fn jwt_token_bound_to_its_workspace_blocks_cross_tenant() {
        let audit = Arc::new(InMemoryAudit::new());
        let auth = InMemoryAuthenticator::new()
            .with_token("tokA", token("agent-a", "acme", WORKSPACE_ADMIN));
        let srv = auth_server(auth, audit.clone());

        // Operating its own workspace: authorized, scope from the claim, tenant = acme.
        let (scope, ws) = srv
            .authorize(
                "issues.close.execute",
                &json!({}),
                &ext_bearer("tokA", Some("acme")),
            )
            .await
            .expect("a token operating its own workspace is allowed");
        assert_eq!(scope.actor, "agent-a", "attribution is the token subject");
        assert_eq!(
            ws.as_deref(),
            Some("acme"),
            "tenant resolved from the claim"
        );

        // Cross-workspace: X-Workspace=beta with an acme token — the leak gate fires.
        let err = srv
            .authorize(
                "issues.close.execute",
                &json!({}),
                &ext_bearer("tokA", Some("beta")),
            )
            .await
            .expect_err("a token for acme must not operate beta");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        // No X-Workspace header: pinned to the claim's workspace, never a header.
        let (_s, ws) = srv
            .authorize(
                "issues.close.execute",
                &json!({}),
                &ext_bearer("tokA", None),
            )
            .await
            .expect("an absent header pins to the claim");
        assert_eq!(ws.as_deref(), Some("acme"));

        let rows = audit.read_all().unwrap();
        let denials = rows
            .iter()
            .filter(|r| r.outcome == Outcome::Unauthorized)
            .count();
        assert_eq!(denials, 1, "only the cross-tenant attempt is audited");
        assert_eq!(rows[0].actor, "agent-a");
    }

    /// JWT mode requires a verifiable bearer token: a missing or unknown token is
    /// denied even though the boot default scope is admin.
    #[tokio::test]
    async fn jwt_missing_or_unverifiable_token_is_denied() {
        let audit = Arc::new(InMemoryAudit::new());
        let auth =
            InMemoryAuthenticator::new().with_token("good", token("a", "acme", WORKSPACE_ADMIN));
        let srv = auth_server(auth, audit.clone());

        let no_token = srv
            .authorize("issues.read", &json!({}), &Extensions::new())
            .await
            .expect_err("JWT mode requires a bearer token");
        assert_eq!(no_token.code, ErrorCode::INVALID_REQUEST);

        let bad_token = srv
            .authorize("issues.read", &json!({}), &ext_bearer("nope", Some("acme")))
            .await
            .expect_err("an unverifiable token is denied");
        assert_eq!(bad_token.code, ErrorCode::INVALID_REQUEST);

        assert_eq!(
            audit.read_all().unwrap().len(),
            2,
            "both failures are audited"
        );
    }

    /// An expired token is rejected by the claim's semantic gate.
    #[tokio::test]
    async fn jwt_expired_token_is_denied() {
        let audit = Arc::new(InMemoryAudit::new());
        let expired = JwtClaims {
            sub: "stale".into(),
            workspace: "acme".into(),
            scopes: vec![WORKSPACE_ADMIN.into()],
            exp: 1, // 1970 — well past
            nbf: None,
            iat: 0,
        };
        let auth = InMemoryAuthenticator::new().with_token("old", expired);
        let srv = auth_server(auth, audit.clone());

        let err = srv
            .authorize("issues.read", &json!({}), &ext_bearer("old", Some("acme")))
            .await
            .expect_err("an expired token is denied");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);
    }

    /// The claim's workspace role drives the per-tool scope: a member may run the
    /// operational tools but not provision a tenant (`workspace.create`).
    #[tokio::test]
    async fn jwt_member_role_cannot_create_a_workspace() {
        let audit = Arc::new(InMemoryAudit::new());
        let auth =
            InMemoryAuthenticator::new().with_token("mem", token("m", "acme", WORKSPACE_MEMBER));
        let srv = auth_server(auth, audit.clone());

        srv.authorize(
            "issues.close.execute",
            &json!({}),
            &ext_bearer("mem", Some("acme")),
        )
        .await
        .expect("a member operates the issue tools");

        let err = srv
            .authorize(
                "workspace.create",
                &json!({}),
                &ext_bearer("mem", Some("acme")),
            )
            .await
            .expect_err("a member cannot provision a tenant");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        // hq-mt-auth.8: the out-of-scope denial is attributed to the token's
        // workspace, not the default tenant — the trail is per-tenant filterable.
        let rows = audit.read_all().unwrap();
        let denial = rows.last().expect("the denial was audited");
        assert_eq!(denial.outcome, Outcome::Unauthorized);
        assert_eq!(denial.workspace_id, "acme");
    }

    // ----- gt-core#6: Personal Access Tokens on the /mcp transport ------------

    /// A PAT verifier double: accepts exactly one opaque `gtpat_` token, resolving it to
    /// admin claims for "pat-agent" in workspace "acme"; everything else is `Err(())`.
    struct OnePat(&'static str);

    #[async_trait::async_trait]
    impl PatAuthenticator for OnePat {
        async fn verify(&self, presented: &str, _now: u64) -> Result<JwtClaims, ()> {
            (presented == self.0)
                .then(|| token("pat-agent", "acme", WORKSPACE_ADMIN))
                .ok_or(())
        }
    }

    /// The fix: a `gtpat_…` bearer on /mcp is verified through the async PAT port — never
    /// RS256-decoded — so a PAT that already authenticates against REST now works on the
    /// transport too (its claim drives the scope + tenant, exactly like a JWT).
    #[tokio::test]
    async fn gtpat_bearer_is_verified_through_the_pat_port() {
        let audit = Arc::new(InMemoryAudit::new());
        // The JWT verifier knows no such token; only the PAT port resolves it.
        let srv = auth_server(InMemoryAuthenticator::new(), audit.clone())
            .with_pat_authenticator(Arc::new(OnePat("gtpat_good")));

        let (scope, ws) = srv
            .authorize(
                "issues.close.execute",
                &json!({}),
                &ext_bearer("gtpat_good", Some("acme")),
            )
            .await
            .expect("a valid PAT authenticates the MCP call");
        assert_eq!(scope.actor, "pat-agent", "attribution is the PAT subject");
        assert_eq!(ws.as_deref(), Some("acme"), "tenant resolved from the PAT claim");
        assert!(
            audit.read_all().unwrap().is_empty(),
            "a valid PAT is not a denial"
        );
    }

    /// An unknown `gtpat_…` bearer is denied through the PAT port (a 401-equivalent), not
    /// mistaken for a JWT.
    #[tokio::test]
    async fn unknown_gtpat_bearer_is_denied() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = auth_server(InMemoryAuthenticator::new(), audit.clone())
            .with_pat_authenticator(Arc::new(OnePat("gtpat_good")));

        let err = srv
            .authorize(
                "issues.read",
                &json!({}),
                &ext_bearer("gtpat_nope", Some("acme")),
            )
            .await
            .expect_err("an unknown PAT is denied");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);
        assert_eq!(audit.read_all().unwrap().len(), 1, "the deny is audited");
    }

    /// A `gtpat_…` bearer is NEVER tried as a JWT: with no PAT verifier wired it is a hard
    /// deny even when the JWT authenticator would (wrongly) accept the same string — the
    /// prefix short-circuits before the RS256 path, mirroring the REST chain.
    #[tokio::test]
    async fn gtpat_bearer_is_never_decoded_as_a_jwt() {
        let audit = Arc::new(InMemoryAudit::new());
        // The JWT verifier WOULD accept this exact string; the PAT prefix must short-circuit first.
        let auth = InMemoryAuthenticator::new()
            .with_token("gtpat_good", token("ghost", "acme", WORKSPACE_ADMIN));
        let srv = auth_server(auth, audit.clone());

        let err = srv
            .authorize(
                "issues.close.execute",
                &json!({}),
                &ext_bearer("gtpat_good", Some("acme")),
            )
            .await
            .expect_err("a PAT with no verifier wired is denied, never JWT-decoded");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);
        assert_eq!(audit.read_all().unwrap().len(), 1, "the deny is audited");
    }

    /// Legacy header mode (no authenticator): an out-of-scope denial is attributed to
    /// the `X-Workspace` header, and a request with no header falls back to "default".
    #[tokio::test]
    async fn legacy_denial_is_attributed_to_x_workspace_then_default() {
        let audit = Arc::new(InMemoryAudit::new());
        // A closed boot scope denies every tool, so each call audits one Unauthorized row.
        let srv = server(Scope::denied("watcher"), None, audit.clone());

        srv.authorize(
            "issues.create.execute",
            &json!({}),
            &ext_with_header("x-workspace", "globex"),
        )
        .await
        .expect_err("execute is out of scope");
        srv.authorize("issues.create.execute", &json!({}), &Extensions::new())
            .await
            .expect_err("execute is out of scope");

        let rows = audit.read_all().unwrap();
        assert_eq!(rows[0].workspace_id, "globex", "header tenant attributed");
        assert_eq!(
            rows[1].workspace_id, "default",
            "no header falls back to default"
        );
    }

    // ----- hq-mt-bootstrap.8: the suspend/archive enforcement gate -------------

    use super::is_mutation;
    use crate::ws_status::{GateStatus, WorkspaceStatusGate};
    use async_trait::async_trait;

    /// A read is never a mutation; the verb (last dot-segment) drives it across both
    /// name shapes, with `meta.help.execute` + `workspace.resume` as the exceptions.
    #[test]
    fn classifies_reads_writes_and_exceptions() {
        // Writes: every .execute, and the domain mutators.
        for w in [
            "issues.create.execute",
            "issues.close.execute",
            "issues.claim.execute",
            "issues.phase.advance",
            "meta.report-gap.execute",
            "workspace.create",
            "workspace.suspend",
            "workspace.archive",
            "rig.add",
        ] {
            assert!(is_mutation(w), "{w} should be a mutation");
        }
        // Reads: .validate dry-runs + the read verbs across namespaces.
        for r in [
            "issues.create.validate",
            "issues.close.validate",
            "workspace.list",
            "workspace.info",
            "rig.owner",
            "rig.prefix",
            "graph.query",
            "graph.explain",
            "graph.status",
        ] {
            assert!(!is_mutation(r), "{r} should be a read");
        }
        // The two exceptions the verb rule alone would misclassify.
        assert!(!is_mutation("meta.help.execute"), "help is a pure read");
        assert!(
            !is_mutation("workspace.resume"),
            "resume is the recovery exception"
        );
    }

    /// Stub gate: maps a fixed set of slugs to a status; anything else is unknown.
    struct StubStatus;
    #[async_trait]
    impl WorkspaceStatusGate for StubStatus {
        async fn status(&self, ws: &str) -> Result<Option<GateStatus>, gt_store_dolt::AppError> {
            Ok(match ws {
                "live" => Some(GateStatus::Active),
                "paused" => Some(GateStatus::Suspended),
                "dead" => Some(GateStatus::Archived),
                _ => None,
            })
        }
    }

    /// Build a server with the stub workspace-status gate wired (admin boot scope so
    /// the gate, not RBAC, is the only thing that can deny).
    fn gated_server(audit: Arc<InMemoryAudit>) -> IssuesServer {
        server(Scope::admin("boot"), None, audit).with_workspace_status(Arc::new(StubStatus))
    }

    /// A mutation against a suspended workspace is rejected + audited; against an
    /// archived one too. The audit row is attributed to the resolved tenant.
    #[tokio::test]
    async fn mutation_on_suspended_or_archived_is_blocked() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = gated_server(audit.clone());

        let err = srv
            .enforce_workspace_active(
                "issues.create.execute",
                &json!({}),
                &Scope::admin("boot"),
                Some("paused"),
            )
            .await
            .expect_err("a suspended tenant rejects mutations");
        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);

        srv.enforce_workspace_active(
            "workspace.create",
            &json!({}),
            &Scope::admin("boot"),
            Some("dead"),
        )
        .await
        .expect_err("an archived tenant rejects mutations");

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 2, "each block is one Unauthorized row");
        assert!(rows.iter().all(|r| r.outcome == Outcome::Unauthorized));
        assert_eq!(
            rows[0].workspace_id, "paused",
            "attributed to the resolved tenant"
        );
        assert_eq!(rows[1].workspace_id, "dead");
    }

    /// Reads + `workspace.resume` pass even on a suspended workspace; nothing audited.
    #[tokio::test]
    async fn reads_and_resume_pass_on_suspended() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = gated_server(audit.clone());
        let boot = Scope::admin("boot");

        for tool in [
            "issues.create.validate",
            "workspace.list",
            "workspace.info",
            "workspace.resume",
        ] {
            srv.enforce_workspace_active(tool, &json!({}), &boot, Some("paused"))
                .await
                .unwrap_or_else(|_| panic!("{tool} must pass on a suspended workspace"));
        }
        assert!(
            audit.read_all().unwrap().is_empty(),
            "a pass audits nothing"
        );
    }

    /// A mutation on an active workspace passes; an unknown slug passes (the gate only
    /// governs known suspended/archived tenants).
    #[tokio::test]
    async fn mutation_on_active_or_unknown_passes() {
        let audit = Arc::new(InMemoryAudit::new());
        let srv = gated_server(audit.clone());
        let boot = Scope::admin("boot");

        srv.enforce_workspace_active("issues.create.execute", &json!({}), &boot, Some("live"))
            .await
            .expect("an active tenant permits mutations");
        srv.enforce_workspace_active("issues.create.execute", &json!({}), &boot, Some("ghost"))
            .await
            .expect("an unknown tenant is not governed by the gate");
        assert!(audit.read_all().unwrap().is_empty());
    }

    /// No gate wired, or no resolved workspace ⇒ a no-op (legacy accept-all).
    #[tokio::test]
    async fn no_gate_or_no_workspace_is_noop() {
        let audit = Arc::new(InMemoryAudit::new());
        // No gate: even a mutation with a workspace passes.
        let ungated = server(Scope::admin("boot"), None, audit.clone());
        ungated
            .enforce_workspace_active(
                "issues.create.execute",
                &json!({}),
                &Scope::admin("boot"),
                Some("paused"),
            )
            .await
            .expect("no gate wired ⇒ accept-all");
        // Gate wired but no resolved workspace (default tenant) ⇒ pass.
        let srv = gated_server(audit.clone());
        srv.enforce_workspace_active(
            "issues.create.execute",
            &json!({}),
            &Scope::admin("boot"),
            None,
        )
        .await
        .expect("no resolved workspace ⇒ the default tenant, ungoverned");
        assert!(audit.read_all().unwrap().is_empty());
    }

    // --- hq-mt-ops.2: the per-call span carries `workspace.id` for Tempo ---

    use std::sync::Mutex;
    use tracing::field::{Field, Visit};
    use tracing::span;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    /// A test [`Layer`] that records every `(field, value)` set on any span,
    /// capturing both the values bound at creation and those filled in later via
    /// [`tracing::Span::record`] (the path `call_span` uses for `workspace.id`).
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<(String, String)>>>);

    struct FieldVisitor<'a>(&'a Captured);
    impl Visit for FieldVisitor<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0
                 .0
                .lock()
                .unwrap()
                .push((field.name().into(), value.into()));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                 .0
                .lock()
                .unwrap()
                .push((field.name().into(), format!("{value:?}")));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for Captured {
        fn on_new_span(&self, attrs: &span::Attributes<'_>, _id: &span::Id, _ctx: Context<'_, S>) {
            attrs.record(&mut FieldVisitor(self));
        }
        fn on_record(&self, _id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
            values.record(&mut FieldVisitor(self));
        }
    }

    /// Capture the fields recorded while building (and entering) a `call_span`.
    fn fields_for(tool: &str, workspace: Option<&str>) -> Vec<(String, String)> {
        let cap = Captured::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        tracing::subscriber::with_default(subscriber, || {
            let span = call_span(tool, workspace);
            let _enter = span.enter();
        });
        let out = cap.0.lock().unwrap().clone();
        out
    }

    #[test]
    fn call_span_records_resolved_workspace_id() {
        let fields = fields_for("workspace.list", Some("acme"));
        assert!(
            fields
                .iter()
                .any(|(k, v)| k == "workspace.id" && v == "acme"),
            "span must carry workspace.id=acme, got {fields:?}"
        );
        assert!(
            fields
                .iter()
                .any(|(k, v)| k == "tool" && v.contains("workspace.list")),
            "span must carry the tool name, got {fields:?}"
        );
    }

    #[test]
    fn call_span_leaves_workspace_id_empty_in_legacy_mode() {
        let fields = fields_for("issues.list.execute", None);
        // No tenant resolved ⇒ workspace.id is never recorded with a value.
        assert!(
            !fields
                .iter()
                .any(|(k, v)| k == "workspace.id" && !v.is_empty()),
            "workspace.id must stay empty without a resolved tenant, got {fields:?}"
        );
    }
}
