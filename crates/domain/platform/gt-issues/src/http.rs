//! The `axum` REST adapter for the `issues.*` surface (`hq-auth-routes.2`).
//!
//! This is the first [`GtModule`](gt_module::GtModule) HTTP surface and the template the
//! `hq-fe-api` epic copies: it exposes the issues tracker as REST routes that dispatch to the
//! **same** transport-free [`handlers`](crate::handlers) and [`resources`](crate::resources)
//! the MCP tools use — no domain logic is duplicated, only the wire shape differs. It folds
//! into `gt-issues` behind the off-by-default `axum` feature (docs/03 Rule 4), exactly as
//! gt-workspace/gt-auth gate their axum adapters, rather than living in a sibling crate.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/issues` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `issues.read` / `issues.write` for the request's verb-class.
//! - **It never reads a workspace from the path or body** (docs/04 §15, docs/03 Rule 6). The
//!   issue id is a plain resource identifier; the tenant a request belongs to is the auth
//!   context's concern, resolved upstream, never a route parameter.
//! - **It skips the git-backed S2/S3 checks.** Surface existence (S3) and delivered-commit
//!   verification (S2) shell out to `git`, which the domain tier may not do — those adapters
//!   live in the orchestration `gt-mcp-server`. The REST path therefore validates against the
//!   permissive [`AllowAllTree`] and passes no [`CommitInspector`](crate::CommitInspector),
//!   exactly the degraded mode the MCP path runs in when `GT_REPO_DIR` is unset. Every other
//!   domain rule — shape validation, NN-16 taxonomy, optimistic concurrency, the status state
//!   machine, and the "code surface ⇒ commit_sha required" close rule — still runs, because it
//!   lives in the shared handlers.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use gt_store_dolt::{
    AppError, ClaimOutcome, DoltIssues, IssueDetail, IssueFilter, IssuePage, WorkspacePools,
};
use gt_workspace::WorkspaceContext;

use crate::commands::{ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue};
use crate::events::{emit_issue_event, IssueEventSink, IssueVerb};
use crate::handlers::{
    run_claim_issue, run_close_issue, run_create_issue, run_transition_issue, run_update_issue,
};
use crate::resources::{read_issue, read_issues, read_issues_page};
use crate::stats::{aggregate, GroupDim, StatsResponse};
use crate::surface::AllowAllTree;

/// Everything the issues REST handlers need, baked into the router with
/// [`Router::with_state`] before it leaves [`issues_router`] so the merged application router
/// carries no outstanding state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct IssuesApiState {
    /// The live Dolt issues adapter the binary supplies — the module owns no store of its own.
    /// In single-tenant mode this is the only store; in multi-tenant mode it is the
    /// seed/fallback (the default workspace's `GT_DOLT_URL` store), used when no per-workspace
    /// resolver is wired.
    store: Arc<DoltIssues>,
    /// Per-workspace Dolt pool cache, present only when the binary runs multi-tenant
    /// (`GT_DOLT_BASE_URL` set). When `Some`, every request resolves its tenant's own
    /// `hq_<ws>` store from the verified workspace — the REST mirror of the MCP path's
    /// claim-scoped routing (`hq-gap-issues-rest-workspace-routing`). When `None`, the REST
    /// surface stays single-tenant on `store`, exactly as before.
    workspaces: Option<Arc<WorkspacePools>>,
    /// The attribution actor stamped on `close`/`claim`, server-injected the same way the MCP
    /// path uses `GT_MCP_ACTOR`. It is the server identity, not the per-request caller, so a
    /// request body can never spoof it.
    actor: Arc<str>,
    /// The SSE-feed sink every successful mutation publishes its [`IssueEvent`](crate::events::IssueEvent)
    /// to (`hq-issues-sse`). `Some` when the composition root wires the event-log-backed sink:
    /// a create/update/transition/close/claim then appears on `GET /stream?channel=issues`
    /// keyed to the request's workspace. `None` keeps the REST surface emit-free (no feed), so a
    /// build that wires no event log behaves exactly as before.
    event_sink: Option<Arc<dyn IssueEventSink>>,
}

impl IssuesApiState {
    /// Build the REST state over a live store and the server attribution actor.
    /// Single-tenant by default; call [`with_workspaces`](Self::with_workspaces) to enable
    /// per-request tenant routing.
    pub fn new(store: Arc<DoltIssues>, actor: impl Into<Arc<str>>) -> Self {
        Self {
            store,
            workspaces: None,
            actor: actor.into(),
            event_sink: None,
        }
    }

    /// Enable multi-tenant routing: each request resolves its tenant's `hq_<ws>` store from the
    /// shared per-workspace pool cache. Without this the REST surface serves the single
    /// fallback `store` for every caller.
    pub fn with_workspaces(mut self, pools: Arc<WorkspacePools>) -> Self {
        self.workspaces = Some(pools);
        self
    }

    /// Wire the SSE-feed event sink (`hq-issues-sse`): every successful mutation then publishes
    /// its [`IssueEvent`](crate::events::IssueEvent) to `sink`, keyed to the request's workspace,
    /// so a frontend sees the tracker move on `GET /stream?channel=issues` without polling.
    /// Additive — without it the REST surface emits nothing, exactly as before.
    pub fn with_event_sink(mut self, sink: Arc<dyn IssueEventSink>) -> Self {
        self.event_sink = Some(sink);
        self
    }

    /// Resolve the Dolt store for one request from its verified workspace. In multi-tenant mode
    /// the tenant's own `hq_<ws>` pool is selected (the slug is already validated by the
    /// `WorkspaceContext` extractor + `pool_for`) and its schema is ensured on first access (F2)
    /// via [`WorkspacePools::ensured_pool`], so a REST-created tenant self-heals an empty
    /// `hq_<ws>` exactly once per slug per process; single-tenant falls back to the shared
    /// `store`, ignoring the workspace exactly as the pre-routing behaviour did.
    async fn resolve(&self, ctx: &WorkspaceContext) -> Result<Arc<DoltIssues>, AppError> {
        match &self.workspaces {
            Some(pools) => Ok(Arc::new(DoltIssues::new(
                pools.ensured_pool(ctx.workspace().as_str()).await?,
            ))),
            None => Ok(self.store.clone()),
        }
    }
}

/// Build the issues REST router with `state` baked in (`hq-auth-routes.2`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/issues` and applies the
/// scope guard. `register_routes` on [`IssuesModule`](crate::IssuesModule) returns exactly this
/// router when the module carries HTTP state.
///
/// | Method + path             | Maps to MCP tool        |
/// |---------------------------|-------------------------|
/// | `GET /`                   | `gt://issues` snapshot  |
/// | `GET /:id`                | `gt://issue/{id}`       |
/// | `POST /`                  | `issues.create.execute` |
/// | `PATCH /:id`              | `issues.update.execute` |
/// | `POST /:id/transition`    | `issues.transition.execute` |
/// | `POST /:id/close`         | `issues.close.execute`  |
/// | `POST /:id/claim`         | `issues.claim.execute`  |
/// | `GET /stats`              | aggregation (`hq-web-extras.12`) |
pub fn issues_router(state: IssuesApiState) -> Router {
    Router::new()
        .route("/", get(list_issues).post(create_issue))
        // Concrete `/stats` is registered before the `/:id` wildcard so the GET never matches
        // an issue literally named "stats"; axum routes the more specific path first regardless,
        // but ordering it here keeps the intent obvious.
        .route("/stats", get(issue_stats))
        .route("/:id", get(get_issue).patch(update_issue))
        .route("/:id/transition", post(transition_issue))
        .route("/:id/close", post(close_issue))
        .route("/:id/claim", post(claim_issue))
        .with_state(state)
}

/// Querystring for `GET /` — the REST mirror of the `gt://issues` filter. Every field is
/// optional; an empty query lists the default page. `status` is a comma-separated list (a
/// querystring carries no native array), mapped onto [`IssueFilter::status`].
#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    /// Comma-separated `status` values (e.g. `open,working`); empty ⇒ no status filter.
    status: Option<String>,
    /// Keep only `priority <= priority_max`.
    priority_max: Option<u8>,
    /// Exact `assignee` match (`""` is the canonical unassigned value).
    assignee: Option<String>,
    /// Exact `external_ref` match (epic linkage).
    external_ref: Option<String>,
    /// Exact `issue_type` match.
    issue_type: Option<String>,
    /// Page size; unset ⇒ the store's configured default.
    limit: Option<u32>,
    /// Zero-based row offset for less-style paging.
    offset: Option<u32>,
    /// Inline the heavy text bodies on every row (`?full=true`).
    #[serde(default)]
    full: bool,
}

impl ListQuery {
    /// Map the parsed querystring onto the store's [`IssueFilter`]. `?ready=` is intentionally
    /// not exposed on REST yet: readiness needs the phase frontier + delivered index + git tree
    /// the domain tier does not hold, so it stays an MCP-resource concern.
    fn into_filter(self) -> IssueFilter {
        IssueFilter {
            status: self
                .status
                .map(|s| {
                    s.split(',')
                        .filter(|p| !p.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            priority_max: self.priority_max,
            assignee: self.assignee,
            external_ref: self.external_ref,
            issue_type: self.issue_type,
            limit: self.limit,
            offset: self.offset,
            full: self.full,
            ready: false,
        }
    }
}

/// `GET /` — a paged snapshot of the tracker (`gt://issues`). Reuses
/// [`read_issues_page`](crate::resources::read_issues_page) so the page semantics match the MCP
/// resource exactly.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    params(
        ("status" = Option<String>, Query, description = "Comma-separated status filter"),
        ("priority_max" = Option<u8>, Query, description = "Keep priority <= this"),
        ("assignee" = Option<String>, Query, description = "Exact assignee match"),
        ("external_ref" = Option<String>, Query, description = "Exact external_ref (epic) match"),
        ("issue_type" = Option<String>, Query, description = "Exact issue_type match"),
        ("limit" = Option<u32>, Query, description = "Page size"),
        ("offset" = Option<u32>, Query, description = "Zero-based row offset"),
        ("full" = Option<bool>, Query, description = "Inline heavy text bodies"),
    ),
    responses((status = 200, description = "A page of issues")),
))]
async fn list_issues(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Query(q): Query<ListQuery>,
) -> Result<Json<IssuePage>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let page = read_issues_page(&store, &q.into_filter()).await?;
    Ok(Json(page))
}

/// Querystring for `GET /stats` (`hq-web-extras.12`). `group_by` is the required, comma-separated
/// list of dimensions (`epic|rig|status|domain|assignee|owner`, combinable); the optional filters
/// mirror [`ListQuery`] so a caller can scope the aggregation to a sub-slice (e.g. one epic) before
/// it is rolled up.
#[derive(Debug, Default, Deserialize)]
struct StatsQuery {
    /// Required comma-separated grouping dimensions (e.g. `assignee,rig`).
    group_by: Option<String>,
    /// Optional comma-separated `status` pre-filter (e.g. `open,working`).
    status: Option<String>,
    /// Optional `priority <= priority_max` pre-filter.
    priority_max: Option<u8>,
    /// Optional exact `assignee` pre-filter (`""` = unassigned).
    assignee: Option<String>,
    /// Optional exact `external_ref` (epic) pre-filter.
    external_ref: Option<String>,
    /// Optional exact `issue_type` pre-filter.
    issue_type: Option<String>,
}

/// `GET /stats` — per-workspace aggregation for the statistics view (`gt-web.5`, `hq-web-extras.12`).
///
/// Returns counts `{open,working,closed,other,total}` + `progress_pct` (`closed/total`) +
/// `lead_time` (from `created_at`/`closed_at` on closed beads) for each bucket of the requested
/// `group_by` dimensions, plus an ungrouped `totals` roll-up. The aggregation runs over the
/// tracker rows of the active workspace (the same auth-scoped store the other handlers use —
/// tenant resolution is upstream, never a route param); the math itself lives in transport-free
/// [`crate::stats`]. Scope: `issues.read` (the GET verb-class the builder guards).
///
/// An unknown or empty `group_by` is a `422` (client error), matching the MCP parse path. The
/// rows are pulled with a high page limit (clamped by `GT_ISSUES_MAX_LIMIT`) so the aggregate
/// covers the whole workspace tracker in one pass.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/stats",
    params(
        ("group_by" = String, Query, description = "Comma-separated dimensions: epic|rig|status|domain|assignee|owner (combinable)"),
        ("status" = Option<String>, Query, description = "Comma-separated status pre-filter"),
        ("priority_max" = Option<u8>, Query, description = "Keep priority <= this"),
        ("assignee" = Option<String>, Query, description = "Exact assignee pre-filter"),
        ("external_ref" = Option<String>, Query, description = "Exact external_ref (epic) pre-filter"),
        ("issue_type" = Option<String>, Query, description = "Exact issue_type pre-filter"),
    ),
    responses(
        (status = 200, description = "Per-bucket counts + progress% + lead_time, plus ungrouped totals", body = StatsResponse),
        (status = 422, description = "Missing/unknown group_by dimension"),
    ),
))]
async fn issue_stats(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Query(q): Query<StatsQuery>,
) -> Result<Json<StatsResponse>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let dims = GroupDim::parse_list(q.group_by.as_deref().unwrap_or_default())
        .map_err(|e| ApiError(AppError::Validation(e)))?;
    // Pull the whole workspace tracker (cheap snapshot rows; `full=false`). A `None` limit would
    // cap at GT_ISSUES_DEFAULT_LIMIT (~200) and silently undercount, so request the max ceiling.
    let filter = IssueFilter {
        status: q
            .status
            .map(|s| {
                s.split(',')
                    .filter(|p| !p.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        priority_max: q.priority_max,
        assignee: q.assignee,
        external_ref: q.external_ref,
        issue_type: q.issue_type,
        limit: Some(gt_store_dolt::issues_max_limit()),
        offset: None,
        full: false,
        ready: false,
    };
    let rows = read_issues(&store, &filter).await?;
    Ok(Json(aggregate(&rows, &dims)))
}

/// `GET /:id` — one issue with its heavy bodies + `version` (`gt://issue/{id}`); `404` when no
/// row matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "The issue with heavy bodies + version"),
        (status = 404, description = "No issue with that id"),
    ),
))]
async fn get_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
) -> Result<Json<IssueDetail>, ApiError> {
    let store = st.resolve(&ctx).await?;
    match read_issue(&store, &id).await? {
        Some(detail) => Ok(Json(detail)),
        None => Err(ApiError(AppError::NotFound(format!("issue {id}")))),
    }
}

/// `POST /` — create a bead (`issues.create.execute`). The id lives in the body (it names the
/// new resource, not an existing path), uniqueness is enforced by the store. `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Issue created"),
        (status = 422, description = "Validation failed (shape / NN-16 taxonomy)"),
    ),
))]
async fn create_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Json(args): Json<CreateIssue>,
) -> Result<Response, ApiError> {
    let store = st.resolve(&ctx).await?;
    run_create_issue(&store, &args, &AllowAllTree, false).await?;
    emit_issue_event(
        st.event_sink.as_deref(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Created,
        &args.id,
        st.actor.as_ref(),
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "ok": true, "id": args.id })),
    )
        .into_response())
}

/// `PATCH /:id` — patch editable fields (`issues.update.execute`). The id always comes from the
/// path and overwrites any `id` in the body (docs/03 Rule 6: identity is never trusted from the
/// payload). Returns the post-bump `version` so a follow-up edit can chain `expected_version`.
#[cfg_attr(feature = "axum", utoipa::path(
    patch, path = "/{id}",
    params(("id" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Patched; returns the post-bump version"),
        (status = 422, description = "Validation failed (incl. version conflict)"),
    ),
))]
async fn update_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let args: UpdateIssue = with_path_id(body, id)?;
    let version = run_update_issue(&store, &args, &AllowAllTree, false).await?;
    emit_issue_event(
        st.event_sink.as_deref(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Updated,
        &args.id,
        st.actor.as_ref(),
    )
    .await;
    let mut body = json!({ "ok": true });
    if let Some(v) = version {
        body["version"] = json!(v);
    }
    Ok(Json(body))
}

/// `POST /:id/transition` — move the status across the open/working/closed machine
/// (`issues.transition.execute`). Illegal moves are rejected by the store's status-guarded
/// update and surface as `422`.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/transition",
    params(("id" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Status transitioned"),
        (status = 422, description = "Illegal or invalid transition"),
    ),
))]
async fn transition_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let args: TransitionIssue = with_path_id(body, id)?;
    run_transition_issue(&store, &args, false).await?;
    emit_issue_event(
        st.event_sink.as_deref(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Transitioned,
        &args.id,
        st.actor.as_ref(),
    )
    .await;
    Ok(Json(json!({ "ok": true })))
}

/// `POST /:id/close` — close the bead (`issues.close.execute`). The attribution actor is the
/// server identity in [`IssuesApiState`], never the body. A bead with a non-planned code
/// surface still requires `commit_sha` (delivered-code proof), enforced by the shared handler;
/// the git delivery verification (S2) is skipped on the REST path (see the module docs).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/close",
    params(("id" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Issue closed"),
        (status = 422, description = "Validation failed (e.g. missing commit_sha for a code surface)"),
        (status = 404, description = "No issue with that id"),
    ),
))]
async fn close_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let args: CloseIssue = with_path_id(body, id)?;
    run_close_issue(&store, &args, &st.actor, None, false).await?;
    emit_issue_event(
        st.event_sink.as_deref(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Closed,
        &args.id,
        st.actor.as_ref(),
    )
    .await;
    Ok(Json(json!({ "ok": true })))
}

/// `POST /:id/claim` — atomically claim an open, unowned bead for the server actor
/// (`issues.claim.execute`). A lost claim is a `409 Conflict` naming the current holder, so the
/// caller cannot proceed as if it owns the bead; a win echoes the post-bump `version` and the
/// S5 prose/phase fields.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/claim",
    params(("id" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Claim won; echoes version + S5 fields"),
        (status = 409, description = "Already claimed by another holder"),
        (status = 404, description = "No issue with that id"),
    ),
))]
async fn claim_issue(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let args: ClaimIssue = with_path_id(body, id)?;
    let res = run_claim_issue(&store, &args, &st.actor, false).await?;
    match res.outcome {
        ClaimOutcome::Won => {
            // Only a won claim mutated the row — a lost claim left it untouched, so it
            // emits no event.
            emit_issue_event(
                st.event_sink.as_deref(),
                &store,
                Some(ctx.workspace().as_str()),
                IssueVerb::Claimed,
                &args.id,
                st.actor.as_ref(),
            )
            .await;
            let mut body = json!({ "outcome": "won", "owner": st.actor.as_ref() });
            if let Some(v) = res.version {
                body["version"] = json!(v);
            }
            if let Some(d) = res.description {
                body["description"] = json!(d);
            }
            if let Some(ac) = res.acceptance_criteria {
                body["acceptance_criteria"] = json!(ac);
            }
            if let Some(p) = res.phase {
                body["phase"] = json!(p);
            }
            Ok(Json(body))
        }
        // A lost claim must not look like success — surface the holder as a conflict.
        ClaimOutcome::Lost { status, holder } => Err(ApiError(AppError::InvalidTransition(
            format!("already claimed: {holder} holds it (status={status})"),
        ))),
    }
}

/// The combined OpenAPI document for the issues REST surface (`hq-auth-routes.2`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/issues/...`,
/// so the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list_issues,
        issue_stats,
        get_issue,
        create_issue,
        update_issue,
        transition_issue,
        close_issue,
        claim_issue,
    ),
    components(schemas(
        StatsResponse,
        crate::stats::StatBucket,
        crate::stats::LeadTime,
        GroupDim,
    ))
)]
pub struct ApiDoc;

/// Deserialize a path-addressed command body, forcing the `id` to the path segment so it is
/// never trusted from the payload (docs/03 Rule 6). The path id overwrites any `id` in the body
/// and supplies it when absent, so a REST client need not repeat the id it already named in the
/// URL. A malformed body surfaces as a `422` (validation), matching the MCP `parse_args` path.
fn with_path_id<T: serde::de::DeserializeOwned>(
    mut body: Value,
    id: String,
) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert("id".to_string(), Value::String(id));
        }
        // A non-object body (e.g. `null` from an empty request) becomes just the id.
        other => *other = json!({ "id": id }),
    }
    serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))
}

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and have it
/// rendered with the right status. The body is the bare error message — the same text the MCP
/// path surfaces — so a client sees an identical reason across transports.
struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::InvalidTransition(_) => StatusCode::CONFLICT,
            AppError::Handler(_) | AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        // The spec the builder mounts: paths are relative (no `/api/v1/issues`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in [
            "/",
            "/stats",
            "/{id}",
            "/{id}/transition",
            "/{id}/close",
            "/{id}/claim",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/issues`.
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn list_query_maps_comma_status_and_flags() {
        let q = ListQuery {
            status: Some("open,working".into()),
            priority_max: Some(1),
            full: true,
            limit: Some(5),
            ..Default::default()
        };
        let f = q.into_filter();
        assert_eq!(f.status, vec!["open".to_string(), "working".to_string()]);
        assert_eq!(f.priority_max, Some(1));
        assert!(f.full);
        assert_eq!(f.limit, Some(5));
        // REST never sets the readiness frontier flag.
        assert!(!f.ready);
    }

    #[test]
    fn empty_status_yields_no_status_filter() {
        // A bare `?status=` (or absent) must not become a `[""]` filter that matches nothing.
        let f = ListQuery {
            status: Some(String::new()),
            ..Default::default()
        }
        .into_filter();
        assert!(f.status.is_empty());
        assert!(ListQuery::default().into_filter().status.is_empty());
    }

    #[test]
    fn error_status_mapping_matches_app_error_kinds() {
        let cases = [
            (AppError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (
                AppError::Validation("x".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                AppError::InvalidTransition("x".into()),
                StatusCode::CONFLICT,
            ),
            (
                AppError::Handler("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                AppError::Other("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (err, want) in cases {
            assert_eq!(ApiError(err).into_response().status(), want);
        }
    }
}
