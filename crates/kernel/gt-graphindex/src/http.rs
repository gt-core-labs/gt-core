//! The `axum` REST adapter for the read-only `graph.*` surface (`hq-fe-api-kernel.1`).
//!
//! Exposes the codebase knowledge graph as REST routes that dispatch to the **same**
//! [`GraphIndexer`] port the MCP `graph.*` tools use — no domain logic is duplicated, only the
//! wire shape differs. It folds into `gt-graphindex` behind the off-by-default `axum` feature
//! (docs/03 Rule 4), exactly as gt-quota/gt-orchestration gate theirs, rather than living in a
//! sibling crate.
//!
//! ## Read-only, per-rig, per-workspace
//!
//! These are the routes other agents use to **consult** a rig's graph for context: `query`,
//! `explain`, `status`, and `list`. The custodian writes (`refresh` / `refresh-stale`) stay an
//! MCP-only concern — they shell out to `git` and append warden events, which the domain tier
//! skips (the issues-S2/S3 precedent), so they are intentionally absent from the REST surface.
//!
//! The repo a query runs against is resolved per-rig by the [`WorkspaceGraph`] provider, which
//! stands in for the MCP handler's warden-state replay: a query only works for a rig the warden
//! has under custody (`graph.list`). The provider is keyed by the request's workspace, read
//! straight off the `X-Workspace` header — **kernel-pure**, so this crate takes no
//! `gt-workspace` dependency (a forbidden kernel→domain edge). An absent header resolves to the
//! single-tenant [`DEFAULT_WORKSPACE`], matching the MCP dispatch default.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/graph` and wraps it with the capability-derived scope guard; the composition root
//!   layers the auth middleware in front. Every route is a GET, so the guard requires `graph.read`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::port::{GraphError, GraphIndexer};

/// The header carrying the request's tenant — server-injected by the auth/workspace middleware,
/// never read from a URL or body field (the same `X-Workspace` convention the MCP dispatch keys
/// on).
const WORKSPACE_HEADER: &str = "x-workspace";

/// The workspace a request resolves to when it carries no `X-Workspace` header — the
/// single-tenant default, matching the MCP dispatch default.
const DEFAULT_WORKSPACE: &str = "default";

/// One rig under warden custody, as `graph.list` reports it. Tool-neutral and serializable so the
/// REST list payload matches the MCP read exactly; the composition backing maps the warden's
/// custody record onto this DTO.
#[derive(Clone, Debug, Serialize)]
pub struct RigCustody {
    /// The rig's stable id.
    pub rig: String,
    /// The on-disk checkout the graph indexes.
    pub repo_dir: String,
    /// Whether the warden has marked the graph stale (changes pending a rebuild).
    pub stale: bool,
    /// How many tracked changes are pending since the last index.
    pub pending_changes: usize,
    /// The commit the graph was last indexed at, if recorded.
    pub last_indexed_commit: Option<String>,
}

/// A per-workspace read provider for the graph REST adapter: it resolves which rigs are under
/// warden custody and where their checkouts live, standing in for the MCP handler's warden-state
/// replay. The binary supplies an implementation that replays the workspace's warden log; a test
/// supplies an in-memory custody map. Read-only — refresh stays the warden's write path.
#[async_trait]
pub trait WorkspaceGraph: Send + Sync {
    /// Every rig under warden custody for `workspace`, with its freshness (`graph.list`).
    async fn list(&self, workspace: &str) -> Result<Vec<RigCustody>, GraphError>;

    /// The on-disk checkout for `rig` in `workspace`, or `None` when the warden has no custody of
    /// it — the REST mirror of the handler's `repo_dir` resolution (a missing rig is a `404`).
    async fn repo_dir(&self, workspace: &str, rig: &str) -> Result<Option<PathBuf>, GraphError>;
}

/// Everything the graph REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`graph_router`] so the merged application router carries no outstanding state
/// type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct GraphApiState {
    /// The per-workspace warden-custody read provider.
    custody: Arc<dyn WorkspaceGraph>,
    /// The active tool-neutral indexer — workspace-neutral (one indexer serves every rig), so it
    /// rides in the state directly rather than the per-workspace provider.
    indexer: Arc<dyn GraphIndexer>,
}

impl GraphApiState {
    /// Build the REST state over a per-workspace custody provider and the active indexer.
    pub fn new(custody: Arc<dyn WorkspaceGraph>, indexer: Arc<dyn GraphIndexer>) -> Self {
        Self { custody, indexer }
    }

    /// Resolve `rig`'s checkout for `workspace`, mapping no-custody onto a `404`.
    async fn repo(&self, workspace: &str, rig: &str) -> Result<PathBuf, ApiError> {
        match self.custody.repo_dir(workspace, rig).await? {
            Some(dir) => Ok(dir),
            None => Err(ApiError::NotFound(format!("no graph custody for rig `{rig}`"))),
        }
    }
}

/// Build the read-only graph REST router with `state` baked in (`hq-fe-api-kernel.1`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/graph` and applies the scope
/// guard. `register_routes` on the HTTP-enabled graph module returns exactly this router.
///
/// | Method + path        | Maps to MCP tool |
/// |----------------------|------------------|
/// | `GET /`              | `graph.list`     |
/// | `GET /:rig`          | `graph.status`   |
/// | `GET /:rig/query`    | `graph.query`    |
/// | `GET /:rig/explain`  | `graph.explain`  |
pub fn graph_router(state: GraphApiState) -> Router {
    Router::new()
        .route("/", get(list_rigs))
        .route("/:rig/query", get(query_rig))
        .route("/:rig/explain", get(explain_rig))
        .route("/:rig", get(rig_status))
        .with_state(state)
}

/// Querystring for `GET /:rig/query` — the free-text question.
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// The natural-language question to answer from the graph.
    q: String,
}

/// Querystring for `GET /:rig/explain` — the node to explain.
#[derive(Debug, Deserialize)]
struct ExplainParams {
    /// The node (crate/concept) label to explain.
    node: String,
}

/// `GET /` — every rig under warden custody with its freshness (`graph.list`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "The rigs under custody with their freshness")),
))]
async fn list_rigs(
    State(st): State<GraphApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let rigs = st.custody.list(ws.as_deref().unwrap_or(DEFAULT_WORKSPACE)).await?;
    Ok(Json(json!({ "rigs": rigs })))
}

/// `GET /:rig` — a rig's graph freshness + index stats (`graph.status`); `404` when the warden has
/// no custody of the rig.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{rig}",
    params(("rig" = String, Path, description = "Rig id")),
    responses(
        (status = 200, description = "The rig's graph freshness + index stats"),
        (status = 404, description = "No graph custody for that rig"),
    ),
))]
async fn rig_status(
    State(st): State<GraphApiState>,
    headers: HeaderMap,
    Path(rig): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let repo = st.repo(ws.as_deref().unwrap_or(DEFAULT_WORKSPACE), &rig).await?;
    let status = st.indexer.status(&repo).await?;
    Ok(Json(json!({
        "built": status.built,
        "tool": status.tool,
        "nodes": status.stats.map(|s| s.nodes),
        "edges": status.stats.map(|s| s.edges),
        "communities": status.stats.map(|s| s.communities),
        "built_at_commit": status.built_at_commit,
    })))
}

/// `GET /:rig/query?q=` — answer a free-text question against a rig's graph (`graph.query`); `404`
/// when the rig is not under custody, and the indexer's own `NotBuilt` also surfaces as `404`.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{rig}/query",
    params(
        ("rig" = String, Path, description = "Rig id"),
        ("q" = String, Query, description = "The natural-language question"),
    ),
    responses(
        (status = 200, description = "The grounded answer + cited nodes"),
        (status = 404, description = "No custody for that rig / graph not built"),
    ),
))]
async fn query_rig(
    State(st): State<GraphApiState>,
    headers: HeaderMap,
    Path(rig): Path<String>,
    Query(q): Query<QueryParams>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let repo = st.repo(ws.as_deref().unwrap_or(DEFAULT_WORKSPACE), &rig).await?;
    let ans = st.indexer.query(&repo, &q.q).await?;
    Ok(Json(json!({ "text": ans.text, "nodes": ans.nodes })))
}

/// `GET /:rig/explain?node=` — explain one node in a rig's graph (`graph.explain`); `404` when the
/// rig is not under custody, and the indexer's own `NotBuilt` also surfaces as `404`.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{rig}/explain",
    params(
        ("rig" = String, Path, description = "Rig id"),
        ("node" = String, Query, description = "The node (crate/concept) to explain"),
    ),
    responses(
        (status = 200, description = "The node explanation + cited nodes"),
        (status = 404, description = "No custody for that rig / graph not built"),
    ),
))]
async fn explain_rig(
    State(st): State<GraphApiState>,
    headers: HeaderMap,
    Path(rig): Path<String>,
    Query(p): Query<ExplainParams>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let repo = st.repo(ws.as_deref().unwrap_or(DEFAULT_WORKSPACE), &rig).await?;
    let ans = st.indexer.explain(&repo, &p.node).await?;
    Ok(Json(json!({ "text": ans.text, "nodes": ans.nodes })))
}

/// The combined OpenAPI document for the read-only graph REST surface (`hq-fe-api-kernel.1`). The
/// builder mounts it under the module prefix and rewrites its relative paths to `/api/v1/graph/...`,
/// so the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(list_rigs, rig_status, query_rig, explain_rig))]
pub struct ApiDoc;

/// Read the request's tenant from the `X-Workspace` header, or `None` when it is absent/empty (the
/// caller then resolves to [`DEFAULT_WORKSPACE`]). Never read from a URL or body field (docs/03
/// Rule 6).
fn workspace_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(WORKSPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// HTTP wrapper over the graph errors so a handler can `?`-propagate them with the right status:
/// a no-custody rig and an unbuilt graph are both client-addressable `404`s; a tool/IO failure is
/// a server `500`; an unsupported op is `501`.
#[derive(Debug)]
enum ApiError {
    /// The rig is not under warden custody for the resolved workspace.
    NotFound(String),
    /// The indexer reported a failure.
    Graph(GraphError),
}

impl From<GraphError> for ApiError {
    fn from(e: GraphError) -> Self {
        ApiError::Graph(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Graph(e) => {
                let status = match &e {
                    GraphError::NotBuilt(_) => StatusCode::NOT_FOUND,
                    GraphError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, e.to_string())
            }
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{rig}", "/{rig}/query", "/{rig}/explain"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn workspace_of_reads_header_or_defaults_none() {
        let mut h = HeaderMap::new();
        assert_eq!(workspace_of(&h), None);
        h.insert(WORKSPACE_HEADER, "acme".parse().unwrap());
        assert_eq!(workspace_of(&h).as_deref(), Some("acme"));
        h.insert(WORKSPACE_HEADER, "".parse().unwrap());
        assert_eq!(workspace_of(&h), None, "empty header is treated as absent");
    }

    #[test]
    fn error_status_mapping_matches_graph_error_kinds() {
        assert_eq!(
            ApiError::NotFound("x".into()).into_response().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::Graph(GraphError::NotBuilt("r".into())).into_response().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::Graph(GraphError::Unsupported("t".into())).into_response().status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            ApiError::Graph(GraphError::Tool("boom".into())).into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
