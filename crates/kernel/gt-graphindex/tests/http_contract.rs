//! Contract for the read-only graph REST adapter (`hq-fe-api-kernel.1`).
//!
//! Drives [`gt_graphindex::graph_router`] in-process with `tower`'s `oneshot` against an
//! **in-memory** custody provider + the dependency-free [`InMemoryGraphIndexer`], proving the REST
//! surface dispatches to the **same** [`GraphIndexer`](gt_graphindex::GraphIndexer) port the MCP
//! `graph.*` tools use: `GET /` lists the rigs under custody, `GET /:rig` reports freshness, `GET
//! /:rig/query` and `GET /:rig/explain` answer from the built graph, a rig with no custody is a
//! `404`, and — critically — custody is per-tenant, so a rig in workspace `a` is invisible to
//! workspace `b`. The wire mapping (error → status, header read) is unit-covered in `http.rs`;
//! this is the integration proof that the routes are wired to a workspace-scoped read provider.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database and no graph
//! tool: the `InMemoryGraphIndexer` answers from memory, so the contract runs on host CI with no
//! sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt; // oneshot

use gt_graphindex::{
    graph_router, GraphApiState, GraphError, GraphIndexer, InMemoryGraphIndexer, RigCustody,
    WorkspaceGraph,
};

/// The `X-Workspace` header the adapter reads the tenant from (kept in sync with `http.rs`).
const WORKSPACE_HEADER: &str = "x-workspace";

/// An in-memory [`WorkspaceGraph`] custody provider: one rig list per workspace slug, the REST
/// mirror of the warden-state replay — two tenants get genuinely separate custody, never any
/// cross-talk.
#[derive(Default)]
struct MemCustody {
    by_ws: HashMap<String, Vec<RigCustody>>,
}

#[async_trait]
impl WorkspaceGraph for MemCustody {
    async fn list(&self, workspace: &str) -> Result<Vec<RigCustody>, GraphError> {
        Ok(self.by_ws.get(workspace).cloned().unwrap_or_default())
    }

    async fn repo_dir(&self, workspace: &str, rig: &str) -> Result<Option<PathBuf>, GraphError> {
        Ok(self
            .by_ws
            .get(workspace)
            .and_then(|rigs| rigs.iter().find(|g| g.rig == rig))
            .map(|g| PathBuf::from(&g.repo_dir)))
    }
}

/// Build the REST router with `acme` holding custody of rig `alpha` at `/repo/alpha`, the indexer
/// pre-built for that repo so queries answer.
async fn router() -> axum::Router {
    let mut by_ws: HashMap<String, Vec<RigCustody>> = HashMap::new();
    by_ws.insert(
        "acme".to_string(),
        vec![RigCustody {
            rig: "alpha".to_string(),
            repo_dir: "/repo/alpha".to_string(),
            stale: false,
            pending_changes: 0,
            last_indexed_commit: Some("abc1234".to_string()),
        }],
    );
    let indexer = Arc::new(InMemoryGraphIndexer::new());
    // InMemory needs a built graph for that repo to answer.
    indexer.build(std::path::Path::new("/repo/alpha")).await.unwrap();

    graph_router(GraphApiState::new(Arc::new(MemCustody { by_ws }), indexer))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn get_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn list_reports_rigs_under_custody() {
    let app = router().await;
    let (status, body) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    let rigs = body["rigs"].as_array().unwrap();
    assert_eq!(rigs.len(), 1);
    assert_eq!(rigs[0]["rig"], "alpha");
    assert_eq!(rigs[0]["stale"], false);
    assert_eq!(rigs[0]["last_indexed_commit"], "abc1234");
}

#[tokio::test]
async fn status_reports_freshness_for_a_rig_under_custody() {
    let app = router().await;
    let (status, body) = send(&app, get_in("acme", "/alpha")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["built"], true);
    assert_eq!(body["tool"], "inmemory");
}

#[tokio::test]
async fn query_answers_from_the_built_graph() {
    let app = router().await;
    let (status, body) = send(&app, get_in("acme", "/alpha/query?q=where%20is%20auth")).await;
    assert_eq!(status, StatusCode::OK, "query ok: {body}");
    assert!(body["text"].as_str().unwrap().contains("where is auth"));
}

#[tokio::test]
async fn explain_answers_for_a_node() {
    let app = router().await;
    let (status, body) = send(&app, get_in("acme", "/alpha/explain?node=gt_module")).await;
    assert_eq!(status, StatusCode::OK, "explain ok: {body}");
    assert!(body["text"].is_string());
}

#[tokio::test]
async fn missing_query_param_is_400() {
    let app = router().await;
    // No `?q=` ⇒ the Query extractor rejects with 400 before the indexer is touched.
    let (status, _) = send(&app, get_in("acme", "/alpha/query")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rig_without_custody_is_404() {
    let app = router().await;
    let (status, _) = send(&app, get_in("acme", "/ghost")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // …and a query against it is likewise a 404 (resolved before the indexer runs).
    let (status, _) = send(&app, get_in("acme", "/ghost/query?q=x")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn custody_is_per_workspace() {
    let app = router().await;
    // `b` holds no custody: an empty list, and `alpha` (acme's rig) is not found in `b`.
    let (_, list_b) = send(&app, get_in("b", "/")).await;
    assert!(list_b["rigs"].as_array().unwrap().is_empty(), "no cross-tenant leak");
    let (status, _) = send(&app, get_in("b", "/alpha")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "acme's rig is not found in b");
}
