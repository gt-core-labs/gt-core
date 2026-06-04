//! Contract for the workspace REST adapter (`hq-fe-api-platform.1`).
//!
//! Drives [`gt_workspace::workspace_router`] in-process with `tower`'s `oneshot`,
//! proving the REST surface dispatches to the **same** [`WorkspaceCommand`] decide/
//! apply layer the MCP `WorkspaceHandler` wraps: a `POST /` create is visible to
//! `GET /:id` and `GET /`, the lifecycle verbs flip status (and reject illegal
//! transitions as `409`), a duplicate create is `409`, and a missing workspace is
//! `404`. Status/JSON wire mapping is unit-covered in `http.rs`; this is the
//! integration proof the routes are wired to the catalog.
//!
//! Backed by the in-memory [`InMemoryWorkspaces`] adapter, so it needs no database
//! and always runs (the catalog command path is backend-agnostic — the PG adapter is
//! contract-tested separately in `pg.rs`).
#![cfg(feature = "axum")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_workspace::{workspace_router, InMemoryWorkspaces, WorkspaceApiState, WorkspaceRepository};

/// A router over a fresh in-memory catalog.
fn router() -> axum::Router {
    let repo: Arc<dyn WorkspaceRepository> = Arc::new(InMemoryWorkspaces::new());
    workspace_router(WorkspaceApiState::new(repo))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn post_empty(uri: &str) -> Request<Body> {
    Request::builder().method("POST").uri(uri).body(Body::empty()).unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn create_then_read_roundtrips_through_the_catalog() {
    let app = router();

    // POST / provisions a new active workspace.
    let (status, body) = send(&app, post_json("/", json!({ "id": "acme", "name": "Acme" }))).await;
    assert_eq!(status, StatusCode::CREATED, "create returns 201: {body}");
    assert_eq!(body["id"], "acme");
    assert_eq!(body["status"], "active");

    // GET /:id sees the just-created workspace.
    let (status, info) = send(&app, get("/acme")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["id"], "acme");
    assert_eq!(info["name"], "Acme");
    assert_eq!(info["status"], "active");

    // GET / lists it in the envelope.
    let (status, page) = send(&app, get("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["workspaces"][0]["id"], "acme");
}

#[tokio::test]
async fn missing_workspace_is_404() {
    let app = router();
    let (status, _) = send(&app, get("/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn duplicate_create_is_409() {
    let app = router();
    let (s, _) = send(&app, post_json("/", json!({ "id": "acme", "name": "Acme" }))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (status, _) = send(&app, post_json("/", json!({ "id": "acme", "name": "Other" }))).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn invalid_id_or_blank_name_is_422() {
    let app = router();
    // A malformed slug (uppercase) is rejected before any catalog work.
    let (status, _) = send(&app, post_json("/", json!({ "id": "Bad_Id", "name": "X" }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    // A blank name is rejected by the decide layer.
    let (status, _) = send(&app, post_json("/", json!({ "id": "ok", "name": "  " }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn lifecycle_suspend_resume_archive_flips_status() {
    let app = router();
    send(&app, post_json("/", json!({ "id": "acme", "name": "Acme" }))).await;

    // Suspend: active -> suspended.
    let (status, body) = send(&app, post_empty("/acme/suspend")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "suspended");
    let (_, info) = send(&app, get("/acme")).await;
    assert_eq!(info["status"], "suspended");

    // Resume: suspended -> active.
    let (status, body) = send(&app, post_empty("/acme/resume")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "active");

    // Archive: active -> archived (terminal).
    let (status, body) = send(&app, post_empty("/acme/archive")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "archived");
    let (_, info) = send(&app, get("/acme")).await;
    assert_eq!(info["status"], "archived");
}

#[tokio::test]
async fn illegal_transition_is_409() {
    let app = router();
    send(&app, post_json("/", json!({ "id": "acme", "name": "Acme" }))).await;

    // Resuming an active workspace is illegal (nothing suspended to restore).
    let (status, _) = send(&app, post_empty("/acme/resume")).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Archive it, then a second archive is illegal (terminal).
    let (s, _) = send(&app, post_empty("/acme/archive")).await;
    assert_eq!(s, StatusCode::OK);
    let (status, _) = send(&app, post_empty("/acme/archive")).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn lifecycle_on_missing_workspace_is_404() {
    let app = router();
    let (status, _) = send(&app, post_empty("/ghost/suspend")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
