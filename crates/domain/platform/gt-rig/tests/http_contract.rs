//! Contract for the rig REST adapter (`hq-fe-api-platform.2`).
//!
//! Drives [`gt_rig::rig_router`] in-process with `tower`'s `oneshot` against an **in-memory**
//! per-workspace provider, proving the REST surface dispatches to the **same**
//! [`RigCommand`](gt_rig::RigCommand) decide/apply layer the MCP tools use: a `POST /` add is
//! visible to `GET /:name` and `GET /`, the name always comes from the path on a mutation, a
//! `GET /lookup` resolves a prefix, a missing rig is a `404`, and — critically — the catalog is
//! per-tenant, so a rig in workspace `a` is invisible to workspace `b`. The wire mapping (error
//! → status, `now_secs` stamping, path-name override) is unit-covered in `http.rs`; this is the
//! integration proof that the routes are wired to a workspace-scoped store.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the
//! provider hands each workspace its own [`gt_rig::InMemoryRigs`], so the contract runs on host
//! CI with no Postgres sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_rig::{rig_router, DynRigRepository, InMemoryRigs, RigApiState, WorkspaceRigs};
use gt_workspace::WORKSPACE_HEADER;

/// An in-memory [`WorkspaceRigs`] provider: one persistent [`InMemoryRigs`] per workspace slug,
/// so two tenants get genuinely separate catalogs (the REST mirror of schema-per-workspace).
#[derive(Default)]
struct MemProvider {
    by_ws: Mutex<HashMap<String, Arc<InMemoryRigs>>>,
}

#[async_trait]
impl WorkspaceRigs for MemProvider {
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynRigRepository>, AppError> {
        let repo = self
            .by_ws
            .lock()
            .unwrap()
            .entry(workspace.to_string())
            .or_default()
            .clone();
        Ok(Box::new(repo))
    }
}

/// Build the REST router over a fresh in-memory provider.
fn router() -> axum::Router {
    rig_router(RigApiState::new(Arc::new(MemProvider::default())))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// `POST`/`DELETE` carrying a tenant header — the workspace comes from the auth context, never
/// the path (docs/04 §15).
fn post_in(ws: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header(WORKSPACE_HEADER, ws)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn delete_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

fn get_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

/// The add body the catalog accepts (epic-free; a rig only needs name/prefix/git_url/branch).
fn add_body(name: &str, prefix: &str) -> Value {
    json!({
        "name": name, "prefix": prefix,
        "git_url": format!("git@x:y/{name}.git"), "default_branch": "main"
    })
}

#[tokio::test]
async fn add_then_read_roundtrips_through_the_store() {
    let app = router();

    // POST / registers a rig; 201 with the name echoed.
    let (status, body) = send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;
    assert_eq!(status, StatusCode::CREATED, "add returns 201: {body}");
    assert_eq!(body["rig"], "plane");

    // GET /:name sees it with the full entry shape.
    let (status, info) = send(&app, get_in("acme", "/plane")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["name"], "plane");
    assert_eq!(info["prefix"], "pl");
    assert_eq!(info["default_branch"], "main");

    // GET / lists it.
    let (status, list) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(list["rigs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["name"] == "plane"));
}

#[tokio::test]
async fn duplicate_add_is_422() {
    let app = router();
    let (s, _) = send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;
    assert_eq!(s, StatusCode::CREATED);
    // Re-add (name + prefix collision) is a validation fault.
    let (s, _) = send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn missing_rig_is_404() {
    let app = router();
    let (status, _) = send(&app, get_in("acme", "/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn set_prefix_takes_name_from_path_and_lookup_resolves_it() {
    let app = router();
    send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;

    // The body names a DECOY rig; the path must win (docs/03 Rule 6) — `plane` is repointed.
    let (status, _) = send(
        &app,
        post_in("acme", "/plane/set-prefix", json!({ "name": "decoy", "new_prefix": "pln" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // GET /lookup resolves the NEW prefix to `plane` (static route, never shadowed by /:name).
    let (status, found) = send(&app, get_in("acme", "/lookup?prefix=pln")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["name"], "plane");

    // The old prefix no longer resolves.
    let (status, _) = send(&app, get_in("acme", "/lookup?prefix=pl")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn set_branch_and_worktree_root_persist() {
    let app = router();
    send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;

    let (s, _) = send(
        &app,
        post_in("acme", "/plane/set-default-branch", json!({ "new_branch": "master" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send(
        &app,
        post_in("acme", "/plane/set-worktree-root", json!({ "new_root": "/srv/wt/plane" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (_, info) = send(&app, get_in("acme", "/plane")).await;
    assert_eq!(info["default_branch"], "master");
    assert_eq!(info["worktree_root"], "/srv/wt/plane");
}

#[tokio::test]
async fn delete_removes_then_404() {
    let app = router();
    send(&app, post_in("acme", "/", add_body("plane", "pl"))).await;

    let (status, body) = send(&app, delete_in("acme", "/plane")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["removed"], true);

    // Gone: a second read is a 404, and deleting again is a 404 (decide rejects an absent rig).
    let (status, _) = send(&app, get_in("acme", "/plane")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, delete_in("acme", "/plane")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn catalog_is_per_workspace() {
    let app = router();

    // The SAME name + prefix registers cleanly in BOTH tenants (per-tenant catalog, no collision).
    let (sa, _) = send(&app, post_in("a", "/", add_body("granite", "gr"))).await;
    let (sb, _) = send(&app, post_in("b", "/", add_body("granite", "gr"))).await;
    assert_eq!(sa, StatusCode::CREATED);
    assert_eq!(sb, StatusCode::CREATED, "same prefix in a distinct ws must not collide");

    // A rig only in `a`.
    send(&app, post_in("a", "/", add_body("plane", "pl"))).await;

    // `b` sees only its own `granite`, never `a`'s `plane` (no cross-tenant leak).
    let (_, list_b) = send(&app, get_in("b", "/")).await;
    let names_b: Vec<&str> = list_b["rigs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(names_b.contains(&"granite"));
    assert!(!names_b.contains(&"plane"), "no cross-tenant leak from a into b");
    let (status, _) = send(&app, get_in("b", "/plane")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a's rig is not found in b");
}

#[tokio::test]
async fn missing_workspace_header_is_rejected() {
    let app = router();
    // No tenant header and no JWT claim ⇒ the WorkspaceContext extractor rejects (400), so a
    // request can never run against an unresolved/ambient tenant.
    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let (status, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
