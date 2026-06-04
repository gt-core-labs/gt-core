//! Contract for the merge REST adapter (`hq-fe-api-orch.2`).
//!
//! Drives [`gt_merge::merge_router`] in-process with `tower`'s `oneshot` against an **in-memory**
//! per-workspace provider, proving the REST surface dispatches to the **same**
//! [`MergeCommand`](gt_merge::MergeCommand) validate/execute layer the MCP tools use: a `POST /`
//! submit is visible to `GET /:bead` and `GET /`, the slot walks `Ready → Merging → Merged` over
//! `start`/`complete`, the bead always comes from the path on a mutation, an illegal transition is
//! a `409`, a missing slot is a `404`, and — critically — the board is per-tenant, so a slot in
//! workspace `a` is invisible to workspace `b`. The wire mapping (error → status, path-bead
//! override) is unit-covered in `http.rs`; this is the integration proof that the routes are wired
//! to a workspace-scoped store.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the provider
//! hands each workspace its own [`gt_merge::InMemoryMergeRepo`], so the contract runs on host CI
//! with no sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_merge::{merge_router, DynMergeRepository, InMemoryMergeRepo, MergeApiState, WorkspaceMerges};
use gt_workspace::WORKSPACE_HEADER;

/// An in-memory [`WorkspaceMerges`] provider: one persistent [`InMemoryMergeRepo`] per workspace
/// slug, so two tenants get genuinely separate boards (the REST mirror of per-workspace state).
#[derive(Default)]
struct MemProvider {
    by_ws: Mutex<HashMap<String, Arc<InMemoryMergeRepo>>>,
}

#[async_trait]
impl WorkspaceMerges for MemProvider {
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynMergeRepository>, AppError> {
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
    merge_router(MergeApiState::new(Arc::new(MemProvider::default())))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// `POST` carrying a tenant header — the workspace comes from the auth context, never the path
/// (docs/04 §15).
fn post_in(ws: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header(WORKSPACE_HEADER, ws)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

/// The submit body the board accepts.
fn submit_body(bead: &str, branch: &str) -> Value {
    json!({ "bead": bead, "branch": branch, "channel_msg_id": "m1" })
}

#[tokio::test]
async fn submit_then_read_roundtrips_through_the_store() {
    let app = router();

    // POST / registers a slot in Ready; 201 with the bead echoed.
    let (status, body) = send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;
    assert_eq!(status, StatusCode::CREATED, "submit returns 201: {body}");
    assert_eq!(body["bead"], "b1");

    // GET /:bead sees it in Ready with the full slot shape.
    let (status, info) = send(&app, get_in("acme", "/b1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["bead"], "b1");
    assert_eq!(info["branch"], "feat/b1");
    assert_eq!(info["state"], "ready");

    // GET / lists it.
    let (status, list) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(list["slots"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["bead"] == "b1"));
}

#[tokio::test]
async fn duplicate_submit_is_422() {
    let app = router();
    let (s, _) = send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;
    assert_eq!(s, StatusCode::CREATED);
    // Re-submit of the same bead is a validation fault against the rehydrated board.
    let (s, _) = send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn lifecycle_start_then_complete_drives_to_merged() {
    let app = router();
    send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;

    let (s, _) = send(&app, post_in("acme", "/b1/start", json!({}))).await;
    assert_eq!(s, StatusCode::OK);
    let (_, info) = send(&app, get_in("acme", "/b1")).await;
    assert_eq!(info["state"], "merging");

    // The bead comes from the path; the body carries only the landed sha.
    let (s, body) = send(&app, post_in("acme", "/b1/complete", json!({ "sha": "abc1234" }))).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["event"], "merge.merged.v1");
    let (_, info) = send(&app, get_in("acme", "/b1")).await;
    assert_eq!(info["state"], "merged");
}

#[tokio::test]
async fn fail_path_marks_slot_failed() {
    let app = router();
    send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;
    send(&app, post_in("acme", "/b1/start", json!({}))).await;

    let (s, _) = send(&app, post_in("acme", "/b1/fail", json!({ "reason": "conflict" }))).await;
    assert_eq!(s, StatusCode::OK);
    let (_, info) = send(&app, get_in("acme", "/b1")).await;
    assert_eq!(info["state"], "failed");
}

#[tokio::test]
async fn illegal_transition_is_409() {
    let app = router();
    send(&app, post_in("acme", "/", submit_body("b1", "feat/b1"))).await;
    // Ready → Merged (skipping Merging) is illegal — a 409 from the state machine.
    let (s, _) = send(&app, post_in("acme", "/b1/complete", json!({ "sha": "abc1234" }))).await;
    assert_eq!(s, StatusCode::CONFLICT);
}

#[tokio::test]
async fn start_takes_bead_from_path_over_body() {
    let app = router();
    send(&app, post_in("acme", "/", submit_body("keep", "feat/keep"))).await;
    send(&app, post_in("acme", "/", submit_body("decoy", "feat/decoy"))).await;

    // The body names a DECOY bead; the path must win (docs/03 Rule 6) — `keep` is the one started.
    let (s, _) = send(&app, post_in("acme", "/keep/start", json!({ "bead": "decoy" }))).await;
    assert_eq!(s, StatusCode::OK);

    let (_, keep) = send(&app, get_in("acme", "/keep")).await;
    assert_eq!(keep["state"], "merging", "path bead `keep` was started");
    let (_, decoy) = send(&app, get_in("acme", "/decoy")).await;
    assert_eq!(decoy["state"], "ready", "body decoy must not be touched");
}

#[tokio::test]
async fn missing_slot_is_404() {
    let app = router();
    let (status, _) = send(&app, get_in("acme", "/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A start on an absent slot is also a not-found.
    let (status, _) = send(&app, post_in("acme", "/nope/start", json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn board_is_per_workspace() {
    let app = router();

    // The SAME bead submits cleanly in BOTH tenants (per-tenant board, no collision).
    let (sa, _) = send(&app, post_in("a", "/", submit_body("b1", "feat/b1"))).await;
    let (sb, _) = send(&app, post_in("b", "/", submit_body("b1", "feat/b1"))).await;
    assert_eq!(sa, StatusCode::CREATED);
    assert_eq!(sb, StatusCode::CREATED, "same bead in a distinct ws must not collide");

    // A slot only in `a`.
    send(&app, post_in("a", "/", submit_body("only-a", "feat/a"))).await;

    // `b` sees only its own `b1`, never `a`'s `only-a` (no cross-tenant leak).
    let (_, list_b) = send(&app, get_in("b", "/")).await;
    let beads_b: Vec<&str> = list_b["slots"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["bead"].as_str())
        .collect();
    assert!(beads_b.contains(&"b1"));
    assert!(!beads_b.contains(&"only-a"), "no cross-tenant leak from a into b");
    let (status, _) = send(&app, get_in("b", "/only-a")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a's slot is not found in b");
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
