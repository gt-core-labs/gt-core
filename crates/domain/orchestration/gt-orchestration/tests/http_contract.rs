//! Contract for the convoy REST adapter (`hq-fe-api-orch.3`).
//!
//! Drives [`gt_orchestration::convoy_router`] in-process with `tower`'s `oneshot` against an
//! **in-memory** per-workspace event-log provider, proving the REST surface dispatches to the
//! **same** [`Command`](gt_events::Command) execute layer the MCP `convoy.*` tools use: a `POST /`
//! launch materializes the convoy with its first member dispatched, `POST
//! /:convoy/complete-member` hands off to the next member and closes the convoy when all are done,
//! `GET /` and `GET /:convoy` read the rehydrated board, the convoy always comes from the path on
//! a member mutation, a missing convoy is a `404`, and — critically — the log is per-tenant, so a
//! convoy in workspace `a` is invisible to workspace `b`. The wire mapping (error → status,
//! path-field override) is unit-covered in `http.rs`; this is the integration proof that the
//! routes are wired to a workspace-scoped, event-sourced store.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the
//! provider keeps each workspace's events in memory and replays them through the same
//! [`OrchState`](gt_orchestration::OrchState) reducer the real `EventLog` uses, so the contract
//! runs on host CI with no sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_orchestration::{convoy_router, ConvoyApiState, ConvoyBoard, OrchEvent, OrchState, WorkspaceConvoy};
use gt_workspace::WORKSPACE_HEADER;

/// An in-memory [`WorkspaceConvoy`] provider: one append-only `Vec<OrchEvent>` per workspace slug,
/// replayed through [`OrchState::apply`] → [`ConvoyBoard::from_state`] on every read — the exact
/// read-modify-append the real `EventLog`-backed handler runs, so two tenants get genuinely
/// separate logs (the REST mirror of the path-partitioned-per-workspace event store).
#[derive(Default)]
struct MemLog {
    by_ws: Mutex<HashMap<String, Vec<OrchEvent>>>,
}

#[async_trait]
impl WorkspaceConvoy for MemLog {
    async fn board(&self, workspace: &str) -> Result<ConvoyBoard, AppError> {
        let guard = self.by_ws.lock().unwrap();
        let mut state = OrchState::default();
        if let Some(events) = guard.get(workspace) {
            for ev in events {
                state.apply(ev);
            }
        }
        Ok(ConvoyBoard::from_state(&state))
    }

    async fn append(&self, workspace: &str, events: Vec<OrchEvent>) -> Result<(), AppError> {
        self.by_ws
            .lock()
            .unwrap()
            .entry(workspace.to_string())
            .or_default()
            .extend(events);
        Ok(())
    }
}

/// Build the REST router over a fresh in-memory log provider.
fn router() -> axum::Router {
    convoy_router(ConvoyApiState::new(Arc::new(MemLog::default())))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// `POST` carrying a tenant header — the workspace comes from the auth context, never the path.
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

#[tokio::test]
async fn launch_then_read_roundtrips_through_the_log() {
    let app = router();

    // POST / launches the convoy and dispatches the first member.
    let (status, body) = send(
        &app,
        post_in("acme", "/", json!({ "convoy": "c1", "members": ["b1", "b2"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "launch ok: {body}");
    let kinds = body["events"].as_array().unwrap();
    assert!(kinds.iter().any(|k| k == "convoy.launched.v1"), "emitted launched: {body}");

    // GET /:convoy sees it: launched, first member active, second pending.
    let (status, info) = send(&app, get_in("acme", "/c1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["state"], "launched");
    let members = info["members"].as_array().unwrap();
    assert_eq!(members[0]["state"], "active", "first member dispatched on launch");
    assert_eq!(members[1]["state"], "pending");

    // GET / lists it.
    let (status, list) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["convoys"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn complete_member_takes_convoy_from_path_and_hands_off() {
    let app = router();
    send(
        &app,
        post_in("acme", "/", json!({ "convoy": "c1", "members": ["b1", "b2"] })),
    )
    .await;

    // The body names a DECOY convoy; the path must win (docs/03 Rule 6) — `c1` advances.
    let (status, body) = send(
        &app,
        post_in("acme", "/c1/complete-member", json!({ "convoy": "decoy", "member": "b1" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "complete ok: {body}");

    // Handoff: b1 done, b2 now active.
    let (_, info) = send(&app, get_in("acme", "/c1")).await;
    let members = info["members"].as_array().unwrap();
    assert_eq!(members[0]["state"], "done");
    assert_eq!(members[1]["state"], "active", "handoff to next member");

    // Completing the last member closes the convoy.
    send(
        &app,
        post_in("acme", "/c1/complete-member", json!({ "member": "b2" })),
    )
    .await;
    let (_, info) = send(&app, get_in("acme", "/c1")).await;
    assert_eq!(info["state"], "closed", "convoy closes when all members done");
}

#[tokio::test]
async fn fail_member_halts_the_convoy() {
    let app = router();
    send(
        &app,
        post_in("acme", "/", json!({ "convoy": "c1", "members": ["b1"] })),
    )
    .await;

    let (status, body) = send(
        &app,
        post_in("acme", "/c1/fail-member", json!({ "member": "b1", "reason": "boom" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fail ok: {body}");
    let kinds = body["events"].as_array().unwrap();
    assert!(kinds.iter().any(|k| k == "convoy.failed.v1"), "emitted failed: {body}");
}

#[tokio::test]
async fn empty_members_is_422() {
    let app = router();
    let (status, _) = send(
        &app,
        post_in("acme", "/", json!({ "convoy": "c1", "members": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "a convoy needs members");
}

#[tokio::test]
async fn missing_convoy_is_404() {
    let app = router();
    let (status, _) = send(&app, get_in("acme", "/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn log_is_per_workspace() {
    let app = router();

    // Launch the SAME convoy id in BOTH tenants — per-tenant logs, no cross-talk.
    send(
        &app,
        post_in("a", "/", json!({ "convoy": "shared", "members": ["x"] })),
    )
    .await;
    send(
        &app,
        post_in("a", "/", json!({ "convoy": "only-a", "members": ["y"] })),
    )
    .await;

    // `b` has never launched anything: it sees an empty board, never `a`'s convoys.
    let (_, list_b) = send(&app, get_in("b", "/")).await;
    assert!(
        list_b["convoys"].as_array().unwrap().is_empty(),
        "no cross-tenant leak from a into b",
    );
    let (status, _) = send(&app, get_in("b", "/only-a")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a's convoy is not found in b");
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
