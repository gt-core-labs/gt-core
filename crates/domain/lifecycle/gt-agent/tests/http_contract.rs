//! Contract for the agent REST adapter (`hq-fe-api-orch.1`).
//!
//! Drives [`gt_agent::http::agent_router`] in-process with `tower`'s `oneshot` against a
//! tempdir-backed event log, proving the REST surface dispatches to the **same** replay+append
//! the MCP `agent.*` tools use: a `POST /` spawn is visible to `GET /:id` and `GET /`, the
//! lifecycle events fold the session state (`end` ⇒ `done`, `kill` ⇒ `killed`), the id comes from
//! the path on heartbeat/end/kill, a re-spawn is rejected, an unknown session is a `404`, and the
//! `X-Workspace` header partitions the log so one tenant never sees another's sessions.
//!
//! Compiled only under the `axum` feature (the adapter's gate). The event log is a file store, so
//! the test needs no external sidecar — it points the state at its own tempdir.
#![cfg(feature = "axum")]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_agent::http::{agent_router, AgentApiState, FileAgentLog};

/// Build the REST router over a throwaway event-log root.
fn router(root: &std::path::Path) -> axum::Router {
    agent_router(AgentApiState::new(std::sync::Arc::new(FileAgentLog::new(root.to_path_buf()))))
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

/// A `POST` with an empty body — heartbeat/end carry no payload.
fn post_empty(uri: &str) -> Request<Body> {
    Request::builder().method("POST").uri(uri).body(Body::empty()).unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn spawn_then_read_roundtrips_through_the_event_log() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());

    // POST / spawns a session (the id rides in the body, it names the new resource).
    let (status, body) = send(
        &app,
        post_json("/", json!({ "session": "s1", "rig": "granite" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "spawn returns 201: {body}");
    assert_eq!(body["session"], "s1");
    assert_eq!(body["event"], "agent.spawned.v1");

    // GET /:id sees the just-spawned session, replayed from the log.
    let (status, info) = send(&app, get("/s1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["id"], "s1");
    assert_eq!(info["rig"], "granite");
    assert_eq!(info["state"], "spawned");
    assert_eq!(info["role"], "polecat", "an omitted role defaults to polecat");

    // GET / lists it.
    let (status, list) = send(&app, get("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn respawn_is_rejected_against_the_rehydrated_registry() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());
    let (status, _) = send(&app, post_json("/", json!({ "session": "s1", "rig": "granite" }))).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(&app, post_json("/", json!({ "session": "s1", "rig": "granite" }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "duplicate id is a 422");
}

#[tokio::test]
async fn lifecycle_events_fold_the_session_state() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());
    send(&app, post_json("/", json!({ "session": "s1", "rig": "granite" }))).await;

    // Heartbeat is accepted + logged (a no-op on the registry).
    let (status, body) = send(&app, post_empty("/s1/heartbeat")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["event"], "agent.heartbeat.v1");

    // End folds the session to `done`.
    let (status, _) = send(&app, post_empty("/s1/end")).await;
    assert_eq!(status, StatusCode::OK);
    let (_, info) = send(&app, get("/s1")).await;
    assert_eq!(info["state"], "done", "SessionEnd folds the session to Done");

    // A separate session, then killed → `killed`.
    send(&app, post_json("/", json!({ "session": "s2", "rig": "granite" }))).await;
    let (status, body) = send(&app, post_json("/s2/kill", json!({ "reason": "timeout" }))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["event"], "agent.killed.v1");
    let (_, info) = send(&app, get("/s2")).await;
    assert_eq!(info["state"], "killed");
}

#[tokio::test]
async fn pause_then_resume_folds_state_in_place() {
    // B2 (gtcore-5731e9): pause-in-place records `agent.paused.v1` and folds the session to
    // `paused` without killing it; resume records `agent.resumed.v1` and folds it back to
    // `working`. The SIGSTOP/SIGCONT side effect targets a tmux pane that does not exist under
    // the test, so it is a silent no-op — the event recording + state fold is what we assert.
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());
    send(&app, post_json("/", json!({ "session": "s1", "rig": "granite" }))).await;

    let (status, body) = send(&app, post_json("/s1/pause", json!({ "reason": "escalation" }))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["event"], "agent.paused.v1");
    let (_, info) = send(&app, get("/s1")).await;
    assert_eq!(info["state"], "paused", "Paused folds the session to paused, not killed");

    let (status, body) = send(&app, post_empty("/s1/resume")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["event"], "agent.resumed.v1");
    let (_, info) = send(&app, get("/s1")).await;
    assert_eq!(info["state"], "working", "Resumed folds the session back to working");
}

#[tokio::test]
async fn pause_on_unknown_session_is_404() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());
    let (status, _) = send(&app, post_json("/nope/pause", json!({ "reason": "x" }))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, post_empty("/nope/resume")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn lifecycle_on_unknown_session_is_404() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());
    let (status, _) = send(&app, get("/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, post_empty("/nope/heartbeat")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, post_empty("/nope/end")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn x_workspace_header_partitions_the_log() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = router(dir.path());

    // Spawn under workspace `acme`.
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .header("x-workspace", "acme")
        .body(Body::from(json!({ "session": "s1", "rig": "granite" }).to_string()))
        .unwrap();
    let (status, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::CREATED);

    // The default workspace cannot see acme's session.
    let (status, _) = send(&app, get("/s1")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "cross-workspace isolation is structural");

    // acme sees its own.
    let req = Request::builder().uri("/s1").header("x-workspace", "acme").body(Body::empty()).unwrap();
    let (status, info) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["id"], "s1");
}
