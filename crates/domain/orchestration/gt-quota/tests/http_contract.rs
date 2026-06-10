//! Contract for the quota REST adapter (`hq-fe-api-orch.4`).
//!
//! Drives [`gt_quota::quota_router`] in-process with `tower`'s `oneshot` against an **in-memory**
//! per-workspace event-log provider, proving the REST surface dispatches to the **same**
//! [`QuotaCommand`](gt_quota::QuotaCommand) execute layer the MCP tools use: a `POST
//! /:account/probe` materializes the account in the replayed projection, a `POST
//! /:account/rotate` parks the source in `Cooldown`, `GET /` and `GET /:account` read the
//! rehydrated registry, the account always comes from the path on a mutation, a missing account
//! is a `404`, and — critically — the log is per-tenant, so an account in workspace `a` is
//! invisible to workspace `b`. The wire mapping (error → status, `now_secs` stamping, path-field
//! override) is unit-covered in `http.rs`; this is the integration proof that the routes are
//! wired to a workspace-scoped, event-sourced store.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the
//! provider keeps each workspace's events in memory and replays them through the same
//! [`QuotaState`](gt_quota::QuotaState) reducer the real `EventLog` uses, so the contract runs on
//! host CI with no sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_quota::{
    quota_router, AccountRegistry, QuotaApiState, QuotaEvent, QuotaState, WorkspaceQuota,
};
use gt_workspace::WORKSPACE_HEADER;

/// An in-memory [`WorkspaceQuota`] provider: one append-only `Vec<QuotaEvent>` per workspace
/// slug, replayed through [`QuotaState::apply`] → [`AccountRegistry::from_state`] on every read —
/// the exact read-modify-append the real `EventLog`-backed handler runs, so two tenants get
/// genuinely separate logs (the REST mirror of the path-partitioned-per-workspace event store).
#[derive(Default)]
struct MemLog {
    by_ws: Mutex<HashMap<String, Vec<QuotaEvent>>>,
}

#[async_trait]
impl WorkspaceQuota for MemLog {
    async fn registry(&self, workspace: &str) -> Result<AccountRegistry, AppError> {
        let guard = self.by_ws.lock().unwrap();
        let mut state = QuotaState::default();
        if let Some(events) = guard.get(workspace) {
            for ev in events {
                state.apply(ev);
            }
        }
        Ok(AccountRegistry::from_state(&state))
    }

    async fn append(&self, workspace: &str, event: QuotaEvent) -> Result<(), AppError> {
        self.by_ws
            .lock()
            .unwrap()
            .entry(workspace.to_string())
            .or_default()
            .push(event);
        Ok(())
    }
}

/// Build the REST router over a fresh in-memory log provider.
fn router() -> axum::Router {
    quota_router(QuotaApiState::new(Arc::new(MemLog::default())))
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
async fn probe_then_read_roundtrips_through_the_log() {
    let app = router();

    // POST /:account/probe materializes the account entry in the replayed projection.
    let (status, body) = send(
        &app,
        post_in("acme", "/acc-1/probe", json!({ "remaining": 250, "resets_at_secs": 20_000 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "probe ok: {body}");
    assert_eq!(body["event"], "quota.usage_probed.v1");

    // GET /:account sees it (Healthy, materialized by the replayed probe).
    let (status, info) = send(&app, get_in("acme", "/acc-1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["id"], "acc-1");
    assert_eq!(info["status"], "Healthy");

    // GET / lists it.
    let (status, list) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(list["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["id"] == "acc-1"));
}

#[tokio::test]
async fn rotate_takes_from_account_from_path_and_parks_source() {
    let app = router();
    // Materialize the source account first.
    send(
        &app,
        post_in("acme", "/acc-1/probe", json!({ "remaining": 100, "resets_at_secs": 20_000 })),
    )
    .await;
    // Materialize the rotation target too: RotateAccount only accepts a destination that is
    // registered AND Healthy (a074fc5), which a probe with remaining>0 creates.
    send(
        &app,
        post_in("acme", "/acc-2/probe", json!({ "remaining": 250, "resets_at_secs": 20_000 })),
    )
    .await;

    // The body names a DECOY from_account; the path must win (docs/03 Rule 6) — `acc-1` rotates.
    let (status, body) = send(
        &app,
        post_in("acme", "/acc-1/rotate", json!({ "from_account": "decoy", "to_account": "acc-2" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["event"], "quota.rotated.v1");

    // The source is parked in Cooldown (replayed onto the entry).
    let (_, info) = send(&app, get_in("acme", "/acc-1")).await;
    assert_eq!(info["status"], "Cooldown", "rotation parked the source");
}

#[tokio::test]
async fn sample_appends_a_usage_event() {
    let app = router();
    let (status, body) = send(
        &app,
        post_in(
            "acme",
            "/acc-1/sample",
            json!({ "session": "s1", "model": "opus", "input": 100, "output": 100, "cache_read": 0, "cache_creation": 0 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sample ok: {body}");
    assert_eq!(body["event"], "quota.tokens_sampled.v1");
}

#[tokio::test]
async fn self_rotation_is_422() {
    let app = router();
    let (status, _) = send(
        &app,
        post_in("acme", "/acc-1/rotate", json!({ "to_account": "acc-1" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "cannot rotate onto itself");
}

#[tokio::test]
async fn missing_account_is_404() {
    let app = router();
    let (status, _) = send(&app, get_in("acme", "/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn log_is_per_workspace() {
    let app = router();

    // Probe the SAME account id in BOTH tenants — per-tenant logs, no cross-talk.
    send(
        &app,
        post_in("a", "/shared/probe", json!({ "remaining": 10, "resets_at_secs": 1 })),
    )
    .await;
    // An account only in `a`.
    send(
        &app,
        post_in("a", "/only-a/probe", json!({ "remaining": 10, "resets_at_secs": 1 })),
    )
    .await;

    // `b` has never probed anything: it sees an empty catalog, never `a`'s accounts.
    let (_, list_b) = send(&app, get_in("b", "/")).await;
    assert!(
        list_b["accounts"].as_array().unwrap().is_empty(),
        "no cross-tenant leak from a into b",
    );
    let (status, _) = send(&app, get_in("b", "/only-a")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a's account is not found in b");
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

// hq-quota-weekly.5 ---------------------------------------------------------------

/// AC2: a probe without weekly headers → `weekly_window` is `null` (not absent) in both
/// `GET /:account` and `GET /`.
#[tokio::test]
async fn weekly_window_is_null_when_no_weekly_probe() {
    let app = router();
    send(
        &app,
        post_in("acme", "/acc-1/probe", json!({ "remaining": 250, "resets_at_secs": 20_000 })),
    )
    .await;

    let (_, info) = send(&app, get_in("acme", "/acc-1")).await;
    assert_eq!(
        info["weekly_window"],
        Value::Null,
        "GET /:account: weekly_window must be null, not absent, when no weekly data"
    );

    let (_, list) = send(&app, get_in("acme", "/")).await;
    let acc = list["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "acc-1")
        .expect("acc-1 in list");
    assert_eq!(
        acc["weekly_window"],
        Value::Null,
        "GET /: weekly_window must be null, not absent, when no weekly data"
    );
}

/// AC1: a probe that includes weekly headers → `weekly_window` carries
/// `{consumed, limit, resets_at_secs, started_at_secs, kind: \"Weekly\"}`.
#[tokio::test]
async fn weekly_window_populated_after_weekly_probe() {
    let app = router();
    send(
        &app,
        post_in(
            "acme",
            "/acc-1/probe",
            json!({
                "remaining": 250,
                "resets_at_secs": 20_000,
                "weekly_remaining": 10_000_000,
                "weekly_resets_at_secs": 604_800,
            }),
        ),
    )
    .await;

    let (_, info) = send(&app, get_in("acme", "/acc-1")).await;
    let ww = &info["weekly_window"];
    assert!(ww.is_object(), "GET /:account: weekly_window must be an object after weekly probe; got {ww}");
    assert_eq!(ww["kind"], "Weekly", "kind must be Weekly");
    assert!(ww["consumed"].as_f64().is_some(), "consumed present");
    assert!(ww["limit"].as_u64().is_some(), "limit present");
    assert!(ww["resets_at_secs"].as_u64().is_some(), "resets_at_secs present");
    assert!(ww["started_at_secs"].as_u64().is_some(), "started_at_secs present");

    // Also verify via GET /.
    let (_, list) = send(&app, get_in("acme", "/")).await;
    let list_ww = list["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "acc-1")
        .and_then(|a| a.get("weekly_window"))
        .expect("acc-1.weekly_window in list");
    assert_eq!(list_ww["kind"], "Weekly", "list: kind must be Weekly");
}
