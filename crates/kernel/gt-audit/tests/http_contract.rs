//! Contract for the read-only audit REST adapter (`hq-fe-api-kernel.2`).
//!
//! Drives [`gt_audit::audit_router`] in-process with `tower`'s `oneshot` against a seeded
//! [`InMemoryAudit`] sink, proving the REST surface tails the **same** [`AuditSink`] the MCP
//! `audit.tail` tool reads: `GET /` returns the caller's workspace records most-recent-first, the
//! `actor`/`tool`/`outcome`/`since`/`limit` querystring filters narrow the window, and —
//! critically — the per-tenant gate keys on the `X-Workspace` header, so workspace `acme` never
//! sees workspace `other`'s calls. The filtering itself is unit-covered in `http.rs`; this is the
//! integration proof that the route is wired to the sink and the header.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the
//! in-memory sink answers from memory, so the contract runs on host CI with no sidecar.
#![cfg(feature = "axum")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_audit::{audit_router, AuditApiState, AuditRecord, AuditSink, InMemoryAudit, Outcome};

/// The `X-Workspace` header the adapter reads the tenant from (kept in sync with `http.rs`).
const WORKSPACE_HEADER: &str = "x-workspace";

/// Build the REST router over a sink seeded with records across two tenants, actors, tools, and
/// outcomes — the same fixture the MCP handler's tests use.
fn router() -> axum::Router {
    let sink = InMemoryAudit::new();
    for r in [
        AuditRecord::new("alice", "issues.close.execute", json!({}), Outcome::Invoked, "2026-06-01T10:00:00Z").in_workspace("acme"),
        AuditRecord::new("bob", "merge.submit.execute", json!({}), Outcome::Unauthorized, "2026-06-02T10:00:00Z").in_workspace("acme"),
        AuditRecord::new("alice", "rig.add.execute", json!({}), Outcome::Invoked, "2026-06-03T10:00:00Z").in_workspace("acme"),
        AuditRecord::new("mallory", "issues.close.execute", json!({}), Outcome::Invoked, "2026-06-03T11:00:00Z").in_workspace("other"),
    ] {
        sink.record(r).unwrap();
    }
    let sink: Arc<dyn AuditSink + Send + Sync> = Arc::new(sink);
    audit_router(AuditApiState::new(sink))
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
async fn tails_caller_workspace_most_recent_first() {
    let app = router();
    let (status, body) = send(&app, get_in("acme", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 3, "the three acme records, not the `other` one");
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs[0]["tool"], "rig.add.execute", "newest first");
    assert_eq!(recs[2]["tool"], "issues.close.execute");
}

#[tokio::test]
async fn never_leaks_another_tenant() {
    let app = router();
    let (_, acme) = send(&app, get_in("acme", "/")).await;
    assert!(acme["records"].as_array().unwrap().iter().all(|r| r["actor"] != "mallory"));
    let (status, other) = send(&app, get_in("other", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other["count"], 1);
    assert_eq!(other["records"][0]["actor"], "mallory");
}

#[tokio::test]
async fn querystring_filters_narrow_the_window() {
    let app = router();
    let (_, by_actor) = send(&app, get_in("acme", "/?actor=alice")).await;
    assert_eq!(by_actor["count"], 2);
    let (_, by_outcome) = send(&app, get_in("acme", "/?outcome=unauthorized")).await;
    assert_eq!(by_outcome["count"], 1);
    assert_eq!(by_outcome["records"][0]["actor"], "bob");
    let (_, since) = send(&app, get_in("acme", "/?since=2026-06-02T00:00:00Z")).await;
    assert_eq!(since["count"], 2);
    let (_, capped) = send(&app, get_in("acme", "/?limit=1")).await;
    assert_eq!(capped["count"], 1);
    assert_eq!(capped["records"][0]["tool"], "rig.add.execute", "the newest");
}

#[tokio::test]
async fn absent_header_defaults_to_the_default_workspace() {
    let app = router();
    // No `X-Workspace` ⇒ the "default" tenant, which has no seeded records ⇒ an empty window.
    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let (status, body) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0, "default workspace holds none of the seeded acme/other records");
}
