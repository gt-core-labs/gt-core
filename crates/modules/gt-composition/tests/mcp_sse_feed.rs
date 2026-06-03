//! SSE feed gate (`hq-mcp-test.7`): `/stream` per-workspace + Last-Event-ID + KeepAlive.
//!
//! Drives the real `feed_router` in-process (a `tower` `oneshot`) and reads the
//! streaming Server-Sent-Events body, asserting the docs/02 contract:
//!
//! - **per-workspace keying** — the tenant is the `X-Workspace` header (never a
//!   URL/body field), and one tenant's stream never carries another's events,
//! - **`?channel=`** narrows the feed to one domain's event namespace,
//! - **Last-Event-ID** resumes strictly after a prior record's id,
//! - **KeepAlive** — the response is a live `text/event-stream` (the keep-alive
//!   comment cadence is configured on the `Sse`).
//!
//! Pure in-process (tempdir event log); runs in host CI with no sidecar.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gt_composition::stream::{feed_router, FeedState};
use gt_composition::mcp::eventlog::EventLog;
use gt_events::EventKind;
use http_body_util::BodyExt;
use serde::Serialize;
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

/// A minimal event for seeding the log (mirrors the stream unit tests).
#[derive(Serialize)]
struct TestEvent {
    #[serde(skip)]
    kind: &'static str,
    n: u64,
}
impl EventKind for TestEvent {
    fn kind(&self) -> &'static str {
        self.kind
    }
}

/// One parsed SSE frame: its `id:` (the Last-Event-ID resume marker) and decoded
/// `data:` JSON payload.
#[derive(Debug)]
struct SseEvent {
    id: Option<String>,
    data: Value,
}

/// Read the SSE body, collecting parsed events until `want` are seen or the
/// bounded wait elapses (the stream stays open for keep-alive, so we never read
/// to EOF). Comment lines (`:` keep-alive) are skipped.
async fn collect_events(body: Body, want: usize) -> Vec<SseEvent> {
    let mut buf = String::new();
    let mut events = Vec::new();
    let mut body = body;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    while events.len() < want && tokio::time::Instant::now() < deadline {
        let frame = match tokio::time::timeout(Duration::from_millis(200), body.frame()).await {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue, // poll timeout — keep waiting until the deadline
        };
        if let Some(chunk) = frame.data_ref() {
            buf.push_str(&String::from_utf8_lossy(chunk));
        }
        // Parse any complete `\n\n`-delimited SSE blocks accumulated so far.
        while let Some(idx) = buf.find("\n\n") {
            let block: String = buf.drain(..idx + 2).collect();
            let mut id = None;
            let mut data = None;
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("id:") {
                    id = Some(v.trim().to_string());
                } else if let Some(v) = line.strip_prefix("data:") {
                    data = serde_json::from_str(v.trim()).ok();
                }
            }
            if let Some(data) = data {
                events.push(SseEvent { id, data });
            }
        }
    }
    events
}

/// `oneshot` a GET `/stream` request with the given headers/query against a fresh
/// router over `log`, returning the response.
async fn stream_request(
    log: Arc<EventLog>,
    query: &str,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let router = feed_router(FeedState::new(log));
    let mut req = Request::builder().uri(format!("/stream{query}"));
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    router.oneshot(req.body(Body::empty()).unwrap()).await.unwrap()
}

fn seeded_log() -> (TempDir, Arc<EventLog>) {
    let dir = TempDir::new().unwrap();
    let log = EventLog::new(Some(dir.path().to_path_buf()));
    // acme: two merge events + one convoy event; beta: one merge event.
    log.append(Some("acme"), TestEvent { kind: "merge.submitted.v1", n: 1 }).unwrap();
    log.append(Some("acme"), TestEvent { kind: "convoy.launched.v1", n: 2 }).unwrap();
    log.append(Some("acme"), TestEvent { kind: "merge.merged.v1", n: 3 }).unwrap();
    log.append(Some("beta"), TestEvent { kind: "merge.merged.v1", n: 9 }).unwrap();
    (dir, Arc::new(log))
}

#[tokio::test]
async fn stream_is_event_stream_keyed_to_the_workspace() {
    let (_dir, log) = seeded_log();

    let resp = stream_request(log, "", &[("x-workspace", "acme")]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ctype = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        ctype.starts_with("text/event-stream"),
        "the feed is a live SSE stream, got content-type `{ctype}`"
    );

    // acme's three events replay; beta's (n=9) never appears.
    let events = collect_events(resp.into_body(), 3).await;
    assert_eq!(events.len(), 3, "acme replays exactly its own three events");
    let ns: Vec<u64> = events.iter().map(|e| e.data["n"].as_u64().unwrap()).collect();
    assert_eq!(ns, vec![1, 2, 3], "chronological, tenant-isolated (no n=9 leak)");
    // The id is the resume marker (the record ts), present on every frame.
    assert!(events.iter().all(|e| e.id.is_some()), "every frame carries an id");
}

#[tokio::test]
async fn channel_query_narrows_the_feed() {
    let (_dir, log) = seeded_log();
    let resp = stream_request(log, "?channel=merge", &[("x-workspace", "acme")]).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let events = collect_events(resp.into_body(), 2).await;
    assert_eq!(events.len(), 2, "only the two merge.* events, not convoy");
    let ns: Vec<u64> = events.iter().map(|e| e.data["n"].as_u64().unwrap()).collect();
    assert_eq!(ns, vec![1, 3]);
}

#[tokio::test]
async fn last_event_id_resumes_strictly_after_the_marker() {
    let (_dir, log) = seeded_log();

    // First connect: capture the first event's id (its resume marker).
    let first = collect_events(
        stream_request(log.clone(), "", &[("x-workspace", "acme")]).await.into_body(),
        3,
    )
    .await;
    let marker = first[0].id.clone().expect("first event has an id");

    // Reconnect with Last-Event-ID = that marker: only strictly-newer events.
    let resumed = collect_events(
        stream_request(log, "", &[("x-workspace", "acme"), ("last-event-id", &marker)])
            .await
            .into_body(),
        2,
    )
    .await;
    assert!(resumed.len() < 3, "resume excludes the marker and everything older");
    assert!(
        resumed.iter().all(|e| e.data["n"].as_u64().unwrap() > 1),
        "only events after the first replay on resume"
    );
}

#[tokio::test]
async fn another_tenant_sees_only_its_own_events() {
    let (_dir, log) = seeded_log();
    let events = collect_events(
        stream_request(log, "", &[("x-workspace", "beta")]).await.into_body(),
        1,
    )
    .await;
    assert_eq!(events.len(), 1, "beta has exactly one event");
    assert_eq!(events[0].data["n"].as_u64(), Some(9), "and it is beta's own");
}
