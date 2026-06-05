//! Issue movement on the SSE feed gate (`hq-issues-sse.5`).
//!
//! End-to-end over the **real** pipeline, pure in-process (tempdir event log, no
//! sidecar): an [`EventLogIssueSink`] — the composition-root sink both the REST and MCP
//! mutation paths publish to — appends an issue mutation to the per-workspace log, and
//! the same `feed_router` the server mounts at `GET /stream` delivers it as an SSE
//! frame on `?channel=issues`, carrying the full changed row.
//!
//! This is the integration the unit tests can't see: that an emitted [`IssueEvent`]
//! actually reaches a `/stream` subscriber with the channel-routable kind, keyed to the
//! mutation's tenant and isolated from every other. The tracker is Dolt-backed and the
//! row read-back needs a live store, so these tests construct the [`IssueEvent`]
//! directly (the sink → log → feed leg is exactly what was previously missing); the
//! Dolt read-back leg is covered by the gated dispatch e2e.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

use gt_composition::mcp::eventlog::{EventLog, EventLogIssueSink};
use gt_composition::stream::{feed_router, FeedState};
use gt_issues::{IssueEvent, IssueEventSink, IssueVerb};

/// One parsed SSE frame: its `event:` name (the versioned kind), `id:` (resume marker)
/// and decoded `data:` JSON payload.
#[derive(Debug)]
struct SseEvent {
    event: Option<String>,
    id: Option<String>,
    data: Value,
}

/// Read the SSE body, collecting parsed frames until `want` are seen or a bounded wait
/// elapses (the stream stays open for keep-alive, so we never read to EOF).
async fn collect_events(body: Body, want: usize) -> Vec<SseEvent> {
    let mut buf = String::new();
    let mut events = Vec::new();
    let mut body = body;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    while events.len() < want && tokio::time::Instant::now() < deadline {
        let frame = match tokio::time::timeout(Duration::from_millis(200), body.frame()).await {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        };
        if let Some(chunk) = frame.data_ref() {
            buf.push_str(&String::from_utf8_lossy(chunk));
        }
        while let Some(idx) = buf.find("\n\n") {
            let block: String = buf.drain(..idx + 2).collect();
            let (mut event, mut id, mut data) = (None, None, None);
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = Some(v.trim().to_string());
                } else if let Some(v) = line.strip_prefix("id:") {
                    id = Some(v.trim().to_string());
                } else if let Some(v) = line.strip_prefix("data:") {
                    data = serde_json::from_str(v.trim()).ok();
                }
            }
            if let Some(data) = data {
                events.push(SseEvent { event, id, data });
            }
        }
    }
    events
}

/// `oneshot` a GET `/stream` request against a fresh feed router over `log`.
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
    router
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// A transitioned-issue event carrying a representative row.
fn transitioned(id: &str, status: &str) -> IssueEvent {
    IssueEvent {
        verb: IssueVerb::Transitioned,
        id: id.to_string(),
        actor: "mcp-local".into(),
        issue: Some(serde_json::json!({ "id": id, "status": status, "version": 2 })),
    }
}

#[tokio::test]
async fn an_issue_mutation_is_delivered_on_the_issues_channel_with_the_full_row() {
    let dir = TempDir::new().unwrap();
    let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
    let sink = EventLogIssueSink::new(log.clone());

    // The mutation path (REST or MCP) emits through the sink.
    sink.emit(Some("acme"), &transitioned("hq-x.1", "working"));

    // A browser EventSource subscribes to the issues channel.
    let resp = stream_request(log, "?channel=issues", &[("x-workspace", "acme")]).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let events = collect_events(resp.into_body(), 1).await;
    assert_eq!(events.len(), 1, "the emitted issue event is delivered");
    let ev = &events[0];
    // Named by the versioned kind so a client subscribes by event name.
    assert_eq!(ev.event.as_deref(), Some("issues.transitioned.v1"));
    // The full changed row rides the payload — the client patches in place, no re-fetch.
    assert_eq!(ev.data["verb"], "transitioned");
    assert_eq!(ev.data["id"], "hq-x.1");
    assert_eq!(ev.data["issue"]["status"], "working");
    assert_eq!(ev.data["issue"]["version"], 2);
    // The id is the resume marker (record ts).
    assert!(ev.id.is_some(), "every frame carries a Last-Event-ID");
}

#[tokio::test]
async fn the_issues_channel_excludes_other_domains() {
    let dir = TempDir::new().unwrap();
    let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
    let sink = EventLogIssueSink::new(log.clone());

    // Two issue events plus a foreign-domain event in the same workspace log.
    sink.emit(Some("acme"), &transitioned("hq-x.1", "working"));
    log.append(
        Some("acme"),
        // A merge event must not leak onto ?channel=issues.
        {
            #[derive(serde::Serialize)]
            struct E {
                #[serde(skip)]
                kind: &'static str,
            }
            impl gt_events::EventKind for E {
                fn kind(&self) -> &'static str {
                    self.kind
                }
            }
            E {
                kind: "merge.merged.v1",
            }
        },
    )
    .unwrap();
    sink.emit(Some("acme"), &transitioned("hq-x.2", "closed"));

    let resp = stream_request(log, "?channel=issues", &[("x-workspace", "acme")]).await;
    let events = collect_events(resp.into_body(), 2).await;
    assert_eq!(
        events.len(),
        2,
        "only the two issue events, never the merge one"
    );
    assert!(
        events
            .iter()
            .all(|e| e.event.as_deref().unwrap_or("").starts_with("issues.")),
        "the issues channel carries only issues.* kinds"
    );
}

#[tokio::test]
async fn an_issue_event_is_isolated_to_its_tenant() {
    let dir = TempDir::new().unwrap();
    let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
    let sink = EventLogIssueSink::new(log.clone());

    // The mutation happened in acme; a beta subscriber must never see it.
    sink.emit(Some("acme"), &transitioned("hq-x.1", "working"));

    let resp = stream_request(log, "?channel=issues", &[("x-workspace", "beta")]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let events = collect_events(resp.into_body(), 1).await;
    assert!(
        events.is_empty(),
        "another tenant's feed never carries acme's issue event"
    );
}
