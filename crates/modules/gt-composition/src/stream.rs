//! Per-workspace SSE event feed (`hq-mcp-dispatch.10`).
//!
//! The streaming side of the MCP-derived API: a `GET /stream` endpoint that fans a
//! workspace's event log out as Server-Sent Events, keyed per `(workspace, channel)`
//! so a subscription only ever sees its own tenant's events. The source is the
//! **path-partitioned** event log (`mt-data.8`): each workspace has its own log
//! directory, so cross-workspace isolation is structural — this handler reads the
//! caller's directory and no other.
//!
//! Per docs/02 (the SSE pattern):
//! - **Per-workspace keying** — the tenant comes from the server-injected
//!   `X-Workspace` header, never a URL/body field. `?channel=<namespace>` narrows
//!   the feed to one domain (`merge`, `convoy`, …).
//! - **Last-Event-ID** — a reconnecting `EventSource` sends the last event's id (its
//!   record `ts`); the handler replays only newer records, then resumes live.
//! - **KeepAlive** — a comment line every 15s so proxies don't drop the idle stream.
//!
//! Live delivery is a **poll-tail** of the partitioned log (there is no broadcast
//! bus on the MCP server path yet): the stream re-reads records newer than the last
//! id every second. That is the docs/02 "polling is fine for cheap state" tier; a
//! `tokio::sync::broadcast` fan-out is the follow-up when one lands on this path.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::Router;
use futures_core::Stream;
use serde::Deserialize;

use gt_eventlog::EventRecord;

use crate::mcp::EventLog;

/// The server-injected tenant header the stream is keyed by (mirrors the MCP
/// dispatch `X-Workspace` convention). The workspace is never read from the URL or
/// body — only this header — so a caller cannot stream another tenant's feed.
const WORKSPACE_HEADER: &str = "x-workspace";

/// Most records a connect/reconnect replays — a reconnect can't drain an unbounded
/// backlog, and a fresh connect seeds with recent context only.
const REPLAY_CAP: usize = 256;

/// Poll cadence for new records on the partitioned log (the live tail).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Proxy keep-alive cadence — proxies drop idle SSE streams after ~60s (docs/02).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Shared state for the feed route: the per-workspace event log.
#[derive(Clone)]
pub struct FeedState {
    event_log: Arc<EventLog>,
}

impl FeedState {
    /// Wrap the per-workspace event log the feed streams from.
    pub fn new(event_log: Arc<EventLog>) -> Self {
        Self { event_log }
    }
}

/// `?channel=<namespace>` narrows the feed to one domain's events (`merge`,
/// `convoy`, `quota`, …). Absent ⇒ the whole workspace feed.
#[derive(Debug, Default, Deserialize)]
pub struct FeedParams {
    #[serde(default)]
    channel: Option<String>,
}

/// The SSE feed router (`GET /stream`), ready to `.merge()` into the server's app.
pub fn feed_router(state: FeedState) -> Router {
    Router::new().route("/stream", get(feed_stream)).with_state(state)
}

/// Stream a workspace's event feed as SSE. The tenant is the `X-Workspace` header;
/// `?channel=` narrows to one domain; `Last-Event-ID` resumes from a prior record.
async fn feed_stream(
    State(state): State<FeedState>,
    headers: HeaderMap,
    Query(params): Query<FeedParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Tenant + channel keying. The workspace is server-injected (header), never the
    // query — the query carries only the channel filter.
    let workspace = header(&headers, WORKSPACE_HEADER);
    let channel = params.channel.filter(|c| !c.is_empty());
    // EventSource resends the last event's id on reconnect; our id is the record ts.
    let resume_from = header(&headers, "last-event-id");

    let log = state.event_log.clone();
    let stream = async_stream::stream! {
        // First pass with `resume_from` replays exactly what a reconnecting client
        // missed (or, on a fresh connect, seeds the recent tail); subsequent passes
        // are the live poll-tail, each reading only records newer than the last id.
        let mut last_ts = resume_from;
        loop {
            let batch = log
                .read_since(workspace.as_deref(), channel.as_deref(), last_ts.as_deref(), REPLAY_CAP)
                .unwrap_or_default();
            for record in batch {
                last_ts = Some(record.ts.clone());
                yield Ok(record_event(&record));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(KEEPALIVE_INTERVAL)
            .text("keep-alive"),
    )
}

/// Trimmed, non-empty value of a request header, or `None`.
fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Render one log record as an SSE event: the event name is the versioned kind
/// (`merge.merged.v1`, so clients subscribe by name), the id is the record `ts`
/// (the `Last-Event-ID` resume marker), and the data is the payload JSON.
fn record_event(record: &EventRecord) -> Event {
    Event::default()
        .event(record.kind.clone())
        .id(record.ts.clone())
        .data(record.payload.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_events::{Envelope, EventKind};
    use serde::Serialize;
    use tempfile::TempDir;

    /// A minimal event for driving the log in tests.
    #[derive(Serialize)]
    struct TestEvent {
        kind: &'static str,
        n: u64,
    }
    impl EventKind for TestEvent {
        fn kind(&self) -> &'static str {
            self.kind
        }
    }

    fn log(dir: &TempDir) -> EventLog {
        EventLog::new(Some(dir.path().to_path_buf()))
    }

    /// `read_since` keys by workspace + channel + resume marker. This is the core of
    /// the SSE feed (the handler is a thin poll-tail over it).
    #[test]
    fn read_since_filters_by_channel_and_resume_marker() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);

        // Two channels in one workspace's log.
        log.append(Some("acme"), TestEvent { kind: "merge.submitted.v1", n: 1 }).unwrap();
        log.append(Some("acme"), TestEvent { kind: "convoy.launched.v1", n: 2 }).unwrap();
        log.append(Some("acme"), TestEvent { kind: "merge.merged.v1", n: 3 }).unwrap();

        // Whole feed.
        let all = log.read_since(Some("acme"), None, None, 256).unwrap();
        assert_eq!(all.len(), 3);

        // Channel filter: only merge.* (not convoy, not a "merger" false-prefix).
        let merges = log.read_since(Some("acme"), Some("merge"), None, 256).unwrap();
        assert_eq!(merges.len(), 2);
        assert!(merges.iter().all(|r| r.kind.starts_with("merge.")));

        // Resume marker: only records strictly newer than a given ts.
        let first_ts = all[0].ts.clone();
        let after = log.read_since(Some("acme"), None, Some(&first_ts), 256).unwrap();
        assert!(after.iter().all(|r| r.ts.as_str() > first_ts.as_str()));
        assert!(after.len() < all.len(), "resume excludes the marker and older");
    }

    /// The feed is path-partitioned per workspace: one tenant's stream never sees
    /// another's events (the cross-workspace isolation guarantee).
    #[test]
    fn read_since_is_isolated_per_workspace() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);

        log.append(Some("acme"), TestEvent { kind: "merge.merged.v1", n: 1 }).unwrap();
        log.append(Some("beta"), TestEvent { kind: "merge.merged.v1", n: 2 }).unwrap();

        let acme = log.read_since(Some("acme"), None, None, 256).unwrap();
        let beta = log.read_since(Some("beta"), None, None, 256).unwrap();
        assert_eq!(acme.len(), 1);
        assert_eq!(beta.len(), 1);
        // Distinct payloads — no bleed between tenants.
        assert_eq!(acme[0].payload["n"], 1);
        assert_eq!(beta[0].payload["n"], 2);
    }

    /// `limit` caps the batch from the newest end.
    #[test]
    fn read_since_caps_from_the_newest_end() {
        let dir = TempDir::new().unwrap();
        let log = log(&dir);
        for n in 0..10 {
            log.append(Some("acme"), TestEvent { kind: "merge.merged.v1", n }).unwrap();
        }
        let capped = log.read_since(Some("acme"), None, None, 3).unwrap();
        assert_eq!(capped.len(), 3, "only the newest 3 are returned");
        // Newest end: the last appended (n=9) is included.
        assert_eq!(capped.last().unwrap().payload["n"], 9);
    }
}
