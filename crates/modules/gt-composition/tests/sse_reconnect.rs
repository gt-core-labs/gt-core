//! SSE reconnect semantics (`hq-mcp-test.7`).
//!
//! The `/stream` feed is a poll-tail over `EventLog::read_since` (`docs/02`): a
//! client reads, remembers the last record's `ts` as its `Last-Event-ID`, and on
//! reconnect replays only records strictly newer. The invariant a streaming API
//! must hold: **no missed and no duplicated events across a disconnect**.
//!
//! `stream.rs` unit-tests `read_since`'s channel/limit/isolation filters; this
//! drives the full reconnect *scenario* — read a batch, emit more while
//! "disconnected", reconnect from the marker — and asserts the seen∪replayed
//! partition is exactly the appended set, disjoint.
//!
//! Async with a 2 ms spacing between batches so the RFC3339 record timestamps are
//! distinct (the marker is `ts`, so a same-instant collision is the one edge that
//! could drop a record); pure, no Dolt/PG.

use std::collections::BTreeSet;

use gt_composition::mcp::eventlog::EventLog;
use gt_events::EventKind;
use serde::Serialize;
use tempfile::TempDir;

#[derive(Serialize)]
struct FeedEvent {
    kind: &'static str,
    seq: u64,
}
impl EventKind for FeedEvent {
    fn kind(&self) -> &'static str {
        self.kind
    }
}

/// The seq numbers a client observed across an initial read + a reconnect must be
/// exactly the appended set, with no value seen twice.
#[tokio::test]
async fn reconnect_from_last_event_id_misses_nothing_and_duplicates_nothing() {
    let dir = TempDir::new().unwrap();
    let log = EventLog::new(Some(dir.path().to_path_buf()));
    let ws = Some("acme");

    // Batch 1 — emitted while the client is connected.
    for seq in 0..3 {
        log.append(ws, FeedEvent { kind: "merge.merged.v1", seq }).unwrap();
    }

    // Client connects and drains the tail; its Last-Event-ID is the newest record.
    let seen = log.read_since(ws, Some("merge"), None, 256).unwrap();
    let marker = seen.last().unwrap().ts.clone();
    let seen_seqs: Vec<u64> = seen.iter().map(|r| r.payload["seq"].as_u64().unwrap()).collect();
    assert_eq!(seen_seqs, [0, 1, 2], "initial read drains the tail in order");

    // Distinct timestamps for the second batch (marker is a ts).
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    // Batch 2 — emitted while the client is "disconnected".
    for seq in 3..6 {
        log.append(ws, FeedEvent { kind: "merge.merged.v1", seq }).unwrap();
    }

    // Reconnect with Last-Event-ID: only strictly-newer records replay.
    let replayed = log.read_since(ws, Some("merge"), Some(&marker), 256).unwrap();
    let replayed_seqs: Vec<u64> =
        replayed.iter().map(|r| r.payload["seq"].as_u64().unwrap()).collect();
    assert_eq!(replayed_seqs, [3, 4, 5], "reconnect replays exactly the missed events");

    // No duplication: nothing seen in batch 1 reappears on reconnect.
    let seen_set: BTreeSet<u64> = seen_seqs.iter().copied().collect();
    let replay_set: BTreeSet<u64> = replayed_seqs.iter().copied().collect();
    assert!(seen_set.is_disjoint(&replay_set), "reconnect duplicated an event");

    // No miss: the union covers every appended event.
    let union: BTreeSet<u64> = seen_set.union(&replay_set).copied().collect();
    assert_eq!(union, BTreeSet::from([0, 1, 2, 3, 4, 5]), "an event was missed");
}

/// A reconnect that has already seen everything (marker == newest ts) replays an
/// empty batch — an idle stream produces no spurious re-delivery.
#[tokio::test]
async fn reconnect_at_head_replays_nothing() {
    let dir = TempDir::new().unwrap();
    let log = EventLog::new(Some(dir.path().to_path_buf()));
    let ws = Some("acme");
    for seq in 0..3 {
        log.append(ws, FeedEvent { kind: "convoy.launched.v1", seq }).unwrap();
    }
    let all = log.read_since(ws, None, None, 256).unwrap();
    let head = all.last().unwrap().ts.clone();
    let after = log.read_since(ws, None, Some(&head), 256).unwrap();
    assert!(after.is_empty(), "caught-up client must replay nothing");
}
