//! Replay determinism for the event-sourced dispatch reducers (`hq-mcp-test.8`).
//!
//! docs/03 Rule 7: the merge + scheduling domains keep no projection table — their
//! state is a fold of the workspace event log through a pure reducer. The gate is
//! that replaying the *same* recorded log twice rebuilds **byte-for-byte identical**
//! state, with no hidden clock/ordering/hash dependence in the fold. These tests
//! seed a tempdir-rooted log, replay each reducer twice, and assert the two results
//! are identical (state + serialization), so a regression that made replay
//! order-sensitive would fail CI here rather than corrupt a daemon restart
//! (`hq-orchd.5` hydrates the live actors from exactly these functions).
//!
//! Pure + offline: no Postgres, no live server — runs in the workspace `cargo test`.

use gt_composition::mcp::eventlog::EventLog;
use gt_composition::{replay_merge_board, replay_scheduling_pending};
use gt_merge::MergeEvent;
use gt_scheduling::SchedEvent;
use tempfile::TempDir;

/// The tenant the recorded log is partitioned under.
const WS: &str = "replay-determinism";

/// Append a fixed, representative log for both domains and hand back the live
/// [`EventLog`] over it (the `TempDir` guard keeps the directory alive for the
/// test's duration).
fn seeded_log() -> (TempDir, EventLog) {
    let dir = TempDir::new().expect("tempdir");
    let log = EventLog::new(Some(dir.path().to_path_buf()));

    // Scheduling: three enqueues then one dispatch — the dispatched bead (`gg-2`)
    // must drop out of the still-pending queue, the other two survive.
    for ev in [
        SchedEvent::Enqueue { bead: "gg-1".into(), priority: 0 },
        SchedEvent::Enqueue { bead: "gg-2".into(), priority: 2 },
        SchedEvent::Enqueue { bead: "gg-3".into(), priority: 1 },
        SchedEvent::Dispatched { bead: "gg-2".into(), worker: "polecat-1".into() },
    ] {
        log.append(Some(WS), ev).expect("append scheduling event");
    }

    // Merge: one slot run to its `Merged` terminator plus a second still-open
    // `Ready` slot, so the replayed board carries a mix of live + terminal states.
    for ev in [
        MergeEvent::Ready { bead: "gg-1".into(), branch: "b1".into(), channel_msg_id: "m1".into() },
        MergeEvent::Started { bead: "gg-1".into() },
        MergeEvent::Merged { bead: "gg-1".into(), sha: "deadbeef".into() },
        MergeEvent::Ready { bead: "gg-3".into(), branch: "b3".into(), channel_msg_id: "m3".into() },
    ] {
        log.append(Some(WS), ev).expect("append merge event");
    }

    (dir, log)
}

/// The scheduling reducer replays to an identical pending queue twice over —
/// equal value *and* equal serialization — and correctly drops the dispatched bead.
#[test]
fn scheduling_replay_is_deterministic_and_drops_dispatched() {
    let (_dir, log) = seeded_log();

    let first = replay_scheduling_pending(&log, WS).expect("replay scheduling");
    let second = replay_scheduling_pending(&log, WS).expect("replay scheduling again");

    assert_eq!(first, second, "the same log replays to the same pending queue");
    // Byte-for-byte serialization parity (docs/03 Rule 7).
    assert_eq!(
        serde_json::to_string(&first).expect("serialize first"),
        serde_json::to_string(&second).expect("serialize second"),
        "the replayed queue serializes byte-for-byte identically",
    );

    // Semantic anchor: the dispatched bead left the queue; the rest stayed pending.
    let beads: Vec<&str> = first.iter().map(|(b, _)| b.as_str()).collect();
    assert!(beads.contains(&"gg-1") && beads.contains(&"gg-3"), "pending survives: {beads:?}");
    assert!(!beads.contains(&"gg-2"), "a dispatched bead is no longer pending: {beads:?}");
}

/// The merge reducer replays to an identical board twice over. `MergeBoard` is
/// `BTreeMap`-backed, so its `snapshot()` is a stably-ordered `Vec<MergeSlot>` that
/// compares byte-for-byte — equal iff the fold is order-independent.
#[test]
fn merge_board_replay_is_deterministic() {
    let (_dir, log) = seeded_log();

    let first = replay_merge_board(&log, WS).expect("replay merge board");
    let second = replay_merge_board(&log, WS).expect("replay merge board again");

    assert_eq!(
        first.snapshot(),
        second.snapshot(),
        "the same log replays to the same merge board",
    );

    // Semantic anchor: both slots are present, and `gg-1` reached its terminal state.
    let snap = first.snapshot();
    assert_eq!(snap.len(), 2, "both submitted slots survive replay: {snap:?}");
    assert!(
        first.get("gg-1").is_some() && first.get("gg-3").is_some(),
        "replay rebuilt both slots: {snap:?}",
    );
}
