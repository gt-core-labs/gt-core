//! Gate tests for `gt-feed` (Paso 6.g).
//!
//! 1. Deterministic replay — `Curator::fold` over the same log twice yields byte-identical
//!    `FeedState` (Paso 3 gate carried into the feed).
//! 2. `detect` flags `UnhandledEvent` when the dead-letter source has one.
//! 3. `detect` flags `DeadLetterDrain` for a handler error.
//! 4. `detect` flags `TimeoutMissed` when a correlation closes on an explicit failure marker
//!    without any success marker.
//! 5. `detect` stays quiet when a failure marker is paired with a later success marker
//!    (recovery — re-enqueue + merged).

use serde_json::json;

use gt_eventlog::EventRecord;
use gt_feed::activity::{self, ActivityInfo};
use gt_feed::{detect, escalation, Curator, DeadEntry, FeedProblem, FeedState};

fn record(event_id: &str, correlation_id: &str, ts: &str, kind: &str) -> EventRecord {
    EventRecord {
        event_id: event_id.into(),
        correlation_id: correlation_id.into(),
        causation_id: None,
        ts: ts.into(),
        kind: kind.into(),
        payload: json!({}),
    }
}

#[test]
fn replay_is_byte_identical() {
    let log = vec![
        record("e1", "c1", "2026-05-27T10:00:00Z", "agent.spawned"),
        record("e2", "c1", "2026-05-27T10:00:01Z", "agent.heartbeat"),
        record("e3", "c2", "2026-05-27T10:00:02Z", "scheduling.enqueue"),
        record("e4", "c2", "2026-05-27T10:00:03Z", "scheduling.dispatched"),
    ];

    let a = Curator::fold(&log);
    let b = Curator::fold(&log);

    assert_eq!(a, b, "FeedState must be deterministic over the same log");
    let a_json = serde_json::to_string(&a).unwrap();
    let b_json = serde_json::to_string(&b).unwrap();
    assert_eq!(a_json, b_json, "serialized FeedState must be byte-identical");

    assert_eq!(a.total_events, 4);
    assert_eq!(a.correlations.len(), 2);
    assert_eq!(a.kind_totals.get("agent.heartbeat"), Some(&1));
}

#[test]
fn fold_equals_streaming_apply() {
    let log = vec![
        record("e1", "c1", "2026-05-27T10:00:00Z", "agent.spawned"),
        record("e2", "c1", "2026-05-27T10:00:01Z", "agent.heartbeat"),
    ];

    let folded = Curator::fold(&log);
    let mut streamed = FeedState::new();
    for rec in &log {
        Curator::apply(&mut streamed, rec);
    }
    assert_eq!(folded, streamed);
}

#[test]
fn detect_flags_unhandled_event() {
    let state = FeedState::new();
    let dead = vec![DeadEntry::Unhandled {
        kind: "agent.spawned".into(),
        event_id: "e1".into(),
        correlation_id: Some("c1".into()),
    }];

    let problems = detect(&state, &dead);

    assert_eq!(problems.len(), 1);
    assert!(matches!(
        problems[0],
        FeedProblem::UnhandledEvent { ref kind, .. } if kind == "agent.spawned"
    ));
}

#[test]
fn detect_flags_handler_error_as_dead_letter_drain() {
    let state = FeedState::new();
    let dead = vec![DeadEntry::HandlerError {
        kind: "merge.ready".into(),
        error: "repo upsert failed: disk full".into(),
    }];

    let problems = detect(&state, &dead);

    assert_eq!(problems.len(), 1);
    assert!(matches!(
        problems[0],
        FeedProblem::DeadLetterDrain { ref kind, .. } if kind == "merge.ready"
    ));
}

#[test]
fn detect_flags_timeout_missed_when_failure_has_no_success() {
    let log = vec![
        record("e1", "c1", "2026-05-27T10:00:00Z", "scheduling.enqueue"),
        record("e2", "c1", "2026-05-27T10:00:01Z", "scheduling.dispatched"),
        record("e3", "c1", "2026-05-27T10:00:30Z", "scheduling.dispatch_timeout"),
    ];

    let state = Curator::fold(&log);
    let problems = detect(&state, &[]);

    assert_eq!(problems.len(), 1, "timeout without success should flag");
    match &problems[0] {
        FeedProblem::TimeoutMissed {
            correlation_id,
            terminal_kind,
            ..
        } => {
            assert_eq!(correlation_id, "c1");
            assert_eq!(terminal_kind, "scheduling.dispatch_timeout");
        }
        other => panic!("expected TimeoutMissed, got {other:?}"),
    }
}

#[test]
fn detect_quiet_when_failure_followed_by_success_recovery() {
    let log = vec![
        record("e1", "c1", "2026-05-27T10:00:00Z", "patrol.lease_registered"),
        record("e2", "c1", "2026-05-27T10:00:30Z", "patrol.lease_expired"),
        record("e3", "c1", "2026-05-27T10:00:40Z", "patrol.lease_closed"),
    ];

    let state = Curator::fold(&log);
    let problems = detect(&state, &[]);

    assert!(
        problems.is_empty(),
        "recovery (expired + later success) must not flag; got {problems:?}",
    );
}

// --- hq-mysw: activity color-coding (port of internal/activity) -------------------------

#[test]
fn activity_color_thresholds_match_go_original() {
    // <5m green, 5-10m yellow, >10m red.
    assert!(ActivityInfo::from_age(0).is_active());
    assert!(ActivityInfo::from_age(4 * 60).is_active());
    assert!(ActivityInfo::from_age(5 * 60).is_stale(), "exactly 5m is stale");
    assert!(ActivityInfo::from_age(9 * 60).is_stale());
    assert!(ActivityInfo::from_age(10 * 60).is_stuck(), "exactly 10m is stuck");
    assert!(ActivityInfo::from_age(60 * 60).is_stuck());
    assert_eq!(ActivityInfo::unknown().color, activity::COLOR_UNKNOWN);
    assert_eq!(ActivityInfo::unknown().age_secs, None);
}

#[test]
fn activity_format_age_is_short() {
    assert_eq!(ActivityInfo::from_age(30).formatted_age, "<1m");
    assert_eq!(ActivityInfo::from_age(5 * 60).formatted_age, "5m");
    assert_eq!(ActivityInfo::from_age(2 * 60 * 60).formatted_age, "2h");
    assert_eq!(ActivityInfo::from_age(3 * 24 * 60 * 60).formatted_age, "3d");
}

#[test]
fn activity_view_codes_each_correlation_against_now() {
    // c-fresh: last event 1 minute before now → green.
    // c-stuck: last event 20 minutes before now → red.
    let log = vec![
        record("e1", "c-fresh", "2026-05-27T10:00:00Z", "agent.spawned"),
        record("e2", "c-fresh", "2026-05-27T10:09:00Z", "agent.heartbeat"),
        record("e3", "c-stuck", "2026-05-27T09:50:00Z", "scheduling.dispatched"),
    ];
    let state = Curator::fold(&log);
    // now = 2026-05-27T10:10:00Z
    let now = activity::parse_epoch_secs("2026-05-27T10:10:00Z").unwrap();

    let rows = activity::activity_view(&state, now);
    let fresh = rows.iter().find(|r| r.subject == "c-fresh").unwrap();
    let stuck = rows.iter().find(|r| r.subject == "c-stuck").unwrap();

    assert!(fresh.activity.is_active(), "1m old → green, got {:?}", fresh.activity);
    assert!(stuck.activity.is_stuck(), "20m old → red, got {:?}", stuck.activity);
}

#[test]
fn activity_view_unknown_for_unparseable_ts() {
    let log = vec![record("e1", "c1", "not-a-timestamp", "agent.spawned")];
    let state = Curator::fold(&log);
    let rows = activity::activity_view(&state, 1_000_000);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].activity.color, activity::COLOR_UNKNOWN);
}

#[test]
fn activity_view_clamps_future_timestamp_to_zero_age() {
    // last_ts after now (clock skew) → age clamped to 0 → green, never underflow.
    let log = vec![record("e1", "c1", "2026-05-27T10:00:00Z", "agent.spawned")];
    let state = Curator::fold(&log);
    let now = activity::parse_epoch_secs("2026-05-27T09:00:00Z").unwrap();
    let rows = activity::activity_view(&state, now);
    assert_eq!(rows[0].activity.age_secs, Some(0));
    assert!(rows[0].activity.is_active());
}

// --- hq-mysw: escalation intents from feed gaps -----------------------------------------

#[test]
fn escalation_intents_raised_for_stuck_and_handler_error() {
    let problems = vec![
        FeedProblem::TimeoutMissed {
            correlation_id: "c-merge".into(),
            terminal_kind: "merge.failed".into(),
            terminal_ts: "2026-05-27T10:00:00Z".into(),
        },
        FeedProblem::DeadLetterDrain {
            kind: "merge.ready".into(),
            error: "disk full".into(),
        },
    ];
    let intents = escalation::intents(&problems);
    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].class, "timeout_missed");
    assert_eq!(intents[0].subject, "c-merge");
    assert_eq!(intents[1].class, "handler_error");
    assert_eq!(intents[1].subject, "merge.ready");
}

#[test]
fn escalation_intents_skip_unhandled_event() {
    // Unhandled = no subscriber → feed-only, no operator escalation.
    let problems = vec![FeedProblem::UnhandledEvent {
        kind: "agent.spawned".into(),
        event_id: "e1".into(),
        correlation_id: Some("c1".into()),
    }];
    assert!(escalation::intents(&problems).is_empty());
}
