//! Paso 9.D gate test (hq-92z9 Sheriff): exercise the actor with a non-trivial command
//! sequence, append every emitted `Envelope<SheriffEvent>` to a real `.events.jsonl`, then
//! replay that log into a fresh `SheriffState` and assert it is byte-identical to the
//! live actor snapshot. Mirrors `gt-merge::tests::orchestration`.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_sheriff::{
    actor as sheriff_actor, ClearWatch, InMemorySheriffRepo, ObserveWatch, RegisterWatch,
    SheriffCommand, SheriffEvent, SheriffState,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-sheriff-replay-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Drain every envelope currently pending on the relay (or arrive within `timeout`),
/// persisting each as an `EventRecord` to the log. Returns when no new event arrives for
/// `timeout` ms — the gate test is non-async-driven and emits in tight bursts so a short
/// quiet period is sufficient.
async fn drain_to_log(
    rx: &mut mpsc::Receiver<Envelope<SheriffEvent>>,
    writer: &JsonlWriter,
) -> Vec<SheriffEvent> {
    let mut log = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(env)) => {
                writer
                    .append(&EventRecord::from_envelope(&env).unwrap())
                    .unwrap();
                log.push(env.payload);
            }
            Ok(None) => break, // relay closed
            Err(_) => break,   // quiet for >100ms — done
        }
    }
    log
}

fn replay_log_into_state(records: &[EventRecord]) -> SheriffState {
    let mut state = SheriffState::default();
    for rec in records {
        let event: SheriffEvent = rec.decode().expect("decode SheriffEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<SheriffEvent>>(64);
    let handle = sheriff_actor::spawn(InMemorySheriffRepo::default(), tx);

    // Register two watches with different patterns + thresholds.
    handle
        .exec(SheriffCommand::Register(RegisterWatch {
            id: "w1".into(),
            pattern: "merge.failed".into(),
            threshold: 2,
        }))
        .await
        .expect("register w1");
    handle
        .exec(SheriffCommand::Register(RegisterWatch {
            id: "w2".into(),
            pattern: "patrol.lease_expired".into(),
            threshold: 1,
        }))
        .await
        .expect("register w2");

    // Observe w1's pattern twice — second observation crosses threshold (Observed + Raised).
    handle
        .exec(SheriffCommand::Observe(ObserveWatch {
            pattern: "merge.failed".into(),
        }))
        .await
        .expect("observe 1");
    handle
        .exec(SheriffCommand::Observe(ObserveWatch {
            pattern: "merge.failed".into(),
        }))
        .await
        .expect("observe 2 (raises w1)");

    // Subsequent observations of w1's pattern are suppressed (w1 already raised), so the
    // log gets nothing for this call — exec still returns Ok.
    handle
        .exec(SheriffCommand::Observe(ObserveWatch {
            pattern: "merge.failed".into(),
        }))
        .await
        .expect("observe 3 (suppressed)");

    // Observe an unwatched pattern → zero events, Ok.
    handle
        .exec(SheriffCommand::Observe(ObserveWatch {
            pattern: "nothing.watches.me".into(),
        }))
        .await
        .expect("observe unrelated");

    // Clear w1 → counter resets to 0, raised flag drops.
    handle
        .exec(SheriffCommand::Clear(ClearWatch { id: "w1".into() }))
        .await
        .expect("clear w1");

    // Drain the relay into the log.
    drop(handle); // closing the handle does NOT close the relay; the actor still holds tx.
    let live_log = drain_to_log(&mut rx, &writer).await;

    // Sanity on what we expect to have appended.
    let kinds: Vec<&str> = live_log
        .iter()
        .map(|e| match e {
            SheriffEvent::Registered { .. } => "registered",
            SheriffEvent::Observed { .. } => "observed",
            SheriffEvent::Raised { .. } => "raised",
            SheriffEvent::Cleared { .. } => "cleared",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "registered", // w1
            "registered", // w2
            "observed",   // w1 -> 1
            "observed",   // w1 -> 2
            "raised",     // w1 crosses threshold
            "cleared",    // w1 reset
        ],
        "unexpected event sequence in log",
    );

    // Replay the persisted log into a fresh state.
    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    // Replay-folded state must equal the in-memory state we'd build from the same events.
    let mut live_from_events = SheriffState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(
        replayed, live_from_events,
        "replay-folded state diverged from in-memory fold",
    );
}
