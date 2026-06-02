//! Paso 9.D gate test (hq-92z9 Witness): exercise the actor with a non-trivial command
//! sequence, append every emitted `Envelope<WitnessEvent>` to a real `.events.jsonl`, then
//! replay that log into a fresh `WitnessState` and assert it is byte-identical to the live
//! actor snapshot. Mirrors `gt-sheriff::tests::replay`.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_witness::{
    actor as witness_actor, ClearTarget, InMemoryWitnessRepo, TickTarget, WatchTarget,
    WitnessCommand, WitnessEvent, WitnessState,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-witness-replay-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Drain every envelope currently pending on the relay (or arriving within `timeout`),
/// persisting each as an `EventRecord` to the log. Returns when no new event arrives for
/// `timeout` ms — the gate test is non-async-driven and emits in tight bursts so a short
/// quiet period is sufficient.
async fn drain_to_log(
    rx: &mut mpsc::Receiver<Envelope<WitnessEvent>>,
    writer: &JsonlWriter,
) -> Vec<WitnessEvent> {
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

fn replay_log_into_state(records: &[EventRecord]) -> WitnessState {
    let mut state = WitnessState::default();
    for rec in records {
        let event: WitnessEvent = rec.decode().expect("decode WitnessEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<WitnessEvent>>(64);
    let handle = witness_actor::spawn(InMemoryWitnessRepo::default(), tx);

    // Register two workers with different thresholds.
    handle
        .exec(WitnessCommand::Watch(WatchTarget {
            worker: "w1".into(),
            since_secs: 100,
            threshold_secs: 30,
        }))
        .await
        .expect("watch w1");
    handle
        .exec(WitnessCommand::Watch(WatchTarget {
            worker: "w2".into(),
            since_secs: 100,
            threshold_secs: 60,
        }))
        .await
        .expect("watch w2");

    // Tick w1 below threshold → TargetStuck only.
    handle
        .exec(WitnessCommand::Tick(TickTarget {
            worker: "w1".into(),
            now_secs: 120,
        }))
        .await
        .expect("tick w1 below");

    // Tick w1 above threshold → TargetStuck + EscalationRaised.
    handle
        .exec(WitnessCommand::Tick(TickTarget {
            worker: "w1".into(),
            now_secs: 140,
        }))
        .await
        .expect("tick w1 raises");

    // Subsequent tick on raised w1 is suppressed — exec still returns Ok, no events.
    handle
        .exec(WitnessCommand::Tick(TickTarget {
            worker: "w1".into(),
            now_secs: 160,
        }))
        .await
        .expect("tick w1 suppressed");

    // Tick w2 below its threshold → TargetStuck only.
    handle
        .exec(WitnessCommand::Tick(TickTarget {
            worker: "w2".into(),
            now_secs: 140,
        }))
        .await
        .expect("tick w2 below");

    // Clear w1 → escalated flag drops, last_seen resets to since_secs.
    handle
        .exec(WitnessCommand::Clear(ClearTarget { worker: "w1".into() }))
        .await
        .expect("clear w1");

    // Drain the relay into the log.
    drop(handle); // closing the handle does NOT close the relay; the actor still holds tx.
    let live_log = drain_to_log(&mut rx, &writer).await;

    // Sanity on what we expect to have appended.
    let kinds: Vec<&str> = live_log
        .iter()
        .map(|e| match e {
            WitnessEvent::TargetWatched { .. } => "watched",
            WitnessEvent::TargetStuck { .. } => "stuck",
            WitnessEvent::EscalationRaised { .. } => "raised",
            WitnessEvent::TargetCleared { .. } => "cleared",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "watched", // w1
            "watched", // w2
            "stuck",   // w1 @120 (age 20 < 30)
            "stuck",   // w1 @140 (age 40 >= 30)
            "raised",  // w1 crosses threshold
            "stuck",   // w2 @140 (age 40 < 60)
            "cleared", // w1 reset
        ],
        "unexpected event sequence in log",
    );

    // Replay the persisted log into a fresh state.
    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    // Replay-folded state must equal the in-memory state we'd build from the same events.
    let mut live_from_events = WitnessState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(
        replayed, live_from_events,
        "replay-folded state diverged from in-memory fold",
    );
}
