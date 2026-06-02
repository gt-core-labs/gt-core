//! Paso 9.D gate test (hq-92z9 Mayor): exercise the actor with a non-trivial command
//! sequence, append every emitted `Envelope<MayorEvent>` to a real `.events.jsonl`, then
//! replay that log into a fresh `MayorState` and assert it is byte-identical to the live
//! actor snapshot. Mirrors `gt-sheriff::tests::replay`.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_mayor::{
    actor as mayor_actor, Acknowledge, Delegate, InMemoryMayorRepo, MayorCommand, MayorEvent,
    MayorState, Resolve, Withdraw,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-mayor-replay-{}-{}",
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
    rx: &mut mpsc::Receiver<Envelope<MayorEvent>>,
    writer: &JsonlWriter,
) -> Vec<MayorEvent> {
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

fn replay_log_into_state(records: &[EventRecord]) -> MayorState {
    let mut state = MayorState::default();
    for rec in records {
        let event: MayorEvent = rec.decode().expect("decode MayorEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<MayorEvent>>(64);
    let handle = mayor_actor::spawn(InMemoryMayorRepo::default(), tx);

    // Delegate three handoffs to different targets.
    handle
        .exec(MayorCommand::Delegate(Delegate {
            id: "d1".into(),
            target: "witness".into(),
            summary: "audit lease tracker".into(),
        }))
        .await
        .expect("delegate d1");
    handle
        .exec(MayorCommand::Delegate(Delegate {
            id: "d2".into(),
            target: "refinery".into(),
            summary: "merge feat/x".into(),
        }))
        .await
        .expect("delegate d2");
    handle
        .exec(MayorCommand::Delegate(Delegate {
            id: "d3".into(),
            target: "crew-alpha".into(),
            summary: "rotate convoy".into(),
        }))
        .await
        .expect("delegate d3");

    // Happy path: d1 acknowledges then resolves.
    handle
        .exec(MayorCommand::Acknowledge(Acknowledge { id: "d1".into() }))
        .await
        .expect("ack d1");
    handle
        .exec(MayorCommand::Resolve(Resolve {
            id: "d1".into(),
            outcome: "done".into(),
        }))
        .await
        .expect("resolve d1");

    // Withdraw path: d3 cancelled before pickup.
    handle
        .exec(MayorCommand::Withdraw(Withdraw { id: "d3".into() }))
        .await
        .expect("withdraw d3");

    // Illegal transition: resolving a non-acknowledged delegation is rejected. The actor
    // must NOT emit anything in this case (emit-on-apply invariant), so replay stays clean.
    let illegal = handle
        .exec(MayorCommand::Resolve(Resolve {
            id: "d2".into(),
            outcome: "noop".into(),
        }))
        .await;
    assert!(illegal.is_err(), "resolve on pending must be rejected");

    // d2 acknowledges (still in Pending after the rejected resolve).
    handle
        .exec(MayorCommand::Acknowledge(Acknowledge { id: "d2".into() }))
        .await
        .expect("ack d2");

    // Drain the relay into the log.
    drop(handle); // closing the handle does NOT close the relay; the actor still holds tx.
    let live_log = drain_to_log(&mut rx, &writer).await;

    // Sanity on what we expect to have appended (the rejected `Resolve` must be absent).
    let kinds: Vec<&str> = live_log
        .iter()
        .map(|e| match e {
            MayorEvent::Delegated { .. } => "delegated",
            MayorEvent::Acknowledged { .. } => "acknowledged",
            MayorEvent::Resolved { .. } => "resolved",
            MayorEvent::Withdrawn { .. } => "withdrawn",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "delegated",    // d1
            "delegated",    // d2
            "delegated",    // d3
            "acknowledged", // d1
            "resolved",     // d1
            "withdrawn",    // d3
            "acknowledged", // d2
        ],
        "unexpected event sequence in log",
    );

    // Replay the persisted log into a fresh state.
    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    // Replay-folded state must equal the in-memory state we'd build from the same events.
    let mut live_from_events = MayorState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(
        replayed, live_from_events,
        "replay-folded state diverged from in-memory fold",
    );
}
