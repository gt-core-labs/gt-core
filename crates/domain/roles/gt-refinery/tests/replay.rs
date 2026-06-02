//! Paso 9.D gate test (hq-92z9 Refinery): drive the actor through a non-trivial command
//! sequence, append every emitted envelope to a real `.events.jsonl`, replay the log into
//! a fresh `RefineryState`, assert byte-identical. Mirrors gt-sheriff / gt-deacon.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_refinery::{
    actor as refinery_actor, InMemoryRefineryRepo, MarkDispatched, ObserveReady,
    RefineryCommand, RefineryEvent, RefineryState,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-refinery-replay-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn drain_to_log(
    rx: &mut mpsc::Receiver<Envelope<RefineryEvent>>,
    writer: &JsonlWriter,
) -> Vec<RefineryEvent> {
    let mut log = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(env)) => {
                writer
                    .append(&EventRecord::from_envelope(&env).unwrap())
                    .unwrap();
                log.push(env.payload);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    log
}

fn replay_log_into_state(records: &[EventRecord]) -> RefineryState {
    let mut state = RefineryState::default();
    for rec in records {
        let event: RefineryEvent = rec.decode().expect("decode RefineryEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<RefineryEvent>>(64);
    let handle = refinery_actor::spawn(InMemoryRefineryRepo::default(), tx);

    // Observe two distinct merge-readies.
    handle
        .exec(RefineryCommand::Observe(ObserveReady {
            bead: "b1".into(),
            branch: "feat/a".into(),
            channel_msg_id: "01H000000000000000000000A".into(),
            at: 1_000,
        }))
        .await
        .expect("observe b1");
    handle
        .exec(RefineryCommand::Observe(ObserveReady {
            bead: "b2".into(),
            branch: "feat/b".into(),
            channel_msg_id: "01H000000000000000000000B".into(),
            at: 1_005,
        }))
        .await
        .expect("observe b2");

    // Mark b1 dispatched; second mark is idempotent (zero events).
    handle
        .exec(RefineryCommand::MarkDispatched(MarkDispatched {
            bead: "b1".into(),
        }))
        .await
        .expect("dispatch b1");
    handle
        .exec(RefineryCommand::MarkDispatched(MarkDispatched {
            bead: "b1".into(),
        }))
        .await
        .expect("dispatch b1 (idempotent)");

    drop(handle);
    let live_log = drain_to_log(&mut rx, &writer).await;

    let kinds: Vec<&str> = live_log
        .iter()
        .map(|e| match e {
            RefineryEvent::Observed { .. } => "observed",
            RefineryEvent::Dispatched { .. } => "dispatched",
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["observed", "observed", "dispatched"],
        "unexpected event sequence (second MarkDispatched should have been idempotent)",
    );

    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    let mut live_from_events = RefineryState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(
        replayed, live_from_events,
        "replay-folded state diverged from in-memory fold",
    );

    // Terminal post-conditions.
    assert_eq!(replayed.entries.len(), 2);
    assert!(replayed.entries.get("b1").map(|e| e.dispatched).unwrap_or(false));
    assert!(!replayed.entries.get("b2").map(|e| e.dispatched).unwrap_or(true));
}
