//! Paso 9.D gate test (hq-92z9 Deacon): exercise the actor with a non-trivial command
//! sequence, append every emitted `Envelope<DeaconEvent>` to a real `.events.jsonl`, then
//! replay that log into a fresh `DeaconState` and assert it is byte-identical to the
//! state we build by folding the in-memory event log. Mirrors gt-sheriff/gt-merge.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_deacon::{
    actor as deacon_actor, BeginDrain, DeaconCommand, DeaconEvent, DeaconState, FinishItem,
    InMemoryDeaconRepo, TrackItem,
};
use gt_events::Envelope;

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-deacon-replay-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn drain_to_log(
    rx: &mut mpsc::Receiver<Envelope<DeaconEvent>>,
    writer: &JsonlWriter,
) -> Vec<DeaconEvent> {
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

fn replay_log_into_state(records: &[EventRecord]) -> DeaconState {
    let mut state = DeaconState::default();
    for rec in records {
        let event: DeaconEvent = rec.decode().expect("decode DeaconEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<DeaconEvent>>(64);
    let handle = deacon_actor::spawn(InMemoryDeaconRepo::default(), tx);

    // Track two items BEFORE drain begins — pending should be {a, b}, draining false.
    handle
        .exec(DeaconCommand::Track(TrackItem {
            id: "a".into(),
            kind: "merge".into(),
        }))
        .await
        .expect("track a");
    handle
        .exec(DeaconCommand::Track(TrackItem {
            id: "b".into(),
            kind: "orch".into(),
        }))
        .await
        .expect("track b");

    // BeginDrain — flips draining=true, pending still {a, b}.
    handle
        .exec(DeaconCommand::BeginDrain(BeginDrain {
            by: "sigterm".into(),
            at: 1_000,
        }))
        .await
        .expect("begin drain");

    // Re-issuing BeginDrain is a no-op (idempotent) — produces zero events.
    handle
        .exec(DeaconCommand::BeginDrain(BeginDrain {
            by: "sigterm".into(),
            at: 1_001,
        }))
        .await
        .expect("begin drain (idempotent)");

    // Finish a — pending {b}, drain still pending.
    handle
        .exec(DeaconCommand::Finish(FinishItem {
            id: "a".into(),
            at: 1_010,
        }))
        .await
        .expect("finish a");

    // Finish b — empties pending; since draining=true, DrainComplete fires in same tick.
    handle
        .exec(DeaconCommand::Finish(FinishItem {
            id: "b".into(),
            at: 1_020,
        }))
        .await
        .expect("finish b (closes drain)");

    drop(handle);
    let live_log = drain_to_log(&mut rx, &writer).await;

    // Sanity on the event sequence.
    let kinds: Vec<&str> = live_log
        .iter()
        .map(|e| match e {
            DeaconEvent::ItemBegun { .. } => "item_begun",
            DeaconEvent::DrainRequested { .. } => "drain_requested",
            DeaconEvent::ItemFinished { .. } => "item_finished",
            DeaconEvent::DrainComplete { .. } => "drain_complete",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "item_begun",      // track a
            "item_begun",      // track b
            "drain_requested", // first BeginDrain
            // second BeginDrain was idempotent: no event
            "item_finished",   // finish a
            "item_finished",   // finish b
            "drain_complete",  // pending empty + draining
        ],
        "unexpected event sequence in log",
    );

    // Replay the persisted log into a fresh state.
    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    // Replay-folded state must equal the in-memory fold over the same events.
    let mut live_from_events = DeaconState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(
        replayed, live_from_events,
        "replay-folded state diverged from in-memory fold",
    );

    // Post-conditions on the terminal state.
    assert!(replayed.draining, "drain flag should remain set");
    assert!(replayed.completed, "completion flag should be set");
    assert!(replayed.pending.is_empty(), "pending set should be empty");
}
