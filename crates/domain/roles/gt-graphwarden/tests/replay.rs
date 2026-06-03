//! Replay gate (docs/04 §11, `hq-graphrig.6`): drive the warden actor with a non-trivial
//! command sequence, append every emitted `Envelope<WardenEvent>` to a real `.events.jsonl`,
//! then replay that log into a fresh `WardenState` and assert it is byte-identical to the
//! in-memory fold. Mirrors `gt-witness::tests::replay`.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, EventRecord, EventStore, JsonlWriter};
use gt_events::Envelope;
use gt_graphwarden::{
    actor as warden_actor, InMemoryGraphWardenRepo, MarkRefreshed, MarkStale, RegisterRig,
    Unregister, WardenCommand, WardenEvent, WardenState,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-graphwarden-replay-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn drain_to_log(
    rx: &mut mpsc::Receiver<Envelope<WardenEvent>>,
    writer: &JsonlWriter,
) -> Vec<WardenEvent> {
    let mut log = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(env)) => {
                writer.append(&EventRecord::from_envelope(&env).unwrap()).unwrap();
                log.push(env.payload);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    log
}

fn replay_log_into_state(records: &[EventRecord]) -> WardenState {
    let mut state = WardenState::default();
    for rec in records {
        let event: WardenEvent = rec.decode().expect("decode WardenEvent");
        state.apply(&event).expect("apply");
    }
    state
}

#[tokio::test]
async fn live_actor_state_matches_replay_byte_identical() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (tx, mut rx) = mpsc::channel::<Envelope<WardenEvent>>(64);
    let handle = warden_actor::spawn(InMemoryGraphWardenRepo::default(), tx);

    // Two rigs enter custody.
    handle
        .exec(WardenCommand::Register(RegisterRig {
            rig: "alpha".into(),
            repo_dir: "/repo/alpha".into(),
            now_secs: 100,
        }))
        .await
        .expect("register alpha");
    handle
        .exec(WardenCommand::Register(RegisterRig {
            rig: "beta".into(),
            repo_dir: "/repo/beta".into(),
            now_secs: 100,
        }))
        .await
        .expect("register beta");

    // alpha goes stale twice (pending accumulates), then refreshes.
    handle
        .exec(WardenCommand::MarkStale(MarkStale { rig: "alpha".into(), changed: 2, now_secs: 110 }))
        .await
        .expect("stale alpha 1");
    handle
        .exec(WardenCommand::MarkStale(MarkStale { rig: "alpha".into(), changed: 3, now_secs: 120 }))
        .await
        .expect("stale alpha 2");
    handle
        .exec(WardenCommand::MarkRefreshed(MarkRefreshed {
            rig: "alpha".into(),
            commit: "abc1234".into(),
            now_secs: 130,
        }))
        .await
        .expect("refresh alpha");

    // beta leaves the catalog.
    handle
        .exec(WardenCommand::Unregister(Unregister { rig: "beta".into(), now_secs: 140 }))
        .await
        .expect("unregister beta");

    drop(handle);
    let live_log = drain_to_log(&mut rx, &writer).await;

    use gt_events::EventKind;
    let kinds: Vec<&str> = live_log.iter().map(|e| e.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            "graphwarden.rig-registered.v1",
            "graphwarden.rig-registered.v1",
            "graphwarden.marked-stale.v1",
            "graphwarden.marked-stale.v1",
            "graphwarden.refreshed.v1",
            "graphwarden.unregistered.v1",
        ],
        "unexpected event sequence in log",
    );

    let records = read_all(&log_path).expect("read log");
    let replayed = replay_log_into_state(&records);

    let mut live_from_events = WardenState::default();
    for ev in &live_log {
        live_from_events.apply(ev).expect("apply");
    }
    assert_eq!(replayed, live_from_events, "replay-folded state diverged from in-memory fold");

    // Concrete post-state: alpha fresh at abc1234, beta gone.
    assert_eq!(replayed.rigs.len(), 1);
    let alpha = &replayed.rigs["alpha"];
    assert!(!alpha.stale);
    assert_eq!(alpha.pending_changes, 0);
    assert_eq!(alpha.last_indexed_commit.as_deref(), Some("abc1234"));
}
