//! Paso 6.b gate (docs/08-getting-started.md): refinery awaits MERGE_READY on a
//! channel-events mailbox; merge actor drives the slot Ready → Merging → Merged;
//! replay of the audit log reconstructs `MergeState` byte-identical.
//!
//! This test plays the composition-root role: it owns the bus relay drain, advances the
//! slot (`Start`, then `Complete` once the simulated edge effect succeeds), persists every
//! envelope to `.events.jsonl`, and reconstructs the state from the log.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, replay, EventRecord, EventStore, JsonlWriter};
use gt_channel::Channel;
use gt_events::{Envelope, EventKind};
use gt_merge::{
    actor as merge_actor, refinery, MergeEvent, MergeReadyPayload, MergeSlotState, MergeState,
};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-merge-orch-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn recv_one(
    rx: &mut mpsc::Receiver<Envelope<MergeEvent>>,
) -> Envelope<MergeEvent> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for merge event")
        .expect("merge relay closed")
}

#[tokio::test]
async fn refinery_drives_slot_to_merged_then_replay_matches() {
    let root = fresh_root();
    let channel = Channel::open(&root, "merge").unwrap();

    let (merge_tx, mut merge_rx) = mpsc::channel::<Envelope<MergeEvent>>(64);
    let merge_handle = merge_actor::spawn(gt_merge::InMemoryMergeRepo::default(), merge_tx);
    refinery::spawn(channel.clone(), merge_handle.clone()).unwrap();

    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    // Producer side (a refinery polecat in real life): drop a MERGE_READY in the channel.
    let payload = serde_json::to_vec(&MergeReadyPayload {
        bead: "b1".into(),
        branch: "feat/work".into(),
    })
    .unwrap();
    let channel_id = channel.emit(&payload).unwrap();

    // Drive the merge flow from the bus side.
    let env1 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env1).unwrap()).unwrap();
    match &env1.payload {
        MergeEvent::Ready { bead, branch, channel_msg_id } => {
            assert_eq!(bead, "b1");
            assert_eq!(branch, "feat/work");
            assert_eq!(channel_msg_id, &channel_id);
            // Composition-root reaction: start the merge.
            merge_handle.start("b1").await;
        }
        other => panic!("expected MergeEvent::Ready, got {other:?}"),
    }

    let env2 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env2).unwrap()).unwrap();
    assert_eq!(env2.payload, MergeEvent::Started { bead: "b1".into() });
    // Simulate the edge effect (real `git merge`) succeeding.
    merge_handle.complete("b1", "deadbeefcafebabe").await;

    let env3 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env3).unwrap()).unwrap();
    assert_eq!(
        env3.payload,
        MergeEvent::Merged { bead: "b1".into(), sha: "deadbeefcafebabe".into() }
    );

    // Final board state (live actor).
    let snap = merge_handle.snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].bead, "b1");
    assert_eq!(snap[0].branch, "feat/work");
    assert_eq!(snap[0].state, MergeSlotState::Merged);

    // Channel directory drained: refinery acked the consumed message.
    // (Give the refinery a tick to ack; the actor side already saw the event but the
    // refinery's `ack` follows the `submit().await`.)
    for _ in 0..20 {
        let remaining = count_event_files(channel.dir());
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        count_event_files(channel.dir()),
        0,
        "all channel messages must be acked"
    );

    // REPLAY: log → MergeState reconstructs the same final board.
    let records = read_all(&log_path).unwrap();
    let merge_records: Vec<EventRecord> = records
        .iter()
        .filter(|r| r.kind.starts_with("merge."))
        .cloned()
        .collect();
    assert_eq!(merge_records.len(), 3, "Ready + Started + Merged");

    let state = replay(&merge_records, MergeState::default(), |s, e: &MergeEvent| s.apply(e)).unwrap();
    assert_eq!(state.merged, vec![("b1".into(), "deadbeefcafebabe".into())]);
    assert!(state.failed.is_empty());
    assert_eq!(state.board.len(), 1);
    assert_eq!(state.board.slots()[0].state, MergeSlotState::Merged);
    assert_eq!(state.board.slots()[0].branch, "feat/work");

    // Byte-identical determinism: replaying again on a fresh accumulator returns the same
    // state (gate del Paso 3 aplicado al subset merge — el núcleo es puro y total).
    let second = replay(&merge_records, MergeState::default(), |s, e: &MergeEvent| s.apply(e)).unwrap();
    assert_eq!(second, state);

    // Sanity: every payload round-trips through EventKind without panic.
    for r in &merge_records {
        let decoded: MergeEvent = r.decode().unwrap();
        assert!(!decoded.kind().is_empty());
    }

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn refinery_failed_merge_records_failed_event_and_replay_matches() {
    let root = fresh_root();
    let channel = Channel::open(&root, "merge").unwrap();

    let (merge_tx, mut merge_rx) = mpsc::channel::<Envelope<MergeEvent>>(64);
    let merge_handle = merge_actor::spawn(gt_merge::InMemoryMergeRepo::default(), merge_tx);
    refinery::spawn(channel.clone(), merge_handle.clone()).unwrap();

    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let payload = serde_json::to_vec(&MergeReadyPayload {
        bead: "b2".into(),
        branch: "feat/conflict".into(),
    })
    .unwrap();
    channel.emit(&payload).unwrap();

    let env1 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env1).unwrap()).unwrap();
    assert!(matches!(env1.payload, MergeEvent::Ready { .. }));
    merge_handle.start("b2").await;

    let env2 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env2).unwrap()).unwrap();
    assert_eq!(env2.payload, MergeEvent::Started { bead: "b2".into() });
    merge_handle.fail("b2", "merge conflict in foo.rs").await;

    let env3 = recv_one(&mut merge_rx).await;
    writer.append(&EventRecord::from_envelope(&env3).unwrap()).unwrap();
    assert!(matches!(env3.payload, MergeEvent::Failed { .. }));

    let snap = merge_handle.snapshot().await;
    assert_eq!(snap[0].state, MergeSlotState::Failed);

    let records = read_all(&log_path).unwrap();
    let state = replay(&records, MergeState::default(), |s, e: &MergeEvent| s.apply(e)).unwrap();
    assert_eq!(state.failed, vec![("b2".into(), "merge conflict in foo.rs".into())]);
    assert!(state.merged.is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

fn count_event_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter(|e| {
                    e.path().extension().and_then(|s| s.to_str()) == Some("event")
                })
                .count()
        })
        .unwrap_or(0)
}
