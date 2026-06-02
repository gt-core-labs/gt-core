//! Paso 6.d gate (docs/08-getting-started.md): a convoy drives an ordered set of member
//! beads sequentially — feed first member on launch, hand off to the next on completion,
//! close the convoy when all are done. Replay of the audit log reconstructs `OrchState`
//! byte-identical.
//!
//! This test plays the composition-root role (the mayor/deacon edge): it owns the bus relay
//! drain, tells the actor when a member's bead finished (`member_done`), persists every
//! envelope to `.events.jsonl`, and reconstructs the state from the log.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_eventlog::{read_all, replay, EventRecord, EventStore, JsonlWriter};
use gt_events::{Envelope, EventKind};
use gt_orchestration::{actor as orch_actor, ConvoyState, MemberState, OrchEvent, OrchState};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-orch-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn recv_one(rx: &mut mpsc::Receiver<Envelope<OrchEvent>>) -> Envelope<OrchEvent> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for orch event")
        .expect("orch relay closed")
}

#[tokio::test]
async fn convoy_drives_members_to_close_then_replay_matches() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (orch_tx, mut orch_rx) = mpsc::channel::<Envelope<OrchEvent>>(64);
    let orch = orch_actor::spawn(gt_orchestration::InMemoryOrchRepo::default(), orch_tx);

    // Record helper: drain one event, append it to the log, return its payload.
    macro_rules! take {
        () => {{
            let env = recv_one(&mut orch_rx).await;
            writer.append(&EventRecord::from_envelope(&env).unwrap()).unwrap();
            env.payload
        }};
    }

    // Deacon plans a 3-member convoy, then launches it.
    orch.create_convoy("cv1", vec!["m1".into(), "m2".into(), "m3".into()])
        .await;
    assert_eq!(
        take!(),
        OrchEvent::ConvoyCreated {
            convoy: "cv1".into(),
            members: vec!["m1".into(), "m2".into(), "m3".into()],
        }
    );

    orch.launch("cv1").await;
    assert_eq!(take!(), OrchEvent::ConvoyLaunched { convoy: "cv1".into() });
    // First handoff (delegation): m1 fed to crew.
    assert_eq!(
        take!(),
        OrchEvent::MemberDispatched { convoy: "cv1".into(), member: "m1".into() }
    );

    // m1's bead closes → completion observed → m2 handed off.
    orch.member_done("cv1", "m1").await;
    assert_eq!(
        take!(),
        OrchEvent::MemberCompleted { convoy: "cv1".into(), member: "m1".into() }
    );
    assert_eq!(
        take!(),
        OrchEvent::MemberDispatched { convoy: "cv1".into(), member: "m2".into() }
    );

    // m2 done → m3 handed off.
    orch.member_done("cv1", "m2").await;
    assert_eq!(
        take!(),
        OrchEvent::MemberCompleted { convoy: "cv1".into(), member: "m2".into() }
    );
    assert_eq!(
        take!(),
        OrchEvent::MemberDispatched { convoy: "cv1".into(), member: "m3".into() }
    );

    // m3 done → all members done → convoy closes.
    orch.member_done("cv1", "m3").await;
    assert_eq!(
        take!(),
        OrchEvent::MemberCompleted { convoy: "cv1".into(), member: "m3".into() }
    );
    assert_eq!(take!(), OrchEvent::ConvoyClosed { convoy: "cv1".into() });

    // Final board (live actor): convoy Closed, every member Done.
    let snap = orch.snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].id, "cv1");
    assert_eq!(snap[0].state, ConvoyState::Closed);
    assert!(snap[0].members.iter().all(|m| m.state == MemberState::Done));

    // REPLAY: the log reconstructs the same OrchState.
    let records = read_all(&log_path).unwrap();
    let orch_records: Vec<EventRecord> = records
        .iter()
        .filter(|r| r.kind.starts_with("convoy."))
        .cloned()
        .collect();
    // ConvoyCreated + Launched + 3×Dispatched + 3×Completed + ConvoyClosed = 9.
    assert_eq!(orch_records.len(), 9);

    let state = replay(&orch_records, OrchState::default(), |s, e: &OrchEvent| s.apply(e)).unwrap();
    assert_eq!(
        state.dispatched,
        vec![
            ("cv1".into(), "m1".into()),
            ("cv1".into(), "m2".into()),
            ("cv1".into(), "m3".into()),
        ]
    );
    assert_eq!(state.closed, vec!["cv1".to_string()]);
    assert!(state.failed.is_empty());
    let cv = &state.convoys["cv1"];
    assert_eq!(cv.state, ConvoyState::Closed);
    assert!(cv.members.iter().all(|m| m.state == MemberState::Done));

    // Byte-identical determinism: a second replay on a fresh accumulator is equal (gate of
    // Paso 3 applied to the orch subset — the core is pure and total).
    let second = replay(&orch_records, OrchState::default(), |s, e: &OrchEvent| s.apply(e)).unwrap();
    assert_eq!(second, state);

    // Sanity: every payload round-trips through EventKind without panic.
    for r in &orch_records {
        let decoded: OrchEvent = r.decode().unwrap();
        assert!(!decoded.kind().is_empty());
    }

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn convoy_member_failure_halts_then_replay_matches() {
    let root = fresh_root();
    let log_path = root.join("events.jsonl");
    let writer = JsonlWriter::new(&log_path);

    let (orch_tx, mut orch_rx) = mpsc::channel::<Envelope<OrchEvent>>(64);
    let orch = orch_actor::spawn(gt_orchestration::InMemoryOrchRepo::default(), orch_tx);

    macro_rules! take {
        () => {{
            let env = recv_one(&mut orch_rx).await;
            writer.append(&EventRecord::from_envelope(&env).unwrap()).unwrap();
            env.payload
        }};
    }

    orch.create_convoy("cv2", vec!["a".into(), "b".into()]).await;
    assert!(matches!(take!(), OrchEvent::ConvoyCreated { .. }));
    orch.launch("cv2").await;
    assert!(matches!(take!(), OrchEvent::ConvoyLaunched { .. }));
    assert!(matches!(take!(), OrchEvent::MemberDispatched { .. }));

    // a's bead fails → member failed + convoy halted. b is never dispatched.
    orch.member_fail("cv2", "a", "build broke").await;
    assert_eq!(
        take!(),
        OrchEvent::MemberFailed {
            convoy: "cv2".into(),
            member: "a".into(),
            reason: "build broke".into(),
        }
    );
    assert_eq!(
        take!(),
        OrchEvent::ConvoyFailed {
            convoy: "cv2".into(),
            member: "a".into(),
            reason: "build broke".into(),
        }
    );

    let snap = orch.snapshot().await;
    assert_eq!(snap[0].state, ConvoyState::Failed);

    let records = read_all(&log_path).unwrap();
    let state = replay(&records, OrchState::default(), |s, e: &OrchEvent| s.apply(e)).unwrap();
    assert_eq!(
        state.failed,
        vec![("cv2".into(), "a".into(), "build broke".into())]
    );
    assert!(state.closed.is_empty());
    assert_eq!(state.convoys["cv2"].state, ConvoyState::Failed);
    // b stayed Pending — never handed off after the halt.
    let b = state.convoys["cv2"].members.iter().find(|m| m.bead == "b").unwrap();
    assert_eq!(b.state, MemberState::Pending);

    let _ = std::fs::remove_dir_all(&root);
}
