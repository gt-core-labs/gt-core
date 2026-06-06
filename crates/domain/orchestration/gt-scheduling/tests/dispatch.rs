//! Gate del trigger de dispatch (`hq-orchd-deploy.4`): una petición soltada en el `gt-channel`
//! atraviesa el loop `dispatch::run`, siembra+encola el bead en el dispatcher, y el actor
//! auto-despacha emitiendo `scheduling.dispatched.v1` sobre el bus — el evento que el supervisor
//! de polecats observa para slingar el agente. Hermano del gate de la refinery en `gt-merge`.

use std::time::Duration;

use tokio::sync::mpsc;

use gt_beads::InMemoryBeads;
use gt_channel::Channel;
use gt_events::Envelope;
use gt_scheduling::{actor, dispatch, DispatchPayload, SchedEvent};

fn fresh_root() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "gt-sched-dispatch-{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn recv_one(rx: &mut mpsc::Receiver<Envelope<SchedEvent>>) -> Envelope<SchedEvent> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for sched event")
        .expect("sched relay closed")
}

#[tokio::test]
async fn channel_request_dispatches_the_bead() {
    let root = fresh_root();
    let channel = Channel::open(&root, "dispatch").unwrap();

    let (sched_tx, mut sched_rx) = mpsc::channel::<Envelope<SchedEvent>>(64);
    let sched = actor::spawn(InMemoryBeads::default(), sched_tx, 4);
    dispatch::spawn(channel.clone(), sched).unwrap();

    // Producer side (an operator / control plane): drop a dispatch request in the channel.
    let payload = serde_json::to_vec(&DispatchPayload {
        bead: "hq-demo.1".into(),
        title: Some("demo bead".into()),
        priority: 0,
    })
    .unwrap();
    channel.emit(&payload).unwrap();

    // The actor logs the enqueue, then the pump claims capacity and dispatches.
    let env1 = recv_one(&mut sched_rx).await;
    assert_eq!(
        env1.payload,
        SchedEvent::Enqueue {
            bead: "hq-demo.1".into(),
            priority: 0
        }
    );

    let env2 = recv_one(&mut sched_rx).await;
    match env2.payload {
        SchedEvent::Dispatched { bead, .. } => assert_eq!(bead, "hq-demo.1"),
        other => panic!("expected Dispatched, got {other:?}"),
    }

    // Channel drained: the loop acked after pushing the work to the actor.
    for _ in 0..20 {
        let remaining = std::fs::read_dir(channel.dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "event").unwrap_or(false))
            .count();
        if remaining == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("dispatch request was never acked");
}

#[tokio::test]
async fn malformed_request_is_acked_not_redelivered() {
    let root = fresh_root();
    let channel = Channel::open(&root, "dispatch").unwrap();

    let (sched_tx, _sched_rx) = mpsc::channel::<Envelope<SchedEvent>>(64);
    let sched = actor::spawn(InMemoryBeads::default(), sched_tx, 4);
    dispatch::spawn(channel.clone(), sched).unwrap();

    channel.emit(b"this is not json").unwrap();

    // A poison message must be acked (deleted) so it never loops.
    for _ in 0..20 {
        let remaining = std::fs::read_dir(channel.dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "event").unwrap_or(false))
            .count();
        if remaining == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("malformed request was not acked");
}
