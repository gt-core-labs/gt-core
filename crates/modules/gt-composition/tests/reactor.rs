//! End-to-end proof (`hq-mod-refactor.22`): [`compose_workspace`] wires the whole per-workspace
//! composition root, and both cross-domain reactor arms fire through the REAL `gt_plugin` relay.
//!
//! - `patrol.lease-expired.v1` → the scheduler re-enqueues the freed bead.
//! - `merge.ready.v1` → the merge actor advances the slot to `Merging`.

use std::time::Duration;

use gt_composition::{compose_workspace, Composed};
use gt_events::Envelope;
use gt_eventlog::EventRecord;
use gt_merge::MergeEvent;
use gt_patrol::PatrolEvent;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;
use tokio::sync::mpsc;

fn record(kind: &str, payload: serde_json::Value) -> EventRecord {
    EventRecord {
        event_id: "e".into(),
        correlation_id: "c".into(),
        causation_id: None,
        ts: "2026-06-02T12:00:00Z".into(),
        kind: kind.into(),
        payload,
    }
}

fn publish(c: &Composed, kind: &str, payload: serde_json::Value) {
    c.handle
        .events_sender()
        .send(record(kind, payload))
        .expect("hub has subscribers");
}

async fn next<T: gt_events::EventKind>(rx: &mut mpsc::Receiver<Envelope<T>>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("actor reacted before timeout")
        .expect("sink open")
        .payload
}

#[tokio::test]
async fn compose_workspace_wires_both_reactor_arms() {
    let mut c = compose_workspace(WorkspaceId::new("acme").expect("valid slug")).await;

    // ARM 1 — patrol.lease-expired.v1 → scheduler re-enqueue.
    let le = PatrolEvent::LeaseExpired {
        bead: "gg-7".into(),
        worker: "polecat-3".into(),
        priority: 1,
    };
    publish(&c, "patrol.lease-expired.v1", serde_json::to_value(&le).unwrap());
    assert!(
        matches!(next(&mut c.sched_events).await, SchedEvent::Enqueue { ref bead, priority } if bead == "gg-7" && priority == 1),
        "lease expiry should re-enqueue the freed bead through the relay",
    );

    // ARM 2 — merge.ready.v1 → merge.start. Pre-submit so the slot exists in Ready, then drain
    // the submit's own Ready event before publishing the observed-ready through the hub.
    c.merge.submit("gg-9", "wt/gg-9", "01ABC.event").await;
    assert!(
        matches!(next(&mut c.merge_events).await, MergeEvent::Ready { ref bead, .. } if bead == "gg-9"),
        "submit creates the Ready slot",
    );
    let ready = MergeEvent::Ready {
        bead: "gg-9".into(),
        branch: "wt/gg-9".into(),
        channel_msg_id: "01ABC.event".into(),
    };
    publish(&c, "merge.ready.v1", serde_json::to_value(&ready).unwrap());
    assert!(
        matches!(next(&mut c.merge_events).await, MergeEvent::Started { ref bead } if bead == "gg-9"),
        "observed-ready should advance the slot to Merging through the relay",
    );

    c.handle.shutdown().await;
}
