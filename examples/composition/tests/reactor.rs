//! End-to-end proof (`hq-mod-refactor.21`): the cross-domain reactor as observers. Assemble a
//! per-workspace [`RootHandle`] event hub, register the role observers + the [`SchedulerPlugin`]
//! onto it, publish a `patrol.lease-expired.v1` through the hub, and assert the scheduler
//! re-enqueues the freed bead — through the REAL `gt_plugin` relay, no direct `on_event` call.

use std::sync::Arc;

use composition::SchedulerPlugin;
use gt_beads::InMemoryBeads;
use gt_events::Envelope;
use gt_eventlog::EventRecord;
use gt_module::RootBuilder;
use gt_patrol::PatrolEvent;
use gt_roles::{RoleSinks, RoleStack};
use gt_runtime::RootHandle;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;
use tokio::sync::mpsc;

#[tokio::test]
async fn lease_expiry_reenqueues_through_the_scheduler_observer() {
    // Per-workspace root + event hub.
    let ws = WorkspaceId::new("acme").expect("valid slug");
    let handle = RootHandle::new(ws, RootBuilder::new().build().expect("empty root builds"));

    // Scheduler actor; keep its event sink to observe the reaction.
    let (sched_tx, mut sched_rx) = mpsc::channel::<Envelope<SchedEvent>>(16);
    let sched = gt_scheduling::actor::spawn(InMemoryBeads::default(), sched_tx, 4);

    // Role actors anchored to the handle's supervisor (sinks parked).
    let (st, _sr) = mpsc::channel(16);
    let (wt, _wr) = mpsc::channel(16);
    let (dt, _dr) = mpsc::channel(16);
    let (mt, _mr) = mpsc::channel(16);
    let (rt, _rr) = mpsc::channel(16);
    let stack = RoleStack::register(
        handle.supervisor(),
        RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt },
    )
    .await
    .expect("registers while Built");

    // Full registry = the role observers + the cross-domain scheduler arm, on one hub.
    let registry = stack
        .plugin_registry()
        .register(SchedulerPlugin::new(sched.clone()));
    handle.spawn_plugins(Arc::new(registry));
    assert!(handle.start().await, "supervisor starts the role actors");

    // A lease expired: publish it through the hub.
    let ev = PatrolEvent::LeaseExpired {
        bead: "gg-7".into(),
        worker: "polecat-3".into(),
        priority: 1,
    };
    let record = EventRecord {
        event_id: "e1".into(),
        correlation_id: "c1".into(),
        causation_id: None,
        ts: "2026-06-02T12:00:00Z".into(),
        kind: "patrol.lease-expired.v1".into(),
        payload: serde_json::to_value(&ev).unwrap(),
    };
    handle.events_sender().send(record).expect("hub has subscribers");

    // The scheduler observer should re-enqueue the bead — its actor emits SchedEvent::Enqueue
    // (the auto-pump then dispatch-fails it since the bead is not a pending row in the repo,
    // which is fine; the re-enqueue reaction is what we assert). All through the real relay.
    let emitted = tokio::time::timeout(std::time::Duration::from_secs(5), sched_rx.recv())
        .await
        .expect("scheduler reacted before timeout")
        .expect("sched sink open");
    assert!(
        matches!(emitted.payload, SchedEvent::Enqueue { ref bead, priority } if bead == "gg-7" && priority == 1),
        "lease expiry should re-enqueue the freed bead, got {:?}",
        emitted.payload
    );

    // merge.merged frees a slot — just exercise the arm end-to-end (no queued work to pump).
    let merged = EventRecord {
        event_id: "e2".into(),
        correlation_id: "c2".into(),
        causation_id: None,
        ts: "2026-06-02T12:00:01Z".into(),
        kind: "merge.merged.v1".into(),
        payload: serde_json::json!({ "Merged": { "bead": "gg-7", "sha": "abc1234" } }),
    };
    handle.events_sender().send(merged).expect("hub open");

    handle.shutdown().await;
}
