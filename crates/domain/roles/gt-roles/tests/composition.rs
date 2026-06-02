//! End-to-end composition test (`hq-mod-refactor.20`): assemble a real per-workspace
//! [`RootHandle`] event hub (`hq-mod-refactor.18`), wire a [`RoleStack`]'s observer plugins
//! onto it (`register_plugins`), then publish an [`EventRecord`] through the hub and assert it
//! reaches a role actor through the **real** `gt_plugin` relay — not a direct `on_event` call.
//!
//! This is the integration the unit tests do not cover: they exercise `plugin.on_event`
//! directly or assert the registry shape; here the full path runs —
//! `events_sender().send()` → broadcast → `spawn_plugin_relay` → `WitnessPlugin` →
//! witness actor → emitted `TargetWatched`.

use tokio::sync::mpsc;

use gt_events::Envelope;
use gt_eventlog::EventRecord;
use gt_module::RootBuilder;
use gt_patrol::PatrolEvent;
use gt_roles::{RoleSinks, RoleStack};
use gt_runtime::RootHandle;
use gt_witness::WitnessEvent;
use gt_workspace::WorkspaceId;

#[tokio::test]
async fn published_event_reaches_a_role_observer_through_the_relay() {
    // A composed (empty) Root bound to a workspace, with its event hub.
    let ws = WorkspaceId::new("acme").expect("valid slug");
    let root = RootBuilder::new().build().expect("empty root builds");
    let handle = RootHandle::new(ws, root);

    // Role actors anchored to the handle's own supervisor; keep the witness sink to observe.
    let (wt, mut witness_rx) = mpsc::channel::<Envelope<WitnessEvent>>(16);
    let (st, _sr) = mpsc::channel(16);
    let (dt, _dr) = mpsc::channel(16);
    let (mt, _mr) = mpsc::channel(16);
    let (rt, _rr) = mpsc::channel(16);
    let sinks = RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt };

    let stack = RoleStack::register(handle.supervisor(), sinks)
        .await
        .expect("registers while Built");
    // Start the anchored actors, then wire the observer plugins onto the hub.
    assert!(handle.start().await, "supervisor starts the role actors");
    stack.register_plugins(&handle);

    // Publish a lease-expiry event through the hub — the witness observer should watch the
    // worker, all the way through the real broadcast relay.
    let ev = PatrolEvent::LeaseExpired {
        bead: "gg-9".into(),
        worker: "polecat-5".into(),
        priority: 2,
    };
    let record = EventRecord {
        event_id: "e1".into(),
        correlation_id: "c1".into(),
        causation_id: None,
        ts: "1970-01-01T00:00:07Z".into(),
        kind: "patrol.lease-expired.v1".into(),
        payload: serde_json::to_value(&ev).unwrap(),
    };
    handle
        .events_sender()
        .send(record)
        .expect("hub has at least the relay subscriber");

    // The relay drains the broadcast on its own task; await the witness emit (bounded).
    let emitted = tokio::time::timeout(std::time::Duration::from_secs(5), witness_rx.recv())
        .await
        .expect("witness reacted before timeout")
        .expect("witness sink open");

    assert!(
        matches!(
            emitted.payload,
            WitnessEvent::TargetWatched { ref worker, since_secs, .. }
                if worker == "polecat-5" && since_secs == 7
        ),
        "lease expiry should reach the witness through the relay, got {:?}",
        emitted.payload
    );

    handle.shutdown().await;
}
