//! Capstone proof (`hq-mod-refactor.25`): a registry-hosted, owned, drained per-workspace
//! composition root round-trips a reaction through the event hub.
//!
//! Publishing `patrol.lease-expired.v1` onto the hub drives the scheduler observer, whose actor
//! enqueues the bead and emits `scheduling.enqueue.v1`; the drain forwards that back onto the hub
//! where the probe records it — proving the full loop hub → observer → actor → drain → hub, with
//! the actors owned by the cached `RootHandle` (anchored) and resolved via `get_or_hydrate_async`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gt_composition::live_root;
use gt_eventlog::EventRecord;
use gt_patrol::PatrolEvent;
use gt_runtime::RootRegistry;
use gt_workspace::WorkspaceId;

#[tokio::test]
async fn live_root_round_trips_a_reaction_through_the_hub_and_caches() {
    let reg = RootRegistry::new();
    let probe = Arc::new(Mutex::new(Vec::new()));
    let h = live_root(&reg, WorkspaceId::new("acme").expect("valid slug"), probe.clone()).await;

    // Publish a lease expiry onto the hub. Expected loop: hub -> SchedulerPlugin -> scheduler
    // actor enqueues -> emits SchedEvent::Enqueue -> drain_events_from -> hub -> ProbePlugin.
    let le = PatrolEvent::LeaseExpired {
        bead: "gg-7".into(),
        worker: "polecat-3".into(),
        priority: 1,
    };
    h.events_sender()
        .send(EventRecord {
            event_id: "e1".into(),
            correlation_id: "c1".into(),
            causation_id: None,
            ts: "2026-06-02T12:00:00Z".into(),
            kind: "patrol.lease-expired.v1".into(),
            payload: serde_json::to_value(&le).unwrap(),
        })
        .expect("hub has subscribers");

    let mut looped = false;
    for _ in 0..200 {
        if probe
            .lock()
            .unwrap()
            .iter()
            .any(|k| k == "scheduling.enqueue.v1")
        {
            looped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        looped,
        "the scheduler reaction must loop back onto the hub; probe saw {:?}",
        probe.lock().unwrap()
    );

    // Registry caches: a second resolve returns the same handle without re-hydrating.
    let again = live_root(
        &reg,
        WorkspaceId::new("acme").expect("valid slug"),
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    assert!(Arc::ptr_eq(&h, &again), "second resolve returns the cached root");
    assert_eq!(reg.len(), 1);

    h.shutdown().await;
}
