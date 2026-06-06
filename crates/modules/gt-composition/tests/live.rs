//! Capstone proof (`hq-mod-refactor.25`): a registry-hosted, owned, drained per-workspace
//! composition root round-trips a reaction through the event hub.
//!
//! Publishing `patrol.lease-expired.v1` onto the hub drives the scheduler observer, whose actor
//! enqueues the bead and emits `scheduling.enqueue.v1`; the drain forwards that back onto the hub
//! where the probe records it — proving the full loop hub → observer → actor → drain → hub, with
//! the actors owned by the cached `RootHandle` (anchored) and resolved via `get_or_hydrate_async`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gt_composition::mcp::eventlog::EventLog;
use gt_composition::{daemon_root, live_root, replay_merge_board, replay_scheduling_pending};
use gt_eventlog::{EventRecord, EventStore, JsonlWriter};
use gt_merge::{MergeEvent, MergeSlotState};
use gt_patrol::PatrolEvent;
use gt_quota::QuotaEvent;
use gt_runtime::RootRegistry;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;

#[tokio::test]
async fn live_root_round_trips_a_reaction_through_the_hub_and_caches() {
    let reg = RootRegistry::new();
    let probe = Arc::new(Mutex::new(Vec::new()));
    let h = live_root(
        &reg,
        WorkspaceId::new("acme").expect("valid slug"),
        None,
        probe.clone(),
    )
    .await;

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
        None,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    assert!(Arc::ptr_eq(&h, &again), "second resolve returns the cached root");
    assert_eq!(reg.len(), 1);

    h.shutdown().await;
}

/// `hq-orchd.5`: boot hydration replays the durable workspace log back into the pending scheduler
/// queue + the in-flight merge board, so a daemon restart resumes still-open work. Drives the two
/// replay helpers `live_root` calls on hydration directly (the cached handle does not expose the
/// domain handles), over a hand-seeded log: a dispatched bead must NOT come back as pending, an
/// open merge slot must be restored at its last state.
#[test]
fn boot_hydration_replays_pending_queue_and_merge_board() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = EventLog::new(Some(dir.path().to_path_buf()));

    // scheduling: g1 enqueued then dispatched (gone), g2 + g3 still pending.
    log.append(Some("acme"), SchedEvent::Enqueue { bead: "g1".into(), priority: 0 })
        .unwrap();
    log.append(Some("acme"), SchedEvent::Enqueue { bead: "g2".into(), priority: 2 })
        .unwrap();
    log.append(
        Some("acme"),
        SchedEvent::Dispatched { bead: "g1".into(), worker: "w1".into() },
    )
    .unwrap();
    log.append(Some("acme"), SchedEvent::Enqueue { bead: "g3".into(), priority: 1 })
        .unwrap();

    let pending = replay_scheduling_pending(&log, "acme").expect("replay scheduling");
    assert_eq!(
        pending,
        vec![("g2".to_string(), 2u8), ("g3".to_string(), 1u8)],
        "dispatched bead dropped, pending beads restored with priority"
    );

    // merge: m1 ready, m2 ready→merging — both open slots, m2 at Merging.
    log.append(
        Some("acme"),
        MergeEvent::Ready { bead: "m1".into(), branch: "b1".into(), channel_msg_id: "x1".into() },
    )
    .unwrap();
    log.append(
        Some("acme"),
        MergeEvent::Ready { bead: "m2".into(), branch: "b2".into(), channel_msg_id: "x2".into() },
    )
    .unwrap();
    log.append(Some("acme"), MergeEvent::Started { bead: "m2".into() })
        .unwrap();

    let board = replay_merge_board(&log, "acme").expect("replay merge");
    assert_eq!(board.len(), 2, "both merge slots restored");
    assert_eq!(board.get("m1").expect("m1 slot").state, MergeSlotState::Ready);
    assert_eq!(board.get("m2").expect("m2 slot").state, MergeSlotState::Merging);

    // Isolation: a different workspace's log is empty (path-partitioned per tenant).
    assert!(replay_scheduling_pending(&log, "other").unwrap().is_empty());
}

/// `hq-orchd.2`: a `live_root` with a `log_root` persists every hub record to the workspace's
/// durable event log. Both the injected trigger and the scheduler's reaction must land on disk,
/// so a restart can rebuild the in-memory projections by replaying the log (boot hydration,
/// `hq-orchd.5`). This is the durability the upstream-model "Dolt-backed repos" bead asked for,
/// realized over gt-core's event-sourced substrate.
#[tokio::test]
async fn live_root_persists_hub_records_to_the_durable_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let reg = RootRegistry::new();
    let probe = Arc::new(Mutex::new(Vec::new()));
    let h = live_root(
        &reg,
        WorkspaceId::new("acme").expect("valid slug"),
        Some(dir.path().to_path_buf()),
        probe.clone(),
    )
    .await;

    let le = PatrolEvent::LeaseExpired {
        bead: "gg-9".into(),
        worker: "polecat-1".into(),
        priority: 2,
    };
    h.events_sender()
        .send(EventRecord {
            event_id: "e9".into(),
            correlation_id: "c9".into(),
            causation_id: None,
            ts: "2026-06-03T12:00:00Z".into(),
            kind: "patrol.lease-expired.v1".into(),
            payload: serde_json::to_value(&le).unwrap(),
        })
        .expect("hub has subscribers");

    // Read the workspace's durable partition until the scheduler's reaction has been persisted
    // (which also proves the trigger reached disk, since the persistence plugin runs first).
    let log = JsonlWriter::for_workspace_in(dir.path(), "acme").expect("open log partition");
    let mut kinds: Vec<String> = Vec::new();
    for _ in 0..200 {
        kinds = log
            .read_all()
            .expect("read log")
            .into_iter()
            .map(|r| r.kind)
            .collect();
        if kinds.iter().any(|k| k == "scheduling.enqueue.v1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        kinds.iter().any(|k| k == "patrol.lease-expired.v1"),
        "the trigger event must be persisted; log saw {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "scheduling.enqueue.v1"),
        "the scheduler reaction must be persisted; log saw {kinds:?}"
    );

    h.shutdown().await;
}

/// `hq-orchd.4`: the daemon root wires the patrol actor into the hub. Registering a lease and
/// ticking past its timeout emits `patrol.lease-expired.v1`, which the scheduler reactor arm turns
/// into a re-enqueue — both events land durably in the workspace log, proving patrol → hub →
/// scheduler round-trips through `daemon_root` (and that its actors are anchored + drained).
#[tokio::test]
async fn daemon_root_patrol_expiry_re_enqueues_through_the_hub() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = daemon_root(
        WorkspaceId::new("acme").expect("valid slug"),
        dir.path().to_path_buf(),
    )
    .await;

    // Register a lease at t=1000, then tick at t=1600 with a 300s timeout → the lease has expired.
    root.patrol.register("gg-7", "polecat-1", 1, 1000).await;
    root.patrol.tick(1600, 300).await;

    let log = JsonlWriter::for_workspace_in(dir.path(), "acme").expect("open log partition");
    let mut kinds: Vec<String> = Vec::new();
    for _ in 0..200 {
        kinds = log.read_all().expect("read log").into_iter().map(|r| r.kind).collect();
        if kinds.iter().any(|k| k == "scheduling.enqueue.v1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        kinds.iter().any(|k| k == "patrol.lease-expired.v1"),
        "lease expiry must be persisted; log saw {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "scheduling.enqueue.v1"),
        "scheduler must re-enqueue the freed bead via the hub; log saw {kinds:?}"
    );

    root.handle.shutdown().await;
}

/// hq-quota-accounts.2: an `AccountRegistered` already in the durable log is rehydrated into the
/// quota actor at boot, so a live-onboarded claude account survives a daemon restart without the
/// `GT_CLAUDE_ACCOUNTS` env — the rotation pool is durable, not env-seeded.
#[tokio::test]
async fn daemon_root_hydrates_registered_accounts_from_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Seed the log as if an account had been onboarded in a prior run.
    let log = EventLog::new(Some(dir.path().to_path_buf()));
    log.append(
        Some("acme"),
        QuotaEvent::AccountRegistered {
            account: "acctX".into(),
            config_dir: "/vol/accounts/acctX".into(),
            now_secs: 1000,
        },
    )
    .expect("append AccountRegistered");

    let root = daemon_root(
        WorkspaceId::new("acme").expect("valid slug"),
        dir.path().to_path_buf(),
    )
    .await;

    // The replayed account is a live rotation candidate in the hydrated quota actor.
    let accounts = root.quota.accounts().await;
    assert!(
        accounts.iter().any(|a| a.id == "acctX"),
        "registered account must hydrate into the quota registry; saw {:?}",
        accounts.iter().map(|a| &a.id).collect::<Vec<_>>()
    );

    root.handle.shutdown().await;
}
