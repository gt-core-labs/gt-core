//! Composition-root PROOF (`hq-mod-refactor.21`/`.22`): the cross-domain reactor — function #4
//! of the composition root — as **observer plugins** (approach A), plus [`compose_workspace`],
//! a single-function per-workspace assembler (function #9).
//!
//! gt-core has no single `GtEvent` enum to match on (one event-kind namespace per module), so a
//! cross-domain reaction is just another `gt_plugin::Plugin` on the per-workspace event hub,
//! like the role observers (`gt_roles::DeaconPlugin` / `WitnessPlugin`). This crate adds the two
//! orchestration arms the gastown `Reactor::react` expressed and that need no edge effects:
//!
//! - [`SchedulerPlugin`] — `patrol.lease-expired.v1` → re-enqueue; `merge.merged.v1` → free a slot.
//! - [`MergePlugin`] — `merge.ready.v1` → advance the slot to `Merging`.
//!
//! and assembles them with the role observers into one live root via [`compose_workspace`].
//!
//! Lives under `examples/` (the only dep tier that may name `gt-scheduling` + `gt-patrol` +
//! `gt-merge` together) so the production composition-root home stays the open decision tracked
//! by `hq-mod-refactor.13`. This is a working proof, not that home. The edge-effect arms
//! (sling/rotate/`git merge`/lease release) and hydration are deferred to the real root, and
//! `RootRegistry::get_or_hydrate` is synchronous so it cannot host this async assembly yet (see
//! the gap filed with `.22`).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use gt_beads::InMemoryBeads;
use gt_events::{AppError, Envelope};
use gt_eventlog::EventRecord;
use gt_merge::actor::MergeHandle;
use gt_merge::{InMemoryMergeRepo, MergeEvent};
use gt_module::RootBuilder;
use gt_patrol::PatrolEvent;
use gt_plugin::Plugin;
use gt_roles::{RoleSinks, RoleStack};
use gt_runtime::RootHandle;
use gt_scheduling::actor::SchedHandle;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;

/// Observer that drives the scheduler from other domains' events — the scheduler reactor arm:
/// `patrol.lease-expired.v1` → re-enqueue the freed bead (`SchedHandle::enqueue`);
/// `merge.merged.v1` → release the dispatch slot (`SchedHandle::capacity_freed`). The scheduler
/// actor owns its queue/governor and emits its own `SchedEvent`s, so this is a read-only
/// observer of OTHER domains.
pub struct SchedulerPlugin {
    sched: SchedHandle,
}

impl SchedulerPlugin {
    /// Wrap a scheduler command handle as a cross-domain observer.
    pub fn new(sched: SchedHandle) -> Self {
        Self { sched }
    }
}

#[async_trait]
impl Plugin for SchedulerPlugin {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "patrol.lease-expired.v1" => {
                if let PatrolEvent::LeaseExpired { bead, priority, .. } =
                    record.decode::<PatrolEvent>()?
                {
                    self.sched.enqueue(bead, priority).await;
                }
                Ok(())
            }
            "merge.merged.v1" => {
                self.sched.capacity_freed().await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Observer that advances a merge slot when it is observed ready — the merge reactor arm:
/// `merge.ready.v1` → `MergeHandle::start` (`Ready → Merging`). The slot must already exist
/// (submitted) for the transition to take; the merge actor validates that and stays silent
/// otherwise (the error lands in the relay dead-letter, never aborting the chain).
pub struct MergePlugin {
    merge: MergeHandle,
}

impl MergePlugin {
    /// Wrap a merge command handle as a cross-domain observer.
    pub fn new(merge: MergeHandle) -> Self {
        Self { merge }
    }
}

#[async_trait]
impl Plugin for MergePlugin {
    fn name(&self) -> &'static str {
        "merge"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind == "merge.ready.v1" {
            if let MergeEvent::Ready { bead, .. } = record.decode::<MergeEvent>()? {
                self.merge.start(bead).await;
            }
        }
        Ok(())
    }
}

/// A fully assembled, started per-workspace root — the output of [`compose_workspace`]. Holds
/// the live [`RootHandle`] plus the domain handles + event sinks a caller (or a test) inspects.
/// Dropping it drops the role stack (the supervisor cancels the actors) and the hub sender (the
/// plugin relay then exits).
pub struct Composed {
    /// The composed application handle (event hub + supervisor) for the workspace.
    pub handle: RootHandle,
    /// Scheduler command handle.
    pub sched: SchedHandle,
    /// Merge command handle.
    pub merge: MergeHandle,
    /// Scheduler event stream (for inspection).
    pub sched_events: mpsc::Receiver<Envelope<SchedEvent>>,
    /// Merge event stream (for inspection).
    pub merge_events: mpsc::Receiver<Envelope<MergeEvent>>,
    /// The role-actor stack, kept alive for the root's lifetime.
    pub roles: RoleStack,
}

/// Assemble and start the per-workspace composition root: build the [`RootHandle`] (event hub),
/// spawn the scheduler + merge actors, register the role observers + the scheduler + merge
/// reactor arms onto the hub, and start the actor stack. One function = the whole
/// composition-root assembly (function #9), minus the edge effects + hydration the production
/// root adds.
///
/// Must run inside a Tokio runtime. Role event sinks are parked (the demo asserts through the
/// scheduler/merge sinks); the production root would drain every actor's events into the hub +
/// the event log.
pub async fn compose_workspace(ws: WorkspaceId) -> Composed {
    let handle = RootHandle::new(ws, RootBuilder::new().build().expect("empty root builds"));

    let (sched_tx, sched_events) = mpsc::channel(64);
    let sched = gt_scheduling::actor::spawn(InMemoryBeads::default(), sched_tx, 4);

    let (merge_tx, merge_events) = mpsc::channel(64);
    let merge = gt_merge::actor::spawn(InMemoryMergeRepo::default(), merge_tx);

    let (st, _sr) = mpsc::channel(16);
    let (wt, _wr) = mpsc::channel(16);
    let (dt, _dr) = mpsc::channel(16);
    let (mt, _mr) = mpsc::channel(16);
    let (rt, _rr) = mpsc::channel(16);
    let roles = RoleStack::register(
        handle.supervisor(),
        RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt },
    )
    .await
    .expect("role stack registers while the supervisor is Built");

    // The full observer registry: role observers (sheriff/deacon/witness) + the two
    // orchestration reactor arms, fanned out in registration order by the relay.
    let registry = roles
        .plugin_registry()
        .register(SchedulerPlugin::new(sched.clone()))
        .register(MergePlugin::new(merge.clone()));
    handle.spawn_plugins(Arc::new(registry));

    handle.start().await;

    Composed { handle, sched, merge, sched_events, merge_events, roles }
}
