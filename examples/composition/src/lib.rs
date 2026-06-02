//! Composition-root PROOF (`hq-mod-refactor.21`): the cross-domain reactor — function #4 of
//! the composition root — expressed as **observer plugins** (approach A), the modular
//! replacement for gastown's monolithic `Reactor::react(GtEvent)` match.
//!
//! gt-core has no single `GtEvent` enum to match on (one event-kind namespace per module), so
//! a cross-domain reaction is just another `gt_plugin::Plugin` on the per-workspace event hub,
//! exactly like the role observers (`gt_roles::DeaconPlugin` / `WitnessPlugin`). This crate
//! adds the **scheduler** arm — the one the gastown reactor expressed as
//! `lease_expired -> sched.enqueue` and `merge_merged -> sched.capacity_freed` — and an
//! [`scheduler_plugin`] helper to fold it onto a role stack's registry.
//!
//! It lives under `examples/` (the only tier the dep table lets name `gt-scheduling` +
//! `gt-patrol` + `gt-merge` together) so the production composition-root home stays the open
//! decision tracked by `hq-mod-refactor.13` — this is a working proof, not that home.

use async_trait::async_trait;

use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_patrol::PatrolEvent;
use gt_plugin::Plugin;
use gt_scheduling::actor::SchedHandle;

/// Observer that drives the scheduler from the events other domains broadcast — the
/// cross-domain reactor arm, as a plugin:
///
/// - `patrol.lease-expired.v1` → re-enqueue the freed bead at its priority
///   (`SchedHandle::enqueue`). The lease *release* in the bead repo is a separate concern
///   (a bead-repo effect), not the scheduler's.
/// - `merge.merged.v1` → release the dispatch slot (`SchedHandle::capacity_freed`) so the
///   pump can dispatch the next pending bead.
///
/// Every other kind is ignored. The scheduler actor owns its queue/governor state and emits
/// its own `SchedEvent`s, so this stays a read-only observer of *other* domains' events —
/// the same carve-out as the role plugins.
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
