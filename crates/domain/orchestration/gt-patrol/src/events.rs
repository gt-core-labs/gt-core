use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Patrol domain events. `Serialize`/`Deserialize` for the audit log + replay.
///
/// `LeaseRegistered`/`Heartbeat`/`LeaseClosed` are the *inputs* observed at the edge and
/// recorded so replay can rebuild the tracker state. `LeaseExpired` is the *output* the
/// scheduling domain reacts to (via the composition root) when the patrol's pure detector
/// fires against a tick. All clock-derived numbers (`now_secs`, ages) travel as data in
/// the events themselves — the core never reads the wall clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatrolEvent {
    /// A bead just transitioned to `dispatched`: start tracking its lease.
    LeaseRegistered {
        bead: String,
        worker: String,
        priority: u8,
        now_secs: u64,
    },
    /// Polecat reported it's still alive.
    Heartbeat {
        worker: String,
        now_secs: u64,
    },
    /// Lease closed by completion/failure (not by patrol): stop tracking.
    LeaseClosed {
        bead: String,
    },
    /// Pure detector found a lease whose worker has not reported in time.
    /// The composition root reacts: `repo.cas_release(bead, worker)` then re-enqueue.
    LeaseExpired {
        bead: String,
        worker: String,
        priority: u8,
    },
}

impl EventKind for PatrolEvent {
    fn kind(&self) -> &'static str {
        match self {
            // Versioned kebab leaf kinds on port-up (gt-core convention; gastown emitted the
            // unversioned `patrol.lease_registered` etc., same .v1 backfill events.2 applied to
            // rig/quota/merge — safe here since gt-core has no patrol replay log yet).
            PatrolEvent::LeaseRegistered { .. } => "patrol.lease-registered.v1",
            PatrolEvent::Heartbeat { .. } => "patrol.heartbeat.v1",
            PatrolEvent::LeaseClosed { .. } => "patrol.lease-closed.v1",
            PatrolEvent::LeaseExpired { .. } => "patrol.lease-expired.v1",
        }
    }
}
