use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Eventos del dominio scheduling. `Serialize`/`Deserialize` para el log + replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedEvent {
    Enqueue { bead: String, priority: u8 },
    Dispatched { bead: String, worker: String },
    DispatchFailed { bead: String, reason: String },
    DispatchTimeout { bead: String },
}

impl EventKind for SchedEvent {
    fn kind(&self) -> &'static str {
        match self {
            // Versioned kebab leaf kinds on port-up (gt-core convention; the upstream app emitted the
            // unversioned `scheduling.enqueue` etc. — same .v1 backfill events.2 applied to
            // rig/quota/merge, safe since gt-core has no scheduling replay log yet).
            SchedEvent::Enqueue { .. } => "scheduling.enqueue.v1",
            SchedEvent::Dispatched { .. } => "scheduling.dispatched.v1",
            SchedEvent::DispatchFailed { .. } => "scheduling.dispatch-failed.v1",
            SchedEvent::DispatchTimeout { .. } => "scheduling.dispatch-timeout.v1",
        }
    }
}
