//! Domain events of the Deacon role.
//!
//! Wire vocabulary is `deacon.*` so `bins/gt/src/event.rs::GtEvent::from_record` routes
//! each record back into the typed variant by domain prefix.

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Events emitted by the deacon actor. `DrainRequested` flips the global drain flag;
/// `ItemBegun`/`ItemFinished` track the pending set; `DrainComplete` is the terminal
/// observation (`draining && pending.is_empty()`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeaconEvent {
    /// Drain initiated. `by` is the operator/actor identity that asked; `at` is the
    /// edge-stamped epoch seconds (recorded so replay does not depend on a clock).
    DrainRequested { by: String, at: u64 },
    /// A new unit of work entered the pending set. `kind` is the source domain
    /// (`"merge"`, `"orch"`, …) so an operator panel can show what is outstanding.
    ItemBegun { id: String, kind: String },
    /// A previously-begun item completed and leaves the pending set. Unknown ids no-op
    /// in the fold so replay over a partial log still terminates cleanly.
    ItemFinished { id: String },
    /// Pending set hit zero while drain was active. Terminal; emitted at most once per
    /// `DrainRequested` cycle. Operators wait on this before tearing the runtime down.
    DrainComplete { at: u64 },
}

impl EventKind for DeaconEvent {
    fn kind(&self) -> &'static str {
        match self {
            DeaconEvent::DrainRequested { .. } => "deacon.drain_requested",
            DeaconEvent::ItemBegun { .. } => "deacon.item_begun",
            DeaconEvent::ItemFinished { .. } => "deacon.item_finished",
            DeaconEvent::DrainComplete { .. } => "deacon.drain_complete",
        }
    }
}
