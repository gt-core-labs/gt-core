//! Domain events of the Mayor role.
//!
//! Wire vocabulary is `mayor.*` so `bins/gt/src/event.rs::GtEvent::from_record` routes
//! each record back into the typed variant by domain prefix (the existing convention).

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Events emitted by the mayor actor. The four variants are the discrete transitions a
/// `Delegation` goes through; the actor always applies its own emit to the local
/// `MayorState` before publishing so the in-process board agrees with the log
/// (emit-on-apply invariant, mirroring `gt-merge` / `gt-sheriff`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MayorEvent {
    /// A new delegation entered the board in `Pending` status.
    Delegated {
        id: String,
        target: String,
        summary: String,
    },
    /// The target picked up the delegation: `Pending → Acknowledged`.
    Acknowledged { id: String },
    /// The delegation finished with `outcome` (free-form, e.g. `"done"`, `"failed: X"`):
    /// `Acknowledged → Resolved`.
    Resolved { id: String, outcome: String },
    /// Operator-initiated cancel before the target acknowledged: `Pending → Withdrawn`.
    Withdrawn { id: String },
}

impl EventKind for MayorEvent {
    fn kind(&self) -> &'static str {
        match self {
            MayorEvent::Delegated { .. } => "mayor.delegated",
            MayorEvent::Acknowledged { .. } => "mayor.acknowledged",
            MayorEvent::Resolved { .. } => "mayor.resolved",
            MayorEvent::Withdrawn { .. } => "mayor.withdrawn",
        }
    }
}
