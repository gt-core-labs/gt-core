//! Domain events of the Witness role.
//!
//! Wire vocabulary is `witness.*` so `bins/gt/src/event.rs::GtEvent::from_record` routes
//! each record back into the typed variant by domain prefix (the existing convention).

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Events emitted by the witness actor. Witness is an escalation observer: `TargetWatched`
/// opens a watch, `TargetStuck` records each tick that finds the worker still pending,
/// `EscalationRaised` fires once the threshold is crossed, and `TargetCleared` resets the
/// flag after operator acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WitnessEvent {
    /// A new worker entered the registry. `threshold_secs > 0`; `since_secs` is the
    /// registration timestamp the producer recorded at watch time.
    TargetWatched {
        worker: String,
        since_secs: u64,
        threshold_secs: u64,
    },
    /// One `Tick` observation found `worker` still pending. `last_seen_secs` is the tick's
    /// `now_secs`; `age_secs = last_seen_secs - since_secs`. Emitted on every tick of a
    /// non-escalated target — once escalated, further ticks are suppressed so the log does
    /// not flood (the operator must `Clear` first).
    TargetStuck {
        worker: String,
        last_seen_secs: u64,
        age_secs: u64,
    },
    /// Threshold crossed: `age_secs >= threshold_secs` on this tick. Operators clear the
    /// flag with `WitnessCommand::Clear`.
    EscalationRaised { worker: String, age_secs: u64 },
    /// Operator-initiated reset: clears the escalated flag and resets `last_seen_secs` so
    /// the next observation cycle starts fresh on the same registration.
    TargetCleared { worker: String },
}

impl EventKind for WitnessEvent {
    fn kind(&self) -> &'static str {
        match self {
            WitnessEvent::TargetWatched { .. } => "witness.target_watched",
            WitnessEvent::TargetStuck { .. } => "witness.target_stuck",
            WitnessEvent::EscalationRaised { .. } => "witness.escalation_raised",
            WitnessEvent::TargetCleared { .. } => "witness.target_cleared",
        }
    }
}
