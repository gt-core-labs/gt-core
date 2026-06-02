//! Domain events of the Sheriff role.
//!
//! Wire vocabulary is `sheriff.*` so `bins/gt/src/event.rs::GtEvent::from_record` routes
//! each record back into the typed variant by domain prefix (the existing convention).

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Events emitted by the sheriff actor. `Registered`/`Observed`/`Raised`/`Cleared` are the
/// four discrete transitions a watch goes through; the actor always applies its own emit to
/// the local `SheriffState` before publishing so the in-process board agrees with the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SheriffEvent {
    /// A new watch entered the registry. `threshold > 0`; the counter starts at 0.
    Registered {
        id: String,
        pattern: String,
        threshold: u32,
    },
    /// A watch's counter advanced to `count` (always strictly greater than the previous
    /// value). The producer fans out `Observe` for every event kind on the bus; the actor
    /// emits `Observed` only for watches whose `pattern` matches and that are not raised.
    Observed { id: String, count: u32 },
    /// The threshold was crossed: `at_count >= threshold` for the watch's current threshold.
    /// Operators clear this with `SheriffCommand::Clear`. While raised, further observations
    /// are suppressed (no event traffic) so the log does not flood.
    Raised {
        id: String,
        pattern: String,
        at_count: u32,
    },
    /// Operator-initiated reset: clears the raised flag and resets the counter to 0 so the
    /// next observation cycle starts fresh on the same registration.
    Cleared { id: String },
}

impl EventKind for SheriffEvent {
    fn kind(&self) -> &'static str {
        match self {
            SheriffEvent::Registered { .. } => "sheriff.registered",
            SheriffEvent::Observed { .. } => "sheriff.observed",
            SheriffEvent::Raised { .. } => "sheriff.raised",
            SheriffEvent::Cleared { .. } => "sheriff.cleared",
        }
    }
}
