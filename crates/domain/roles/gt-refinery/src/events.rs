//! Domain events of the Refinery role. Wire vocabulary is `refinery.*`.

use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Refinery observations. `Observed` records that a `MERGE_READY` arrived and was
/// projected; `Dispatched` marks the matching merge slot left `Ready`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefineryEvent {
    Observed {
        bead: String,
        branch: String,
        channel_msg_id: String,
        /// Edge-stamped epoch seconds — recorded so replay does not depend on a clock.
        at: u64,
    },
    Dispatched {
        bead: String,
    },
}

impl EventKind for RefineryEvent {
    fn kind(&self) -> &'static str {
        match self {
            RefineryEvent::Observed { .. } => "refinery.observed",
            RefineryEvent::Dispatched { .. } => "refinery.dispatched",
        }
    }
}
