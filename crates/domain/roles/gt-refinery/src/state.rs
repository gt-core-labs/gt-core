//! State of the Refinery domain: a projection over the merge-ready channel observations
//! that gives operators a "what is waiting to be merged" view independent of the
//! gt-merge slot board. Each entry tracks one observed `MERGE_READY` until the merge
//! actor acknowledges progress (`MarkDispatched`).
//!
//! Coexists with `gt_merge::refinery` (the existing channel→merge producer): that crate
//! still owns the channel watcher and emits `MergeEvent::Ready`. This crate is the
//! parallel observer that records the refinery-side lifecycle for diagnostics + replay.

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::RefineryEvent;

/// One observed `MERGE_READY`. `dispatched` flips when the matching merge slot moves out
/// of `Ready`; until then the entry is visible in the pending view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefineryEntry {
    pub bead: String,
    pub branch: String,
    pub channel_msg_id: String,
    pub observed_at: u64,
    pub dispatched: bool,
}

/// Aggregate state of the refinery domain.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RefineryState {
    pub entries: BTreeMap<String, RefineryEntry>,
}

impl RefineryState {
    /// Pure fold over a `RefineryEvent`. **Total** — `MarkDispatched` for an unknown bead
    /// no-ops so replay over a partial log still terminates cleanly.
    pub fn apply(&mut self, ev: &RefineryEvent) -> Result<(), AppError> {
        match ev {
            RefineryEvent::Observed {
                bead,
                branch,
                channel_msg_id,
                at,
            } => {
                self.entries.insert(
                    bead.clone(),
                    RefineryEntry {
                        bead: bead.clone(),
                        branch: branch.clone(),
                        channel_msg_id: channel_msg_id.clone(),
                        observed_at: *at,
                        dispatched: false,
                    },
                );
            }
            RefineryEvent::Dispatched { bead } => {
                if let Some(e) = self.entries.get_mut(bead) {
                    e.dispatched = true;
                }
            }
        }
        Ok(())
    }
}
