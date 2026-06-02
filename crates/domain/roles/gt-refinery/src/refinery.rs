//! Producer helpers for the Refinery role. The actual `MERGE_READY` channel watcher
//! still lives in `gt_merge::refinery` (it has its own gate test and is the existing edge
//! into the merge actor). This module exposes the handle-side helpers so a future root
//! reaction that observes `MergeEvent::Ready` / `MergeEvent::Started` can record the
//! corresponding refinery-side observations without constructing `RefineryCommand`s by
//! hand. Same shape as `gt_sheriff::sheriff::observe` / `gt_deacon::deacon::*`.

use gt_events::AppError;

use crate::actor::RefineryHandle;
use crate::commands::{MarkDispatched, ObserveReady, RefineryCommand};

/// Record an observed `MERGE_READY`. Called by the root edge after the channel watcher
/// (in `gt_merge::refinery`) emits `MergeEvent::Ready`, to populate the refinery projection.
pub async fn observe(
    handle: &RefineryHandle,
    bead: &str,
    branch: &str,
    channel_msg_id: &str,
    at_epoch_secs: u64,
) -> Result<(), AppError> {
    handle
        .exec(RefineryCommand::Observe(ObserveReady {
            bead: bead.to_string(),
            branch: branch.to_string(),
            channel_msg_id: channel_msg_id.to_string(),
            at: at_epoch_secs,
        }))
        .await
}

/// Flip the entry's `dispatched` flag. Called when the matching merge slot leaves
/// `Ready` (`MergeEvent::Started`).
pub async fn mark_dispatched(handle: &RefineryHandle, bead: &str) -> Result<(), AppError> {
    handle
        .exec(RefineryCommand::MarkDispatched(MarkDispatched {
            bead: bead.to_string(),
        }))
        .await
}
