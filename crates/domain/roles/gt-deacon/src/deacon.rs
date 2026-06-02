//! Producer helpers for the Deacon role. The runtime-level trigger (SIGTERM → BeginDrain)
//! lives at the edge in `bins/gt`; this module keeps the thin handle-side helpers so callers
//! do not have to construct `DeaconCommand` variants by hand. Same shape as
//! `gt_sheriff::sheriff::observe`.
//!
//! Cross-domain wiring (track a `merge.ready` so the deacon waits for it) is also a
//! composition-root concern — those root reactions call `track`/`finish` from here.

use gt_events::AppError;

use crate::actor::DeaconHandle;
use crate::commands::{BeginDrain, DeaconCommand, FinishItem, TrackItem};

/// Trigger a drain. Idempotent at the actor — re-calling while already draining is a no-op
/// in `DeaconCommand::execute` and produces no event.
pub async fn begin_drain(handle: &DeaconHandle, by: &str, at_epoch_secs: u64) -> Result<(), AppError> {
    handle
        .exec(DeaconCommand::BeginDrain(BeginDrain {
            by: by.to_string(),
            at: at_epoch_secs,
        }))
        .await
}

/// Register an in-flight item the deacon must wait for.
pub async fn track(handle: &DeaconHandle, id: &str, kind: &str) -> Result<(), AppError> {
    handle
        .exec(DeaconCommand::Track(TrackItem {
            id: id.to_string(),
            kind: kind.to_string(),
        }))
        .await
}

/// Mark a tracked item complete. If the deacon is draining and the pending set hits zero,
/// the actor follows with a `DrainComplete` event in the same exec tick.
pub async fn finish(handle: &DeaconHandle, id: &str, at_epoch_secs: u64) -> Result<(), AppError> {
    handle
        .exec(DeaconCommand::Finish(FinishItem {
            id: id.to_string(),
            at: at_epoch_secs,
        }))
        .await
}
