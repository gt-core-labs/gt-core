//! Thin producer helpers for the Witness role. Same shape as the producer surface
//! `gt-deacon` will expose once filled (`begin_drain`/`track`/`finish`): tiny wrappers
//! around [`WitnessHandle::exec`] that name the producer call sites for cross-domain
//! wiring without pulling the caller into the command enum.
//!
//! No timer loop here yet: the composition root currently drives ticks from the operator
//! edge (gt-mcp / CLI). A periodic producer task can be added later — when wired, it would
//! call [`tick`] for every watched worker on every interval.

use gt_events::AppError;

use crate::actor::WitnessHandle;
use crate::commands::{ClearTarget, TickTarget, WatchTarget, WitnessCommand};

/// Register a worker to watch. `since_secs` is the wall-clock timestamp the edge recorded
/// at watch time; `threshold_secs` is the inactivity budget before escalation.
pub async fn watch(
    handle: &WitnessHandle,
    worker: impl Into<String>,
    since_secs: u64,
    threshold_secs: u64,
) -> Result<(), AppError> {
    handle
        .exec(WitnessCommand::Watch(WatchTarget {
            worker: worker.into(),
            since_secs,
            threshold_secs,
        }))
        .await
}

/// Push one tick observation for `worker`. `Ok` covers all three outcomes: target still
/// below threshold (one `TargetStuck` emitted), threshold crossed (`TargetStuck` +
/// `EscalationRaised`), already escalated (no events).
pub async fn tick(
    handle: &WitnessHandle,
    worker: impl Into<String>,
    now_secs: u64,
) -> Result<(), AppError> {
    handle
        .exec(WitnessCommand::Tick(TickTarget {
            worker: worker.into(),
            now_secs,
        }))
        .await
}

/// Clear an escalated target after operator acknowledgement.
pub async fn clear(handle: &WitnessHandle, worker: impl Into<String>) -> Result<(), AppError> {
    handle
        .exec(WitnessCommand::Clear(ClearTarget {
            worker: worker.into(),
        }))
        .await
}
