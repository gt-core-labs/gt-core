//! State of the Deacon domain: drain coordination for graceful town shutdown.
//!
//! Tracks an in-flight set of `DrainItem`s the operator (or the SIGTERM-driven producer)
//! wants the town to finish before exiting. When a `BeginDrain` is observed the actor flips
//! to `draining`; each `TrackItem`/`FinishItem` mutation updates the pending set; once
//! `draining && pending.is_empty()` a `DrainComplete` fires. Reducer + actor share the type.

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::DeaconEvent;

/// An in-flight unit the deacon is waiting on. `kind` is the originating domain
/// (`"merge"`, `"orch"`, `"sched"`, etc.) so a panel can show what is still pending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainItem {
    pub id: String,
    pub kind: String,
}

/// Aggregate state of the deacon domain. `draining = false` is the steady-state ("the
/// town is up"). `pending` is the open set the deacon is waiting on, empty when nothing
/// needs to finish before exit.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeaconState {
    pub draining: bool,
    pub completed: bool,
    pub pending: BTreeMap<String, DrainItem>,
}

impl DeaconState {
    /// Pure fold over a `DeaconEvent`. **Total** — re-entering `DrainRequested` while
    /// already draining is a no-op (idempotent); `FinishItem` for an unknown id is a no-op.
    /// Validation lives on the write path (`DeaconCommand::validate`).
    pub fn apply(&mut self, ev: &DeaconEvent) -> Result<(), AppError> {
        match ev {
            DeaconEvent::DrainRequested { .. } => {
                self.draining = true;
                self.completed = false;
            }
            DeaconEvent::ItemBegun { id, kind } => {
                self.pending.insert(
                    id.clone(),
                    DrainItem {
                        id: id.clone(),
                        kind: kind.clone(),
                    },
                );
            }
            DeaconEvent::ItemFinished { id } => {
                self.pending.remove(id);
            }
            DeaconEvent::DrainComplete { .. } => {
                self.completed = true;
            }
        }
        Ok(())
    }
}
