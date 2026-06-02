//! State of the Witness domain: a map of registered watch targets keyed by worker id. A
//! target tracks `(since_secs, threshold_secs, last_seen_secs, escalated)` — the live and
//! replay-folded state share one type (small domain, no `Board`/`State` split, same as
//! `SheriffState`).

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::WitnessEvent;

/// A watched worker. `since_secs` is the registration timestamp the producer recorded at
/// `Watch` time; `threshold_secs` is how long the worker may remain unseen before the
/// next `Tick` raises an escalation. `last_seen_secs` advances on every observation,
/// `escalated` flips when the threshold is crossed and only resets via `Clear`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckTarget {
    pub worker: String,
    pub since_secs: u64,
    pub threshold_secs: u64,
    pub last_seen_secs: u64,
    pub escalated: bool,
}

/// Aggregate state of the witness domain. `Default` is the empty registry — pre-`Watched`
/// boot has no targets and ignores every tick.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WitnessState {
    pub targets: BTreeMap<String, StuckTarget>,
}

impl WitnessState {
    /// Pure fold over a `WitnessEvent`. **Total** — unknown workers in `TargetStuck` /
    /// `EscalationRaised` / `TargetCleared` are no-ops, matching the reducer pattern used
    /// by `SheriffState`. Validation lives on the write path (`WitnessCommand::validate`);
    /// the fold trusts the log.
    pub fn apply(&mut self, ev: &WitnessEvent) -> Result<(), AppError> {
        match ev {
            WitnessEvent::TargetWatched {
                worker,
                since_secs,
                threshold_secs,
            } => {
                self.targets.insert(
                    worker.clone(),
                    StuckTarget {
                        worker: worker.clone(),
                        since_secs: *since_secs,
                        threshold_secs: *threshold_secs,
                        last_seen_secs: *since_secs,
                        escalated: false,
                    },
                );
            }
            WitnessEvent::TargetStuck {
                worker,
                last_seen_secs,
                ..
            } => {
                if let Some(t) = self.targets.get_mut(worker) {
                    t.last_seen_secs = *last_seen_secs;
                }
            }
            WitnessEvent::EscalationRaised { worker, .. } => {
                if let Some(t) = self.targets.get_mut(worker) {
                    t.escalated = true;
                }
            }
            WitnessEvent::TargetCleared { worker } => {
                if let Some(t) = self.targets.get_mut(worker) {
                    t.escalated = false;
                    t.last_seen_secs = t.since_secs;
                }
            }
        }
        Ok(())
    }
}
