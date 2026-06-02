//! Owned `Command` structs over `WitnessState`. Same shape as `SheriffCommand`:
//! `validate` inspects without mutating, `execute` re-validates and returns the events the
//! actor must apply + emit in the same tick. Keeping the produced events as the command
//! output preserves the emit-on-apply invariant: only legal transitions reach the log, so
//! replay stays deterministic.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::WitnessEvent;
use crate::state::WitnessState;

/// Register a new worker to watch. Each `worker` id is unique; re-registering an existing
/// id is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WatchTarget {
    /// Stable identifier for the worker (typically the polecat / session id).
    pub worker: String,
    /// Registration timestamp the producer recorded at watch time (wall-clock seconds).
    pub since_secs: u64,
    /// Seconds of inactivity before the next tick raises an escalation. Must be `>= 1`.
    pub threshold_secs: u64,
}

/// Observe one tick against `worker`. The producer (an edge timer task or a cross-domain
/// reaction) supplies `now_secs`; the actor compares it against the recorded `since_secs`
/// and decides whether to advance `last_seen_secs` and raise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TickTarget {
    pub worker: String,
    pub now_secs: u64,
}

/// Clear an escalated target and reset its `last_seen_secs` so the next cycle starts
/// fresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClearTarget {
    pub worker: String,
}

/// The set of mutations the witness actor accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum WitnessCommand {
    Watch(WatchTarget),
    Tick(TickTarget),
    Clear(ClearTarget),
}

impl WitnessCommand {
    /// Inspect `state` and return `Ok` iff the command would apply cleanly.
    pub fn validate(&self, state: &WitnessState) -> Result<(), AppError> {
        match self {
            WitnessCommand::Watch(w) => {
                if w.worker.is_empty() {
                    return Err(AppError::Validation("watch worker empty".into()));
                }
                if w.threshold_secs == 0 {
                    return Err(AppError::Validation(
                        "watch threshold_secs must be >= 1".into(),
                    ));
                }
                if state.targets.contains_key(&w.worker) {
                    return Err(AppError::Validation(format!(
                        "worker {} already watched",
                        w.worker
                    )));
                }
                Ok(())
            }
            WitnessCommand::Tick(t) => {
                let target = state.targets.get(&t.worker).ok_or_else(|| {
                    AppError::NotFound(format!("worker {} not watched", t.worker))
                })?;
                if t.now_secs < target.since_secs {
                    return Err(AppError::Validation(format!(
                        "tick now_secs={} precedes since_secs={} for worker {}",
                        t.now_secs, target.since_secs, t.worker
                    )));
                }
                Ok(())
            }
            WitnessCommand::Clear(c) => {
                let target = state.targets.get(&c.worker).ok_or_else(|| {
                    AppError::NotFound(format!("worker {} not watched", c.worker))
                })?;
                if !target.escalated {
                    return Err(AppError::Validation(format!(
                        "worker {} not escalated",
                        c.worker
                    )));
                }
                Ok(())
            }
        }
    }

    /// Re-validate and produce the events the actor must apply + emit. `Tick` may produce
    /// zero events (already escalated), one (`TargetStuck` below threshold) or two
    /// (`TargetStuck` + `EscalationRaised` when the bump crosses the threshold).
    pub fn execute(&self, state: &WitnessState) -> Result<Vec<WitnessEvent>, AppError> {
        self.validate(state)?;
        match self {
            WitnessCommand::Watch(w) => Ok(vec![WitnessEvent::TargetWatched {
                worker: w.worker.clone(),
                since_secs: w.since_secs,
                threshold_secs: w.threshold_secs,
            }]),
            WitnessCommand::Tick(t) => {
                let target = state
                    .targets
                    .get(&t.worker)
                    .expect("validate ensures target exists");
                if target.escalated {
                    return Ok(Vec::new());
                }
                let age = t.now_secs.saturating_sub(target.since_secs);
                let mut out = vec![WitnessEvent::TargetStuck {
                    worker: t.worker.clone(),
                    last_seen_secs: t.now_secs,
                    age_secs: age,
                }];
                if age >= target.threshold_secs {
                    out.push(WitnessEvent::EscalationRaised {
                        worker: t.worker.clone(),
                        age_secs: age,
                    });
                }
                Ok(out)
            }
            WitnessCommand::Clear(c) => Ok(vec![WitnessEvent::TargetCleared {
                worker: c.worker.clone(),
            }]),
        }
    }
}
