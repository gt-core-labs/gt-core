//! Owned `Command` structs over `SheriffState`. Same shape as the gt-merge commands:
//! `validate` inspects without mutating, `execute` re-validates and returns the events the
//! actor must apply + emit in the same tick. Keeping the produced events as the command
//! output preserves the emit-on-apply invariant: only legal transitions reach the log, so
//! replay stays deterministic.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::SheriffEvent;
use crate::state::SheriffState;

/// Register a new watch. Each `id` is unique; re-registering an existing id is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterWatch {
    /// Stable identifier for this watch (operator-facing, used to clear).
    pub id: String,
    /// Event kind string to count (matches `EventKind::kind()`, e.g. `"merge.failed"`).
    pub pattern: String,
    /// Number of observations that raise the alarm. Must be `>= 1`.
    pub threshold: u32,
}

/// Observe one occurrence of `pattern`. Producer fires this for every event seen on the
/// bus; the actor advances every matching, non-raised watch by one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ObserveWatch {
    pub pattern: String,
}

/// Clear a raised watch and reset its counter so the next cycle starts fresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClearWatch {
    pub id: String,
}

/// The set of mutations the sheriff actor accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SheriffCommand {
    Register(RegisterWatch),
    Observe(ObserveWatch),
    Clear(ClearWatch),
}

impl SheriffCommand {
    /// Inspect `state` and return `Ok` iff the command would apply cleanly.
    pub fn validate(&self, state: &SheriffState) -> Result<(), AppError> {
        match self {
            SheriffCommand::Register(r) => {
                if r.id.is_empty() {
                    return Err(AppError::Validation("watch id empty".into()));
                }
                if r.pattern.is_empty() {
                    return Err(AppError::Validation("watch pattern empty".into()));
                }
                if r.threshold == 0 {
                    return Err(AppError::Validation("watch threshold must be >= 1".into()));
                }
                if state.watches.contains_key(&r.id) {
                    return Err(AppError::Validation(format!(
                        "watch {} already registered",
                        r.id
                    )));
                }
                Ok(())
            }
            // Observing an unmatched pattern is a no-op, not an error: the producer is
            // pattern-agnostic and will fire for every event on the bus.
            SheriffCommand::Observe(o) => {
                if o.pattern.is_empty() {
                    return Err(AppError::Validation("observe pattern empty".into()));
                }
                Ok(())
            }
            SheriffCommand::Clear(c) => {
                let w = state.watches.get(&c.id).ok_or_else(|| {
                    AppError::NotFound(format!("watch {} not registered", c.id))
                })?;
                if !w.raised {
                    return Err(AppError::Validation(format!(
                        "watch {} not raised",
                        c.id
                    )));
                }
                Ok(())
            }
        }
    }

    /// Re-validate and produce the events the actor must apply + emit. `Observe` may produce
    /// zero events (no matching watch) or up to two per matching watch (`Observed` plus
    /// `Raised` when the bump crosses the threshold).
    pub fn execute(&self, state: &SheriffState) -> Result<Vec<SheriffEvent>, AppError> {
        self.validate(state)?;
        match self {
            SheriffCommand::Register(r) => Ok(vec![SheriffEvent::Registered {
                id: r.id.clone(),
                pattern: r.pattern.clone(),
                threshold: r.threshold,
            }]),
            SheriffCommand::Observe(o) => {
                let mut out = Vec::new();
                for w in state.watches.values() {
                    if w.pattern != o.pattern || w.raised {
                        continue;
                    }
                    let new_count = w.count.saturating_add(1);
                    out.push(SheriffEvent::Observed {
                        id: w.id.clone(),
                        count: new_count,
                    });
                    if new_count >= w.threshold {
                        out.push(SheriffEvent::Raised {
                            id: w.id.clone(),
                            pattern: w.pattern.clone(),
                            at_count: new_count,
                        });
                    }
                }
                Ok(out)
            }
            SheriffCommand::Clear(c) => Ok(vec![SheriffEvent::Cleared { id: c.id.clone() }]),
        }
    }
}
