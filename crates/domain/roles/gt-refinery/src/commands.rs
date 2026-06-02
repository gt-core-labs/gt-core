//! Owned `Command` structs over `RefineryState`. Same shape as gt-deacon: `validate`
//! inspects without mutating; `execute` re-validates and returns the events to apply +
//! emit. Producers (the channel watcher edge / a root reaction over MergeEvent::Started)
//! call these via [`crate::refinery`] helpers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::RefineryEvent;
use crate::state::RefineryState;

/// Record an observed `MERGE_READY` channel message. Rejected if the bead already has an
/// entry (re-observations should arrive only after a `MarkDispatched`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ObserveReady {
    pub bead: String,
    pub branch: String,
    /// The `<ulid>.event` file id from the channel mailbox (audit trail).
    pub channel_msg_id: String,
    pub at: u64,
}

/// Flip the entry's `dispatched` flag. Idempotent: re-marking is `Ok` with zero events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkDispatched {
    pub bead: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RefineryCommand {
    Observe(ObserveReady),
    MarkDispatched(MarkDispatched),
}

impl RefineryCommand {
    pub fn validate(&self, state: &RefineryState) -> Result<(), AppError> {
        match self {
            RefineryCommand::Observe(o) => {
                if o.bead.is_empty() {
                    return Err(AppError::Validation("refinery observe bead empty".into()));
                }
                if o.branch.is_empty() {
                    return Err(AppError::Validation("refinery observe branch empty".into()));
                }
                if o.channel_msg_id.is_empty() {
                    return Err(AppError::Validation(
                        "refinery observe channel_msg_id empty".into(),
                    ));
                }
                if state.entries.contains_key(&o.bead) {
                    return Err(AppError::Validation(format!(
                        "refinery entry for {} already observed",
                        o.bead
                    )));
                }
                Ok(())
            }
            RefineryCommand::MarkDispatched(m) => {
                if !state.entries.contains_key(&m.bead) {
                    return Err(AppError::NotFound(format!(
                        "refinery entry {} not observed",
                        m.bead
                    )));
                }
                Ok(())
            }
        }
    }

    pub fn execute(&self, state: &RefineryState) -> Result<Vec<RefineryEvent>, AppError> {
        self.validate(state)?;
        match self {
            RefineryCommand::Observe(o) => Ok(vec![RefineryEvent::Observed {
                bead: o.bead.clone(),
                branch: o.branch.clone(),
                channel_msg_id: o.channel_msg_id.clone(),
                at: o.at,
            }]),
            RefineryCommand::MarkDispatched(m) => {
                // Idempotent: if already dispatched, return zero events.
                let already = state
                    .entries
                    .get(&m.bead)
                    .map(|e| e.dispatched)
                    .unwrap_or(false);
                if already {
                    Ok(Vec::new())
                } else {
                    Ok(vec![RefineryEvent::Dispatched {
                        bead: m.bead.clone(),
                    }])
                }
            }
        }
    }
}
