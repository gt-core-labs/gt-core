//! Owned `Command` structs over `MayorState`. Same shape as the gt-merge / gt-sheriff
//! commands: `validate` inspects without mutating, `execute` re-validates and returns the
//! events the actor must apply + emit in the same tick. Keeping the produced events as
//! the command output preserves the emit-on-apply invariant: only legal transitions reach
//! the log, so replay stays deterministic.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::MayorEvent;
use crate::state::{DelegationStatus, MayorState};

/// Create a new delegation. Each `id` is unique; re-delegating an existing id is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Delegate {
    /// Stable identifier for this delegation (operator-facing, used by `Acknowledge` /
    /// `Resolve` / `Withdraw`).
    pub id: String,
    /// Role or crew the mayor is handing off to (e.g. `"witness"`, `"refinery"`,
    /// `"crew-alpha"`). Opaque to the actor — pattern-matching is the producer's job.
    pub target: String,
    /// Free-form description of the work. Echoed back in the `Delegated` event so consumers
    /// of the audit log have the full context without joining tables.
    pub summary: String,
}

/// Mark a pending delegation acknowledged: the target picked it up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Acknowledge {
    pub id: String,
}

/// Resolve an acknowledged delegation with a free-form `outcome`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Resolve {
    pub id: String,
    pub outcome: String,
}

/// Operator-initiated cancel of a still-pending delegation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Withdraw {
    pub id: String,
}

/// The set of mutations the mayor actor accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum MayorCommand {
    Delegate(Delegate),
    Acknowledge(Acknowledge),
    Resolve(Resolve),
    Withdraw(Withdraw),
}

impl MayorCommand {
    /// Inspect `state` and return `Ok` iff the command would apply cleanly.
    pub fn validate(&self, state: &MayorState) -> Result<(), AppError> {
        match self {
            MayorCommand::Delegate(d) => {
                if d.id.is_empty() {
                    return Err(AppError::Validation("delegation id empty".into()));
                }
                if d.target.is_empty() {
                    return Err(AppError::Validation("delegation target empty".into()));
                }
                if d.summary.is_empty() {
                    return Err(AppError::Validation("delegation summary empty".into()));
                }
                if state.delegations.contains_key(&d.id) {
                    return Err(AppError::Validation(format!(
                        "delegation {} already exists",
                        d.id
                    )));
                }
                Ok(())
            }
            MayorCommand::Acknowledge(a) => {
                let d = state.delegations.get(&a.id).ok_or_else(|| {
                    AppError::NotFound(format!("delegation {} not found", a.id))
                })?;
                if d.status != DelegationStatus::Pending {
                    return Err(AppError::InvalidTransition(format!(
                        "delegation {}: {} → acknowledged",
                        d.id,
                        d.status.as_str()
                    )));
                }
                Ok(())
            }
            MayorCommand::Resolve(r) => {
                let d = state.delegations.get(&r.id).ok_or_else(|| {
                    AppError::NotFound(format!("delegation {} not found", r.id))
                })?;
                if d.status != DelegationStatus::Acknowledged {
                    return Err(AppError::InvalidTransition(format!(
                        "delegation {}: {} → resolved",
                        d.id,
                        d.status.as_str()
                    )));
                }
                Ok(())
            }
            MayorCommand::Withdraw(w) => {
                let d = state.delegations.get(&w.id).ok_or_else(|| {
                    AppError::NotFound(format!("delegation {} not found", w.id))
                })?;
                if d.status != DelegationStatus::Pending {
                    return Err(AppError::InvalidTransition(format!(
                        "delegation {}: {} → withdrawn",
                        d.id,
                        d.status.as_str()
                    )));
                }
                Ok(())
            }
        }
    }

    /// Re-validate and produce the events the actor must apply + emit. Each command yields
    /// exactly one event on success.
    pub fn execute(&self, state: &MayorState) -> Result<Vec<MayorEvent>, AppError> {
        self.validate(state)?;
        Ok(match self {
            MayorCommand::Delegate(d) => vec![MayorEvent::Delegated {
                id: d.id.clone(),
                target: d.target.clone(),
                summary: d.summary.clone(),
            }],
            MayorCommand::Acknowledge(a) => vec![MayorEvent::Acknowledged { id: a.id.clone() }],
            MayorCommand::Resolve(r) => vec![MayorEvent::Resolved {
                id: r.id.clone(),
                outcome: r.outcome.clone(),
            }],
            MayorCommand::Withdraw(w) => vec![MayorEvent::Withdrawn { id: w.id.clone() }],
        })
    }
}
