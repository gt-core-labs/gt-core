//! State of the Mayor domain: a board of `Delegation`s keyed by id. A delegation is the
//! unit the mayor hands off when it drives the orchestration loop — a target (the role or
//! crew that will execute) plus a free-form summary describing the work. The reducer and
//! the actor share this one shape, mirroring `gt-sheriff::SheriffState` (small domain, no
//! `Board`/`State` split).

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::MayorEvent;

/// Lifecycle of a delegation. `Pending → Acknowledged → Resolved` is the happy path;
/// `Withdrawn` is the operator-initiated cancel and is only legal from `Pending` (once the
/// target has acknowledged, only `Resolve` ends the delegation — withdrawing in-flight work
/// belongs to the target, not the mayor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegationStatus {
    Pending,
    Acknowledged,
    Resolved,
    Withdrawn,
}

impl DelegationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DelegationStatus::Pending => "pending",
            DelegationStatus::Acknowledged => "acknowledged",
            DelegationStatus::Resolved => "resolved",
            DelegationStatus::Withdrawn => "withdrawn",
        }
    }
}

/// One handoff the mayor produced. `outcome` is populated on `Resolved` and remains `None`
/// in every earlier status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delegation {
    pub id: String,
    pub target: String,
    pub summary: String,
    pub status: DelegationStatus,
    pub outcome: Option<String>,
}

/// Aggregate state of the mayor domain. `Default` is the empty board.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MayorState {
    pub delegations: BTreeMap<String, Delegation>,
}

impl MayorState {
    /// Pure fold over a `MayorEvent`. **Total** — unknown ids on transition events are
    /// no-ops, matching the reducer pattern used by `MergeState` / `SheriffState`.
    /// Validation lives on the write path (`MayorCommand::validate`); the fold trusts the
    /// log.
    pub fn apply(&mut self, ev: &MayorEvent) -> Result<(), AppError> {
        match ev {
            MayorEvent::Delegated { id, target, summary } => {
                self.delegations.insert(
                    id.clone(),
                    Delegation {
                        id: id.clone(),
                        target: target.clone(),
                        summary: summary.clone(),
                        status: DelegationStatus::Pending,
                        outcome: None,
                    },
                );
            }
            MayorEvent::Acknowledged { id } => {
                if let Some(d) = self.delegations.get_mut(id) {
                    d.status = DelegationStatus::Acknowledged;
                }
            }
            MayorEvent::Resolved { id, outcome } => {
                if let Some(d) = self.delegations.get_mut(id) {
                    d.status = DelegationStatus::Resolved;
                    d.outcome = Some(outcome.clone());
                }
            }
            MayorEvent::Withdrawn { id } => {
                if let Some(d) = self.delegations.get_mut(id) {
                    d.status = DelegationStatus::Withdrawn;
                }
            }
        }
        Ok(())
    }
}
