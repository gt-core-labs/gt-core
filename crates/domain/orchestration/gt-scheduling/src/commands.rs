//! Owned `Command` structs over [`SchedCore`] (see `apps/api/docs/09-llm-integration.md`).
//!
//! Each command is pure and sync: `validate` inspects the queue/capacity without mutation;
//! `execute` re-validates and applies the change. The async edge (the dispatcher actor) is
//! what emits the resulting `SchedEvent` to the log and runs the CAS-claim — never the
//! command. External clients (e.g. `gt-mcp`) call `validate` to "ask without doing".

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::state::SchedCore;

/// Highest priority value accepted: 0 = P0 (highest) .. 2 = P2.
const MAX_PRIORITY: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Enqueue {
    /// Bead id to enqueue. Must be non-empty.
    pub bead: String,
    /// Priority: 0 = P0 (highest) .. 2 = P2.
    pub priority: u8,
}

impl Command for Enqueue {
    type Output = ();
    type State = SchedCore;

    fn validate(&self, _state: &Self::State) -> Result<(), AppError> {
        if self.bead.is_empty() {
            return Err(AppError::Validation("bead id is empty".into()));
        }
        if self.priority > MAX_PRIORITY {
            return Err(AppError::Validation(format!(
                "priority {} out of range (0..={MAX_PRIORITY})",
                self.priority
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.queue.enqueue(self.bead.clone(), self.priority);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkDispatched {
    /// Bead id to mark as dispatched.
    pub bead: String,
    /// Worker the bead is assigned to. Must be non-empty.
    pub worker: String,
}

impl Command for MarkDispatched {
    type Output = ();
    type State = SchedCore;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.bead.is_empty() {
            return Err(AppError::Validation("bead id is empty".into()));
        }
        if self.worker.is_empty() {
            return Err(AppError::Validation("worker is empty".into()));
        }
        if !state.gov.can_dispatch() {
            return Err(AppError::Validation(
                "no dispatch capacity available".into(),
            ));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // The bead is no longer pending; consume a capacity slot. The matching
        // `SchedEvent::Dispatched` is emitted by the actor at the edge, not here.
        state.queue.remove(&self.bead);
        state.gov.acquire();
        Ok(())
    }
}

/// Sum type so the actor can route any scheduling command through one message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedCommand {
    Enqueue(Enqueue),
    MarkDispatched(MarkDispatched),
}

impl SchedCommand {
    /// Stable tool name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Enqueue(_) => "scheduling.enqueue",
            Self::MarkDispatched(_) => "scheduling.mark_dispatched",
        }
    }
}

impl Command for SchedCommand {
    type Output = ();
    type State = SchedCore;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Enqueue(c) => c.validate(state),
            Self::MarkDispatched(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Enqueue(c) => c.execute(state),
            Self::MarkDispatched(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_rejects_empty_and_out_of_range() {
        let mut core = SchedCore::new(2);
        assert!(Enqueue { bead: "".into(), priority: 0 }.validate(&core).is_err());
        assert!(Enqueue { bead: "b1".into(), priority: 3 }.validate(&core).is_err());
        Enqueue { bead: "b1".into(), priority: 1 }
            .execute(&mut core)
            .unwrap();
        assert_eq!(core.queue.len(), 1);
    }

    #[test]
    fn mark_dispatched_needs_capacity_and_consumes_a_slot() {
        let mut core = SchedCore::new(1);
        core.queue.enqueue("b1", 1);
        let cmd = MarkDispatched { bead: "b1".into(), worker: "w1".into() };
        cmd.validate(&core).unwrap();
        cmd.execute(&mut core).unwrap();
        assert_eq!(core.gov.in_flight(), 1);
        assert!(core.queue.is_empty(), "dispatched bead leaves the pending queue");
        // No capacity left → a second mark is rejected by validate.
        let cmd2 = MarkDispatched { bead: "b2".into(), worker: "w2".into() };
        assert!(cmd2.validate(&core).is_err());
    }

    #[test]
    fn validate_does_not_mutate() {
        let core = SchedCore::new(1);
        let before = core.gov.in_flight();
        MarkDispatched { bead: "b1".into(), worker: "w1".into() }
            .validate(&core)
            .unwrap();
        assert_eq!(core.gov.in_flight(), before, "validate must not consume capacity");
    }
}
