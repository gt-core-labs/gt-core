//! Owned `Command` structs over `SessionRegistry` (see `docs/09-llm-integration.md`).
//!
//! Each command is pure and sync: `validate` inspects the state without mutation;
//! `execute` re-validates and applies the change. The actor invokes them in a single
//! tick so there is no TOCTOU window. External clients (e.g. `gt-mcp`) call `validate`
//! to "ask without doing" — the answer is a snapshot, so the actor revalidates on exec.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::state::{Session, SessionRegistry, SessionState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AddSession {
    /// Unique session id. Must be non-empty.
    pub id: String,
    /// Rig the session runs in. Must be non-empty.
    pub rig: String,
}

impl Command for AddSession {
    type Output = ();
    type State = SessionRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.id.is_empty() {
            return Err(AppError::Validation("session id is empty".into()));
        }
        if self.rig.is_empty() {
            return Err(AppError::Validation("rig is empty".into()));
        }
        if state.get(&self.id).is_some() {
            return Err(AppError::Validation(format!(
                "session {} already exists",
                self.id
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.add(Session::new(&self.id, &self.rig));
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RemoveSession {
    /// Id of an existing session.
    pub id: String,
}

impl Command for RemoveSession {
    type Output = ();
    type State = SessionRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if state.get(&self.id).is_none() {
            return Err(AppError::NotFound(format!("session {}", self.id)));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.remove(&self.id);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TransitionSession {
    /// Id of an existing session.
    pub id: String,
    /// Target lifecycle state. Illegal transitions are rejected by the state machine.
    pub to: SessionState,
}

impl Command for TransitionSession {
    type Output = ();
    type State = SessionRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        let sess = state
            .get(&self.id)
            .ok_or_else(|| AppError::NotFound(format!("session {}", self.id)))?;
        let mut probe = sess.clone();
        probe.transition(self.to)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        state.transition(&self.id, self.to)
    }
}

/// Sum type so the actor can route any agent command through a single message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentCommand {
    Add(AddSession),
    Remove(RemoveSession),
    Transition(TransitionSession),
}

impl AgentCommand {
    /// Stable tool name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Add(_) => "agent.add",
            Self::Remove(_) => "agent.remove",
            Self::Transition(_) => "agent.transition",
        }
    }
}

impl Command for AgentCommand {
    type Output = ();
    type State = SessionRegistry;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Add(c) => c.validate(state),
            Self::Remove(c) => c.validate(state),
            Self::Transition(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Add(c) => c.execute(state),
            Self::Remove(c) => c.execute(state),
            Self::Transition(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_rejects_duplicate() {
        let mut reg = SessionRegistry::default();
        let cmd = AddSession {
            id: "p1".into(),
            rig: "granite".into(),
        };
        cmd.execute(&mut reg).unwrap();
        assert!(matches!(cmd.validate(&reg), Err(AppError::Validation(_))));
    }

    #[test]
    fn transition_validate_does_not_mutate() {
        let mut reg = SessionRegistry::default();
        AddSession {
            id: "p1".into(),
            rig: "granite".into(),
        }
        .execute(&mut reg)
        .unwrap();
        let cmd = TransitionSession {
            id: "p1".into(),
            to: SessionState::Working,
        };
        cmd.validate(&reg).unwrap();
        let original = reg.get("p1").unwrap().state;
        assert_eq!(original, SessionState::Spawned, "validate must not mutate");
    }

    #[test]
    fn transition_illegal_target_fails_both() {
        let mut reg = SessionRegistry::default();
        AddSession {
            id: "p1".into(),
            rig: "granite".into(),
        }
        .execute(&mut reg)
        .unwrap();
        // Spawned -> Done is illegal (must pass through Working).
        let cmd = TransitionSession {
            id: "p1".into(),
            to: SessionState::Done,
        };
        assert!(cmd.validate(&reg).is_err());
        assert!(cmd.execute(&mut reg).is_err());
    }

    #[test]
    fn remove_unknown_fails() {
        let reg = SessionRegistry::default();
        let cmd = RemoveSession { id: "ghost".into() };
        assert!(matches!(cmd.validate(&reg), Err(AppError::NotFound(_))));
    }
}
