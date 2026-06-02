//! Owned `Command` structs over [`ConvoyBoard`] (see `apps/api/docs/09-llm-integration.md`).
//!
//! Each command is pure and sync: `validate` inspects the board without mutation; `execute`
//! re-validates and applies the change, returning the events to emit. Like `gt-patrol::Tick`,
//! the output is variadic (`Vec<OrchEvent>`) because a convoy advances by **handoff**: one
//! frontier call can launch a convoy *and* dispatch its first member, or complete a member
//! *and* either dispatch the next or close the convoy. The actor relays whatever `execute`
//! returns; the convoy advances on facts only — no clock in the core (`docs/06`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::OrchEvent;
use crate::state::ConvoyBoard;

/// Push the next handoff event for a launched convoy: close it if every member is done,
/// otherwise dispatch the next pending member. At most one event (the convoy is sequential).
fn feed_or_close(board: &mut ConvoyBoard, convoy: &str, out: &mut Vec<OrchEvent>) {
    if board.all_done(convoy) {
        if board.close(convoy).is_ok() {
            out.push(OrchEvent::ConvoyClosed { convoy: convoy.to_string() });
        }
    } else if let Some(member) = board.dispatch_next(convoy) {
        out.push(OrchEvent::MemberDispatched { convoy: convoy.to_string(), member });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LaunchConvoy {
    /// Convoy id. Must be non-empty and not already exist.
    pub convoy: String,
    /// Ordered member beads driven to completion. Must be non-empty.
    pub members: Vec<String>,
}

impl Command for LaunchConvoy {
    type Output = Vec<OrchEvent>;
    type State = ConvoyBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.convoy.is_empty() {
            return Err(AppError::Validation("convoy id is empty".into()));
        }
        if self.members.is_empty() {
            return Err(AppError::Validation("convoy has no members".into()));
        }
        if state.get(&self.convoy).is_some() {
            return Err(AppError::Validation(format!(
                "convoy {} already exists",
                self.convoy
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.create(self.convoy.clone(), self.members.clone())?;
        state.launch(&self.convoy)?;
        let mut out = vec![
            OrchEvent::ConvoyCreated {
                convoy: self.convoy.clone(),
                members: self.members.clone(),
            },
            OrchEvent::ConvoyLaunched { convoy: self.convoy.clone() },
        ];
        feed_or_close(state, &self.convoy, &mut out);
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompleteMember {
    /// Convoy the member belongs to.
    pub convoy: String,
    /// Member bead that just finished. Must be the active member.
    pub member: String,
}

impl Command for CompleteMember {
    type Output = Vec<OrchEvent>;
    type State = ConvoyBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        let convoy = state
            .get(&self.convoy)
            .ok_or_else(|| AppError::NotFound(format!("convoy {}", self.convoy)))?;
        if !convoy.members.iter().any(|m| m.bead == self.member) {
            return Err(AppError::NotFound(format!(
                "convoy {}: member {}",
                self.convoy, self.member
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        // `complete` enforces Active → Done; an illegal move surfaces as InvalidTransition.
        state.complete(&self.convoy, &self.member)?;
        let mut out = vec![OrchEvent::MemberCompleted {
            convoy: self.convoy.clone(),
            member: self.member.clone(),
        }];
        feed_or_close(state, &self.convoy, &mut out);
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FailMember {
    /// Convoy the member belongs to.
    pub convoy: String,
    /// Member bead that failed. Must be the active member.
    pub member: String,
    /// Why it failed (surfaced in the convoy-failed event).
    pub reason: String,
}

impl Command for FailMember {
    type Output = Vec<OrchEvent>;
    type State = ConvoyBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        let convoy = state
            .get(&self.convoy)
            .ok_or_else(|| AppError::NotFound(format!("convoy {}", self.convoy)))?;
        if !convoy.members.iter().any(|m| m.bead == self.member) {
            return Err(AppError::NotFound(format!(
                "convoy {}: member {}",
                self.convoy, self.member
            )));
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.fail(&self.convoy, &self.member)?;
        let mut out = vec![OrchEvent::MemberFailed {
            convoy: self.convoy.clone(),
            member: self.member.clone(),
            reason: self.reason.clone(),
        }];
        // A member failure halts the convoy (Launched → Failed). Already-failed is a no-op.
        if state.mark_failed(&self.convoy).is_ok() {
            out.push(OrchEvent::ConvoyFailed {
                convoy: self.convoy.clone(),
                member: self.member.clone(),
                reason: self.reason.clone(),
            });
        }
        Ok(out)
    }
}

/// Sum type so the actor can route any orchestration command through one message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrchCommand {
    Launch(LaunchConvoy),
    Complete(CompleteMember),
    Fail(FailMember),
}

impl OrchCommand {
    /// Stable tool name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Launch(_) => "convoy.launch",
            Self::Complete(_) => "convoy.complete-member",
            Self::Fail(_) => "convoy.fail-member",
        }
    }
}

impl Command for OrchCommand {
    type Output = Vec<OrchEvent>;
    type State = ConvoyBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Launch(c) => c.validate(state),
            Self::Complete(c) => c.validate(state),
            Self::Fail(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Launch(c) => c.execute(state),
            Self::Complete(c) => c.execute(state),
            Self::Fail(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ConvoyState, MemberState};

    #[test]
    fn launch_creates_launches_and_feeds_first_member() {
        let mut board = ConvoyBoard::default();
        let evs = LaunchConvoy { convoy: "cv".into(), members: vec!["a".into(), "b".into()] }
            .execute(&mut board)
            .unwrap();
        assert!(matches!(evs[0], OrchEvent::ConvoyCreated { .. }));
        assert!(matches!(evs[1], OrchEvent::ConvoyLaunched { .. }));
        assert!(matches!(&evs[2], OrchEvent::MemberDispatched { member, .. } if member == "a"));
        assert_eq!(board.get("cv").unwrap().state, ConvoyState::Launched);
    }

    #[test]
    fn complete_hands_off_then_closes() {
        let mut board = ConvoyBoard::default();
        LaunchConvoy { convoy: "cv".into(), members: vec!["a".into(), "b".into()] }
            .execute(&mut board)
            .unwrap();
        // a done → b dispatched.
        let evs = CompleteMember { convoy: "cv".into(), member: "a".into() }
            .execute(&mut board)
            .unwrap();
        assert!(matches!(&evs.last().unwrap(), OrchEvent::MemberDispatched { member, .. } if member == "b"));
        // b done → convoy closes.
        let evs = CompleteMember { convoy: "cv".into(), member: "b".into() }
            .execute(&mut board)
            .unwrap();
        assert!(matches!(evs.last().unwrap(), OrchEvent::ConvoyClosed { .. }));
        assert_eq!(board.get("cv").unwrap().state, ConvoyState::Closed);
    }

    #[test]
    fn fail_member_halts_convoy() {
        let mut board = ConvoyBoard::default();
        LaunchConvoy { convoy: "cv".into(), members: vec!["a".into(), "b".into()] }
            .execute(&mut board)
            .unwrap();
        let evs = FailMember { convoy: "cv".into(), member: "a".into(), reason: "boom".into() }
            .execute(&mut board)
            .unwrap();
        assert!(matches!(&evs[0], OrchEvent::MemberFailed { .. }));
        assert!(matches!(&evs[1], OrchEvent::ConvoyFailed { .. }));
        assert_eq!(board.get("cv").unwrap().state, ConvoyState::Failed);
        // b never advanced.
        let b = board.get("cv").unwrap().members.iter().find(|m| m.bead == "b").unwrap();
        assert_eq!(b.state, MemberState::Pending);
    }

    #[test]
    fn validate_rejects_unknown_and_duplicate() {
        let mut board = ConvoyBoard::default();
        assert!(LaunchConvoy { convoy: "".into(), members: vec!["a".into()] }.validate(&board).is_err());
        assert!(LaunchConvoy { convoy: "cv".into(), members: vec![] }.validate(&board).is_err());
        assert!(CompleteMember { convoy: "ghost".into(), member: "a".into() }.validate(&board).is_err());
        LaunchConvoy { convoy: "cv".into(), members: vec!["a".into()] }.execute(&mut board).unwrap();
        assert!(LaunchConvoy { convoy: "cv".into(), members: vec!["a".into()] }.validate(&board).is_err(), "duplicate convoy");
        assert!(CompleteMember { convoy: "cv".into(), member: "ghost".into() }.validate(&board).is_err(), "unknown member");
    }
}
