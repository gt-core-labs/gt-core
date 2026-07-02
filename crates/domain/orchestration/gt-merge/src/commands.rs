//! Owned `Command` structs over `MergeBoard` (see `docs/09-llm-integration.md`).
//!
//! Mirrors the gt-agent retrofit: each mutation the actor performs is also a pure, sync
//! [`Command`]. `validate` inspects the board without mutating it; `execute` re-validates,
//! applies the state-machine transition, and **returns the `MergeEvent` it produced** so the
//! actor can emit that event to the relay in the same tick. Keeping the produced event as the
//! command `Output` preserves the emit-on-apply invariant of `actor.rs`: only legal
//! transitions reach the log, so replay stays deterministic.
//!
//! External clients (a model via `gt-mcp`) call `validate` to "ask without doing" — the
//! answer is a snapshot, so the actor revalidates on exec, closing the TOCTOU window.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::{AppError, Command};

use crate::events::MergeEvent;
use crate::state::{MergeBoard, MergeSlotState, MAX_MERGE_RETRIES};

/// Refinery (or an MCP client) requests a merge: register a new slot in `Ready`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SubmitMerge {
    /// Bead awaiting merge. Must be non-empty and not already submitted.
    pub bead: String,
    /// Branch to merge. Must be non-empty.
    pub branch: String,
    /// Id of the channel-events `<ulid>.event` that triggered this (auditing).
    pub channel_msg_id: String,
}

impl Command for SubmitMerge {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        if self.bead.is_empty() {
            return Err(AppError::Validation("merge bead is empty".into()));
        }
        if self.branch.is_empty() {
            return Err(AppError::Validation("merge branch is empty".into()));
        }
        // Failed slots may be re-submitted: the sheriff repairs the branch and re-queues it.
        // Any other state (Ready / Merging / Merged) rejects the duplicate — those are in-flight
        // or terminal and must not be displaced. Mirrors MergeBoard::submit's state check.
        if let Some(slot) = state.get(&self.bead) {
            if slot.state != MergeSlotState::Failed {
                return Err(AppError::Validation(format!(
                    "merge slot for {} already submitted",
                    self.bead
                )));
            }
        }
        Ok(())
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.submit(self.bead.clone(), self.branch.clone())?;
        Ok(MergeEvent::Ready {
            bead: self.bead.clone(),
            branch: self.branch.clone(),
            channel_msg_id: self.channel_msg_id.clone(),
        })
    }
}

/// Take the slot to the physical merge: `Ready → Merging`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StartMerge {
    /// Bead of an existing slot in `Ready`.
    pub bead: String,
}

impl Command for StartMerge {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        probe_transition(state, &self.bead, MergeSlotState::Merging)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        state.start(&self.bead)?;
        Ok(MergeEvent::Started {
            bead: self.bead.clone(),
        })
    }
}

/// Edge reports a successful merge: `Merging → Merged`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompleteMerge {
    /// Bead of an existing slot in `Merging`.
    pub bead: String,
    /// Resulting merge commit sha.
    pub sha: String,
}

impl Command for CompleteMerge {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        probe_transition(state, &self.bead, MergeSlotState::Merged)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        state.complete(&self.bead)?;
        Ok(MergeEvent::Merged {
            bead: self.bead.clone(),
            sha: self.sha.clone(),
        })
    }
}

/// Edge reports a failed merge (conflict, hook): `Merging → Failed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FailMerge {
    /// Bead of an existing slot in `Merging`.
    pub bead: String,
    /// Why the merge failed.
    pub reason: String,
}

impl Command for FailMerge {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        probe_transition(state, &self.bead, MergeSlotState::Failed)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        state.fail(&self.bead)?;
        Ok(MergeEvent::Failed {
            bead: self.bead.clone(),
            reason: self.reason.clone(),
        })
    }
}

/// Operator/sheriff re-queues a `Failed` slot: `Failed → Ready` (gtcore-4ad682).
///
/// This is the missing exit from `failed`: `SubmitMerge` answers "already submitted" for a slot
/// that is not `Failed`, and `StartMerge` rejects `failed → merging`, so before this command a
/// slot that failed the physical merge had no MCP path back into the queue — sheriffs/polecats
/// with READY, pushed work had to ask for a manual DB edit. `ResetMerge` gives them that path,
/// bounded by [`MAX_MERGE_RETRIES`] so a branch that keeps failing cannot loop forever.
///
/// The "already delivered" guard (bead `closed` with a `delivered_sha`, or the branch is
/// `ahead=0` of `main`) is NOT here: it needs the issues store / git, which the pure command
/// cannot see. It lives in the MCP handler (`merge.reset` dispatch) which reads the bead before
/// running this command. What the command DOES enforce is the state-machine half — only a
/// `Failed` slot resets; a `Merged` slot (the board's record that the work reached `main`) is
/// rejected, pointing the caller at `merge.complete` instead of a pointless retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResetMerge {
    /// Bead of an existing slot in `Failed`.
    pub bead: String,
}

impl Command for ResetMerge {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        let slot = state
            .get(&self.bead)
            .ok_or_else(|| AppError::NotFound(format!("merge slot {}", self.bead)))?;
        // A Merged slot means the work already landed on main — retrying is meaningless; the
        // caller wanted `merge.complete`, not `reset`. Give that hint instead of a bare
        // "invalid transition".
        if slot.state == MergeSlotState::Merged {
            return Err(AppError::Validation(format!(
                "merge slot {} is already merged — nothing to reset (use merge.complete)",
                self.bead
            )));
        }
        // Cap the retry loop (acceptance #4). A re-`submit` (fresh branch) zeroes the counter;
        // blind resets of the same failed branch are bounded.
        if slot.retries >= MAX_MERGE_RETRIES {
            return Err(AppError::Validation(format!(
                "merge slot {} hit the reset cap ({MAX_MERGE_RETRIES}); re-submit a fixed branch or merge.complete it",
                self.bead
            )));
        }
        // State-machine legality: only `Failed → Ready` is legal here.
        probe_transition(state, &self.bead, MergeSlotState::Ready)
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        self.validate(state)?;
        state.reset(&self.bead)?;
        Ok(MergeEvent::Reset {
            bead: self.bead.clone(),
        })
    }
}

/// Check a transition without mutating the board: clone the slot and probe it.
fn probe_transition(
    state: &MergeBoard,
    bead: &str,
    to: MergeSlotState,
) -> Result<(), AppError> {
    let slot = state
        .get(bead)
        .ok_or_else(|| AppError::NotFound(format!("merge slot {bead}")))?;
    slot.clone().transition(to)
}

/// Sum type so the actor routes any merge command through a single message variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MergeCommand {
    Submit(SubmitMerge),
    Start(StartMerge),
    Complete(CompleteMerge),
    Fail(FailMerge),
    Reset(ResetMerge),
}

impl MergeCommand {
    /// Stable tool base name used by `gt-mcp` to dispatch. Matches the pattern in `docs/09`.
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Submit(_) => "merge.submit",
            Self::Start(_) => "merge.start",
            Self::Complete(_) => "merge.complete",
            Self::Fail(_) => "merge.fail",
            Self::Reset(_) => "merge.reset",
        }
    }
}

impl Command for MergeCommand {
    type Output = MergeEvent;
    type State = MergeBoard;

    fn validate(&self, state: &Self::State) -> Result<(), AppError> {
        match self {
            Self::Submit(c) => c.validate(state),
            Self::Start(c) => c.validate(state),
            Self::Complete(c) => c.validate(state),
            Self::Fail(c) => c.validate(state),
            Self::Reset(c) => c.validate(state),
        }
    }

    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError> {
        match self {
            Self::Submit(c) => c.execute(state),
            Self::Start(c) => c.execute(state),
            Self::Complete(c) => c.execute(state),
            Self::Fail(c) => c.execute(state),
            Self::Reset(c) => c.execute(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_rejects_duplicate_and_empty() {
        let mut board = MergeBoard::default();
        let cmd = SubmitMerge {
            bead: "b1".into(),
            branch: "feat/x".into(),
            channel_msg_id: "01".into(),
        };
        let ev = cmd.execute(&mut board).unwrap();
        assert_eq!(
            ev,
            MergeEvent::Ready {
                bead: "b1".into(),
                branch: "feat/x".into(),
                channel_msg_id: "01".into()
            }
        );
        // Re-submit of the same bead is rejected.
        assert!(matches!(cmd.validate(&board), Err(AppError::Validation(_))));
        // Empty fields are rejected.
        let empty = SubmitMerge {
            bead: String::new(),
            branch: "feat/y".into(),
            channel_msg_id: "02".into(),
        };
        assert!(matches!(empty.validate(&board), Err(AppError::Validation(_))));
    }

    #[test]
    fn validate_does_not_mutate() {
        let mut board = MergeBoard::default();
        SubmitMerge {
            bead: "b1".into(),
            branch: "feat/x".into(),
            channel_msg_id: "01".into(),
        }
        .execute(&mut board)
        .unwrap();
        let cmd = StartMerge { bead: "b1".into() };
        cmd.validate(&board).unwrap();
        assert_eq!(
            board.get("b1").unwrap().state,
            MergeSlotState::Ready,
            "validate must not mutate the board",
        );
    }

    #[test]
    fn illegal_transition_fails_both_validate_and_execute() {
        let mut board = MergeBoard::default();
        SubmitMerge {
            bead: "b1".into(),
            branch: "feat/x".into(),
            channel_msg_id: "01".into(),
        }
        .execute(&mut board)
        .unwrap();
        // Ready → Merged (skipping Merging) is illegal.
        let cmd = CompleteMerge {
            bead: "b1".into(),
            sha: "abc".into(),
        };
        assert!(cmd.validate(&board).is_err());
        assert!(cmd.execute(&mut board).is_err());
    }

    #[test]
    fn full_lifecycle_emits_expected_events() {
        let mut board = MergeBoard::default();
        let r = MergeCommand::Submit(SubmitMerge {
            bead: "b1".into(),
            branch: "feat/x".into(),
            channel_msg_id: "01".into(),
        })
        .execute(&mut board)
        .unwrap();
        assert!(matches!(r, MergeEvent::Ready { .. }));
        let s = MergeCommand::Start(StartMerge { bead: "b1".into() })
            .execute(&mut board)
            .unwrap();
        assert_eq!(s, MergeEvent::Started { bead: "b1".into() });
        let m = MergeCommand::Complete(CompleteMerge {
            bead: "b1".into(),
            sha: "abc".into(),
        })
        .execute(&mut board)
        .unwrap();
        assert_eq!(
            m,
            MergeEvent::Merged {
                bead: "b1".into(),
                sha: "abc".into()
            }
        );
    }

    #[test]
    fn start_unknown_slot_fails() {
        let board = MergeBoard::default();
        let cmd = StartMerge { bead: "ghost".into() };
        assert!(matches!(cmd.validate(&board), Err(AppError::NotFound(_))));
    }

    /// Drive a slot to Failed via the commands so ResetMerge sees a real board.
    fn board_with_failed_slot() -> MergeBoard {
        let mut board = MergeBoard::default();
        SubmitMerge { bead: "b1".into(), branch: "feat/x".into(), channel_msg_id: "01".into() }
            .execute(&mut board)
            .unwrap();
        StartMerge { bead: "b1".into() }.execute(&mut board).unwrap();
        FailMerge { bead: "b1".into(), reason: "conflict".into() }
            .execute(&mut board)
            .unwrap();
        board
    }

    #[test]
    fn reset_emits_reset_event_and_requeues() {
        let mut board = board_with_failed_slot();
        let ev = ResetMerge { bead: "b1".into() }.execute(&mut board).unwrap();
        assert_eq!(ev, MergeEvent::Reset { bead: "b1".into() });
        assert_eq!(board.get("b1").unwrap().state, MergeSlotState::Ready);
    }

    #[test]
    fn reset_rejects_unknown_merged_and_capped() {
        // Unknown slot → NotFound.
        let empty = MergeBoard::default();
        assert!(matches!(
            ResetMerge { bead: "ghost".into() }.validate(&empty),
            Err(AppError::NotFound(_))
        ));

        // Merged slot → Validation pointing at merge.complete (not a bare invalid-transition).
        let mut merged = MergeBoard::default();
        SubmitMerge { bead: "b1".into(), branch: "x".into(), channel_msg_id: "01".into() }
            .execute(&mut merged)
            .unwrap();
        StartMerge { bead: "b1".into() }.execute(&mut merged).unwrap();
        CompleteMerge { bead: "b1".into(), sha: "abc".into() }
            .execute(&mut merged)
            .unwrap();
        assert!(matches!(
            ResetMerge { bead: "b1".into() }.validate(&merged),
            Err(AppError::Validation(_))
        ));

        // Retry cap (acceptance #4): after MAX_MERGE_RETRIES resets, the next is rejected. Each
        // cycle is reset → start → fail so the slot is Failed again before the next reset.
        let mut board = board_with_failed_slot();
        for _ in 0..MAX_MERGE_RETRIES {
            ResetMerge { bead: "b1".into() }.execute(&mut board).unwrap();
            StartMerge { bead: "b1".into() }.execute(&mut board).unwrap();
            FailMerge { bead: "b1".into(), reason: "again".into() }
                .execute(&mut board)
                .unwrap();
        }
        assert_eq!(board.get("b1").unwrap().retries, MAX_MERGE_RETRIES);
        let capped = ResetMerge { bead: "b1".into() }.execute(&mut board).unwrap_err();
        assert!(matches!(capped, AppError::Validation(_)), "reset is capped");
    }
}
