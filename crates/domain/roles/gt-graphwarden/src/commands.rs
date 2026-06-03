//! Owned `Command` structs over `WardenState`. Same shape as `WitnessCommand`:
//! `validate` inspects without mutating, `execute` re-validates and returns the events the
//! actor must apply + emit in the same tick (emit-on-apply: only legal transitions reach
//! the log, so replay stays deterministic).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::WardenEvent;
use crate::state::WardenState;

/// Put a rig's repo under graph custody. `rig` is unique; re-registering is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegisterRig {
    /// Stable rig id (matches the rig catalog).
    pub rig: String,
    /// On-disk checkout the indexer runs against.
    pub repo_dir: String,
    /// Wall-clock seconds the producer recorded at registration.
    pub now_secs: u64,
}

/// Record that a registered rig's graph fell behind by `changed` files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkStale {
    pub rig: String,
    /// Files that moved since the last index (>= 1).
    pub changed: u64,
    pub now_secs: u64,
}

/// Record that a re-index completed, bringing the rig's graph current as of `commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkRefreshed {
    pub rig: String,
    /// Repo commit the graph is now current at (non-empty).
    pub commit: String,
    pub now_secs: u64,
}

/// End custody of a rig (it left the catalog).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Unregister {
    pub rig: String,
    pub now_secs: u64,
}

/// The set of mutations the warden actor accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum WardenCommand {
    Register(RegisterRig),
    MarkStale(MarkStale),
    MarkRefreshed(MarkRefreshed),
    Unregister(Unregister),
}

impl WardenCommand {
    /// Inspect `state` and return `Ok` iff the command would apply cleanly.
    pub fn validate(&self, state: &WardenState) -> Result<(), AppError> {
        match self {
            WardenCommand::Register(r) => {
                if r.rig.is_empty() {
                    return Err(AppError::Validation("register rig empty".into()));
                }
                if r.repo_dir.is_empty() {
                    return Err(AppError::Validation("register repo_dir empty".into()));
                }
                if state.rigs.contains_key(&r.rig) {
                    return Err(AppError::Validation(format!("rig {} already registered", r.rig)));
                }
                Ok(())
            }
            WardenCommand::MarkStale(m) => {
                if m.changed == 0 {
                    return Err(AppError::Validation("mark-stale changed must be >= 1".into()));
                }
                require_registered(state, &m.rig)
            }
            WardenCommand::MarkRefreshed(m) => {
                if m.commit.is_empty() {
                    return Err(AppError::Validation("refresh commit empty".into()));
                }
                require_registered(state, &m.rig)
            }
            WardenCommand::Unregister(u) => require_registered(state, &u.rig),
        }
    }

    /// Re-validate and produce the events the actor must apply + emit.
    pub fn execute(&self, state: &WardenState) -> Result<Vec<WardenEvent>, AppError> {
        self.validate(state)?;
        Ok(match self {
            WardenCommand::Register(r) => vec![WardenEvent::RigRegistered {
                rig: r.rig.clone(),
                repo_dir: r.repo_dir.clone(),
                now_secs: r.now_secs,
            }],
            WardenCommand::MarkStale(m) => vec![WardenEvent::MarkedStale {
                rig: m.rig.clone(),
                changed: m.changed,
                now_secs: m.now_secs,
            }],
            WardenCommand::MarkRefreshed(m) => vec![WardenEvent::Refreshed {
                rig: m.rig.clone(),
                commit: m.commit.clone(),
                now_secs: m.now_secs,
            }],
            WardenCommand::Unregister(u) => vec![WardenEvent::Unregistered {
                rig: u.rig.clone(),
                now_secs: u.now_secs,
            }],
        })
    }
}

fn require_registered(state: &WardenState, rig: &str) -> Result<(), AppError> {
    if state.rigs.contains_key(rig) {
        Ok(())
    } else {
        Err(AppError::NotFound(format!("rig {rig} not registered")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered() -> WardenState {
        let mut s = WardenState::default();
        WardenCommand::Register(RegisterRig {
            rig: "alpha".into(),
            repo_dir: "/repo/alpha".into(),
            now_secs: 100,
        })
        .execute(&s)
        .unwrap()
        .iter()
        .for_each(|e| s.apply(e).unwrap());
        s
    }

    #[test]
    fn register_then_duplicate_rejected() {
        let s = registered();
        let err = WardenCommand::Register(RegisterRig {
            rig: "alpha".into(),
            repo_dir: "/repo/alpha".into(),
            now_secs: 1,
        })
        .validate(&s)
        .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn stale_then_refresh_clears_pending() {
        let mut s = registered();
        for e in WardenCommand::MarkStale(MarkStale { rig: "alpha".into(), changed: 3, now_secs: 200 })
            .execute(&s)
            .unwrap()
        {
            s.apply(&e).unwrap();
        }
        assert!(s.rigs["alpha"].stale);
        assert_eq!(s.rigs["alpha"].pending_changes, 3);

        for e in WardenCommand::MarkRefreshed(MarkRefreshed {
            rig: "alpha".into(),
            commit: "abc1234".into(),
            now_secs: 300,
        })
        .execute(&s)
        .unwrap()
        {
            s.apply(&e).unwrap();
        }
        assert!(!s.rigs["alpha"].stale);
        assert_eq!(s.rigs["alpha"].pending_changes, 0);
        assert_eq!(s.rigs["alpha"].last_indexed_commit.as_deref(), Some("abc1234"));
    }

    #[test]
    fn stale_unknown_rig_is_not_found() {
        let s = WardenState::default();
        let err = WardenCommand::MarkStale(MarkStale { rig: "ghost".into(), changed: 1, now_secs: 1 })
            .validate(&s)
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }
}
