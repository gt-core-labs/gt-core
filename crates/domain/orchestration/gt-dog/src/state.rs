//! Dog identity + lifecycle state + reducer primitives (`hq-mod-dogs.1`).
//!
//! A [`Dog`](crate::Dog) moves through a small lifecycle as it picks up and runs
//! work: [`Idle`](DogStatus::Idle) → claim → [`Claimed`](DogStatus::Claimed) →
//! start → [`Running`](DogStatus::Running) → finish/fail → back to `Idle`, with
//! [`Retired`](DogStatus::Retired) as the terminal sink. [`DogState`] is the
//! in-memory projection of one worker, and its mutators
//! ([`claim`](DogState::claim), [`start`](DogState::start),
//! [`finish`](DogState::finish), [`fail`](DogState::fail),
//! [`retire`](DogState::retire)) are the **reducer primitives**: a Dog's state is
//! rebuilt by folding its event log through them, and they are the only way it
//! changes. They are total and side-effect-free — same starting state + same
//! call always yields the same result — so replay is byte-for-byte deterministic
//! (non-negotiable #4).
//!
//! The command/event sum types that drive these primitives, and the dispatcher
//! that owns a pool of Dogs and applies events through them, land in
//! `hq-mod-dogs.2`. This module deliberately defines neither — it is pure state
//! plus the mutation surface they build on, so no event type is forward-
//! referenced here.

use serde::{Deserialize, Serialize};

/// Stable identifier for a [`Dog`](crate::Dog) worker.
///
/// A non-empty slug (e.g. `sheriff`, `digest`). Kept as a validated newtype so a
/// blank id can never reach the dispatcher; richer validation (charset, length)
/// is added alongside the pool in `hq-mod-dogs.8` if the registry needs it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DogId(String);

/// Why constructing a [`DogId`] was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DogIdError {
    /// The id was empty or whitespace-only.
    Empty,
}

impl std::fmt::Display for DogIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DogIdError::Empty => write!(f, "dog id must not be empty"),
        }
    }
}

impl std::error::Error for DogIdError {}

impl DogId {
    /// Build a [`DogId`], rejecting an empty or whitespace-only slug.
    pub fn new(id: impl Into<String>) -> Result<Self, DogIdError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(DogIdError::Empty);
        }
        Ok(DogId(id))
    }

    /// Borrow the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lifecycle state of a single [`Dog`](crate::Dog).
///
/// A Dog starts [`Idle`](DogStatus::Idle). Claiming a task moves it to
/// [`Claimed`](DogStatus::Claimed); starting execution to
/// [`Running`](DogStatus::Running); finishing or failing returns it to `Idle`,
/// ready for the next claim. [`Retired`](DogStatus::Retired) is terminal — a
/// retired Dog accepts no further transition. Which transitions a command may
/// request is the dispatcher's call (`hq-mod-dogs.2`); this type is the
/// vocabulary, and [`DogState`] enforces the shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DogStatus {
    /// Available: holds no claim, ready to be matched.
    Idle,
    /// Holds a claim but has not started executing it.
    Claimed,
    /// Executing its claim.
    Running,
    /// Terminal: decommissioned, accepts no further work.
    Retired,
}

/// The projected state of one Dog worker.
///
/// Carries the immutable [`id`](DogState::id), the current
/// [`status`](DogState::status), and the [`claim`](DogState::claim) reference it
/// holds while [`Claimed`](DogStatus::Claimed) or [`Running`](DogStatus::Running)
/// (`None` when `Idle`/`Retired`). Pure data — no handles — so it is cheap to
/// clone and safe to snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DogState {
    /// The worker's stable identifier. Immutable for the life of the Dog.
    pub id: DogId,
    /// Current lifecycle state.
    pub status: DogStatus,
    /// The claim reference held while `Claimed`/`Running`; `None` otherwise.
    pub claim: Option<String>,
}

/// Why a [`DogState`] transition was rejected.
///
/// Each variant names the offending starting [`DogStatus`] verbatim so a replay
/// inconsistency surfaces with the source state, not just "illegal".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DogError {
    /// A transition was requested from a state that does not allow it.
    IllegalTransition {
        /// The state the Dog was in.
        from: DogStatus,
        /// The primitive that was attempted.
        action: &'static str,
    },
    /// Any transition was requested on a [`Retired`](DogStatus::Retired) Dog.
    Retired,
}

impl std::fmt::Display for DogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DogError::IllegalTransition { from, action } => {
                write!(f, "cannot {action} a dog in {from:?}")
            }
            DogError::Retired => write!(f, "dog is retired"),
        }
    }
}

impl std::error::Error for DogError {}

impl DogState {
    /// A fresh [`Idle`](DogStatus::Idle) Dog with no claim.
    pub fn new(id: DogId) -> Self {
        DogState { id, status: DogStatus::Idle, claim: None }
    }

    /// Whether the Dog is available to be matched (holds no claim).
    pub fn is_idle(&self) -> bool {
        self.status == DogStatus::Idle
    }

    // --- Reducer primitives -------------------------------------------------

    /// Claim a task: `Idle` → `Claimed`, recording the `claim` reference.
    ///
    /// Rejects from any other state. A retired Dog reports
    /// [`Retired`](DogError::Retired) so the caller can distinguish a terminal
    /// worker from a merely-busy one.
    pub fn claim(&mut self, claim: impl Into<String>) -> Result<(), DogError> {
        self.guard_live("claim")?;
        if self.status != DogStatus::Idle {
            return Err(DogError::IllegalTransition { from: self.status, action: "claim" });
        }
        self.status = DogStatus::Claimed;
        self.claim = Some(claim.into());
        Ok(())
    }

    /// Begin execution: `Claimed` → `Running`. Rejects from any other state.
    pub fn start(&mut self) -> Result<(), DogError> {
        self.guard_live("start")?;
        if self.status != DogStatus::Claimed {
            return Err(DogError::IllegalTransition { from: self.status, action: "start" });
        }
        self.status = DogStatus::Running;
        Ok(())
    }

    /// Complete the claim: `Running` → `Idle`, dropping the claim reference.
    /// Rejects from any other state.
    pub fn finish(&mut self) -> Result<(), DogError> {
        self.guard_live("finish")?;
        if self.status != DogStatus::Running {
            return Err(DogError::IllegalTransition { from: self.status, action: "finish" });
        }
        self.status = DogStatus::Idle;
        self.claim = None;
        Ok(())
    }

    /// Abandon the claim after a failure: `Claimed`/`Running` → `Idle`, dropping
    /// the claim reference. Rejects from `Idle` (nothing to fail).
    pub fn fail(&mut self) -> Result<(), DogError> {
        self.guard_live("fail")?;
        match self.status {
            DogStatus::Claimed | DogStatus::Running => {
                self.status = DogStatus::Idle;
                self.claim = None;
                Ok(())
            }
            from => Err(DogError::IllegalTransition { from, action: "fail" }),
        }
    }

    /// Decommission the Dog: any live state → [`Retired`](DogStatus::Retired),
    /// dropping any held claim. Terminal — a retired Dog rejects every primitive.
    pub fn retire(&mut self) -> Result<(), DogError> {
        self.guard_live("retire")?;
        self.status = DogStatus::Retired;
        self.claim = None;
        Ok(())
    }

    /// Reject any transition on a retired Dog.
    fn guard_live(&self, _action: &'static str) -> Result<(), DogError> {
        if self.status == DogStatus::Retired {
            return Err(DogError::Retired);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dog(id: &str) -> DogState {
        DogState::new(DogId::new(id).unwrap())
    }

    #[test]
    fn id_rejects_empty_and_whitespace() {
        assert_eq!(DogId::new(""), Err(DogIdError::Empty));
        assert_eq!(DogId::new("   "), Err(DogIdError::Empty));
        assert_eq!(DogId::new("sheriff").unwrap().as_str(), "sheriff");
    }

    #[test]
    fn new_dog_is_idle_without_a_claim() {
        let d = dog("sheriff");
        assert_eq!(d.status, DogStatus::Idle);
        assert!(d.is_idle());
        assert_eq!(d.claim, None);
    }

    #[test]
    fn happy_path_claim_start_finish() {
        let mut d = dog("sheriff");
        d.claim("premerge-42").unwrap();
        assert_eq!(d.status, DogStatus::Claimed);
        assert_eq!(d.claim.as_deref(), Some("premerge-42"));
        d.start().unwrap();
        assert_eq!(d.status, DogStatus::Running);
        d.finish().unwrap();
        assert_eq!(d.status, DogStatus::Idle);
        assert_eq!(d.claim, None);
    }

    #[test]
    fn fail_returns_to_idle_from_claimed_or_running() {
        let mut d = dog("sheriff");
        d.claim("x").unwrap();
        d.fail().unwrap();
        assert!(d.is_idle());
        assert_eq!(d.claim, None);

        d.claim("y").unwrap();
        d.start().unwrap();
        d.fail().unwrap();
        assert!(d.is_idle());
    }

    #[test]
    fn illegal_transitions_name_the_source_state() {
        let mut d = dog("sheriff");
        // start before claim
        assert_eq!(
            d.start(),
            Err(DogError::IllegalTransition { from: DogStatus::Idle, action: "start" })
        );
        // finish before running
        d.claim("x").unwrap();
        assert_eq!(
            d.finish(),
            Err(DogError::IllegalTransition { from: DogStatus::Claimed, action: "finish" })
        );
        // double claim
        assert_eq!(
            d.claim("y"),
            Err(DogError::IllegalTransition { from: DogStatus::Claimed, action: "claim" })
        );
        // fail from idle
        let mut idle = dog("other");
        assert_eq!(
            idle.fail(),
            Err(DogError::IllegalTransition { from: DogStatus::Idle, action: "fail" })
        );
    }

    #[test]
    fn retire_is_terminal_and_drops_claim() {
        let mut d = dog("sheriff");
        d.claim("x").unwrap();
        d.retire().unwrap();
        assert_eq!(d.status, DogStatus::Retired);
        assert_eq!(d.claim, None);
        // every primitive now rejects with Retired
        assert_eq!(d.claim("y"), Err(DogError::Retired));
        assert_eq!(d.start(), Err(DogError::Retired));
        assert_eq!(d.finish(), Err(DogError::Retired));
        assert_eq!(d.fail(), Err(DogError::Retired));
        assert_eq!(d.retire(), Err(DogError::Retired));
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&DogStatus::Running).unwrap(), "\"running\"");
    }

    #[test]
    fn state_round_trips_through_serde() {
        let mut d = dog("sheriff");
        d.claim("premerge-42").unwrap();
        d.start().unwrap();
        let json = serde_json::to_string(&d).unwrap();
        let back: DogState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
    }
}
