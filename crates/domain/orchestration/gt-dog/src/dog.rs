//! [`Dog`] — the worker abstraction + its [`DogId`] (`hq-mod-dogs.1`).
//!
//! A Dog is the thing that picks up a plugin task: it is handed a claim, runs
//! the work for it, and reports back. This module owns the *worker trait* and
//! its identity; the [`DogDispatcher`](crate) that matches plugin gates to Dogs
//! and capacity lands in `hq-mod-dogs.2`, the [`Gate`] evaluator in `.3`, and
//! the `PluginExecutor` that turns a claim into real execution in `.4`. The
//! lifecycle a Dog moves through as it claims and runs is the reducer in
//! [`crate::state`].
//!
//! ## NN#1 boundary
//!
//! Non-negotiable #1 confines `dyn` + `#[async_trait]` to plugin/observer
//! surfaces. A Dog is a heterogeneous, side-effecting worker dispatched at
//! runtime — the same observer-plugin case `gt-hooks` and `gt-plugin` name as
//! the exception. It runs at the I/O edge (claim, execute, emit a receipt),
//! never inside the sync replay core, so NN#2 is preserved: the only thing this
//! crate replays is [`DogState`](crate::DogState), which is pure.

use async_trait::async_trait;

use crate::state::DogId;

/// What a [`Dog`] reports after running a claim.
///
/// `.1` carries the coarse outcome; the rich digest (receipt bead, tracking
/// labels) is added when [`PluginExecutor`](crate) and the tracking surface land
/// in `hq-mod-dogs.4`/`.5`. `#[non_exhaustive]` so later beads can add variants
/// (e.g. `Deferred`) without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DogReport {
    /// The claim ran to completion.
    Completed,
    /// The claim failed, carrying a human-readable reason.
    Failed(String),
}

/// A worker that claims a plugin task and runs it.
///
/// Implementors are held as trait objects by the dispatcher (`hq-mod-dogs.2`);
/// see the NN#1 reconciliation in this module's docs. [`id`](Dog::id) gives a
/// stable identity for dispatch and diagnostics; [`run`](Dog::run) performs the
/// work for a claim and reports a [`DogReport`]. It is `async` because a Dog
/// lives at the I/O edge (spawn an agent, run a script, call a wrapper).
///
/// `.1` defines the trait against a `claim` reference string — the id of the
/// plugin task being run. The structured `Claim` payload (gate snapshot, actor
/// scope, execution type) is threaded through here by `.2`/`.4`; growing the
/// argument is a deliberate later step, so this signature is intentionally thin.
#[async_trait]
pub trait Dog: Send + Sync {
    /// Stable identifier for this worker, used in dispatch and diagnostics.
    fn id(&self) -> &DogId;

    /// Run the work for `claim` and report the outcome.
    async fn run(&self, claim: &str) -> DogReport;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo {
        id: DogId,
    }

    #[async_trait]
    impl Dog for Echo {
        fn id(&self) -> &DogId {
            &self.id
        }
        async fn run(&self, claim: &str) -> DogReport {
            if claim.is_empty() {
                DogReport::Failed("empty claim".to_string())
            } else {
                DogReport::Completed
            }
        }
    }

    #[tokio::test]
    async fn dog_runs_a_claim_to_completion() {
        let dog = Echo { id: DogId::new("sheriff").unwrap() };
        assert_eq!(dog.id().as_str(), "sheriff");
        assert_eq!(dog.run("premerge-42").await, DogReport::Completed);
    }

    #[tokio::test]
    async fn dog_reports_failure_with_reason() {
        let dog = Echo { id: DogId::new("sheriff").unwrap() };
        assert_eq!(dog.run("").await, DogReport::Failed("empty claim".to_string()));
    }
}
