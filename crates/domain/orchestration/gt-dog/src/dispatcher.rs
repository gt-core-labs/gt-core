//! [`DogEvent`] + [`DogDispatcher`] — the pool that matches ready claims to
//! idle Dogs within capacity (`hq-mod-dogs.2`).
//!
//! `hq-mod-dogs.1` left two things to this bead, named in its module docs: the
//! **event sum type** that drives the [`DogState`] reducer primitives, and the
//! **dispatcher** that owns a pool of Dogs and folds events through them. Both
//! live here.
//!
//! ## Why this is the sync core, not the I/O edge
//!
//! The dispatcher is pure: it owns [`DogState`] projections (data, no handles),
//! and every mutation goes through a [`DogEvent`] applied via the reducer
//! primitives. Same pool + same event sequence always yields the same pool, so
//! it replays byte-for-byte (non-negotiable #4) and stays in the sync core
//! (non-negotiable #2). The *async* side — actually running a claim on a
//! [`Dog`](crate::Dog) — is [`PluginExecutor`](crate) in `hq-mod-dogs.4`; it
//! drives this state machine by emitting `Started`/`Finished`/`Failed` events,
//! but performs no I/O here.
//!
//! ## Capacity
//!
//! Matching is gated by a capacity budget — the in-repo stand-in for the
//! `gt-quota` capacity signal (`gt-quota` migrates up in Phase 4; until then the
//! dispatcher takes a plain in-flight ceiling). A Dog is *in flight* while it
//! holds a claim ([`Claimed`](DogStatus::Claimed) or
//! [`Running`](DogStatus::Running)); [`dispatch`](DogDispatcher::dispatch)
//! assigns a new claim only while the in-flight count is below capacity, so the
//! pool never runs more concurrent work than the budget allows even when idle
//! Dogs remain.

use std::collections::BTreeMap;

use crate::state::{DogError, DogId, DogState, DogStatus};

/// A lifecycle event for one Dog — the replayable record that drives the
/// [`DogState`] reducer primitives.
///
/// Each variant maps to exactly one primitive
/// ([`claim`](DogState::claim), [`start`](DogState::start),
/// [`finish`](DogState::finish), [`fail`](DogState::fail),
/// [`retire`](DogState::retire)). Folding a Dog's event stream through
/// [`DogDispatcher::apply`] rebuilds its state deterministically.
/// `#[non_exhaustive]` so later beads can add variants without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DogEvent {
    /// The Dog took a claim (`Idle` → `Claimed`).
    Claimed {
        /// Which Dog took the claim.
        dog: DogId,
        /// The claim reference taken.
        claim: String,
    },
    /// The Dog began executing its claim (`Claimed` → `Running`).
    Started {
        /// Which Dog started.
        dog: DogId,
    },
    /// The Dog completed its claim (`Running` → `Idle`).
    Finished {
        /// Which Dog finished.
        dog: DogId,
    },
    /// The Dog abandoned its claim after a failure (`Claimed`/`Running` → `Idle`).
    Failed {
        /// Which Dog failed.
        dog: DogId,
    },
    /// The Dog was decommissioned (any live state → `Retired`).
    Retired {
        /// Which Dog retired.
        dog: DogId,
    },
}

impl DogEvent {
    /// The Dog this event targets.
    pub fn dog(&self) -> &DogId {
        match self {
            DogEvent::Claimed { dog, .. }
            | DogEvent::Started { dog }
            | DogEvent::Finished { dog }
            | DogEvent::Failed { dog }
            | DogEvent::Retired { dog } => dog,
        }
    }
}

/// Why applying a [`DogEvent`] to the pool was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchError {
    /// The event named a Dog the dispatcher does not hold.
    UnknownDog {
        /// The id that was not registered.
        dog: DogId,
    },
    /// The event was illegal for the Dog's current state; carries the underlying
    /// [`DogState`] rejection verbatim.
    Rejected {
        /// The Dog the transition was attempted on.
        dog: DogId,
        /// The reducer-primitive rejection.
        source: DogError,
    },
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::UnknownDog { dog } => write!(f, "unknown dog `{dog}`"),
            DispatchError::Rejected { dog, source } => write!(f, "dog `{dog}`: {source}"),
        }
    }
}

impl std::error::Error for DispatchError {}

/// The result of a [`dispatch`](DogDispatcher::dispatch) attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Dispatch {
    /// The claim was assigned to this Dog (now `Claimed`).
    Assigned(DogId),
    /// No assignment: the in-flight count is at the capacity budget.
    AtCapacity,
    /// No assignment: every Dog is busy or retired, none is idle.
    NoIdleDog,
}

/// Owns a pool of [`DogState`]s and matches ready claims to idle Dogs within a
/// capacity budget.
///
/// Construct with [`with_capacity`](DogDispatcher::with_capacity),
/// [`register`](DogDispatcher::register) the workers, then drive it with
/// [`dispatch`](DogDispatcher::dispatch) (assign a claim) and
/// [`apply`](DogDispatcher::apply) (fold a lifecycle event). The pool is keyed
/// by [`DogId`] in a [`BTreeMap`], so iteration — and therefore which idle Dog a
/// dispatch picks — is deterministic for replay.
#[derive(Clone, Debug)]
pub struct DogDispatcher {
    dogs: BTreeMap<DogId, DogState>,
    capacity: usize,
}

impl DogDispatcher {
    /// A dispatcher with no Dogs and the given in-flight capacity budget.
    ///
    /// `capacity` is the maximum number of Dogs that may hold a claim
    /// concurrently. `0` parks every dispatch at [`Dispatch::AtCapacity`].
    pub fn with_capacity(capacity: usize) -> Self {
        DogDispatcher {
            dogs: BTreeMap::new(),
            capacity,
        }
    }

    /// Register a fresh [`Idle`](DogStatus::Idle) Dog.
    ///
    /// Returns `true` if it was added, `false` if a Dog with that id was already
    /// present (left untouched — registration never resets a live worker).
    pub fn register(&mut self, id: DogId) -> bool {
        if self.dogs.contains_key(&id) {
            return false;
        }
        self.dogs.insert(id.clone(), DogState::new(id));
        true
    }

    /// The capacity budget — the maximum number of concurrently claimed Dogs.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many Dogs currently hold a claim (`Claimed` or `Running`).
    pub fn in_flight(&self) -> usize {
        self.dogs
            .values()
            .filter(|d| matches!(d.status, DogStatus::Claimed | DogStatus::Running))
            .count()
    }

    /// Whether another claim may be assigned under the capacity budget.
    pub fn has_capacity(&self) -> bool {
        self.in_flight() < self.capacity
    }

    /// Borrow a Dog's state by id.
    pub fn dog(&self, id: &DogId) -> Option<&DogState> {
        self.dogs.get(id)
    }

    /// The number of registered Dogs (any state).
    pub fn len(&self) -> usize {
        self.dogs.len()
    }

    /// Whether the pool holds no Dogs.
    pub fn is_empty(&self) -> bool {
        self.dogs.is_empty()
    }

    /// Match `claim` to an idle Dog and assign it, subject to capacity.
    ///
    /// Returns [`Dispatch::AtCapacity`] without touching state if the in-flight
    /// count is already at the budget; otherwise picks the first idle Dog in id
    /// order, applies a [`DogEvent::Claimed`], and returns
    /// [`Dispatch::Assigned`]. If no Dog is idle, returns
    /// [`Dispatch::NoIdleDog`]. The capacity check precedes the idle scan so a
    /// saturated pool reports `AtCapacity` rather than masking it as
    /// `NoIdleDog`.
    pub fn dispatch(&mut self, claim: impl Into<String>) -> Dispatch {
        if !self.has_capacity() {
            return Dispatch::AtCapacity;
        }
        // First idle Dog in deterministic (id-sorted) order.
        let Some(id) = self
            .dogs
            .iter()
            .find(|(_, d)| d.is_idle())
            .map(|(id, _)| id.clone())
        else {
            return Dispatch::NoIdleDog;
        };
        // The chosen Dog is Idle, so `claim` cannot fail; surface any future
        // invariant break loudly rather than silently dropping the assignment.
        self.dogs
            .get_mut(&id)
            .expect("idle dog just located by id")
            .claim(claim)
            .expect("claiming an idle dog never fails");
        Dispatch::Assigned(id)
    }

    /// Fold a [`DogEvent`] into the pool, driving the named Dog's reducer.
    ///
    /// Errors with [`DispatchError::UnknownDog`] if the event names an
    /// unregistered Dog, or [`DispatchError::Rejected`] if the transition is
    /// illegal for that Dog's current state.
    pub fn apply(&mut self, event: &DogEvent) -> Result<(), DispatchError> {
        let id = event.dog().clone();
        let dog = self
            .dogs
            .get_mut(&id)
            .ok_or(DispatchError::UnknownDog { dog: id.clone() })?;
        let result = match event {
            DogEvent::Claimed { claim, .. } => dog.claim(claim.clone()),
            DogEvent::Started { .. } => dog.start(),
            DogEvent::Finished { .. } => dog.finish(),
            DogEvent::Failed { .. } => dog.fail(),
            DogEvent::Retired { .. } => dog.retire(),
        };
        result.map_err(|source| DispatchError::Rejected { dog: id, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> DogId {
        DogId::new(s).unwrap()
    }

    fn pool(capacity: usize, dogs: &[&str]) -> DogDispatcher {
        let mut d = DogDispatcher::with_capacity(capacity);
        for name in dogs {
            assert!(d.register(id(name)));
        }
        d
    }

    #[test]
    fn register_is_idempotent_and_does_not_reset() {
        let mut d = pool(2, &["sheriff"]);
        // Move sheriff out of Idle, then re-register: state must be preserved.
        assert_eq!(d.dispatch("x"), Dispatch::Assigned(id("sheriff")));
        assert!(!d.register(id("sheriff")));
        assert_eq!(d.dog(&id("sheriff")).unwrap().status, DogStatus::Claimed);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn dispatch_assigns_the_first_idle_dog_in_id_order() {
        let mut d = pool(4, &["witness", "sheriff", "digest"]);
        // Sorted ids: digest < sheriff < witness → digest first.
        assert_eq!(d.dispatch("a"), Dispatch::Assigned(id("digest")));
        assert_eq!(d.dispatch("b"), Dispatch::Assigned(id("sheriff")));
        assert_eq!(d.dispatch("c"), Dispatch::Assigned(id("witness")));
    }

    #[test]
    fn dispatch_records_the_claim_on_the_assigned_dog() {
        let mut d = pool(1, &["sheriff"]);
        assert_eq!(d.dispatch("premerge-42"), Dispatch::Assigned(id("sheriff")));
        let dog = d.dog(&id("sheriff")).unwrap();
        assert_eq!(dog.status, DogStatus::Claimed);
        assert_eq!(dog.claim.as_deref(), Some("premerge-42"));
    }

    #[test]
    fn dispatch_reports_no_idle_dog_when_all_busy() {
        let mut d = pool(4, &["sheriff"]);
        assert_eq!(d.dispatch("a"), Dispatch::Assigned(id("sheriff")));
        // Capacity (4) is fine, but the only Dog is now Claimed.
        assert_eq!(d.dispatch("b"), Dispatch::NoIdleDog);
    }

    #[test]
    fn dispatch_respects_the_capacity_budget() {
        let mut d = pool(1, &["sheriff", "digest"]);
        assert_eq!(d.dispatch("a"), Dispatch::Assigned(id("digest")));
        assert_eq!(d.in_flight(), 1);
        // digest is in flight and capacity is 1, so even though `sheriff` is idle
        // the budget blocks a second assignment.
        assert_eq!(d.dispatch("b"), Dispatch::AtCapacity);
        assert_eq!(d.dog(&id("sheriff")).unwrap().status, DogStatus::Idle);
    }

    #[test]
    fn zero_capacity_parks_every_dispatch() {
        let mut d = pool(0, &["sheriff"]);
        assert_eq!(d.dispatch("a"), Dispatch::AtCapacity);
        assert!(!d.has_capacity());
    }

    #[test]
    fn finishing_frees_capacity_for_the_next_claim() {
        let mut d = pool(1, &["sheriff", "digest"]);
        assert_eq!(d.dispatch("a"), Dispatch::Assigned(id("digest")));
        assert_eq!(d.dispatch("b"), Dispatch::AtCapacity);
        // Run digest to completion: claim → start → finish.
        d.apply(&DogEvent::Started { dog: id("digest") }).unwrap();
        d.apply(&DogEvent::Finished { dog: id("digest") }).unwrap();
        assert_eq!(d.in_flight(), 0);
        // Budget freed → next dispatch lands (digest is idle again, sorts first).
        assert_eq!(d.dispatch("c"), Dispatch::Assigned(id("digest")));
    }

    #[test]
    fn apply_drives_the_full_lifecycle() {
        let mut d = pool(1, &["sheriff"]);
        d.apply(&DogEvent::Claimed { dog: id("sheriff"), claim: "x".into() }).unwrap();
        assert_eq!(d.dog(&id("sheriff")).unwrap().status, DogStatus::Claimed);
        d.apply(&DogEvent::Started { dog: id("sheriff") }).unwrap();
        assert_eq!(d.dog(&id("sheriff")).unwrap().status, DogStatus::Running);
        d.apply(&DogEvent::Finished { dog: id("sheriff") }).unwrap();
        assert!(d.dog(&id("sheriff")).unwrap().is_idle());
    }

    #[test]
    fn apply_to_unknown_dog_is_rejected() {
        let mut d = pool(1, &["sheriff"]);
        let err = d
            .apply(&DogEvent::Started { dog: id("ghost") })
            .unwrap_err();
        assert_eq!(err, DispatchError::UnknownDog { dog: id("ghost") });
    }

    #[test]
    fn apply_illegal_transition_carries_the_reducer_error() {
        let mut d = pool(1, &["sheriff"]);
        // Start before claim → the DogState rejection surfaces verbatim.
        let err = d
            .apply(&DogEvent::Started { dog: id("sheriff") })
            .unwrap_err();
        assert_eq!(
            err,
            DispatchError::Rejected {
                dog: id("sheriff"),
                source: DogError::IllegalTransition {
                    from: DogStatus::Idle,
                    action: "start"
                }
            }
        );
    }

    #[test]
    fn failed_event_returns_dog_to_idle_and_frees_capacity() {
        let mut d = pool(1, &["sheriff"]);
        assert_eq!(d.dispatch("x"), Dispatch::Assigned(id("sheriff")));
        d.apply(&DogEvent::Failed { dog: id("sheriff") }).unwrap();
        assert!(d.dog(&id("sheriff")).unwrap().is_idle());
        assert_eq!(d.in_flight(), 0);
    }

    #[test]
    fn retired_dog_is_not_idle_and_never_dispatched() {
        let mut d = pool(4, &["sheriff"]);
        d.apply(&DogEvent::Retired { dog: id("sheriff") }).unwrap();
        assert_eq!(d.dog(&id("sheriff")).unwrap().status, DogStatus::Retired);
        // Retired Dogs are not in flight and not idle.
        assert_eq!(d.in_flight(), 0);
        assert_eq!(d.dispatch("x"), Dispatch::NoIdleDog);
    }

    #[test]
    fn event_dog_accessor_names_the_target() {
        assert_eq!(
            DogEvent::Claimed { dog: id("a"), claim: "c".into() }.dog(),
            &id("a")
        );
        assert_eq!(DogEvent::Retired { dog: id("b") }.dog(), &id("b"));
    }
}
