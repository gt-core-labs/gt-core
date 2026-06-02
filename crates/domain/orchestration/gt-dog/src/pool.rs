//! Per-workspace Dog pool under a host-wide cap (`hq-mod-dogs.8`).
//!
//! [`crate::DogDispatcher`] already matches claims to idle Dogs within a single capacity budget.
//! In a multi-tenant host each workspace gets its **own** dispatcher (its own Dogs + its own
//! per-workspace budget), and on top of that the sum of in-flight Dogs across every workspace
//! must stay within a global host cap — the same shape as the polecat budget allocator
//! (`hq-mt-runtime.2`), so a workspace's Dog pool can be sized from its polecat budget.
//!
//! [`WorkspaceDogPool`] owns one [`DogDispatcher`] per workspace and consults the global cap
//! **before** delegating a dispatch to the workspace's pool. The host cap is a plain number
//! (the composition root derives it from the polecat budget), so this orchestration crate takes
//! no dependency on the lifecycle `gt-polecat` allocator. The workspace key is a `String` slug.

use std::collections::BTreeMap;

use crate::dispatcher::{Dispatch, DispatchError, DogDispatcher, DogEvent};
use crate::state::{DogId, DogState};

/// A pool of [`DogDispatcher`]s keyed by workspace, bounded by a global host cap on the total
/// number of in-flight Dogs across all workspaces.
#[derive(Clone, Debug)]
pub struct WorkspaceDogPool {
    host_cap: usize,
    pools: BTreeMap<String, DogDispatcher>,
}

impl WorkspaceDogPool {
    /// A new pool with the given host-wide in-flight cap and no workspaces yet.
    pub fn new(host_cap: usize) -> Self {
        WorkspaceDogPool {
            host_cap,
            pools: BTreeMap::new(),
        }
    }

    /// Ensure a workspace has a dispatcher with the given per-workspace capacity, returning it
    /// mutably. Idempotent: an existing workspace pool is left untouched (its capacity and live
    /// Dogs are preserved) and returned as-is.
    pub fn ensure_workspace(&mut self, workspace: impl Into<String>, capacity: usize) -> &mut DogDispatcher {
        self.pools
            .entry(workspace.into())
            .or_insert_with(|| DogDispatcher::with_capacity(capacity))
    }

    /// Register a fresh idle Dog into `workspace` (creating the pool at `capacity` if absent).
    /// Returns `true` if the Dog was added, `false` if that id was already present.
    pub fn register(&mut self, workspace: impl Into<String>, id: DogId, capacity: usize) -> bool {
        self.ensure_workspace(workspace, capacity).register(id)
    }

    /// Borrow a workspace's dispatcher, if it exists.
    pub fn workspace(&self, workspace: &str) -> Option<&DogDispatcher> {
        self.pools.get(workspace)
    }

    /// The host-wide cap on concurrently-claimed Dogs across all workspaces.
    pub fn host_cap(&self) -> usize {
        self.host_cap
    }

    /// In-flight (claimed or running) Dogs in one workspace.
    pub fn in_flight(&self, workspace: &str) -> usize {
        self.pools.get(workspace).map_or(0, DogDispatcher::in_flight)
    }

    /// In-flight Dogs summed across every workspace — what the host cap bounds.
    pub fn total_in_flight(&self) -> usize {
        self.pools.values().map(DogDispatcher::in_flight).sum()
    }

    /// Whether another claim could be admitted under the host cap.
    pub fn has_host_capacity(&self) -> bool {
        self.total_in_flight() < self.host_cap
    }

    /// Match `claim` to an idle Dog in `workspace`, subject to **both** the host cap and the
    /// workspace's own budget.
    ///
    /// The host cap is checked first: a saturated host reports [`Dispatch::AtCapacity`] before
    /// the workspace pool is touched. An unknown workspace (none registered) yields
    /// [`Dispatch::NoIdleDog`]. Otherwise the workspace dispatcher decides (its own capacity +
    /// idle scan), so the result is `Assigned` / `AtCapacity` / `NoIdleDog` exactly as a
    /// single-pool dispatch.
    pub fn dispatch(&mut self, workspace: &str, claim: impl Into<String>) -> Dispatch {
        if !self.has_host_capacity() {
            return Dispatch::AtCapacity;
        }
        match self.pools.get_mut(workspace) {
            Some(pool) => pool.dispatch(claim),
            None => Dispatch::NoIdleDog,
        }
    }

    /// Fold a lifecycle [`DogEvent`] into `workspace`'s dispatcher.
    ///
    /// Errors with [`DispatchError::UnknownDog`] if the workspace has no pool or the event names
    /// an unregistered Dog, mirroring the single-pool [`DogDispatcher::apply`].
    pub fn apply(&mut self, workspace: &str, event: &DogEvent) -> Result<(), DispatchError> {
        match self.pools.get_mut(workspace) {
            Some(pool) => pool.apply(event),
            None => Err(DispatchError::UnknownDog {
                dog: event.dog().clone(),
            }),
        }
    }

    /// Borrow a Dog's state by workspace + id.
    pub fn dog(&self, workspace: &str, id: &DogId) -> Option<&DogState> {
        self.pools.get(workspace).and_then(|p| p.dog(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> DogId {
        DogId::new(s).unwrap()
    }

    #[test]
    fn dispatch_is_bounded_by_the_workspace_pool() {
        let mut pool = WorkspaceDogPool::new(100);
        pool.register("acme", id("sheriff"), 1);
        pool.register("acme", id("witness"), 1); // capacity is the pool's, set at creation = 1
        // acme pool capacity is 1 → first dispatch assigns, second is AtCapacity.
        assert!(matches!(pool.dispatch("acme", "a"), Dispatch::Assigned(_)));
        assert_eq!(pool.dispatch("acme", "b"), Dispatch::AtCapacity);
    }

    #[test]
    fn host_cap_bounds_the_sum_across_workspaces() {
        let mut pool = WorkspaceDogPool::new(2);
        pool.register("a", id("dog-a"), 10);
        pool.register("b", id("dog-b"), 10);
        pool.register("c", id("dog-c"), 10);
        assert!(matches!(pool.dispatch("a", "x"), Dispatch::Assigned(_)));
        assert!(matches!(pool.dispatch("b", "y"), Dispatch::Assigned(_)));
        assert_eq!(pool.total_in_flight(), 2);
        // host cap reached → a claim in a workspace with idle Dogs is still refused.
        assert_eq!(pool.dispatch("c", "z"), Dispatch::AtCapacity);
    }

    #[test]
    fn unknown_workspace_has_no_idle_dog() {
        let mut pool = WorkspaceDogPool::new(10);
        assert_eq!(pool.dispatch("ghost", "x"), Dispatch::NoIdleDog);
    }

    #[test]
    fn finishing_frees_workspace_and_host_capacity() {
        let mut pool = WorkspaceDogPool::new(1);
        pool.register("acme", id("sheriff"), 1);
        let assigned = pool.dispatch("acme", "claim-1");
        assert!(matches!(assigned, Dispatch::Assigned(_)));
        assert_eq!(pool.total_in_flight(), 1);
        // host is full now.
        pool.register("other", id("dog"), 1);
        assert_eq!(pool.dispatch("other", "claim-2"), Dispatch::AtCapacity);
        // run the acme Dog to completion → Idle, capacity returns.
        pool.apply("acme", &DogEvent::Started { dog: id("sheriff") }).unwrap();
        pool.apply("acme", &DogEvent::Finished { dog: id("sheriff") }).unwrap();
        assert_eq!(pool.total_in_flight(), 0);
        assert!(matches!(pool.dispatch("other", "claim-2"), Dispatch::Assigned(_)));
    }

    #[test]
    fn ensure_workspace_is_idempotent() {
        let mut pool = WorkspaceDogPool::new(10);
        pool.register("acme", id("sheriff"), 2);
        pool.dispatch("acme", "x"); // sheriff now Claimed
        // re-ensuring must not reset the existing pool.
        pool.ensure_workspace("acme", 99);
        assert_eq!(pool.workspace("acme").unwrap().capacity(), 2);
        assert_eq!(pool.in_flight("acme"), 1);
    }

    #[test]
    fn apply_to_unknown_workspace_errors() {
        let mut pool = WorkspaceDogPool::new(10);
        let err = pool.apply("ghost", &DogEvent::Started { dog: id("x") });
        assert!(matches!(err, Err(DispatchError::UnknownDog { .. })));
    }
}
