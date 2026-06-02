//! Per-workspace polecat pool sizing + host capacity admission (`hq-mt-runtime.1` follow-up
//! `hq-mt-runtime.2`).
//!
//! Each workspace gets a configured `pool_size` (from `workspaces.config`); on one host the
//! sum of every workspace's live polecats must also stay within a global cap (≈ host CPU), so
//! no single tenant — and no combination of tenants — can oversubscribe the box. [`PoolAllocator`]
//! is the **pure** admission-control core: it counts live claims per workspace and in total, and
//! answers whether the next claim fits. It performs no I/O and spawns nothing — the supervisor
//! at the edge calls [`claim`](PoolAllocator::claim) before launching a polecat and
//! [`release`](PoolAllocator::release) when one exits.
//!
//! "Rebalancing" on reconfigure is admission-side, not eviction: [`set_pool_size`] can shrink a
//! workspace below its current live count; [`overflow`](PoolAllocator::overflow) reports the
//! excess so the supervisor can drain it. Evicting a running polecat is side-effecting and stays
//! at the edge (NN#2) — this core only accounts.

use std::collections::BTreeMap;

/// Which budget a [`claim`](PoolAllocator::claim) hit when it was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExhaustedScope {
    /// The workspace is already at its own configured `pool_size`.
    WorkspacePool,
    /// The host-wide cap (sum across all workspaces) is reached.
    Host,
}

impl std::fmt::Display for ExhaustedScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExhaustedScope::WorkspacePool => write!(f, "workspace pool"),
            ExhaustedScope::Host => write!(f, "host"),
        }
    }
}

/// Why a claim could not be admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AllocError {
    /// No capacity left in the named scope; the polecat must not be launched.
    ResourceExhausted {
        /// The workspace the claim was for.
        workspace: String,
        /// Which budget was saturated.
        scope: ExhaustedScope,
    },
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocError::ResourceExhausted { workspace, scope } => {
                write!(f, "resource exhausted: {scope} cap reached for workspace `{workspace}`")
            }
        }
    }
}

impl std::error::Error for AllocError {}

/// Pure admission controller for per-workspace polecat pools under a global host cap.
///
/// `host_cap` bounds the sum of live polecats across all workspaces. Each workspace has a
/// configured `pool_size`; an unconfigured workspace falls back to `default_pool_size`. The
/// workspace key is a plain `String` slug, so the lifecycle tier needs no dependency on the
/// platform `gt-workspace` type — the composition root passes the slug.
#[derive(Clone, Debug)]
pub struct PoolAllocator {
    host_cap: usize,
    default_pool_size: usize,
    pool_size: BTreeMap<String, usize>,
    in_flight: BTreeMap<String, usize>,
}

impl PoolAllocator {
    /// A new allocator with the given host-wide cap and the default per-workspace pool size
    /// applied to any workspace without an explicit [`set_pool_size`].
    pub fn new(host_cap: usize, default_pool_size: usize) -> Self {
        PoolAllocator {
            host_cap,
            default_pool_size,
            pool_size: BTreeMap::new(),
            in_flight: BTreeMap::new(),
        }
    }

    /// Builder-style: set a workspace's configured pool size. Chainable.
    pub fn with_pool_size(mut self, workspace: impl Into<String>, size: usize) -> Self {
        self.pool_size.insert(workspace.into(), size);
        self
    }

    /// Set (or update) a workspace's configured pool size. Shrinking below the live count is
    /// allowed — see [`overflow`](Self::overflow) — so a reconfigure can rebalance down.
    pub fn set_pool_size(&mut self, workspace: impl Into<String>, size: usize) {
        self.pool_size.insert(workspace.into(), size);
    }

    /// The configured pool size for `workspace` (its explicit value, else the default).
    pub fn pool_size(&self, workspace: &str) -> usize {
        self.pool_size
            .get(workspace)
            .copied()
            .unwrap_or(self.default_pool_size)
    }

    /// Live polecats currently counted for `workspace`.
    pub fn in_flight(&self, workspace: &str) -> usize {
        self.in_flight.get(workspace).copied().unwrap_or(0)
    }

    /// Live polecats across every workspace.
    pub fn total_in_flight(&self) -> usize {
        self.in_flight.values().sum()
    }

    /// Admit a new polecat for `workspace`, incrementing its live count on success.
    ///
    /// Refused with [`AllocError::ResourceExhausted`] if the workspace is already at its
    /// `pool_size` ([`ExhaustedScope::WorkspacePool`]) or the host is at `host_cap`
    /// ([`ExhaustedScope::Host`]); the workspace check precedes the host check so a tenant that
    /// blew its own budget is told so rather than blaming the shared host. State is untouched on
    /// refusal.
    pub fn claim(&mut self, workspace: &str) -> Result<(), AllocError> {
        if self.in_flight(workspace) >= self.pool_size(workspace) {
            return Err(AllocError::ResourceExhausted {
                workspace: workspace.to_string(),
                scope: ExhaustedScope::WorkspacePool,
            });
        }
        if self.total_in_flight() >= self.host_cap {
            return Err(AllocError::ResourceExhausted {
                workspace: workspace.to_string(),
                scope: ExhaustedScope::Host,
            });
        }
        *self.in_flight.entry(workspace.to_string()).or_insert(0) += 1;
        Ok(())
    }

    /// Whether a [`claim`](Self::claim) for `workspace` would be admitted right now.
    pub fn can_claim(&self, workspace: &str) -> bool {
        self.in_flight(workspace) < self.pool_size(workspace)
            && self.total_in_flight() < self.host_cap
    }

    /// Release one live polecat for `workspace` (saturating at 0). Idempotent at the floor.
    pub fn release(&mut self, workspace: &str) {
        if let Some(n) = self.in_flight.get_mut(workspace) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.in_flight.remove(workspace);
            }
        }
    }

    /// How many live polecats exceed `workspace`'s current `pool_size` (0 unless a reconfigure
    /// shrank the pool below the live count). The supervisor drains this many at the edge.
    pub fn overflow(&self, workspace: &str) -> usize {
        self.in_flight(workspace).saturating_sub(self.pool_size(workspace))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_up_to_the_workspace_pool_size_then_refuses() {
        let mut alloc = PoolAllocator::new(100, 1).with_pool_size("acme", 2);
        assert!(alloc.claim("acme").is_ok());
        assert!(alloc.claim("acme").is_ok());
        assert_eq!(alloc.in_flight("acme"), 2);
        assert_eq!(
            alloc.claim("acme"),
            Err(AllocError::ResourceExhausted {
                workspace: "acme".into(),
                scope: ExhaustedScope::WorkspacePool,
            })
        );
    }

    #[test]
    fn host_cap_bounds_the_sum_across_workspaces() {
        // Generous per-ws pools, but the host only fits 2 total.
        let mut alloc = PoolAllocator::new(2, 10);
        assert!(alloc.claim("a").is_ok());
        assert!(alloc.claim("b").is_ok());
        assert_eq!(alloc.total_in_flight(), 2);
        // A third claim from a fresh workspace, well under its own pool, still hits the host cap.
        assert_eq!(
            alloc.claim("c"),
            Err(AllocError::ResourceExhausted {
                workspace: "c".into(),
                scope: ExhaustedScope::Host,
            })
        );
    }

    #[test]
    fn workspace_check_precedes_host_check() {
        // host has room (cap 5), but acme's own pool is full at 1.
        let mut alloc = PoolAllocator::new(5, 1);
        assert!(alloc.claim("acme").is_ok());
        match alloc.claim("acme") {
            Err(AllocError::ResourceExhausted { scope, .. }) => {
                assert_eq!(scope, ExhaustedScope::WorkspacePool)
            }
            other => panic!("expected WorkspacePool exhaustion, got {other:?}"),
        }
    }

    #[test]
    fn release_frees_capacity() {
        let mut alloc = PoolAllocator::new(1, 1);
        assert!(alloc.claim("acme").is_ok());
        assert!(!alloc.can_claim("acme"));
        alloc.release("acme");
        assert_eq!(alloc.in_flight("acme"), 0);
        assert!(alloc.can_claim("acme"));
    }

    #[test]
    fn unconfigured_workspace_uses_default_pool_size() {
        let mut alloc = PoolAllocator::new(100, 3);
        assert_eq!(alloc.pool_size("novel"), 3);
        for _ in 0..3 {
            assert!(alloc.claim("novel").is_ok());
        }
        assert!(alloc.claim("novel").is_err());
    }

    #[test]
    fn shrinking_the_pool_reports_overflow_to_drain() {
        let mut alloc = PoolAllocator::new(100, 5).with_pool_size("acme", 4);
        for _ in 0..4 {
            assert!(alloc.claim("acme").is_ok());
        }
        assert_eq!(alloc.overflow("acme"), 0);
        // Reconfigure down to 2 → two live polecats now exceed the pool.
        alloc.set_pool_size("acme", 2);
        assert_eq!(alloc.overflow("acme"), 2);
        // ...and no new claim is admitted until the drain brings it back under.
        assert!(!alloc.can_claim("acme"));
    }

    #[test]
    fn release_at_floor_is_idempotent() {
        let mut alloc = PoolAllocator::new(10, 10);
        alloc.release("never-claimed");
        assert_eq!(alloc.in_flight("never-claimed"), 0);
    }
}
