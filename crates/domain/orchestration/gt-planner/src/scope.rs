//! The planner's RBAC scope — distinct from the worker's (hq-auto.2).
//!
//! The autonomous loop has two agent *tiers* with deliberately different
//! authority over the tracker:
//!
//! - **Planner** (this crate, the *expert* tier) decides structure: it may
//!   `create` beads, edit their `depends_on` *edges* (via `issues.update`), and
//!   advance the global `phase` frontier. It must **not** claim or close — it
//!   does not do the work, it shapes it.
//! - **Worker** (the dispatched agent, hq-auto.1) does the work: it may `claim`
//!   (transition open→working) and `close`. It must **not** create beads or
//!   advance the phase — generation is the planner's job, so a worker cannot
//!   widen its own backlog.
//!
//! Separating the two is least-privilege for a self-driving system: neither tier
//! holds the whole pipeline, so a misbehaving worker cannot manufacture work and
//! a misbehaving planner cannot bank deliveries it never did. Both are built on
//! the kernel [`Scope`] allow-list, so [`Scope::check`] enforces them with the
//! same deny-by-default machinery the MCP server already runs.

use std::collections::BTreeSet;

use gt_rbac::Scope;

/// Tool-name glob patterns the **planner** tier is allowed to call: create
/// beads, edit edges (`issues.update`), and advance the phase frontier.
///
/// `phase.advance` is itself an irreversible op (the park floor, hq-auto.3,
/// classifies it [`IrreversibleKind::PhaseAdvance`](gt_issues::IrreversibleKind));
/// being *in scope* means the planner may *initiate* it, not that it bypasses the
/// park queue — an unattended phase advance is still parked for a human.
pub const PLANNER_ALLOW: &[&str] =
    &["issues.create.*", "issues.update.*", "issues.phase.*"];

/// Tool-name glob patterns the **worker** tier is allowed to call: claim
/// (transition) and close. No create, no phase — a worker cannot generate work.
pub const WORKER_ALLOW: &[&str] = &["issues.transition.*", "issues.close.*"];

/// Build the planner [`Scope`] for `actor` (full execute authority over the
/// planner allow-list).
pub fn planner_scope(actor: impl Into<String>) -> Scope {
    scope_from(actor, PLANNER_ALLOW)
}

/// Build the worker [`Scope`] for `actor` (claim + close only).
pub fn worker_scope(actor: impl Into<String>) -> Scope {
    scope_from(actor, WORKER_ALLOW)
}

fn scope_from(actor: impl Into<String>, allow: &[&str]) -> Scope {
    Scope {
        actor: actor.into(),
        allow: allow.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>(),
        validate_only: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_may_create_and_edit_edges_and_phase() {
        let s = planner_scope("planner");
        s.check("issues.create.execute").unwrap();
        s.check("issues.update.execute").unwrap();
        s.check("issues.phase.advance.execute").unwrap();
    }

    #[test]
    fn planner_may_not_claim_or_close() {
        let s = planner_scope("planner");
        assert!(s.check("issues.transition.execute").is_err(), "planner does not do the work");
        assert!(s.check("issues.close.execute").is_err(), "planner does not bank deliveries");
    }

    #[test]
    fn worker_may_claim_and_close() {
        let s = worker_scope("worker");
        s.check("issues.transition.execute").unwrap();
        s.check("issues.close.execute").unwrap();
    }

    #[test]
    fn worker_may_not_create_or_advance_phase() {
        let s = worker_scope("worker");
        assert!(s.check("issues.create.execute").is_err(), "a worker cannot manufacture work");
        assert!(s.check("issues.phase.advance.execute").is_err(), "phase is the planner's lever");
    }

    #[test]
    fn the_two_scopes_are_disjoint() {
        // No tool is grantable to BOTH tiers — the split is total.
        let p = planner_scope("p");
        let w = worker_scope("w");
        for tool in [
            "issues.create.execute",
            "issues.update.execute",
            "issues.phase.advance.execute",
            "issues.transition.execute",
            "issues.close.execute",
        ] {
            assert_ne!(
                p.check(tool).is_ok(),
                w.check(tool).is_ok(),
                "tool {tool} must be in exactly one tier"
            );
        }
    }
}
