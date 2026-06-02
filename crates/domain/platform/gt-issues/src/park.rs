//! Park-don't-proceed safety floor for the autonomous loop (hq-auto.3).
//!
//! The autonomous dispatcher (hq-auto.1) runs workers without a per-decision
//! human. This module is the **safety floor** that keeps it from taking grave,
//! hard-to-undo actions while unattended: every operation the loop is about to
//! run is first classified by *reversibility*. Reversible work proceeds; an
//! irreversible op with no human on-loop is **parked** into a pending-approval
//! lane instead of executed, and a human drains that lane on-loop
//! ([`ParkQueue`]).
//!
//! ## Why a domain lane, not a new `IssueStatus`
//!
//! The bead sketch suggests "repurpose hooked status", but the store
//! deliberately keeps the user-facing state machine to `open`/`working`/`closed`
//! (see [`gt_store_dolt::IssueStatus`]) for predictable kanban semantics, and the
//! `hooked` label is owned by the polecat actor. Adding a fourth SQL status would
//! widen the `ENUM` column and the replay surface for what is really an
//! *in-flight operation* concern, not a *bead lifecycle* concern. So the parked
//! lane is modelled here as a pure in-memory queue of pending operations — the
//! dispatcher owns one; nothing about a bead's persisted status changes.
//!
//! The whole module is pure (no async, no store, no clock): the dispatcher feeds
//! it operations and consults the verdict, which keeps the safety floor trivially
//! testable and free of the runtime concerns that live a tier up.

use std::fmt;

/// The closed set of grave operation categories the safety floor refuses to
/// auto-run while no human is on-loop. Named verbatim by hq-auto.3.
///
/// "Irreversible" is the operative bar: each category produces an effect that
/// cannot be cleanly walked back the way a reopened bead or a re-edited field
/// can, so it must wait for a human to authorize it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IrreversibleKind {
    /// Strategic `[FROZEN]` takedown — a bead pulled out of play. Reversing it
    /// means re-litigating a deliberate planning decision, not a status flip.
    Freeze,
    /// Row / branch / data deletion. No undo once the bytes are gone.
    Delete,
    /// Ships to production. The effect is externally visible the instant it lands.
    ProdDeploy,
    /// Token / scope / RBAC change. Blast radius beyond the tracker.
    Security,
    /// Advancing the global phase frontier ([`issues.phase.advance`]). A singleton
    /// that ungates beads tracker-wide; re-ratifying a *lower* phase does not undo
    /// work the higher phase already unblocked.
    ///
    /// [`issues.phase.advance`]: gt_store_dolt::DoltIssues::advance_phase
    PhaseAdvance,
}

impl IrreversibleKind {
    /// The canonical lowercase token (`"freeze"`, `"delete"`, `"prod-deploy"`,
    /// `"security"`, `"phase.advance"`) for logs and the park-lane payload.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Freeze => "freeze",
            Self::Delete => "delete",
            Self::ProdDeploy => "prod-deploy",
            Self::Security => "security",
            Self::PhaseAdvance => "phase.advance",
        }
    }

    /// One-line rationale shown to the human reviewing the park lane, so the
    /// reason an op was held is legible without cross-referencing this code.
    pub fn why(self) -> &'static str {
        match self {
            Self::Freeze => "freezing a bead is a deliberate planning takedown — not a status flip",
            Self::Delete => "deletion destroys data with no undo",
            Self::ProdDeploy => "a production deploy is externally visible the instant it lands",
            Self::Security => "a token/scope/RBAC change has blast radius beyond the tracker",
            Self::PhaseAdvance => {
                "advancing the global phase frontier ungates beads tracker-wide"
            }
        }
    }
}

impl fmt::Display for IrreversibleKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an operation can be cleanly undone after the fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reversibility {
    /// Walk-back-able: a status reopen, a field re-edit, a re-claim. Safe to run
    /// unattended.
    Reversible,
    /// Grave and hard-or-impossible to undo; carries the offending category.
    Irreversible(IrreversibleKind),
}

/// Whether a human is currently on-loop to own a grave call. The dispatcher
/// supplies this; the safety floor uses it to decide whether an irreversible op
/// may run now or must be parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanPresence {
    /// A human is on-loop and owns the decision; grave ops may proceed.
    Present,
    /// Unattended. Grave ops are parked for later review.
    Absent,
}

/// An operation the dispatcher is about to run, tagged with its reversibility and
/// a human-readable summary for the park lane / audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    /// Human-readable one-liner (e.g. `"close hq-x with [FROZEN] prefix"`).
    pub summary: String,
    /// How undoable this op is — the input to the park decision.
    pub reversibility: Reversibility,
}

impl Operation {
    /// A reversible op that always proceeds.
    pub fn reversible(summary: impl Into<String>) -> Self {
        Self { summary: summary.into(), reversibility: Reversibility::Reversible }
    }

    /// A grave op of the given category — parked when no human is on-loop.
    pub fn irreversible(kind: IrreversibleKind, summary: impl Into<String>) -> Self {
        Self { summary: summary.into(), reversibility: Reversibility::Irreversible(kind) }
    }
}

/// The safety-floor verdict for one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkDecision {
    /// Run it now — reversible, or grave-but-a-human-is-on-loop.
    Proceed,
    /// Hold for human approval; carries the grave category to record in the lane.
    Park(IrreversibleKind),
}

/// Decide whether `op` may proceed now or must be parked.
///
/// The rule is the bead sentence verbatim: reversible work proceeds; an
/// irreversible op is parked when no human is present, and proceeds when a human
/// is on-loop to own the call.
pub fn decide(op: &Operation, presence: HumanPresence) -> ParkDecision {
    match (op.reversibility, presence) {
        (Reversibility::Reversible, _) => ParkDecision::Proceed,
        (Reversibility::Irreversible(_), HumanPresence::Present) => ParkDecision::Proceed,
        (Reversibility::Irreversible(kind), HumanPresence::Absent) => ParkDecision::Park(kind),
    }
}

/// A grave operation held in the pending-approval lane, with the ticket id the
/// human uses to approve or reject it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedOp {
    /// Monotonic ticket id, unique within a [`ParkQueue`]'s lifetime. Stable
    /// across approvals of other tickets so a human reference never shifts.
    pub id: u64,
    /// The grave category that caused the park.
    pub kind: IrreversibleKind,
    /// The original operation's human-readable summary.
    pub summary: String,
}

/// The pending-approval lane: grave operations the autonomous loop refused to run
/// unattended, awaiting a human verdict on-loop.
///
/// In-memory and owned by the dispatcher; nothing here persists, because a parked
/// op is an in-flight runtime concern, not a tracked bead. Ticket ids are
/// monotonic so an approve/reject never collides with a later park.
#[derive(Debug, Default)]
pub struct ParkQueue {
    next_id: u64,
    pending: Vec<ParkedOp>,
}

impl ParkQueue {
    /// A fresh, empty lane.
    pub fn new() -> Self {
        Self::default()
    }

    /// Park a grave operation and return its ticket id. The dispatcher calls this
    /// after [`decide`] returns [`ParkDecision::Park`].
    pub fn park(&mut self, kind: IrreversibleKind, summary: impl Into<String>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.pending.push(ParkedOp { id, kind, summary: summary.into() });
        id
    }

    /// The ops awaiting review, oldest first — what the human sees on-loop.
    pub fn pending(&self) -> &[ParkedOp] {
        &self.pending
    }

    /// Number of parked ops awaiting review.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the lane is empty (nothing to review).
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Approve ticket `id`: remove it from the lane and hand it back so the caller
    /// executes the now-authorized op. `None` if the id is unknown (already
    /// drained or never parked) — an idempotent no-op rather than a panic.
    pub fn approve(&mut self, id: u64) -> Option<ParkedOp> {
        self.take(id)
    }

    /// Reject ticket `id`: remove it from the lane and discard it (the human
    /// declined). Returns the dropped op for an audit log, or `None` if unknown.
    pub fn reject(&mut self, id: u64) -> Option<ParkedOp> {
        self.take(id)
    }

    /// Remove the ticket with `id`, preserving the order of the rest.
    fn take(&mut self, id: u64) -> Option<ParkedOp> {
        let pos = self.pending.iter().position(|op| op.id == id)?;
        Some(self.pending.remove(pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reversible_always_proceeds() {
        let op = Operation::reversible("reopen hq-x");
        assert_eq!(decide(&op, HumanPresence::Absent), ParkDecision::Proceed);
        assert_eq!(decide(&op, HumanPresence::Present), ParkDecision::Proceed);
    }

    #[test]
    fn irreversible_parks_only_when_unattended() {
        let op = Operation::irreversible(IrreversibleKind::Delete, "drop ws_foo");
        // No human on-loop → parked.
        assert_eq!(
            decide(&op, HumanPresence::Absent),
            ParkDecision::Park(IrreversibleKind::Delete)
        );
        // Human on-loop owns the call → proceeds.
        assert_eq!(decide(&op, HumanPresence::Present), ParkDecision::Proceed);
    }

    #[test]
    fn every_grave_category_parks_unattended() {
        for kind in [
            IrreversibleKind::Freeze,
            IrreversibleKind::Delete,
            IrreversibleKind::ProdDeploy,
            IrreversibleKind::Security,
            IrreversibleKind::PhaseAdvance,
        ] {
            let op = Operation::irreversible(kind, "grave");
            assert_eq!(decide(&op, HumanPresence::Absent), ParkDecision::Park(kind));
            // Token and rationale are non-empty for the lane payload.
            assert!(!kind.as_str().is_empty());
            assert!(!kind.why().is_empty());
        }
    }

    #[test]
    fn kind_tokens_are_stable() {
        assert_eq!(IrreversibleKind::Freeze.as_str(), "freeze");
        assert_eq!(IrreversibleKind::Delete.as_str(), "delete");
        assert_eq!(IrreversibleKind::ProdDeploy.as_str(), "prod-deploy");
        assert_eq!(IrreversibleKind::Security.as_str(), "security");
        assert_eq!(IrreversibleKind::PhaseAdvance.as_str(), "phase.advance");
        assert_eq!(IrreversibleKind::PhaseAdvance.to_string(), "phase.advance");
    }

    #[test]
    fn queue_parks_and_assigns_monotonic_ids() {
        let mut q = ParkQueue::new();
        assert!(q.is_empty());
        let a = q.park(IrreversibleKind::Freeze, "freeze hq-x");
        let b = q.park(IrreversibleKind::Delete, "drop ws_y");
        assert_eq!((a, b), (0, 1));
        assert_eq!(q.len(), 2);
        assert_eq!(q.pending()[0].summary, "freeze hq-x");
    }

    #[test]
    fn approve_returns_op_for_execution_and_removes_it() {
        let mut q = ParkQueue::new();
        let id = q.park(IrreversibleKind::ProdDeploy, "deploy gt-mcp-server");
        let approved = q.approve(id).expect("ticket present");
        assert_eq!(approved.kind, IrreversibleKind::ProdDeploy);
        assert_eq!(approved.summary, "deploy gt-mcp-server");
        assert!(q.is_empty());
        // Idempotent: a second approve of the same id is a no-op.
        assert_eq!(q.approve(id), None);
    }

    #[test]
    fn reject_drops_op_and_leaves_others() {
        let mut q = ParkQueue::new();
        let keep = q.park(IrreversibleKind::Security, "rotate token");
        let drop = q.park(IrreversibleKind::Delete, "drop ws_z");
        let rejected = q.reject(drop).expect("ticket present");
        assert_eq!(rejected.kind, IrreversibleKind::Delete);
        // The other ticket is untouched and its id still resolves.
        assert_eq!(q.len(), 1);
        assert_eq!(q.pending()[0].id, keep);
        assert_eq!(q.reject(9999), None);
    }
}
