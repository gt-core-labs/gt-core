//! Triage / prioritization — the substrate the mayor's orchestration loop runs on each
//! wake (`gtcore-5c50f0`, child of `gtcore-47522f`).
//!
//! When the orchd wakes the mayor it hands it a **frontier**: the slice of the board the
//! mayor could act on right now (open/ready beads, plus the epics sitting above them). The
//! mayor's job is pure decision-making — given that frontier and the free capacity in the
//! polecat pool, decide:
//!
//! - which beads to **delegate** now (one bead → one polecat), highest priority first,
//! - which ready epics to **decompose** into actionable children first (they have none yet),
//! - and which actionable beads to **queue** because the pool is already full.
//!
//! [`triage`] is that decision as a pure, total function: no clock, no I/O, deterministic
//! for replay and trivially testable — exactly the shape the crate docs promised the loop
//! would call into (`lib.rs`: "the helpers in [`mayor`] are the substrate it will call
//! into"). The async edge (issuing the `Delegate` commands, calling `issues.create` to
//! decompose, emitting feed notes) lives in the producer ([`crate::mayor`]) and the
//! composition root; this module never touches a handle.
//!
//! Invariants the AC pins down:
//! - **P0 first.** Lower `priority` number wins (0 = P0); ties break by frontier order
//!   (stable), so a deterministic, dependency-respecting input order is honored.
//! - **Capacity is a hard ceiling.** `delegate.len() <= capacity`; everything actionable
//!   beyond the cap lands in `queue`, never dropped.
//! - **Blocked work is invisible.** A bead whose deps/locks are not satisfied
//!   (`ready == false`) is neither delegated nor queued — it is not the mayor's to move yet.
//! - **Idle is free.** An empty (or all-blocked) frontier yields an empty plan; the loop
//!   does nothing and burns no tokens until the next orchd wake.

/// What kind of frontier entry this is. The mayor delegates `Actionable` beads to polecats;
/// an `Epic` is a container — it is never delegated directly. A *ready* epic that still has
/// no actionable children is decomposed first (the agent calls `issues.create`); once it has
/// children, those children show up in the frontier as their own `Actionable` entries and the
/// epic itself is inert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierKind {
    /// A concrete, executable bead — the unit a single polecat takes end to end.
    Actionable,
    /// A container/epic. Decomposed (not delegated) when ready without actionable children.
    Epic,
}

/// One entry of the frontier the orchd hands the mayor on wake. Deliberately self-contained
/// (no `gt-beads`/`gt-issues` coupling): the loop projects whatever board snapshot it has into
/// this shape, and the triage stays a pure function over plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontierBead {
    /// Bead id — the handoff id used by `Delegate` and the `issues.create` of a decomposition.
    pub id: String,
    /// Free-form title/summary, echoed into the `Delegate` command so the audit log and the
    /// polecat prompt carry the work description without a second lookup.
    pub summary: String,
    /// Queue priority: `0` = P0 (highest). Lower wins.
    pub priority: u8,
    /// Actionable bead vs. epic container.
    pub kind: FrontierKind,
    /// Whether deps and locks are satisfied — i.e. the mayor may act on it now. A blocked
    /// bead (`false`) is skipped entirely until a later wake clears its blockers.
    pub ready: bool,
    /// For an `Epic`: whether it already has at least one actionable child. Ignored for
    /// `Actionable` entries. A ready epic with `false` here is surfaced for decomposition.
    pub has_actionable_children: bool,
}

impl FrontierBead {
    /// Construct an actionable, ready frontier bead — the common case in tests and callers.
    pub fn actionable(id: impl Into<String>, summary: impl Into<String>, priority: u8) -> Self {
        Self {
            id: id.into(),
            summary: summary.into(),
            priority,
            kind: FrontierKind::Actionable,
            ready: true,
            has_actionable_children: false,
        }
    }

    /// Construct a ready epic with the given child-availability flag.
    pub fn epic(id: impl Into<String>, summary: impl Into<String>, has_children: bool) -> Self {
        Self {
            id: id.into(),
            summary: summary.into(),
            priority: 1,
            kind: FrontierKind::Epic,
            ready: true,
            has_actionable_children: has_children,
        }
    }

    /// Mark this bead blocked (deps/locks unmet) — builder for tests/callers.
    pub fn blocked(mut self) -> Self {
        self.ready = false;
        self
    }

    /// Override the priority — builder for tests/callers.
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// One delegation the mayor will hand to a polecat this tick. Carries the id + summary the
/// producer feeds into a `Delegate` command (`target` is filled in by the producer — it is
/// always a polecat, "one bead → one polecat").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedDelegation {
    pub id: String,
    pub summary: String,
    pub priority: u8,
}

/// The decision the triage produced for one wake. Three disjoint buckets over the frontier:
/// what to delegate now, what to queue (ready but over capacity), and which ready epics to
/// decompose first. Blocked beads appear in none of them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TriagePlan {
    /// Beads to delegate this tick, P0-first, length `<= capacity`. One polecat each.
    pub delegate: Vec<PlannedDelegation>,
    /// Ready actionable beads that did not fit under the cap, in the same priority order.
    /// They wait for the next wake (when slots free up) — never dropped.
    pub queue: Vec<PlannedDelegation>,
    /// Ready epics with no actionable children yet: decompose (`issues.create`) before they
    /// can be delegated. Surfaced in priority order.
    pub decompose: Vec<PlannedDelegation>,
}

impl TriagePlan {
    /// Nothing actionable on the frontier — the loop idles until the next orchd wake and
    /// spends no tokens. The "no quema tokens en idle" AC reduces to this being `true`.
    pub fn is_idle(&self) -> bool {
        self.delegate.is_empty() && self.queue.is_empty() && self.decompose.is_empty()
    }
}

/// Decide what to delegate, queue, and decompose for one wake.
///
/// `capacity` is the number of free polecat slots the mayor may fill right now (the pool /
/// host-cap minus what is already in flight). It is a hard ceiling: at most `capacity` beads
/// are delegated; the rest of the ready, actionable work is queued in priority order.
///
/// Pure and total — same frontier + capacity always yields the same plan.
pub fn triage(frontier: &[FrontierBead], capacity: usize) -> TriagePlan {
    // Stable priority order: P0 first (lowest number), ties keep frontier order. `enumerate`
    // before sort makes the tie-break explicit and the sort total.
    let mut ordered: Vec<(usize, &FrontierBead)> = frontier.iter().enumerate().collect();
    ordered.sort_by_key(|(idx, b)| (b.priority, *idx));

    let mut plan = TriagePlan::default();
    for (_, bead) in ordered {
        // Blocked work (unmet deps/locks) is not the mayor's to move yet.
        if !bead.ready {
            continue;
        }
        let planned = PlannedDelegation {
            id: bead.id.clone(),
            summary: bead.summary.clone(),
            priority: bead.priority,
        };
        match bead.kind {
            FrontierKind::Epic => {
                // A ready epic is only the mayor's concern when it has no actionable children:
                // it must be broken down first. With children, the children are the actionable
                // entries — the epic itself is inert.
                if !bead.has_actionable_children {
                    plan.decompose.push(planned);
                }
            }
            FrontierKind::Actionable => {
                // Fill the pool to the cap; queue the surplus, preserving priority order.
                if plan.delegate.len() < capacity {
                    plan.delegate.push(planned);
                } else {
                    plan.queue.push(planned);
                }
            }
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC: on wake with N ready beads, the mayor delegates N up to the cap. With capacity to
    /// spare, every actionable ready bead is delegated and nothing is queued.
    #[test]
    fn delegates_all_ready_when_capacity_spare() {
        let frontier = vec![
            FrontierBead::actionable("a", "first", 1),
            FrontierBead::actionable("b", "second", 1),
            FrontierBead::actionable("c", "third", 1),
        ];
        let plan = triage(&frontier, 4);
        assert_eq!(plan.delegate.len(), 3);
        assert!(plan.queue.is_empty());
        assert!(plan.decompose.is_empty());
        assert!(!plan.is_idle());
    }

    /// AC: prioritize P0. A P0 bead arriving after lower-priority beads is delegated first.
    #[test]
    fn p0_delegated_first() {
        let frontier = vec![
            FrontierBead::actionable("p2", "low", 2),
            FrontierBead::actionable("p1", "mid", 1),
            FrontierBead::actionable("p0", "top", 0),
        ];
        let plan = triage(&frontier, 3);
        let ids: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, ["p0", "p1", "p2"]);
    }

    /// Ties at the same priority keep frontier (arrival/dependency) order — deterministic.
    #[test]
    fn equal_priority_keeps_frontier_order() {
        let frontier = vec![
            FrontierBead::actionable("first", "x", 1),
            FrontierBead::actionable("second", "y", 1),
            FrontierBead::actionable("third", "z", 1),
        ];
        let plan = triage(&frontier, 3);
        let ids: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, ["first", "second", "third"]);
    }

    /// AC: do not exceed pool/host_cap; queue the surplus. With 4 ready beads and cap 2, two
    /// are delegated (the two highest priority) and two are queued — none dropped.
    #[test]
    fn caps_delegations_and_queues_surplus() {
        let frontier = vec![
            FrontierBead::actionable("p1a", "a", 1),
            FrontierBead::actionable("p0", "b", 0),
            FrontierBead::actionable("p1b", "c", 1),
            FrontierBead::actionable("p2", "d", 2),
        ];
        let plan = triage(&frontier, 2);
        let del: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
        let q: Vec<&str> = plan.queue.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(del, ["p0", "p1a"], "highest priority fills the two slots");
        assert_eq!(q, ["p1b", "p2"], "surplus queued in priority order");
        assert_eq!(plan.delegate.len() + plan.queue.len(), 4, "nothing dropped");
    }

    /// Capacity 0 (pool full): nothing is delegated, all ready work queues. Not idle —
    /// there is queued work waiting for a free slot.
    #[test]
    fn zero_capacity_queues_everything() {
        let frontier = vec![
            FrontierBead::actionable("a", "x", 0),
            FrontierBead::actionable("b", "y", 1),
        ];
        let plan = triage(&frontier, 0);
        assert!(plan.delegate.is_empty());
        assert_eq!(plan.queue.len(), 2);
        assert!(!plan.is_idle());
    }

    /// AC: a ready epic with no actionable children is surfaced for decomposition, not
    /// delegated. It must not consume a pool slot.
    #[test]
    fn ready_epic_without_children_is_decomposed_not_delegated() {
        let frontier = vec![
            FrontierBead::epic("epic-1", "umbrella", false),
            FrontierBead::actionable("task-1", "real work", 1),
        ];
        let plan = triage(&frontier, 4);
        let del: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
        let dec: Vec<&str> = plan.decompose.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(del, ["task-1"], "only the actionable bead is delegated");
        assert_eq!(dec, ["epic-1"], "the childless epic is queued for decomposition");
    }

    /// An epic that already has actionable children is inert: its children are the actionable
    /// frontier entries, so the epic is neither delegated nor decomposed.
    #[test]
    fn epic_with_children_is_inert() {
        let frontier = vec![
            FrontierBead::epic("epic-1", "umbrella", true),
            FrontierBead::actionable("child-1", "child work", 1),
        ];
        let plan = triage(&frontier, 4);
        assert_eq!(plan.delegate.len(), 1);
        assert_eq!(plan.delegate[0].id, "child-1");
        assert!(plan.decompose.is_empty());
    }

    /// AC: blocked beads (unmet deps/locks) are skipped entirely — neither delegated nor
    /// queued — until a later wake clears the blocker.
    #[test]
    fn blocked_beads_are_skipped() {
        let frontier = vec![
            FrontierBead::actionable("ready", "go", 1),
            FrontierBead::actionable("blocked", "wait", 0).blocked(),
        ];
        let plan = triage(&frontier, 4);
        let del: Vec<&str> = plan.delegate.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(del, ["ready"], "the blocked P0 bead is not delegated");
        assert!(plan.queue.is_empty());
    }

    /// AC: no tokens burned in idle. An empty frontier — or one with only blocked work —
    /// yields an idle plan, so the loop does nothing until the next orchd wake.
    #[test]
    fn empty_or_blocked_frontier_is_idle() {
        assert!(triage(&[], 8).is_idle(), "empty frontier is idle");

        let all_blocked = vec![
            FrontierBead::actionable("a", "x", 0).blocked(),
            FrontierBead::actionable("b", "y", 1).blocked(),
        ];
        assert!(
            triage(&all_blocked, 8).is_idle(),
            "all-blocked frontier is idle"
        );
    }
}
