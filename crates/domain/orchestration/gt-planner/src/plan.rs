//! Decomposition core: a goal's intent → beads + intent edges + phase, as pure
//! graph structure (hq-auto.2).
//!
//! The planner is the autonomous *expert* tier. Its output is **structure, never
//! prose**: a set of [`CreateBead`] operations carrying ids, titles, phase, and
//! `depends_on` edges. It supplies only the **intent edges** a human/expert knows
//! (semantic "this needs that first"); the mechanical Cargo-graph edges are left
//! for the reconciler to auto-derive (§A, hq-core-mcp.12), so the planner never
//! hand-encodes what the build graph already implies.
//!
//! Two invariants the decomposition enforces:
//!
//! - **NN-16 taxonomy** — every bead id is `<external_ref>.<n>`, numbered from the
//!   epic's next free index, so the output drops straight into the
//!   `epic → sub-epic → bead` hierarchy.
//! - **Park irreversible forks** — an intent classified irreversible
//!   ([`gt_issues::park`]) is *not* auto-emitted when no human is on-loop; it is
//!   routed to the park queue ([`ParkedFork`]) for review, exactly as the
//!   dispatcher's safety floor (hq-auto.3) parks a grave op. The planner reuses
//!   that classifier rather than forking its own.
//!
//! Pure and synchronous: [`decompose`] is a total function of its inputs,
//! unit-tested without a store or a model. Applying the plan (calling
//! `issues.create` under the [`planner_scope`](crate::planner_scope)) is the
//! caller's job a tier up.

use gt_issues::park::{decide, HumanPresence, IrreversibleKind, Operation, ParkDecision, Reversibility};
use gt_memory::{Corpus, MemoryKind};

/// One intended unit of work the planner was asked to realize, before it is
/// assigned a bead id. Edges are **intent edges**: indices of the other intents
/// in the same [`Goal`] this one depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub title: String,
    /// Optional phase tag (e.g. `"P1"`, `"P4"`); `None` leaves it unset.
    pub phase: Option<String>,
    /// How undoable realizing this intent is — drives the park decision.
    pub reversibility: Reversibility,
    /// Intent edges: indices into [`Goal::intents`] this intent depends on.
    pub edges: Vec<usize>,
}

impl Intent {
    /// A reversible task with no phase and no edges.
    pub fn task(title: impl Into<String>) -> Self {
        Intent {
            title: title.into(),
            phase: None,
            reversibility: Reversibility::Reversible,
            edges: Vec::new(),
        }
    }

    /// A grave intent of `kind` — parked when no human is on-loop.
    pub fn irreversible(kind: IrreversibleKind, title: impl Into<String>) -> Self {
        Intent {
            title: title.into(),
            phase: None,
            reversibility: Reversibility::Irreversible(kind),
            edges: Vec::new(),
        }
    }

    /// Builder: tag the phase.
    pub fn with_phase(mut self, phase: impl Into<String>) -> Self {
        self.phase = Some(phase.into());
        self
    }

    /// Builder: add an intent edge to the intent at `index` in the goal.
    pub fn needs(mut self, index: usize) -> Self {
        self.edges.push(index);
        self
    }
}

/// The goal/epic intent to decompose. Bead ids are minted as
/// `<external_ref>.<n>` starting at [`next_index`](Goal::next_index), so the plan
/// composes with an epic that already has beads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Goal {
    /// The sub-epic id that is the beads' `external_ref` (NN-16).
    pub external_ref: String,
    /// First free bead number under the epic; the nth created bead is
    /// `<external_ref>.<next_index + n>` (0-based n).
    pub next_index: usize,
    pub intents: Vec<Intent>,
}

impl Goal {
    /// A goal whose beads number from `.1` (a fresh sub-epic).
    pub fn new(external_ref: impl Into<String>, intents: Vec<Intent>) -> Self {
        Goal { external_ref: external_ref.into(), next_index: 1, intents }
    }

    /// A goal whose beads number from `.start` (an epic with existing beads).
    pub fn from_index(external_ref: impl Into<String>, start: usize, intents: Vec<Intent>) -> Self {
        Goal { external_ref: external_ref.into(), next_index: start, intents }
    }
}

/// A bead the planner decided to create — pure structure, no description/notes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBead {
    /// `<external_ref>.<n>` (NN-16).
    pub id: String,
    pub external_ref: String,
    pub title: String,
    /// Always `"task"` for now; epics/sub-epics are authored, not auto-planned.
    pub issue_type: String,
    pub phase: Option<String>,
    /// Resolved intent edges as bead ids. Edges to a parked (un-created) intent
    /// are dropped — you cannot depend on work that was never scheduled.
    pub depends_on: Vec<String>,
    /// The planner actor, recorded so authorship is audited (`created_by`).
    pub created_by: String,
}

/// An irreversible fork the planner refused to auto-schedule; held for a human to
/// approve on-loop before it becomes a bead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedFork {
    pub title: String,
    pub kind: IrreversibleKind,
}

/// The decomposition result: beads to create, forks parked for review, and the
/// author of record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub create: Vec<CreateBead>,
    pub parked: Vec<ParkedFork>,
    pub author: String,
}

impl Plan {
    /// Number of beads this plan would create.
    pub fn created_len(&self) -> usize {
        self.create.len()
    }

    /// Number of forks held in the park lane.
    pub fn parked_len(&self) -> usize {
        self.parked.len()
    }
}

/// Decompose `goal` into a [`Plan`] authored by `author`, with the park floor
/// gated on `presence`.
///
/// Reversible intents (and irreversible ones when a human is on-loop) become
/// beads, numbered `<external_ref>.<next_index..>` in goal order. Irreversible
/// intents with no human present are parked instead. Intent edges are resolved to
/// bead ids in a second pass (so an edge may point forward); an edge to a parked
/// intent is dropped.
pub fn decompose(goal: &Goal, author: &str, presence: HumanPresence) -> Plan {
    // Pass 1: classify each intent and, for those that proceed, mint a bead id.
    // `id_of[i]` is Some when intent i becomes a bead.
    let mut id_of: Vec<Option<String>> = vec![None; goal.intents.len()];
    let mut parked = Vec::new();
    let mut seq = goal.next_index;
    for (i, intent) in goal.intents.iter().enumerate() {
        let op = Operation {
            summary: intent.title.clone(),
            reversibility: intent.reversibility,
        };
        match decide(&op, presence) {
            ParkDecision::Proceed => {
                id_of[i] = Some(format!("{}.{}", goal.external_ref, seq));
                seq += 1;
            }
            ParkDecision::Park(kind) => {
                parked.push(ParkedFork { title: intent.title.clone(), kind });
            }
        }
    }

    // Pass 2: build the create-ops, resolving intent edges to bead ids now that
    // every created intent has one.
    let mut create = Vec::new();
    for (i, intent) in goal.intents.iter().enumerate() {
        let Some(id) = &id_of[i] else { continue };
        let depends_on: Vec<String> = intent
            .edges
            .iter()
            .filter_map(|&j| id_of.get(j).and_then(|o| o.clone()))
            .collect();
        create.push(CreateBead {
            id: id.clone(),
            external_ref: goal.external_ref.clone(),
            title: intent.title.clone(),
            issue_type: "task".to_string(),
            phase: intent.phase.clone(),
            depends_on,
            created_by: author.to_string(),
        });
    }

    Plan { create, parked, author: author.to_string() }
}

/// The reminders the planner loads from the memory corpus **before** planning
/// (hq-auto.8): the always-on operating rules plus the goal-relevant recall.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanningBrief {
    /// `feedback` memories — hard operating rules, always loaded in full.
    pub rules: Vec<String>,
    /// Relevance-filtered memories (any kind) bearing on the goal text.
    pub recalled: Vec<String>,
}

/// Build the planning brief: every operating rule, plus the `limit` memories most
/// relevant to `query` (the goal/epic text). RECALL is relevance-gated so the
/// planner pulls only what bears on the task; operating rules are not gated,
/// because a rule you fail to recall is a rule you violate.
///
/// STALENESS GUARD: these memories reflect what was true when written. A planner
/// acting on a recalled memory that names a file/flag must verify it against the
/// live graph first (the corpus contract, surfaced on [`Corpus`]).
pub fn planning_brief(corpus: &Corpus, query: &str, limit: usize) -> PlanningBrief {
    let line = |m: &gt_memory::Memory| format!("{}: {}", m.name, m.description);
    let rules: Vec<String> = corpus.operating_rules().iter().map(|m| line(m)).collect();
    let recalled: Vec<String> = corpus
        .recall(query, limit)
        .into_iter()
        // Don't echo a feedback memory already listed as a rule.
        .filter(|m| m.kind != MemoryKind::Feedback)
        .map(line)
        .collect();
    PlanningBrief { rules, recalled }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_memory::Memory;

    fn present() -> HumanPresence {
        HumanPresence::Present
    }
    fn absent() -> HumanPresence {
        HumanPresence::Absent
    }

    #[test]
    fn numbers_beads_nn16_from_next_index() {
        let goal = Goal::new("hq-foo", vec![Intent::task("a"), Intent::task("b")]);
        let plan = decompose(&goal, "planner", absent());
        let ids: Vec<&str> = plan.create.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["hq-foo.1", "hq-foo.2"]);
        assert!(plan.create.iter().all(|c| c.external_ref == "hq-foo" && c.issue_type == "task"));
        assert_eq!(plan.author, "planner");
    }

    #[test]
    fn honours_a_nonzero_start_index() {
        let goal = Goal::from_index("hq-bar", 5, vec![Intent::task("x")]);
        let plan = decompose(&goal, "p", absent());
        assert_eq!(plan.create[0].id, "hq-bar.5");
    }

    #[test]
    fn resolves_intent_edges_to_bead_ids() {
        // intent 1 depends on intent 0.
        let goal = Goal::new(
            "hq-foo",
            vec![Intent::task("base"), Intent::task("uses base").needs(0)],
        );
        let plan = decompose(&goal, "p", absent());
        let dep = &plan.create[1];
        assert_eq!(dep.id, "hq-foo.2");
        assert_eq!(dep.depends_on, vec!["hq-foo.1".to_string()], "edge resolved to the base bead id");
        assert!(plan.create[0].depends_on.is_empty());
    }

    #[test]
    fn forward_edges_resolve() {
        // intent 0 depends on intent 1 (declared later) — second pass resolves it.
        let goal = Goal::new(
            "hq-foo",
            vec![Intent::task("first").needs(1), Intent::task("second")],
        );
        let plan = decompose(&goal, "p", absent());
        assert_eq!(plan.create[0].depends_on, vec!["hq-foo.2".to_string()]);
    }

    #[test]
    fn carries_phase_through() {
        let goal = Goal::new("hq-foo", vec![Intent::task("a").with_phase("P4")]);
        let plan = decompose(&goal, "p", absent());
        assert_eq!(plan.create[0].phase.as_deref(), Some("P4"));
    }

    #[test]
    fn irreversible_intent_is_parked_when_unattended() {
        let goal = Goal::new(
            "hq-foo",
            vec![
                Intent::task("safe work"),
                Intent::irreversible(IrreversibleKind::ProdDeploy, "ship it"),
            ],
        );
        let plan = decompose(&goal, "p", absent());
        assert_eq!(plan.created_len(), 1, "only the reversible intent becomes a bead");
        assert_eq!(plan.create[0].title, "safe work");
        assert_eq!(plan.parked_len(), 1);
        assert_eq!(plan.parked[0].kind, IrreversibleKind::ProdDeploy);
        // The parked intent never consumed a bead id: numbering skips it.
        assert_eq!(plan.create[0].id, "hq-foo.1");
    }

    #[test]
    fn irreversible_intent_proceeds_when_human_present() {
        let goal = Goal::new(
            "hq-foo",
            vec![Intent::irreversible(IrreversibleKind::Freeze, "freeze hq-x")],
        );
        let plan = decompose(&goal, "p", present());
        assert_eq!(plan.created_len(), 1, "a human on-loop owns the grave call");
        assert!(plan.parked.is_empty());
    }

    #[test]
    fn edge_to_a_parked_intent_is_dropped() {
        // intent 1 depends on intent 0, which gets parked → its edge has no target.
        let goal = Goal::new(
            "hq-foo",
            vec![
                Intent::irreversible(IrreversibleKind::Delete, "drop schema"),
                Intent::task("rebuild").needs(0),
            ],
        );
        let plan = decompose(&goal, "p", absent());
        assert_eq!(plan.created_len(), 1);
        let rebuild = &plan.create[0];
        assert_eq!(rebuild.title, "rebuild");
        assert!(rebuild.depends_on.is_empty(), "edge to the parked intent is dropped");
        assert_eq!(rebuild.id, "hq-foo.1", "parked intent consumed no id");
    }

    fn mem(name: &str, desc: &str, kind: MemoryKind) -> Memory {
        Memory { name: name.into(), description: desc.into(), kind, body: String::new() }
    }

    #[test]
    fn planning_brief_loads_rules_and_relevant_recall() {
        let corpus = Corpus::from_memories(vec![
            mem("verify-build-before-commit", "cargo test before merge", MemoryKind::Feedback),
            mem("hq-mt-data", "PG schema-per-workspace partitioning", MemoryKind::Project),
            mem("unrelated", "kubernetes ingress tuning", MemoryKind::Reference),
        ]);
        let brief = planning_brief(&corpus, "workspace partitioning schema", 5);
        // Operating rule always present.
        assert_eq!(brief.rules.len(), 1);
        assert!(brief.rules[0].starts_with("verify-build-before-commit:"));
        // Relevant project memory recalled; the unrelated one is not.
        assert_eq!(brief.recalled.len(), 1);
        assert!(brief.recalled[0].starts_with("hq-mt-data:"));
    }

    #[test]
    fn planning_brief_does_not_echo_rules_in_recall() {
        let corpus = Corpus::from_memories(vec![mem(
            "verify-build-before-commit",
            "cargo test build gate",
            MemoryKind::Feedback,
        )]);
        // Even though the query matches the feedback memory, recall excludes it
        // (it is already a rule), so it is not double-counted.
        let brief = planning_brief(&corpus, "cargo build test", 5);
        assert_eq!(brief.rules.len(), 1);
        assert!(brief.recalled.is_empty());
    }
}
