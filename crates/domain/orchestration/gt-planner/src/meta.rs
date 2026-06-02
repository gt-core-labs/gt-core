//! Self-improvement meta-loop (hq-auto.7) — process-level, not weight-level.
//!
//! Closes the learning loop the rest of the autonomous stack opens. The planner
//! (hq-auto.2) decides structure; the dispatcher runs it; the reconciler's
//! fitness scorecard (hq-auto.6) measures the outcome. This module feeds those
//! outcomes back: it reads how a past planning cycle actually turned out and
//! **writes the lesson into the memory corpus** (hq-auto.8). Because the planner
//! reads its operating rules from that same corpus before planning
//! ([`planning_brief`](crate::planning_brief)), a lesson recorded here changes
//! the *next* decomposition — the adjustment is the corpus, not a tuned weight.
//! Each closed gap + saved lesson improves the next cycle.
//!
//! The feedback substrate is the graph history itself: each bead a plan created
//! has a realized fate ([`Outcome`]) derivable from its status + the fitness
//! signals. This module is pure over those outcomes — no store, no clock, no
//! model — so the reflection is deterministic and unit-tested; the host supplies
//! the outcomes (read from the tracker) and persists the lessons
//! ([`Memory::to_markdown`]).

use gt_memory::{Corpus, Memory, MemoryKind};

/// The realized fate of one bead a past plan created. Mirrors the correctness
/// vocabulary of the fitness scorecard (hq-auto.6): a delivery banked, or one of
/// the three defect signals the loop must learn to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Closed with a verifiable `delivered_sha` — work banked. The good case.
    Delivered,
    /// Reopened or superseded after closing — churn that signals the bead was
    /// mis-scoped or closed too early.
    Reworked,
    /// Closed but its delivery cannot be verified (no resolvable sha) — work
    /// *claimed* but not banked. A "trap" in fitness terms.
    Trap,
    /// Was actually ready but an unsound dependency graph hid it from the
    /// frontier, so it sat un-dispatched.
    FalseHidden,
}

impl Outcome {
    /// Whether this outcome is a defect the meta-loop should learn from (anything
    /// but a clean [`Delivered`](Outcome::Delivered)).
    pub fn is_defect(self) -> bool {
        !matches!(self, Outcome::Delivered)
    }

    /// Stable token used in a lesson's slug.
    fn token(self) -> &'static str {
        match self {
            Outcome::Delivered => "delivered",
            Outcome::Reworked => "reworked",
            Outcome::Trap => "trap",
            Outcome::FalseHidden => "false-hidden",
        }
    }

    /// The corrective operating rule this defect implies — the lesson body.
    fn corrective(self) -> &'static str {
        match self {
            Outcome::Delivered => "",
            Outcome::Reworked => {
                "Decompose into smaller, independently-verifiable beads and verify before \
                 closing; reopen/supersede churn means the bead was mis-scoped or closed early."
            }
            Outcome::Trap => {
                "Never close a bead without the delivering commit_sha; a close with no \
                 verifiable delivery banks work that was only claimed."
            }
            Outcome::FalseHidden => {
                "Supply only precise intent edges and let the reconciler auto-derive mechanical \
                 Cargo edges; an over- or mis-specified depends_on hides ready beads from the frontier."
            }
        }
    }
}

/// The realized fate of one bead the cycle's plan created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadOutcome {
    pub bead_id: String,
    pub outcome: Outcome,
}

impl BeadOutcome {
    pub fn new(bead_id: impl Into<String>, outcome: Outcome) -> Self {
        BeadOutcome { bead_id: bead_id.into(), outcome }
    }
}

/// One completed planning cycle's results, keyed to the goal (`external_ref`) it
/// realized — the unit the meta-loop reflects on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cycle {
    pub external_ref: String,
    pub outcomes: Vec<BeadOutcome>,
}

impl Cycle {
    pub fn new(external_ref: impl Into<String>, outcomes: Vec<BeadOutcome>) -> Self {
        Cycle { external_ref: external_ref.into(), outcomes }
    }

    /// Beads that banked a verifiable delivery.
    pub fn delivered(&self) -> usize {
        self.outcomes.iter().filter(|o| o.outcome == Outcome::Delivered).count()
    }

    /// Beads with any defect outcome.
    pub fn defects(&self) -> usize {
        self.outcomes.iter().filter(|o| o.outcome.is_defect()).count()
    }
}

/// What the meta-loop learned from a cycle: the lessons to write back, and
/// whether the cycle was defect-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reflection {
    /// Lessons to upsert into the corpus, best-grouped one per defect kind seen.
    pub lessons: Vec<Memory>,
    /// True when the cycle had no defects (nothing corrective to learn).
    pub clean: bool,
}

/// Reflect on a finished `cycle`: group its defects by kind and mint one
/// corrective **operating rule** ([`MemoryKind::Feedback`]) per kind observed,
/// naming the offending beads. A defect-free cycle yields one **project** memory
/// recording the clean delivery (state, not a rule).
///
/// Deterministic: lessons are ordered by the fixed defect-kind order and each
/// carries a stable `name` slug, so re-reflecting the same cycle upserts the
/// same memories (idempotent write-back) rather than accreting duplicates.
pub fn reflect(cycle: &Cycle) -> Reflection {
    // Fixed order so the lesson list is deterministic across runs.
    const DEFECT_KINDS: [Outcome; 3] = [Outcome::Trap, Outcome::Reworked, Outcome::FalseHidden];

    let mut lessons = Vec::new();
    for kind in DEFECT_KINDS {
        let hit: Vec<&str> = cycle
            .outcomes
            .iter()
            .filter(|o| o.outcome == kind)
            .map(|o| o.bead_id.as_str())
            .collect();
        if hit.is_empty() {
            continue;
        }
        lessons.push(Memory {
            name: format!("lesson-{}-{}", cycle.external_ref, kind.token()),
            description: format!(
                "{}: {} bead(s) {} in {} — {}",
                cycle.external_ref,
                hit.len(),
                kind.token(),
                cycle.external_ref,
                short_desc(kind),
            ),
            kind: MemoryKind::Feedback,
            body: format!("Beads: {}.\n\n{}", hit.join(", "), kind.corrective()),
        });
    }

    let clean = lessons.is_empty();
    if clean && cycle.delivered() > 0 {
        // No defects: record the win as project state so the next cycle sees the
        // pattern that worked, without minting a (non-existent) corrective rule.
        lessons.push(Memory {
            name: format!("win-{}", cycle.external_ref),
            description: format!(
                "{}: {} bead(s) delivered cleanly, no rework/trap/false-hidden",
                cycle.external_ref,
                cycle.delivered(),
            ),
            kind: MemoryKind::Project,
            body: format!(
                "Cycle {} closed with {} clean deliveries. The decomposition that produced it is \
                 a known-good pattern to repeat.",
                cycle.external_ref,
                cycle.delivered(),
            ),
        });
    }

    Reflection { lessons, clean }
}

/// Reflect on `cycle` and **write back** its lessons into `corpus`
/// ([`Corpus::upsert`], so re-learning a cycle replaces rather than duplicates),
/// returning the reflection. This is the seam that closes the loop: the upserted
/// `Feedback` lessons become operating rules the next
/// [`planning_brief`](crate::planning_brief) surfaces to the planner.
///
/// The corpus is in-memory; persisting it to the corpus directory (via
/// [`Memory::to_markdown`]) is the host's job.
pub fn learn(corpus: &mut Corpus, cycle: &Cycle) -> Reflection {
    let reflection = reflect(cycle);
    for lesson in &reflection.lessons {
        corpus.upsert(lesson.clone());
    }
    reflection
}

/// A one-line consequence phrase per defect kind, for the lesson description.
fn short_desc(kind: Outcome) -> &'static str {
    match kind {
        Outcome::Trap => "closed without verifiable delivery",
        Outcome::Reworked => "reopened/superseded after closing",
        Outcome::FalseHidden => "ready but hidden by an unsound graph",
        Outcome::Delivered => "delivered",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cycle(ref_: &str, outs: &[(&str, Outcome)]) -> Cycle {
        Cycle::new(ref_, outs.iter().map(|(id, o)| BeadOutcome::new(*id, *o)).collect())
    }

    #[test]
    fn clean_cycle_records_a_win_not_a_rule() {
        let c = cycle("hq-foo", &[("hq-foo.1", Outcome::Delivered), ("hq-foo.2", Outcome::Delivered)]);
        let r = reflect(&c);
        assert!(r.clean);
        assert_eq!(r.lessons.len(), 1);
        assert_eq!(r.lessons[0].kind, MemoryKind::Project, "a win is state, not an operating rule");
        assert_eq!(r.lessons[0].name, "win-hq-foo");
    }

    #[test]
    fn a_trap_mints_a_feedback_rule_naming_the_bead() {
        let c = cycle("hq-bar", &[("hq-bar.1", Outcome::Trap), ("hq-bar.2", Outcome::Delivered)]);
        let r = reflect(&c);
        assert!(!r.clean);
        assert_eq!(r.lessons.len(), 1);
        let l = &r.lessons[0];
        assert_eq!(l.kind, MemoryKind::Feedback, "a defect is a hard operating rule");
        assert_eq!(l.name, "lesson-hq-bar-trap");
        assert!(l.body.contains("hq-bar.1"), "offending bead named in the body");
        assert!(l.body.contains("commit_sha"), "carries the corrective guidance");
    }

    #[test]
    fn one_lesson_per_defect_kind_in_fixed_order() {
        let c = cycle(
            "hq-x",
            &[
                ("hq-x.1", Outcome::FalseHidden),
                ("hq-x.2", Outcome::Trap),
                ("hq-x.3", Outcome::Reworked),
                ("hq-x.4", Outcome::Delivered),
            ],
        );
        let r = reflect(&c);
        // Trap, Reworked, FalseHidden — the fixed DEFECT_KINDS order, not input order.
        let names: Vec<&str> = r.lessons.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["lesson-hq-x-trap", "lesson-hq-x-reworked", "lesson-hq-x-false-hidden"]);
        // No win memory when there were defects.
        assert!(r.lessons.iter().all(|m| m.kind == MemoryKind::Feedback));
    }

    #[test]
    fn multiple_beads_same_defect_collapse_to_one_lesson() {
        let c = cycle("hq-y", &[("hq-y.1", Outcome::Reworked), ("hq-y.2", Outcome::Reworked)]);
        let r = reflect(&c);
        assert_eq!(r.lessons.len(), 1);
        assert!(r.lessons[0].body.contains("hq-y.1"));
        assert!(r.lessons[0].body.contains("hq-y.2"));
        assert!(r.lessons[0].description.contains("2 bead(s)"));
    }

    #[test]
    fn empty_cycle_is_clean_with_no_lessons() {
        let r = reflect(&cycle("hq-empty", &[]));
        assert!(r.clean);
        assert!(r.lessons.is_empty(), "nothing delivered and no defects => nothing to record");
    }

    #[test]
    fn learn_writes_lessons_into_corpus_as_operating_rules() {
        let mut corpus = Corpus::default();
        let c = cycle("hq-bar", &[("hq-bar.1", Outcome::Trap)]);
        let r = learn(&mut corpus, &c);
        assert_eq!(r.lessons.len(), 1);
        // The lesson is now an operating rule the next planning_brief will surface.
        let rules: Vec<&str> = corpus.operating_rules().iter().map(|m| m.name.as_str()).collect();
        assert_eq!(rules, vec!["lesson-hq-bar-trap"]);
    }

    #[test]
    fn re_learning_is_idempotent_not_duplicative() {
        let mut corpus = Corpus::default();
        let c = cycle("hq-bar", &[("hq-bar.1", Outcome::Trap)]);
        learn(&mut corpus, &c);
        learn(&mut corpus, &c);
        // Stable name => upsert replaces, the corpus holds one copy.
        assert_eq!(corpus.len(), 1, "same cycle re-learned must not accrete duplicates");
    }

    #[test]
    fn lessons_round_trip_through_markdown() {
        // A written-back lesson must survive persist + reload (Memory::to_markdown).
        let c = cycle("hq-z", &[("hq-z.1", Outcome::FalseHidden)]);
        let lesson = &reflect(&c).lessons[0];
        let reloaded = Memory::parse(&lesson.to_markdown()).unwrap();
        assert_eq!(&reloaded, lesson);
    }

    #[test]
    fn cycle_counts_delivered_and_defects() {
        let c = cycle(
            "hq-c",
            &[("hq-c.1", Outcome::Delivered), ("hq-c.2", Outcome::Trap), ("hq-c.3", Outcome::Reworked)],
        );
        assert_eq!(c.delivered(), 1);
        assert_eq!(c.defects(), 2);
    }
}
