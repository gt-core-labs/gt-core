//! Policy-as-code: the docs/04 non-negotiables as machine-checkable predicates
//! (hq-auto.4).
//!
//! The autonomous loop (epic hq-auto) plans and runs work without a per-decision
//! human. For that to be *safe without blind trust*, the planner must be able to
//! consult the 16 docs/04 non-negotiables as policy instead of asking a human for
//! every known category. This module encodes them.
//!
//! ## Two honest tiers
//!
//! Most of the 16 invariants are *architectural* — dependency direction, a sync
//! core, owned-enum events — and are enforced by the Rust compiler, the
//! byte-for-byte replay gate, or the store/MCP boundary at execute time. The
//! planner cannot re-derive those from a bead's metadata, and pretending it can
//! would be the "blind trust" the bead warns against. So [`INVARIANTS`] is an
//! honest catalog: each of the 16 carries an [`Enforcement`] tag saying *where*
//! it is machine-checked (or that it needs [`ManualReview`](Enforcement::ManualReview)).
//! "Known categories" the planner may proceed on = those with an automated
//! enforcement point; the rest escalate.
//!
//! The subset that *is* checkable against bead metadata at the tracking layer is
//! evaluated by [`check_bead`], which aggregates the existing tracker validators
//! (NN-16 taxonomy, acyclic `depends_on`, the `P1..P4` phase closed set) into one
//! stop-the-line [`PolicyVerdict`]. A violation trips stop-the-line rather than
//! "log and continue".

use std::collections::HashSet;

use gt_module_mcp::taxonomy::{validate as taxonomy_validate, BeadTaxonomy};
use gt_store_dolt::{IssuePhase, IssueStatus};

/// Where a non-negotiable is machine-enforced. The autonomous planner treats an
/// invariant as a "known category" it may proceed on iff that category has an
/// automated enforcement point; [`ManualReview`](Self::ManualReview) is the
/// escalate-to-human signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enforcement {
    /// Checked here, against bead metadata, by [`check_bead`].
    BeadPredicate,
    /// The Rust type system / build enforces it (dependency direction, sync core,
    /// actor-owned state, owned-enum events, single runtime).
    Compiler,
    /// The Paso-3 byte-for-byte replay gate catches violations (determinism,
    /// event-shape stability).
    ReplayGate,
    /// Enforced at execute/startup by the store, the MCP boundary, or the
    /// migration runner (status transitions, migration checksums, server-injected
    /// `workspace_id`, idempotent redelivery, MCP-only mutation).
    RuntimeGuard,
    /// No static check exists; an operational/architectural rule that needs human
    /// or external-tooling review (storage consistency, worktree workflow).
    ManualReview,
}

/// One docs/04 non-negotiable: its number (1..=16), a short name, and how it is
/// machine-enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Invariant {
    /// The doc's rule number, 1..=16.
    pub id: u8,
    /// Short slug naming the rule.
    pub name: &'static str,
    /// Where this rule is machine-enforced (or that it is not).
    pub enforcement: Enforcement,
}

use Enforcement::*;

/// The 16 docs/04 non-negotiables with their enforcement classification. The
/// source of truth for "what policy covers" the autonomous planner consults.
///
/// The classification is deliberately conservative: a rule is only
/// [`BeadPredicate`] if [`check_bead`] actually evaluates it; anything the
/// compiler / replay gate / runtime covers is tagged there, and the genuinely
/// uncheckable operational rules are [`ManualReview`] rather than silently
/// assumed satisfied.
pub const INVARIANTS: [Invariant; 16] = [
    Invariant { id: 1, name: "dependency-direction", enforcement: Compiler },
    Invariant { id: 2, name: "sync-core-async-edges", enforcement: Compiler },
    Invariant { id: 3, name: "determinism-no-clock-no-random", enforcement: ReplayGate },
    Invariant { id: 4, name: "mutable-state-in-actor", enforcement: Compiler },
    Invariant { id: 5, name: "events-owned-enums", enforcement: Compiler },
    Invariant { id: 6, name: "storage-two-engine-consistency", enforcement: ManualReview },
    Invariant { id: 7, name: "idempotency", enforcement: RuntimeGuard },
    Invariant { id: 8, name: "state-machines-reject-illegal", enforcement: RuntimeGuard },
    Invariant { id: 9, name: "migrations-append-only", enforcement: RuntimeGuard },
    Invariant { id: 10, name: "worktree-workflow", enforcement: ManualReview },
    Invariant { id: 11, name: "replay-byte-for-byte-gate", enforcement: ReplayGate },
    Invariant { id: 12, name: "mcp-canonical-for-agents", enforcement: RuntimeGuard },
    Invariant { id: 13, name: "bead-driven-development", enforcement: BeadPredicate },
    Invariant { id: 14, name: "single-tokio-runtime", enforcement: Compiler },
    Invariant { id: 15, name: "workspace-boundary", enforcement: RuntimeGuard },
    Invariant { id: 16, name: "bead-taxonomy-nn16", enforcement: BeadPredicate },
];

/// Look up an invariant by its doc number (1..=16). `None` for any other id.
pub fn invariant(id: u8) -> Option<&'static Invariant> {
    INVARIANTS.iter().find(|i| i.id == id)
}

/// The bead-metadata subset [`check_bead`] needs to evaluate the bead-checkable
/// invariants. Borrowed so the planner passes a view of the proposed bead with no
/// allocation.
#[derive(Debug, Clone, Copy)]
pub struct BeadFacts<'a> {
    /// Bead id (`<sub-epic>.<n>`, or the bare epic id for an epic).
    pub id: &'a str,
    /// `epic` / `task` / `spike` — epics are exempt from NN-16.
    pub issue_type: &'a str,
    /// The sub-epic this bead belongs to. Empty is a violation for a non-epic.
    pub external_ref: &'a str,
    /// Forward dependency edges; checked for self-edge and duplicates.
    pub depends_on: &'a [String],
    /// Lifecycle phase token. `None` defers to the store's `P1` default; `Some`
    /// is checked against the `P1..P4` closed set.
    pub phase: Option<&'a str>,
}

/// A single policy violation: the rule that tripped, an optional docs/04
/// invariant number it maps to, and a human-readable detail for the stop-the-line
/// report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The docs/04 invariant number this violation maps to, when one applies.
    /// `None` for a tracker shape-rule that supports an invariant without being
    /// one itself (acyclic `depends_on`, phase closed set).
    pub invariant: Option<u8>,
    /// Short slug naming the predicate that failed.
    pub rule: &'static str,
    /// What was wrong, naming the offending value.
    pub detail: String,
}

/// The verdict of consulting policy on a proposed bead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyVerdict {
    /// No bead-checkable invariant is violated; the planner may proceed.
    Pass,
    /// One or more invariants tripped — a stop-the-line event. The planner must
    /// not "just this once"; it escalates with these violations attached.
    StopTheLine(Vec<Violation>),
    /// The action is allowed to proceed *despite* an unmet requirement, because a
    /// subsystem signal (not the agent) authorizes deferring it — leaving a debt
    /// breadcrumb to settle later. Carries the human-readable reason the deferral
    /// was granted. Used by [`guard_claim_context`]: when there is no quota to feed
    /// the work, demanding the claim context would only block a model that cannot
    /// run anyway, so the custodian defers the requirement rather than stop the line.
    Deferred(String),
}

impl PolicyVerdict {
    /// Whether the verdict allows proceeding.
    pub fn is_pass(&self) -> bool {
        matches!(self, PolicyVerdict::Pass)
    }
}

/// Minimum trimmed length of a claim context. Short enough to never reject a real
/// "what/files/decisions" breadcrumb, long enough that a placeholder like `wip` or
/// `.` cannot satisfy the requirement (CLAUDE.md §2: leave enough for another agent
/// to resume). Only enforced on a *present* context; absence is the custodian's call.
pub const MIN_CONTEXT_LEN: usize = 12;

/// The custodian of the claim-context contract (docs/04 §13 / CLAUDE.md §2): a bead
/// may not enter `working` without recording the "what/files/decisions" another agent
/// would need to resume — UNLESS the quota subsystem confirms there is no capacity to
/// feed the work, in which case the requirement is *deferred with debt*, never waived
/// by the agent itself.
///
/// Mirrors the [`CloseIssue`](crate::CloseIssue) precedent: just as a close conditions
/// the mandatory `commit_sha` on the bead's code surface, a working transition
/// conditions the mandatory context on the actor's live quota — the gate lives in the
/// handler, fed a signal the caller cannot forge.
///
/// Verdicts:
/// - `target != Working` → [`Pass`](PolicyVerdict::Pass): only the claim transition is
///   gated; `open`/`closed` moves carry no context contract.
/// - context present (non-blank) → `Pass`: the breadcrumb is recorded, proceed.
/// - context absent + `quota_blocked == false` → [`StopTheLine`](PolicyVerdict::StopTheLine):
///   capacity exists, so the missing context is a real violation; reject before any write.
/// - context absent + `quota_blocked == true` → [`Deferred`](PolicyVerdict::Deferred):
///   no capacity to feed the work, so requiring context would only block a model that
///   cannot run; allow the move and stamp the debt.
///
/// `quota_blocked` MUST originate from the quota subsystem's *freshly confirmed* block
/// signal (`hq-context-custodian.3`), never from a field the agent sets — otherwise the
/// deferral is a loophole. This function takes it as a plain `bool` so the domain stays
/// free of the quota dependency; the composition root wires the real signal in `.2`.
pub fn guard_claim_context(
    target: IssueStatus,
    context: Option<&str>,
    quota_blocked: bool,
) -> PolicyVerdict {
    if target != IssueStatus::Working {
        return PolicyVerdict::Pass;
    }
    let has_context = context
        .map(str::trim)
        .is_some_and(|c| !c.is_empty());
    if has_context {
        return PolicyVerdict::Pass;
    }
    if quota_blocked {
        return PolicyVerdict::Deferred(
            "quota blocked (no capacity to feed the work) — context requirement deferred as debt"
                .to_string(),
        );
    }
    PolicyVerdict::StopTheLine(vec![Violation {
        invariant: Some(13),
        rule: "claim-context-required",
        detail: "transition to `working` requires a context recording the what/files/decisions \
                 so another agent can resume (CLAUDE.md §2). Provide `context`, or — only when the \
                 quota subsystem confirms no capacity — the move is deferred automatically."
            .into(),
    }])
}

/// Consult policy on a proposed bead: run every bead-checkable invariant and
/// return a single [`PolicyVerdict`]. This is the planner's "ask policy instead
/// of a human" entry point.
///
/// Predicates, attributed to their docs/04 rule:
/// - **#13 bead-driven** — the bead carries a non-empty id.
/// - **#16 NN-16 taxonomy** — non-epic carries `external_ref` and id matches
///   `<external_ref>.<n>`; reuses the canonical [`taxonomy_validate`] so the rule
///   matches the MCP boundary exactly.
/// - **acyclic `depends_on`** (supports #8) — no self-edge, no duplicate.
/// - **phase closed set** (supports #16's closed-set discipline) — `phase`, when
///   set, parses as `P1..P4`.
///
/// All violations are collected (not short-circuited) so the stop-the-line report
/// names every problem at once.
pub fn check_bead(facts: &BeadFacts<'_>) -> PolicyVerdict {
    let mut violations = Vec::new();

    // #13 — bead-driven development: a change must be anchored to a bead id.
    if facts.id.is_empty() {
        violations.push(Violation {
            invariant: Some(13),
            rule: "bead-id-present",
            detail: "bead id is empty — every change must be anchored to a bead (docs/04 §13)"
                .into(),
        });
    }

    // #16 — NN-16 taxonomy, via the canonical validator the MCP boundary uses.
    if let Err(e) = taxonomy_validate(&BeadTaxonomy {
        id: facts.id,
        issue_type: facts.issue_type,
        external_ref: facts.external_ref,
    }) {
        violations.push(Violation {
            invariant: Some(16),
            rule: "bead-taxonomy-nn16",
            detail: e.to_string(),
        });
    }

    // Acyclic depends_on (supports #8): no self-edge, no duplicate edge.
    if facts.depends_on.iter().any(|d| d == facts.id) {
        violations.push(Violation {
            invariant: None,
            rule: "acyclic-deps",
            detail: format!("depends_on contains the bead's own id ({}) — self-cycle", facts.id),
        });
    }
    let mut seen = HashSet::new();
    for dep in facts.depends_on {
        if !seen.insert(dep.as_str()) {
            violations.push(Violation {
                invariant: None,
                rule: "acyclic-deps",
                detail: format!("depends_on lists `{dep}` more than once"),
            });
        }
    }

    // Phase closed set (supports #16's closed-set discipline).
    if let Some(phase) = facts.phase {
        if IssuePhase::parse(phase).is_none() {
            violations.push(Violation {
                invariant: None,
                rule: "phase-closed-set",
                detail: format!("unknown phase `{phase}` (expected one of P1/P2/P3/P4)"),
            });
        }
    }

    if violations.is_empty() {
        PolicyVerdict::Pass
    } else {
        PolicyVerdict::StopTheLine(violations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> BeadFacts<'static> {
        BeadFacts {
            id: "hq-auto.4",
            issue_type: "task",
            external_ref: "hq-auto",
            depends_on: &[],
            phase: Some("P1"),
        }
    }

    #[test]
    fn catalog_covers_all_sixteen_uniquely() {
        let ids: HashSet<u8> = INVARIANTS.iter().map(|i| i.id).collect();
        assert_eq!(ids.len(), 16);
        assert_eq!(*ids.iter().min().unwrap(), 1);
        assert_eq!(*ids.iter().max().unwrap(), 16);
        // Lookup resolves and rejects out-of-range.
        assert_eq!(invariant(16).unwrap().name, "bead-taxonomy-nn16");
        assert!(invariant(0).is_none());
        assert!(invariant(17).is_none());
    }

    #[test]
    fn only_bead_local_rules_are_bead_predicates() {
        // Exactly #13 and #16 are evaluable from bead metadata here; the rest are
        // enforced by compiler / replay / runtime / manual review.
        let bead_checkable: Vec<u8> = INVARIANTS
            .iter()
            .filter(|i| i.enforcement == Enforcement::BeadPredicate)
            .map(|i| i.id)
            .collect();
        assert_eq!(bead_checkable, vec![13, 16]);
    }

    #[test]
    fn well_formed_bead_passes() {
        assert_eq!(check_bead(&good()), PolicyVerdict::Pass);
        assert!(check_bead(&good()).is_pass());
    }

    #[test]
    fn empty_id_trips_bead_driven() {
        let mut f = good();
        f.id = "";
        let v = check_bead(&f);
        assert!(!v.is_pass());
        // The empty id trips #13 (and NN-16, which also requires a non-empty id).
        if let PolicyVerdict::StopTheLine(vs) = v {
            assert!(vs.iter().any(|x| x.invariant == Some(13)));
        } else {
            panic!("expected stop-the-line");
        }
    }

    #[test]
    fn mismatched_external_ref_trips_nn16() {
        let mut f = good();
        f.external_ref = "hq-other";
        match check_bead(&f) {
            PolicyVerdict::StopTheLine(vs) => {
                assert!(vs.iter().any(|x| x.invariant == Some(16)));
            }
            PolicyVerdict::Pass | PolicyVerdict::Deferred(_) => panic!("expected NN-16 violation"),
        }
    }

    #[test]
    fn self_and_duplicate_deps_trip_acyclic() {
        let mut f = good();
        let deps = vec!["hq-auto.4".to_string(), "hq-x.1".to_string(), "hq-x.1".to_string()];
        f.depends_on = &deps;
        match check_bead(&f) {
            PolicyVerdict::StopTheLine(vs) => {
                let acyclic = vs.iter().filter(|x| x.rule == "acyclic-deps").count();
                // One self-edge + one duplicate.
                assert_eq!(acyclic, 2);
            }
            PolicyVerdict::Pass | PolicyVerdict::Deferred(_) => panic!("expected acyclic violations"),
        }
    }

    #[test]
    fn bad_phase_trips_closed_set() {
        let mut f = good();
        f.phase = Some("P5");
        match check_bead(&f) {
            PolicyVerdict::StopTheLine(vs) => {
                assert!(vs.iter().any(|x| x.rule == "phase-closed-set"));
            }
            PolicyVerdict::Pass | PolicyVerdict::Deferred(_) => panic!("expected phase violation"),
        }
        // Omitted phase is fine (store applies the P1 default).
        let mut f = good();
        f.phase = None;
        assert!(check_bead(&f).is_pass());
    }

    #[test]
    fn epic_is_exempt_from_nn16() {
        let f = BeadFacts {
            id: "hq-auto",
            issue_type: "epic",
            external_ref: "",
            depends_on: &[],
            phase: None,
        };
        assert!(check_bead(&f).is_pass());
    }

    #[test]
    fn all_violations_collected_not_short_circuited() {
        // A bead that breaks NN-16, acyclic, and phase at once reports all three.
        let deps = vec!["hq-auto.4".to_string()];
        let f = BeadFacts {
            id: "hq-auto.4",
            issue_type: "task",
            external_ref: "hq-wrong",
            depends_on: &deps,
            phase: Some("P9"),
        };
        match check_bead(&f) {
            PolicyVerdict::StopTheLine(vs) => {
                assert!(vs.iter().any(|x| x.invariant == Some(16)));
                assert!(vs.iter().any(|x| x.rule == "acyclic-deps"));
                assert!(vs.iter().any(|x| x.rule == "phase-closed-set"));
            }
            PolicyVerdict::Pass => panic!("expected multiple violations"),
            PolicyVerdict::Deferred(_) => panic!("bead policy never defers"),
        }
    }

    // --- guard_claim_context: the three custodian paths (hq-context-custodian.1) -------------

    #[test]
    fn claim_context_present_passes() {
        let v = guard_claim_context(
            IssueStatus::Working,
            Some("rewiring run_transition_issue; touches handlers.rs + policy.rs"),
            false,
        );
        assert_eq!(v, PolicyVerdict::Pass);
        // A blocked pool with context present still passes (no deferral needed).
        assert!(guard_claim_context(IssueStatus::Working, Some("real context here"), true).is_pass());
    }

    #[test]
    fn claim_context_absent_with_healthy_quota_stops_the_line() {
        for ctx in [None, Some(""), Some("   ")] {
            match guard_claim_context(IssueStatus::Working, ctx, false) {
                PolicyVerdict::StopTheLine(vs) => {
                    assert!(vs.iter().any(|v| v.rule == "claim-context-required"));
                }
                other => panic!("expected stop-the-line for ctx={ctx:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn claim_context_absent_with_blocked_quota_defers() {
        match guard_claim_context(IssueStatus::Working, None, true) {
            PolicyVerdict::Deferred(reason) => assert!(reason.contains("quota blocked")),
            other => panic!("expected deferred, got {other:?}"),
        }
    }

    #[test]
    fn claim_context_only_gates_the_working_transition() {
        // open/closed moves carry no context contract, even with healthy quota.
        assert!(guard_claim_context(IssueStatus::Open, None, false).is_pass());
        assert!(guard_claim_context(IssueStatus::Closed, None, false).is_pass());
    }
}
