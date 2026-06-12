//! Server-computed dependency readiness (hq-core-mcp.11, docs/10 §S4).
//!
//! A bead picked off `gt://issues?ready=true` is **sound**: every clause that the
//! tracking-model refactor made structural holds, so an agent never re-claims a
//! trap (a bead the dep graph reported ready but whose blocker lived only in
//! prose). Readiness ANDs four clauses:
//!
//! ```text
//! ready(b) :=
//!     (∀ d ∈ b.depends_on:  d.delivered_sha IS NOT NULL)   -- S2: deps delivered, not merely closed
//!   ∧ (b.phase ≤ phase_frontier.open_phase)                -- S1: phase open
//!   ∧ (∀ s ∈ b.surface where ¬s.planned: s exists on main) -- S3: own non-planned surfaces real
//!   ∧ b.status = 'open'                                    -- not already claimed/closed
//! ```
//!
//! It is a **one-hop AND**, not a transitive graph walk: a dep that is itself
//! blocked simply has `delivered_sha NULL`, which fails the first clause, and a
//! dep in a not-yet-open phase cannot have delivered (its sha is not on `main`),
//! so the phase gate on deps is enforced transitively through `delivered_sha`
//! without a second predicate. The pure predicate lives here; the server gathers
//! the inputs (the delivered index, the open phase, the git tree) and applies it.

use gt_store_dolt::{DepFact, IssuePhase, IssueRow};

use crate::surface::{parse_surface_json, SurfaceTree};

/// The epic-dep rule (hq-core-mcp.12 §C): when does a `depends_on` edge count as
/// satisfied?
///
/// - An **epic** dependency delivers-by-close — epics have no single
///   `delivered_sha` (they are satisfied by the union of their children closing),
///   so `status == "closed"` is the signal.
/// - A **non-epic** dependency (task/spike/…) is satisfied only by a non-NULL
///   `delivered_sha` — `closed` alone is not enough (a wontfix close delivers no
///   artifact), per docs/10 §S2.
///
/// Before this rule a dep-on-a-closed-epic could never go ready (epics never
/// stamp `delivered_sha`), false-hiding every bead that depends on an epic.
pub fn dep_satisfied(fact: &DepFact) -> bool {
    if fact.issue_type == "epic" {
        fact.status == "closed"
    } else {
        fact.delivered
    }
}

/// True when `row` passes all four readiness clauses (§S4).
///
/// - `open_phase` is the current `phase_frontier.open_phase`.
/// - `dep_fact(dep_id)` looks up a dependency's [`DepFact`] (the
///   [`dep_index`](gt_store_dolt::DoltIssues::dep_index) lookup); an unknown dep
///   id counts as **not** satisfied. The edge is judged by [`dep_satisfied`]
///   (the epic-dep rule, §C).
/// - `tree` is the `main` git tree the bead's own non-`planned` surfaces are
///   checked against (an [`AllowAllTree`](crate::AllowAllTree) when no repo is
///   wired, so the surface clause passes — mirroring the §S3 degradation).
pub fn is_ready(
    row: &IssueRow,
    deps: &[String],
    open_phase: IssuePhase,
    dep_fact: &dyn Fn(&str) -> Option<DepFact>,
    tree: &(dyn SurfaceTree + Sync),
) -> bool {
    // Clause 4: only an open bead is claimable.
    if row.status != "open" {
        return false;
    }
    // Clause 2 (S1): the bead's phase must not exceed the open frontier. An
    // unparseable phase token is treated as gated (excluded) rather than ready.
    match IssuePhase::parse(&row.phase) {
        Some(phase) if phase <= open_phase => {}
        _ => return false,
    }
    // Clause 1 (S2 + §C): every dependency must be satisfied — a non-epic dep by
    // delivery (sha on main), an epic dep by close — not merely be marked closed.
    // Deps come from the `issue_relations` table (passed in by caller).
    if !deps
        .iter()
        .all(|d| dep_fact(d).as_ref().map(dep_satisfied).unwrap_or(false))
    {
        return false;
    }
    // Clause 3 (S3): the bead's own non-`planned` surfaces must exist on `main`.
    // A `planned:true` surface is skipped — the bead will create it.
    parse_surface_json(&row.surface_json)
        .iter()
        .filter(|e| !e.planned)
        .all(|e| tree.contains(e.path.trim().trim_end_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::surface::AllowAllTree;

    /// Minimal `IssueRow` with the readiness-relevant fields set; the rest take
    /// cheap defaults the predicate ignores.
    fn row(status: &str, phase: &str, surface: &str) -> IssueRow {
        IssueRow {
            id: "hq-x.1".into(),
            title: "t".into(),
            status: status.into(),
            priority: 2,
            issue_type: "task".into(),
            assignee: None,
            owner: None,
            created_at: None,
            updated_at: None,
            closed_at: None,
            spec_id: None,
            domain_json: "[]".into(),
            surface_json: surface.into(),
            role_scope: None,
            version: 0,
            phase: phase.into(),
            delivered_sha: None,
            rig: String::new(),
            workspace: "default".into(),
            board_rank: String::new(),
            estimated_hours: None,
            start_date: None,
            due_date: None,
            dispatch: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    /// No dep is resolvable → every edge unsatisfied (unknown ids count as not
    /// satisfied).
    fn none_delivered(_: &str) -> Option<DepFact> {
        None
    }
    /// Every dep is a delivered non-epic task → every edge satisfied.
    fn all_delivered(_: &str) -> Option<DepFact> {
        Some(DepFact {
            issue_type: "task".into(),
            status: "closed".into(),
            delivered: true,
        })
    }

    #[test]
    fn open_no_deps_no_surface_is_ready() {
        let r = row("open", "P1", "[]");
        assert!(is_ready(&r, &[], IssuePhase::P3, &none_delivered, &AllowAllTree));
    }

    #[test]
    fn closed_or_working_is_never_ready() {
        assert!(!is_ready(&row("closed", "P1", "[]"), &[], IssuePhase::P3, &all_delivered, &AllowAllTree));
        assert!(!is_ready(&row("working", "P1", "[]"), &[], IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn phase_above_frontier_is_gated() {
        assert!(!is_ready(&row("open", "P4", "[]"), &[], IssuePhase::P3, &all_delivered, &AllowAllTree));
        assert!(is_ready(&row("open", "P3", "[]"), &[], IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn undelivered_dependency_blocks() {
        let r = row("open", "P1", "[]");
        let deps = vec!["hq-dep.1".to_string()];
        assert!(!is_ready(&r, &deps, IssuePhase::P3, &none_delivered, &AllowAllTree));
        assert!(is_ready(&r, &deps, IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn epic_dep_rule_closed_epic_satisfies_undelivered_task_does_not() {
        let closed_epic = |_: &str| {
            Some(DepFact {
                issue_type: "epic".into(),
                status: "closed".into(),
                delivered: false,
            })
        };
        let r = row("open", "P1", "[]");
        let epic_deps = vec!["hq-epic".to_string()];
        assert!(is_ready(&r, &epic_deps, IssuePhase::P3, &closed_epic, &AllowAllTree));

        let closed_task_no_sha = |_: &str| {
            Some(DepFact {
                issue_type: "task".into(),
                status: "closed".into(),
                delivered: false,
            })
        };
        let task_deps = vec!["hq-task.1".to_string()];
        assert!(!is_ready(&r, &task_deps, IssuePhase::P3, &closed_task_no_sha, &AllowAllTree));

        let open_epic = |_: &str| {
            Some(DepFact {
                issue_type: "epic".into(),
                status: "open".into(),
                delivered: false,
            })
        };
        assert!(!is_ready(&r, &epic_deps, IssuePhase::P3, &open_epic, &AllowAllTree));
    }

    #[test]
    fn missing_non_planned_surface_blocks() {
        struct Empty;
        impl SurfaceTree for Empty {
            fn contains(&self, _: &str) -> bool {
                false
            }
        }
        let blocked = row("open", "P1", r#"[{"path":"crates/missing","planned":false}]"#);
        assert!(!is_ready(&blocked, &[], IssuePhase::P3, &all_delivered, &Empty));
        let planned = row("open", "P1", r#"[{"path":"crates/missing","planned":true}]"#);
        assert!(is_ready(&planned, &[], IssuePhase::P3, &all_delivered, &Empty));
    }
}
