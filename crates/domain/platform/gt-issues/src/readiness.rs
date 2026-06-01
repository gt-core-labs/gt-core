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

use gt_store_dolt::{IssuePhase, IssueRow};

use crate::surface::{parse_surface_json, SurfaceTree};

/// True when `row` passes all four readiness clauses (§S4).
///
/// - `open_phase` is the current `phase_frontier.open_phase`.
/// - `is_delivered(dep_id)` reports whether a dependency has a non-NULL
///   `delivered_sha` (the [`delivered_index`](gt_store_dolt::DoltIssues::delivered_index)
///   lookup); an unknown dep id counts as **not** delivered.
/// - `tree` is the `main` git tree the bead's own non-`planned` surfaces are
///   checked against (an [`AllowAllTree`](crate::AllowAllTree) when no repo is
///   wired, so the surface clause passes — mirroring the §S3 degradation).
pub fn is_ready(
    row: &IssueRow,
    open_phase: IssuePhase,
    is_delivered: &dyn Fn(&str) -> bool,
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
    // Clause 1 (S2): every dependency must have delivered (sha on main), not just
    // be marked closed.
    let deps: Vec<String> = serde_json::from_str(&row.depends_on_json).unwrap_or_default();
    if !deps.iter().all(|d| is_delivered(d)) {
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
    fn row(status: &str, phase: &str, depends_on: &str, surface: &str) -> IssueRow {
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
            external_ref: None,
            spec_id: None,
            domain_json: "[]".into(),
            surface_json: surface.into(),
            depends_on_json: depends_on.into(),
            role_scope: None,
            version: 0,
            phase: phase.into(),
            delivered_sha: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    fn none_delivered(_: &str) -> bool {
        false
    }
    fn all_delivered(_: &str) -> bool {
        true
    }

    #[test]
    fn open_no_deps_no_surface_is_ready() {
        let r = row("open", "P1", "[]", "[]");
        assert!(is_ready(&r, IssuePhase::P3, &none_delivered, &AllowAllTree));
    }

    #[test]
    fn closed_or_working_is_never_ready() {
        assert!(!is_ready(&row("closed", "P1", "[]", "[]"), IssuePhase::P3, &all_delivered, &AllowAllTree));
        assert!(!is_ready(&row("working", "P1", "[]", "[]"), IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn phase_above_frontier_is_gated() {
        // P4 bead while open_phase=P3 → excluded (the mt-data.3/refactor.2 case).
        assert!(!is_ready(&row("open", "P4", "[]", "[]"), IssuePhase::P3, &all_delivered, &AllowAllTree));
        // P3 bead at the frontier → allowed.
        assert!(is_ready(&row("open", "P3", "[]", "[]"), IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn undelivered_dependency_blocks() {
        let r = row("open", "P1", r#"["hq-dep.1"]"#, "[]");
        assert!(!is_ready(&r, IssuePhase::P3, &none_delivered, &AllowAllTree));
        assert!(is_ready(&r, IssuePhase::P3, &all_delivered, &AllowAllTree));
    }

    #[test]
    fn missing_non_planned_surface_blocks() {
        struct Empty;
        impl SurfaceTree for Empty {
            fn contains(&self, _: &str) -> bool {
                false
            }
        }
        // non-planned + absent on main → not ready.
        let blocked = row("open", "P1", "[]", r#"[{"path":"crates/missing","planned":false}]"#);
        assert!(!is_ready(&blocked, IssuePhase::P3, &all_delivered, &Empty));
        // planned:true → the surface clause is skipped, so it is ready.
        let planned = row("open", "P1", "[]", r#"[{"path":"crates/missing","planned":true}]"#);
        assert!(is_ready(&planned, IssuePhase::P3, &all_delivered, &Empty));
    }
}
