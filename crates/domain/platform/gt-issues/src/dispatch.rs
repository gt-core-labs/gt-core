//! Dispatch-policy resolution with `child_of` inheritance (gtcore-1acbcf — C1
//! of the "Control de despacho" epic).
//!
//! The STATIC half of the dispatch-control design: every bead carries an
//! optional `dispatch` column (`auto` | `manual`); a bead WITHOUT an own value
//! inherits its parent epic's via the `child_of` relation in `issue_relations`,
//! resolved **in read** — never materialized — so flipping one epic's policy
//! retargets its whole subtree with a single write. The chain bottoms out at
//! [`Dispatch::Manual`]: a bead with no parent (or a chain that never sets a
//! value) is NOT agent-dispatchable. `auto` is always an explicit opt-in
//! somewhere up the chain.
//!
//! C2 (operator locks) and C3 (the `ready_for_auto` frontier) layer on top:
//! C3 consumes [`resolve_dispatch`] with the same two maps the list filter
//! uses — [`parent_map`](gt_store_dolt::DoltIssues::parent_map) (unscoped) and
//! [`dispatch_index`](gt_store_dolt::DoltIssues::dispatch_index).

use std::collections::{HashMap, HashSet};

use gt_store_dolt::{DepFact, IssuePhase, IssueRow};

use crate::readiness::is_ready;
use crate::surface::{parse_surface_json, SurfaceTree};
use crate::taxonomy::Dispatch;

/// Resolve the EFFECTIVE dispatch policy of bead `id`.
///
/// - `own` — the bead's raw column value (`IssueRow::dispatch` /
///   `IssueDetail::dispatch`); a parseable own value always wins over anything
///   inherited.
/// - `parents` — `child_id → parent_epic_id` over the `child_of` relation
///   (use the **unscoped** [`parent_map`](gt_store_dolt::DoltIssues::parent_map)
///   — an ancestor may live outside any rig/workspace-filtered display set).
/// - `raw` — `id → raw dispatch` for the whole table
///   ([`dispatch_index`](gt_store_dolt::DoltIssues::dispatch_index)).
///
/// Walks the parent chain taking the FIRST parseable value; a missing parent,
/// an exhausted chain, or a cycle (guarded by a hop cap so corrupt relations
/// can never hang a list call) resolves to [`Dispatch::Manual`] — the
/// nothing-dispatchable-by-accident default. An unparseable stored token (a
/// legacy/corrupt row) is treated as unset and the walk continues.
pub fn resolve_dispatch(
    id: &str,
    own: Option<&str>,
    parents: &HashMap<String, String>,
    raw: &HashMap<String, Option<String>>,
) -> Dispatch {
    if let Some(d) = own.and_then(Dispatch::parse) {
        return d;
    }
    // Hop cap doubles as the cycle guard: a `child_of` cycle (corrupt data —
    // nothing legitimate nests this deep) terminates at the Manual default
    // instead of looping.
    const MAX_HOPS: usize = 64;
    let mut cursor = id;
    for _ in 0..MAX_HOPS {
        let Some(parent) = parents.get(cursor) else {
            return Dispatch::Manual;
        };
        if let Some(d) = raw.get(parent.as_str()).and_then(|v| v.as_deref()).and_then(Dispatch::parse) {
            return d;
        }
        cursor = parent;
    }
    Dispatch::Manual
}

/// Narrow a snapshot to the rows whose RESOLVED dispatch (own value, else
/// `child_of` inheritance, else `manual`) equals `want` — the
/// `issues.list?dispatch=` filter (gtcore-1acbcf).
///
/// **Cost note**: the filter works on the resolved value, so it cannot run in
/// the SQL `WHERE` (inheritance walks a relation chain). The caller pulls the
/// candidate rows plus the two whole-table maps (`parent_map` +
/// `dispatch_index` — one 2-column scan each) and this resolves per row:
/// O(rows × chain-depth) lookups, all in-memory. Fine at tracker scale (≤ the
/// `GT_ISSUES_MAX_LIMIT` ceiling); revisit with a materialized view only if the
/// table outgrows it.
pub fn filter_dispatch(
    rows: Vec<IssueRow>,
    want: Dispatch,
    parents: &HashMap<String, String>,
    raw: &HashMap<String, Option<String>>,
) -> Vec<IssueRow> {
    rows.into_iter()
        .filter(|r| resolve_dispatch(&r.id, r.dispatch.as_deref(), parents, raw) == want)
        .collect()
}

// ---------------------------------------------------------------------------
// Operator locks (gtcore-a33952 — C2 of the "Control de despacho" epic)
// ---------------------------------------------------------------------------

/// Heuristic: does `actor` look like an AGENT session rather than a human?
///
/// Polecat MCP tokens carry `sub = <session>` (`token_for`, hq-agent-provisioning.3)
/// and session names are dash-chains (`gt-hq-abc-1`, `gtweb-gtweb-daf156`): ≥3
/// dash-separated tokens. Humans are short names (`admin`) or emails — neither
/// matches. **Deliberately conservative**: an unrecognized actor is HUMAN, so
/// its working epics lock their subtrees — the failure mode is "agents skip
/// work", never "agents trample the operator". Until per-agent identity is
/// universal (Fase 2 A2), shared-PAT claims (`admin`) therefore always lock.
pub fn session_like_actor(actor: &str) -> bool {
    if actor.contains('@') || actor.is_empty() {
        return false;
    }
    actor.split('-').filter(|seg| !seg.is_empty()).count() >= 3
}

/// The lock roots: epics in `working` whose owner is an EXPLICIT HUMAN claim
/// (a non-empty owner that `is_agent` rejects). Feed it the unscoped
/// working-epic snapshot
/// ([`working_epics`](gt_store_dolt::DoltIssues::working_epics)) — an operator
/// may hold an epic outside the rig/workspace being listed.
///
/// An EMPTY owner does NOT lock (gtcore-bf0fc9): `working_epics` maps a
/// `NULL`/unset owner to `""`, and a container epic flipped to `working` with
/// no owner is NOT "a human claiming the subtree" — treating it as one
/// silently freezes the whole auto-dispatch frontier beneath it (a bead like
/// `gtcore-0179f8`, `open` + `dispatch=auto`, never gets slung). C2 is an
/// OPT-IN operator lock, so it requires an explicit human owner; an orphaned
/// `working` epic is left to the frontier rather than locking it.
pub fn locked_roots<'a>(
    working_epics: impl IntoIterator<Item = (&'a str, &'a str)>,
    is_agent: &dyn Fn(&str) -> bool,
) -> HashSet<String> {
    working_epics
        .into_iter()
        .filter(|(_, owner)| !owner.is_empty() && !is_agent(owner))
        .map(|(id, _)| id.to_string())
        .collect()
}

/// Is `id` inside an operator-locked subtree? True when the bead itself or ANY
/// `child_of` ancestor is a lock root — the C2 guarantee: the operator claims
/// ONE epic and every descendant leaves the agent frontier, even those marked
/// `auto`. Same hop-cap cycle guard as [`resolve_dispatch`].
pub fn operator_locked(
    id: &str,
    parents: &HashMap<String, String>,
    locked: &HashSet<String>,
) -> bool {
    if locked.is_empty() {
        return false;
    }
    const MAX_HOPS: usize = 64;
    let mut cursor = id;
    for _ in 0..=MAX_HOPS {
        if locked.contains(cursor) {
            return true;
        }
        match parents.get(cursor) {
            Some(parent) => cursor = parent,
            None => return false,
        }
    }
    false
}

// ---------------------------------------------------------------------------
// C3: ready_for_auto frontier (gtcore-7bec8c)
// ---------------------------------------------------------------------------

/// Collect the set of surface paths currently occupied by `working` beads.
/// Each path is normalized (trimmed, trailing `/` stripped) so overlap checks
/// are case-insensitive to formatting differences.
pub fn occupied_surfaces(working: &[(String, String)]) -> HashSet<String> {
    working
        .iter()
        .flat_map(|(_, sj)| {
            parse_surface_json(sj)
                .into_iter()
                .map(|e| e.path.trim().trim_end_matches('/').to_string())
        })
        .collect()
}

/// Does `row`'s surface overlap with any path in `occupied`?
pub fn surface_overlaps(row: &IssueRow, occupied: &HashSet<String>) -> bool {
    if occupied.is_empty() {
        return false;
    }
    parse_surface_json(&row.surface_json)
        .iter()
        .any(|e| occupied.contains(e.path.trim().trim_end_matches('/')))
}

/// The UNIFIED slingability predicate (gtcore-db99e0): should a live polecat exist
/// for this bead right now? ONE decision — previously drifted across the three sites
/// that each independently re-slung beads they never should have: the auto-dispatch
/// frontier ([`ready_for_auto`]), boot re-hydration of crash-orphaned work, and the
/// supervisor's dead-polecat re-sling.
///
/// ```text
/// should_sling(b) :=
///       status ∈ {open, working}   -- a closed/delivered bead has nothing to sling
///     ∧ issue_type ≠ epic          -- epics are CONTAINERS; their task/bug/feature
///                                     children get slung, never the epic itself
///     ∧ dispatch = Auto            -- manual beads are operator-driven, never auto-(re)slung
/// ```
///
/// `dispatch` is the bead's ALREADY-RESOLVED policy (own value → `child_of`
/// inheritance → `Manual`, from [`resolve_dispatch`]). The frontier resolves it from
/// the maps it already holds; a caller that only knows a bead id reads the bead +
/// the same two maps and resolves it the identical way before calling this. The
/// status/type comparisons are whitespace- and case-tolerant so a raw tracker column
/// never trips the predicate on formatting alone.
pub fn should_sling(status: &str, issue_type: &str, dispatch: Dispatch) -> bool {
    let status = status.trim();
    (status.eq_ignore_ascii_case("open") || status.eq_ignore_ascii_case("working"))
        && !issue_type.trim().eq_ignore_ascii_case("epic")
        && dispatch == Dispatch::Auto
}

/// The agent-dispatch frontier: beads that pass ALL clauses and are safe to hand to
/// an autonomous agent right now.
///
/// ```text
/// ready_for_auto(b) :=
///     is_ready(b)                                    -- §S4: deps + phase + surface + open
///   ∧ should_sling(b)                                -- C1 + gtcore-db99e0: dispatch=Auto ∧ ¬epic
///   ∧ ¬operator_locked(b)                            -- C2: no human epic claim above
///   ∧ ¬surface_overlaps(b, working_surfaces)         -- C3: no in-flight crate collision
/// ```
///
/// The [`should_sling`] clause folds in C1's `dispatch = Auto` opt-in AND the
/// `¬epic` exclusion the frontier was missing (gtcore-dc3c13): an `open`, `auto`
/// epic used to satisfy [`is_ready`] and land a polecat ON the container instead of
/// its children. `is_ready` already restricts to `open`, so the predicate's broader
/// `{open, working}` status clause is a no-op here — the net new effect is dropping
/// epics.
///
/// The caller supplies the pre-fetched maps (all unscoped — an ancestor or a
/// working bead may live outside the display scope):
///
/// - `deps_of(id)` — dependency edges from `issue_relations`
/// - `dep_fact(id)` — per-dep delivery status ([`DepFact`])
/// - `open_phase` — current phase frontier
/// - `tree` — `main` git tree for surface existence
/// - `parents` / `dispatch_raw` — the two `child_of` + dispatch maps
/// - `locked` — operator lock roots (from [`locked_roots`])
/// - `occupied` — surfaces of `working` beads (from [`occupied_surfaces`])
/// - `held_rigs` — rig names on dispatch hold (rig-hold H2, gtcore-1f5e67); a bead whose `rig`
///   is in this set is excluded from the frontier even when otherwise ready+auto, so an operator
///   can pause a rig without the orchestrator dispatching its beads. Resuming the rig (dropping it
///   from the set) makes its ready beads eligible again on the next poll. In-flight polecats are
///   untouched — this gates only what the frontier hands out next.
pub fn ready_for_auto(
    rows: Vec<IssueRow>,
    deps_of: &HashMap<String, Vec<String>>,
    dep_fact: &dyn Fn(&str) -> Option<DepFact>,
    open_phase: IssuePhase,
    tree: &(dyn SurfaceTree + Sync),
    parents: &HashMap<String, String>,
    dispatch_raw: &HashMap<String, Option<String>>,
    locked: &HashSet<String>,
    occupied: &HashSet<String>,
    held_rigs: &HashSet<String>,
) -> Vec<IssueRow> {
    rows.into_iter()
        .filter(|r| {
            let deps = deps_of.get(&r.id).map(|v| v.as_slice()).unwrap_or(&[]);
            let dispatch = resolve_dispatch(&r.id, r.dispatch.as_deref(), parents, dispatch_raw);
            is_ready(r, deps, open_phase, dep_fact, tree)
                && should_sling(&r.status, &r.issue_type, dispatch)
                && !operator_locked(&r.id, parents, locked)
                && !surface_overlaps(r, occupied)
                && !held_rigs.contains(r.rig.trim())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn maps(
        edges: &[(&str, &str)],
        own: &[(&str, Option<&str>)],
    ) -> (HashMap<String, String>, HashMap<String, Option<String>>) {
        let parents = edges
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect();
        let raw = own
            .iter()
            .map(|(id, v)| (id.to_string(), v.map(str::to_string)))
            .collect();
        (parents, raw)
    }

    #[test]
    fn own_value_wins_over_inherited() {
        let (parents, raw) = maps(
            &[("hq-epic-1", "hq-epic")],
            &[("hq-epic", Some("auto")), ("hq-epic-1", Some("manual"))],
        );
        assert_eq!(
            resolve_dispatch("hq-epic-1", Some("manual"), &parents, &raw),
            Dispatch::Manual
        );
        // And the inverse override: auto child under a manual epic.
        let (parents, raw) = maps(
            &[("hq-epic-1", "hq-epic")],
            &[("hq-epic", Some("manual")), ("hq-epic-1", Some("auto"))],
        );
        assert_eq!(
            resolve_dispatch("hq-epic-1", Some("auto"), &parents, &raw),
            Dispatch::Auto
        );
    }

    #[test]
    fn null_child_inherits_from_epic() {
        let (parents, raw) = maps(
            &[("hq-epic-1", "hq-epic")],
            &[("hq-epic", Some("auto")), ("hq-epic-1", None)],
        );
        assert_eq!(
            resolve_dispatch("hq-epic-1", None, &parents, &raw),
            Dispatch::Auto
        );
    }

    #[test]
    fn inheritance_walks_a_multi_level_chain() {
        // grandchild → child (NULL) → epic (auto).
        let (parents, raw) = maps(
            &[("c", "b"), ("b", "a")],
            &[("a", Some("auto")), ("b", None), ("c", None)],
        );
        assert_eq!(resolve_dispatch("c", None, &parents, &raw), Dispatch::Auto);
        // The middle level overriding wins over the root.
        let (parents, raw) = maps(
            &[("c", "b"), ("b", "a")],
            &[("a", Some("auto")), ("b", Some("manual")), ("c", None)],
        );
        assert_eq!(resolve_dispatch("c", None, &parents, &raw), Dispatch::Manual);
    }

    #[test]
    fn no_parent_or_unset_chain_defaults_manual() {
        let (parents, raw) = maps(&[], &[("orphan", None)]);
        assert_eq!(
            resolve_dispatch("orphan", None, &parents, &raw),
            Dispatch::Manual
        );
        // Parent exists but never sets a value.
        let (parents, raw) = maps(&[("b", "a")], &[("a", None), ("b", None)]);
        assert_eq!(resolve_dispatch("b", None, &parents, &raw), Dispatch::Manual);
    }

    #[test]
    fn unparseable_own_value_falls_back_to_inheritance() {
        // A legacy/corrupt own token is treated as unset, not as manual-by-error.
        let (parents, raw) = maps(
            &[("b", "a")],
            &[("a", Some("auto")), ("b", Some("banana"))],
        );
        assert_eq!(
            resolve_dispatch("b", Some("banana"), &parents, &raw),
            Dispatch::Auto
        );
    }

    #[test]
    fn relation_cycle_terminates_manual() {
        let (parents, raw) = maps(&[("a", "b"), ("b", "a")], &[("a", None), ("b", None)]);
        assert_eq!(resolve_dispatch("a", None, &parents, &raw), Dispatch::Manual);
    }

    #[test]
    fn filter_keeps_only_resolved_matches() {
        fn row(id: &str, dispatch: Option<&str>) -> IssueRow {
            IssueRow {
                id: id.to_string(),
                title: id.to_string(),
                status: "open".to_string(),
                priority: 2,
                issue_type: "task".to_string(),
                assignee: None,
                owner: None,
                created_at: None,
                updated_at: None,
                closed_at: None,
                spec_id: None,
                domain_json: "[]".to_string(),
                surface_json: "[]".to_string(),
                role_scope: None,
                version: 0,
                phase: "P1".to_string(),
                delivered_sha: None,
                rig: String::new(),
                workspace: "default".into(),
                board_rank: String::new(),
                estimated_hours: None,
                start_date: None,
                due_date: None,
                dispatch: dispatch.map(str::to_string),
                description: None,
                design: None,
                acceptance_criteria: None,
                notes: None,
            }
        }
        let (parents, raw) = maps(
            &[("e1-a", "e1"), ("e1-b", "e1"), ("e2-a", "e2")],
            &[
                ("e1", Some("auto")),
                ("e1-a", None),            // inherits auto
                ("e1-b", Some("manual")), // explicit override beats the epic
                ("e2", None),
                ("e2-a", None), // chain unset → manual
            ],
        );
        let rows = vec![
            row("e1-a", None),
            row("e1-b", Some("manual")),
            row("e2-a", None),
        ];
        let auto = filter_dispatch(rows.clone(), Dispatch::Auto, &parents, &raw);
        assert_eq!(
            auto.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["e1-a"]
        );
        let manual = filter_dispatch(rows, Dispatch::Manual, &parents, &raw);
        assert_eq!(
            manual.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["e1-b", "e2-a"]
        );
    }

    // --- C2: operator locks (gtcore-a33952) ---

    #[test]
    fn session_actors_are_agents_humans_are_not() {
        assert!(session_like_actor("gt-hq-abc-1"));
        assert!(session_like_actor("gtweb-gtweb-daf156"));
        assert!(!session_like_actor("admin"));
        assert!(!session_like_actor("ops@gt.local"));
        assert!(!session_like_actor(""));
        // A bare rig-bead pair (2 tokens) is ambiguous -> human (conservative).
        assert!(!session_like_actor("gtweb-daf156"));
    }

    #[test]
    fn human_claimed_epics_lock_agent_claimed_do_not() {
        let epics = [("hq-epic", "admin"), ("hq-other", "gt-hq-abc-1")];
        let locked = locked_roots(epics, &|a: &str| session_like_actor(a));
        assert!(locked.contains("hq-epic"), "human claim locks");
        assert!(!locked.contains("hq-other"), "agent claim does not lock");
    }

    #[test]
    fn empty_owner_working_epic_does_not_lock() {
        // gtcore-bf0fc9: a container epic flipped to `working` with NO owner
        // (working_epics maps NULL → "") must NOT lock its subtree — only an
        // EXPLICIT human claim does. Otherwise the whole auto-dispatch frontier
        // beneath an orphaned working epic freezes.
        let epics = [
            ("epic-orphan", ""),                  // empty owner → NOT a lock
            ("epic-human", "ops@gt.local"),       // explicit human email → lock
            ("epic-agent", "gtcore-gtcore-d72302"), // agent session → NOT a lock
        ];
        let locked = locked_roots(epics, &|a: &str| session_like_actor(a));
        assert!(
            !locked.contains("epic-orphan"),
            "empty-owner working epic must not lock its subtree"
        );
        assert!(locked.contains("epic-human"), "explicit human claim locks");
        assert!(
            !locked.contains("epic-agent"),
            "agent-session claim does not lock"
        );
    }

    #[test]
    fn lock_covers_whole_subtree_and_releases() {
        let (parents, _) = maps(
            &[("hq-epic-1", "hq-epic"), ("hq-epic-1-a", "hq-epic-1"), ("free-1", "free")],
            &[],
        );
        let locked: HashSet<String> = ["hq-epic".to_string()].into_iter().collect();
        // Direct child, grandchild, and the root itself are all locked.
        assert!(operator_locked("hq-epic", &parents, &locked));
        assert!(operator_locked("hq-epic-1", &parents, &locked));
        assert!(operator_locked("hq-epic-1-a", &parents, &locked));
        // A sibling tree stays free.
        assert!(!operator_locked("free-1", &parents, &locked));
        // Releasing the claim (empty lock set) frees everything without rewrites.
        assert!(!operator_locked("hq-epic-1-a", &parents, &HashSet::new()));
    }

    #[test]
    fn lock_walk_survives_relation_cycles() {
        let (parents, _) = maps(&[("a", "b"), ("b", "a")], &[]);
        let locked: HashSet<String> = ["other".to_string()].into_iter().collect();
        assert!(!operator_locked("a", &parents, &locked), "cycle terminates, not hangs");
    }

    // --- C3: ready_for_auto frontier (gtcore-7bec8c) ---

    fn auto_row(id: &str, surface: &str) -> IssueRow {
        IssueRow {
            id: id.to_string(),
            title: id.to_string(),
            status: "open".to_string(),
            priority: 2,
            issue_type: "task".to_string(),
            assignee: None,
            owner: None,
            created_at: None,
            updated_at: None,
            closed_at: None,
            spec_id: None,
            domain_json: "[]".to_string(),
            surface_json: surface.to_string(),
            role_scope: None,
            version: 0,
            phase: "P1".to_string(),
            delivered_sha: None,
            rig: String::new(),
            workspace: "default".into(),
            board_rank: String::new(),
            estimated_hours: None,
            start_date: None,
            due_date: None,
            dispatch: Some("auto".to_string()),
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    struct AllowAll;
    impl crate::surface::SurfaceTree for AllowAll {
        fn contains(&self, _: &str) -> bool {
            true
        }
    }

    #[test]
    fn occupied_surfaces_collects_working_paths() {
        let working = vec![
            ("b1".into(), r#"["crates/a","crates/b"]"#.into()),
            ("b2".into(), r#"[{"path":"crates/c","planned":false}]"#.into()),
        ];
        let occ = occupied_surfaces(&working);
        assert!(occ.contains("crates/a"));
        assert!(occ.contains("crates/b"));
        assert!(occ.contains("crates/c"));
        assert!(!occ.contains("crates/d"));
    }

    #[test]
    fn surface_overlap_excludes_colliding_bead() {
        let row = auto_row("x", r#"["crates/a","crates/d"]"#);
        let occupied: HashSet<String> = ["crates/a".to_string()].into_iter().collect();
        assert!(surface_overlaps(&row, &occupied));

        let no_overlap = auto_row("y", r#"["crates/e"]"#);
        assert!(!surface_overlaps(&no_overlap, &occupied));
    }

    #[test]
    fn ready_for_auto_filters_all_clauses() {
        // Setup: two beads under an auto epic, one overlapping a working surface,
        // one in a locked subtree.
        let b_ok = auto_row("b-ok", r#"["crates/free"]"#);
        let b_overlap = auto_row("b-overlap", r#"["crates/busy"]"#);
        let b_locked = auto_row("b-locked", r#"["crates/other"]"#);
        let b_manual = {
            let mut r = auto_row("b-manual", r#"["crates/also-free"]"#);
            r.dispatch = Some("manual".to_string());
            r
        };

        let rows = vec![b_ok, b_overlap, b_locked, b_manual];

        let deps_of: HashMap<String, Vec<String>> = HashMap::new();
        let dep_fact = |_: &str| -> Option<DepFact> { None };
        let (parents, dispatch_raw) = maps(
            &[
                ("b-ok", "epic-a"),
                ("b-overlap", "epic-a"),
                ("b-locked", "epic-b"),
                ("b-manual", "epic-a"),
            ],
            &[
                ("epic-a", Some("auto")),
                ("epic-b", Some("auto")),
            ],
        );
        let locked: HashSet<String> = ["epic-b".to_string()].into_iter().collect();
        let occupied: HashSet<String> = ["crates/busy".to_string()].into_iter().collect();

        let frontier = ready_for_auto(
            rows,
            &deps_of,
            &dep_fact,
            IssuePhase::P1,
            &AllowAll,
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &HashSet::new(),
        );
        assert_eq!(
            frontier.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["b-ok"],
            "only the bead that passes all clauses survives"
        );
    }

    #[test]
    fn ready_for_auto_excludes_beads_of_a_held_rig() {
        // rig-hold H2 (gtcore-1f5e67): a ready+auto bead whose rig is on hold is dropped from the
        // frontier; an otherwise-identical bead on an auto rig still passes. Resuming the rig
        // (dropping it from the held set) makes its bead eligible again next poll.
        let mk_rows = || {
            let held_bead = {
                let mut r = auto_row("held-bead", r#"["crates/a"]"#);
                r.rig = "gtcore".into();
                r
            };
            let auto_bead = {
                let mut r = auto_row("auto-bead", r#"["crates/b"]"#);
                r.rig = "gtweb".into();
                r
            };
            vec![held_bead, auto_bead]
        };

        let deps_of: HashMap<String, Vec<String>> = HashMap::new();
        let dep_fact = |_: &str| -> Option<DepFact> { None };
        let (parents, dispatch_raw) = maps(&[], &[]);
        let locked: HashSet<String> = HashSet::new();
        let occupied: HashSet<String> = HashSet::new();

        // gtcore on hold → only the gtweb bead survives.
        let held: HashSet<String> = ["gtcore".to_string()].into_iter().collect();
        let frontier = ready_for_auto(
            mk_rows(),
            &deps_of,
            &dep_fact,
            IssuePhase::P1,
            &AllowAll,
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &held,
        );
        assert_eq!(
            frontier.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["auto-bead"],
            "a held rig's bead is excluded; the auto rig's bead passes"
        );

        // Resume gtcore (empty held set) → both ready beads are eligible again.
        let frontier = ready_for_auto(
            mk_rows(),
            &deps_of,
            &dep_fact,
            IssuePhase::P1,
            &AllowAll,
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &HashSet::new(),
        );
        assert_eq!(
            frontier.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["held-bead", "auto-bead"],
            "resuming the rig restores its bead to the frontier"
        );
    }

    #[test]
    fn frontier_reaches_bead_under_orphaned_working_epic() {
        // gtcore-bf0fc9 regression: an open + dispatch=auto bead nested under a
        // container epic that is `working` with NO owner must reach the DIRECT
        // auto-dispatch frontier. The pre-fix `locked_roots` treated the empty
        // owner as a human claim and froze the whole subtree, so the bead was
        // never slung despite spare pool/host/quota capacity.
        let bead = auto_row("gtcore-0179f8", r#"["crates/free"]"#);
        let rows = vec![bead];

        let deps_of: HashMap<String, Vec<String>> = HashMap::new();
        let dep_fact = |_: &str| -> Option<DepFact> { None };
        // child_of chain: bead → child epic (auto) → orphaned working epic.
        let (parents, dispatch_raw) = maps(
            &[
                ("gtcore-0179f8", "epic-child"),
                ("epic-child", "epic-orphan"),
            ],
            &[("epic-child", Some("auto"))],
        );
        // The lock set is computed exactly as FrontierSource::ready does: the
        // container epic is `working` with an empty owner.
        let working_epics = [("epic-orphan", "")];
        let locked = locked_roots(working_epics, &|a: &str| session_like_actor(a));
        assert!(
            locked.is_empty(),
            "orphaned working epic must not produce a lock root"
        );
        let occupied: HashSet<String> = HashSet::new();

        let frontier = ready_for_auto(
            rows,
            &deps_of,
            &dep_fact,
            IssuePhase::P1,
            &AllowAll,
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &HashSet::new(),
        );
        assert_eq!(
            frontier.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["gtcore-0179f8"],
            "bead under an orphaned working epic reaches the frontier"
        );
    }

    // --- gtcore-db99e0: the unified should_sling predicate ---

    #[test]
    fn should_sling_open_or_working_auto_task_is_slingable() {
        // The only YES cases: an open OR working, non-epic, dispatch=Auto bead.
        assert!(should_sling("open", "task", Dispatch::Auto));
        assert!(should_sling("working", "bug", Dispatch::Auto));
        assert!(should_sling("working", "feature", Dispatch::Auto));
        // Tolerant of casing / surrounding whitespace on the raw tracker columns.
        assert!(should_sling("  Working ", "Task", Dispatch::Auto));
    }

    #[test]
    fn should_sling_excludes_closed_epic_and_manual() {
        // Closed → delivered, nothing to sling (gtcore-e7a851: re-hydration re-slung a merged bead).
        assert!(!should_sling("closed", "task", Dispatch::Auto));
        // Epic → container; its children get slung, never the epic (gtcore-dc3c13).
        assert!(!should_sling("open", "epic", Dispatch::Auto));
        assert!(!should_sling("working", "epic", Dispatch::Auto));
        // Manual → operator-driven, never auto-(re)slung (gtcore-e7a851: 915232).
        assert!(!should_sling("open", "task", Dispatch::Manual));
        assert!(!should_sling("working", "task", Dispatch::Manual));
        // An unknown/empty status (neither open nor working) is not slingable.
        assert!(!should_sling("", "task", Dispatch::Auto));
    }

    #[test]
    fn ready_for_auto_never_includes_an_epic_but_keeps_its_children() {
        // gtcore-dc3c13: an open + dispatch=auto EPIC used to satisfy is_ready and land a polecat
        // on the container. Its task/bug children must still reach the frontier.
        let epic = {
            let mut r = auto_row("epic-1", r#"[]"#);
            r.issue_type = "epic".to_string();
            r
        };
        let child = auto_row("epic-1-task", r#"["crates/child"]"#);
        let rows = vec![epic, child];

        let deps_of: HashMap<String, Vec<String>> = HashMap::new();
        let dep_fact = |_: &str| -> Option<DepFact> { None };
        // The epic carries dispatch=auto itself; the child inherits it via child_of.
        let (parents, dispatch_raw) =
            maps(&[("epic-1-task", "epic-1")], &[("epic-1", Some("auto"))]);
        let locked: HashSet<String> = HashSet::new();
        let occupied: HashSet<String> = HashSet::new();

        let frontier = ready_for_auto(
            rows,
            &deps_of,
            &dep_fact,
            IssuePhase::P1,
            &AllowAll,
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &HashSet::new(),
        );
        assert_eq!(
            frontier.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["epic-1-task"],
            "the epic is excluded; its child is on the frontier"
        );
    }
}
