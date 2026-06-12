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

use std::collections::HashMap;

use gt_store_dolt::IssueRow;

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
}
