//! Auto-derive `depends_on` edges from the Cargo graph (hq-core-mcp.12 §A).
//!
//! `depends_on` is hand-maintained and drifts; the Cargo manifest is
//! authoritative. For a bead X whose surface owns crate A, and A depends on
//! workspace crate B, the *correct* artifact-dep is `X → owner(B)` — where
//! `owner(B)` is the closed bead that delivered B. This module computes the set of
//! such edges that are **missing** from the recorded `depends_on`, so the caller
//! can MERGE them in (never overwrite — an existing edge, e.g. the dirty
//! `migrate.5 → migrate.4`, is left untouched).
//!
//! Pure: it takes the [`CrateGraph`] + the parsed [`Bead`] list and returns the
//! delta, so it unit-tests without git or a store.

use std::collections::{BTreeMap, BTreeSet};

use crate::cargo_graph::CrateGraph;
use crate::model::Bead;

/// The result of the §A diff.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct EdgeDerivation {
    /// `(bead_id, target_bead_id)` edges that are correct per the Cargo graph but
    /// absent from the bead's recorded `depends_on`. Sorted + deduped.
    pub missing_edges: Vec<(String, String)>,
    /// Crates a bead consumes that no *closed* bead delivered — there is no
    /// producer node to point at. Reported (not auto-minted) so an operator mints
    /// a producer bead deliberately (`hq-core-port` pattern); auto-creating beads
    /// from a batch job risks junk rows. `crate name → consumer bead ids`.
    pub missing_producers: BTreeMap<String, BTreeSet<String>>,
}

/// The crates a bead's surface owns (each non-stale surface path mapped through
/// [`CrateGraph::owning_crate`]). Both `planned` and real paths count — a producer
/// owns the crate it will create just as a consumer owns the crate it edits.
fn owned_crates(bead: &Bead, graph: &CrateGraph) -> BTreeSet<String> {
    bead.surface
        .iter()
        .filter_map(|e| graph.owning_crate(&e.path).map(str::to_string))
        .collect()
}

/// `crate name → the closed bead that delivered it` (§A `owner(B)`): among closed
/// beads whose surface owns the crate, the one with the **earliest** `closed_at`
/// (the first delivery wins; ISO-8601 sorts lexicographically). A bead with no
/// `closed_at` sorts last so a properly-timestamped delivery is preferred.
pub fn owner_index(beads: &[Bead], graph: &CrateGraph) -> BTreeMap<String, String> {
    let mut owners: BTreeMap<String, (String, String)> = BTreeMap::new(); // crate -> (closed_at, id)
    for bead in beads {
        if bead.status != "closed" {
            continue;
        }
        // No timestamp → sort after any real one (use a sentinel that is
        // lexicographically greater than any ISO-8601 stamp).
        let key = bead.closed_at.clone().unwrap_or_else(|| "~".to_string());
        for c in owned_crates(bead, graph) {
            match owners.get(&c) {
                Some((existing_key, _)) if *existing_key <= key => {}
                _ => {
                    owners.insert(c, (key.clone(), bead.id.clone()));
                }
            }
        }
    }
    owners
        .into_iter()
        .map(|(c, (_, id))| (c, id))
        .collect()
}

/// Compute the missing `X → owner(B)` edges across all beads (§A).
pub fn derive_missing_edges(graph: &CrateGraph, beads: &[Bead]) -> EdgeDerivation {
    let owners = owner_index(beads, graph);
    let mut missing_edges: BTreeSet<(String, String)> = BTreeSet::new();
    let mut missing_producers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for bead in beads {
        let recorded: BTreeSet<&str> = bead.depends_on.iter().map(String::as_str).collect();
        for crate_a in owned_crates(bead, graph) {
            let Some(crate_deps) = graph.deps.get(&crate_a) else {
                continue;
            };
            for crate_b in crate_deps {
                match owners.get(crate_b) {
                    Some(owner) if *owner != bead.id => {
                        if !recorded.contains(owner.as_str()) {
                            missing_edges.insert((bead.id.clone(), owner.clone()));
                        }
                    }
                    Some(_) => {} // self-delivery: the bead owns B itself, no edge.
                    None => {
                        missing_producers
                            .entry(crate_b.clone())
                            .or_default()
                            .insert(bead.id.clone());
                    }
                }
            }
        }
    }

    EdgeDerivation {
        missing_edges: missing_edges.into_iter().collect(),
        missing_producers,
    }
}

/// True when `goal` is reachable from `start` by following `depends_on` edges in
/// `adj` (`adj[x]` = the ids `x` depends on). Used to detect that adding
/// `from → to` would close a cycle (i.e. `to` already reaches `from`).
fn reachable(adj: &BTreeMap<String, BTreeSet<String>>, start: &str, goal: &str) -> bool {
    let mut stack = vec![start.to_string()];
    let mut seen: BTreeSet<String> = BTreeSet::new();
    while let Some(node) = stack.pop() {
        if node == goal {
            return true;
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        if let Some(deps) = adj.get(&node) {
            stack.extend(deps.iter().cloned());
        }
    }
    false
}

/// Filter proposed edges to those that do NOT create a cycle, given the existing
/// `depends_on` graph (the server rejects cyclic `depends_on`). Edges are taken in
/// the given order; each accepted edge updates the working adjacency so the batch
/// itself also stays acyclic. Returns `(acyclic, skipped_cyclic)`.
///
/// An edge `from → to` is cyclic when `to` already reaches `from` — adding it
/// would let `from` reach itself. The pre-existing reverse edge is left untouched
/// (we only ever add), so the skip is reported, not resolved here.
pub fn filter_acyclic(
    beads: &[Bead],
    proposed: &[(String, String)],
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let mut adj: BTreeMap<String, BTreeSet<String>> = beads
        .iter()
        .map(|b| (b.id.clone(), b.depends_on.iter().cloned().collect()))
        .collect();
    let mut acyclic = Vec::new();
    let mut skipped = Vec::new();
    for (from, to) in proposed {
        if from == to || reachable(&adj, to, from) {
            skipped.push((from.clone(), to.clone()));
        } else {
            adj.entry(from.clone()).or_default().insert(to.clone());
            acyclic.push((from.clone(), to.clone()));
        }
    }
    (acyclic, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_issues::SurfaceEntry;

    fn bead(id: &str, status: &str, closed_at: Option<&str>, surfaces: &[&str], deps: &[&str]) -> Bead {
        Bead {
            id: id.into(),
            issue_type: "task".into(),
            status: status.into(),
            closed_at: closed_at.map(str::to_string),
            surface: surfaces
                .iter()
                .map(|p| SurfaceEntry { path: p.to_string(), planned: false })
                .collect(),
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            delivered: false,
        }
    }

    /// A→B (A=gt-issues depends on B=gt-module); a closed bead delivered each.
    fn graph() -> CrateGraph {
        let json = r#"{"packages":[
          {"name":"gt-module","manifest_path":"/repo/crates/kernel/gt-module/Cargo.toml","dependencies":[]},
          {"name":"gt-issues","manifest_path":"/repo/crates/domain/platform/gt-issues/Cargo.toml","dependencies":[{"name":"gt-module"}]}
        ]}"#;
        CrateGraph::from_metadata(json, "/repo").unwrap()
    }

    #[test]
    fn derives_x_to_owner_of_b() {
        let g = graph();
        let beads = vec![
            // producer of gt-module, closed first.
            bead("prod.module", "closed", Some("2026-01-01T00:00:00Z"), &["crates/kernel/gt-module"], &[]),
            // X owns gt-issues (which depends on gt-module) but records no edge.
            bead("x.consumer", "open", None, &["crates/domain/platform/gt-issues/src/lib.rs"], &[]),
        ];
        let d = derive_missing_edges(&g, &beads);
        assert_eq!(d.missing_edges, vec![("x.consumer".into(), "prod.module".into())]);
        assert!(d.missing_producers.is_empty());
    }

    #[test]
    fn idempotent_when_edge_already_recorded() {
        let g = graph();
        let beads = vec![
            bead("prod.module", "closed", Some("2026-01-01T00:00:00Z"), &["crates/kernel/gt-module"], &[]),
            // already records the edge → no new edge.
            bead("x.consumer", "open", None, &["crates/domain/platform/gt-issues/src/lib.rs"], &["prod.module"]),
        ];
        let d = derive_missing_edges(&g, &beads);
        assert!(d.missing_edges.is_empty(), "re-run yields zero new edges");
    }

    #[test]
    fn earliest_close_owns_the_crate() {
        let g = graph();
        let beads = vec![
            bead("late", "closed", Some("2026-05-01T00:00:00Z"), &["crates/kernel/gt-module"], &[]),
            bead("early", "closed", Some("2026-01-01T00:00:00Z"), &["crates/kernel/gt-module"], &[]),
            bead("x", "open", None, &["crates/domain/platform/gt-issues/src/lib.rs"], &[]),
        ];
        let d = derive_missing_edges(&g, &beads);
        assert_eq!(d.missing_edges, vec![("x".into(), "early".into())]);
    }

    #[test]
    fn cyclic_edge_is_skipped() {
        // existing: b depends_on a. proposed: a -> b would cycle (b reaches a).
        let beads = vec![
            bead("a", "open", None, &[], &[]),
            bead("b", "open", None, &[], &["a"]),
        ];
        let (ok, skipped) = filter_acyclic(&beads, &[("a".into(), "b".into())]);
        assert!(ok.is_empty());
        assert_eq!(skipped, vec![("a".into(), "b".into())]);
        // a non-cyclic edge is kept.
        let (ok, skipped) = filter_acyclic(&beads, &[("a".into(), "c".into())]);
        assert_eq!(ok, vec![("a".into(), "c".into())]);
        assert!(skipped.is_empty());
    }

    #[test]
    fn no_producer_is_reported_not_minted() {
        let g = graph();
        // gt-module has no closed deliverer → consuming gt-issues yields a
        // missing-producer report, no edge.
        let beads = vec![bead("x", "open", None, &["crates/domain/platform/gt-issues/src/lib.rs"], &[])];
        let d = derive_missing_edges(&g, &beads);
        assert!(d.missing_edges.is_empty());
        assert!(d.missing_producers.contains_key("gt-module"));
    }
}
