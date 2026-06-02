//! Module dependency graph snapshot for runtime disable-safety.
//!
//! Resolves the gap `feature.disable.validate dep-aware rejection needs module
//! dependency graph at runtime`: `gt-module`'s builder already rejects, *at
//! composition*, a module that depends on a disabled one (`hq-mod-core.7`), but
//! `feature.disable` toggles a flag at *runtime*, long after the graph was built.
//! Disabling `module.<x>` while an enabled `module.<y>` still depends on `<x>`
//! would strand `<y>` with a missing dependency. This module gives
//! [`run_disable_feature`](crate::run_disable_feature) the dependency view it
//! needs to reject that before the override is written.
//!
//! ## Why a string snapshot, not a `gt-module` dependency
//!
//! `gt-feature-flags` is a kernel crate that deliberately speaks in plain `&str`
//! ids (the same reason [`repo`](crate::repo) takes a `&str` workspace, not the
//! `gt-workspace` type): pulling in `gt-module` to read `ModuleId` /
//! `GtModule::dependencies()` would couple two kernel crates for nothing. So the
//! composition root — which already holds the registered modules — translates
//! each module's declared dependencies into `(dependent, dependency)` string
//! edges and hands them to [`ModuleDepGraph::from_edges`]. This crate stays a
//! pure, dependency-light consumer of that snapshot.

use std::collections::HashMap;

use crate::key::FlagKey;

/// The `module.` key prefix that marks a flag toggling a whole module (as
/// opposed to a finer `feature.<name>` capability).
const MODULE_PREFIX: &str = "module.";

/// If `key` toggles a module (`module.<id>`), the module id it controls.
///
/// A `feature.*` (or any non-`module.`) key is not a module: it has no
/// dependency edges, so disabling it is always dep-safe and this returns `None`.
pub fn module_id_of(key: &FlagKey) -> Option<&str> {
    key.as_str().strip_prefix(MODULE_PREFIX)
}

/// A read-only snapshot of the module dependency graph, built once at
/// composition and consulted by `feature.disable` to keep a runtime toggle from
/// orphaning an enabled dependent.
///
/// Stores the *reverse* edges (module → the modules that directly depend on it),
/// because the disable check only ever asks "who depends on the module I am about
/// to turn off?". Construction is the only place forward edges are read.
#[derive(Debug, Clone, Default)]
pub struct ModuleDepGraph {
    /// module id → ids that directly declare it as a dependency, sorted + deduped
    /// for a deterministic rejection message.
    dependents: HashMap<String, Vec<String>>,
}

impl ModuleDepGraph {
    /// An empty graph (no module declares any dependency). Every disable is
    /// dep-safe under it.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from `(dependent, dependency)` edges, where `dependent` requires
    /// `dependency`. Duplicate edges collapse; dependents are sorted so the
    /// rejection message is stable across runs (replay-friendly).
    pub fn from_edges(edges: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        for (dependent, dependency) in edges {
            let v = dependents.entry(dependency).or_default();
            if !v.contains(&dependent) {
                v.push(dependent);
            }
        }
        for v in dependents.values_mut() {
            v.sort();
        }
        Self { dependents }
    }

    /// The modules that directly declare `module` as a dependency. Empty slice if
    /// nothing depends on it.
    pub fn direct_dependents(&self, module: &str) -> &[String] {
        self.dependents.get(module).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// The still-enabled modules that directly depend on `target_module` and would
/// be orphaned by disabling it. `is_enabled(id)` resolves a module's current
/// effective state; an empty result means disabling `target_module` is dep-safe.
///
/// Pure: the `is_enabled` closure is the seam where the caller plugs in its
/// enabled-state source (the disable handler uses override presence; a caller
/// holding the full flag registry could pass a default-aware resolver) without
/// this predicate changing.
pub fn blocking_dependents<'g>(
    graph: &'g ModuleDepGraph,
    target_module: &str,
    is_enabled: impl Fn(&str) -> bool,
) -> Vec<&'g str> {
    graph
        .direct_dependents(target_module)
        .iter()
        .filter(|dep| is_enabled(dep))
        .map(String::as_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> FlagKey {
        FlagKey::new(s).unwrap()
    }

    #[test]
    fn module_id_extracted_only_from_module_keys() {
        assert_eq!(module_id_of(&key("module.beads")), Some("beads"));
        assert_eq!(module_id_of(&key("module.gt-rig")), Some("gt-rig"));
        // A finer feature flag is not a module → no dependency edges.
        assert_eq!(module_id_of(&key("feature.dark-mode")), None);
        // A bare `module` segment without an id is not a module key.
        assert_eq!(module_id_of(&key("module")), None);
    }

    #[test]
    fn from_edges_builds_reverse_graph_sorted_and_deduped() {
        // rigs and feed both depend on beads; feed also depends on audit.
        let g = ModuleDepGraph::from_edges([
            ("rigs".to_string(), "beads".to_string()),
            ("feed".to_string(), "beads".to_string()),
            ("feed".to_string(), "beads".to_string()), // duplicate collapses
            ("feed".to_string(), "audit".to_string()),
        ]);
        assert_eq!(g.direct_dependents("beads"), ["feed", "rigs"]); // sorted
        assert_eq!(g.direct_dependents("audit"), ["feed"]);
        // Nothing depends on rigs → safe leaf.
        assert!(g.direct_dependents("rigs").is_empty());
        assert!(g.direct_dependents("unknown").is_empty());
    }

    #[test]
    fn blocking_dependents_filters_by_enabled_state() {
        let g = ModuleDepGraph::from_edges([
            ("rigs".to_string(), "beads".to_string()),
            ("feed".to_string(), "beads".to_string()),
        ]);
        // All enabled → both block disabling beads.
        let mut blocking = blocking_dependents(&g, "beads", |_| true);
        blocking.sort();
        assert_eq!(blocking, ["feed", "rigs"]);
        // feed already disabled → only rigs blocks.
        let blocking = blocking_dependents(&g, "beads", |id| id != "feed");
        assert_eq!(blocking, ["rigs"]);
        // both dependents disabled → safe to disable beads.
        assert!(blocking_dependents(&g, "beads", |_| false).is_empty());
    }

    #[test]
    fn leaf_with_no_dependents_is_always_safe() {
        let g = ModuleDepGraph::from_edges([("rigs".to_string(), "beads".to_string())]);
        assert!(blocking_dependents(&g, "rigs", |_| true).is_empty());
    }
}
