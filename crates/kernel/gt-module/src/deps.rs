//! Dependency-cycle detection + topological ordering of modules.
//!
//! Landed by [`hq-mod-core.5`]. A module declares the other modules it requires
//! through [`GtModule::dependencies`](crate::GtModule::dependencies); the builder
//! must initialize each module *after* every module it depends on. This pass
//! turns the registered set into that init order, and rejects the two ways the
//! graph can be unbuildable:
//!
//! - a declared dependency names a module that was never registered
//!   ([`BuildError::UnknownDependency`]);
//! - the dependencies form a cycle, so no valid init order exists
//!   ([`BuildError::DependencyCycle`]).
//!
//! The algorithm is Kahn's topological sort (sync, allocation-light, no
//! recursion — non-negotiable #2). Among modules that are simultaneously ready
//! (no unsatisfied dependency) it preserves registration order, so a set with no
//! dependencies builds in exactly the order the author wrote, and the ordering
//! is deterministic for replay.

use std::collections::HashMap;
use std::collections::VecDeque;

use crate::builder::{BuildError, ModuleEntry};

/// Resolve module init order: every module appears after all of its
/// dependencies. Returns indices into `entries` in topological order.
///
/// Errors with [`BuildError::UnknownDependency`] if a module depends on an id
/// that is not present, or [`BuildError::DependencyCycle`] if the graph cannot
/// be linearized.
pub(crate) fn resolve_order(entries: &[ModuleEntry]) -> Result<Vec<usize>, BuildError> {
    let n = entries.len();

    // id -> registration index. A duplicate id keeps the first registration;
    // genuine duplicate-module-id rejection is capability-conflict territory
    // (`hq-mod-core.6`), not this pass.
    let mut index_of: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, e) in entries.iter().enumerate() {
        index_of.entry(e.meta.id.as_str()).or_insert(i);
    }

    // indegree[i] = number of i's dependencies; dependents[j] = modules that
    // depend on j (the edges j -> i we relax once j is emitted).
    let mut indegree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, e) in entries.iter().enumerate() {
        for dep in &e.depends_on {
            let Some(&j) = index_of.get(dep.as_str()) else {
                return Err(BuildError::UnknownDependency {
                    module: e.meta.id.clone(),
                    missing: dep.clone(),
                });
            };
            indegree[i] += 1;
            dependents[j].push(i);
        }
    }

    // Kahn: seed with the already-satisfied modules in registration order, then
    // relax. Pushing newly-ready modules to the back keeps the output stable.
    let mut ready: VecDeque<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    while let Some(i) = ready.pop_front() {
        order.push(i);
        for &k in &dependents[i] {
            indegree[k] -= 1;
            if indegree[k] == 0 {
                ready.push_back(k);
            }
        }
    }

    if order.len() != n {
        // Whatever still carries indegree is on or downstream of a cycle.
        let cycle = (0..n)
            .filter(|&i| indegree[i] > 0)
            .map(|i| entries[i].meta.id.clone())
            .collect();
        return Err(BuildError::DependencyCycle(cycle));
    }

    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::Capability;
    use crate::meta::{ModuleId, ModuleMeta};
    use semver::Version;

    /// Build a bare entry with the given id and dependency ids.
    fn entry(id: &str, deps: &[&str]) -> ModuleEntry {
        ModuleEntry {
            meta: ModuleMeta::new(
                ModuleId::new(id).unwrap(),
                id,
                Version::new(1, 0, 0),
                "test module",
            ),
            capability: Capability::empty(),
            depends_on: deps.iter().map(|d| ModuleId::new(*d).unwrap()).collect(),
            mcp_tools: Vec::new(),
        }
    }

    /// Map a resolved index order back to ids for readable assertions.
    fn ids(entries: &[ModuleEntry], order: &[usize]) -> Vec<String> {
        order.iter().map(|&i| entries[i].meta.id.to_string()).collect()
    }

    #[test]
    fn no_deps_preserves_registration_order() {
        let es = [entry("a", &[]), entry("b", &[]), entry("c", &[])];
        let order = resolve_order(&es).unwrap();
        assert_eq!(ids(&es, &order), ["a", "b", "c"]);
    }

    #[test]
    fn linear_chain_orders_dependency_first() {
        // c depends on b, b depends on a — registered out of order.
        let es = [entry("c", &["b"]), entry("b", &["a"]), entry("a", &[])];
        let order = resolve_order(&es).unwrap();
        assert_eq!(ids(&es, &order), ["a", "b", "c"]);
    }

    #[test]
    fn diamond_places_root_first_and_sink_last() {
        // d depends on b and c; b and c depend on a.
        let es = [
            entry("a", &[]),
            entry("b", &["a"]),
            entry("c", &["a"]),
            entry("d", &["b", "c"]),
        ];
        let order = resolve_order(&es).unwrap();
        let got = ids(&es, &order);
        let pos = |id: &str| got.iter().position(|x| x == id).unwrap();
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
    }

    #[test]
    fn detects_two_node_cycle() {
        let es = [entry("a", &["b"]), entry("b", &["a"])];
        match resolve_order(&es) {
            Err(BuildError::DependencyCycle(ids)) => {
                let mut ids: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
                ids.sort();
                assert_eq!(ids, ["a", "b"]);
            }
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn detects_self_cycle() {
        let es = [entry("a", &["a"])];
        assert!(matches!(
            resolve_order(&es),
            Err(BuildError::DependencyCycle(_))
        ));
    }

    #[test]
    fn rejects_unknown_dependency() {
        let es = [entry("a", &["ghost"])];
        match resolve_order(&es) {
            Err(BuildError::UnknownDependency { module, missing }) => {
                assert_eq!(module.as_str(), "a");
                assert_eq!(missing.as_str(), "ghost");
            }
            other => panic!("expected UnknownDependency, got {other:?}"),
        }
    }

    #[test]
    fn empty_set_is_empty_order() {
        let es: [ModuleEntry; 0] = [];
        assert_eq!(resolve_order(&es).unwrap(), Vec::<usize>::new());
    }
}
