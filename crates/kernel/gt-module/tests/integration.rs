//! Integration matrix for the `gt-module` composition seam (`hq-mod-core.8`).
//!
//! Black-box: drives only the crate's public API the way an app composition
//! root would, exercising the four shapes the builder must handle —
//! empty, single-module, dependency cycle, capability conflict — plus the
//! dependency-ordering and feature-flag behavior the unit tests cover in
//! isolation. If a later bead changes an internal detail but keeps the public
//! contract, these stay green.

use gt_module::{
    BuildError, Capability, DisabledModules, GtModule, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// Test module: configurable id, dependencies, and claimed scopes — enough to
/// drive every builder path from outside the crate.
struct TestModule {
    id: &'static str,
    deps: &'static [&'static str],
    scopes: &'static [&'static str],
}

impl GtModule for TestModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            ModuleId::new(self.id).unwrap(),
            self.id,
            Version::new(1, 0, 0),
            "integration test module",
        )
    }

    fn capability(&self) -> Capability {
        Capability::empty().claiming_all(self.scopes.iter().map(|s| Scope::new(*s).unwrap()))
    }

    fn dependencies(&self) -> Vec<ModuleId> {
        self.deps.iter().map(|d| ModuleId::new(*d).unwrap()).collect()
    }
}

const fn module(
    id: &'static str,
    deps: &'static [&'static str],
    scopes: &'static [&'static str],
) -> TestModule {
    TestModule { id, deps, scopes }
}

/// Collect built module ids in init order.
fn order(root: &gt_module::Root) -> Vec<String> {
    root.modules().map(|m| m.id.to_string()).collect()
}

#[test]
fn empty_stack_builds_to_empty_root() {
    let root = gt_module::RootBuilder::new().build().unwrap();
    assert!(root.is_empty());
    assert_eq!(root.len(), 0);
}

#[test]
fn single_module_builds_and_is_queryable() {
    let root = gt_module::RootBuilder::new()
        .module(module("beads", &[], &["beads.read", "beads.write"]))
        .build()
        .unwrap();
    assert_eq!(root.len(), 1);
    let id = ModuleId::new("beads").unwrap();
    assert_eq!(root.module(&id).unwrap().name, "beads");
}

#[test]
fn realistic_stack_orders_dependencies_first() {
    // feed depends on beads; merge depends on beads + rigs. Registered shuffled.
    let root = gt_module::RootBuilder::new()
        .module(module("merge", &["beads", "rigs"], &["merge.read"]))
        .module(module("feed", &["beads"], &["feed.read"]))
        .module(module("beads", &[], &["beads.read"]))
        .module(module("rigs", &[], &["rigs.read"]))
        .build()
        .unwrap();
    let got = order(&root);
    let pos = |id: &str| got.iter().position(|x| x == id).unwrap();
    assert!(pos("beads") < pos("feed"));
    assert!(pos("beads") < pos("merge"));
    assert!(pos("rigs") < pos("merge"));
    assert_eq!(root.len(), 4);
}

#[test]
fn dependency_cycle_is_rejected() {
    let err = gt_module::RootBuilder::new()
        .module(module("a", &["b"], &[]))
        .module(module("b", &["a"], &[]))
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::DependencyCycle(_)), "got {err:?}");
}

#[test]
fn unknown_dependency_is_rejected() {
    let err = gt_module::RootBuilder::new()
        .module(module("a", &["ghost"], &[]))
        .build()
        .unwrap_err();
    let BuildError::UnknownDependency { module, missing } = err else {
        panic!("expected UnknownDependency, got {err:?}");
    };
    assert_eq!(module.as_str(), "a");
    assert_eq!(missing.as_str(), "ghost");
}

#[test]
fn capability_scope_conflict_is_rejected() {
    let err = gt_module::RootBuilder::new()
        .module(module("beads", &[], &["beads.write"]))
        .module(module("legacy", &[], &["beads.write"]))
        .build()
        .unwrap_err();
    let BuildError::ScopeConflict { scope, claimants } = err else {
        panic!("expected ScopeConflict, got {err:?}");
    };
    assert_eq!(scope, Scope::new("beads.write").unwrap());
    assert_eq!(claimants.len(), 2);
}

#[test]
fn feature_flag_drops_disabled_module() {
    let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
    let root = gt_module::RootBuilder::new()
        .module(module("beads", &[], &["beads.read"]))
        .module(module("rigs", &[], &["rigs.read"]))
        .build_with_flags(&flags)
        .unwrap();
    assert_eq!(order(&root), ["beads"]);
}

#[test]
fn disabling_a_dependency_in_use_is_rejected() {
    let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
    let err = gt_module::RootBuilder::new()
        .module(module("beads", &[], &[]))
        .module(module("merge", &["beads"], &[]))
        .build_with_flags(&flags)
        .unwrap_err();
    assert!(matches!(err, BuildError::DisabledDependency { .. }), "got {err:?}");
}

#[test]
fn disabling_a_scope_claimant_clears_the_conflict() {
    // Two modules claim the same scope; disabling one makes the stack buildable.
    let flags = DisabledModules::new([ModuleId::new("legacy").unwrap()]);
    let root = gt_module::RootBuilder::new()
        .module(module("beads", &[], &["beads.write"]))
        .module(module("legacy", &[], &["beads.write"]))
        .build_with_flags(&flags)
        .unwrap();
    assert_eq!(order(&root), ["beads"]);
}
