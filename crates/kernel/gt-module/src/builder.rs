//! `RootBuilder` — the single composition seam that assembles a list of
//! [`GtModule`]s into a built [`Root`].
//!
//! Landed by [`hq-mod-core.4`]: the skeleton — register modules with a generic
//! `.module::<M>()` chain, then [`build`](RootBuilder::build). The app
//! composition root hand-wires nothing (see `docs/03-architecture-guardrails.md`
//! rule 3); it writes `RootBuilder::new().module(BeadsModule).module(RigModule).build()?`.
//!
//! ## What this bead does and does not do
//!
//! `.module::<M>()` consumes the module value, calls its pure-data accessors
//! ([`meta`](GtModule::meta), [`capability`](GtModule::capability)) eagerly, and
//! stores **only the extracted data** — never the module itself. There is no
//! `Box<dyn GtModule>` in the registry: non-negotiable #1 confines `dyn` to
//! `gt-plugin`, so the builder relies on static dispatch through the generic
//! call site. Behavioral contributions (routes, MCP tools, actors, migrations)
//! attach in later epics by growing [`Capability`] and the per-contribution
//! registration hooks; each is additive.
//!
//! [`build`](RootBuilder::build) returns a [`Result`] so the validation beads
//! slot in without churning the signature:
//!
//! - dependency-cycle detection + topological ordering — `hq-mod-core.5` (done)
//! - capability-conflict detection — `hq-mod-core.6` (done)
//! - feature-flag filtering of disabled modules — `hq-mod-core.7`
//!
//! Each grows a [`BuildError`] variant. As of `.5`, `build()` orders the
//! registered modules so each appears after every module it depends on (see
//! [`crate::deps`]); as of `.6` it also rejects two modules claiming the same
//! authorization scope.

use std::collections::{BTreeMap, BTreeSet};

use crate::capability::Capability;
use crate::flags::{AllEnabled, FeatureFlags};
use crate::mcp::{McpRegistry, McpTool};
use crate::meta::{ModuleId, ModuleMeta};
use crate::module_trait::GtModule;
use crate::scope::Scope;

/// Accumulates modules and assembles them into a [`Root`].
///
/// Constructed with [`RootBuilder::new`], extended one module at a time through
/// the generic [`module`](RootBuilder::module) method, and finalized with
/// [`build`](RootBuilder::build). Holds extracted data only, so it is cheap and
/// carries no runtime handles.
#[derive(Debug, Default)]
pub struct RootBuilder {
    /// One entry per registered module, in registration order.
    entries: Vec<ModuleEntry>,
}

/// Pure-data snapshot the builder keeps for one registered module.
///
/// The originating `M: GtModule` value is dropped after extraction; everything
/// the builder and the validation beads need is captured here. `pub(crate)` so
/// the sibling validation passes ([`crate::deps`]) can read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModuleEntry {
    pub(crate) meta: ModuleMeta,
    pub(crate) capability: Capability,
    pub(crate) depends_on: Vec<ModuleId>,
    /// MCP tools this module contributes, in declaration order (`hq-mod-mcp.1`).
    pub(crate) mcp_tools: Vec<McpTool>,
}

impl RootBuilder {
    /// Start an empty builder.
    pub fn new() -> Self {
        RootBuilder::default()
    }

    /// Register one module.
    ///
    /// Takes the module by value (implementors are zero-sized marker structs),
    /// reads its pure-data [`meta`](GtModule::meta) and
    /// [`capability`](GtModule::capability), harvests the MCP tools it pushes
    /// into a fresh [`McpRegistry`](crate::McpRegistry) via
    /// [`register_mcp_tools`](GtModule::register_mcp_tools) (`hq-mod-mcp.1`),
    /// stores the snapshot, and drops the value. Returns `self` so registrations
    /// chain. Dispatch is static — no trait object is retained (non-negotiable
    /// #1).
    pub fn module<M: GtModule>(mut self, module: M) -> Self {
        let mut mcp = McpRegistry::new();
        module.register_mcp_tools(&mut mcp);
        self.entries.push(ModuleEntry {
            meta: module.meta(),
            capability: module.capability(),
            depends_on: module.dependencies(),
            mcp_tools: mcp.into_tools(),
        });
        self
    }

    /// Finalize the registry with every module enabled.
    ///
    /// Shorthand for [`build_with_flags`](RootBuilder::build_with_flags) with
    /// [`AllEnabled`]. See that method for the validation passes.
    pub fn build(self) -> Result<Root, BuildError> {
        self.build_with_flags(&AllEnabled)
    }

    /// Finalize the registry, dropping modules `flags` reports as disabled.
    ///
    /// Runs, in order, and returns the first [`BuildError`]:
    ///
    /// 1. drop every module for which `flags.is_enabled` is false
    ///    (`hq-mod-core.7`);
    /// 2. reject an enabled module that depends on a disabled one
    ///    ([`BuildError::DisabledDependency`]) — a disabled dependency is a
    ///    configuration mistake, not a silent cascade;
    /// 3. reject two enabled modules claiming the same scope (`hq-mod-core.6`);
    /// 4. resolve the dependency graph into init order ([`crate::deps`],
    ///    `hq-mod-core.5`).
    ///
    /// Dispatch over `flags` is static (non-negotiable #1): no trait object is
    /// stored or boxed.
    pub fn build_with_flags<F: FeatureFlags>(self, flags: &F) -> Result<Root, BuildError> {
        // 1. Partition by flag. Keep enabled entries; remember disabled ids so a
        //    dangling enabled->disabled edge reports precisely. Owned ids so the
        //    set outlives the `into_iter` move below.
        let disabled: BTreeSet<ModuleId> = self
            .entries
            .iter()
            .map(|e| &e.meta.id)
            .filter(|id| !flags.is_enabled(id))
            .cloned()
            .collect();
        // 2. An enabled module may not depend on a disabled one.
        for entry in &self.entries {
            if disabled.contains(&entry.meta.id) {
                continue;
            }
            for dep in &entry.depends_on {
                if disabled.contains(dep) {
                    return Err(BuildError::DisabledDependency {
                        module: entry.meta.id.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
        let enabled: Vec<ModuleEntry> = self
            .entries
            .into_iter()
            .filter(|e| !disabled.contains(&e.meta.id))
            .collect();

        // 3 + 4. Validate and order the surviving set.
        Self::check_scope_conflicts(&enabled)?;
        let order = crate::deps::resolve_order(&enabled)?;
        // Reorder into init order. The module set is a handful of entries, so
        // cloning by index is cheaper than threading an in-place permutation.
        let entries = order.iter().map(|&i| enabled[i].clone()).collect();
        Ok(Root { entries })
    }

    /// Reject a stack where two distinct modules claim the same authorization
    /// scope (`hq-mod-core.6`).
    ///
    /// A scope names a single source of truth for a `<resource>.<verb>`
    /// authority; two modules owning it is a wiring bug (overlapping route
    /// guards, ambiguous audit attribution). Detection is deterministic: scopes
    /// are visited in sorted order, so a given malformed stack always reports the
    /// same conflict. A module listing the same scope twice is not a conflict —
    /// only distinct claimants count.
    fn check_scope_conflicts(entries: &[ModuleEntry]) -> Result<(), BuildError> {
        let mut claimants: BTreeMap<&Scope, BTreeSet<&ModuleId>> = BTreeMap::new();
        for entry in entries {
            for scope in entry.capability.scopes() {
                claimants.entry(scope).or_default().insert(&entry.meta.id);
            }
        }
        for (scope, owners) in claimants {
            if owners.len() > 1 {
                return Err(BuildError::ScopeConflict {
                    scope: scope.clone(),
                    claimants: owners.into_iter().cloned().collect(),
                });
            }
        }
        Ok(())
    }
}

/// Why [`RootBuilder::build`] rejected a module set.
///
/// Marked `#[non_exhaustive]` so later epics can add variants — and so
/// downstream `match` arms keep a wildcard from day one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    /// A module declared a dependency on a module id that was never registered.
    UnknownDependency {
        /// The module carrying the dangling dependency.
        module: ModuleId,
        /// The unregistered id it named.
        missing: ModuleId,
    },
    /// An enabled module depends on a module the feature flags disabled
    /// (`hq-mod-core.7`). Enable the dependency or disable the dependent.
    DisabledDependency {
        /// The enabled module with the unsatisfiable dependency.
        module: ModuleId,
        /// The disabled module it depends on.
        dependency: ModuleId,
    },
    /// The module dependency graph contains a cycle, so no init order exists.
    /// Holds the ids on or downstream of the cycle.
    DependencyCycle(Vec<ModuleId>),
    /// Two or more distinct modules claim the same authorization scope
    /// (`hq-mod-core.6`). A scope must have exactly one owning module.
    ScopeConflict {
        /// The contested `<resource>.<verb>` scope.
        scope: Scope,
        /// The modules claiming it, sorted by id (always 2+).
        claimants: Vec<ModuleId>,
    },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::UnknownDependency { module, missing } => {
                write!(f, "module {module} depends on unregistered module {missing}")
            }
            BuildError::DisabledDependency { module, dependency } => {
                write!(f, "enabled module {module} depends on disabled module {dependency}")
            }
            BuildError::DependencyCycle(ids) => {
                write!(f, "module dependency cycle among: ")?;
                for (i, id) in ids.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{id}")?;
                }
                Ok(())
            }
            BuildError::ScopeConflict { scope, claimants } => {
                let ids: Vec<&str> = claimants.iter().map(ModuleId::as_str).collect();
                write!(
                    f,
                    "scope `{scope}` is claimed by multiple modules: {}",
                    ids.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// The assembled module registry produced by [`RootBuilder::build`].
///
/// Read-only view over the registered modules' metadata. Later epics extend it
/// with the wired routers, MCP tool tables, and lifecycle handles; for now it
/// exposes the module list so the composition root and diagnostics can enumerate
/// what was loaded.
#[derive(Debug)]
pub struct Root {
    entries: Vec<ModuleEntry>,
}

impl Root {
    /// Number of registered modules.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no modules were registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Metadata of every registered module, in init order — each module after
    /// every module it depends on (`hq-mod-core.5`). With no dependencies this
    /// is the registration order.
    pub fn modules(&self) -> impl Iterator<Item = &ModuleMeta> {
        self.entries.iter().map(|e| &e.meta)
    }

    /// Look up a module's metadata by id.
    pub fn module(&self, id: &ModuleId) -> Option<&ModuleMeta> {
        self.entries
            .iter()
            .map(|e| &e.meta)
            .find(|m| &m.id == id)
    }

    /// Every MCP tool contributed by the loaded modules, in module init order
    /// then per-module declaration order (`hq-mod-mcp.1`).
    ///
    /// Tools from feature-flag-disabled modules never appear: a disabled module
    /// is dropped before its [`ModuleEntry`] reaches the [`Root`], so its tools
    /// drop with it.
    pub fn mcp_tools(&self) -> impl Iterator<Item = &McpTool> {
        self.entries.iter().flat_map(|e| e.mcp_tools.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use semver::Version;

    struct Beads;
    impl GtModule for Beads {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("beads").unwrap(),
                "Beads",
                Version::new(1, 0, 0),
                "Issue tracking aggregate.",
            )
        }
    }

    struct Rigs;
    impl GtModule for Rigs {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("rigs").unwrap(),
                "Rigs",
                Version::new(0, 2, 0),
                "Repo + worktree registry.",
            )
        }
    }

    /// Depends on `beads`; used to assert build orders dependencies first.
    struct Merge;
    impl GtModule for Merge {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("merge").unwrap(),
                "Merge",
                Version::new(1, 0, 0),
                "Merge queue over beads.",
            )
        }
        fn dependencies(&self) -> Vec<ModuleId> {
            vec![ModuleId::new("beads").unwrap()]
        }
    }

    #[test]
    fn empty_builder_builds_empty_root() {
        let root = RootBuilder::new().build().unwrap();
        assert!(root.is_empty());
        assert_eq!(root.len(), 0);
        assert_eq!(root.modules().count(), 0);
    }

    #[test]
    fn registers_modules_in_order() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        assert_eq!(root.len(), 2);
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "rigs"]);
    }

    #[test]
    fn looks_up_module_by_id() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        let id = ModuleId::new("rigs").unwrap();
        assert_eq!(root.module(&id).unwrap().version, Version::new(0, 2, 0));
        assert!(root.module(&ModuleId::new("absent").unwrap()).is_none());
    }

    #[test]
    fn build_orders_dependencies_before_dependents() {
        // Merge registered before its dependency beads — build must reorder.
        let root = RootBuilder::new().module(Merge).module(Beads).build().unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads", "merge"]);
    }

    #[test]
    fn build_rejects_unknown_dependency() {
        // Merge depends on beads, which is never registered.
        let err = RootBuilder::new().module(Merge).build().unwrap_err();
        assert!(matches!(err, BuildError::UnknownDependency { .. }));
    }

    use crate::scope::Scope;

    /// Module with a configurable id + claimed scopes, for conflict tests.
    struct Claimer {
        id: &'static str,
        scopes: Vec<&'static str>,
    }

    impl GtModule for Claimer {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new(self.id).unwrap(),
                self.id,
                Version::new(1, 0, 0),
                "scope-claiming test module",
            )
        }

        fn capability(&self) -> Capability {
            Capability::empty().claiming_all(self.scopes.iter().map(|s| Scope::new(*s).unwrap()))
        }
    }

    fn claimer(id: &'static str, scopes: &[&'static str]) -> Claimer {
        Claimer { id, scopes: scopes.to_vec() }
    }

    #[test]
    fn disjoint_scopes_build_cleanly() {
        let root = RootBuilder::new()
            .module(claimer("beads", &["beads.read", "beads.write"]))
            .module(claimer("rigs", &["rigs.read"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 2);
    }

    #[test]
    fn two_modules_same_scope_conflict() {
        let err = RootBuilder::new()
            .module(claimer("beads", &["beads.write"]))
            .module(claimer("beads-legacy", &["beads.write"]))
            .build()
            .unwrap_err();
        let BuildError::ScopeConflict { scope, claimants } = err else {
            panic!("expected ScopeConflict, got {err:?}");
        };
        assert_eq!(scope, Scope::new("beads.write").unwrap());
        // Sorted by id, both claimants present.
        let ids: Vec<String> = claimants.iter().map(|m| m.to_string()).collect();
        assert_eq!(ids, ["beads", "beads-legacy"]);
    }

    #[test]
    fn same_module_repeating_a_scope_is_not_a_conflict() {
        // One module listing the same scope twice is harmless.
        let root = RootBuilder::new()
            .module(claimer("beads", &["beads.write", "beads.write"]))
            .build()
            .unwrap();
        assert_eq!(root.len(), 1);
    }

    #[test]
    fn conflict_detection_is_deterministic() {
        // Two independent conflicts; the lexicographically-first scope wins.
        let make = || {
            RootBuilder::new()
                .module(claimer("a", &["alpha.read"]))
                .module(claimer("b", &["alpha.read", "zeta.read"]))
                .module(claimer("c", &["zeta.read"]))
                .build()
                .unwrap_err()
        };
        let BuildError::ScopeConflict { scope, .. } = make() else {
            panic!("expected ScopeConflict");
        };
        assert_eq!(scope, Scope::new("alpha.read").unwrap());
        // Repeated runs report the same scope.
        let BuildError::ScopeConflict { scope: again, .. } = make() else {
            panic!("expected ScopeConflict");
        };
        assert_eq!(again, Scope::new("alpha.read").unwrap());
    }

    #[test]
    fn conflict_message_names_scope_and_modules() {
        let err = RootBuilder::new()
            .module(claimer("beads", &["beads.write"]))
            .module(claimer("audit", &["beads.write"]))
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("beads.write"), "got: {msg}");
        assert!(msg.contains("audit") && msg.contains("beads"), "got: {msg}");
    }

    use crate::flags::DisabledModules;

    #[test]
    fn disabled_module_is_dropped_from_root() {
        let flags = DisabledModules::new([ModuleId::new("rigs").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Rigs)
            .build_with_flags(&flags)
            .unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["beads"]);
    }

    #[test]
    fn disabling_a_dependency_used_by_enabled_module_is_rejected() {
        // merge depends on beads; disabling beads leaves merge unsatisfiable.
        let flags = DisabledModules::new([ModuleId::new("beads").unwrap()]);
        let err = RootBuilder::new()
            .module(Beads)
            .module(Merge)
            .build_with_flags(&flags)
            .unwrap_err();
        let BuildError::DisabledDependency { module, dependency } = err else {
            panic!("expected DisabledDependency, got {err:?}");
        };
        assert_eq!(module.as_str(), "merge");
        assert_eq!(dependency.as_str(), "beads");
    }

    #[test]
    fn disabling_a_module_and_its_dependent_together_builds() {
        // Disable both merge and its dependency beads — no dangling edge.
        let flags =
            DisabledModules::new([ModuleId::new("beads").unwrap(), ModuleId::new("merge").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Merge)
            .module(Rigs)
            .build_with_flags(&flags)
            .unwrap();
        let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["rigs"]);
    }

    #[test]
    fn build_defaults_to_all_enabled() {
        let root = RootBuilder::new().module(Beads).module(Rigs).build().unwrap();
        assert_eq!(root.len(), 2);
    }

    use crate::mcp::McpRegistry;

    /// Module that contributes two MCP tools, for collection tests.
    struct Toolful;
    impl GtModule for Toolful {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("toolful").unwrap(),
                "Toolful",
                Version::new(1, 0, 0),
                "Contributes MCP tools.",
            )
        }
        fn register_mcp_tools(&self, reg: &mut McpRegistry) {
            reg.tool("toolful.create.execute", "Create")
                .tool("toolful.close.execute", "Close");
        }
    }

    #[test]
    fn module_with_no_tools_contributes_nothing() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        assert_eq!(root.mcp_tools().count(), 0);
    }

    #[test]
    fn builder_collects_contributed_tools() {
        let root = RootBuilder::new().module(Toolful).build().unwrap();
        let names: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["toolful.create.execute", "toolful.close.execute"]);
    }

    #[test]
    fn tools_follow_module_init_order() {
        // Merge depends on beads, so beads inits first; tools must follow.
        struct ToolfulBeads;
        impl GtModule for ToolfulBeads {
            fn meta(&self) -> ModuleMeta {
                ModuleMeta::new(
                    ModuleId::new("beads").unwrap(),
                    "Beads",
                    Version::new(1, 0, 0),
                    "Beads.",
                )
            }
            fn register_mcp_tools(&self, reg: &mut McpRegistry) {
                reg.tool("beads.read.execute", "Read");
            }
        }
        struct ToolfulMerge;
        impl GtModule for ToolfulMerge {
            fn meta(&self) -> ModuleMeta {
                ModuleMeta::new(
                    ModuleId::new("merge").unwrap(),
                    "Merge",
                    Version::new(1, 0, 0),
                    "Merge.",
                )
            }
            fn dependencies(&self) -> Vec<ModuleId> {
                vec![ModuleId::new("beads").unwrap()]
            }
            fn register_mcp_tools(&self, reg: &mut McpRegistry) {
                reg.tool("merge.submit.execute", "Submit");
            }
        }
        // Register merge first; build reorders so beads (dependency) inits first.
        let root = RootBuilder::new().module(ToolfulMerge).module(ToolfulBeads).build().unwrap();
        let names: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["beads.read.execute", "merge.submit.execute"]);
    }

    #[test]
    fn disabled_module_tools_are_dropped() {
        let flags = DisabledModules::new([ModuleId::new("toolful").unwrap()]);
        let root = RootBuilder::new()
            .module(Beads)
            .module(Toolful)
            .build_with_flags(&flags)
            .unwrap();
        assert_eq!(root.mcp_tools().count(), 0);
    }
}
