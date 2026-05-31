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
//! - capability-conflict detection — `hq-mod-core.6`
//! - feature-flag filtering of disabled modules — `hq-mod-core.7`
//!
//! Each grows a [`BuildError`] variant. As of `.5`, `build()` orders the
//! registered modules so each appears after every module it depends on (see
//! [`crate::deps`]).

use crate::capability::Capability;
use crate::meta::{ModuleId, ModuleMeta};
use crate::module_trait::GtModule;

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
    /// [`capability`](GtModule::capability) once, stores the snapshot, and drops
    /// the value. Returns `self` so registrations chain. Dispatch is static —
    /// no trait object is retained (non-negotiable #1).
    pub fn module<M: GtModule>(mut self, module: M) -> Self {
        self.entries.push(ModuleEntry {
            meta: module.meta(),
            capability: module.capability(),
            depends_on: module.dependencies(),
        });
        self
    }

    /// Finalize the registry.
    ///
    /// Resolves module init order so each module appears after every module it
    /// depends on, rejecting dependency cycles and references to unregistered
    /// modules ([`crate::deps`], `hq-mod-core.5`). Later beads add further
    /// [`BuildError`] variants (`.6` capability conflicts, `.7` flags) without
    /// changing this signature.
    pub fn build(self) -> Result<Root, BuildError> {
        let order = crate::deps::resolve_order(&self.entries)?;
        // Reorder into init order. The module set is a handful of entries, so
        // cloning by index is cheaper than threading an in-place permutation.
        let entries = order.iter().map(|&i| self.entries[i].clone()).collect();
        Ok(Root { entries })
    }
}

/// Why [`RootBuilder::build`] rejected a module set.
///
/// Marked `#[non_exhaustive]` so later validation beads (`.6` capability
/// conflicts, `.7` flags) can add variants — and so downstream `match` arms keep
/// a wildcard from day one.
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
    /// The module dependency graph contains a cycle, so no init order exists.
    /// Holds the ids on or downstream of the cycle.
    DependencyCycle(Vec<ModuleId>),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::UnknownDependency { module, missing } => {
                write!(f, "module {module} depends on unregistered module {missing}")
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
}
