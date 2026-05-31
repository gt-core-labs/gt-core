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
//! [`build`](RootBuilder::build) already returns a [`Result`] so the validation
//! beads slot in without churning the signature:
//!
//! - dependency-cycle detection — `hq-mod-core.5`
//! - capability-conflict detection — `hq-mod-core.6`
//! - feature-flag filtering of disabled modules — `hq-mod-core.7`
//!
//! Each grows a [`BuildError`] variant; the skeleton itself never fails.

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
/// the builder and the validation beads need is captured here.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ModuleEntry {
    meta: ModuleMeta,
    capability: Capability,
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
        });
        self
    }

    /// Finalize the registry.
    ///
    /// The skeleton always succeeds; the [`Result`] reserves the seam for the
    /// validation beads (`.5` cycles, `.6` capability conflicts, `.7` flags),
    /// each of which adds a [`BuildError`] variant rather than changing this
    /// signature.
    pub fn build(self) -> Result<Root, BuildError> {
        Ok(Root {
            entries: self.entries,
        })
    }
}

/// Why [`RootBuilder::build`] rejected a module set.
///
/// Intentionally empty in `hq-mod-core.4`: the skeleton never fails. Marked
/// `#[non_exhaustive]` so the validation beads (`.5`/`.6`/`.7`) can add variants
/// — and so downstream `match` arms are forced to keep a wildcard from day one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {}

impl std::fmt::Display for BuildError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No variants yet; match exhaustively so adding one is a compile error
        // here, forcing a real message.
        match *self {}
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

    /// Metadata of every registered module, in registration order.
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
}
