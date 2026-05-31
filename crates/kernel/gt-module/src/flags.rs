//! Feature flags — which modules are enabled for a given build.
//!
//! Landed by [`hq-mod-core.7`]. A deployment does not always want every
//! registered module live: a feature may be dark-launched, disabled per
//! workspace, or rolled back. [`FeatureFlags`] is the read-only predicate the
//! [`RootBuilder`](crate::RootBuilder) consults to drop disabled modules before
//! validation and ordering.
//!
//! The trait is sync and consumed through generics (non-negotiable #1/#2 — no
//! `dyn`, no async). The default [`AllEnabled`] keeps every module; [`DisabledModules`]
//! names an explicit deny-set. A real per-workspace flag store (Postgres-backed,
//! with overrides) lands in the `hq-mod-flags` epic and only has to implement
//! this same trait.
//!
//! ## Disabling and dependencies
//!
//! Filtering is not transitive. If an *enabled* module depends on a *disabled*
//! one, the build fails with
//! [`BuildError::DisabledDependency`](crate::BuildError::DisabledDependency)
//! rather than silently cascading the disable — a disabled dependency is a
//! configuration mistake the operator should see, not absorb.

use std::collections::BTreeSet;

use crate::meta::ModuleId;

/// Read-only predicate: is a given module enabled for this build?
///
/// Implementors must be pure (no I/O, deterministic) so the build stays
/// replay-safe. Querying the same id twice in one build must return the same
/// answer.
pub trait FeatureFlags {
    /// Whether `module` should be included in the built [`Root`](crate::Root).
    fn is_enabled(&self, module: &ModuleId) -> bool;
}

/// Every module enabled. The default when [`build`](crate::RootBuilder::build)
/// is called without explicit flags.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllEnabled;

impl FeatureFlags for AllEnabled {
    fn is_enabled(&self, _module: &ModuleId) -> bool {
        true
    }
}

/// An explicit deny-set: the named modules are disabled, everything else is
/// enabled.
#[derive(Clone, Debug, Default)]
pub struct DisabledModules {
    disabled: BTreeSet<ModuleId>,
}

impl DisabledModules {
    /// Build a deny-set from the given module ids.
    pub fn new(ids: impl IntoIterator<Item = ModuleId>) -> Self {
        DisabledModules {
            disabled: ids.into_iter().collect(),
        }
    }
}

impl FeatureFlags for DisabledModules {
    fn is_enabled(&self, module: &ModuleId) -> bool {
        !self.disabled.contains(module)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> ModuleId {
        ModuleId::new(s).unwrap()
    }

    #[test]
    fn all_enabled_enables_everything() {
        assert!(AllEnabled.is_enabled(&id("anything")));
    }

    #[test]
    fn disabled_set_disables_only_named() {
        let flags = DisabledModules::new([id("legacy"), id("beta")]);
        assert!(!flags.is_enabled(&id("legacy")));
        assert!(!flags.is_enabled(&id("beta")));
        assert!(flags.is_enabled(&id("beads")));
    }

    #[test]
    fn empty_disabled_set_enables_everything() {
        let flags = DisabledModules::default();
        assert!(flags.is_enabled(&id("beads")));
    }
}
