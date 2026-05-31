//! The [`GtModule`] trait — the single on-ramp for a pluggable feature.
//!
//! Landed by [`hq-mod-core.2`]: identity ([`meta`](GtModule::meta)) and the
//! capability seam ([`capability`](GtModule::capability)). Behavioral
//! contribution hooks arrive in later beads and epics, each additive so a
//! module written today keeps compiling:
//!
//! - `register_routes` — `hq-mod-routes`
//! - `register_mcp_tools` — `hq-mod-mcp`
//! - `register` (actors/observers) + lifecycle — `hq-mod-core.4` (`RootBuilder`)
//! - migrations — `hq-mod-migrate`
//!
//! ## Why the trait is sync
//!
//! Per non-negotiable #2 (sync core) and #1 (`dyn` + `#[async_trait]` only in
//! `gt-plugin`), `GtModule` is a plain trait dispatched statically by the
//! builder's generic `.module::<M>()` chain. A module that needs I/O at startup
//! registers an actor whose runtime handle is supplied by the binary
//! (non-negotiable #14); it does not make trait methods `async`.

use crate::capability::Capability;
use crate::meta::{ModuleId, ModuleMeta};

/// A pluggable feature: one crate, registered with the builder in one line.
///
/// Implementors are zero-sized marker structs (e.g. `struct BeadsModule;`) that
/// describe themselves through [`meta`](GtModule::meta) and declare their
/// contributions through [`capability`](GtModule::capability). The
/// `RootBuilder` (`hq-mod-core.4`) consumes these to wire routes, MCP tools,
/// migrations, and lifecycle without the composition root hand-wiring anything.
///
/// ```
/// use gt_module::{Capability, GtModule, ModuleId, ModuleMeta};
/// use semver::Version;
///
/// struct BeadsModule;
///
/// impl GtModule for BeadsModule {
///     fn meta(&self) -> ModuleMeta {
///         ModuleMeta::new(
///             ModuleId::new("beads").unwrap(),
///             "Beads",
///             Version::new(1, 0, 0),
///             "Issue tracking aggregate backed by Dolt.",
///         )
///     }
/// }
///
/// assert_eq!(BeadsModule.meta().id.as_str(), "beads");
/// assert_eq!(BeadsModule.capability(), Capability::empty());
/// ```
pub trait GtModule {
    /// Descriptive identity of this module. Pure data; called any number of
    /// times by the builder, diagnostics, and `meta.help`.
    fn meta(&self) -> ModuleMeta;

    /// What this module contributes and requires.
    ///
    /// Defaults to [`Capability::empty`] so a freshly scaffolded or
    /// not-yet-ported module compiles before its contributions are declared
    /// (`hq-mod-core.3` grows the real shape; `.6` uses it for conflict
    /// detection).
    fn capability(&self) -> Capability {
        Capability::empty()
    }

    /// Ids of the other modules this module requires to be present and
    /// initialized before it.
    ///
    /// The builder uses these to order module wiring (dependencies first) and to
    /// reject dependency cycles and dangling references (`hq-mod-core.5`).
    /// Defaults to none, so a module with no inter-module dependencies — the
    /// common case — implements nothing.
    fn dependencies(&self) -> Vec<ModuleId> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::ModuleId;
    use semver::Version;

    struct Bare;
    impl GtModule for Bare {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("bare").unwrap(),
                "Bare",
                Version::new(0, 1, 0),
                "Minimal module for trait tests.",
            )
        }
    }

    #[test]
    fn default_capability_is_empty() {
        assert_eq!(Bare.capability(), Capability::empty());
    }

    #[test]
    fn meta_exposes_identity() {
        let m = Bare.meta();
        assert_eq!(m.id.as_str(), "bare");
        assert_eq!(m.version, Version::new(0, 1, 0));
    }

    #[test]
    fn dyn_object_safe_for_registry_storage() {
        // The builder stores heterogeneous modules; confirm the trait is
        // object-safe even though the hot path uses static dispatch.
        let modules: Vec<Box<dyn GtModule>> = vec![Box::new(Bare)];
        assert_eq!(modules[0].meta().id.as_str(), "bare");
    }
}
