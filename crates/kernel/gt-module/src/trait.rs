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

use axum::Router;

use crate::capability::Capability;
use crate::mcp::McpRegistry;
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

    /// Declare the MCP tools this module serves (`hq-mod-mcp.1`).
    ///
    /// The builder hands each module a fresh [`McpRegistry`] and harvests the
    /// tools pushed into it, so the composition root never lists MCP tools by
    /// hand (see `docs/03-architecture-guardrails.md` rule 3). Defaults to a
    /// no-op so a module that serves no MCP tools — and a not-yet-ported one —
    /// implements nothing.
    fn register_mcp_tools(&self, _registry: &mut McpRegistry) {}

    /// The HTTP routes this module contributes, as a self-contained
    /// [`axum::Router`] (`hq-mod-routes.1`).
    ///
    /// The builder calls this once, in init order, when `.module()` registers the
    /// module, and merges the returned routers into one application router
    /// ([`RootBuilder`](crate::RootBuilder) → [`Root::into_router`](crate::Root::into_router)).
    /// The composition root hand-wires nothing — it never calls
    /// `Router::route` for a module (see `docs/03-architecture-guardrails.md`
    /// rule 3 and the `register_routes` callout).
    ///
    /// ## Why the return type is state-erased (`Router<()>`)
    ///
    /// A module owns its dependencies (its repository ports, per the hexagonal
    /// rule) and bakes them into its handlers with
    /// [`Router::with_state`](axum::Router::with_state) *before* returning, so the
    /// router the builder merges carries no outstanding state type. This keeps
    /// modules decoupled: the kernel never learns a shared application-state type,
    /// and two modules cannot collide on one. A module that needs runtime handles
    /// at construction takes them in its own constructor (the trait takes `&self`,
    /// so an implementor need not be zero-sized); the actor handles a module may
    /// require are supplied by the binary (non-negotiable #14).
    ///
    /// Defaults to an empty router so a module that contributes no HTTP surface —
    /// or one not yet ported — implements nothing. Path namespacing
    /// (`/api/v1/<module>`) is applied by the builder in `hq-mod-routes.2`, and
    /// per-route scope guards derived from [`capability`](GtModule::capability) in
    /// `hq-mod-routes.3`; an implementor declares plain relative routes here.
    ///
    /// ```
    /// use axum::{routing::get, Router};
    /// use gt_module::{GtModule, ModuleId, ModuleMeta};
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
    ///
    ///     fn register_routes(&self) -> Router {
    ///         Router::new().route("/beads", get(|| async { "[]" }))
    ///     }
    /// }
    /// ```
    fn register_routes(&self) -> Router {
        Router::new()
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
