//! `mod-hello` — the smallest viable [`GtModule`], end-to-end.
//!
//! This is the learn-by-example reference (`hq-mod-docs.2`): one module that
//! touches every contribution point the kernel offers, so a module author can
//! read one file and copy the shape. It pairs with the authoring guide in
//! `docs/08-module-authoring.md` (`hq-mod-docs.1`).
//!
//! What it demonstrates, and where each piece is proved by the end-to-end gate
//! in `tests/e2e.rs`:
//!
//! - **Identity** — [`GtModule::meta`] returns a validated [`ModuleId`], name,
//!   semver, and description.
//! - **Capability** — [`GtModule::capability`] declares the authorization scopes
//!   the module owns (which opts its routes into RBAC) and the versioned event
//!   kind it emits. Declaration is validated at build time; live emission onto a
//!   bus is kernel wiring that migrates up later (`gt-events`, Phase 4).
//! - **MCP** — [`GtModule::register_mcp_tools`] pushes one tool whose name is
//!   namespaced under the module id.
//! - **Migrations** — [`GtModule::migrations`] ships one SQL migration via
//!   `include_str!`, owned by the module and applied in dependency-first order.
//! - **Routes** — [`GtModule::register_routes`] contributes a plain relative
//!   router; the builder mounts it under `/api/v1/hello` and wraps it with the
//!   per-method scope guard because the module claimed scopes.
//!
//! The composition root never hand-wires any of this: it writes
//! `RootBuilder::new().module(HelloModule).build()` and the builder harvests
//! every contribution.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use axum::routing::get;
use axum::{Json, Router};
use gt_module::{
    Capability, EventKind, GtModule, McpRegistry, Migration, ModuleId, ModuleMeta, Scope,
};
use semver::Version;

/// The versioned event this module emits. Event kinds are always
/// `module.noun.vN`; bumping the payload shape is a new `vN`, never a mutation
/// of `v1` (non-negotiable: events are versioned + replay-safe).
pub const GREETED_V1: &str = "hello.greeted.v1";

/// The reference module. Zero-sized: it owns no runtime state, so the unit
/// struct is all the composition root needs to register it.
#[derive(Clone, Copy, Debug, Default)]
pub struct HelloModule;

impl HelloModule {
    /// The module's stable id (`hello`). The literal is a known-valid slug, so
    /// callers get a plain [`ModuleId`].
    pub fn id() -> ModuleId {
        ModuleId::new("hello").expect("`hello` is a valid module id")
    }
}

impl GtModule for HelloModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Hello",
            Version::new(1, 0, 0),
            "Smallest viable GtModule — the learn-by-example reference.",
        )
    }

    fn capability(&self) -> Capability {
        // Claiming any scope opts every one of this module's routes into RBAC:
        // the builder wraps the router so a GET needs `hello.read` and a
        // mutating method needs `hello.write`. Declaring the event kind records
        // the contract the kernel validates and other modules subscribe against.
        Capability::empty()
            .claiming_all([
                Scope::new("hello.read").expect("valid scope"),
                Scope::new("hello.write").expect("valid scope"),
            ])
            .emitting(EventKind::new(GREETED_V1).expect("valid event kind"))
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // Tool names are `<module-id>.<action>.<verb>`; the builder additionally
        // checks the first segment matches this module, so a module cannot squat
        // another's namespace.
        registry.tool("hello.greeting.show", "Return the module's greeting.");
    }

    fn migrations(&self) -> Vec<Migration> {
        vec![Migration::new(
            1,
            "create_greetings",
            include_str!("../migrations/hello/0001_create_greetings.sql"),
        )]
    }

    fn register_routes(&self) -> Router {
        // Plain relative paths — the builder namespaces them under
        // `/api/v1/hello`, so this answers at `/api/v1/hello/greeting`. The
        // author never types the prefix.
        Router::new().route(
            "/greeting",
            get(|| async { Json(serde_json::json!({ "message": "hello, gas town" })) })
                .post(|| async { Json(serde_json::json!({ "greeted": true })) }),
        )
    }
}
