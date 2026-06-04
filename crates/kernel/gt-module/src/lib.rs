//! Module system foundation — the [`GtModule`] trait and its metadata types.
//!
//! A feature in Gas Town (and downstream apps) is one crate that implements
//! [`GtModule`]. The `RootBuilder` (`hq-mod-core.4`) consumes a list of them and
//! wires routes, MCP tools, migrations, and lifecycle — the composition root
//! hand-wires nothing (see [`docs/03-architecture-guardrails.md`] rule 3).
//!
//! ## Landed so far
//!
//! - [`ModuleId`] / [`ModuleMeta`] — module identity (`hq-mod-core.2`).
//! - [`GtModule`] — the trait (`hq-mod-core.2`).
//! - [`Capability`] + [`Scope`] + [`EventKind`] — contribution/requirement
//!   declaration and its vocabulary (`hq-mod-core.3`).
//! - [`RootBuilder`] / [`Root`] — the composition seam that collects modules and
//!   builds the registry (`hq-mod-core.4`).
//! - Dependency-cycle detection + topological init ordering — `build()` orders
//!   modules so each follows its dependencies, rejecting cycles and dangling
//!   references via [`BuildError`] (`hq-mod-core.5`).
//! - Capability-scope conflict detection — [`Capability`] now carries the scopes
//!   a module claims, and `build()` rejects two modules claiming the same one
//!   (`hq-mod-core.6`).
//! - Feature-flag filtering — [`RootBuilder::build_with_flags`] drops modules a
//!   [`FeatureFlags`] predicate disables, rejecting an enabled module that
//!   depends on a disabled one (`hq-mod-core.7`).
//! - Black-box integration matrix in `tests/integration.rs` — empty,
//!   single-module, dependency cycle, capability conflict, plus ordering and
//!   flag behavior against the public API (`hq-mod-core.8`).
//!
//! The `hq-mod-core` sub-epic is complete: this crate is the module-system
//! foundation the rest of `hq-mod` (routes, MCP, events, migrate, flags,
//! frontend, contracts, refactor) builds on.
//!
//! ## Routes
//!
//! - [`GtModule::register_routes`] — a module contributes a self-contained
//!   [`axum::Router`]; the builder mounts them in init order and exposes the
//!   application router through [`Root::into_router`] (`hq-mod-routes.1`).
//! - [`module_prefix`] / [`API_BASE`] — each module's routes are auto-mounted
//!   under `/api/v1/<module-id>`, so modules declare plain relative paths and
//!   never collide (`hq-mod-routes.2`).
//! - [`guard_module_scopes`] / [`CallerScopes`] — a module that declares scopes
//!   in its [`Capability`] gets every route guarded by a method-derived
//!   `<module>.<read|write>` scope check; modules claiming none stay public
//!   (`hq-mod-routes.3`).
//! - [`GtModule::openapi`] — a module documents its routes with a `utoipa` spec;
//!   [`Root::openapi`] mounts each under `/api/v1/<module>` and merges them into
//!   one OpenAPI document (`hq-mod-routes.4`).
//!
//! ## Hooks
//!
//! - [`HookPoint`] — the lifecycle-point vocabulary; a module declares the points
//!   it observes through [`Capability::subscribing`] and the builder exposes the
//!   active subscriptions via [`Root::hook_subscriptions`] (`hq-mod-hooks.3`). The
//!   handlers + registry + dispatcher live in `gt-hooks`, which re-exports
//!   `HookPoint`; `gt-module` holds only the pure-data vocabulary, never the async
//!   `dyn` handler (non-negotiable #1/#2).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod builder;
mod capability;
mod deps;
mod event_kind;
mod flags;
mod hook;
mod mcp;
mod meta;
mod migrate;
mod routes;
mod scope;
#[path = "trait.rs"]
mod module_trait;

pub use builder::{BuildError, Root, RootBuilder};
pub use capability::{Capability, Deprecation};
pub use event_kind::{EventKind, EventKindError};
pub use flags::{AllEnabled, DisabledModules, FeatureFlags};
pub use hook::HookPoint;
pub use mcp::{McpRegistry, McpTool, McpToolNameError};
pub use meta::{ModuleId, ModuleIdError, ModuleMeta};
pub use migrate::Migration;
pub use module_trait::GtModule;
pub use routes::{guard_module_scopes, module_prefix, CallerScopes, ScopeDenied, API_BASE};
pub use scope::{Scope, ScopeError};
