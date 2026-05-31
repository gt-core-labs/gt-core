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
//!
//! Still to come on this epic: feature-flag filtering (`.7`), the test matrix
//! (`.8`). Each grows a [`BuildError`] variant
//! — the `build()` signature is already fallible.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod builder;
mod capability;
mod deps;
mod event_kind;
mod meta;
mod scope;
#[path = "trait.rs"]
mod module_trait;

pub use builder::{BuildError, Root, RootBuilder};
pub use capability::Capability;
pub use event_kind::{EventKind, EventKindError};
pub use meta::{ModuleId, ModuleIdError, ModuleMeta};
pub use module_trait::GtModule;
pub use scope::{Scope, ScopeError};
