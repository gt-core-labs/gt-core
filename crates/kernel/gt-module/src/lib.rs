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
//! - [`Capability`] — contribution/requirement declaration; minimal seam now,
//!   real shape in `hq-mod-core.3`.
//!
//! Still to come on this epic: capability shape (`.3`), `RootBuilder` (`.4`),
//! dependency-cycle detection (`.5`), capability-conflict detection (`.6`),
//! feature-flag filtering (`.7`), the test matrix (`.8`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod capability;
mod meta;
#[path = "trait.rs"]
mod module_trait;

pub use capability::Capability;
pub use meta::{ModuleId, ModuleIdError, ModuleMeta};
pub use module_trait::GtModule;
