//! Module contract versioning — frozen-surface tracking per module.
//!
//! A module's contract is its wire surface (route DTOs, MCP tool schemas, event
//! payloads). This crate lets each module carry a declared [`ContractVersion`]
//! alongside a computed [`SurfaceHash`] of the actual surface, so drift (hash
//! changed without a version bump) is detectable.
//!
//! ## Landed so far
//!
//! - [`ContractVersion`] — semantic `major.minor.patch` with a compatibility rule.
//! - [`SurfaceHash`] — order-independent fingerprint of a module's surface items.
//! - [`typescript_module`] — TypeScript DTO codegen from a module's JSON Schemas.
//! - [`ContractBaseline`] + [`DriftCheck`] — frozen baseline + CI drift check.
//!
//! Part of the gt-core module system (`hq-mod`). Semver enforcement (a breaking
//! surface change must bump *major*, not just any field) lands in `.4`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod baseline;
mod codegen;
mod version;

pub use baseline::{ContractBaseline, DriftCheck};
pub use codegen::typescript_module;
pub use version::{ContractVersion, SurfaceHash};
