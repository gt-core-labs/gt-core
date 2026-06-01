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
//! - [`enforce`] + [`SurfaceChange`] / [`SemverVerdict`] — semver enforcement
//!   (a breaking surface change must bump *major*, not just any field).
//!
//! Part of the gt-core module system (`hq-mod`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod baseline;
mod codegen;
mod semver;
mod version;

pub use baseline::{ContractBaseline, DriftCheck};
pub use codegen::typescript_module;
pub use semver::{enforce, version_file_name, SemverVerdict, SurfaceChange};
pub use version::{ContractVersion, SurfaceHash};
