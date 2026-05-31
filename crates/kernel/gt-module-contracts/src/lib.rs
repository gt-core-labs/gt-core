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
//!
//! Part of the gt-core module system (`hq-mod`). The version registry +
//! compatibility check land in `hq-mod-contracts.2`; TS DTO generation in `.3`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod version;

pub use version::{ContractVersion, SurfaceHash};
