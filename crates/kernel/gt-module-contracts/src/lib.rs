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
//!
//! # Authoring a contract
//!
//! A module owns a contract: the wire surface it promises downstream consumers
//! (the TS frontend, other modules). The author's job is to declare that surface
//! once and let CI hold the line. The flow:
//!
//! 1. **Declare the DTOs** with `#[derive(schemars::JsonSchema)]` and dump each
//!    as a JSON Schema (`gt-module-mcp::schema_for` does this for MCP tools).
//! 2. **Generate TypeScript** for the frontend with [`typescript_module`] — no
//!    Node toolchain in the build; the `.ts` is a pure function of the schemas.
//! 3. **List the surface items** (one string per route / tool / event, e.g.
//!    `"event:bead.created.v1"`) and fold them into a [`SurfaceHash`].
//! 4. **Freeze a baseline** — pair the hash with a declared [`ContractVersion`]
//!    in a [`ContractBaseline`] and check it into the per-major file named by
//!    [`version_file_name`].
//! 5. **Gate in CI**: recompute the live surface each build and run both checks:
//!    [`ContractBaseline::check`] (coarse — *did the surface move without a
//!    bump*) and [`enforce`] (strict — *does the bump's size match the change*).
//!
//! ```
//! use gt_module_contracts::{
//!     enforce, ContractBaseline, ContractVersion, DriftCheck, SemverVerdict, SurfaceHash,
//! };
//!
//! // Frozen at v1.0.0 with one tool + one event.
//! let v1 = ContractVersion::new(1, 0, 0);
//! let frozen = ["tool:beads.create.execute", "event:bead.created.v1"];
//! let baseline = ContractBaseline::freeze(v1, SurfaceHash::compute(frozen));
//!
//! // A later build adds a tool — an additive change.
//! let live = ["tool:beads.create.execute", "tool:beads.close.execute", "event:bead.created.v1"];
//! let live_hash = SurfaceHash::compute(live);
//!
//! // Forgetting to bump is rejected by the coarse drift check...
//! assert_eq!(baseline.check(v1, live_hash), DriftCheck::UnbumpedDrift);
//!
//! // ...and a minor bump satisfies both checks for an additive change.
//! let v1_1 = ContractVersion::new(1, 1, 0);
//! assert_eq!(baseline.check(v1_1, live_hash), DriftCheck::BumpedAndChanged);
//! assert_eq!(enforce(v1, v1_1, frozen, live), SemverVerdict::Ok);
//! ```

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
