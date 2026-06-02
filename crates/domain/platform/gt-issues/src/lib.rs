//! `gt-issues` — the issues tracker as a [`GtModule`](gt_module::GtModule).
//!
//! Lifted from gastown `apps/api/crates/bins/gt-mcp/src/service.rs`
//! (hq-core-host.2, the MVP path to host the issues tracker inside gt-core and
//! retire the gastown `gt-mcp` bin). This crate wraps the already-ported Dolt
//! issues store ([`gt_store_dolt::DoltIssues`], hq-core-host.1) behind the kernel
//! module seam so the composition root harvests the `issues.*` tools instead of
//! hand-listing them.
//!
//! ## What landed here (.2)
//!
//! - [`IssuesModule`] — the [`GtModule`](gt_module::GtModule) facade. Its
//!   [`register_mcp_tools`](gt_module::GtModule::register_mcp_tools) declares the
//!   ten `issues.{create,update,transition,close,claim}.{validate,execute}` tools
//!   (names + descriptions + input schemas verbatim from the gastown service).
//! - [`commands`] — the tool-arg structs ([`CreateIssue`], [`UpdateIssue`],
//!   [`TransitionIssue`], [`CloseIssue`], [`ClaimIssue`]) with their shape-only
//!   `validate()` and the `to_new`/`to_patch` mappers onto the store types.
//! - [`handlers`] — the transport-free `run_*_issue` helpers that drive
//!   [`DoltIssues`](gt_store_dolt::DoltIssues). They carry the scope/audit-free
//!   core of the gastown handlers; the rmcp wrapping (scope check, audit row,
//!   `CallToolResult`) is the server bin's job (`hq-core-host.3`).
//! - [`resources`] — the `gt://issues` (with `?full=1`) and `gt://issue/{id}`
//!   read helpers the server's resource router calls.
//!
//! ## NN-16 taxonomy
//!
//! `issues.create` (and an `external_ref`-bearing `issues.update`) reuse the
//! already-ported [`gt_module_mcp::taxonomy::validate`] guard so every non-epic
//! bead carries `external_ref = <sub-epic>` and an id of `<external_ref>.<n>`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod commands;
pub mod delivery;
pub mod handlers;
pub mod park;
pub mod policy;
pub mod readiness;
pub mod resources;
pub mod surface;
pub mod taxonomy;
mod module;

pub use commands::{
    AdvancePhase, ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue,
};
pub use delivery::{CommitInfo, CommitInspector};
pub use module::IssuesModule;
pub use park::{
    decide as park_decide, HumanPresence, IrreversibleKind, Operation, ParkDecision, ParkQueue,
    ParkedOp, Reversibility,
};
pub use policy::{
    check_bead as check_bead_policy, invariant, BeadFacts, Enforcement, Invariant, PolicyVerdict,
    Violation, INVARIANTS,
};
pub use readiness::is_ready;
pub use surface::{AllowAllTree, SurfaceEntry, SurfaceTree};
pub use taxonomy::Domain;
