//! `gt-issues` — the issues tracker as a [`GtModule`](gt_module::GtModule).
//!
//! Lifted from the upstream `apps/api/crates/bins/gt-mcp/src/service.rs`
//! (hq-core-host.2, the MVP path to host the issues tracker inside gt-core and
//! retire the upstream `gt-mcp` bin). This crate wraps the already-ported Dolt
//! issues store ([`gt_store_dolt::DoltIssues`], hq-core-host.1) behind the kernel
//! module seam so the composition root harvests the `issues.*` tools instead of
//! hand-listing them.
//!
//! ## What landed here (.2)
//!
//! - [`IssuesModule`] — the [`GtModule`](gt_module::GtModule) facade. Its
//!   [`register_mcp_tools`](gt_module::GtModule::register_mcp_tools) declares the
//!   ten `issues.{create,update,transition,close,claim}.{validate,execute}` tools
//!   (names + descriptions + input schemas verbatim from the upstream service).
//! - [`commands`] — the tool-arg structs ([`CreateIssue`], [`UpdateIssue`],
//!   [`TransitionIssue`], [`CloseIssue`], [`ClaimIssue`]) with their shape-only
//!   `validate()` and the `to_new`/`to_patch` mappers onto the store types.
//! - [`handlers`] — the transport-free `run_*_issue` helpers that drive
//!   [`DoltIssues`](gt_store_dolt::DoltIssues). They carry the scope/audit-free
//!   core of the upstream handlers; the rmcp wrapping (scope check, audit row,
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
/// Issue-mutation events for the per-workspace SSE feed (`hq-issues-sse`): the
/// versioned [`events::IssueEvent`] every successful mutation emits, the
/// [`events::IssueEventSink`] seam the composition root backs with the event log, and
/// the shared [`events::emit_issue_event`] both the REST and MCP paths call so the two
/// transports emit the identical event.
pub mod events;
pub mod handlers;
/// The off-by-default `axum` REST adapter (`hq-auth-routes.2`): the first GtModule HTTP surface,
/// mapping `issues.*` to REST routes that reuse the same handlers as the MCP tools.
#[cfg(feature = "axum")]
pub mod http;
/// The off-by-default `axum` REST adapter for the cross-workspace `me.*` surface
/// (`hq-web-extras.15`): `GET /api/v1/me/stats` rolls up issue progress across every workspace the
/// caller is a member of, complementing the per-workspace `GET /api/v1/issues/stats`.
#[cfg(feature = "axum")]
pub mod me;
/// The [`GtModule`](gt_module::GtModule) facade for the cross-workspace `me.*` surface
/// (`hq-web-extras.15`), mounted at `/api/v1/me` behind the `me.read` guard.
pub mod me_module;
mod module;
pub mod park;
pub mod policy;
pub mod readiness;
pub mod resources;
/// Transport-free statistics aggregation (`hq-web-extras.12`): counts + progress + lead-time
/// roll-ups over the tracker rows, grouped by epic/rig/status/domain/assignee/owner. The cheap
/// `gt://issues` snapshot feeds it; the `GET /api/v1/issues/stats` REST handler in [`http`] wires
/// it to the wire.
pub mod stats;
pub mod surface;
pub mod taxonomy;

pub use commands::{
    AdvancePhase, ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue,
};
pub use delivery::{CommitInfo, CommitInspector};
pub use events::{emit_issue_event, IssueEvent, IssueEventSink, IssueVerb};
#[cfg(feature = "axum")]
pub use http::{issues_router, ApiDoc, IssuesApiState};
#[cfg(feature = "axum")]
pub use me::{me_router, MeApiState, MeMembership, MeStatsSource};
pub use me_module::MeModule;
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
pub use stats::{MeStatsResponse, WorkspaceStats};
pub use surface::{AllowAllTree, SurfaceEntry, SurfaceTree};
pub use taxonomy::Domain;
