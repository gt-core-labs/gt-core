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

/// The Kanban analytics projection (hq-1cd840): the four operator KPIs
/// (avance/errores/pendientes/retrasos) + chart series, pure over the same
/// rows board.list reads (read-only, no new store).
pub mod analytics;
/// The Kanban board projection (hq-62130a, ADR hq-423a4b): lexorank ordering,
/// the `board.{list,move,reorder}` commands + transport-free handlers, and the
/// pure column/lane bucketer. Cards ARE beads — no second store.
pub mod board;
/// The off-by-default `axum` REST adapter for the board projection (hq-62130a),
/// mounted at `/api/v1/board` behind the `board.read`/`board.write` guard.
#[cfg(feature = "axum")]
pub mod board_http;
/// The [`GtModule`](gt_module::GtModule) facade for the board projection
/// (hq-62130a): declares the `board.*` MCP tools + the REST surface.
pub mod board_module;
pub mod commands;
pub mod delivery;
/// Runtime domain validation against the per-workspace `domain_catalog`
/// (gtcore-d81e77 H2): the catalog — not the closed [`taxonomy::Domain`] enum — is
/// the write-path arbiter, with an enum fallback for un-seeded workspaces pending
/// the H3 backfill.
pub mod domain_validate;
/// Dispatch-policy resolution with `child_of` inheritance (gtcore-1acbcf C1):
/// [`dispatch::resolve_dispatch`] (the helper C3's `ready_for_auto` frontier
/// consumes) and [`dispatch::filter_dispatch`] (the resolved `?dispatch=` list
/// filter).
pub mod dispatch;
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
/// Read-side seam for the agent operating a bead (`hq-agent-observability.3`): the
/// [`operator::OperatorResource`] provider the composition root backs with an event-log fold, and
/// [`operator::attach_operated_by`] which inlines its result as `operated_by` on a served row.
pub mod operator;
pub mod park;
pub mod policy;
pub mod readiness;
/// The operator-report projection (hq-fc7d6a): the mockup spreadsheet
/// (per-module sections + TOTAL HORAS) over the same rows `board.list` reads,
/// with CSV + XLSX serializers. Delivery (doc attach / outbox email) is the
/// composition handler's job.
pub mod report;
/// HTML render of the scheduled report digest (hq-562e0b): the same
/// [`report`] bitácora + [`analytics`] KPIs as one standalone email body.
pub mod report_html;
pub mod resources;
/// Transport-free statistics aggregation (`hq-web-extras.12`): counts + progress + lead-time
/// roll-ups over the tracker rows, grouped by epic/rig/status/domain/assignee/owner. The cheap
/// `gt://issues` snapshot feeds it; the `GET /api/v1/issues/stats` REST handler in [`http`] wires
/// it to the wire.
pub mod stats;
pub mod surface;
pub mod taxonomy;

pub use analytics::{summarize as analytics_summarize, AnalyticsSummary};
pub use board::{
    project_board, project_scopes, rank_between, run_board_list, run_board_move,
    run_board_reorder, run_board_scopes, spread_ranks, BoardList, BoardMove, BoardReorder,
    BoardScope, BoardScopes, BoardSnapshot,
};
pub use board_module::BoardModule;
pub use commands::{
    AdvancePhase, ClaimIssue, CloseIssue, CreateIssue, ListIssues, ReadIssue, TransitionIssue,
    UpdateIssue,
};
pub use delivery::{CommitInfo, CommitInspector, InspectorProvider};
pub use domain_validate::validate_domains;
pub use dispatch::{
    filter_dispatch, locked_roots, occupied_surfaces, operator_locked, ready_for_auto,
    resolve_dispatch, session_like_actor, should_sling, surface_overlaps,
};
pub use events::{emit_issue_event, IssueEvent, IssueEventSink, IssueVerb};
#[cfg(feature = "axum")]
pub use http::{issues_router, ApiDoc, IssuesApiState};
#[cfg(feature = "axum")]
pub use me::{me_router, MeApiState, MeMembership, MeStatsSource};
pub use me_module::MeModule;
pub use module::IssuesModule;
pub use operator::{attach_operated_by, OperatorResource};
pub use park::{
    decide as park_decide, HumanPresence, IrreversibleKind, Operation, ParkDecision, ParkQueue,
    ParkedOp, Reversibility,
};
pub use policy::{
    check_bead as check_bead_policy, guard_claim_context, invariant, BeadFacts, Enforcement,
    Invariant, PolicyVerdict, Violation, INVARIANTS, MIN_CONTEXT_LEN,
};
pub use readiness::is_ready;
pub use report::{
    build_report, to_csv, to_xlsx, OperatorReport, ReportComment, ReportRow, ReportSection,
};
pub use report_html::{
    collect_report_mermaid_sources, render_digest, render_digest_with_diagrams,
};
pub use stats::{MeStatsResponse, WorkspaceStats};
pub use surface::{AllowAllProvider, AllowAllTree, SurfaceEntry, SurfaceProvider, SurfaceTree};
pub use taxonomy::{Dispatch, Domain, IssueType, RoleScope};
