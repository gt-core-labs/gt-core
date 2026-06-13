//! MCP domain dispatch handlers (`hq-mcp-dispatch.2..7`).
//!
//! Each ported domain registers a [`DomainHandler`](gt_mcp_server::DomainHandler)
//! for its tool namespace, so the gt-core MCP server operates every domain
//! function — not just issue tracking. The handlers live here, in the `modules`
//! tier, because only `modules` may depend on every `domain/*` crate (docs/03
//! Rule 4); the orchestration-tier `gt-mcp-server` library owns the router
//! contract, this crate owns the per-domain implementations.
//!
//! Each handler is durable across calls, but over one of two stores depending on
//! the domain's persistence model:
//!
//! - **PG-backed** ([`WorkspaceHandler`], [`RigHandler`]): drives the domain
//!   port's `pg` adapter over a shared [`PgPool`](sqlx::PgPool), so a `create`
//!   then a `list` see the same Postgres state (the actors hydrate per request).
//! - **Event-log-backed** ([`MergeHandler`], [`ConvoyHandler`], [`AgentHandler`],
//!   [`QuotaHandler`]): these domains keep no projection table — their state is a
//!   replayed event stream — so each call rehydrates from the per-workspace event
//!   log, executes, and appends the produced event(s) back (see [`EventLog`]).

pub mod a2a_delegate;
pub mod agent;
pub mod analytics;
pub mod comments;
pub mod audit;
pub mod convoy;
pub mod dispatch;
pub mod documents;
pub mod email;
pub mod escalate;
pub mod invites;
pub mod report;
pub mod eventlog;
pub mod graph;
pub mod memory;
pub mod merge;
pub mod notify;
pub mod pools;
pub mod quota;
pub mod rest_backings;
pub mod rig;
pub mod util;
pub mod workspace;

pub use a2a_delegate::A2aDelegateHandler;
pub use agent::AgentHandler;
pub use audit::AuditHandler;
pub use convoy::ConvoyHandler;
pub use dispatch::DispatchHandler;
pub use analytics::AnalyticsHandler;
pub use comments::CommentsHandler;
pub use email::EmailHandler;
pub use escalate::EscalateHandler;
pub use invites::InvitesHandler;
pub use report::ReportHandler;
pub use documents::{DocumentsHandler, PgDocumentsResource};
pub use eventlog::{EventLog, EventLogIssueSink};
pub use graph::{GraphHandler, RigProvisioner};
pub use memory::MemoryHandler;
pub use merge::MergeHandler;
pub use notify::NotifyHandler;
pub use pools::WsPools;
pub use quota::{QuotaBlockGuard, QuotaHandler};
pub use rest_backings::{
    EventLogConvoy, EventLogFeed, EventLogGraph, EventLogHooks, EventLogMerges, EventLogQuota,
    EventLogSkills, FsAccountCatalog, GraphHandlerRefresher, IdentityDoltMeStats, WsPoolRigs,
};
pub use rig::{PgRigPrefixes, RigHandler};
pub use workspace::{CompositionTenantProvisioner, PgWorkspaceStatus, WorkspaceHandler};
