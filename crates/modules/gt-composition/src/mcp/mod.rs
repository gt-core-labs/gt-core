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

pub mod agent;
pub mod convoy;
pub mod eventlog;
pub mod graph;
pub mod merge;
pub mod pools;
pub mod quota;
pub mod rig;
pub mod util;
pub mod workspace;

pub use agent::AgentHandler;
pub use convoy::ConvoyHandler;
pub use eventlog::EventLog;
pub use graph::GraphHandler;
pub use merge::MergeHandler;
pub use pools::WsPools;
pub use quota::QuotaHandler;
pub use rig::{PgRigPrefixes, RigHandler};
pub use workspace::{PgWorkspaceStatus, WorkspaceHandler};
