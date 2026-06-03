//! MCP domain dispatch handlers (`hq-mcp-dispatch.2..7`).
//!
//! Each ported domain registers a [`DomainHandler`](gt_mcp_server::DomainHandler)
//! for its tool namespace, so the gt-core MCP server operates every domain
//! function — not just issue tracking. The handlers live here, in the `modules`
//! tier, because only `modules` may depend on every `domain/*` crate (docs/03
//! Rule 4); the orchestration-tier `gt-mcp-server` library owns the router
//! contract, this crate owns the per-domain implementations.
//!
//! Every handler is **PG-backed**: it drives the domain port's `pg` adapter over
//! a shared [`PgPool`](sqlx::PgPool), so a `create` then a `list` see the same
//! durable state across calls (the actors hydrate from Postgres per request).

pub mod pools;
pub mod rig;
pub mod workspace;

pub use pools::WsPools;
pub use rig::RigHandler;
pub use workspace::WorkspaceHandler;
