//! `gt-mcp-server` — the gt-core MCP server library (hq-core-host.3).
//!
//! The transport pillar of the MVP that hosts the issues tracker inside gt-core
//! and retired the gastown `gt-mcp` bin. This crate is the orchestration-tier
//! **library**: the rmcp [`ServerHandler`](server::IssuesServer), the issues +
//! meta tool dispatch, the per-connection scope/audit boundary, the per-request
//! workspace resolver, and the [`DomainRouter`](domain::DomainRouter) seam that
//! lets every ported domain register a [`DomainHandler`](domain::DomainHandler).
//!
//! The composing binary lives **up-tier** (`crates/modules/gt-composition`,
//! `hq-mcp-dispatch`): only the `modules` tier may depend on all `domain/*`
//! crates (docs/03 Rule 4), so the domain handlers — `workspace.*`, `rig.*`,
//! `quota.*`, `merge.*`, `convoy.*`, `sessions.*` — and the wiring that builds the
//! [`DomainRouter`] from them cannot live here in an `orchestration` crate. This
//! crate owns the contract + issues/meta dispatch; the app crate owns the
//! handlers + the `gt-mcp-server` binary.

#![forbid(unsafe_code)]

pub mod dispatch;
pub mod documents;
pub mod domain;
pub mod git_tree;
pub mod health;
pub mod pg_audit;
pub mod prefixes;
pub mod server;
pub mod workspace;
pub mod ws_status;

pub use documents::DocumentsResource;
pub use domain::{DomainCtx, DomainHandler, DomainRouter};
pub use health::HealthState;
pub use pg_audit::PgAuditSink;
pub use prefixes::{bead_prefix, WorkspaceRigPrefixes};
pub use server::IssuesServer;
pub use workspace::{workspace_from_ext, WorkspaceStores, WORKSPACE_HEADER};
pub use ws_status::{GateStatus, WorkspaceStatusGate};
