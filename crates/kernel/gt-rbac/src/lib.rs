//! `gt-rbac` — unified RBAC config + frontier scope check, lifted from gastown
//! `gt-rbac` + `gt-mcp::auth` (hq-core-host.4).
//!
//! One TOML/JSON file ([`RbacConfig`]) is the source of truth for two consumers:
//! the MCP tool dispatch path resolves a per-actor [`Scope`] and calls
//! [`Scope::check`] before every tool, and gt-web resolves a [`WebGrant`] to stamp
//! JWT roles + scopes. Deny-by-default throughout — an unknown actor gets a closed
//! scope, never admin.

mod config;
mod error;
mod scope;

pub use config::{ActorSpec, RbacConfig, RoleSpec, WebGrant};
pub use error::AppError;
pub use scope::{ResolveScope, Scope};
