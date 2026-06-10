//! `gt-vcs` — per-workspace VCS connections (the foundation of the hq-vcs-connections epic).
//!
//! A workspace connects GitHub — via a GitHub App installation (the server mints ephemeral
//! installation tokens JIT; only the `installation_id` is stored) or a Personal Access Token (the
//! fallback, sealed at rest) — so the server can clone the workspace's private repos for the
//! knowledge graph without an operator hand-cloning on the host. This crate owns the CONNECTION
//! facet: the `public.vcs_connections` store ([`repo`]) and the REST CRUD over it ([`http`], behind
//! the off-by-default `axum` feature), wrapped in a [`GtModule`](gt_module::GtModule)
//! ([`VcsModule`]).
//!
//! Storage MIRRORS [`gt_auth`]'s OAuth provider store: a single GLOBAL `public` table with an
//! optional `workspace_id`, and the PAT sealed at rest with the SAME AES-GCM helper
//! ([`gt_auth::seal`] / [`gt_auth::unseal`], `GT_SECRET_KEY`) — never a new cipher.

pub mod repo;

#[cfg(feature = "axum")]
pub mod http;

pub mod module;

pub use module::VcsModule;
#[cfg(feature = "axum")]
pub use module::VcsHttpModule;

pub use repo::{
    ConnectionKind, ConnectionStatus, NewConnection, PatchConnection, VcsConnection,
    VcsConnectionRepo,
};
#[cfg(feature = "pg")]
pub use repo::PgVcsConnections;

#[cfg(feature = "axum")]
pub use http::ConnectionApiState;

/// The module-owned migration SQL, carried inline with `include_str!` so the boot migration runner
/// (`gt_module_migrate::apply`) seeds `public.vcs_connections`. A const is INERT until it is added
/// to the boot loop (`apply_pg_catalog` in `gt-mcp-server`) — see the system-providers-500 incident.
pub mod migrations {
    /// `public.vcs_connections` (hq-vcs-connections.1): the GLOBAL table with a `workspace_id`
    /// column, mirroring `public.oauth_providers`.
    pub const CREATE_VCS_CONNECTIONS: &str =
        include_str!("../migrations/vcs/0001__create_vcs_connections.sql");
}
