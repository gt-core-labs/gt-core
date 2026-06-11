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

/// The platform GitHub App client (hq-vcs-connections.2): App-JWT minting + JIT installation-token
/// exchange + repo listing — the reusable engine `.4`/`.6`/`.7`/`.8` consume.
#[cfg(feature = "github")]
pub mod github;

/// DB-backed platform GitHub App config (hq-61ea43): the `public.github_app_config` row + the
/// [`GithubAppSource`](github_config::GithubAppSource) that resolves a [`github::GithubAppConfig`]
/// from the DB (falling back to env), so the App is configured from the UI rather than mounted files.
#[cfg(feature = "github")]
pub mod github_config;

/// The GitHub App install-flow REST adapter (hq-vcs-connections.2): install redirect, callback that
/// persists a `github_app` connection (no token stored), and a JIT repo-listing endpoint.
#[cfg(feature = "axum")]
pub mod github_http;

pub mod module;

#[cfg(feature = "axum")]
pub use module::VcsHttpModule;
pub use module::VcsModule;

#[cfg(feature = "pg")]
pub use repo::PgVcsConnections;
pub use repo::{
    ConnectionKind, ConnectionStatus, NewConnection, PatchConnection, VcsConnection,
    VcsConnectionRepo,
};

#[cfg(feature = "axum")]
pub use http::ConnectionApiState;

#[cfg(feature = "github")]
pub use github::{
    GithubAppClient, GithubAppConfig, InstallationRepo, InstallationToken, ENV_APP_ID,
    ENV_APP_ID_FILE, ENV_APP_PRIVATE_KEY_FILE, ENV_APP_SLUG, ENV_APP_WEBHOOK_SECRET_FILE,
};

#[cfg(feature = "github")]
pub use github_config::{GithubAppConfigInput, GithubAppConfigView, GithubAppSource};
#[cfg(all(feature = "github", feature = "pg"))]
pub use github_config::PgGithubAppConfig;

#[cfg(feature = "axum")]
pub use github_http::{github_router, GithubApiState};

/// The module-owned migration SQL, carried inline with `include_str!` so the boot migration runner
/// (`gt_module_migrate::apply`) seeds `public.vcs_connections`. A const is INERT until it is added
/// to the boot loop (`apply_pg_catalog` in `gt-mcp-server`) — see the system-providers-500 incident.
pub mod migrations {
    /// `public.vcs_connections` (hq-vcs-connections.1): the GLOBAL table with a `workspace_id`
    /// column, mirroring `public.oauth_providers`.
    pub const CREATE_VCS_CONNECTIONS: &str =
        include_str!("../migrations/vcs/0001__create_vcs_connections.sql");
    /// `public.github_app_config` (hq-61ea43): the single global row holding the platform GitHub App
    /// identity (App ID, slug) + the AES-GCM-sealed private key + webhook secret, so the App is
    /// configured from the UI/DB instead of mounted files.
    pub const CREATE_GITHUB_APP_CONFIG: &str =
        include_str!("../migrations/vcs/0002__create_github_app_config.sql");
}
