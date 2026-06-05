//! Workspace identity + catalog — the tenant-boundary primitive.
//!
//! ## Landed so far
//!
//! - [`WorkspaceId`] — the validated tenant-boundary slug (`hq-mt-core.1`).
//! - [`WorkspaceCatalog`] + [`WorkspaceEntry`] + [`WorkspaceStatus`] — the
//!   in-memory projection and its reducer primitives (`hq-mt-core.2`).
//! - [`WorkspaceCommand`] + [`WorkspaceEvent`] — the decide/apply command layer
//!   (Create/Rename/Suspend/Archive) over the catalog (`hq-mt-core.3`).
//! - [`WorkspaceRepository`] + [`InMemoryWorkspaces`] — the persistence port and
//!   its in-memory adapter (`hq-mt-core.4`).
//! - [`WorkspaceActor`] — the write-side owner that decides/applies/persists/logs
//!   commands and guards the byte-for-byte replay gate (`hq-mt-core.7`).
//!
//! ## Adapters
//!
//! Adapters of the [`WorkspaceRepository`] port live in this crate beside the
//! port, so the dependency points inward (adapter → port) and never crosses a
//! tier boundary. [`InMemoryWorkspaces`] is always present; heavier adapters are
//! gated so the default build pulls no database or web framework:
//!
//! - `pg` → [`PgWorkspaces`], the Postgres adapter (`hq-mt-core.6`).
//! - `axum` → [`WorkspaceContext`], the request extractor (`hq-mt-core.8`).
//!
//! The generic migration SQL lives in the kernel `gt-store-pg` crate (no domain
//! dependency); the `pg` adapter reads that table.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod actor;
mod commands;
mod events;
/// The off-by-default `axum` REST adapter (`hq-fe-api-platform.1`): maps `workspace.*`
/// to REST routes dispatching to the same command layer the MCP handler wraps.
#[cfg(feature = "axum")]
pub mod http;
#[cfg(feature = "axum")]
mod module;
#[cfg(feature = "pg")]
mod pg;
mod repo;
mod state;
#[cfg(feature = "axum")]
mod web;
mod workspace_id;

pub use actor::{ActorError, WorkspaceActor};
pub use commands::{WorkspaceCommand, WorkspaceError};
pub use events::WorkspaceEvent;
pub use repo::{InMemoryWorkspaces, RepoError, WorkspaceRepository};
pub use state::{CatalogError, WorkspaceCatalog, WorkspaceEntry, WorkspaceStatus};
pub use workspace_id::{WorkspaceId, WorkspaceIdError, MAX_WORKSPACE_ID_LEN};

#[cfg(feature = "pg")]
pub use pg::PgWorkspaces;
#[cfg(feature = "axum")]
pub use http::{workspace_router, ApiDoc, TenantProvisioner, WorkspaceApiState};
#[cfg(feature = "axum")]
pub use module::WorkspaceModule;
#[cfg(feature = "axum")]
pub use web::{WorkspaceClaim, WorkspaceContext, WorkspaceContextRejection, WORKSPACE_HEADER};
