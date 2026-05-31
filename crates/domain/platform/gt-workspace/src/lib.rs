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
//!
//! Still to come on this epic: the PG adapter (`.5`/`.6`) and the workspace actor
//! (`.7`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod commands;
mod events;
mod repo;
mod state;
mod workspace_id;

pub use commands::{WorkspaceCommand, WorkspaceError};
pub use events::WorkspaceEvent;
pub use repo::{InMemoryWorkspaces, RepoError, WorkspaceRepository};
pub use state::{CatalogError, WorkspaceCatalog, WorkspaceEntry, WorkspaceStatus};
pub use workspace_id::{WorkspaceId, WorkspaceIdError, MAX_WORKSPACE_ID_LEN};
