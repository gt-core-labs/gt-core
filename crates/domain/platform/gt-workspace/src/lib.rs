//! Workspace identity + catalog — the tenant-boundary primitive.
//!
//! ## Landed so far
//!
//! - [`WorkspaceId`] — the validated tenant-boundary slug (`hq-mt-core.1`).
//! - [`WorkspaceCatalog`] + [`WorkspaceEntry`] + [`WorkspaceStatus`] — the
//!   in-memory projection and its reducer primitives (`hq-mt-core.2`).
//!
//! Still to come on this epic: commands and events (`.3`), the repository port +
//! in-memory adapter (`.4`), the PG adapter (`.5`/`.6`), and the workspace actor
//! (`.7`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod state;
mod workspace_id;

pub use state::{CatalogError, WorkspaceCatalog, WorkspaceEntry, WorkspaceStatus};
pub use workspace_id::{WorkspaceId, WorkspaceIdError, MAX_WORKSPACE_ID_LEN};
