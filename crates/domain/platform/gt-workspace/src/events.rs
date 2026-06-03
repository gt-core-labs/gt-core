//! [`WorkspaceEvent`] — the workspace lifecycle event log (`hq-mt-core.3`).
//!
//! `hq-mt-core.2` left the event type to this bead, named in its module docs:
//! the enum that drives the [`WorkspaceCatalog`] reducer primitives. An event is
//! the *already-decided* record of a change — [`WorkspaceCommand`] in
//! [`commands`](crate::commands) validates intent against catalog state and
//! emits these; [`apply`](WorkspaceEvent::apply) folds one back into a catalog
//! through the reducer primitives, so the catalog is rebuilt deterministically
//! by replaying the log (non-negotiable #4).
//!
//! ## Versioned kinds
//!
//! Each variant carries a versioned [`kind`](WorkspaceEvent::kind) string
//! (`workspace.created.v1`, …) so the audit log and a replay reducer can match
//! kind + version and a future `.v2` can coexist (non-negotiable: event kinds
//! are versioned, never bare).

use serde::{Deserialize, Serialize};

use crate::state::{CatalogError, WorkspaceCatalog, WorkspaceStatus};
use crate::workspace_id::WorkspaceId;

/// A decided change to the workspace catalog.
///
/// Produced by [`WorkspaceCommand::decide`](crate::commands::WorkspaceCommand::decide)
/// and consumed by [`apply`](WorkspaceEvent::apply). `#[non_exhaustive]` so later
/// beads can add variants without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WorkspaceEvent {
    /// A workspace was created [`Active`](WorkspaceStatus::Active).
    Created {
        /// The new workspace's id.
        id: WorkspaceId,
        /// Its initial display name.
        name: String,
    },
    /// A workspace's display name changed.
    Renamed {
        /// The workspace renamed.
        id: WorkspaceId,
        /// The new display name.
        name: String,
    },
    /// A workspace was suspended (reversibly disabled).
    Suspended {
        /// The workspace suspended.
        id: WorkspaceId,
    },
    /// A suspended workspace was resumed back to active.
    Resumed {
        /// The workspace resumed.
        id: WorkspaceId,
    },
    /// A workspace was archived (terminal).
    Archived {
        /// The workspace archived.
        id: WorkspaceId,
    },
}

impl WorkspaceEvent {
    /// The workspace this event targets.
    pub fn id(&self) -> &WorkspaceId {
        match self {
            WorkspaceEvent::Created { id, .. }
            | WorkspaceEvent::Renamed { id, .. }
            | WorkspaceEvent::Suspended { id }
            | WorkspaceEvent::Resumed { id }
            | WorkspaceEvent::Archived { id } => id,
        }
    }

    /// The versioned event-kind string, for the audit log and replay matching.
    pub fn kind(&self) -> &'static str {
        match self {
            WorkspaceEvent::Created { .. } => "workspace.created.v1",
            WorkspaceEvent::Renamed { .. } => "workspace.renamed.v1",
            WorkspaceEvent::Suspended { .. } => "workspace.suspended.v1",
            WorkspaceEvent::Resumed { .. } => "workspace.resumed.v1",
            WorkspaceEvent::Archived { .. } => "workspace.archived.v1",
        }
    }

    /// Fold this event into `catalog` through the reducer primitives.
    ///
    /// `Created` inserts, `Renamed` renames, `Suspended`/`Archived` set the
    /// lifecycle status. Errors with the underlying [`CatalogError`] if the log
    /// is inconsistent with the catalog (e.g. a `Created` over an existing id, or
    /// a transition on an absent id) — the same loud surface the primitives give.
    pub fn apply(&self, catalog: &mut WorkspaceCatalog) -> Result<(), CatalogError> {
        match self {
            WorkspaceEvent::Created { id, name } => catalog.create(id.clone(), name.clone()),
            WorkspaceEvent::Renamed { id, name } => catalog.rename(id, name.clone()),
            WorkspaceEvent::Suspended { id } => {
                catalog.set_status(id, WorkspaceStatus::Suspended)
            }
            WorkspaceEvent::Resumed { id } => catalog.set_status(id, WorkspaceStatus::Active),
            WorkspaceEvent::Archived { id } => catalog.set_status(id, WorkspaceStatus::Archived),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    #[test]
    fn kinds_are_versioned() {
        assert_eq!(
            WorkspaceEvent::Created { id: id("a"), name: "A".into() }.kind(),
            "workspace.created.v1"
        );
        assert_eq!(WorkspaceEvent::Renamed { id: id("a"), name: "B".into() }.kind(), "workspace.renamed.v1");
        assert_eq!(WorkspaceEvent::Suspended { id: id("a") }.kind(), "workspace.suspended.v1");
        assert_eq!(WorkspaceEvent::Resumed { id: id("a") }.kind(), "workspace.resumed.v1");
        assert_eq!(WorkspaceEvent::Archived { id: id("a") }.kind(), "workspace.archived.v1");
    }

    #[test]
    fn id_accessor_names_the_target() {
        assert_eq!(WorkspaceEvent::Suspended { id: id("acme") }.id(), &id("acme"));
    }

    #[test]
    fn apply_folds_the_lifecycle_into_a_catalog() {
        let mut cat = WorkspaceCatalog::new();
        WorkspaceEvent::Created { id: id("acme"), name: "Acme".into() }
            .apply(&mut cat)
            .unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Active);

        WorkspaceEvent::Renamed { id: id("acme"), name: "Acme Corp".into() }
            .apply(&mut cat)
            .unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().name, "Acme Corp");

        WorkspaceEvent::Suspended { id: id("acme") }.apply(&mut cat).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Suspended);

        WorkspaceEvent::Resumed { id: id("acme") }.apply(&mut cat).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Active);

        WorkspaceEvent::Archived { id: id("acme") }.apply(&mut cat).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Archived);
    }

    #[test]
    fn apply_surfaces_an_inconsistent_log() {
        let mut cat = WorkspaceCatalog::new();
        // Renaming before creation is a log inconsistency.
        assert_eq!(
            WorkspaceEvent::Renamed { id: id("ghost"), name: "x".into() }.apply(&mut cat),
            Err(CatalogError::NotFound(id("ghost")))
        );
    }

    #[test]
    fn event_round_trips_through_serde() {
        let ev = WorkspaceEvent::Created { id: id("acme"), name: "Acme".into() };
        let json = serde_json::to_string(&ev).unwrap();
        let back: WorkspaceEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }
}
