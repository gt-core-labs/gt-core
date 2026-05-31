//! [`WorkspaceCommand`] — intent the command layer validates into events
//! (`hq-mt-core.3`).
//!
//! A command is a *request* to change the catalog; [`decide`](WorkspaceCommand::decide)
//! validates it against current [`WorkspaceCatalog`] state and yields the
//! [`WorkspaceEvent`]s that record the change (or a [`WorkspaceError`] if the
//! request is illegal). This is the decide half of decide/apply: it is pure and
//! reads state but never mutates it — applying the returned events through
//! [`WorkspaceEvent::apply`] is the only mutation path, so replay stays
//! authoritative (non-negotiable #4).
//!
//! Legal lifecycle: a workspace is born [`Active`](WorkspaceStatus::Active), may
//! be [`Suspended`](WorkspaceStatus::Suspended) from `Active`, and
//! [`Archived`](WorkspaceStatus::Archived) from either `Active` or `Suspended`.
//! `Archived` is terminal. The reducer primitives in [`state`](crate::state)
//! deliberately enforce none of this — they apply already-decided events — so
//! the transition policy lives here, in one place.

use crate::events::WorkspaceEvent;
use crate::state::{WorkspaceCatalog, WorkspaceStatus};
use crate::workspace_id::WorkspaceId;

/// A request to change the workspace catalog.
///
/// Validated by [`decide`](WorkspaceCommand::decide). `#[non_exhaustive]` so the
/// lifecycle can grow (e.g. `Resume`) without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkspaceCommand {
    /// Create a new active workspace.
    Create {
        /// The id to create.
        id: WorkspaceId,
        /// Its display name.
        name: String,
    },
    /// Rename an existing workspace.
    Rename {
        /// The workspace to rename.
        id: WorkspaceId,
        /// The new display name.
        name: String,
    },
    /// Suspend an active workspace.
    Suspend {
        /// The workspace to suspend.
        id: WorkspaceId,
    },
    /// Archive a workspace (terminal).
    Archive {
        /// The workspace to archive.
        id: WorkspaceId,
    },
}

/// Why a [`WorkspaceCommand`] was rejected at decide time.
///
/// Each variant names the offending id (and, for a bad transition, the source
/// state) verbatim, so a rejected request is self-describing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceError {
    /// `Create` targeted an id that already exists.
    AlreadyExists(WorkspaceId),
    /// A command targeted an id that is not present.
    NotFound(WorkspaceId),
    /// `Create`/`Rename` carried a blank display name.
    EmptyName(WorkspaceId),
    /// A lifecycle transition was illegal for the workspace's current state.
    IllegalTransition {
        /// The workspace the transition was requested on.
        id: WorkspaceId,
        /// The state it was in.
        from: WorkspaceStatus,
        /// The command that was attempted.
        action: &'static str,
    },
}

impl std::fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkspaceError::AlreadyExists(id) => write!(f, "workspace {id} already exists"),
            WorkspaceError::NotFound(id) => write!(f, "workspace {id} not found"),
            WorkspaceError::EmptyName(id) => write!(f, "workspace {id}: display name must not be empty"),
            WorkspaceError::IllegalTransition { id, from, action } => {
                write!(f, "cannot {action} workspace {id} in {from:?}")
            }
        }
    }
}

impl std::error::Error for WorkspaceError {}

impl WorkspaceCommand {
    /// Validate this command against `catalog` and produce the resulting events.
    ///
    /// Pure: reads the catalog, never mutates it. Returns the events to apply on
    /// success, or the [`WorkspaceError`] explaining the rejection. The events
    /// are returned in a `Vec` so a future command can decide into more than one
    /// without changing the signature.
    pub fn decide(&self, catalog: &WorkspaceCatalog) -> Result<Vec<WorkspaceEvent>, WorkspaceError> {
        match self {
            WorkspaceCommand::Create { id, name } => {
                if catalog.contains(id) {
                    return Err(WorkspaceError::AlreadyExists(id.clone()));
                }
                require_name(id, name)?;
                Ok(vec![WorkspaceEvent::Created { id: id.clone(), name: name.clone() }])
            }
            WorkspaceCommand::Rename { id, name } => {
                require_present(catalog, id)?;
                require_name(id, name)?;
                Ok(vec![WorkspaceEvent::Renamed { id: id.clone(), name: name.clone() }])
            }
            WorkspaceCommand::Suspend { id } => {
                let status = require_present(catalog, id)?;
                // Only an active workspace may be suspended.
                if status != WorkspaceStatus::Active {
                    return Err(WorkspaceError::IllegalTransition {
                        id: id.clone(),
                        from: status,
                        action: "suspend",
                    });
                }
                Ok(vec![WorkspaceEvent::Suspended { id: id.clone() }])
            }
            WorkspaceCommand::Archive { id } => {
                let status = require_present(catalog, id)?;
                // Archive is terminal: an already-archived workspace cannot be
                // re-archived; Active and Suspended both archive.
                if status == WorkspaceStatus::Archived {
                    return Err(WorkspaceError::IllegalTransition {
                        id: id.clone(),
                        from: status,
                        action: "archive",
                    });
                }
                Ok(vec![WorkspaceEvent::Archived { id: id.clone() }])
            }
        }
    }
}

/// Require `id` to be present, returning its current status.
fn require_present(
    catalog: &WorkspaceCatalog,
    id: &WorkspaceId,
) -> Result<WorkspaceStatus, WorkspaceError> {
    catalog
        .get(id)
        .map(|e| e.status)
        .ok_or_else(|| WorkspaceError::NotFound(id.clone()))
}

/// Require a non-blank display name.
fn require_name(id: &WorkspaceId, name: &str) -> Result<(), WorkspaceError> {
    if name.trim().is_empty() {
        return Err(WorkspaceError::EmptyName(id.clone()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    /// Decide a command and apply the resulting events, returning the new catalog.
    fn run(mut catalog: WorkspaceCatalog, cmd: &WorkspaceCommand) -> WorkspaceCatalog {
        for ev in cmd.decide(&catalog).unwrap() {
            ev.apply(&mut catalog).unwrap();
        }
        catalog
    }

    fn with_acme() -> WorkspaceCatalog {
        run(
            WorkspaceCatalog::new(),
            &WorkspaceCommand::Create { id: id("acme"), name: "Acme".into() },
        )
    }

    #[test]
    fn create_emits_created_and_inserts_active() {
        let cat = with_acme();
        let e = cat.get(&id("acme")).unwrap();
        assert_eq!(e.name, "Acme");
        assert_eq!(e.status, WorkspaceStatus::Active);
    }

    #[test]
    fn create_rejects_duplicate_and_blank_name() {
        let cat = with_acme();
        assert_eq!(
            WorkspaceCommand::Create { id: id("acme"), name: "Other".into() }.decide(&cat),
            Err(WorkspaceError::AlreadyExists(id("acme")))
        );
        assert_eq!(
            WorkspaceCommand::Create { id: id("new"), name: "  ".into() }
                .decide(&WorkspaceCatalog::new()),
            Err(WorkspaceError::EmptyName(id("new")))
        );
    }

    #[test]
    fn rename_requires_presence_and_a_name() {
        let cat = with_acme();
        assert_eq!(
            WorkspaceCommand::Rename { id: id("acme"), name: "Acme Corp".into() }
                .decide(&cat)
                .unwrap(),
            vec![WorkspaceEvent::Renamed { id: id("acme"), name: "Acme Corp".into() }]
        );
        assert_eq!(
            WorkspaceCommand::Rename { id: id("ghost"), name: "x".into() }.decide(&cat),
            Err(WorkspaceError::NotFound(id("ghost")))
        );
        assert_eq!(
            WorkspaceCommand::Rename { id: id("acme"), name: "".into() }.decide(&cat),
            Err(WorkspaceError::EmptyName(id("acme")))
        );
    }

    #[test]
    fn suspend_only_from_active() {
        let cat = with_acme();
        let suspended = run(cat, &WorkspaceCommand::Suspend { id: id("acme") });
        assert_eq!(suspended.get(&id("acme")).unwrap().status, WorkspaceStatus::Suspended);
        // Suspending an already-suspended workspace is illegal.
        assert_eq!(
            WorkspaceCommand::Suspend { id: id("acme") }.decide(&suspended),
            Err(WorkspaceError::IllegalTransition {
                id: id("acme"),
                from: WorkspaceStatus::Suspended,
                action: "suspend",
            })
        );
    }

    #[test]
    fn archive_from_active_or_suspended_but_not_archived() {
        // From Active.
        let cat = with_acme();
        let archived = run(cat, &WorkspaceCommand::Archive { id: id("acme") });
        assert_eq!(archived.get(&id("acme")).unwrap().status, WorkspaceStatus::Archived);
        // Re-archiving is illegal (terminal).
        assert_eq!(
            WorkspaceCommand::Archive { id: id("acme") }.decide(&archived),
            Err(WorkspaceError::IllegalTransition {
                id: id("acme"),
                from: WorkspaceStatus::Archived,
                action: "archive",
            })
        );
        // From Suspended.
        let suspended = run(with_acme(), &WorkspaceCommand::Suspend { id: id("acme") });
        let archived2 = run(suspended, &WorkspaceCommand::Archive { id: id("acme") });
        assert_eq!(archived2.get(&id("acme")).unwrap().status, WorkspaceStatus::Archived);
    }

    #[test]
    fn decide_does_not_mutate_the_catalog() {
        let cat = with_acme();
        let _ = cat
            .get(&id("acme"))
            .map(|e| e.status)
            .filter(|s| *s == WorkspaceStatus::Active)
            .expect("active");
        // Decide a mutating command, then confirm the source catalog is unchanged.
        let _events = WorkspaceCommand::Suspend { id: id("acme") }.decide(&cat).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Active);
    }
}
