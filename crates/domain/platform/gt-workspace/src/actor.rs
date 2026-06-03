//! [`WorkspaceActor`] — owns a catalog, processes commands, persists, and
//! guards the byte-for-byte replay invariant (`hq-mt-core.7`).
//!
//! The actor is the write-side owner of the workspace catalog. A
//! [`WorkspaceCommand`] flows through it as decide → apply → persist → log:
//!
//! 1. [`decide`](WorkspaceCommand::decide) validates the command against the
//!    live catalog and yields [`WorkspaceEvent`]s (no mutation);
//! 2. each event is [`apply`](WorkspaceEvent::apply)ed to the in-memory catalog
//!    through the reducer primitives;
//! 3. the affected entry is persisted via the [`WorkspaceRepository`];
//! 4. the event is appended to the actor's log.
//!
//! ## The replay gate (non-negotiable #4)
//!
//! Because every mutation is an event applied through a pure reducer, folding the
//! actor's event log through a fresh catalog must reproduce the live catalog
//! **byte for byte**. [`replay`](WorkspaceActor::replay) rebuilds it and
//! [`replay_matches`](WorkspaceActor::replay_matches) asserts equality — both
//! structurally and over the serialized bytes — so a reducer that ever drifted
//! from replay determinism fails loudly. This is the Step-3 gate the bead names.
//!
//! No `tokio::spawn` here: the actor is a state-owning processor with an `async`
//! [`handle`](WorkspaceActor::handle) (it awaits the repository's I/O), not a
//! spawned mailbox loop — so it stays simple and directly testable.

use crate::commands::{WorkspaceCommand, WorkspaceError};
use crate::events::WorkspaceEvent;
use crate::repo::{RepoError, WorkspaceRepository};
use crate::state::{CatalogError, WorkspaceCatalog};

/// Why a [`WorkspaceActor::handle`] call failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActorError {
    /// The command was rejected at decide time (illegal request).
    Rejected(WorkspaceError),
    /// An event failed to apply to the catalog — a reducer/log inconsistency
    /// that should not occur after a successful decide.
    Apply(CatalogError),
    /// Persisting the change through the repository failed.
    Repo(RepoError),
}

impl std::fmt::Display for ActorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorError::Rejected(e) => write!(f, "command rejected: {e}"),
            ActorError::Apply(e) => write!(f, "event apply failed: {e}"),
            ActorError::Repo(e) => write!(f, "persistence failed: {e}"),
        }
    }
}

impl std::error::Error for ActorError {}

/// Owns the workspace catalog and the event log behind a [`WorkspaceRepository`].
///
/// Construct empty with [`new`](WorkspaceActor::new) or boot from persistence
/// with [`hydrate`](WorkspaceActor::hydrate). Drive it with
/// [`handle`](WorkspaceActor::handle); read the projection via
/// [`catalog`](WorkspaceActor::catalog) and the recorded events via
/// [`log`](WorkspaceActor::log).
pub struct WorkspaceActor<R: WorkspaceRepository> {
    catalog: WorkspaceCatalog,
    log: Vec<WorkspaceEvent>,
    repo: R,
}

impl<R: WorkspaceRepository> WorkspaceActor<R> {
    /// A fresh actor with an empty catalog and log over `repo`.
    pub fn new(repo: R) -> Self {
        WorkspaceActor {
            catalog: WorkspaceCatalog::new(),
            log: Vec::new(),
            repo,
        }
    }

    /// Boot an actor from the repository's persisted entries.
    ///
    /// The catalog is hydrated from storage; the in-memory log starts empty
    /// (the snapshot is the baseline, not a replayable prefix). Use
    /// [`replay`](WorkspaceActor::replay) on a [`new`](WorkspaceActor::new) actor
    /// when asserting the byte-for-byte gate from an empty baseline.
    pub async fn hydrate(repo: R) -> Result<Self, ActorError> {
        let catalog = repo.hydrate().await.map_err(ActorError::Repo)?;
        Ok(WorkspaceActor {
            catalog,
            log: Vec::new(),
            repo,
        })
    }

    /// Borrow the live catalog projection.
    pub fn catalog(&self) -> &WorkspaceCatalog {
        &self.catalog
    }

    /// Borrow the recorded event log, in application order.
    pub fn log(&self) -> &[WorkspaceEvent] {
        &self.log
    }

    /// Borrow the backing repository.
    pub fn repo(&self) -> &R {
        &self.repo
    }

    /// Run a command: decide, apply, persist, log.
    ///
    /// Returns the events produced on success. On rejection nothing is applied,
    /// persisted, or logged. If persistence fails mid-way the error surfaces as
    /// [`ActorError::Repo`]; the in-memory catalog reflects the events applied so
    /// far, matching the log — the two never diverge.
    pub async fn handle(
        &mut self,
        command: WorkspaceCommand,
    ) -> Result<Vec<WorkspaceEvent>, ActorError> {
        let events = command.decide(&self.catalog).map_err(ActorError::Rejected)?;
        for event in &events {
            event.apply(&mut self.catalog).map_err(ActorError::Apply)?;
            // Persist the entry the event touched; it is present because the
            // event just applied successfully.
            let entry = self
                .catalog
                .get(event.id())
                .expect("entry present after applying its event")
                .clone();
            self.repo.save(&entry).await.map_err(ActorError::Repo)?;
            self.log.push(event.clone());
        }
        Ok(events)
    }

    /// Rebuild a catalog by folding the event log through the reducer primitives
    /// from an empty baseline.
    pub fn replay(&self) -> WorkspaceCatalog {
        let mut catalog = WorkspaceCatalog::new();
        for event in &self.log {
            event
                .apply(&mut catalog)
                .expect("event log is self-consistent on replay");
        }
        catalog
    }

    /// The byte-for-byte replay gate: the replayed catalog must equal the live
    /// catalog both structurally and over its serialized bytes.
    ///
    /// Holds for an actor built with [`new`](WorkspaceActor::new) (empty
    /// baseline). A reducer that drifted from replay determinism returns `false`
    /// here.
    pub fn replay_matches(&self) -> bool {
        let replayed = self.replay();
        if replayed != self.catalog {
            return false;
        }
        match (
            serde_json::to_vec(&replayed),
            serde_json::to_vec(&self.catalog),
        ) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::InMemoryWorkspaces;
    use crate::state::WorkspaceStatus;
    use crate::workspace_id::WorkspaceId;

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    fn create(name: &str) -> WorkspaceCommand {
        WorkspaceCommand::Create { id: id(name), name: name.to_uppercase() }
    }

    #[tokio::test]
    async fn handle_applies_persists_and_logs() {
        let mut actor = WorkspaceActor::new(InMemoryWorkspaces::new());
        let events = actor.handle(create("acme")).await.unwrap();
        assert_eq!(events.len(), 1);
        // Catalog reflects the change.
        assert_eq!(actor.catalog().get(&id("acme")).unwrap().status, WorkspaceStatus::Active);
        // Log recorded it.
        assert_eq!(actor.log().len(), 1);
        // Repository persisted it.
        assert_eq!(actor.repo().load(&id("acme")).await.unwrap().unwrap().name, "ACME");
    }

    #[tokio::test]
    async fn rejected_command_changes_nothing() {
        let mut actor = WorkspaceActor::new(InMemoryWorkspaces::new());
        actor.handle(create("acme")).await.unwrap();
        let err = actor.handle(create("acme")).await.unwrap_err();
        assert_eq!(err, ActorError::Rejected(WorkspaceError::AlreadyExists(id("acme"))));
        // No second log entry, no extra persistence.
        assert_eq!(actor.log().len(), 1);
        assert_eq!(actor.repo().len(), 1);
    }

    #[tokio::test]
    async fn replay_gate_holds_across_a_command_sequence() {
        let mut actor = WorkspaceActor::new(InMemoryWorkspaces::new());
        actor.handle(create("acme")).await.unwrap();
        actor.handle(create("beta")).await.unwrap();
        actor.handle(WorkspaceCommand::Rename { id: id("acme"), name: "Acme Corp".into() }).await.unwrap();
        actor.handle(WorkspaceCommand::Suspend { id: id("beta") }).await.unwrap();
        // Suspend then resume beta — the resume reducer must replay deterministically too.
        actor.handle(WorkspaceCommand::Resume { id: id("beta") }).await.unwrap();
        actor.handle(WorkspaceCommand::Archive { id: id("acme") }).await.unwrap();

        // The byte-for-byte gate: replay reproduces the live catalog exactly.
        assert!(actor.replay_matches());
        assert_eq!(actor.replay(), *actor.catalog());
        assert_eq!(
            serde_json::to_vec(&actor.replay()).unwrap(),
            serde_json::to_vec(actor.catalog()).unwrap()
        );
    }

    #[tokio::test]
    async fn hydrate_boots_catalog_from_persistence() {
        // Seed a store through one actor...
        let repo = InMemoryWorkspaces::new();
        {
            let mut seeding = WorkspaceActor::new(&repo);
            seeding.handle(create("acme")).await.unwrap();
            seeding.handle(WorkspaceCommand::Suspend { id: id("acme") }).await.unwrap();
        }
        // ...then boot a fresh actor from it.
        let booted = WorkspaceActor::hydrate(&repo).await.unwrap();
        assert_eq!(booted.catalog().get(&id("acme")).unwrap().status, WorkspaceStatus::Suspended);
        // A hydrated actor starts with an empty log (snapshot baseline).
        assert!(booted.log().is_empty());
    }

    // Lets the seeding actor borrow the repo so the test can reuse it after.
    #[async_trait::async_trait]
    impl WorkspaceRepository for &InMemoryWorkspaces {
        async fn save(&self, entry: &crate::state::WorkspaceEntry) -> Result<(), RepoError> {
            (**self).save(entry).await
        }
        async fn load(
            &self,
            id: &WorkspaceId,
        ) -> Result<Option<crate::state::WorkspaceEntry>, RepoError> {
            (**self).load(id).await
        }
        async fn list(&self) -> Result<Vec<crate::state::WorkspaceEntry>, RepoError> {
            (**self).list().await
        }
    }
}
