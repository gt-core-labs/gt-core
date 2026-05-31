//! [`WorkspaceRepository`] + [`InMemoryWorkspaces`] — persistence for the
//! workspace catalog (`hq-mt-core.4`).
//!
//! The catalog ([`WorkspaceCatalog`]) and its command layer
//! ([`commands`](crate::commands)) are pure in-memory state. This bead adds the
//! port that persists a [`WorkspaceEntry`] across restarts — the seam a Postgres
//! adapter (`PgWorkspaces`, `hq-mt-core.6`) and the workspace actor
//! (`hq-mt-core.7`) plug into.
//!
//! Per the crate's hexagonal rule the module ships the **port** plus an
//! **in-memory adapter** ([`InMemoryWorkspaces`]); the PG adapter lands in
//! `gt-store-pg` (Phase 4). The port is `async` because the real adapter does
//! database I/O at the edge — [`hydrate`](WorkspaceRepository::hydrate) rebuilds
//! a [`WorkspaceCatalog`] from the stored entries so a caller can boot from
//! persistence in one call.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::state::{WorkspaceCatalog, WorkspaceEntry, WorkspaceStatus};
use crate::workspace_id::WorkspaceId;

/// Why a [`WorkspaceRepository`] operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepoError {
    /// The backing store could not complete the operation; carries an
    /// adapter-specific reason (a DB error, a connection failure).
    Backend(String),
    /// [`hydrate`](WorkspaceRepository::hydrate) read a store whose rows do not
    /// form a valid catalog (e.g. a duplicate id); carries the reason.
    Inconsistent(String),
}

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoError::Backend(reason) => write!(f, "workspace store backend failed: {reason}"),
            RepoError::Inconsistent(reason) => write!(f, "workspace store inconsistent: {reason}"),
        }
    }
}

impl std::error::Error for RepoError {}

/// Persistence port for workspace entries.
///
/// [`save`](WorkspaceRepository::save) upserts one entry (insert or overwrite by
/// id), [`load`](WorkspaceRepository::load) fetches one, and
/// [`list`](WorkspaceRepository::list) returns every entry ordered by id. The
/// provided [`hydrate`](WorkspaceRepository::hydrate) rebuilds a
/// [`WorkspaceCatalog`] from `list`. `async` + `dyn`-friendly so a Postgres
/// adapter (`hq-mt-core.6`) and the in-memory one share the seam.
#[async_trait]
pub trait WorkspaceRepository: Send + Sync {
    /// Insert or overwrite `entry`, keyed by its id.
    async fn save(&self, entry: &WorkspaceEntry) -> Result<(), RepoError>;

    /// Fetch the entry for `id`, or `None` if absent.
    async fn load(&self, id: &WorkspaceId) -> Result<Option<WorkspaceEntry>, RepoError>;

    /// Every stored entry, ordered by id.
    async fn list(&self) -> Result<Vec<WorkspaceEntry>, RepoError>;

    /// Rebuild a [`WorkspaceCatalog`] from the stored entries.
    ///
    /// Folds [`list`](WorkspaceRepository::list) through the catalog reducer
    /// primitives: each entry is created, then set to its persisted status.
    /// Returns [`RepoError::Inconsistent`] if the rows do not form a valid
    /// catalog (a duplicate id). Default-implemented over `list`, so every
    /// adapter gets it for free.
    async fn hydrate(&self) -> Result<WorkspaceCatalog, RepoError> {
        let mut catalog = WorkspaceCatalog::new();
        for entry in self.list().await? {
            catalog
                .create(entry.id.clone(), entry.name.clone())
                .map_err(|e| RepoError::Inconsistent(e.to_string()))?;
            if entry.status != WorkspaceStatus::Active {
                // Created lands Active; replay the persisted terminal/suspended
                // status. `create` just succeeded, so this id is present.
                catalog
                    .set_status(&entry.id, entry.status)
                    .map_err(|e| RepoError::Inconsistent(e.to_string()))?;
            }
        }
        Ok(catalog)
    }
}

/// In-memory [`WorkspaceRepository`] backed by an id-keyed map.
///
/// The hexagonal in-repo adapter (Port + InMemory): a process-lifetime store for
/// tests and single-node boots, with no database dependency. Entries are held in
/// a [`BTreeMap`] so [`list`](WorkspaceRepository::list) is ordered by id.
#[derive(Default)]
pub struct InMemoryWorkspaces {
    entries: Mutex<BTreeMap<WorkspaceId, WorkspaceEntry>>,
}

impl InMemoryWorkspaces {
    /// An empty store.
    pub fn new() -> Self {
        InMemoryWorkspaces::default()
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("workspace store mutex not poisoned").len()
    }

    /// Whether the store holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl WorkspaceRepository for InMemoryWorkspaces {
    async fn save(&self, entry: &WorkspaceEntry) -> Result<(), RepoError> {
        self.entries
            .lock()
            .expect("workspace store mutex not poisoned")
            .insert(entry.id.clone(), entry.clone());
        Ok(())
    }

    async fn load(&self, id: &WorkspaceId) -> Result<Option<WorkspaceEntry>, RepoError> {
        Ok(self
            .entries
            .lock()
            .expect("workspace store mutex not poisoned")
            .get(id)
            .cloned())
    }

    async fn list(&self) -> Result<Vec<WorkspaceEntry>, RepoError> {
        Ok(self
            .entries
            .lock()
            .expect("workspace store mutex not poisoned")
            .values()
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    fn entry(name: &str, status: WorkspaceStatus) -> WorkspaceEntry {
        WorkspaceEntry { id: id(name), name: name.to_uppercase(), status }
    }

    #[tokio::test]
    async fn save_then_load_round_trips() {
        let repo = InMemoryWorkspaces::new();
        assert!(repo.is_empty());
        let e = entry("acme", WorkspaceStatus::Active);
        repo.save(&e).await.unwrap();
        assert_eq!(repo.load(&id("acme")).await.unwrap(), Some(e));
        assert_eq!(repo.len(), 1);
    }

    #[tokio::test]
    async fn load_absent_is_none() {
        let repo = InMemoryWorkspaces::new();
        assert_eq!(repo.load(&id("ghost")).await.unwrap(), None);
    }

    #[tokio::test]
    async fn save_overwrites_by_id() {
        let repo = InMemoryWorkspaces::new();
        repo.save(&entry("acme", WorkspaceStatus::Active)).await.unwrap();
        repo.save(&entry("acme", WorkspaceStatus::Suspended)).await.unwrap();
        assert_eq!(repo.len(), 1);
        assert_eq!(repo.load(&id("acme")).await.unwrap().unwrap().status, WorkspaceStatus::Suspended);
    }

    #[tokio::test]
    async fn list_is_ordered_by_id() {
        let repo = InMemoryWorkspaces::new();
        repo.save(&entry("zeta", WorkspaceStatus::Active)).await.unwrap();
        repo.save(&entry("alpha", WorkspaceStatus::Active)).await.unwrap();
        repo.save(&entry("mike", WorkspaceStatus::Active)).await.unwrap();
        let ids: Vec<String> = repo.list().await.unwrap().into_iter().map(|e| e.id.as_str().to_string()).collect();
        assert_eq!(ids, ["alpha", "mike", "zeta"]);
    }

    #[tokio::test]
    async fn hydrate_rebuilds_a_catalog_preserving_status() {
        let repo = InMemoryWorkspaces::new();
        repo.save(&entry("acme", WorkspaceStatus::Active)).await.unwrap();
        repo.save(&entry("beta", WorkspaceStatus::Suspended)).await.unwrap();
        repo.save(&entry("gone", WorkspaceStatus::Archived)).await.unwrap();
        let catalog = repo.hydrate().await.unwrap();
        assert_eq!(catalog.len(), 3);
        assert_eq!(catalog.get(&id("acme")).unwrap().status, WorkspaceStatus::Active);
        assert_eq!(catalog.get(&id("beta")).unwrap().status, WorkspaceStatus::Suspended);
        assert_eq!(catalog.get(&id("gone")).unwrap().status, WorkspaceStatus::Archived);
    }

    #[tokio::test]
    async fn hydrate_of_empty_store_is_an_empty_catalog() {
        let repo = InMemoryWorkspaces::new();
        assert!(repo.hydrate().await.unwrap().is_empty());
    }
}
