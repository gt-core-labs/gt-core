//! Workspace catalog state + reducer primitives (`hq-mt-core.2`).
//!
//! The in-memory projection a [`WorkspaceId`] keys into: a [`WorkspaceEntry`]
//! per tenant, held in a [`WorkspaceCatalog`]. The catalog's mutators
//! ([`create`](WorkspaceCatalog::create), [`rename`](WorkspaceCatalog::rename),
//! [`set_status`](WorkspaceCatalog::set_status)) are the **reducer primitives**:
//! the catalog is rebuilt by folding the event log through them, and they are
//! the only way its contents change. They are total and side-effect-free —
//! given the same starting catalog and the same call they always produce the
//! same result — so replay is byte-for-byte deterministic (non-negotiable #4).
//!
//! The `WorkspaceCommand` sum type and the event enum that drive these
//! primitives land in `hq-mt-core.3` (`commands.rs` / `events.rs`); the actor
//! that owns a catalog and applies events through it lands in `.7`. This module
//! deliberately defines neither — it is pure state plus the mutation surface
//! they build on, so no event type is forward-referenced here.

use std::collections::btree_map::{BTreeMap, Values};

use serde::{Deserialize, Serialize};

use crate::workspace_id::WorkspaceId;

/// Lifecycle state of a workspace.
///
/// A workspace is [`Active`](WorkspaceStatus::Active) when created. It may be
/// [`Suspended`](WorkspaceStatus::Suspended) (reversibly disabled — kept on disk,
/// no compute) and ultimately [`Archived`](WorkspaceStatus::Archived). The
/// command layer (`hq-mt-core.3`) decides which transitions are legal; this
/// type is just the vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStatus {
    /// Live: routable, hydratable, accepting work.
    Active,
    /// Reversibly disabled: retained but not running.
    Suspended,
    /// Terminal: retired, retained for audit only.
    Archived,
}

/// A single workspace's projected state.
///
/// Keyed by its immutable [`WorkspaceId`]; carries the mutable display
/// [`name`](WorkspaceEntry::name) and lifecycle [`status`](WorkspaceEntry::status).
/// Pure data — no handles — so it is cheap to clone and safe to snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntry {
    /// The tenant-boundary identifier. Immutable for the life of the workspace.
    pub id: WorkspaceId,
    /// Human-readable display name. Free-form; may change via `rename`.
    pub name: String,
    /// Current lifecycle state.
    pub status: WorkspaceStatus,
}

/// The set of known workspaces, keyed by [`WorkspaceId`].
///
/// Built by folding the workspace event log through the reducer primitives.
/// Iteration is ordered by id (the keys are a `BTreeMap`), so a listing is
/// deterministic without a sort at the call site.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCatalog {
    entries: BTreeMap<WorkspaceId, WorkspaceEntry>,
}

/// Why a [`WorkspaceCatalog`] mutation was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// `create` targeted an id that is already present.
    Duplicate(WorkspaceId),
    /// `rename`/`set_status` targeted an id that is not present.
    NotFound(WorkspaceId),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::Duplicate(id) => write!(f, "workspace {id} already exists"),
            CatalogError::NotFound(id) => write!(f, "workspace {id} not found"),
        }
    }
}

impl std::error::Error for CatalogError {}

impl WorkspaceCatalog {
    /// An empty catalog.
    pub fn new() -> Self {
        WorkspaceCatalog::default()
    }

    // --- Queries ------------------------------------------------------------

    /// Borrow the entry for `id`, if present.
    pub fn get(&self, id: &WorkspaceId) -> Option<&WorkspaceEntry> {
        self.entries.get(id)
    }

    /// Whether a workspace with `id` exists.
    pub fn contains(&self, id: &WorkspaceId) -> bool {
        self.entries.contains_key(id)
    }

    /// All entries, ordered by id.
    pub fn iter(&self) -> Values<'_, WorkspaceId, WorkspaceEntry> {
        self.entries.values()
    }

    /// Number of workspaces.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the catalog holds no workspaces.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // --- Reducer primitives -------------------------------------------------

    /// Insert a new [`Active`](WorkspaceStatus::Active) workspace.
    ///
    /// Rejects a [`Duplicate`](CatalogError::Duplicate) if `id` already exists —
    /// creation is not idempotent re-application; replaying a `Created` event
    /// over an existing id is a log inconsistency the caller must surface.
    pub fn create(&mut self, id: WorkspaceId, name: impl Into<String>) -> Result<(), CatalogError> {
        if self.entries.contains_key(&id) {
            return Err(CatalogError::Duplicate(id));
        }
        self.entries.insert(
            id.clone(),
            WorkspaceEntry { id, name: name.into(), status: WorkspaceStatus::Active },
        );
        Ok(())
    }

    /// Change a workspace's display name.
    ///
    /// Rejects [`NotFound`](CatalogError::NotFound) if `id` is absent.
    pub fn rename(&mut self, id: &WorkspaceId, name: impl Into<String>) -> Result<(), CatalogError> {
        let entry = self.entries.get_mut(id).ok_or_else(|| CatalogError::NotFound(id.clone()))?;
        entry.name = name.into();
        Ok(())
    }

    /// Set a workspace's lifecycle [`WorkspaceStatus`].
    ///
    /// Rejects [`NotFound`](CatalogError::NotFound) if `id` is absent. Whether a
    /// given transition is legal is the command layer's call (`hq-mt-core.3`);
    /// this primitive applies whatever the (already-validated) event carries.
    pub fn set_status(&mut self, id: &WorkspaceId, status: WorkspaceStatus) -> Result<(), CatalogError> {
        let entry = self.entries.get_mut(id).ok_or_else(|| CatalogError::NotFound(id.clone()))?;
        entry.status = status;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> WorkspaceId {
        WorkspaceId::new(s).unwrap()
    }

    #[test]
    fn new_catalog_is_empty() {
        let cat = WorkspaceCatalog::new();
        assert!(cat.is_empty());
        assert_eq!(cat.len(), 0);
        assert_eq!(cat.iter().count(), 0);
    }

    #[test]
    fn create_inserts_an_active_workspace() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("acme"), "Acme Inc").unwrap();
        let e = cat.get(&id("acme")).unwrap();
        assert_eq!(e.name, "Acme Inc");
        assert_eq!(e.status, WorkspaceStatus::Active);
        assert!(cat.contains(&id("acme")));
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn create_rejects_duplicate() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("acme"), "Acme").unwrap();
        assert_eq!(cat.create(id("acme"), "Other"), Err(CatalogError::Duplicate(id("acme"))));
        // The original entry is untouched.
        assert_eq!(cat.get(&id("acme")).unwrap().name, "Acme");
    }

    #[test]
    fn rename_changes_name_only() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("acme"), "Acme").unwrap();
        cat.rename(&id("acme"), "Acme Corp").unwrap();
        let e = cat.get(&id("acme")).unwrap();
        assert_eq!(e.name, "Acme Corp");
        assert_eq!(e.status, WorkspaceStatus::Active);
    }

    #[test]
    fn set_status_transitions() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("acme"), "Acme").unwrap();
        cat.set_status(&id("acme"), WorkspaceStatus::Suspended).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Suspended);
        cat.set_status(&id("acme"), WorkspaceStatus::Archived).unwrap();
        assert_eq!(cat.get(&id("acme")).unwrap().status, WorkspaceStatus::Archived);
    }

    #[test]
    fn mutators_reject_unknown_id() {
        let mut cat = WorkspaceCatalog::new();
        assert_eq!(cat.rename(&id("ghost"), "x"), Err(CatalogError::NotFound(id("ghost"))));
        assert_eq!(
            cat.set_status(&id("ghost"), WorkspaceStatus::Suspended),
            Err(CatalogError::NotFound(id("ghost")))
        );
    }

    #[test]
    fn iter_is_ordered_by_id() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("zeta"), "Z").unwrap();
        cat.create(id("alpha"), "A").unwrap();
        cat.create(id("mike"), "M").unwrap();
        let ids: Vec<&str> = cat.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["alpha", "mike", "zeta"]);
    }

    #[test]
    fn status_serializes_snake_case() {
        let json = serde_json::to_string(&WorkspaceStatus::Suspended).unwrap();
        assert_eq!(json, "\"suspended\"");
    }

    #[test]
    fn catalog_round_trips_through_serde() {
        let mut cat = WorkspaceCatalog::new();
        cat.create(id("acme"), "Acme").unwrap();
        cat.set_status(&id("acme"), WorkspaceStatus::Suspended).unwrap();
        let json = serde_json::to_string(&cat).unwrap();
        let back: WorkspaceCatalog = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cat);
    }
}
