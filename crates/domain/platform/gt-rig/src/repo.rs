//! Domain persistence port + in-memory implementation.
//!
//! The catalog table (`rigs` in Postgres, eventually) is light-write (a registration is rare)
//! and read-mostly. The port lets `gt-store-pg` plug a real implementation later; the
//! in-memory version is the test safety net and the default the actor uses until the edge
//! installs one.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::RigEntry;

/// Port: rig registry persistence. RPITIT with `+ Send` so the port needs no `async_trait`
/// or boxing (same pattern as `QuotaRepository`, `BeadRepository`).
pub trait RigRepository: Send + Sync {
    /// Upsert a rig entry (idempotent: re-insert with same name replaces).
    fn upsert(&self, entry: &RigEntry) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Remove a rig by name. Returns `Ok(false)` if the entry was absent.
    fn remove(&self, name: &str) -> impl Future<Output = Result<bool, AppError>> + Send;

    /// Fetch a single entry by name.
    fn get(&self, name: &str) -> impl Future<Output = Result<Option<RigEntry>, AppError>> + Send;

    /// Owner of a given prefix, if any. The catalog enforces prefix uniqueness; the adapter
    /// is expected to mirror that with a unique index.
    fn prefix_owner(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Option<String>, AppError>> + Send;

    /// All entries, sorted by name. Used for hydration on boot.
    fn list(&self) -> impl Future<Output = Result<Vec<RigEntry>, AppError>> + Send;
}

/// Delegates through `Arc`.
impl<R: RigRepository + ?Sized> RigRepository for std::sync::Arc<R> {
    fn upsert(&self, entry: &RigEntry) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert(entry).await }
    }
    fn remove(&self, name: &str) -> impl Future<Output = Result<bool, AppError>> + Send {
        async move { (**self).remove(name).await }
    }
    fn get(&self, name: &str) -> impl Future<Output = Result<Option<RigEntry>, AppError>> + Send {
        async move { (**self).get(name).await }
    }
    fn prefix_owner(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Option<String>, AppError>> + Send {
        async move { (**self).prefix_owner(prefix).await }
    }
    fn list(&self) -> impl Future<Output = Result<Vec<RigEntry>, AppError>> + Send {
        async move { (**self).list().await }
    }
}

/// In-memory implementation: tests + the default before an edge adapter is wired.
#[derive(Default)]
pub struct InMemoryRigs {
    entries: Mutex<std::collections::BTreeMap<String, RigEntry>>,
}

impl RigRepository for InMemoryRigs {
    async fn upsert(&self, entry: &RigEntry) -> Result<(), AppError> {
        self.entries
            .lock()
            .unwrap()
            .insert(entry.name.clone(), entry.clone());
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<bool, AppError> {
        Ok(self.entries.lock().unwrap().remove(name).is_some())
    }

    async fn get(&self, name: &str) -> Result<Option<RigEntry>, AppError> {
        Ok(self.entries.lock().unwrap().get(name).cloned())
    }

    async fn prefix_owner(&self, prefix: &str) -> Result<Option<String>, AppError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .values()
            .find(|e| e.prefix == prefix)
            .map(|e| e.name.clone()))
    }

    async fn list(&self) -> Result<Vec<RigEntry>, AppError> {
        Ok(self.entries.lock().unwrap().values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_roundtrips() {
        let repo = InMemoryRigs::default();
        let entry = RigEntry::new("plane", "pl", "git@x:y/plane.git", "main", 1);
        repo.upsert(&entry).await.unwrap();
        assert_eq!(repo.get("plane").await.unwrap(), Some(entry.clone()));
        assert_eq!(repo.prefix_owner("pl").await.unwrap(), Some("plane".into()));
        assert_eq!(repo.list().await.unwrap(), vec![entry]);
        assert!(repo.remove("plane").await.unwrap());
        assert!(!repo.remove("plane").await.unwrap());
        assert!(repo.get("plane").await.unwrap().is_none());
    }
}
