//! Persistence port for Deacon: mirror each tracked / finished `DrainItem` into an
//! external store so an operator panel can see what is still pending without touching the
//! actor. Same shape as `MergeRepository` (returns `impl Future + Send` — no `async_trait`
//! / `dyn` boxing). The repo is **not** authoritative: the event log + `DeaconState`
//! reducer are. A write failure is best-effort.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::DrainItem;

pub trait DeaconRepository: Send + Sync {
    /// Insert or replace a tracked item. Called after every successful `Track` transition.
    fn upsert_item(&self, item: &DrainItem) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Remove a tracked item. Called after every successful `Finish` transition.
    fn remove_item(&self, id: &str) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one tracked item. Used by dashboards / read-side adapters, not by the actor.
    fn get_item(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<DrainItem>, AppError>> + Send;
}

/// In-memory `DeaconRepository` for tests and the composition root's bootstrap default.
#[derive(Default)]
pub struct InMemoryDeaconRepo {
    inner: Mutex<BTreeMap<String, DrainItem>>,
}

impl DeaconRepository for InMemoryDeaconRepo {
    async fn upsert_item(&self, item: &DrainItem) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-deacon repo poisoned".into()))?;
        g.insert(item.id.clone(), item.clone());
        Ok(())
    }

    async fn remove_item(&self, id: &str) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-deacon repo poisoned".into()))?;
        g.remove(id);
        Ok(())
    }

    async fn get_item(&self, id: &str) -> Result<Option<DrainItem>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-deacon repo poisoned".into()))?;
        Ok(g.get(id).cloned())
    }
}
