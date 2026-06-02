//! Persistence port for Sheriff: mirror each registered/cleared `Watch` into an external
//! store so dashboards see the live registry without touching the actor. Same shape as
//! `MergeRepository` (returns `impl Future + Send` — no `async_trait` / `dyn` boxing).
//!
//! The repo is **not** authoritative: the event log + `SheriffState` reducer are. A write
//! failure is best-effort, the actor logs and keeps emitting.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::Watch;

pub trait SheriffRepository: Send + Sync {
    /// Insert or replace a watch. Called after every successful transition.
    fn upsert_watch(&self, watch: &Watch) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one watch. Used by dashboards / read-side adapters, not by the actor.
    fn get_watch(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Watch>, AppError>> + Send;
}

/// In-memory `SheriffRepository` for tests and the composition root's bootstrap default.
#[derive(Default)]
pub struct InMemorySheriffRepo {
    inner: Mutex<BTreeMap<String, Watch>>,
}

impl SheriffRepository for InMemorySheriffRepo {
    async fn upsert_watch(&self, watch: &Watch) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-sheriff repo poisoned".into()))?;
        g.insert(watch.id.clone(), watch.clone());
        Ok(())
    }

    async fn get_watch(&self, id: &str) -> Result<Option<Watch>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-sheriff repo poisoned".into()))?;
        Ok(g.get(id).cloned())
    }
}
