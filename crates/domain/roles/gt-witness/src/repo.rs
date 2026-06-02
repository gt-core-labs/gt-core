//! Persistence port for Witness: mirror each watched / cleared target into an external
//! store so dashboards see the live registry without touching the actor. Same shape as
//! `SheriffRepository` (returns `impl Future + Send` — no `async_trait` / `dyn` boxing).
//!
//! The repo is **not** authoritative: the event log + `WitnessState` reducer are. A write
//! failure is best-effort; the actor logs and keeps emitting.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::StuckTarget;

pub trait WitnessRepository: Send + Sync {
    /// Insert or replace a target. Called after every successful transition.
    fn upsert_target(
        &self,
        target: &StuckTarget,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one target. Used by dashboards / read-side adapters, not by the actor.
    fn get_target(
        &self,
        worker: &str,
    ) -> impl Future<Output = Result<Option<StuckTarget>, AppError>> + Send;
}

/// In-memory `WitnessRepository` for tests and the composition root's bootstrap default.
#[derive(Default)]
pub struct InMemoryWitnessRepo {
    inner: Mutex<BTreeMap<String, StuckTarget>>,
}

impl WitnessRepository for InMemoryWitnessRepo {
    async fn upsert_target(&self, target: &StuckTarget) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-witness repo poisoned".into()))?;
        g.insert(target.worker.clone(), target.clone());
        Ok(())
    }

    async fn get_target(&self, worker: &str) -> Result<Option<StuckTarget>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-witness repo poisoned".into()))?;
        Ok(g.get(worker).cloned())
    }
}
