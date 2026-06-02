//! Persistence port for Mayor: mirror each touched `Delegation` into an external store so
//! dashboards see the live board without touching the actor. Same shape as
//! `MergeRepository` / `SheriffRepository` (returns `impl Future + Send` — no
//! `async_trait` / `dyn` boxing).
//!
//! The repo is **not** authoritative: the event log + `MayorState` reducer are. A write
//! failure is best-effort; the actor logs and keeps emitting.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::Delegation;

pub trait MayorRepository: Send + Sync {
    /// Insert or replace a delegation. Called after every successful transition.
    fn upsert_delegation(
        &self,
        delegation: &Delegation,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one delegation. Used by dashboards / read-side adapters, not by the actor.
    fn get_delegation(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Delegation>, AppError>> + Send;
}

/// In-memory `MayorRepository` for tests and the composition root's bootstrap default.
#[derive(Default)]
pub struct InMemoryMayorRepo {
    inner: Mutex<BTreeMap<String, Delegation>>,
}

impl MayorRepository for InMemoryMayorRepo {
    async fn upsert_delegation(&self, delegation: &Delegation) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-mayor repo poisoned".into()))?;
        g.insert(delegation.id.clone(), delegation.clone());
        Ok(())
    }

    async fn get_delegation(&self, id: &str) -> Result<Option<Delegation>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-mayor repo poisoned".into()))?;
        Ok(g.get(id).cloned())
    }
}
