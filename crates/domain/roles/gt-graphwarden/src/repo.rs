//! Persistence port for the Graph Warden: mirror each rig's custody record into an
//! external store so dashboards see freshness without touching the actor. Same shape as
//! `WitnessRepository` (`impl Future + Send`, no `async_trait`/`dyn`).
//!
//! The repo is **not** authoritative — the event log + `WardenState` reducer are. A write
//! failure is best-effort; the actor logs and keeps going.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::RigGraph;

/// Read/write mirror of warden custody records.
pub trait GraphWardenRepository: Send + Sync {
    /// Insert or replace a rig's custody record. Called after every transition.
    fn upsert_rig(&self, rig: &RigGraph) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one rig's record. For dashboards / read adapters, not the actor.
    fn get_rig(&self, rig: &str) -> impl Future<Output = Result<Option<RigGraph>, AppError>> + Send;

    /// Drop a rig's record (custody ended).
    fn remove_rig(&self, rig: &str) -> impl Future<Output = Result<(), AppError>> + Send;
}

/// In-memory `GraphWardenRepository` for tests and the composition root's default.
#[derive(Default)]
pub struct InMemoryGraphWardenRepo {
    inner: Mutex<BTreeMap<String, RigGraph>>,
}

impl GraphWardenRepository for InMemoryGraphWardenRepo {
    async fn upsert_rig(&self, rig: &RigGraph) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-graphwarden repo poisoned".into()))?;
        g.insert(rig.rig.clone(), rig.clone());
        Ok(())
    }

    async fn get_rig(&self, rig: &str) -> Result<Option<RigGraph>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-graphwarden repo poisoned".into()))?;
        Ok(g.get(rig).cloned())
    }

    async fn remove_rig(&self, rig: &str) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-graphwarden repo poisoned".into()))?;
        g.remove(rig);
        Ok(())
    }
}
