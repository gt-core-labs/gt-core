//! Persistence port for the lease tracker (hq-03aw.7 / epic hq-bdn8).
//!
//! Mirrors each register/heartbeat/close/expire into an external store so dashboards see
//! live leases without going through the actor. Same best-effort contract as the merge
//! repo: the audit log + [`crate::PatrolState`] reducer remain authoritative.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::Lease;

/// Persistence port for the patrol's live-lease set.
pub trait PatrolRepository: Send + Sync {
    /// Insert or replace a live lease.
    fn upsert_lease(&self, lease: &Lease) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Remove a lease (Close / LeaseExpired). Idempotent — missing rows are fine.
    fn delete_lease(&self, bead: &str) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Refresh `last_seen_secs` on every lease owned by `worker`. Returns rows touched.
    fn heartbeat_worker(
        &self,
        worker: &str,
        now_secs: u64,
    ) -> impl Future<Output = Result<usize, AppError>> + Send;

    /// Read one lease.
    fn get_lease(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<Lease>, AppError>> + Send;

    /// Snapshot of every live lease, sorted by bead id.
    fn list_leases(&self) -> impl Future<Output = Result<Vec<Lease>, AppError>> + Send;
}

impl<R: PatrolRepository + ?Sized> PatrolRepository for std::sync::Arc<R> {
    fn upsert_lease(&self, lease: &Lease) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_lease(lease).await }
    }
    fn delete_lease(&self, bead: &str) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).delete_lease(bead).await }
    }
    fn heartbeat_worker(
        &self,
        worker: &str,
        now_secs: u64,
    ) -> impl Future<Output = Result<usize, AppError>> + Send {
        async move { (**self).heartbeat_worker(worker, now_secs).await }
    }
    fn get_lease(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<Lease>, AppError>> + Send {
        async move { (**self).get_lease(bead).await }
    }
    fn list_leases(&self) -> impl Future<Output = Result<Vec<Lease>, AppError>> + Send {
        async move { (**self).list_leases().await }
    }
}

/// In-memory adapter used by tests and the no-Dolt fallback.
#[derive(Default)]
pub struct InMemoryPatrolRepo {
    leases: Mutex<std::collections::BTreeMap<String, Lease>>,
}

impl InMemoryPatrolRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PatrolRepository for InMemoryPatrolRepo {
    async fn upsert_lease(&self, lease: &Lease) -> Result<(), AppError> {
        self.leases
            .lock()
            .unwrap()
            .insert(lease.bead.clone(), lease.clone());
        Ok(())
    }

    async fn delete_lease(&self, bead: &str) -> Result<(), AppError> {
        self.leases.lock().unwrap().remove(bead);
        Ok(())
    }

    async fn heartbeat_worker(&self, worker: &str, now_secs: u64) -> Result<usize, AppError> {
        let mut g = self.leases.lock().unwrap();
        let mut n = 0;
        for lease in g.values_mut() {
            if lease.worker == worker {
                lease.last_seen_secs = now_secs;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn get_lease(&self, bead: &str) -> Result<Option<Lease>, AppError> {
        Ok(self.leases.lock().unwrap().get(bead).cloned())
    }

    async fn list_leases(&self) -> Result<Vec<Lease>, AppError> {
        Ok(self.leases.lock().unwrap().values().cloned().collect())
    }
}
