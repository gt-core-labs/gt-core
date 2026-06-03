//! Persistence port for wisps + an in-memory adapter.
//!
//! Same shape as `gt_merge::MergeRepository`: methods return `impl Future + Send` so the
//! reaper can `.await` without `async_trait` / `dyn` boxing. The Dolt adapter (`DoltWisp`)
//! implements this against the `wisps` table; tests and host runs use `InMemoryWispRepo`.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use time::OffsetDateTime;

use gt_events::AppError;

use crate::wisp::{Wisp, WispStatus};

/// Port the reaper drives. Reads are snapshots; the two mutations ([`mark_reaped`],
/// [`purge`]) return whether they changed a row, which is what lets a second reaper pass
/// report "nothing to do" instead of re-counting already-compacted wisps.
///
/// [`mark_reaped`]: WispRepository::mark_reaped
/// [`purge`]: WispRepository::purge
pub trait WispRepository: Send + Sync {
    /// Insert or replace a wisp by id. Seeds rows in tests; adapters use it for emission.
    fn upsert(&self, wisp: &Wisp) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one wisp by id.
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<Wisp>, AppError>> + Send;

    /// All currently-open wisps. Stable order is the adapter's job (Dolt: `ORDER BY id`).
    fn list_open(&self) -> impl Future<Output = Result<Vec<Wisp>, AppError>> + Send;

    /// Closed wisps whose `closed_at` is strictly before `cutoff` — the purge candidates.
    fn list_closed_before(
        &self,
        cutoff: OffsetDateTime,
    ) -> impl Future<Output = Result<Vec<Wisp>, AppError>> + Send;

    /// Close an open wisp. Returns `true` only on a real `Open → Closed` transition; an
    /// already-closed or missing id returns `false` (idempotent — the reaper's "no duplica").
    fn mark_reaped(
        &self,
        id: &str,
        closed_at: OffsetDateTime,
    ) -> impl Future<Output = Result<bool, AppError>> + Send;

    /// Delete a wisp row. Returns `true` if a row was removed, `false` if already gone.
    fn purge(&self, id: &str) -> impl Future<Output = Result<bool, AppError>> + Send;
}

impl<R: WispRepository + ?Sized> WispRepository for std::sync::Arc<R> {
    fn upsert(&self, wisp: &Wisp) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert(wisp).await }
    }
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<Wisp>, AppError>> + Send {
        async move { (**self).get(id).await }
    }
    fn list_open(&self) -> impl Future<Output = Result<Vec<Wisp>, AppError>> + Send {
        async move { (**self).list_open().await }
    }
    fn list_closed_before(
        &self,
        cutoff: OffsetDateTime,
    ) -> impl Future<Output = Result<Vec<Wisp>, AppError>> + Send {
        async move { (**self).list_closed_before(cutoff).await }
    }
    fn mark_reaped(
        &self,
        id: &str,
        closed_at: OffsetDateTime,
    ) -> impl Future<Output = Result<bool, AppError>> + Send {
        async move { (**self).mark_reaped(id, closed_at).await }
    }
    fn purge(&self, id: &str) -> impl Future<Output = Result<bool, AppError>> + Send {
        async move { (**self).purge(id).await }
    }
}

/// In-memory adapter for domain tests and host runs without a Dolt server. Same contract as
/// `DoltWisp`: `list_open` / `list_closed_before` return id-ordered snapshots.
#[derive(Default)]
pub struct InMemoryWispRepo {
    wisps: Mutex<BTreeMap<String, Wisp>>,
}

impl InMemoryWispRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WispRepository for InMemoryWispRepo {
    async fn upsert(&self, wisp: &Wisp) -> Result<(), AppError> {
        self.wisps
            .lock()
            .unwrap()
            .insert(wisp.id.clone(), wisp.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Wisp>, AppError> {
        Ok(self.wisps.lock().unwrap().get(id).cloned())
    }

    async fn list_open(&self) -> Result<Vec<Wisp>, AppError> {
        Ok(self
            .wisps
            .lock()
            .unwrap()
            .values()
            .filter(|w| w.status == WispStatus::Open)
            .cloned()
            .collect())
    }

    async fn list_closed_before(&self, cutoff: OffsetDateTime) -> Result<Vec<Wisp>, AppError> {
        Ok(self
            .wisps
            .lock()
            .unwrap()
            .values()
            .filter(|w| {
                w.status == WispStatus::Closed && w.closed_at.is_some_and(|c| c < cutoff)
            })
            .cloned()
            .collect())
    }

    async fn mark_reaped(&self, id: &str, closed_at: OffsetDateTime) -> Result<bool, AppError> {
        let mut guard = self.wisps.lock().unwrap();
        match guard.get_mut(id) {
            Some(w) if w.status == WispStatus::Open => {
                w.status = WispStatus::Closed;
                w.closed_at = Some(closed_at);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn purge(&self, id: &str) -> Result<bool, AppError> {
        Ok(self.wisps.lock().unwrap().remove(id).is_some())
    }
}
