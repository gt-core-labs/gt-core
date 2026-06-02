//! Persistence port for the merge board (hq-03aw.6 / epic hq-bdn8).
//!
//! Today the actor owns `MergeBoard` in RAM; on restart the state vanishes and dashboards
//! can only reach the live values by replaying the audit log. The port lets the composition
//! root snapshot every state-machine transition into an external store (Dolt in production,
//! in-memory in tests) so the SQL panels see live merge slots without touching the actor.
//!
//! The repo is **not** authoritative — the event log + [`crate::MergeState`] reducer are. A
//! write failure is best-effort: the actor logs and keeps emitting (replay still rebuilds
//! the truth). Mirrors the bead-repo pattern from `docs/04-persistence.md`.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::MergeSlot;

/// Persistence port for the merge board. Methods return `impl Future + Send` so the actor
/// can `.await` them without `async_trait` / `dyn` boxing — same shape as `BeadRepository`.
pub trait MergeRepository: Send + Sync {
    /// Insert or replace the slot identified by `slot.bead`. Called after every successful
    /// transition; the row mirrors the live `MergeSlot`.
    fn upsert_slot(&self, slot: &MergeSlot) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one slot. Used by dashboards / read-side adapters, not by the actor.
    fn get_slot(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<MergeSlot>, AppError>> + Send;

    /// Snapshot of every slot in the store. Stable order is the adapter's responsibility
    /// (Dolt: `ORDER BY bead`; in-memory: sorted clone).
    fn list_slots(&self) -> impl Future<Output = Result<Vec<MergeSlot>, AppError>> + Send;
}

impl<R: MergeRepository + ?Sized> MergeRepository for std::sync::Arc<R> {
    fn upsert_slot(&self, slot: &MergeSlot) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_slot(slot).await }
    }
    fn get_slot(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<MergeSlot>, AppError>> + Send {
        async move { (**self).get_slot(bead).await }
    }
    fn list_slots(&self) -> impl Future<Output = Result<Vec<MergeSlot>, AppError>> + Send {
        async move { (**self).list_slots().await }
    }
}

/// In-memory adapter used by domain tests and by the bin when `GT_DOLT_URL` is unset. Same
/// contract as the Dolt impl — slots ordered by bead id on `list_slots`.
#[derive(Default)]
pub struct InMemoryMergeRepo {
    slots: Mutex<std::collections::BTreeMap<String, MergeSlot>>,
}

impl InMemoryMergeRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MergeRepository for InMemoryMergeRepo {
    async fn upsert_slot(&self, slot: &MergeSlot) -> Result<(), AppError> {
        self.slots
            .lock()
            .unwrap()
            .insert(slot.bead.clone(), slot.clone());
        Ok(())
    }

    async fn get_slot(&self, bead: &str) -> Result<Option<MergeSlot>, AppError> {
        Ok(self.slots.lock().unwrap().get(bead).cloned())
    }

    async fn list_slots(&self) -> Result<Vec<MergeSlot>, AppError> {
        Ok(self.slots.lock().unwrap().values().cloned().collect())
    }
}
