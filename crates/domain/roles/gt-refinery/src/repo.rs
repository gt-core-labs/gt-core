//! Persistence port for Refinery. Same `impl Future + Send` shape as the other domain
//! repos. Best-effort mirror — the event log + `RefineryState` reducer are authoritative.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::RefineryEntry;

pub trait RefineryRepository: Send + Sync {
    fn upsert_entry(
        &self,
        entry: &RefineryEntry,
    ) -> impl Future<Output = Result<(), AppError>> + Send;
    fn get_entry(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<RefineryEntry>, AppError>> + Send;
}

#[derive(Default)]
pub struct InMemoryRefineryRepo {
    inner: Mutex<BTreeMap<String, RefineryEntry>>,
}

impl RefineryRepository for InMemoryRefineryRepo {
    async fn upsert_entry(&self, entry: &RefineryEntry) -> Result<(), AppError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-refinery repo poisoned".into()))?;
        g.insert(entry.bead.clone(), entry.clone());
        Ok(())
    }

    async fn get_entry(&self, bead: &str) -> Result<Option<RefineryEntry>, AppError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| AppError::Other("gt-refinery repo poisoned".into()))?;
        Ok(g.get(bead).cloned())
    }
}
