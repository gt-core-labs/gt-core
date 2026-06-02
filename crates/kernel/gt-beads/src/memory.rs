use std::collections::HashMap;
use std::sync::Mutex;

use gt_events::AppError;

use crate::bead::{Bead, BeadStatus};
use crate::repo::BeadRepository;

/// Implementación in-memory del puerto. Sin Dolt: el claim por CAS se serializa con el
/// `Mutex`, igual que el adaptador real lo serializa con el dispatcher único + CAS al commit.
#[derive(Default)]
pub struct InMemoryBeads {
    beads: Mutex<HashMap<String, Bead>>,
}

impl BeadRepository for InMemoryBeads {
    async fn upsert(&self, bead: &Bead) -> Result<(), AppError> {
        self.beads.lock().unwrap().insert(bead.id.clone(), bead.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Bead>, AppError> {
        Ok(self.beads.lock().unwrap().get(id).cloned())
    }

    async fn list_by_status(&self, status: BeadStatus) -> Result<Vec<Bead>, AppError> {
        Ok(self
            .beads
            .lock()
            .unwrap()
            .values()
            .filter(|b| b.status == status)
            .cloned()
            .collect())
    }

    async fn cas_claim(&self, id: &str, worker: &str) -> Result<bool, AppError> {
        let mut beads = self.beads.lock().unwrap();
        match beads.get_mut(id) {
            Some(b) if b.status == BeadStatus::Pending => {
                b.status = BeadStatus::Dispatched;
                b.assignee = Some(worker.to_string());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn cas_release(&self, id: &str, expected_worker: &str) -> Result<bool, AppError> {
        let mut beads = self.beads.lock().unwrap();
        match beads.get_mut(id) {
            Some(b)
                if b.status == BeadStatus::Dispatched
                    && b.assignee.as_deref() == Some(expected_worker) =>
            {
                b.status = BeadStatus::Pending;
                b.assignee = None;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
