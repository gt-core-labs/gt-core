//! Persistence port for the convoy board (hq-03aw.8 / epic hq-bdn8).
//!
//! Snapshot of each convoy + per-member state into an external store so SQL panels see
//! convoy progress without going through the actor. Best-effort: the audit log + replay
//! reducer remain authoritative.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::{Convoy, ConvoyState, MemberState};

/// Persistence port for the orchestration board.
pub trait OrchRepository: Send + Sync {
    /// Insert or update the whole convoy (header row + every member row). Convoy state and
    /// member states are both snapshots — the adapter overwrites whatever was there.
    fn upsert_convoy(
        &self,
        convoy: &Convoy,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Patch one member's state. Called when only a single member transitioned (handoff,
    /// completion, failure).
    fn set_member_state(
        &self,
        convoy: &str,
        member: &str,
        state: MemberState,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Patch the convoy header state (Launched / Closed / Failed).
    fn set_convoy_state(
        &self,
        convoy: &str,
        state: ConvoyState,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read one convoy.
    fn get_convoy(
        &self,
        convoy: &str,
    ) -> impl Future<Output = Result<Option<Convoy>, AppError>> + Send;

    /// Snapshot of every convoy ordered by id.
    fn list_convoys(&self) -> impl Future<Output = Result<Vec<Convoy>, AppError>> + Send;
}

impl<R: OrchRepository + ?Sized> OrchRepository for std::sync::Arc<R> {
    fn upsert_convoy(
        &self,
        convoy: &Convoy,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_convoy(convoy).await }
    }
    fn set_member_state(
        &self,
        convoy: &str,
        member: &str,
        state: MemberState,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).set_member_state(convoy, member, state).await }
    }
    fn set_convoy_state(
        &self,
        convoy: &str,
        state: ConvoyState,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).set_convoy_state(convoy, state).await }
    }
    fn get_convoy(
        &self,
        convoy: &str,
    ) -> impl Future<Output = Result<Option<Convoy>, AppError>> + Send {
        async move { (**self).get_convoy(convoy).await }
    }
    fn list_convoys(&self) -> impl Future<Output = Result<Vec<Convoy>, AppError>> + Send {
        async move { (**self).list_convoys().await }
    }
}

/// In-memory adapter used by tests and the no-Dolt fallback.
#[derive(Default)]
pub struct InMemoryOrchRepo {
    convoys: Mutex<std::collections::BTreeMap<String, Convoy>>,
}

impl InMemoryOrchRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OrchRepository for InMemoryOrchRepo {
    async fn upsert_convoy(&self, convoy: &Convoy) -> Result<(), AppError> {
        self.convoys
            .lock()
            .unwrap()
            .insert(convoy.id.clone(), convoy.clone());
        Ok(())
    }

    async fn set_member_state(
        &self,
        convoy: &str,
        member: &str,
        state: MemberState,
    ) -> Result<(), AppError> {
        if let Some(c) = self.convoys.lock().unwrap().get_mut(convoy) {
            if let Some(m) = c.members.iter_mut().find(|m| m.bead == member) {
                m.state = state;
            }
        }
        Ok(())
    }

    async fn set_convoy_state(&self, convoy: &str, state: ConvoyState) -> Result<(), AppError> {
        if let Some(c) = self.convoys.lock().unwrap().get_mut(convoy) {
            c.state = state;
        }
        Ok(())
    }

    async fn get_convoy(&self, convoy: &str) -> Result<Option<Convoy>, AppError> {
        Ok(self.convoys.lock().unwrap().get(convoy).cloned())
    }

    async fn list_convoys(&self) -> Result<Vec<Convoy>, AppError> {
        Ok(self.convoys.lock().unwrap().values().cloned().collect())
    }
}
