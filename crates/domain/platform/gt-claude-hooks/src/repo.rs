//! The store seam for the global hook registry.
//!
//! The registry is event-sourced at the GLOBAL scope. The domain defines this port; the
//! composition root implements it over the shared event log (`gt-composition::EventLogHooks`). A
//! sync port mirrors the event log's own sync `replay_domain` / `append` (the same shape
//! `terminal.rs` already calls), so no async runtime leaks into the domain.

use gt_events::AppError;

use crate::events::HookEvent;
use crate::state::{HooksRegistry, HooksState};

/// Read + write the global hook registry. `registry` replays the `hooks.*` stream into the current
/// snapshot; `append` persists a decided [`HookEvent`].
pub trait HooksStore: Send + Sync {
    /// The current global registry snapshot (replayed from the `hooks.*` log).
    fn registry(&self) -> Result<HooksRegistry, AppError>;
    /// Persist `event` into the global `hooks.*` stream.
    fn append(&self, event: HookEvent) -> Result<(), AppError>;
}

/// In-memory store for tests: an append-only event vec replayed on each `registry()`.
#[derive(Default)]
pub struct InMemoryHooks {
    events: std::sync::Mutex<Vec<HookEvent>>,
}

impl HooksStore for InMemoryHooks {
    fn registry(&self) -> Result<HooksRegistry, AppError> {
        let mut state = HooksState::default();
        for e in self.events.lock().expect("hooks mutex").iter() {
            state.apply(e);
        }
        Ok(state.registry)
    }

    fn append(&self, event: HookEvent) -> Result<(), AppError> {
        self.events.lock().expect("hooks mutex").push(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::HookTarget;

    #[test]
    fn in_memory_round_trips_register_and_retire() {
        let store = InMemoryHooks::default();
        store
            .append(HookEvent::Registered {
                id: "g".into(),
                event: "Stop".into(),
                matcher: String::new(),
                command: "c".into(),
                target: HookTarget::default(),
                now_secs: 1,
            })
            .unwrap();
        assert_eq!(store.registry().unwrap().len(), 1);
        store.append(HookEvent::Retired { id: "g".into(), now_secs: 2 }).unwrap();
        assert!(store.registry().unwrap().is_empty());
    }
}
