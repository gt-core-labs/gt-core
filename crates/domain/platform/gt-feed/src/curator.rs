//! `Curator` — the pure reducer. Folds `EventRecord`s into `FeedState`.
//!
//! Sync, no clock, no random, no I/O. Replay re-runs the same records against `apply` and
//! reconstructs a byte-identical `FeedState` (gate of Paso 3 carried into the feed domain).

use gt_eventlog::EventRecord;

use crate::state::FeedState;

pub struct Curator;

impl Curator {
    /// Fold one record into the running state. Append-only; nothing is mutated retroactively.
    pub fn apply(state: &mut FeedState, record: &EventRecord) {
        state.ingest(record);
    }

    /// Convenience: fold a whole slice from a fresh `FeedState`.
    pub fn fold(records: &[EventRecord]) -> FeedState {
        let mut state = FeedState::new();
        for record in records {
            Self::apply(&mut state, record);
        }
        state
    }
}
