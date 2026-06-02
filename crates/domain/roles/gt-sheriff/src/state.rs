//! State of the Sheriff domain: a map of registered `Watch`es keyed by id. A `Watch` is a
//! `(pattern, threshold)` tuple plus a live counter and a `raised` flag. The reducer and the
//! actor share this one type — the live state and the replay-folded state are the same
//! shape (no `Board`/`State` split; the domain is small).

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::SheriffEvent;

/// A registered watchdog: count occurrences of `pattern` and raise once `count >= threshold`.
/// Cleared explicitly by operator (`SheriffCommand::Clear`); a cleared watch resets its
/// counter so the next observation cycle starts fresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    pub id: String,
    pub pattern: String,
    pub threshold: u32,
    pub count: u32,
    pub raised: bool,
}

/// Aggregate state of the sheriff domain. `Default` is the empty registry — pre-`Registered`
/// boot has no watches and ignores every observation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SheriffState {
    pub watches: BTreeMap<String, Watch>,
}

impl SheriffState {
    /// Pure fold over a `SheriffEvent`. **Total** — unknown ids in `Observed`/`Raised`/
    /// `Cleared` are no-ops, matching the reducer pattern used by `MergeState`. Validation
    /// lives on the write path (`SheriffCommand::validate`); the fold trusts the log.
    pub fn apply(&mut self, ev: &SheriffEvent) -> Result<(), AppError> {
        match ev {
            SheriffEvent::Registered {
                id,
                pattern,
                threshold,
            } => {
                self.watches.insert(
                    id.clone(),
                    Watch {
                        id: id.clone(),
                        pattern: pattern.clone(),
                        threshold: *threshold,
                        count: 0,
                        raised: false,
                    },
                );
            }
            SheriffEvent::Observed { id, count } => {
                if let Some(w) = self.watches.get_mut(id) {
                    w.count = *count;
                }
            }
            SheriffEvent::Raised { id, .. } => {
                if let Some(w) = self.watches.get_mut(id) {
                    w.raised = true;
                }
            }
            SheriffEvent::Cleared { id } => {
                if let Some(w) = self.watches.get_mut(id) {
                    w.raised = false;
                    w.count = 0;
                }
            }
        }
        Ok(())
    }
}
