//! State of the Graph Warden domain: a map of rigs under graph custody keyed by rig id.
//! Each entry tracks the repo checkout, the last indexed commit, whether the graph is
//! stale, and how many file-changes are pending a refresh. Live and replay-folded state
//! share one type (small domain, same as `WitnessState`).

use std::collections::BTreeMap;

use gt_events::AppError;

use crate::events::WardenEvent;

/// A rig whose codebase graph the warden keeps fresh. `last_indexed_commit` is `None`
/// until the first refresh; `stale` is set on registration (an initial build is owed) and
/// on every `MarkedStale`, and cleared by `Refreshed`. `pending_changes` accumulates the
/// changed-file counts reported between refreshes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RigGraph {
    pub rig: String,
    pub repo_dir: String,
    pub last_indexed_commit: Option<String>,
    pub stale: bool,
    pub pending_changes: u64,
}

/// Aggregate state of the warden domain. `Default` is the empty registry — no rig under
/// custody, every stale/refresh event for an unknown rig is a no-op.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WardenState {
    pub rigs: BTreeMap<String, RigGraph>,
}

impl WardenState {
    /// Pure fold over a `WardenEvent`. **Total** — stale/refresh/unregister for an unknown
    /// rig are no-ops (validation lives on the write path, `WardenCommand::validate`; the
    /// fold trusts the log). Replay-deterministic: no clock or I/O.
    pub fn apply(&mut self, ev: &WardenEvent) -> Result<(), AppError> {
        match ev {
            WardenEvent::RigRegistered { rig, repo_dir, .. } => {
                self.rigs.insert(
                    rig.clone(),
                    RigGraph {
                        rig: rig.clone(),
                        repo_dir: repo_dir.clone(),
                        last_indexed_commit: None,
                        stale: true,
                        pending_changes: 0,
                    },
                );
            }
            WardenEvent::MarkedStale { rig, changed, .. } => {
                if let Some(g) = self.rigs.get_mut(rig) {
                    g.stale = true;
                    g.pending_changes = g.pending_changes.saturating_add(*changed);
                }
            }
            WardenEvent::Refreshed { rig, commit, .. } => {
                if let Some(g) = self.rigs.get_mut(rig) {
                    g.stale = false;
                    g.pending_changes = 0;
                    g.last_indexed_commit = Some(commit.clone());
                }
            }
            WardenEvent::Unregistered { rig, .. } => {
                self.rigs.remove(rig);
            }
        }
        Ok(())
    }
}
