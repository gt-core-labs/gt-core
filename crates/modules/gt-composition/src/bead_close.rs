//! `BeadClosePlugin` — auto-close beads in Dolt when their merge lands on main.
//!
//! Root cause of the "zombie surface" bug: when `merge.merged.v1` fires (the
//! branch fast-forwarded onto `main`), nothing transitioned the bead's Dolt
//! `status` from `'working'` to `'closed'`. The `working_surfaces()` query
//! (`SELECT ... WHERE status = 'working'`) kept returning these stale beads,
//! their crate surfaces stayed in `occupied_surfaces`, and `dispatch.probe`
//! blocked new beads from dispatching on overlapping files.
//!
//! This plugin closes the loop: on `merge.merged.v1` it calls
//! [`DoltIssues::close`] to flip the bead to `closed` + stamp `closed_at`. The
//! surfaces are then freed and the scheduler can dispatch new work onto them.
//!
//! Registered on the `pol_registry` in `gt-orch-server` (an edge effect, like
//! [`GitMergePlugin`](crate::git_merge::GitMergePlugin) and
//! [`PatrolBridgePlugin`](crate::patrol_bridge::PatrolBridgePlugin)), gated on
//! `GT_DOLT_URL`.

use std::sync::Arc;

use async_trait::async_trait;

use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_merge::MergeEvent;
use gt_plugin::Plugin;

/// Port: close a bead by id with attribution. Production adapts
/// [`DoltIssues::close`](gt_store_dolt::DoltIssues); tests use a fake.
#[async_trait]
pub trait BeadCloser: Send + Sync {
    /// Stamp `sha` as the bead's `delivered_sha` and transition it from `working`/`open` →
    /// `closed`. Returns `Ok(())` on success or if the bead is already closed (idempotent on
    /// replay). The sha stamp is NOT optional (gtcore-ac9782): `dep_satisfied` requires a
    /// non-epic dependency to be DELIVERED (sha on main), so a merged-but-unstamped bead stalls
    /// every dependent — observed live: the authapp wave froze after its first two merges left
    /// `delivered_sha` NULL.
    async fn close_bead(&self, bead: &str, sha: &str, attribution: &str) -> Result<(), String>;
}

#[async_trait]
impl BeadCloser for gt_store_dolt::DoltIssues {
    async fn close_bead(&self, bead: &str, sha: &str, attribution: &str) -> Result<(), String> {
        // Stamp FIRST: the merge event is the delivery proof, and the stamp must land even when
        // the close below is a no-op because the agent (or an operator) already closed the bead.
        if let Err(e) = self.set_delivered_sha(bead, sha).await {
            // Best-effort: a missing bead falls through to close()'s own handling.
            eprintln!("[bead-close] {bead}: delivered_sha stamp failed ({e}) — closing anyway");
        }
        match self.close(bead, attribution).await {
            Ok(()) => Ok(()),
            // Already closed (invalid transition) or not found — both are fine
            // on replay or if an operator closed it manually first.
            Err(gt_store_dolt::AppError::Validation(_)) => Ok(()),
            Err(gt_store_dolt::AppError::NotFound(_)) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Observes `merge.merged.v1` and closes the bead in the tracker store so its
/// surfaces are freed for new dispatch.
pub struct BeadClosePlugin {
    closer: Arc<dyn BeadCloser>,
}

impl BeadClosePlugin {
    pub fn new(closer: Arc<dyn BeadCloser>) -> Self {
        Self { closer }
    }
}

#[async_trait]
impl Plugin for BeadClosePlugin {
    fn name(&self) -> &'static str {
        "bead-close"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind != "merge.merged.v1" {
            return Ok(());
        }
        let MergeEvent::Merged { bead, sha } = record.decode::<MergeEvent>()? else {
            return Ok(());
        };
        let attribution = format!("merge:{sha}");
        match self.closer.close_bead(&bead, &sha, &attribution).await {
            Ok(()) => {
                eprintln!("[bead-close] {bead}: closed after merge (sha {sha})");
            }
            Err(e) => {
                // Best-effort: the merge event is durable, so an operator or
                // reconciler can close the bead later. Never poison the hub.
                eprintln!("[bead-close] {bead}: close FAILED after merge — {e}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};

    use gt_events::Envelope;

    struct FakeCloser {
        called: AtomicBool,
        /// The sha handed to `close_bead` (gtcore-ac9782): the plugin must forward the merge
        /// sha so the store stamps `delivered_sha`, not just the attribution note.
        sha: std::sync::Mutex<Option<String>>,
    }

    impl FakeCloser {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                called: AtomicBool::new(false),
                sha: std::sync::Mutex::new(None),
            })
        }
        fn was_called(&self) -> bool {
            self.called.load(Ordering::SeqCst)
        }
        fn stamped_sha(&self) -> Option<String> {
            self.sha.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl BeadCloser for FakeCloser {
        async fn close_bead(&self, _bead: &str, sha: &str, _attribution: &str) -> Result<(), String> {
            self.called.store(true, Ordering::SeqCst);
            *self.sha.lock().unwrap() = Some(sha.to_string());
            Ok(())
        }
    }

    fn merged_record(bead: &str, sha: &str) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(MergeEvent::Merged {
            bead: bead.into(),
            sha: sha.into(),
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn closes_bead_on_merge_merged() {
        let closer = FakeCloser::new();
        let plugin = BeadClosePlugin::new(closer.clone() as Arc<dyn BeadCloser>);
        plugin.on_event(&merged_record("gtcore-abc", "deadbeef")).await.unwrap();
        assert!(closer.was_called(), "close_bead should have been called");
        assert_eq!(
            closer.stamped_sha().as_deref(),
            Some("deadbeef"),
            "the merge sha rides into the store so delivered_sha is stamped (gtcore-ac9782)"
        );
    }

    #[tokio::test]
    async fn ignores_other_events() {
        let closer = FakeCloser::new();
        let plugin = BeadClosePlugin::new(closer.clone() as Arc<dyn BeadCloser>);
        let record = EventRecord::from_envelope(&Envelope::root(MergeEvent::Started {
            bead: "gtcore-abc".into(),
        }))
        .unwrap();
        plugin.on_event(&record).await.unwrap();
        assert!(!closer.was_called(), "close_bead should NOT be called for non-merged events");
    }
}
