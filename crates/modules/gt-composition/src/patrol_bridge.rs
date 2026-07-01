//! `PatrolBridgePlugin` — closes the lease loop end-to-end (gtcore-a33952 — C2
//! of the "Control de despacho" epic).
//!
//! gt-patrol's actor and the `patrol.lease-expired.v1 → re-enqueue` reaction
//! ([`SchedulerPlugin`](crate::SchedulerPlugin)) have existed since Paso 6, but
//! NOTHING registered leases or fed heartbeats — the expiry never fired, and a
//! dead polecat left its bead `working` forever (the exact C2 failure mode:
//! crashed agent = work locked until an operator notices). This observer wires
//! the three missing arcs from events already on the hub:
//!
//! - `agent.spawned.v1` (polecat, `crew = bead`) → [`PatrolHandle::register`]
//!   keyed by bead with the SESSION as the lease worker — the same key
//!   `agent.heartbeat.v1` reports, so the heartbeat keeps the lease alive.
//! - `agent.session-end.v1` / `agent.killed.v1` / `issues.closed.v1` →
//!   [`PatrolHandle::close`] — a normal finish never expires.
//! - `patrol.lease-expired.v1` → release the TRACKER claim (`working → open`,
//!   owner cleared) through the [`ClaimRelease`] port, so the bead re-enters
//!   the frontier; the scheduler re-enqueue stays [`SchedulerPlugin`]'s job.
//!
//! The Dolt write is an edge effect, so the plugin registers on the bin's hub
//! relay next to [`GitMergePlugin`](crate::git_merge::GitMergePlugin) — never
//! in the pure-state `daemon_root`. Operator claims are untouched by design:
//! only beads an agent SPAWN registered carry a lease; a human claim has no
//! lease and therefore can never expire (C2: "el claim del operador no expira").

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use gt_agent::AgentEvent;
use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_patrol::actor::PatrolHandle;
use gt_patrol::PatrolEvent;
use gt_plugin::Plugin;

/// Port: flip a `working` bead back to `open` when its lease expires. Faked in
/// tests; production adapts [`DoltIssues::release_claim`](gt_store_dolt::DoltIssues::release_claim).
#[async_trait]
pub trait ClaimRelease: Send + Sync {
    /// `Ok(true)` when the CAS released; `Ok(false)` on a stale expiry (bead
    /// already closed / re-claimed) — a no-op, never an error.
    async fn release_claim(&self, bead: &str) -> Result<bool, String>;
}

#[async_trait]
impl ClaimRelease for gt_store_dolt::DoltIssues {
    async fn release_claim(&self, bead: &str) -> Result<bool, String> {
        gt_store_dolt::DoltIssues::release_claim(self, bead)
            .await
            .map_err(|e| e.to_string())
    }
}

/// The bridge observer. Session→bead links live in memory: leases themselves
/// are in-memory actor state with the same orchd-process lifetime, so a restart
/// drops both halves coherently (the session reconciler + heartbeat sweep cover
/// the restart window).
pub struct PatrolBridgePlugin {
    patrol: PatrolHandle,
    release: Arc<dyn ClaimRelease>,
    sessions: Mutex<HashMap<String, String>>,
    now_secs: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl PatrolBridgePlugin {
    pub fn new(patrol: PatrolHandle, release: Arc<dyn ClaimRelease>) -> Self {
        Self {
            patrol,
            release,
            sessions: Mutex::new(HashMap::new()),
            now_secs: Box::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            }),
        }
    }

    /// Pin the clock for tests.
    #[cfg(test)]
    fn with_clock(mut self, now: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.now_secs = Box::new(now);
        self
    }

    fn forget_bead(&self, bead: &str) {
        self.sessions
            .lock()
            .expect("sessions mutex")
            .retain(|_, b| b != bead);
    }
}

#[async_trait]
impl Plugin for PatrolBridgePlugin {
    fn name(&self) -> &'static str {
        "patrol-bridge"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "agent.spawned.v1" => {
                let AgentEvent::Spawned { session, crew: Some(bead), maintains_heartbeat, .. } =
                    record.decode::<AgentEvent>()?
                else {
                    return Ok(());
                };
                // Only heartbeat-bearing sessions (polecats) lease: an interactive
                // mayor/dog never promises beats, so a lease would always expire.
                if !maintains_heartbeat {
                    return Ok(());
                }
                self.sessions
                    .lock()
                    .expect("sessions mutex")
                    .insert(session.clone(), bead.clone());
                self.patrol.register(bead, session, 1, (self.now_secs)()).await;
                Ok(())
            }
            "agent.heartbeat.v1" => {
                if let AgentEvent::Heartbeat { session, timestamp_secs } =
                    record.decode::<AgentEvent>()?
                {
                    let now = timestamp_secs.unwrap_or_else(|| (self.now_secs)());
                    self.patrol.heartbeat(session, now).await;
                }
                Ok(())
            }
            "agent.session-end.v1" | "agent.killed.v1" => {
                let session = match record.decode::<AgentEvent>()? {
                    AgentEvent::SessionEnd { session, .. } => session,
                    AgentEvent::Killed { session, .. } => session,
                    _ => return Ok(()),
                };
                let bead = self.sessions.lock().expect("sessions mutex").remove(&session);
                if let Some(bead) = bead {
                    self.patrol.close(bead.clone()).await;
                    // Release the Dolt claim (working → open) so the bead can
                    // be re-dispatched or closed by BeadClosePlugin on merge.
                    // Without this, a killed/done session leaves the bead
                    // `working` forever — the "zombie claim" bug. The CAS is
                    // idempotent: if the bead is already closed (merged while
                    // the session was alive) the release is a stale no-op.
                    match self.release.release_claim(&bead).await {
                        Ok(true) => {
                            eprintln!("[patrol-bridge] session ended — released {bead} back to open")
                        }
                        Ok(false) => {} // already closed/re-claimed, fine
                        Err(e) => eprintln!("[patrol-bridge] release of {bead} on session end FAILED: {e}"),
                    }
                }
                Ok(())
            }
            "issues.closed.v1" => {
                if let Some(id) = record.payload.get("id").and_then(|v| v.as_str()) {
                    self.patrol.close(id.to_string()).await;
                    self.forget_bead(id);
                }
                Ok(())
            }
            "patrol.lease-expired.v1" => {
                if let PatrolEvent::LeaseExpired { bead, .. } = record.decode::<PatrolEvent>()? {
                    self.forget_bead(&bead);
                    match self.release.release_claim(&bead).await {
                        Ok(true) => {
                            eprintln!("[patrol-bridge] lease expired — released {bead} back to open")
                        }
                        Ok(false) => eprintln!(
                            "[patrol-bridge] lease expired for {bead} but claim already moved (stale, ok)"
                        ),
                        // Best-effort edge write: the expiry event is durable, so an
                        // operator (or the next sweep) can reconcile; never poison the hub.
                        Err(e) => eprintln!("[patrol-bridge] release of {bead} FAILED: {e}"),
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::mpsc;

    use gt_events::Envelope;
    use gt_patrol::{actor::spawn, InMemoryPatrolRepo};

    /// Stub that records whether `release_claim` was called.
    struct FakeRelease {
        called: AtomicBool,
        result: bool,
    }

    impl FakeRelease {
        fn new(result: bool) -> Arc<Self> {
            Arc::new(Self {
                called: AtomicBool::new(false),
                result,
            })
        }
        fn was_called(&self) -> bool {
            self.called.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ClaimRelease for FakeRelease {
        async fn release_claim(&self, _bead: &str) -> Result<bool, String> {
            self.called.store(true, Ordering::SeqCst);
            Ok(self.result)
        }
    }

    fn make_bridge(release: Arc<dyn ClaimRelease>) -> (PatrolBridgePlugin, mpsc::Receiver<Envelope<gt_patrol::PatrolEvent>>) {
        let (tx, rx) = mpsc::channel(64);
        let repo = InMemoryPatrolRepo::new();
        let handle = spawn(repo, tx);
        let plugin = PatrolBridgePlugin::new(handle, release).with_clock(|| 1000);
        (plugin, rx)
    }

    fn record(kind: &str, payload: serde_json::Value) -> EventRecord {
        EventRecord {
            event_id: "test".into(),
            correlation_id: "corr".into(),
            causation_id: None,
            ts: "2026-06-13T00:00:00Z".into(),
            kind: kind.into(),
            payload,
        }
    }

    #[tokio::test]
    async fn spawned_registers_lease_and_session_end_releases_claim() {
        let release = FakeRelease::new(true);
        let release_spy: Arc<FakeRelease> = Arc::clone(&release);
        let (plugin, _rx) = make_bridge(release);

        // Spawn a heartbeat-bearing polecat with crew = bead
        let ev = record(
            "agent.spawned.v1",
            serde_json::json!({"Spawned": {
                "session": "gt-hq-abc-1",
                "rig": "gtcore",
                "crew": "gtcore-bead1",
                "maintains_heartbeat": true,
            }}),
        );
        plugin.on_event(&ev).await.unwrap();

        // Session→bead link recorded
        assert_eq!(
            plugin.sessions.lock().unwrap().get("gt-hq-abc-1"),
            Some(&"gtcore-bead1".to_string()),
        );

        // Session end removes the link, closes the lease, AND releases the Dolt claim
        let ev = record(
            "agent.session-end.v1",
            serde_json::json!({"SessionEnd": { "session": "gt-hq-abc-1" }}),
        );
        plugin.on_event(&ev).await.unwrap();
        assert!(plugin.sessions.lock().unwrap().is_empty());
        assert!(
            release_spy.was_called(),
            "session end must release the Dolt claim (working → open)"
        );
    }

    #[tokio::test]
    async fn no_heartbeat_flag_skips_registration() {
        let release = FakeRelease::new(true);
        let (plugin, _rx) = make_bridge(release);

        let ev = record(
            "agent.spawned.v1",
            serde_json::json!({"Spawned": {
                "session": "gt-mayor-1",
                "rig": "gtcore",
                "crew": "gtcore-bead2",
                "maintains_heartbeat": false,
            }}),
        );
        plugin.on_event(&ev).await.unwrap();
        assert!(plugin.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn issues_closed_closes_lease_and_forgets() {
        let release = FakeRelease::new(true);
        let (plugin, _rx) = make_bridge(release);

        // Register first
        let ev = record(
            "agent.spawned.v1",
            serde_json::json!({"Spawned": {
                "session": "gt-hq-x-1",
                "rig": "gtcore",
                "crew": "gtcore-bead3",
                "maintains_heartbeat": true,
            }}),
        );
        plugin.on_event(&ev).await.unwrap();
        assert!(plugin.sessions.lock().unwrap().contains_key("gt-hq-x-1"));

        // Close the issue externally
        let ev = record(
            "issues.closed.v1",
            serde_json::json!({ "id": "gtcore-bead3" }),
        );
        plugin.on_event(&ev).await.unwrap();
        assert!(plugin.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn lease_expired_calls_release() {
        let release = FakeRelease::new(true);
        let release_spy: Arc<FakeRelease> = Arc::clone(&release);
        let (tx, rx) = mpsc::channel(64);
        let repo = InMemoryPatrolRepo::new();
        let handle = spawn(repo, tx);
        let plugin = PatrolBridgePlugin {
            patrol: handle,
            release: release as Arc<dyn ClaimRelease>,
            sessions: Mutex::new(HashMap::new()),
            now_secs: Box::new(|| 1000),
        };

        let ev = record(
            "patrol.lease-expired.v1",
            serde_json::json!({"LeaseExpired": { "bead": "gtcore-bead4", "worker": "gt-hq-y-1", "priority": 1 }}),
        );
        plugin.on_event(&ev).await.unwrap();

        assert!(release_spy.was_called(), "release_claim should have been called");
        drop(rx); // keep receiver alive until here
    }
}
