//! Session reconciler (`hq-orchd-deploy.23`): close orphaned sessions so the Sessions view
//! reflects reality.
//!
//! gt-agent sessions are event-sourced — the backend's `/api/v1/agent` Sessions view (and the
//! gt-web Sessions tab) is a fold over the shared workspace event log: `agent.spawned.v1` opens a
//! session, `agent.session-end.v1` / `agent.killed.v1` close it. A polecat that was spawned but
//! whose daemon stopped (or crashed) before emitting a close event leaves an `agent.spawned.v1`
//! with no terminator, so the fold shows it **"spawned" forever** even though its process is long
//! gone. Observed live: `gt-hq-orchd-smoke-11` stuck at `spawned` after a prior daemon run.
//!
//! This is the reconciler for exactly that, modeled on the witness ([`crate::witness_sweep`]):
//! each tick [`SessionReconciler::sweep`] replays the workspace `agent.*` log into a
//! [`SessionRegistry`], and for every still-active session whose tmux session is gone AND whose
//! heartbeat is stale (the polecat is provably not running) it emits `agent.killed.v1` onto the
//! hub. The daemon's event-log sink persists it, so the next backend fold shows the session
//! `killed` instead of a ghost `spawned`. Idempotent: once killed the session is terminal in the
//! replay, so it is not active on the next sweep.
//!
//! It only ever CLOSES a session that is demonstrably dead (no tmux session + stale heartbeat) —
//! a live or recently-active polecat (tmux present or heartbeat fresh) is left untouched, so a
//! healthy session is never prematurely reaped.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionState};
use gt_eventlog::EventRecord;
use gt_events::Envelope;
use gt_polecat::tmux::Tmux;

use crate::mcp::eventlog::EventLog;

/// Decide whether an active session is an orphan to reap: it is dead iff its tmux session is gone
/// AND its heartbeat is stale. A single live signal (session present OR fresh heartbeat) keeps it.
/// Pure, so the reap policy is unit-tested without tmux/fs.
fn is_orphan(tmux_alive: bool, heartbeat_stale: bool) -> bool {
    !tmux_alive && heartbeat_stale
}

/// The session reconciler. Replays the workspace `agent.*` log to find still-open sessions, and
/// emits `agent.killed.v1` for the ones whose process is provably gone.
pub struct SessionReconciler {
    /// Durable event-log root (`GT_EVENTLOG_ROOT`) — the same log the backend folds for its
    /// Sessions view.
    event_root: PathBuf,
    /// Workspace slug whose `agent.*` log is reconciled.
    workspace: String,
    /// Heartbeat directory (`GT_HEARTBEAT_DIR`); a session's file is `<dir>/<session>.heartbeat`.
    heartbeat_dir: PathBuf,
    /// A heartbeat older than this counts the session as dead (with a missing tmux session).
    stale_after: Duration,
    /// tmux probe — `has_session(id)` ⇒ the polecat is still up.
    tmux: Arc<dyn Tmux>,
    /// Hub sender — `agent.killed.v1` is published here and persisted by the event-log sink.
    hub: broadcast::Sender<EventRecord>,
}

impl SessionReconciler {
    /// Wire the reconciler with every source it observes.
    pub fn new(
        event_root: PathBuf,
        workspace: String,
        heartbeat_dir: PathBuf,
        stale_after: Duration,
        tmux: Arc<dyn Tmux>,
        hub: broadcast::Sender<EventRecord>,
    ) -> Self {
        Self {
            event_root,
            workspace,
            heartbeat_dir,
            stale_after,
            tmux,
            hub,
        }
    }

    /// Reconcile once: close every orphaned session. Returns the number of sessions reaped this
    /// sweep. Best-effort: a log-replay failure logs and yields 0 rather than aborting the daemon.
    pub async fn sweep(&self) -> usize {
        let log = EventLog::new(Some(self.event_root.clone()));
        let registry = match log.replay_domain::<gt_agent::SessionRegistry, AgentEvent, _>(
            Some(&self.workspace),
            "agent.",
            gt_agent::SessionRegistry::default(),
            gt_agent::SessionRegistry::apply,
        ) {
            Ok(reg) => reg,
            Err(e) => {
                eprintln!("[session-reconcile] replay agent log failed: {e}");
                return 0;
            }
        };

        let mut reaped = 0;
        for session in registry.active() {
            // `active()` already excludes Done/Killed; defensively skip anything terminal.
            if matches!(session.state, SessionState::Done | SessionState::Killed) {
                continue;
            }
            let hb = self.heartbeat_dir.join(format!("{}.heartbeat", session.id));
            let alive = self.tmux.has_session(&session.id);
            let stale = gt_polecat::lifecycle::heartbeat_is_stale(&hb, self.stale_after);
            if !is_orphan(alive, stale) {
                continue;
            }
            let event = AgentEvent::Killed {
                session: session.id.clone(),
                reason: "orphaned: no tmux session, heartbeat stale".to_string(),
            };
            match EventRecord::from_envelope(&Envelope::root(event)) {
                Ok(record) => {
                    if self.hub.send(record).is_ok() {
                        eprintln!(
                            "[session-reconcile] {} ({:?}) orphaned — emitted agent.killed",
                            session.id, session.state
                        );
                        reaped += 1;
                    }
                }
                Err(e) => eprintln!(
                    "[session-reconcile] {} encode killed failed: {e}",
                    session.id
                ),
            }
        }
        reaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_only_when_both_signals_dead() {
        // Reap only when the tmux session is gone AND the heartbeat is stale.
        assert!(
            is_orphan(false, true),
            "no session + stale heartbeat ⇒ orphan"
        );
        // Any single live signal keeps the session.
        assert!(!is_orphan(true, true), "tmux session present ⇒ keep");
        assert!(!is_orphan(false, false), "fresh heartbeat ⇒ keep");
        assert!(!is_orphan(true, false), "alive on both ⇒ keep");
    }
}
