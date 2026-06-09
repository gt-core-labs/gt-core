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
//! [`SessionRegistry`], and for every still-active session whose process is provably gone it emits
//! `agent.killed.v1` onto the hub. The daemon's event-log sink persists it, so the next backend
//! fold shows the session `killed` instead of a ghost `spawned`. Idempotent: once killed the
//! session is terminal in the replay, so it is not active on the next sweep.
//!
//! ## Death signals per session kind
//!
//! Sessions declare two pieces of liveness information at spawn time:
//!
//! - `maintains_heartbeat`: polecats touch a file every tick; interactive sessions (mayor, dog)
//!   do not. Only a session that promised a heartbeat can be declared dead by a stale one.
//! - `tmux_socket`: the `-L <socket>` server where the session's tmux lives. Polecats use the
//!   default server (`None`); interactive sessions use `Some("gt-<workspace>")`.
//!
//! Reap policy (`is_orphan`):
//! - tmux alive on the declared socket → keep (positive liveness signal wins).
//! - `maintains_heartbeat = true` (polecat): orphan iff tmux gone AND heartbeat stale.
//! - `maintains_heartbeat = false, tmux_socket = Some(_)` (interactive, socket known): orphan
//!   iff tmux gone — absence on the correct socket is a positive death signal.
//! - `maintains_heartbeat = false, tmux_socket = None` (interactive, socket unknown — old events
//!   before this fix): keep (cannot verify, safe fallback prevents false kills).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionState};
use gt_eventlog::EventRecord;
use gt_events::Envelope;
use gt_polecat::tmux::{Tmux, TmuxCli};

use crate::mcp::eventlog::EventLog;

/// Decide whether an active session is an orphan to reap.
///
/// - tmux alive → keep (positive liveness always wins).
/// - polecat (`maintains_heartbeat`): orphan iff heartbeat stale (tmux already gone).
/// - interactive with known socket (`!maintains_heartbeat, tmux_socket_known`): orphan —
///   the tmux probe ran on the correct server and returned absent.
/// - interactive without known socket (old events, `!maintains_heartbeat, !tmux_socket_known`):
///   keep — we cannot verify, safe fallback prevents false kills.
///
/// Pure, so the reap policy is unit-tested without tmux/fs.
fn is_orphan(
    tmux_alive: bool,
    heartbeat_stale: bool,
    maintains_heartbeat: bool,
    tmux_socket_known: bool,
) -> bool {
    if tmux_alive {
        return false;
    }
    if maintains_heartbeat {
        return heartbeat_stale;
    }
    // Interactive: only conclude death if we queried the right socket.
    tmux_socket_known
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
    /// tmux probe for sessions on the default server (polecats).
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

            // Use the tmux adapter that targets the server where this session lives.
            // Polecats (tmux_socket=None) → shared default adapter.
            // Interactive (tmux_socket=Some(s)) → per-call TmuxCli pinned to that socket.
            let alive = match &session.tmux_socket {
                None => self.tmux.has_session(&session.id),
                Some(socket) => TmuxCli::new().with_socket(socket.clone()).has_session(&session.id),
            };
            let hb = self.heartbeat_dir.join(format!("{}.heartbeat", session.id));
            let stale = gt_polecat::lifecycle::heartbeat_is_stale(&hb, self.stale_after);
            let socket_known = session.tmux_socket.is_some();
            if !is_orphan(alive, stale, session.maintains_heartbeat, socket_known) {
                continue;
            }
            let reason = if session.maintains_heartbeat {
                "orphaned: no tmux session, heartbeat stale".to_string()
            } else {
                format!(
                    "orphaned: tmux session absent on {} (interactive, no heartbeat)",
                    session.tmux_socket.as_deref().unwrap_or("default")
                )
            };
            let event = AgentEvent::Killed {
                session: session.id.clone(),
                reason,
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
    fn polecat_orphan_requires_both_signals_dead() {
        // Polecat (maintains_heartbeat=true, socket_known=false for default server sessions).
        assert!(
            is_orphan(false, true, true, false),
            "polecat: no tmux + stale heartbeat ⇒ orphan"
        );
        assert!(!is_orphan(true, true, true, false), "polecat: tmux present ⇒ keep");
        assert!(!is_orphan(false, false, true, false), "polecat: fresh heartbeat ⇒ keep");
        assert!(!is_orphan(true, false, true, false), "polecat: alive on both ⇒ keep");
    }

    #[test]
    fn interactive_with_socket_reaped_when_tmux_gone() {
        // Mayor/dog with known socket: tmux gone = orphan.
        assert!(
            is_orphan(false, true, false, true),
            "mayor: no tmux + socket known ⇒ orphan"
        );
        assert!(
            is_orphan(false, false, false, true),
            "mayor: no tmux + socket known ⇒ orphan (heartbeat irrelevant)"
        );
        assert!(!is_orphan(true, true, false, true), "mayor: tmux present ⇒ keep");
        assert!(!is_orphan(true, false, false, true), "mayor: tmux present ⇒ keep");
    }

    #[test]
    fn interactive_without_socket_never_reaped() {
        // Pre-flag events (tmux_socket=None, maintains_heartbeat=false): safe fallback.
        assert!(
            !is_orphan(false, true, false, false),
            "old event: no socket known ⇒ keep (can't verify)"
        );
        assert!(!is_orphan(false, false, false, false), "old event ⇒ keep");
        assert!(!is_orphan(true, true, false, false), "old event: tmux present ⇒ keep");
    }
}
