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
//! `agent.killed.v1`. The emit is persisted (hub sink or direct log append, see [`ReapSink`]), so
//! the next backend fold shows the session `killed` instead of a ghost `spawned`. Idempotent: once
//! killed the session is terminal in the replay, so it is not active on the next sweep.
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
//!
//! ## Ownership: only judge a session whose tmux you can reach (`hq-flow-validation-20260609.5`)
//!
//! A reconciler can only conclude death from a tmux probe that actually reaches the session's
//! server. The two session classes live on tmux servers in **different containers**:
//!
//! - **polecats** run on the default server inside the orchd container (`gt-app-orchd`).
//! - **interactive** (mayor/dog) run on the per-workspace `gt-<ws>` socket inside the mcp-server
//!   container (`gt-app-mcp-server`) — its terminal WS handler created them there.
//!
//! tmux `-L` sockets are files under each container's own `$TMUX_TMPDIR` (`/tmp` is not shared),
//! so the orchd reconciler probing `gt-<ws>` always sees *absent* even for a live mayor — which
//! would false-kill it. Therefore each [`SessionReconciler`] declares a [`ReapScope`]: it only
//! judges the class whose server it can reach, and runs in that class's container. orchd runs
//! `Heartbeat` (polecats); mcp-server runs `Interactive` (mayor/dog). The emit path differs per
//! host too ([`ReapSink`]): orchd publishes onto its daemon hub; mcp-server, which has no daemon
//! hub, appends straight to the shared per-workspace log (the SSE feed reads it immediately).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionState};
use gt_eventlog::EventRecord;
use gt_events::Envelope;
use gt_polecat::tmux::{Tmux, TmuxCli};

use crate::mcp::eventlog::EventLog;

/// Which session class a reconciler instance owns — and therefore the only class it judges.
///
/// A reconciler must run in the container that can reach the class's tmux server (see the module
/// doc): polecats live on the orchd default server, interactive mayor/dog on the mcp-server
/// `gt-<ws>` socket. Probing a class you cannot reach reports it absent and false-kills a live
/// session, so each instance filters [`SessionRegistry::active`] down to the class it owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReapScope {
    /// Heartbeat-bearing sessions (polecats), reachable from the orchd default server.
    Heartbeat,
    /// Interactive sessions (mayor/dog), reachable from the mcp-server per-workspace socket.
    Interactive,
}

impl ReapScope {
    /// Does this scope own a session with the given heartbeat promise? Polecats
    /// (`maintains_heartbeat = true`) belong to `Heartbeat`; interactive sessions to `Interactive`.
    fn owns(&self, maintains_heartbeat: bool) -> bool {
        match self {
            ReapScope::Heartbeat => maintains_heartbeat,
            ReapScope::Interactive => !maintains_heartbeat,
        }
    }
}

/// Where a reaped `agent.killed.v1` is published so the backend fold and the SSE feed pick it up.
///
/// The two host processes persist events differently: the orchd daemon drains a broadcast hub into
/// the durable per-workspace log, while the mcp-server has no such daemon hub and writes the log
/// directly. Both ultimately land in the same per-workspace event log the Sessions view folds.
pub enum ReapSink {
    /// Publish onto the daemon hub; the orchd persistence sink writes it to the log.
    Hub(broadcast::Sender<EventRecord>),
    /// Append straight to the shared event log (immediately visible to the SSE feed).
    Log(Arc<EventLog>),
}

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

/// The session reconciler. Replays the workspace `agent.*` log to find still-open sessions of the
/// class it [owns](ReapScope), and emits `agent.killed.v1` for the ones whose process is provably
/// gone.
pub struct SessionReconciler {
    /// Durable event-log root (`GT_EVENTLOG_ROOT`) — the same log the backend folds for its
    /// Sessions view.
    event_root: PathBuf,
    /// Workspace slug whose `agent.*` log is reconciled.
    workspace: String,
    /// Heartbeat directory (`GT_HEARTBEAT_DIR`); a session's file is `<dir>/<session>.heartbeat`.
    /// Only consulted for the `Heartbeat` scope.
    heartbeat_dir: PathBuf,
    /// A heartbeat older than this counts the session as dead (with a missing tmux session).
    stale_after: Duration,
    /// tmux probe for sessions on the default server (polecats). Interactive sessions are probed
    /// per-session on their declared `gt-<ws>` socket, so this adapter is only used by `Heartbeat`.
    tmux: Arc<dyn Tmux>,
    /// The session class this instance owns — the only one it judges (see [`ReapScope`]).
    scope: ReapScope,
    /// Where the reaped `agent.killed.v1` is published (see [`ReapSink`]).
    sink: ReapSink,
}

impl SessionReconciler {
    /// Wire the reconciler with every source it observes, the class it owns, and the emit sink.
    pub fn new(
        event_root: PathBuf,
        workspace: String,
        heartbeat_dir: PathBuf,
        stale_after: Duration,
        tmux: Arc<dyn Tmux>,
        scope: ReapScope,
        sink: ReapSink,
    ) -> Self {
        Self {
            event_root,
            workspace,
            heartbeat_dir,
            stale_after,
            tmux,
            scope,
            sink,
        }
    }

    /// Reconcile once: close every orphaned session of the owned class. Returns the number of
    /// sessions reaped this sweep. Best-effort: a log-replay failure logs and yields 0 rather than
    /// aborting the daemon.
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

            // Only judge the class this instance can actually reach — probing the other class's
            // tmux (in another container) reports it absent and would false-kill a live session.
            if !self.scope.owns(session.maintains_heartbeat) {
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
            if self.emit(event) {
                eprintln!(
                    "[session-reconcile] {} ({:?}) orphaned — emitted agent.killed",
                    session.id, session.state
                );
                reaped += 1;
            }
        }
        reaped
    }

    /// Publish one `agent.killed.v1` via the configured sink. Returns whether it was accepted.
    fn emit(&self, event: AgentEvent) -> bool {
        match &self.sink {
            ReapSink::Hub(hub) => match EventRecord::from_envelope(&Envelope::root(event)) {
                Ok(record) => hub.send(record).is_ok(),
                Err(e) => {
                    eprintln!("[session-reconcile] encode killed failed: {e}");
                    false
                }
            },
            ReapSink::Log(log) => match log.append(Some(&self.workspace), event) {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("[session-reconcile] append killed failed: {e}");
                    false
                }
            },
        }
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

    #[test]
    fn scope_owns_only_its_class() {
        // Heartbeat scope owns polecats (maintains_heartbeat=true), not interactive sessions.
        assert!(ReapScope::Heartbeat.owns(true), "heartbeat owns polecats");
        assert!(!ReapScope::Heartbeat.owns(false), "heartbeat skips interactive");
        // Interactive scope owns mayor/dog (maintains_heartbeat=false), not polecats.
        assert!(ReapScope::Interactive.owns(false), "interactive owns mayor/dog");
        assert!(!ReapScope::Interactive.owns(true), "interactive skips polecats");
    }
}
