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

use gt_agent::{AgentEvent, Session, SessionRole, SessionState};
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
    /// Does this scope own `session`? Heartbeat-bearing sessions (polecats, and role agents whose
    /// exit watch heartbeats for them) belong to `Heartbeat`; interactive sessions on a declared
    /// per-workspace socket belong to `Interactive`.
    ///
    /// gtcore-efb7e6: dog-role sessions with NO declared socket are the orchd's single-shot role
    /// agents (sheriff/witness/deacon/…) — `SpecRoleLauncher` spawns them on the orchd DEFAULT
    /// tmux server, the same server the `Heartbeat` reconciler probes. Legacy registrations of
    /// this shape (spawned with `maintains_heartbeat=false` before the exit watch heartbeated)
    /// previously fell into the never-judge fallback and sat in `spawned` forever (~9 zombie
    /// sheriffs observed); owning them here lets the orchd sweep reap them. An interactive
    /// mayor/dog on the mcp-server ALWAYS declares its `gt-<ws>` socket
    /// ([`SessionRole::tmux_socket`]), so it can never be misread as this shape.
    fn owns(&self, session: &Session) -> bool {
        match self {
            ReapScope::Heartbeat => {
                session.maintains_heartbeat || orchd_hosted_role_agent(session)
            }
            ReapScope::Interactive => {
                !session.maintains_heartbeat && session.tmux_socket.is_some()
            }
        }
    }
}

/// A single-shot role agent hosted on the orchd default tmux server: dog-role with no declared
/// socket (see [`ReapScope::owns`]). Judged like a heartbeat-bearing session — its exit watch
/// maintains the heartbeat file; a registration with NO file (the watch died with the daemon, or
/// predates the watch heartbeating at all) reads as stale and is reapable once tmux is gone.
fn orchd_hosted_role_agent(session: &Session) -> bool {
    matches!(session.role, SessionRole::Dog(_)) && session.tmux_socket.is_none()
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
    /// Extra workspaces also swept (gtcore-717e13): tenants whose adopted rigs' sessions this
    /// daemon announces into their own logs. Reaps for them append directly to their logs.
    extra_workspaces: Vec<String>,
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
            extra_workspaces: Vec::new(),
        }
    }

    /// Extra workspaces whose `agent.*` registries this reconciler also sweeps (gtcore-717e13):
    /// the tenants whose adopted rigs' sessions this daemon announces. Their reaps land in their
    /// own logs.
    pub fn with_extra_workspaces(mut self, workspaces: Vec<String>) -> Self {
        self.extra_workspaces = workspaces;
        self
    }

    /// Reconcile once: close every orphaned session of the owned class, across the primary
    /// workspace AND every extra workspace whose sessions this daemon announces (gtcore-717e13 —
    /// adopted rigs' sessions live in their tenant's log). Returns the number of sessions reaped.
    /// Best-effort: a log-replay failure logs and yields 0 for that workspace.
    pub async fn sweep(&self) -> usize {
        let mut reaped = self.sweep_workspace(&self.workspace).await;
        for ws in &self.extra_workspaces {
            reaped += self.sweep_workspace(ws).await;
        }
        reaped
    }

    /// Sweep ONE workspace's `agent.*` registry; reaps go back to that same workspace's log.
    async fn sweep_workspace(&self, workspace: &str) -> usize {
        let log = EventLog::new(Some(self.event_root.clone()));
        let registry = match log.replay_domain::<gt_agent::SessionRegistry, AgentEvent, _>(
            Some(workspace),
            "agent.",
            gt_agent::SessionRegistry::default(),
            gt_agent::SessionRegistry::apply,
        ) {
            Ok(reg) => reg,
            Err(e) => {
                eprintln!("[session-reconcile] replay agent log failed ({workspace}): {e}");
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
            if !self.scope.owns(&session) {
                continue;
            }

            // Use the tmux adapter that targets the server where this session lives.
            // Polecats + orchd role agents (tmux_socket=None) → shared default adapter.
            // Interactive (tmux_socket=Some(s)) → per-call TmuxCli pinned to that socket.
            let alive = match &session.tmux_socket {
                None => self.tmux.has_session(&session.id),
                Some(socket) => TmuxCli::new().with_socket(socket.clone()).has_session(&session.id),
            };
            let hb = self.heartbeat_dir.join(format!("{}.heartbeat", session.id));
            let stale = gt_polecat::lifecycle::heartbeat_is_stale(&hb, self.stale_after);
            let socket_known = session.tmux_socket.is_some();
            // An orchd-hosted role agent is judged like a heartbeat-bearing session
            // (gtcore-efb7e6): its exit watch maintains the file while the tmux lives, and a
            // legacy registration with no file at all reads stale — reapable once tmux is gone.
            let heartbeat_judged =
                session.maintains_heartbeat || orchd_hosted_role_agent(&session);
            if !is_orphan(alive, stale, heartbeat_judged, socket_known) {
                continue;
            }
            let reason = if session.maintains_heartbeat {
                "orphaned: no tmux session, heartbeat stale".to_string()
            } else if heartbeat_judged {
                "orphaned: role-agent registration with no tmux session and no heartbeat"
                    .to_string()
            } else {
                format!(
                    "orphaned: tmux session absent on {} (interactive, no heartbeat)",
                    session.tmux_socket.as_deref().unwrap_or("default")
                )
            };
            let event = AgentEvent::killed(session.id.clone(), reason);
            if self.emit(workspace, event) {
                eprintln!(
                    "[session-reconcile] {} ({:?}) orphaned in {workspace} — emitted agent.killed",
                    session.id, session.state
                );
                reaped += 1;
            }
        }
        reaped
    }

    /// Publish one `agent.killed.v1` via the configured sink. A kill for an EXTRA workspace's
    /// session must land in THAT workspace's log (the hub sink only persists to the primary), so
    /// it always appends directly (gtcore-717e13). Returns whether it was accepted.
    fn emit(&self, workspace: &str, event: AgentEvent) -> bool {
        if workspace != self.workspace {
            let log = EventLog::new(Some(self.event_root.clone()));
            return match log.append(Some(workspace), event) {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("[session-reconcile] append killed failed ({workspace}): {e}");
                    false
                }
            };
        }
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

    fn session(role: SessionRole, maintains_heartbeat: bool, socket: Option<&str>) -> Session {
        let mut s = Session::with_role("s-1", "granite", role, None);
        s.maintains_heartbeat = maintains_heartbeat;
        s.tmux_socket = socket.map(str::to_string);
        s
    }

    #[test]
    fn scope_owns_only_its_class() {
        use gt_agent::DogKind;
        // Heartbeat scope owns polecats (maintains_heartbeat=true), not socketed interactive.
        let polecat = session(SessionRole::Polecat, true, None);
        let mayor_socketed = session(SessionRole::Mayor, false, Some("gt-ws"));
        assert!(ReapScope::Heartbeat.owns(&polecat), "heartbeat owns polecats");
        assert!(!ReapScope::Heartbeat.owns(&mayor_socketed), "heartbeat skips interactive");
        // Interactive scope owns socketed mayor/dog, not polecats.
        assert!(ReapScope::Interactive.owns(&mayor_socketed), "interactive owns mayor/dog");
        assert!(!ReapScope::Interactive.owns(&polecat), "interactive skips polecats");

        // gtcore-efb7e6: a dog-role session with NO socket is an orchd-hosted role agent — owned
        // by the Heartbeat scope (its tmux lives on the orchd default server), never Interactive.
        let sheriff = session(SessionRole::Dog(DogKind::Sheriff), false, None);
        assert!(ReapScope::Heartbeat.owns(&sheriff), "heartbeat owns orchd role agents");
        assert!(!ReapScope::Interactive.owns(&sheriff), "interactive skips orchd role agents");
        // A socketed dog (mcp-server interactive) stays Interactive.
        let dog_socketed = session(SessionRole::Dog(DogKind::Witness), false, Some("gt-ws"));
        assert!(!ReapScope::Heartbeat.owns(&dog_socketed));
        assert!(ReapScope::Interactive.owns(&dog_socketed));
        // A legacy pre-role event (defaults: polecat role, no heartbeat, no socket) is owned by
        // NEITHER scope — same never-judge outcome as before, just decided at ownership.
        let legacy = session(SessionRole::Polecat, false, None);
        assert!(!ReapScope::Heartbeat.owns(&legacy));
        assert!(!ReapScope::Interactive.owns(&legacy));
    }

    #[test]
    fn orchd_role_agent_is_judged_like_a_heartbeat_session() {
        // The sweep passes `heartbeat_judged=true` for a dog-with-no-socket: no tmux + missing
        // heartbeat file (stale=true) ⇒ orphan; a live tmux or fresh file keeps it.
        assert!(
            is_orphan(false, true, true, false),
            "zombie role agent: no tmux + no/stale heartbeat ⇒ reap"
        );
        assert!(!is_orphan(true, true, true, false), "live tmux ⇒ keep");
        assert!(!is_orphan(false, false, true, false), "fresh heartbeat ⇒ keep");
    }
}
