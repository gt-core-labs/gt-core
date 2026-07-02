use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gt_events::AppError;

use crate::events::AgentEvent;

/// Ciclo de vida de una sesión. Las transiciones ilegales se rechazan (error semántico
/// atrapado por el tipo, ver `docs/06-observability.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Spawned,
    Working,
    /// Suspended in place (B2, gtcore-5731e9): the coding-agent process has been stopped with
    /// `SIGSTOP` (pause-in-place) but its context is intact. Non-terminal — a `Resumed` event
    /// (`SIGCONT`) returns it to [`Working`](SessionState::Working); a `Killed` may still end it.
    Paused,
    Done,
    Killed,
}

/// Kind of dog (a town/rig supervisory agent). Enumerated from the Go source
/// (`internal/session/identity.go` + `internal/config/roles/`): deacon, overseer, witness,
/// refinery, plus the bare `dog`. `Sheriff` is reserved for the GitHub-sheriff agent that
/// the parity audit (hq-8iur.3) is folding in. Coordinate new kinds there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DogKind {
    #[default]
    Witness,
    Refinery,
    Deacon,
    Overseer,
    Sheriff,
    /// The bare `dog` role (generic supervisory agent).
    Dog,
}

/// Role of an agent session. `Crew` is deliberately NOT a role: it is the claude agent
/// running *inside* a polecat right now, carried as [`Session::crew`]. Mirrors the Go
/// `Role` constants minus `crew` (which is an attribute, not a session kind).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    Mayor,
    Dog(DogKind),
    Polecat,
}

impl Default for SessionRole {
    /// Legacy sessions (pre-8.7 events without a role, and `gt sling` rows) are polecats.
    fn default() -> Self {
        SessionRole::Polecat
    }
}

impl SessionRole {
    /// Flat canonical string for the Dolt `role` column, the `/api/sessions` DTO and the
    /// `?role=` filter. Matches the Go role names so the two runtimes agree during cutover.
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionRole::Mayor => "mayor",
            SessionRole::Polecat => "polecat",
            SessionRole::Dog(DogKind::Witness) => "witness",
            SessionRole::Dog(DogKind::Refinery) => "refinery",
            SessionRole::Dog(DogKind::Deacon) => "deacon",
            SessionRole::Dog(DogKind::Overseer) => "overseer",
            SessionRole::Dog(DogKind::Sheriff) => "sheriff",
            SessionRole::Dog(DogKind::Dog) => "dog",
        }
    }

    /// Parse the flat canonical string back into a role. Unknown strings → `None`.
    pub fn parse(s: &str) -> Option<SessionRole> {
        Some(match s {
            "mayor" => SessionRole::Mayor,
            "polecat" => SessionRole::Polecat,
            "witness" => SessionRole::Dog(DogKind::Witness),
            "refinery" => SessionRole::Dog(DogKind::Refinery),
            "deacon" => SessionRole::Dog(DogKind::Deacon),
            "overseer" => SessionRole::Dog(DogKind::Overseer),
            "sheriff" => SessionRole::Dog(DogKind::Sheriff),
            "dog" => SessionRole::Dog(DogKind::Dog),
            _ => return None,
        })
    }

    pub fn is_dog(&self) -> bool {
        matches!(self, SessionRole::Dog(_))
    }

    /// Whether this role actively maintains a heartbeat file while its session is alive.
    /// Only polecats do; interactive sessions (mayor, dog) rely on tmux presence alone.
    pub fn maintains_heartbeat(&self) -> bool {
        matches!(self, SessionRole::Polecat)
    }

    /// The tmux server socket (`-L <socket>`) for a session of this role in `workspace`, or
    /// `None` when the role uses the default (socketless) server.
    ///
    /// Polecats run on the default server (spawned by `PolecatSupervisor` via `TmuxCli::new()`).
    /// Interactive sessions (mayor, dog) are launched by the terminal WS handler with
    /// `tmux -L gt-<workspace>` and must be probed on that socket.
    /// Mirrors the `gt_polecat::tmux_server_name` convention without the cross-crate dep.
    pub fn tmux_socket(&self, workspace: &str) -> Option<String> {
        match self {
            SessionRole::Polecat => None,
            _ => Some(format!("gt-{workspace}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub rig: String,
    pub state: SessionState,
    /// What kind of agent this session is (mayor/dog/polecat). Defaults to polecat for
    /// legacy events that predate role tracking (hq-8iur.7).
    #[serde(default)]
    pub role: SessionRole,
    /// The claude crew member running inside a polecat right now. Only meaningful for
    /// polecats; `None` for mayor/dogs.
    #[serde(default)]
    pub crew: Option<String>,
    /// Skills the agent has loaded, stamped at sling (`hq-orch-sessions.2`). Empty for sessions
    /// spawned without a worktree manifest.
    #[serde(default)]
    pub skills: Vec<String>,
    /// Hook kinds loaded into the agent. Same provenance as `skills`.
    #[serde(default)]
    pub hooks: Vec<String>,
    /// Mirrors `AgentEvent::Spawned::maintains_heartbeat`. The session reconciler reads this
    /// to decide whether a stale heartbeat is evidence of death. Defaults to `false` for
    /// sessions replayed from pre-flag log entries.
    #[serde(default)]
    pub maintains_heartbeat: bool,
    /// The tmux server socket (`-L <socket>`) where this session's tmux lives, or `None` for
    /// the default server. Polecats → `None`; interactive sessions (mayor/dog) →
    /// `Some("gt-<workspace>")`. The reconciler uses this to probe the correct server.
    /// `#[serde(default)]` → `None` for pre-flag log entries (reconciler skips those safely).
    #[serde(default)]
    pub tmux_socket: Option<String>,
    /// Unix seconds of the most recent `AgentEvent::Heartbeat` that carried a timestamp.
    /// `None` until the first stamped heartbeat arrives (pre-timestamp log entries, or sessions
    /// that have not yet received their first supervisor-tick heartbeat).
    #[serde(default)]
    pub last_heartbeat_at: Option<u64>,
    /// Unix seconds at which the session reached a terminal state (`Done`/`Killed`), stamped from
    /// the terminal event's `at_secs` (gtcore-065009). `None` for a live session, and for terminal
    /// sessions replayed from log entries written before the field existed — those are treated as
    /// old and pruned from the default `agent.list` view (see [`SessionRegistry::visible`]).
    #[serde(default)]
    pub ended_at: Option<u64>,
}

/// Default retention window for terminated sessions in the `agent.list` default view: a session
/// that ended more than this many seconds ago drops out of the default listing (the full history
/// stays reachable via the `all` flag). 7 days — matches the bead's suggested cutoff
/// (gtcore-065009).
pub const DEFAULT_SESSION_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// Reject a malformed session id before it enters the registry (gtcore-065009). An id is
/// malformed when it is empty or carries an empty `<role>-<suffix>` segment — a leading or
/// trailing `-`. This is the shape that produced the anomalous `"mayor-"` session (a mayor woken
/// for an empty rig, `mayor_session("mayor", "")` → `"mayor-"`); rejecting it at the write path
/// stops any transport from recording such a session.
///
/// Transport-agnostic: returns the error message on rejection so each caller can wrap it in its
/// own `AppError` variant (the MCP and REST surfaces carry different `AppError` types).
pub fn validate_session_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.starts_with('-') || id.ends_with('-') {
        return Err(format!(
            "malformed session id {id:?}: empty role or suffix segment"
        ));
    }
    Ok(())
}

impl Session {
    /// Backwards-compatible constructor: a polecat session with no crew attribution. Real
    /// roles arrive via [`Session::with_role`] (the supervisor/sling edge).
    pub fn new(id: impl Into<String>, rig: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            rig: rig.into(),
            state: SessionState::Spawned,
            role: SessionRole::Polecat,
            crew: None,
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: true,
            tmux_socket: None,
            last_heartbeat_at: None,
            ended_at: None,
        }
    }

    /// Full constructor carrying the role and (polecat-only) crew attribution.
    pub fn with_role(
        id: impl Into<String>,
        rig: impl Into<String>,
        role: SessionRole,
        crew: Option<String>,
    ) -> Self {
        Self {
            id: id.into(),
            rig: rig.into(),
            state: SessionState::Spawned,
            role,
            crew,
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: role.maintains_heartbeat(),
            tmux_socket: None,
            last_heartbeat_at: None,
            ended_at: None,
        }
    }

    /// Aplica una transición o la rechaza. Reglas (corrige el ejemplo incompleto del doc):
    /// `Spawned→Working→Done`, y `Killed` admisible desde cualquier estado **activo**.
    /// `Done`/`Killed` son terminales. Pause-in-place (B2, gtcore-5731e9) añade un par
    /// reversible `Working↔Paused`; una sesión pausada sigue siendo activa, así que `Killed`
    /// también es admisible desde `Paused`.
    pub fn transition(&mut self, to: SessionState) -> Result<(), AppError> {
        use SessionState::*;
        let legal = matches!(
            (self.state, to),
            (Spawned, Working)
                | (Working, Done)
                | (Spawned, Killed)
                | (Working, Killed)
                | (Working, Paused)
                | (Paused, Working)
                | (Paused, Killed)
        );
        if legal {
            self.state = to;
            Ok(())
        } else {
            Err(AppError::InvalidTransition(format!(
                "{:?} -> {:?}",
                self.state, to
            )))
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.state, SessionState::Done | SessionState::Killed)
    }
}

/// Registro de sesiones — struct **owned**. Vive dentro de UNA task (el actor); nadie más
/// lo toca, así que no hay `Arc<Mutex>`. Derivación de estado pura → replay-able.
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: HashMap<String, Session>,
}

impl SessionRegistry {
    pub fn add(&mut self, session: Session) {
        self.sessions.insert(session.id.clone(), session);
    }

    pub fn remove(&mut self, id: &str) -> Option<Session> {
        self.sessions.remove(id)
    }

    pub fn get(&self, id: &str) -> Option<&Session> {
        self.sessions.get(id)
    }

    /// Aplica una transición a una sesión existente.
    pub fn transition(&mut self, id: &str, to: SessionState) -> Result<(), AppError> {
        match self.sessions.get_mut(id) {
            Some(s) => s.transition(to),
            None => Err(AppError::NotFound(format!("session {id}"))),
        }
    }

    /// Foto de todas las sesiones (orden no garantizado).
    pub fn snapshot(&self) -> Vec<Session> {
        self.sessions.values().cloned().collect()
    }

    /// All sessions whose `crew` field equals `mayor_id` — the polecats supervised by
    /// that mayor session. Order is not guaranteed (HashMap source).
    pub fn crew_of(&self, mayor_id: &str) -> Vec<&Session> {
        self.sessions
            .values()
            .filter(|s| s.crew.as_deref() == Some(mayor_id))
            .collect()
    }

    /// Solo sesiones no-terminales.
    pub fn active(&self) -> Vec<Session> {
        self.sessions
            .values()
            .filter(|s| !s.is_terminal())
            .cloned()
            .collect()
    }

    /// Sessions shown in the default `agent.list` view (gtcore-065009): every non-terminal
    /// (live) session, plus terminal sessions that ended within `retention_secs` of `now_secs`
    /// (recently finished). A terminal session with no recorded `ended_at` (historical / pre-field
    /// log entries) is treated as old and pruned — the full history stays reachable via
    /// [`snapshot`](Self::snapshot). Order is not guaranteed (HashMap source).
    pub fn visible(&self, now_secs: u64, retention_secs: u64) -> Vec<Session> {
        self.sessions
            .values()
            .filter(|s| {
                !s.is_terminal()
                    || s.ended_at
                        .is_some_and(|t| now_secs.saturating_sub(t) <= retention_secs)
            })
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// **Reducer** del dominio (event-sourcing): pliega un evento como un hecho, sin
    /// validar (la legalidad la chequea el camino de escritura, no el replay). Puro y
    /// total → reconstrucción determinista del estado desde el log (gate del Paso 3).
    pub fn apply(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::Spawned { session, rig, role, crew, skills, hooks, maintains_heartbeat, tmux_socket, .. } => {
                let mut s = Session::with_role(session.clone(), rig.clone(), *role, crew.clone());
                s.skills = skills.clone();
                s.hooks = hooks.clone();
                s.maintains_heartbeat = *maintains_heartbeat;
                s.tmux_socket = tmux_socket.clone();
                self.add(s);
            }
            AgentEvent::Heartbeat { session, timestamp_secs } => {
                if let (Some(s), Some(ts)) = (self.sessions.get_mut(session), *timestamp_secs) {
                    s.last_heartbeat_at = Some(ts);
                    // A heartbeat proves the agent process is alive and executing — a session
                    // still `spawned` at its first stamped heartbeat is de-facto working
                    // (gtcore-efb7e6: the sessions view showed live daemons stuck in `spawned`
                    // because nothing ever transitioned them). Fold it as a fact; terminal and
                    // paused states are untouched (a late heartbeat must not resurrect/resume).
                    if s.state == SessionState::Spawned {
                        s.state = SessionState::Working;
                    }
                }
            }
            AgentEvent::SessionEnd { session, at_secs } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Done;
                    s.ended_at = *at_secs;
                }
            }
            AgentEvent::Killed { session, at_secs, .. } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Killed;
                    s.ended_at = *at_secs;
                }
            }
            // Pause-in-place (B2): fold the suspend/resume facts as state, unconditionally — the
            // reducer derives, it does not validate (legality is the write path's job). A `Resumed`
            // returns the session to `Working`, the state it was suspended from.
            AgentEvent::Paused { session, .. } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Paused;
                }
            }
            AgentEvent::Resumed { session } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Working;
                }
            }
        }
    }

    /// Huella determinista del estado: sesiones ordenadas por id. Igualdad de strings =
    /// estado byte-idéntico (independiente del orden del `HashMap`).
    pub fn fingerprint(&self) -> String {
        let mut rows: Vec<String> = self
            .sessions
            .values()
            .map(|s| {
                format!(
                    "{}:{}:{:?}:{}:{}",
                    s.id,
                    s.rig,
                    s.state,
                    s.role.as_str(),
                    s.crew.as_deref().unwrap_or("-"),
                )
            })
            .collect();
        rows.sort();
        rows.join("|")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_heartbeat_folds_spawned_into_working() {
        // gtcore-efb7e6: live daemons sat in `spawned` forever because nothing transitioned them;
        // a stamped heartbeat is proof of execution, so the reducer folds it as Working.
        let mut reg = SessionRegistry::default();
        reg.apply(&AgentEvent::Spawned {
            session: "sheriff-1".into(),
            rig: "granite".into(),
            role: SessionRole::Dog(DogKind::Sheriff),
            crew: None,
            spawned_by: Some("role-agent".into()),
            skills: vec![],
            hooks: vec![],
            maintains_heartbeat: true,
            tmux_socket: None,
        });
        assert_eq!(reg.get("sheriff-1").unwrap().state, SessionState::Spawned);
        // A timestamp-less heartbeat (pre-field log entry) changes nothing.
        reg.apply(&AgentEvent::Heartbeat { session: "sheriff-1".into(), timestamp_secs: None });
        assert_eq!(reg.get("sheriff-1").unwrap().state, SessionState::Spawned);
        // The first stamped heartbeat flips Spawned → Working and records the timestamp.
        reg.apply(&AgentEvent::Heartbeat { session: "sheriff-1".into(), timestamp_secs: Some(42) });
        let s = reg.get("sheriff-1").unwrap();
        assert_eq!(s.state, SessionState::Working);
        assert_eq!(s.last_heartbeat_at, Some(42));
        // A late heartbeat never resurrects a terminal session.
        reg.apply(&AgentEvent::killed("sheriff-1", "done"));
        reg.apply(&AgentEvent::Heartbeat { session: "sheriff-1".into(), timestamp_secs: Some(99) });
        assert_eq!(reg.get("sheriff-1").unwrap().state, SessionState::Killed);
    }

    #[test]
    fn legal_lifecycle() {
        let mut s = Session::new("p1", "granite");
        assert_eq!(s.state, SessionState::Spawned);
        s.transition(SessionState::Working).unwrap();
        s.transition(SessionState::Done).unwrap();
        assert!(s.is_terminal());
    }

    #[test]
    fn illegal_skip_is_rejected() {
        let mut s = Session::new("p1", "granite");
        // Spawned -> Done sin pasar por Working: error semántico atrapado.
        assert!(s.transition(SessionState::Done).is_err());
        assert_eq!(s.state, SessionState::Spawned, "estado intacto tras rechazo");
    }

    #[test]
    fn kill_allowed_from_active_states() {
        let mut s = Session::new("p1", "granite");
        s.transition(SessionState::Killed).unwrap(); // desde Spawned
        assert!(s.transition(SessionState::Working).is_err()); // terminal, ya no se mueve
    }

    #[test]
    fn pause_resume_is_a_reversible_working_loop() {
        // B2: Working↔Paused is reversible and Paused stays active (non-terminal); a paused
        // session can still be killed, but cannot jump straight to Done.
        let mut s = Session::new("p1", "granite");
        s.transition(SessionState::Working).unwrap();
        s.transition(SessionState::Paused).unwrap();
        assert_eq!(s.state, SessionState::Paused);
        assert!(!s.is_terminal(), "paused session is still active");
        // No skipping a resume to reach Done.
        assert!(s.transition(SessionState::Done).is_err());
        s.transition(SessionState::Working).unwrap();
        assert_eq!(s.state, SessionState::Working);
        s.transition(SessionState::Done).unwrap();
        assert!(s.is_terminal());
    }

    #[test]
    fn pause_is_illegal_from_spawned() {
        // Pause-in-place suspends a *running* agent; a session that never reached Working has no
        // live turn to stop.
        let mut s = Session::new("p1", "granite");
        assert!(s.transition(SessionState::Paused).is_err());
        assert_eq!(s.state, SessionState::Spawned, "estado intacto tras rechazo");
    }

    #[test]
    fn paused_session_can_still_be_killed() {
        let mut s = Session::new("p1", "granite");
        s.transition(SessionState::Working).unwrap();
        s.transition(SessionState::Paused).unwrap();
        s.transition(SessionState::Killed).unwrap();
        assert!(s.is_terminal());
    }

    #[test]
    fn apply_folds_pause_then_resume_back_to_working() {
        // The pure reducer derives suspend/resume as state: Paused → Paused, Resumed → Working.
        let mut reg = SessionRegistry::default();
        reg.apply(&AgentEvent::Spawned {
            session: "p1".into(),
            rig: "r".into(),
            role: SessionRole::Dog(DogKind::Dog),
            crew: None,
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: false,
            tmux_socket: Some("gt-default".into()),
            spawned_by: None,
        });
        reg.apply(&AgentEvent::Paused { session: "p1".into(), reason: "escalation".into() });
        assert_eq!(reg.get("p1").unwrap().state, SessionState::Paused);
        reg.apply(&AgentEvent::Resumed { session: "p1".into() });
        assert_eq!(reg.get("p1").unwrap().state, SessionState::Working);
    }

    #[test]
    fn registry_active_excludes_terminal() {
        let mut reg = SessionRegistry::default();
        reg.add(Session::new("a", "r"));
        reg.add(Session::new("b", "r"));
        reg.transition("b", SessionState::Killed).unwrap();
        assert_eq!(reg.active().len(), 1);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn visible_keeps_live_and_recent_prunes_old_and_undated() {
        // gtcore-065009: the default agent.list view keeps live sessions and terminal sessions
        // that ended within the retention window, and prunes both old-terminal and
        // undated-terminal (historical/pre-field) sessions.
        let now = 100_000_000u64;
        let day = 24 * 60 * 60u64;
        let mut reg = SessionRegistry::default();

        // Live session — always visible.
        reg.add(Session::new("live", "r"));

        // Ended 1 day ago — recent, visible.
        reg.apply(&AgentEvent::Spawned {
            session: "recent".into(), rig: "r".into(), role: SessionRole::Polecat, crew: None,
            skills: Vec::new(), hooks: Vec::new(), maintains_heartbeat: false, tmux_socket: None,
            spawned_by: None,
        });
        reg.apply(&AgentEvent::SessionEnd { session: "recent".into(), at_secs: Some(now - day) });

        // Ended 30 days ago — past the 7-day window, pruned.
        reg.apply(&AgentEvent::Spawned {
            session: "old".into(), rig: "r".into(), role: SessionRole::Polecat, crew: None,
            skills: Vec::new(), hooks: Vec::new(), maintains_heartbeat: false, tmux_socket: None,
            spawned_by: None,
        });
        reg.apply(&AgentEvent::Killed { session: "old".into(), reason: "x".into(), at_secs: Some(now - 30 * day) });

        // Terminal with no recorded end time (historical) — pruned.
        reg.add(Session::new("undated", "r"));
        reg.transition("undated", SessionState::Killed).unwrap();

        let mut ids: Vec<String> = reg
            .visible(now, DEFAULT_SESSION_RETENTION_SECS)
            .into_iter()
            .map(|s| s.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["live".to_string(), "recent".to_string()]);
        // The full history stays reachable via snapshot() (the `all` flag path).
        assert_eq!(reg.snapshot().len(), 4);
    }

    #[test]
    fn validate_session_id_rejects_empty_role_or_suffix() {
        assert!(validate_session_id("mayor-").is_err(), "empty suffix rejected");
        assert!(validate_session_id("-gtcore").is_err(), "empty role rejected");
        assert!(validate_session_id("").is_err(), "empty id rejected");
        assert!(validate_session_id("mayor-gtcore").is_ok());
        assert!(validate_session_id("gtcore-065009").is_ok());
    }
}
