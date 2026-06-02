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
        }
    }

    /// Aplica una transición o la rechaza. Reglas (corrige el ejemplo incompleto del doc):
    /// `Spawned→Working→Done`, y `Killed` admisible desde cualquier estado **activo**.
    /// `Done`/`Killed` son terminales.
    pub fn transition(&mut self, to: SessionState) -> Result<(), AppError> {
        use SessionState::*;
        let legal = matches!(
            (self.state, to),
            (Spawned, Working) | (Working, Done) | (Spawned, Killed) | (Working, Killed)
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

    /// Solo sesiones no-terminales.
    pub fn active(&self) -> Vec<Session> {
        self.sessions
            .values()
            .filter(|s| !s.is_terminal())
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
            AgentEvent::Spawned { session, rig, role, crew } => {
                self.add(Session::with_role(
                    session.clone(),
                    rig.clone(),
                    *role,
                    crew.clone(),
                ));
            }
            AgentEvent::Heartbeat { .. } => {} // no cambia el registro
            AgentEvent::SessionEnd { session } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Done;
                }
            }
            AgentEvent::Killed { session, .. } => {
                if let Some(s) = self.sessions.get_mut(session) {
                    s.state = SessionState::Killed;
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
    fn registry_active_excludes_terminal() {
        let mut reg = SessionRegistry::default();
        reg.add(Session::new("a", "r"));
        reg.add(Session::new("b", "r"));
        reg.transition("b", SessionState::Killed).unwrap();
        assert_eq!(reg.active().len(), 1);
        assert_eq!(reg.len(), 2);
    }
}
