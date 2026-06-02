use serde::{Deserialize, Serialize};

use gt_events::EventKind;

use crate::state::SessionRole;

/// Eventos del dominio agente. Enum **owned** (sin lifetimes, trivialmente `Send`); el
/// `match` de `kind()` es exhaustivo y lo verifica el compilador (añade variante y olvida
/// el brazo → no compila). `Serialize`/`Deserialize` para el log de eventos (gt-audit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentEvent {
    /// A session was spawned. `role`/`crew` (hq-8iur.7) carry the agent kind and the crew
    /// running inside a polecat. `#[serde(default)]` keeps legacy log entries (pre-8.7,
    /// without these fields) decodable → replay over an old log still works.
    Spawned {
        session: String,
        rig: String,
        #[serde(default)]
        role: SessionRole,
        #[serde(default)]
        crew: Option<String>,
    },
    Heartbeat { session: String },
    SessionEnd { session: String },
    Killed { session: String, reason: String },
}

impl EventKind for AgentEvent {
    fn kind(&self) -> &'static str {
        match self {
            AgentEvent::Spawned { .. } => "agent.spawned",
            AgentEvent::Heartbeat { .. } => "agent.heartbeat",
            AgentEvent::SessionEnd { .. } => "agent.session_end",
            AgentEvent::Killed { .. } => "agent.killed",
        }
    }
}
