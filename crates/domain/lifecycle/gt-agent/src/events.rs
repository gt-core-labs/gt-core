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
        // Versioned + kebab-only kinds (docs/04 "versioned event kinds"), matching the
        // canonical shape `AgentModule::capability` already declares. The ported kinds were
        // bare/underscored (`agent.spawned`, `agent.session_end`) — a gastown carry-over the
        // rig/quota/merge kinds were `.v1`-backfilled for in events.2 but agent missed. The
        // event-log NS filter is the `agent.` prefix, so old bare-kind records still replay.
        match self {
            AgentEvent::Spawned { .. } => "agent.spawned.v1",
            AgentEvent::Heartbeat { .. } => "agent.heartbeat.v1",
            AgentEvent::SessionEnd { .. } => "agent.session-end.v1",
            AgentEvent::Killed { .. } => "agent.killed.v1",
        }
    }
}
