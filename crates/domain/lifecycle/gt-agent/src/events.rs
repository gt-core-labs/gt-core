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
        /// A5 (gtcore-f3a016): who triggered this spawn — the parent session id, an operator
        /// actor name, or `"manual"` for CLI. `None` for pre-A5 events (serde default).
        /// Enables inter-agent audit: tracing delegation chains when an agent spawns another.
        #[serde(default)]
        spawned_by: Option<String>,
        /// Skills the agent has loaded (its worktree `.claude/skills`), stamped by the polecat
        /// supervisor at sling (`hq-orch-sessions.2`). Empty for sessions spawned without a
        /// worktree (a manual `agent.spawn`). `#[serde(default)]` keeps pre-`.2` log entries
        /// replayable.
        #[serde(default)]
        skills: Vec<String>,
        /// Hook kinds loaded into the agent (`SessionStart`/`Stop`/…). Same provenance + back-compat
        /// as `skills`.
        #[serde(default)]
        hooks: Vec<String>,
        /// Whether this session actively maintains a heartbeat file while alive. Polecats do;
        /// interactive sessions (mayor, dog) do not. The session reconciler uses this flag to
        /// decide whether a stale heartbeat is evidence of death. `#[serde(default)]` keeps
        /// pre-flag log entries replayable — old events default to `false` (safe: the reconciler
        /// cannot conclude death from a missing heartbeat it was never promised).
        #[serde(default)]
        maintains_heartbeat: bool,
        /// The tmux server socket (`-L <socket>`) where this session lives, or `None` for the
        /// default server. Polecats → `None`; interactive sessions (mayor/dog) →
        /// `Some("gt-<workspace>")`. The reconciler needs this to probe the right server.
        /// `#[serde(default)]` → `None` for pre-flag events; the reconciler skips those safely.
        #[serde(default)]
        tmux_socket: Option<String>,
    },
    Heartbeat {
        session: String,
        /// Unix seconds at emission. `None` for log entries written before this field was added
        /// (`#[serde(default)]` keeps those replayable; the registry skips updating
        /// `last_heartbeat_at` when the timestamp is absent rather than stamping epoch zero).
        #[serde(default)]
        timestamp_secs: Option<u64>,
    },
    SessionEnd { session: String },
    Killed { session: String, reason: String },
    /// A session was suspended **in place** (B2, gtcore-5731e9): its coding-agent process is
    /// stopped with `SIGSTOP` so its in-memory context survives, instead of being killed and
    /// re-slung. Pause-in-place is the kill-preserving alternative for interactive sessions
    /// (mayor, dog) where losing the live conversation is expensive. `reason` records why the
    /// session was paused (the escalation/operator action). This is **not** terminal: a matching
    /// [`AgentEvent::Resumed`] (`SIGCONT`) folds the session back to `Working`.
    Paused { session: String, reason: String },
    /// A suspended session was resumed with `SIGCONT` (B2, gtcore-5731e9) — folds the session
    /// back to `Working`.
    Resumed { session: String },
}

impl EventKind for AgentEvent {
    fn kind(&self) -> &'static str {
        // Versioned + kebab-only kinds (docs/04 "versioned event kinds"), matching the
        // canonical shape `AgentModule::capability` already declares. The ported kinds were
        // bare/underscored (`agent.spawned`, `agent.session_end`) — an upstream carry-over the
        // rig/quota/merge kinds were `.v1`-backfilled for in events.2 but agent missed. The
        // event-log NS filter is the `agent.` prefix, so old bare-kind records still replay.
        match self {
            AgentEvent::Spawned { .. } => "agent.spawned.v1",
            AgentEvent::Heartbeat { .. } => "agent.heartbeat.v1", // kind unchanged; new field is additive
            AgentEvent::SessionEnd { .. } => "agent.session-end.v1",
            AgentEvent::Killed { .. } => "agent.killed.v1",
            // Pause-in-place lifecycle (B2). New kinds, born versioned + kebab — no legacy
            // bare-kind records exist, so no back-compat entry is needed in the reader.
            AgentEvent::Paused { .. } => "agent.paused.v1",
            AgentEvent::Resumed { .. } => "agent.resumed.v1",
        }
    }
}
