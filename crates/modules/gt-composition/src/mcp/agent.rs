//! `agent.*` (session lifecycle) domain dispatch (`hq-mcp-dispatch.7`).
//!
//! gt-agent's sessions are event-sourced: the [`SessionRegistry`] keeps no table
//! and is rebuilt by folding the [`AgentEvent`] stream through its `apply`
//! reducer. So like merge/convoy, this handler is backed by [`EventLog`] —
//! rehydrate the registry from the workspace log, then append the lifecycle event.
//!
//! ## Why the event facet, not `AgentCommand`
//!
//! gt-agent exposes two facets that do **not** mirror each other: the in-memory
//! `AgentCommand` path (Add/Remove/Transition over the registry) returns `()` and
//! emits **no** events, while the durable, replayable facet is the `AgentEvent`
//! stream (Spawned/Heartbeat/SessionEnd/Killed) the registry's `apply` reducer
//! folds. With no projection table behind the command path, only the event facet
//! has durable backing — and that event stream *is* the session lifecycle
//! (spawn → heartbeat → end/kill). So the durable dispatch surfaces it directly:
//! `agent.spawn` / `agent.heartbeat` / `agent.end` / `agent.kill` + the reads.
//!
//! NOTE: the ported `AgentEvent` kinds are unversioned (`agent.spawned`, not
//! `…​.v1`) — a docs/04 §"versioned event kinds" deviation carried over from
//! gastown, tracked separately, not fixed here.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use gt_agent::{AgentEvent, Session, SessionRegistry, SessionRole, SessionState};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{parse, str_arg};

/// The event-log kind prefix for every agent event (`agent.*`).
const NS: &str = "agent.";

/// Event-sourced handler for the `agent.*` tool namespace.
pub struct AgentHandler {
    log: Arc<EventLog>,
}

impl AgentHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }

    /// Rebuild the session registry from the workspace's agent events.
    fn registry(&self, ws: Option<&str>) -> Result<SessionRegistry, AppError> {
        self.log
            .replay_domain(ws, NS, SessionRegistry::default(), SessionRegistry::apply)
    }

    /// Append a lifecycle event after a light existence check against the
    /// rehydrated registry (the event log itself folds facts unconditionally; the
    /// check keeps the dispatch surface from recording a no-op for a missing id).
    fn record(&self, ws: Option<&str>, event: AgentEvent, session: &str) -> Result<Value, AppError> {
        let kind = gt_events::EventKind::kind(&event).to_string();
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "session": session, "event": kind }))
    }
}

/// `agent.spawn` payload: a session id + rig, with an optional role/crew (the
/// `Spawned` event's own defaults apply when omitted).
#[derive(Deserialize)]
struct SpawnArgs {
    session: String,
    rig: String,
    #[serde(default)]
    role: SessionRole,
    #[serde(default)]
    crew: Option<String>,
}

#[async_trait]
impl DomainHandler for AgentHandler {
    fn namespace(&self) -> &'static str {
        "agent"
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "agent.spawn" => {
                let a: SpawnArgs = parse(ctx.args)?;
                if self.registry(ws)?.get(&a.session).is_some() {
                    return Err(AppError::Validation(format!("session {} already exists", a.session)));
                }
                let session = a.session.clone();
                self.record(
                    ws,
                    AgentEvent::Spawned {
                        session: a.session,
                        rig: a.rig,
                        role: a.role,
                        crew: a.crew,
                    },
                    &session,
                )
            }
            "agent.heartbeat" => {
                let session = self.require_session(ws, &ctx.args)?;
                self.record(ws, AgentEvent::Heartbeat { session: session.clone() }, &session)
            }
            "agent.end" => {
                let session = self.require_session(ws, &ctx.args)?;
                self.record(ws, AgentEvent::SessionEnd { session: session.clone() }, &session)
            }
            "agent.kill" => {
                let session = self.require_session(ws, &ctx.args)?;
                let reason = str_arg(&ctx.args, "reason")?.to_string();
                self.record(ws, AgentEvent::Killed { session: session.clone(), reason }, &session)
            }
            "agent.list" => {
                let reg = self.registry(ws)?;
                Ok(json!({ "sessions": reg.snapshot().iter().map(session_json).collect::<Vec<_>>() }))
            }
            "agent.info" => {
                let id = str_arg(&ctx.args, "session")?;
                match self.registry(ws)?.get(id) {
                    Some(s) => Ok(session_json(s)),
                    None => Err(AppError::NotFound(format!("session {id}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

impl AgentHandler {
    /// Resolve the `session` argument and require it to exist in the rehydrated
    /// registry — a heartbeat/end/kill on an unknown session is a not-found.
    fn require_session(&self, ws: Option<&str>, args: &Value) -> Result<String, AppError> {
        let id = str_arg(args, "session")?;
        if self.registry(ws)?.get(id).is_none() {
            return Err(AppError::NotFound(format!("session {id}")));
        }
        Ok(id.to_string())
    }
}

/// Stable spelling of a session state.
fn state_str(state: SessionState) -> &'static str {
    match state {
        SessionState::Spawned => "spawned",
        SessionState::Working => "working",
        SessionState::Done => "done",
        SessionState::Killed => "killed",
    }
}

/// Shape one session as the dispatch payload.
fn session_json(session: &Session) -> Value {
    json!({
        "id": session.id,
        "rig": session.rig,
        "state": state_str(session.state),
        "role": session.role.as_str(),
        "crew": session.crew,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> AgentHandler {
        AgentHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx { workspace: None, actor: "tester", args }
    }

    /// Session lifecycle over the event log: spawn → heartbeat → end, with the
    /// registry rehydrated from the log on every call.
    #[tokio::test]
    async fn session_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch("agent.spawn", ctx(json!({ "session": "s1", "rig": "granite" })))
            .await
            .unwrap();

        // Re-spawn rejected against the rehydrated registry.
        let dup = h
            .dispatch("agent.spawn", ctx(json!({ "session": "s1", "rig": "granite" })))
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        let info = h.dispatch("agent.info", ctx(json!({ "session": "s1" }))).await.unwrap();
        assert_eq!(info["state"], "spawned");
        assert_eq!(info["rig"], "granite");

        // Heartbeat is a no-op on the registry but must be accepted + logged.
        h.dispatch("agent.heartbeat", ctx(json!({ "session": "s1" }))).await.unwrap();

        h.dispatch("agent.end", ctx(json!({ "session": "s1" }))).await.unwrap();
        let info = h.dispatch("agent.info", ctx(json!({ "session": "s1" }))).await.unwrap();
        assert_eq!(info["state"], "done", "SessionEnd folds the session to Done");

        let list = h.dispatch("agent.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn heartbeat_and_info_on_unknown_session() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let gone = h.dispatch("agent.heartbeat", ctx(json!({ "session": "nope" }))).await.unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
        let gone2 = h.dispatch("agent.info", ctx(json!({ "session": "nope" }))).await.unwrap_err();
        assert!(matches!(gone2, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn kill_transitions_to_killed() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        h.dispatch("agent.spawn", ctx(json!({ "session": "s1", "rig": "granite" }))).await.unwrap();
        h.dispatch("agent.kill", ctx(json!({ "session": "s1", "reason": "timeout" }))).await.unwrap();
        let info = h.dispatch("agent.info", ctx(json!({ "session": "s1" }))).await.unwrap();
        assert_eq!(info["state"], "killed");
    }
}
