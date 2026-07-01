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
//! The emitted kinds are versioned + kebab-only (`agent.spawned.v1`,
//! `agent.session-end.v1`, …) per docs/04 §"versioned event kinds", matching the
//! shape `AgentModule::capability` declares. The dispatch `tool` names above stay
//! bare (`agent.spawn`/…) — that is the MCP-tool namespace, not the event vocab.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use gt_agent::{
    validate_session_id, AgentEvent, Session, SessionRegistry, SessionRole, SessionState,
    DEFAULT_SESSION_RETENTION_SECS,
};
use gt_channel::DispatchSink;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_scheduling::dispatch::DispatchPayload;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, opt, parse, req, str_arg};

/// The event-log kind prefix for every agent event (`agent.*`).
const NS: &str = "agent.";

/// Event-sourced handler for the `agent.*` tool namespace.
pub struct AgentHandler {
    log: Arc<EventLog>,
    /// Optional bridge to the orchd scheduler. When wired, `agent.spawn` with a `bead`
    /// argument drops a `{bead,priority}` request on the same channel orchd's dispatch
    /// loop consumes, causing a real polecat to sling. `None` ⇒ spawn only records the
    /// event (in-memory tests, GT_CHANNEL_ROOT/GT_EVENTLOG_PG unset).
    dispatch: Option<Arc<DispatchSink>>,
}

impl AgentHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log, dispatch: None }
    }

    /// Wire the dispatch sink so `agent.spawn` with a `bead` argument bridges to orchd.
    pub fn with_dispatch_channel(mut self, channel: Arc<DispatchSink>) -> Self {
        self.dispatch = Some(channel);
        self
    }

    /// Drop a `{bead,priority}` dispatch request on the channel — orchd picks it up and
    /// slings a real polecat. Best-effort: a failure logs but never fails the spawn call.
    fn bridge_to_scheduler(&self, channel: &DispatchSink, bead: &str) {
        let payload = DispatchPayload {
            bead: bead.to_string(),
            title: None,
            priority: 1,
        };
        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                if let Err(e) = channel.emit(&bytes) {
                    eprintln!("[agent] dispatch bridge: emit failed for {bead} — {e}");
                }
            }
            Err(e) => eprintln!("[agent] dispatch bridge: serialize failed — {e}"),
        }
    }

    /// Rebuild the session registry from the workspace's agent events.
    fn registry(&self, ws: Option<&str>) -> Result<SessionRegistry, AppError> {
        self.log
            .replay_domain(ws, NS, SessionRegistry::default(), SessionRegistry::apply)
    }

    /// Append a lifecycle event after a light existence check against the
    /// rehydrated registry (the event log itself folds facts unconditionally; the
    /// check keeps the dispatch surface from recording a no-op for a missing id).
    fn record(
        &self,
        ws: Option<&str>,
        event: AgentEvent,
        session: &str,
    ) -> Result<Value, AppError> {
        let kind = gt_events::EventKind::kind(&event).to_string();
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "session": session, "event": kind }))
    }
}

/// `agent.spawn` payload: a session id + rig, with an optional role/crew (the
/// `Spawned` event's own defaults apply when omitted). `bead` is optional: when
/// provided and the dispatch channel is wired, the bead is bridged to the orchd
/// scheduler so a real tmux polecat actually slings.
#[derive(Deserialize)]
struct SpawnArgs {
    session: String,
    rig: String,
    #[serde(default)]
    role: SessionRole,
    #[serde(default)]
    crew: Option<String>,
    /// Bead id to dispatch to orchd after recording the spawn event.
    #[serde(default)]
    bead: Option<String>,
}

#[async_trait]
impl DomainHandler for AgentHandler {
    fn namespace(&self) -> &'static str {
        "agent"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "agent.spawn",
                "Record a new agent session (spawn) under a session id + rig. Pass `bead` to also dispatch to orchd so a real polecat slings.",
                &[
                    req("session", "string"),
                    req("rig", "string"),
                    opt("role", "string"),
                    opt("crew", "string"),
                    opt("bead", "string"),
                ],
            ),
            descriptor(
                "agent.heartbeat",
                "Record a liveness heartbeat for a session.",
                &[req("session", "string")],
            ),
            descriptor(
                "agent.end",
                "Record a session's normal end.",
                &[req("session", "string")],
            ),
            descriptor(
                "agent.kill",
                "Record a session forcibly killed with a reason.",
                &[req("session", "string"), req("reason", "string")],
            ),
            descriptor(
                "agent.pause",
                "Record a session suspended in place (pause-in-place, B2): the coding agent is \
                 SIGSTOP'd so its context survives instead of being killed. Use agent.resume to \
                 lift it. Records agent.paused.v1; the REST /:id/pause endpoint performs the signal.",
                &[req("session", "string"), req("reason", "string")],
            ),
            descriptor(
                "agent.resume",
                "Record a suspended session resumed (SIGCONT). Records agent.resumed.v1, folding \
                 the session back to working.",
                &[req("session", "string")],
            ),
            descriptor(
                "agent.list",
                "List agent sessions in the workspace. By default only live sessions and those that \
                 ended within the retention window are returned; pass `all=true` to include \
                 terminated sessions past it. `crew` narrows to one mayor's supervised polecats.",
                &[opt("crew", "string"), opt("all", "boolean")],
            ),
            descriptor(
                "agent.info",
                "Show one agent session's state.",
                &[req("session", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "agent.spawn" => {
                let a: SpawnArgs = parse(ctx.args)?;
                // Reject a malformed id (empty role/suffix, e.g. `"mayor-"`) before it enters the
                // log (gtcore-065009).
                validate_session_id(&a.session).map_err(AppError::Validation)?;
                if self.registry(ws)?.get(&a.session).is_some() {
                    return Err(AppError::Validation(format!(
                        "session {} already exists",
                        a.session
                    )));
                }
                let session = a.session.clone();
                let bead = a.bead.clone();
                let result = self.record(
                    ws,
                    AgentEvent::Spawned {
                        session: a.session,
                        rig: a.rig,
                        role: a.role,
                        crew: a.crew,
                        // A manual MCP spawn carries no worktree manifest (hq-orch-sessions.2).
                        skills: Vec::new(),
                        hooks: Vec::new(),
                        maintains_heartbeat: a.role.maintains_heartbeat(),
                        tmux_socket: a.role.tmux_socket(ws.unwrap_or("default")),
                        // A5 (gtcore-f3a016): the calling actor triggered this spawn.
                        spawned_by: Some(ctx.actor.to_string()),
                    },
                    &session,
                )?;
                // Bridge the bead to orchd so a real tmux polecat slings. Best-effort:
                // the spawn event is already recorded regardless of dispatch outcome.
                if let (Some(bead_id), Some(channel)) = (&bead, &self.dispatch) {
                    self.bridge_to_scheduler(channel, bead_id);
                }
                Ok(result)
            }
            "agent.heartbeat" => {
                let session = self.require_session(ws, &ctx.args)?;
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .ok();
                self.record(
                    ws,
                    AgentEvent::Heartbeat {
                        session: session.clone(),
                        timestamp_secs: ts,
                    },
                    &session,
                )
            }
            "agent.end" => {
                let session = self.require_session(ws, &ctx.args)?;
                self.record(ws, AgentEvent::session_end(session.clone()), &session)
            }
            "agent.kill" => {
                let session = self.require_session(ws, &ctx.args)?;
                let reason = str_arg(&ctx.args, "reason")?.to_string();
                self.record(ws, AgentEvent::killed(session.clone(), reason), &session)
            }
            "agent.pause" => {
                // Pause-in-place (B2): record-only, mirroring agent.kill — the REST /:id/pause
                // endpoint is the door that both records and performs the SIGSTOP (a2a.rs notes the
                // REST surface is the one place that signals the tmux process).
                let session = self.require_session(ws, &ctx.args)?;
                let reason = str_arg(&ctx.args, "reason")?.to_string();
                self.record(
                    ws,
                    AgentEvent::Paused {
                        session: session.clone(),
                        reason,
                    },
                    &session,
                )
            }
            "agent.resume" => {
                let session = self.require_session(ws, &ctx.args)?;
                self.record(
                    ws,
                    AgentEvent::Resumed {
                        session: session.clone(),
                    },
                    &session,
                )
            }
            "agent.list" => {
                let reg = self.registry(ws)?;
                let crew_filter = ctx.args.get("crew").and_then(|v| v.as_str());
                let all = ctx.args.get("all").and_then(|v| v.as_bool()).unwrap_or(false);
                let sessions: Vec<_> = if let Some(mayor_id) = crew_filter {
                    // An explicit crew query returns that mayor's supervised polecats verbatim.
                    reg.crew_of(mayor_id).iter().map(|s| session_json(s)).collect()
                } else if all {
                    reg.snapshot().iter().map(session_json).collect()
                } else {
                    // Retention view (gtcore-065009): drop terminated sessions older than the
                    // window so agent.list stays auditable.
                    let now = gt_agent::now_secs().unwrap_or(0);
                    reg.visible(now, DEFAULT_SESSION_RETENTION_SECS)
                        .iter()
                        .map(session_json)
                        .collect()
                };
                Ok(json!({ "sessions": sessions }))
            }
            "agent.info" => {
                let id = str_arg(&ctx.args, "session")?;
                let reg = self.registry(ws)?;
                match reg.get(id) {
                    Some(s) => {
                        let mut val = session_json(s);
                        // For mayor sessions, include the polecats supervised by this session.
                        if s.role == SessionRole::Mayor {
                            let members: Vec<_> = reg
                                .crew_of(id)
                                .iter()
                                .map(|p| json!({ "id": p.id, "role": p.role.as_str(), "state": state_str(p.state) }))
                                .collect();
                            val["members"] = json!(members);
                        }
                        Ok(val)
                    }
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
        SessionState::Paused => "paused",
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
        "last_heartbeat_at": session.last_heartbeat_at,
        "ended_at": session.ended_at,
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
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// Session lifecycle over the event log: spawn → heartbeat → end, with the
    /// registry rehydrated from the log on every call.
    #[tokio::test]
    async fn session_lifecycle_through_event_log() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch(
            "agent.spawn",
            ctx(json!({ "session": "s1", "rig": "granite" })),
        )
        .await
        .unwrap();

        // Re-spawn rejected against the rehydrated registry.
        let dup = h
            .dispatch(
                "agent.spawn",
                ctx(json!({ "session": "s1", "rig": "granite" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        let info = h
            .dispatch("agent.info", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "spawned");
        assert_eq!(info["rig"], "granite");

        // Heartbeat updates last_heartbeat_at on the session and must be accepted + logged.
        h.dispatch("agent.heartbeat", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        let info = h
            .dispatch("agent.info", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        assert!(
            info["last_heartbeat_at"].is_number(),
            "heartbeat stamps last_heartbeat_at"
        );

        h.dispatch("agent.end", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        let info = h
            .dispatch("agent.info", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        assert_eq!(
            info["state"], "done",
            "SessionEnd folds the session to Done"
        );

        let list = h.dispatch("agent.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn heartbeat_and_info_on_unknown_session() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let gone = h
            .dispatch("agent.heartbeat", ctx(json!({ "session": "nope" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
        let gone2 = h
            .dispatch("agent.info", ctx(json!({ "session": "nope" })))
            .await
            .unwrap_err();
        assert!(matches!(gone2, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn kill_transitions_to_killed() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        h.dispatch(
            "agent.spawn",
            ctx(json!({ "session": "s1", "rig": "granite" })),
        )
        .await
        .unwrap();
        h.dispatch(
            "agent.kill",
            ctx(json!({ "session": "s1", "reason": "timeout" })),
        )
        .await
        .unwrap();
        let info = h
            .dispatch("agent.info", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "killed");
    }

    #[tokio::test]
    async fn pause_then_resume_folds_state_in_place() {
        // B2: pause-in-place folds the session to `paused` (not terminal); resume returns it to
        // `working`. Record-only on the MCP surface — the REST endpoint owns the SIGSTOP/SIGCONT.
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        h.dispatch("agent.spawn", ctx(json!({ "session": "s1", "rig": "granite" })))
            .await
            .unwrap();

        let res = h
            .dispatch("agent.pause", ctx(json!({ "session": "s1", "reason": "escalation" })))
            .await
            .unwrap();
        assert_eq!(res["event"], "agent.paused.v1");
        let info = h.dispatch("agent.info", ctx(json!({ "session": "s1" }))).await.unwrap();
        assert_eq!(info["state"], "paused");

        let res = h
            .dispatch("agent.resume", ctx(json!({ "session": "s1" })))
            .await
            .unwrap();
        assert_eq!(res["event"], "agent.resumed.v1");
        let info = h.dispatch("agent.info", ctx(json!({ "session": "s1" }))).await.unwrap();
        assert_eq!(info["state"], "working");
    }

    #[tokio::test]
    async fn pause_on_unknown_session_is_not_found() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let gone = h
            .dispatch("agent.pause", ctx(json!({ "session": "nope", "reason": "x" })))
            .await
            .unwrap_err();
        assert!(matches!(gone, AppError::NotFound(_)));
    }
}
