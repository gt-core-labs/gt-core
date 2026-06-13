//! `escalate.*` domain dispatch (A6, gtcore-46c9dc).
//!
//! Human escalation for autonomous agents: an agent calls `escalate.request` when
//! it encounters a decision it cannot make alone (destructive operations, ambiguity,
//! policy gates). The request is event-sourced to the workspace log, an operator
//! notification is emitted (SSE `escalation.requested.v1`), and the agent receives
//! an instruction to stop work on that bead.
//!
//! The human operator reviews the request on the dashboard (or via `escalate.list`)
//! and calls `escalate.respond` with their decision. The response is logged, and
//! when `approved=true` and a `bead` was associated, the bead is re-dispatched to
//! the scheduler via the dispatch channel so a fresh polecat picks it up.
//!
//! The pattern follows kill-and-re-sling (not pause-in-place): tmux sessions are
//! ephemeral and Claude CLI state is lost on crash anyway, so the re-sling gives
//! the next polecat the human's response as context.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use gt_channel::DispatchSink;
use gt_events::EventKind;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_scheduling::dispatch::DispatchPayload;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, opt, req, str_arg};

/// The event-log kind prefix for every escalation event (`escalation.*`).
const NS: &str = "escalation.";

// ── Events ──────────────────────────────────────────────────────────────────

/// Lifecycle events for human escalation requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EscalationEvent {
    /// An agent requested human input.
    Requested {
        id: String,
        session: String,
        #[serde(default)]
        bead: Option<String>,
        reason: String,
    },
    /// A human resolved the escalation.
    Resolved {
        id: String,
        response: String,
        approved: bool,
    },
}

impl EventKind for EscalationEvent {
    fn kind(&self) -> &'static str {
        match self {
            EscalationEvent::Requested { .. } => "escalation.requested.v1",
            EscalationEvent::Resolved { .. } => "escalation.resolved.v1",
        }
    }
}

// ── State (replay projection) ───────────────────────────────────────────────

/// One escalation request's projected state.
#[derive(Debug, Clone, Serialize)]
struct Escalation {
    id: String,
    session: String,
    bead: Option<String>,
    reason: String,
    state: EscalationState,
    response: Option<String>,
    approved: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EscalationState {
    Pending,
    Resolved,
}

/// In-memory projection rebuilt by folding the escalation event stream.
#[derive(Debug, Default)]
struct EscalationRegistry {
    entries: Vec<Escalation>,
}

impl EscalationRegistry {
    /// Event-sourced reducer: fold one event into the registry.
    fn apply(reg: &mut Self, event: &EscalationEvent) {
        match event {
            EscalationEvent::Requested {
                id,
                session,
                bead,
                reason,
            } => {
                reg.entries.push(Escalation {
                    id: id.clone(),
                    session: session.clone(),
                    bead: bead.clone(),
                    reason: reason.clone(),
                    state: EscalationState::Pending,
                    response: None,
                    approved: None,
                });
            }
            EscalationEvent::Resolved {
                id,
                response,
                approved,
            } => {
                if let Some(e) = reg.entries.iter_mut().find(|e| e.id == *id) {
                    e.state = EscalationState::Resolved;
                    e.response = Some(response.clone());
                    e.approved = Some(*approved);
                }
            }
        }
    }

    fn get(&self, id: &str) -> Option<&Escalation> {
        self.entries.iter().find(|e| e.id == id)
    }

    fn pending(&self) -> Vec<&Escalation> {
        self.entries
            .iter()
            .filter(|e| e.state == EscalationState::Pending)
            .collect()
    }
}

// ── Handler ─────────────────────────────────────────────────────────────────

/// Event-sourced handler for the `escalate.*` tool namespace.
pub struct EscalateHandler {
    log: Arc<EventLog>,
    dispatch: Option<Arc<DispatchSink>>,
}

impl EscalateHandler {
    /// Wrap the per-workspace event log.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self {
            log,
            dispatch: None,
        }
    }

    /// Wire the dispatch sink so `escalate.respond` with `approved=true` re-slings
    /// the bead through the scheduler.
    pub fn with_dispatch_channel(mut self, channel: Arc<DispatchSink>) -> Self {
        self.dispatch = Some(channel);
        self
    }

    /// Rebuild the escalation registry from the workspace's event stream.
    fn registry(&self, ws: Option<&str>) -> Result<EscalationRegistry, AppError> {
        self.log.replay_domain(
            ws,
            NS,
            EscalationRegistry::default(),
            EscalationRegistry::apply,
        )
    }

    /// Append a lifecycle event to the log and return a confirmation payload.
    fn record(
        &self,
        ws: Option<&str>,
        event: EscalationEvent,
        id: &str,
    ) -> Result<Value, AppError> {
        let kind = gt_events::EventKind::kind(&event).to_string();
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "id": id, "event": kind }))
    }

    /// Best-effort re-dispatch of a bead through the scheduler.
    fn bridge_to_scheduler(&self, channel: &DispatchSink, bead: &str) {
        let payload = DispatchPayload {
            bead: bead.to_string(),
            title: None,
            priority: 1,
        };
        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                if let Err(e) = channel.emit(&bytes) {
                    eprintln!("[escalate] dispatch bridge: emit failed for {bead} — {e}");
                }
            }
            Err(e) => eprintln!("[escalate] dispatch bridge: serialize failed — {e}"),
        }
    }
}

#[async_trait]
impl DomainHandler for EscalateHandler {
    fn namespace(&self) -> &'static str {
        "escalate"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "escalate.request",
                "Request human input. The agent should stop work on this bead after calling this. \
                 An operator notification is emitted; use escalate.respond to unblock.",
                &[
                    req("id", "string"),
                    req("session", "string"),
                    req("reason", "string"),
                    opt("bead", "string"),
                ],
            ),
            descriptor(
                "escalate.respond",
                "Resolve a pending escalation with a human decision. When approved=true and the \
                 escalation has a bead, the bead is re-dispatched to the scheduler.",
                &[
                    req("id", "string"),
                    req("response", "string"),
                    opt("approved", "boolean"),
                ],
            ),
            descriptor(
                "escalate.list",
                "List escalation requests. By default shows only pending; pass all=true to include resolved.",
                &[opt("all", "boolean")],
            ),
            descriptor(
                "escalate.info",
                "Show one escalation request's state.",
                &[req("id", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "escalate.request" => {
                let id = str_arg(&ctx.args, "id")?.to_string();
                let session = str_arg(&ctx.args, "session")?.to_string();
                let reason = str_arg(&ctx.args, "reason")?.to_string();
                let bead = ctx
                    .args
                    .get("bead")
                    .and_then(Value::as_str)
                    .map(String::from);

                // Reject duplicate requests.
                if self.registry(ws)?.get(&id).is_some() {
                    return Err(AppError::Validation(format!(
                        "escalation {id} already exists"
                    )));
                }

                self.record(
                    ws,
                    EscalationEvent::Requested {
                        id: id.clone(),
                        session,
                        bead,
                        reason,
                    },
                    &id,
                )
            }
            "escalate.respond" => {
                let id = str_arg(&ctx.args, "id")?.to_string();
                let response = str_arg(&ctx.args, "response")?.to_string();
                let approved = ctx
                    .args
                    .get("approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                let reg = self.registry(ws)?;
                let esc = reg
                    .get(&id)
                    .ok_or_else(|| AppError::NotFound(format!("escalation {id}")))?;
                if esc.state == EscalationState::Resolved {
                    return Err(AppError::Validation(format!(
                        "escalation {id} already resolved"
                    )));
                }
                let bead = esc.bead.clone();

                let result = self.record(
                    ws,
                    EscalationEvent::Resolved {
                        id: id.clone(),
                        response,
                        approved,
                    },
                    &id,
                )?;

                // Re-dispatch the bead when approved and a dispatch channel is wired.
                if approved {
                    if let (Some(bead_id), Some(channel)) = (&bead, &self.dispatch) {
                        self.bridge_to_scheduler(channel, bead_id);
                    }
                }

                Ok(result)
            }
            "escalate.list" => {
                let reg = self.registry(ws)?;
                let show_all = ctx
                    .args
                    .get("all")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let entries: Vec<Value> = if show_all {
                    reg.entries.iter().map(escalation_json).collect()
                } else {
                    reg.pending().iter().map(|e| escalation_json(e)).collect()
                };
                Ok(json!({ "escalations": entries }))
            }
            "escalate.info" => {
                let id = str_arg(&ctx.args, "id")?;
                let reg = self.registry(ws)?;
                match reg.get(id) {
                    Some(e) => Ok(escalation_json(e)),
                    None => Err(AppError::NotFound(format!("escalation {id}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one escalation as a JSON payload.
fn escalation_json(e: &Escalation) -> Value {
    json!({
        "id": e.id,
        "session": e.session,
        "bead": e.bead,
        "reason": e.reason,
        "state": e.state,
        "response": e.response,
        "approved": e.approved,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn handler(dir: &TempDir) -> EscalateHandler {
        EscalateHandler::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))))
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    #[tokio::test]
    async fn request_respond_lifecycle() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        // Request an escalation.
        let res = h
            .dispatch(
                "escalate.request",
                ctx(json!({
                    "id": "esc-1",
                    "session": "s1",
                    "reason": "destructive migration",
                    "bead": "gtcore-abc"
                })),
            )
            .await
            .unwrap();
        assert_eq!(res["ok"], true);
        assert_eq!(res["event"], "escalation.requested.v1");

        // Duplicate rejected.
        let dup = h
            .dispatch(
                "escalate.request",
                ctx(json!({
                    "id": "esc-1",
                    "session": "s1",
                    "reason": "duplicate"
                })),
            )
            .await
            .unwrap_err();
        assert!(matches!(dup, AppError::Validation(_)));

        // Info shows pending.
        let info = h
            .dispatch("escalate.info", ctx(json!({ "id": "esc-1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "pending");
        assert_eq!(info["reason"], "destructive migration");

        // List shows one pending.
        let list = h
            .dispatch("escalate.list", ctx(json!({})))
            .await
            .unwrap();
        assert_eq!(list["escalations"].as_array().unwrap().len(), 1);

        // Respond with approval.
        let res = h
            .dispatch(
                "escalate.respond",
                ctx(json!({
                    "id": "esc-1",
                    "response": "go ahead",
                    "approved": true
                })),
            )
            .await
            .unwrap();
        assert_eq!(res["ok"], true);
        assert_eq!(res["event"], "escalation.resolved.v1");

        // Info now shows resolved.
        let info = h
            .dispatch("escalate.info", ctx(json!({ "id": "esc-1" })))
            .await
            .unwrap();
        assert_eq!(info["state"], "resolved");
        assert_eq!(info["approved"], true);

        // Pending list is now empty.
        let list = h
            .dispatch("escalate.list", ctx(json!({})))
            .await
            .unwrap();
        assert_eq!(list["escalations"].as_array().unwrap().len(), 0);

        // All list includes resolved.
        let list = h
            .dispatch("escalate.list", ctx(json!({ "all": true })))
            .await
            .unwrap();
        assert_eq!(list["escalations"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn respond_on_unknown_escalation() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);
        let err = h
            .dispatch(
                "escalate.respond",
                ctx(json!({
                    "id": "nope",
                    "response": "irrelevant"
                })),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn double_resolve_rejected() {
        let dir = TempDir::new().unwrap();
        let h = handler(&dir);

        h.dispatch(
            "escalate.request",
            ctx(json!({
                "id": "esc-2",
                "session": "s1",
                "reason": "policy gate"
            })),
        )
        .await
        .unwrap();

        h.dispatch(
            "escalate.respond",
            ctx(json!({
                "id": "esc-2",
                "response": "denied",
                "approved": false
            })),
        )
        .await
        .unwrap();

        let err = h
            .dispatch(
                "escalate.respond",
                ctx(json!({
                    "id": "esc-2",
                    "response": "actually yes"
                })),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }
}
