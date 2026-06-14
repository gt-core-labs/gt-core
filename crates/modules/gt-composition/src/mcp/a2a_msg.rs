//! `a2a.send` / `a2a.inbox` — structured inter-agent messaging channel.
//!
//! Enables operators and agents to exchange messages with running polecats
//! without killing the session. Messages are event-sourced to the workspace
//! log (`message.*`), and the target agent polls its inbox via `a2a.inbox`.
//!
//! ## Tools
//!
//! - **`a2a.send`**: post a message to a target session's inbox. The sender's
//!   MCP session id is stamped automatically.
//! - **`a2a.inbox`**: read unacknowledged messages for the caller's session
//!   (or an explicit target). Returns newest-first, capped at `limit`.
//! - **`a2a.ack`**: acknowledge a message by id so it no longer appears in
//!   the inbox. Idempotent.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use gt_events::EventKind;
use gt_mcp_server::DomainCtx;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::str_arg;

const NS: &str = "message.";

// ── Events ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageEvent {
    Sent {
        id: String,
        from: String,
        to: String,
        body: String,
        #[serde(default)]
        in_reply_to: Option<String>,
    },
    Acked {
        id: String,
        by: String,
    },
}

impl EventKind for MessageEvent {
    fn kind(&self) -> &'static str {
        match self {
            MessageEvent::Sent { .. } => "message.sent.v1",
            MessageEvent::Acked { .. } => "message.acked.v1",
        }
    }
}

// ── State (replay projection) ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct Message {
    id: String,
    from: String,
    to: String,
    body: String,
    in_reply_to: Option<String>,
    acked: bool,
}

#[derive(Debug, Default)]
struct Inbox {
    messages: Vec<Message>,
}

impl Inbox {
    fn apply(inbox: &mut Self, event: &MessageEvent) {
        match event {
            MessageEvent::Sent {
                id,
                from,
                to,
                body,
                in_reply_to,
            } => {
                inbox.messages.push(Message {
                    id: id.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    body: body.clone(),
                    in_reply_to: in_reply_to.clone(),
                    acked: false,
                });
            }
            MessageEvent::Acked { id, .. } => {
                if let Some(m) = inbox.messages.iter_mut().find(|m| m.id == *id) {
                    m.acked = true;
                }
            }
        }
    }

    fn unread_for(&self, session: &str, limit: usize) -> Vec<&Message> {
        self.messages
            .iter()
            .rev()
            .filter(|m| m.to == session && !m.acked)
            .take(limit)
            .collect()
    }
}

// ── Handler ─────────────────────────────────────────────────────────────────

/// Event-sourced handler for the inter-agent message channel.
pub struct A2aMessageHandler {
    log: Arc<EventLog>,
}

impl A2aMessageHandler {
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }

    fn inbox(&self, ws: Option<&str>) -> Result<Inbox, AppError> {
        self.log
            .replay_domain(ws, NS, Inbox::default(), Inbox::apply)
    }

    fn record(
        &self,
        ws: Option<&str>,
        event: MessageEvent,
        id: &str,
    ) -> Result<Value, AppError> {
        let kind = gt_events::EventKind::kind(&event).to_string();
        self.log.append(ws, event)?;
        Ok(json!({ "ok": true, "id": id, "event": kind }))
    }

    /// Dispatch a message tool call. Called by `A2aDelegateHandler` which owns
    /// the `a2a` namespace in the domain router.
    pub async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "a2a.send" => {
                let to = str_arg(&ctx.args, "to")?;
                let body = str_arg(&ctx.args, "body")?;
                let in_reply_to = ctx.args.get("in_reply_to").and_then(Value::as_str);

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                let id = format!("msg-{:x}", ts % 0xFFFF_FFFF);
                let event = MessageEvent::Sent {
                    id: id.clone(),
                    from: ctx.actor.to_string(),
                    to: to.to_string(),
                    body: body.to_string(),
                    in_reply_to: in_reply_to.map(String::from),
                };
                self.record(ctx.workspace, event, &id)
            }

            "a2a.inbox" => {
                let session = ctx
                    .args
                    .get("session")
                    .and_then(Value::as_str)
                    .unwrap_or(ctx.actor);
                let limit = ctx
                    .args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(10) as usize;

                let inbox = self.inbox(ctx.workspace)?;
                let messages: Vec<Value> = inbox
                    .unread_for(session, limit)
                    .into_iter()
                    .map(|m| {
                        json!({
                            "id": m.id,
                            "from": m.from,
                            "body": m.body,
                            "in_reply_to": m.in_reply_to,
                        })
                    })
                    .collect();
                Ok(json!({ "messages": messages, "count": messages.len() }))
            }

            "a2a.ack" => {
                let id = str_arg(&ctx.args, "id")?;

                // Verify message exists
                let inbox = self.inbox(ctx.workspace)?;
                if !inbox.messages.iter().any(|m| m.id == id) {
                    return Err(AppError::NotFound(format!("message {id}")));
                }

                let event = MessageEvent::Acked {
                    id: id.to_string(),
                    by: ctx.actor.to_string(),
                };
                self.record(ctx.workspace, event, id)
            }

            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbox_replay_filters_by_session_and_ack() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));

        // Send two messages to agent-1, one to agent-2
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-1".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "check the logs".into(),
                in_reply_to: None,
            },
        )
        .unwrap();
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-2".into(),
                from: "operator".into(),
                to: "agent-1".into(),
                body: "also fix the test".into(),
                in_reply_to: None,
            },
        )
        .unwrap();
        log.append(
            Some("default"),
            MessageEvent::Sent {
                id: "msg-3".into(),
                from: "operator".into(),
                to: "agent-2".into(),
                body: "unrelated".into(),
                in_reply_to: None,
            },
        )
        .unwrap();

        let inbox = log
            .replay_domain(Some("default"), NS, Inbox::default(), Inbox::apply)
            .unwrap();

        // agent-1 sees 2 messages
        assert_eq!(inbox.unread_for("agent-1", 10).len(), 2);
        // agent-2 sees 1
        assert_eq!(inbox.unread_for("agent-2", 10).len(), 1);

        // Ack msg-1
        log.append(
            Some("default"),
            MessageEvent::Acked {
                id: "msg-1".into(),
                by: "agent-1".into(),
            },
        )
        .unwrap();

        let inbox = log
            .replay_domain(Some("default"), NS, Inbox::default(), Inbox::apply)
            .unwrap();
        assert_eq!(inbox.unread_for("agent-1", 10).len(), 1);
        assert_eq!(inbox.unread_for("agent-1", 10)[0].id, "msg-2");
    }

    #[test]
    fn limit_caps_results() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));

        for i in 0..5 {
            log.append(
                Some("default"),
                MessageEvent::Sent {
                    id: format!("msg-{i}"),
                    from: "op".into(),
                    to: "agent".into(),
                    body: format!("msg {i}"),
                    in_reply_to: None,
                },
            )
            .unwrap();
        }

        let inbox = log
            .replay_domain(Some("default"), NS, Inbox::default(), Inbox::apply)
            .unwrap();
        assert_eq!(inbox.unread_for("agent", 3).len(), 3);
        // newest first
        assert_eq!(inbox.unread_for("agent", 3)[0].id, "msg-4");
    }
}
