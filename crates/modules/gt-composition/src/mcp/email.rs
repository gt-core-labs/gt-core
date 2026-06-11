//! `email.*` domain dispatch — the programmed-send surface of the outbox
//! (hq-f24599, epic hq-56b5ee).
//!
//! `email.schedule` enqueues a row (`send_at` optional, defaults to now);
//! `email.list` shows the caller's workspace queue; `email.cancel` recalls a
//! still-pending/retry row. Delivery is the drain daemon's job
//! ([`crate::email_outbox_drain`]) through the `gt_notify::EmailTransport`
//! seam — this handler never touches a transport, so the pending SMTP server
//! costs nothing here.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::PgPool;

use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;
use gt_store_pg::{EmailOutboxRepository, NewEmail, OutboxEntry, OutboxError, PgEmailOutbox};

use super::util::{descriptor, opt, req, str_arg};

const DEFAULT_WORKSPACE: &str = "default";
const DEFAULT_LIST_LIMIT: i64 = 50;

/// PG-backed handler for the `email.*` tool namespace.
pub struct EmailHandler {
    repo: Arc<PgEmailOutbox>,
}

impl EmailHandler {
    /// Wire the shared public-schema pool the outbox lives in.
    pub fn new(pool: PgPool) -> Self {
        Self { repo: Arc::new(PgEmailOutbox::new(pool)) }
    }
}

fn outbox_err(e: OutboxError) -> AppError {
    match e {
        OutboxError::NotFound(id) => AppError::NotFound(format!(
            "outbox entry {id} (or it is no longer cancellable)"
        )),
        OutboxError::Db(e) => AppError::Other(format!("email outbox: {e}")),
    }
}

fn entry_json(e: &OutboxEntry) -> Value {
    json!({
        "id": e.id,
        "to": e.recipient,
        "subject": e.subject,
        "template_ref": e.template_ref,
        "send_at": e.send_at.to_rfc3339(),
        "status": e.status,
        "attempts": e.attempts,
        "last_error": e.last_error,
        "created_by": e.created_by,
        "created_at": e.created_at.to_rfc3339(),
        "sent_at": e.sent_at.map(|t| t.to_rfc3339()),
    })
}

#[async_trait]
impl DomainHandler for EmailHandler {
    fn namespace(&self) -> &'static str {
        "email"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "email.schedule",
                "Schedule a programmed email: enqueue an outbox row the drain daemon delivers \
                 through the configured transport when send_at falls due (omit send_at for \
                 as-soon-as-possible). The SMTP server is a pluggable seam — with the default \
                 log transport the send is recorded, not mailed. Returns the outbox entry.",
                &[
                    req("to", "string"),
                    req("subject", "string"),
                    opt("body", "string"),
                    opt("template_ref", "string"),
                    opt("send_at", "string"),
                ],
            ),
            descriptor(
                "email.list",
                "The caller workspace's outbox, newest first: status \
                 pending|sending|sent|retry|failed|cancelled, attempts, last_error, send_at. \
                 Read-only.",
                &[opt("status", "string"), opt("limit", "integer")],
            ),
            descriptor(
                "email.cancel",
                "Cancel a still-pending (or retry-scheduled) outbox entry of this workspace. \
                 A row already sending/sent cannot be recalled.",
                &[req("id", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE);
        match tool {
            "email.schedule" => {
                let to = str_arg(&ctx.args, "to")?.trim().to_string();
                let subject = str_arg(&ctx.args, "subject")?.trim().to_string();
                if to.is_empty() || !to.contains('@') {
                    return Err(AppError::Validation(format!(
                        "`to` must be an email address, got `{to}`"
                    )));
                }
                if subject.is_empty() {
                    return Err(AppError::Validation("subject must not be empty".into()));
                }
                let body = ctx.args.get("body").and_then(Value::as_str).unwrap_or("").to_string();
                let template_ref = ctx
                    .args
                    .get("template_ref")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let send_at = match ctx.args.get("send_at").and_then(Value::as_str) {
                    Some(raw) => Some(
                        DateTime::parse_from_rfc3339(raw)
                            .map_err(|e| {
                                AppError::Validation(format!(
                                    "send_at must be RFC3339 (e.g. 2026-06-12T09:00:00Z): {e}"
                                ))
                            })?
                            .with_timezone(&Utc),
                    ),
                    None => None,
                };
                let entry = self
                    .repo
                    .enqueue(NewEmail {
                        id: ulid::Ulid::new().to_string(),
                        workspace: workspace.to_string(),
                        recipient: to,
                        subject,
                        body,
                        template_ref,
                        send_at,
                        created_by: ctx.actor.to_string(),
                    })
                    .await
                    .map_err(outbox_err)?;
                Ok(json!({ "ok": true, "entry": entry_json(&entry) }))
            }
            "email.list" => {
                let status = ctx.args.get("status").and_then(Value::as_str);
                if let Some(s) = status {
                    if !["pending", "sending", "sent", "retry", "failed", "cancelled"].contains(&s) {
                        return Err(AppError::Validation(format!(
                            "unknown status `{s}` (pending|sending|sent|retry|failed|cancelled)"
                        )));
                    }
                }
                let limit = ctx
                    .args
                    .get("limit")
                    .and_then(Value::as_i64)
                    .unwrap_or(DEFAULT_LIST_LIMIT)
                    .clamp(1, 500);
                let entries = self
                    .repo
                    .list(workspace, status, limit)
                    .await
                    .map_err(outbox_err)?;
                Ok(json!({
                    "workspace": workspace,
                    "entries": entries.iter().map(entry_json).collect::<Vec<_>>(),
                }))
            }
            "email.cancel" => {
                let id = str_arg(&ctx.args, "id")?;
                self.repo.cancel(id, workspace).await.map_err(outbox_err)?;
                Ok(json!({ "ok": true, "id": id, "cancelled": true }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
