//! `notify.*` domain dispatch — operator notification channel.
//!
//! Agents (primarily `mayor`) call `notify.send` to push a notification into the
//! human operator's dashboard. The notification is written to the public-schema
//! `notifications` table and an SSE event (`notification.created.v1`) is emitted
//! so any open browser session sees the badge update instantly.
//!
//! The REST surface (`POST /api/v1/notifications`) is the HTTP twin; both paths
//! write the same row and emit the same SSE event.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;

use gt_events::EventKind;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, opt, req, str_arg};

const DEFAULT_KIND: &str = "decision";
const DEFAULT_FROM_ROLE: &str = "mayor";
const DEFAULT_WORKSPACE: &str = "default";

/// SSE event mirroring the REST surface's `notification.created.v1`.
struct NotificationCreated {
    id: String,
    workspace: String,
    from_role: String,
    title: String,
    body: String,
    kind: String,
}

impl serde::Serialize for NotificationCreated {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("NotificationCreated", 6)?;
        st.serialize_field("id", &self.id)?;
        st.serialize_field("workspace", &self.workspace)?;
        st.serialize_field("from_role", &self.from_role)?;
        st.serialize_field("title", &self.title)?;
        st.serialize_field("body", &self.body)?;
        st.serialize_field("kind", &self.kind)?;
        st.end()
    }
}

impl EventKind for NotificationCreated {
    fn kind(&self) -> &'static str {
        "notification.created.v1"
    }
}

/// PG-backed handler for the `notify.*` tool namespace.
pub struct NotifyHandler {
    pool: PgPool,
    log: Arc<EventLog>,
}

impl NotifyHandler {
    pub fn new(pool: PgPool, log: Arc<EventLog>) -> Self {
        Self { pool, log }
    }
}

#[async_trait]
impl DomainHandler for NotifyHandler {
    fn namespace(&self) -> &'static str {
        "notify"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![descriptor(
            "notify.send",
            "Send a notification to the human operator dashboard. \
             Use for decisions requiring human input, alerts on failures, or FYI updates.",
            &[
                req("title", "string"),
                opt("body", "string"),
                opt("kind", "string"),
                opt("from_role", "string"),
            ],
        )]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "notify.send" => {
                let title = str_arg(&ctx.args, "title")?.trim().to_string();
                if title.is_empty() {
                    return Err(AppError::Validation("title must not be empty".into()));
                }
                let body = ctx.args.get("body").and_then(Value::as_str).unwrap_or("").to_string();
                let kind = ctx.args.get("kind").and_then(Value::as_str).unwrap_or(DEFAULT_KIND);
                if !["decision", "info", "alert"].contains(&kind) {
                    return Err(AppError::Validation(
                        "kind must be one of: decision, info, alert".into(),
                    ));
                }
                let from_role = ctx
                    .args
                    .get("from_role")
                    .and_then(Value::as_str)
                    .unwrap_or(DEFAULT_FROM_ROLE)
                    .to_string();
                let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE).to_string();

                let row: Result<(String,), _> = sqlx::query_as(
                    "INSERT INTO notifications (workspace, from_role, title, body, kind) \
                     VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
                )
                .bind(&workspace)
                .bind(&from_role)
                .bind(&title)
                .bind(&body)
                .bind(kind)
                .fetch_one(&self.pool)
                .await;

                match row {
                    Ok((id,)) => {
                        let ev = NotificationCreated {
                            id: id.clone(),
                            workspace: workspace.clone(),
                            from_role: from_role.clone(),
                            title: title.clone(),
                            body: body.clone(),
                            kind: kind.to_string(),
                        };
                        let ws_opt = if workspace == DEFAULT_WORKSPACE {
                            None
                        } else {
                            Some(workspace.as_str())
                        };
                        if let Err(e) = self.log.append(ws_opt, ev) {
                            eprintln!("[notify.send] SSE emit failed: {e}");
                        }
                        Ok(json!({ "ok": true, "id": id }))
                    }
                    Err(e) => Err(AppError::Other(e.to_string())),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
