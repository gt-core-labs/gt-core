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

use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, opt, req, str_arg};
use crate::notify_bell::{ring_bell, BellWrite, DEFAULT_DEDUP_WINDOW_SECS};
use crate::notify_kind::NotificationKind;

const DEFAULT_FROM_ROLE: &str = "mayor";
const DEFAULT_WORKSPACE: &str = "default";

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
        vec![
            descriptor(
                "notify.send",
                "Send a notification to the human operator dashboard. Use for decisions requiring \
                 human input, alerts on failures, or FYI updates. kind is a CLOSED set \
                 (decision|info|alert). Re-sends of the same finding dedup instead of re-paging: \
                 pass `fingerprint` to name the finding explicitly, or omit it and one is derived \
                 from (from_role, kind, title) — repeats inside the window (default 24h, override \
                 with `dedup_window_secs`) bump a counter on the existing row. Pass fingerprint:\"\" \
                 to force a one-shot row (never deduped).",
                &[
                    req("title", "string"),
                    opt("body", "string"),
                    opt("kind", "string"),
                    opt("from_role", "string"),
                    opt("fingerprint", "string"),
                    opt("dedup_window_secs", "integer"),
                ],
            ),
            descriptor(
                "notify.list",
                "List operator notifications for the workspace, newest activity first. \
                 `state` narrows to new|acked|resolved (omit for every state); `limit` caps \
                 rows (default 50). A daemon should consult this before re-alerting: a finding \
                 already acked/resolved needs no new page. Read-only.",
                &[opt("state", "string"), opt("limit", "integer")],
            ),
            descriptor(
                "notify.ack",
                "Acknowledge a notification (state new → acked): the operator has seen it; \
                 dedup keeps absorbing re-emissions of its fingerprint. Idempotent on an \
                 already-acked row; a resolved row stays resolved (error).",
                &[req("id", "string")],
            ),
            descriptor(
                "notify.resolve",
                "Resolve a notification (→ resolved): the underlying finding is fixed/cleared. \
                 Terminal and idempotent. The emitter+fingerprint stops being absorbed only \
                 after the dedup window expires, so a genuinely recurring finding re-pages.",
                &[req("id", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "notify.send" => {
                let title = str_arg(&ctx.args, "title")?.trim().to_string();
                if title.is_empty() {
                    return Err(AppError::Validation("title must not be empty".into()));
                }
                let body = ctx.args.get("body").and_then(Value::as_str).unwrap_or("").to_string();
                // Closed set (gtcore-7a707a): an out-of-set kind is an EXPLICIT, listing error at
                // the frontier. The old inline check pre-dated `warning`/`escalation` senders,
                // whose writes died at the DB CHECK as an opaque 500 — callers then retried under
                // a different kind, producing the audit's double-sends.
                let kind_raw = ctx
                    .args
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or(NotificationKind::default().as_str());
                let Some(kind) = NotificationKind::parse(kind_raw) else {
                    return Err(AppError::Validation(format!(
                        "unknown kind `{kind_raw}` (expected one of: {})",
                        NotificationKind::allowed()
                    )));
                };
                let from_role = ctx
                    .args
                    .get("from_role")
                    .and_then(Value::as_str)
                    .unwrap_or(DEFAULT_FROM_ROLE)
                    .to_string();
                let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE).to_string();

                // Dedup key (gtcore-7a707a): an explicit `fingerprint` wins; an EMPTY explicit
                // one opts out (one-shot); absent derives from (from_role, kind, title) so the
                // daemons that re-emit the same finding verbatim on every tick — the audit's 13
                // identical rows — collapse without any caller change.
                let explicit = ctx.args.get("fingerprint").and_then(Value::as_str);
                let derived;
                let fingerprint: Option<&str> = match explicit {
                    Some("") => None,
                    Some(f) => Some(f),
                    None => {
                        derived = derive_fingerprint(&from_role, kind, &title);
                        Some(derived.as_str())
                    }
                };
                let window_secs = ctx
                    .args
                    .get("dedup_window_secs")
                    .and_then(Value::as_i64)
                    .unwrap_or(DEFAULT_DEDUP_WINDOW_SECS);

                let outcome = ring_bell(
                    &self.pool,
                    &self.log,
                    BellWrite {
                        workspace: &workspace,
                        from_role: &from_role,
                        title: &title,
                        body: &body,
                        kind,
                        fingerprint,
                    },
                    window_secs,
                )
                .await?;
                Ok(json!({
                    "ok": true,
                    "id": outcome.id,
                    "deduped": outcome.deduped,
                    "count": outcome.count,
                }))
            }
            // gtcore-73d4ab: the read/lifecycle half that closes the emitter↔operator loop —
            // without it agents could only ever ADD to the bell, never see or settle it.
            "notify.list" => {
                let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE).to_string();
                let state = match ctx.args.get("state").and_then(Value::as_str) {
                    None | Some("") => None,
                    Some(s @ ("new" | "acked" | "resolved")) => Some(s.to_string()),
                    Some(other) => {
                        return Err(AppError::Validation(format!(
                            "unknown state `{other}` (expected one of: new, acked, resolved)"
                        )))
                    }
                };
                let limit = ctx
                    .args
                    .get("limit")
                    .and_then(Value::as_i64)
                    .unwrap_or(50)
                    .clamp(1, 500);
                let rows: Vec<NotificationRow> = sqlx::query_as(
                    "SELECT id::text, from_role, title, body, kind, state, count, fingerprint, \
                            to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, \
                            to_char(last_seen_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS last_seen_at \
                     FROM notifications \
                     WHERE workspace = $1 AND ($2::text IS NULL OR state = $2) \
                     ORDER BY last_seen_at DESC LIMIT $3",
                )
                .bind(&workspace)
                .bind(&state)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| AppError::Other(e.to_string()))?;
                Ok(json!({ "rows": rows, "total": rows.len() }))
            }
            "notify.ack" | "notify.resolve" => {
                let id = str_arg(&ctx.args, "id")?.trim().to_string();
                if id.is_empty() {
                    return Err(AppError::Validation("id must not be empty".into()));
                }
                let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE).to_string();
                // Legal moves only: ack settles a NEW row (idempotent on acked; never
                // un-resolves), resolve is terminal from either live state (idempotent).
                // `read_at` keeps mirroring ack for the legacy dashboard badge.
                let query = if tool == "notify.ack" {
                    "UPDATE notifications \
                     SET state = 'acked', read_at = coalesce(read_at, now()) \
                     WHERE id = $1::uuid AND workspace = $2 AND state <> 'resolved' \
                     RETURNING state"
                } else {
                    "UPDATE notifications SET state = 'resolved' \
                     WHERE id = $1::uuid AND workspace = $2 \
                     RETURNING state"
                };
                let updated: Option<(String,)> = sqlx::query_as(query)
                    .bind(&id)
                    .bind(&workspace)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| AppError::Other(e.to_string()))?;
                match updated {
                    Some((state,)) => Ok(json!({ "ok": true, "id": id, "state": state })),
                    None if tool == "notify.ack" => {
                        // Distinguish "gone" from "already resolved" for a usable error.
                        let exists: Option<(String,)> = sqlx::query_as(
                            "SELECT state FROM notifications WHERE id = $1::uuid AND workspace = $2",
                        )
                        .bind(&id)
                        .bind(&workspace)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| AppError::Other(e.to_string()))?;
                        match exists {
                            Some(_) => Err(AppError::Validation(format!(
                                "notification {id} is resolved — ack would regress it"
                            ))),
                            None => Err(AppError::NotFound(format!("notification {id}"))),
                        }
                    }
                    None => Err(AppError::NotFound(format!("notification {id}"))),
                }
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// One `notify.list` row, straight off the store. String timestamps (UTC, RFC3339-ish) so the
/// JSON needs no chrono round-trip; `fingerprint` is `None` for one-shot rows.
#[derive(Debug, serde::Serialize, sqlx::FromRow)]
struct NotificationRow {
    id: String,
    from_role: String,
    title: String,
    body: String,
    kind: String,
    state: String,
    count: i32,
    fingerprint: Option<String>,
    created_at: String,
    last_seen_at: String,
}

/// Stable derived dedup key for a caller that names no fingerprint: the exact re-emission shape
/// the audit found is the SAME (from_role, kind, title) sent on every daemon tick, so that triple
/// IS the finding's identity. Hex-encoded FNV-1a — collision-tolerant (a collision only merges
/// two bell rows), no new dependency.
fn derive_fingerprint(from_role: &str, kind: NotificationKind, title: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for chunk in [from_role.as_bytes(), &[0u8], kind.as_str().as_bytes(), &[0u8], title.as_bytes()]
    {
        for b in chunk {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    format!("auto-{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_fingerprint_is_stable_and_field_sensitive() {
        // The audit's re-emission shape: identical (from_role, kind, title) every tick must
        // derive the SAME key so the rows collapse…
        let a = derive_fingerprint("deacon", NotificationKind::Alert, "stuck slot gtcore-08a8be");
        let b = derive_fingerprint("deacon", NotificationKind::Alert, "stuck slot gtcore-08a8be");
        assert_eq!(a, b, "same finding ⇒ same key");
        assert!(a.starts_with("auto-"), "derived keys are namespaced: {a}");

        // …while any differing field is a different finding (separator-fenced, so the
        // concatenation ambiguity `ab|c` vs `a|bc` cannot collide either).
        let other_role = derive_fingerprint("witness", NotificationKind::Alert, "stuck slot gtcore-08a8be");
        let other_kind = derive_fingerprint("deacon", NotificationKind::Info, "stuck slot gtcore-08a8be");
        let other_title = derive_fingerprint("deacon", NotificationKind::Alert, "another finding");
        assert_ne!(a, other_role);
        assert_ne!(a, other_kind);
        assert_ne!(a, other_title);
    }
}
