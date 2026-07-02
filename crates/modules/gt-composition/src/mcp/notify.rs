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
        vec![descriptor(
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
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
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
