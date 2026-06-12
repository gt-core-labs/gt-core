//! Email-outbox drain daemon (hq-f24599, epic hq-56b5ee).
//!
//! The scheduling half of the outbox: a low-cadence loop that claims due rows
//! (`send_at <= now`, status `pending`|`retry` — `FOR UPDATE SKIP LOCKED`, so
//! replicas stay disjoint) and pushes each through the configured
//! [`EmailTransport`] (gt-notify seam). Settlement:
//!
//! - delivered → `sent` (+ `sent_at`);
//! - transport error with attempts left → `retry`, `send_at` pushed forward by
//!   [`backoff_delay`] (exponential, capped);
//! - attempts exhausted → `failed` (+ `last_error`).
//!
//! Producers never see any of this: they only INSERT outbox rows. The SMTP
//! server being pending costs nothing — the default [`LogTransport`]
//! (`gt_notify::transport_from_env`) drains the queue today.
//!
//! [`LogTransport`]: gt_notify::LogTransport

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::types::chrono::Utc;
use sqlx::PgPool;

use gt_events::EventKind;
use gt_notify::{EmailMessage, EmailTransport};
use gt_store_pg::{EmailOutboxRepository, PgEmailOutbox};

use crate::mcp::eventlog::EventLog;

const DEFAULT_WORKSPACE: &str = "default";
const FROM_ROLE: &str = "report-digest";
/// template_ref prefix that marks a scheduled report email.
const REPORT_REF_PREFIX: &str = "report:";

/// Bell-notification emitter for the drain (hq-9643ec). Fires an `info`
/// notification when a report email is delivered and an `alert` when one
/// permanently fails — keeps operators informed without tailing logs.
///
/// Only emails whose `template_ref` starts with `"report:"` are notified;
/// merge-failure emails already have their own notification from
/// `workflow_notify`.
pub struct DrainNotifier {
    pool: PgPool,
    log: Arc<EventLog>,
}

impl DrainNotifier {
    pub fn new(pool: PgPool, log: Arc<EventLog>) -> Self {
        Self { pool, log }
    }

    /// Insert a notification row + append the SSE event. Best-effort: any
    /// error is logged and swallowed — a notification outage never kills the
    /// drain.
    async fn fire(&self, workspace: &str, title: &str, body: &str, kind: &str) {
        let row: Result<(String,), _> = sqlx::query_as(
            "INSERT INTO notifications (workspace, from_role, title, body, kind) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
        )
        .bind(workspace)
        .bind(FROM_ROLE)
        .bind(title)
        .bind(body)
        .bind(kind)
        .fetch_one(&self.pool)
        .await;

        let id = match row {
            Ok((id,)) => id,
            Err(e) => {
                eprintln!("[email-outbox] notification insert failed: {e}");
                return;
            }
        };

        #[derive(Serialize)]
        struct NotificationCreated {
            id: String,
            workspace: String,
            from_role: String,
            title: String,
            body: String,
            kind: String,
        }
        impl EventKind for NotificationCreated {
            fn kind(&self) -> &'static str {
                "notification.created.v1"
            }
        }
        let ev = NotificationCreated {
            id,
            workspace: workspace.to_string(),
            from_role: FROM_ROLE.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            kind: kind.to_string(),
        };
        let ws_opt = if workspace == DEFAULT_WORKSPACE { None } else { Some(workspace) };
        if let Err(e) = self.log.append(ws_opt, ev) {
            eprintln!("[email-outbox] notification SSE emit failed: {e}");
        }
    }
}

/// Delivery attempts before a row settles as `failed`.
pub const MAX_ATTEMPTS: i32 = 5;

/// Rows claimed per tick — bounds one tick's work; the next tick takes the rest.
const CLAIM_BATCH: i64 = 50;

/// Exponential backoff for attempt `n` (0-based prior attempts): 60s · 2^n,
/// capped at one hour.
pub fn backoff_delay(prior_attempts: i32) -> Duration {
    let exp = prior_attempts.clamp(0, 6) as u32; // 60 * 2^6 = 64 min ≈ the cap
    Duration::from_secs((60u64 << exp).min(3600))
}

/// Drain one batch of due rows through `transport`. Returns how many rows were
/// claimed (delivered or settled otherwise). Split from [`run`] so a test can
/// tick deterministically without the interval loop.
pub async fn drain_once(
    repo: &PgEmailOutbox,
    transport: &Arc<dyn EmailTransport>,
    notifier: Option<&DrainNotifier>,
) -> Result<usize, gt_store_pg::OutboxError> {
    let claimed = repo.claim_due(CLAIM_BATCH).await?;
    let n = claimed.len();
    for entry in claimed {
        let is_report = entry
            .template_ref
            .as_deref()
            .map(|r| r.starts_with(REPORT_REF_PREFIX))
            .unwrap_or(false);
        let msg = EmailMessage {
            to: entry.recipient.clone(),
            subject: entry.subject.clone(),
            body: entry.body.clone(),
        };
        // The transport is sync/blocking by design (gt-notify port discipline);
        // hop off the async worker for the actual delivery.
        let t = transport.clone();
        let outcome = tokio::task::spawn_blocking(move || t.send(&msg))
            .await
            .unwrap_or_else(|e| Err(format!("transport panicked: {e}")));
        match outcome {
            Ok(()) => {
                repo.mark_sent(&entry.id).await?;
                if is_report {
                    if let Some(n) = notifier {
                        let body = format!("{}\n→ {}", entry.subject, entry.recipient);
                        n.fire(&entry.workspace, "Reporte enviado", &body, "info").await;
                    }
                }
            }
            Err(reason) => {
                if entry.attempts + 1 >= MAX_ATTEMPTS {
                    repo.mark_failed(&entry.id, &reason).await?;
                    eprintln!(
                        "[email-outbox] {} failed permanently after {} attempts: {reason}",
                        entry.id,
                        entry.attempts + 1
                    );
                    if is_report {
                        if let Some(n) = notifier {
                            let body = format!(
                                "{}\n→ {}\n{}",
                                entry.subject, entry.recipient, reason
                            );
                            n.fire(
                                &entry.workspace,
                                "Error de envío de reporte",
                                &body,
                                "alert",
                            )
                            .await;
                        }
                    }
                } else {
                    let next = Utc::now()
                        + chrono::Duration::from_std(backoff_delay(entry.attempts))
                            .unwrap_or_else(|_| chrono::Duration::seconds(60));
                    repo.mark_retry(&entry.id, &reason, next).await?;
                }
            }
        }
    }
    Ok(n)
}

/// The daemon loop: tick every `interval`, drain a batch, log failures and keep
/// going — a transient PG error never kills the drain.
pub async fn run(
    interval: Duration,
    pool: PgPool,
    transport: Arc<dyn EmailTransport>,
    notifier: Option<DrainNotifier>,
) {
    let repo = PgEmailOutbox::new(pool);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        match drain_once(&repo, &transport, notifier.as_ref()).await {
            Ok(0) => {}
            Ok(n) => eprintln!("[email-outbox] drained {n} due email(s) via {}", transport.label()),
            Err(e) => eprintln!("[email-outbox] drain tick failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_delay(0), Duration::from_secs(60));
        assert_eq!(backoff_delay(1), Duration::from_secs(120));
        assert_eq!(backoff_delay(2), Duration::from_secs(240));
        // Capped at one hour no matter how many priors.
        assert_eq!(backoff_delay(6), Duration::from_secs(3600));
        assert_eq!(backoff_delay(60), Duration::from_secs(3600));
        // Negative (corrupt) input clamps to the floor instead of panicking.
        assert_eq!(backoff_delay(-3), Duration::from_secs(60));
    }
}
