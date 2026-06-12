//! Workflow notifications (`hq-b7f7c1`, epic hq-bb12a2): the observer that keeps the human
//! operator informed of the autonomous loop's milestones without tailing daemon logs.
//!
//! The notification *infrastructure* already exists — `notify.send` (mcp/notify.rs) inserts into
//! `public.notifications` and appends `notification.created.v1` to the event log, and gt-web's
//! NotificationBell renders the unread badge/panel from exactly those two surfaces. What was
//! missing is an *emitter* on the workflow: dispatch, merge-landed and merge-failed all happened
//! silently. [`WorkflowNotifyPlugin`] is that emitter: registered by the bin on the same hub
//! relay as the polecat supervisor and the git-merge edge, it mirrors three hub events into
//! operator notifications:
//!
//! - `scheduling.dispatched.v1` → `info` — a polecat was assigned to the bead.
//! - `merge.merged.v1`          → `info` — the bead's work landed on main (short sha).
//! - `merge.failed.v1`          → `alert` — the merge failed, with the reason (actionable from
//!   the bell, which can open a tracker issue from a notification).
//!
//! Everything is best-effort: a PG outage or a log-append failure is logged and never breaks the
//! relay — notifications are observational, the workflow itself is the source of truth.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::PgPool;

use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope, EventKind};
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
use gt_scheduling::SchedEvent;

use crate::mcp::eventlog::EventLog;

/// The workspace whose SSE stream is the unscoped (`None`) log scope — mirrors
/// `mcp/notify.rs::DEFAULT_WORKSPACE` so the bell's feed sees both writers the same way.
const DEFAULT_WORKSPACE: &str = "default";

/// The `from_role` stamped on workflow notifications: the daemon itself is the sender.
const FROM_ROLE: &str = "orchd";

/// What a hub event renders to before any I/O happens. Pure, so the mapping is unit-testable
/// without Postgres.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationDraft {
    pub title: String,
    pub body: String,
    /// One of the `notify.send` kinds: `info` | `alert` (`decision` is never emitted here —
    /// the workflow informs, it does not ask).
    pub kind: &'static str,
}

/// Render a hub record into the operator notification it warrants, or `None` for the (vast
/// majority of) events that warrant none. Decode failures yield `None` — a malformed payload is
/// the producer's bug, not a reason to error the relay.
pub fn draft_for(record: &EventRecord) -> Option<NotificationDraft> {
    match record.kind.as_str() {
        "scheduling.dispatched.v1" => {
            let SchedEvent::Dispatched { bead, .. } = record.decode::<SchedEvent>().ok()? else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Polecat asignado a {bead}"),
                body: format!("El scheduler despachó {bead}; un polecat autónomo lo está trabajando."),
                kind: "info",
            })
        }
        "merge.merged.v1" => {
            let MergeEvent::Merged { bead, sha } = record.decode::<MergeEvent>().ok()? else {
                return None;
            };
            let short = if sha.len() > 7 { &sha[..7] } else { sha.as_str() };
            Some(NotificationDraft {
                title: format!("{bead} entregado a main @ {short}"),
                body: format!("El refinery aterrizó la rama {bead} en main ({sha})."),
                kind: "info",
            })
        }
        "merge.failed.v1" => {
            let MergeEvent::Failed { bead, reason } = record.decode::<MergeEvent>().ok()? else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Merge de {bead} falló"),
                body: reason,
                kind: "alert",
            })
        }
        _ => None,
    }
}

/// The SSE half of the write, mirroring `mcp/notify.rs`: same wire shape, same kind, so the
/// bell's `notification.created.v1` listener treats both writers identically.
#[derive(Debug, Serialize)]
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

/// The workflow-notification observer (`hq-b7f7c1`). Holds the same two write surfaces
/// `notify.send` uses: the `public.notifications` pool and the event log for SSE. Registered by
/// the bin only when `GT_PG_URL` is configured.
pub struct WorkflowNotifyPlugin {
    pool: PgPool,
    log: Arc<EventLog>,
    workspace: String,
    /// Failure-email recipient (`hq-80e92c`, phase 2): when set, a `merge.failed.v1` ALSO
    /// enqueues an email into the existing `email_outbox` (drained by the scheduling daemon's
    /// EmailTransport). `None` ⇒ bell only. Only failures email — the bell suffices for the
    /// happy-path milestones.
    failure_email: Option<String>,
}

impl WorkflowNotifyPlugin {
    pub fn new(pool: PgPool, log: Arc<EventLog>, workspace: impl Into<String>) -> Self {
        Self {
            pool,
            log,
            workspace: workspace.into(),
            failure_email: None,
        }
    }

    /// Also email `recipient` on every merge failure (`hq-80e92c`, `GT_NOTIFY_EMAIL`). Empty ⇒
    /// unchanged (bell only).
    pub fn with_failure_email(mut self, recipient: impl Into<String>) -> Self {
        let recipient = recipient.into();
        self.failure_email = if recipient.trim().is_empty() {
            None
        } else {
            Some(recipient)
        };
        self
    }
}

#[async_trait]
impl Plugin for WorkflowNotifyPlugin {
    fn name(&self) -> &'static str {
        "workflow-notify"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        let Some(draft) = draft_for(record) else {
            return Ok(());
        };
        // Same INSERT as notify.send — the bell's REST list reads this row.
        let row: Result<(String,), _> = sqlx::query_as(
            "INSERT INTO notifications (workspace, from_role, title, body, kind) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
        )
        .bind(&self.workspace)
        .bind(FROM_ROLE)
        .bind(&draft.title)
        .bind(&draft.body)
        .bind(draft.kind)
        .fetch_one(&self.pool)
        .await;
        let id = match row {
            Ok((id,)) => id,
            Err(e) => {
                eprintln!("[workflow-notify] insert failed for '{}': {e}", draft.title);
                return Ok(()); // best-effort: never break the relay over a notification
            }
        };
        // Same SSE append as notify.send — the bell's live listener reacts to this.
        let (title_for_email, body_for_email) = (draft.title.clone(), draft.body.clone());
        let ev = NotificationCreated {
            id,
            workspace: self.workspace.clone(),
            from_role: FROM_ROLE.to_string(),
            title: draft.title,
            body: draft.body,
            kind: draft.kind.to_string(),
        };
        let ws_opt = if self.workspace == DEFAULT_WORKSPACE {
            None
        } else {
            Some(self.workspace.as_str())
        };
        if let Err(e) = self.log.append(ws_opt, ev) {
            eprintln!("[workflow-notify] SSE emit failed: {e}");
        }
        // Phase 2 (hq-80e92c): a merge FAILURE also lands in the operator's inbox via the
        // existing email_outbox (the scheduling drain + EmailTransport deliver it). Only the
        // alert kind emails — dispatched/merged stay bell-only. Best-effort like the rest.
        if draft.kind == "alert" {
            if let Some(recipient) = &self.failure_email {
                use gt_store_pg::{EmailOutboxRepository, NewEmail, PgEmailOutbox};
                let outbox = PgEmailOutbox::new(self.pool.clone());
                let new = NewEmail {
                    id: ulid::Ulid::new().to_string(),
                    workspace: self.workspace.clone(),
                    recipient: recipient.clone(),
                    subject: format!("[gt] {}", title_for_email),
                    body: body_for_email,
                    template_ref: None,
                    send_at: None, // now
                    created_by: FROM_ROLE.to_string(),
                };
                if let Err(e) = outbox.enqueue(new).await {
                    eprintln!("[workflow-notify] failure email enqueue failed: {e}");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record<E: EventKind + Serialize>(event: E) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(event)).expect("encode")
    }

    #[test]
    fn dispatched_renders_an_info_naming_the_bead() {
        let d = draft_for(&record(SchedEvent::Dispatched {
            bead: "gtweb-907ee6".into(),
            worker: "w1".into(),
        }))
        .expect("dispatched warrants a notification");
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("gtweb-907ee6"));
    }

    #[test]
    fn merged_renders_an_info_with_short_sha() {
        let d = draft_for(&record(MergeEvent::Merged {
            bead: "gtweb-907ee6".into(),
            sha: "002e8d025d47e8c4d476bb070aa204c3f9d44f01".into(),
        }))
        .expect("merged warrants a notification");
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("gtweb-907ee6"));
        assert!(d.title.contains("002e8d0"), "title carries the SHORT sha: {}", d.title);
        assert!(!d.title.contains("002e8d025d47"), "not the full sha in the title");
    }

    #[test]
    fn failed_renders_an_alert_carrying_the_reason() {
        let d = draft_for(&record(MergeEvent::Failed {
            bead: "gtweb-1".into(),
            reason: "rebase onto origin/main conflicted: f".into(),
        }))
        .expect("failed warrants a notification");
        assert_eq!(d.kind, "alert");
        assert!(d.title.contains("gtweb-1"));
        assert!(d.body.contains("conflicted"));
    }

    #[test]
    fn other_workflow_events_warrant_no_notification() {
        // Enqueue / Started / Ready are internal mechanics — only the three milestones notify.
        assert!(draft_for(&record(SchedEvent::Enqueue {
            bead: "b".into(),
            priority: 1
        }))
        .is_none());
        assert!(draft_for(&record(MergeEvent::Started { bead: "b".into() })).is_none());
        assert!(draft_for(&record(MergeEvent::Ready {
            bead: "b".into(),
            branch: "b".into(),
            channel_msg_id: "m".into()
        }))
        .is_none());
    }
}
