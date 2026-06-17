//! Operator escalation notifications + reminder ticker (A6, gtcore-46c9dc, epic hq-bb12a2).
//!
//! `escalate.*` (mcp/escalate.rs) is the *event-sourced lifecycle* of a human escalation: an
//! agent calls `escalate.request` when it blocks (Session Waiting → `input-required`), the
//! operator answers with `escalate.respond` (or, the A2A way, a multi-turn `tasks/send` — see
//! `a2a.rs`). What that lifecycle did NOT do was *reach the human*: the request only appended
//! `escalation.requested.v1` to the log. A blocked agent surfaced on the dashboard only if the
//! operator happened to be watching the feed.
//!
//! This module closes that gap with the same observer/ticker shape as
//! [`crate::delegation`] and [`crate::workflow_notify`]:
//!
//! 1. [`EscalationNotifyPlugin`] — a hub observer. On `escalation.requested.v1` it rings the
//!    operator bell (`public.notifications` + the `notification.created.v1` SSE the dashboard
//!    bell renders) with kind `decision`, and — when an operator address is configured — drops
//!    an email into the existing `email_outbox` (drained by the scheduling daemon's
//!    EmailTransport). Both carry a **direct link** to the escalation on the dashboard.
//!
//! 2. [`EscalationReminderTicker`] — a periodic sweep. Any escalation still `pending` longer
//!    than `reminder_secs` (and not reminded again within that window) re-pings the operator and
//!    appends `escalation.reminded.v1`, which resets its clock so the same request never spams.
//!    `reminder_secs == 0` disables reminders.
//!
//! Everything is best-effort: a PG outage or an email-enqueue failure is logged and never breaks
//! the relay or the tick. The escalation's own event log stays the source of truth — a missed
//! bell is an observability gap, not a lost decision.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::PgPool;

use gt_eventlog::EventRecord;
use gt_events::{AppError, EventKind};
use gt_plugin::Plugin;
use gt_store_dolt::AppError as StoreError;
use gt_store_pg::{EmailOutboxRepository, NewEmail, PgEmailOutbox};

use crate::mcp::escalate::{
    rfc3339_now, Escalation, EscalationEvent, EscalationRegistry, NS,
};
use crate::mcp::eventlog::EventLog;

/// The workspace whose SSE scope is the unscoped (`None`) log — mirrors
/// `mcp/notify.rs::DEFAULT_WORKSPACE` so the bell's feed sees this writer like the others.
const DEFAULT_WORKSPACE: &str = "default";

/// The `from_role` stamped on escalation notifications: the gateway/daemon asks on the blocked
/// agent's behalf, not a human (mirrors `delegation`'s and `workflow_notify`'s sender roles).
const FROM_ROLE: &str = "orchd";

/// Default reminder cadence when `GT_ESCALATION_REMINDER_SECS` is unset: 4 hours. The bead's
/// "input-required > N horas dispara recordatorio" with a sane default N.
pub const DEFAULT_REMINDER_SECS: u64 = 4 * 60 * 60;

// ── direct link ───────────────────────────────────────────────────────────────

/// Build the operator's direct link to one escalation on the dashboard. `public_url` is
/// `GT_PUBLIC_URL` (e.g. `https://gt-dev.example.com`); the dashboard reads `?escalation=<id>`
/// to open the request. Pure, so the format is unit-testable without any I/O.
pub fn dashboard_link(public_url: &str, id: &str) -> String {
    format!("{}/?escalation={}", public_url.trim_end_matches('/'), id)
}

// ── operator notifier (bell + email) ────────────────────────────────────────────

/// The SSE half of a bell write, identical in shape to `mcp/notify.rs`, `workflow_notify.rs`
/// and `delegation.rs` so the dashboard's `notification.created.v1` listener treats every
/// writer the same.
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

/// The two operator-facing surfaces, bundled so the plugin and the ticker share one writer:
/// the bell (`public.notifications` + SSE) and, optionally, the email outbox. Built only when
/// `GT_PG_URL` is configured — without Postgres there is no bell and no outbox.
#[derive(Clone)]
pub struct OperatorNotifier {
    pool: PgPool,
    log: Arc<EventLog>,
    workspace: String,
    /// Operator email (`GT_NOTIFY_EMAIL`); `None` ⇒ bell only.
    email: Option<String>,
    /// `GT_PUBLIC_URL` base for the direct link in the body.
    public_url: String,
}

impl OperatorNotifier {
    pub fn new(pool: PgPool, log: Arc<EventLog>, workspace: impl Into<String>) -> Self {
        Self {
            pool,
            log,
            workspace: workspace.into(),
            email: None,
            public_url: String::new(),
        }
    }

    /// Also email this address on every escalation/reminder. Empty ⇒ unchanged (bell only).
    pub fn with_email(mut self, recipient: impl Into<String>) -> Self {
        let recipient = recipient.into();
        self.email = (!recipient.trim().is_empty()).then_some(recipient);
        self
    }

    /// The `GT_PUBLIC_URL` base used to mint the direct link.
    pub fn with_public_url(mut self, public_url: impl Into<String>) -> Self {
        self.public_url = public_url.into();
        self
    }

    fn link(&self, id: &str) -> String {
        dashboard_link(&self.public_url, id)
    }

    /// Best-effort bell: insert the row, then mirror it to the SSE log. Never errors — a
    /// notification problem must not break a relay or a tick.
    async fn ring(&self, title: &str, body: &str, kind: &str) {
        let row: Result<(String,), _> = sqlx::query_as(
            "INSERT INTO notifications (workspace, from_role, title, body, kind) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
        )
        .bind(&self.workspace)
        .bind(FROM_ROLE)
        .bind(title)
        .bind(body)
        .bind(kind)
        .fetch_one(&self.pool)
        .await;
        let id = match row {
            Ok((id,)) => id,
            Err(e) => {
                eprintln!("[escalation-notify] bell insert failed for '{title}': {e}");
                return;
            }
        };
        let ev = NotificationCreated {
            id,
            workspace: self.workspace.clone(),
            from_role: FROM_ROLE.to_string(),
            title: title.to_string(),
            body: body.to_string(),
            kind: kind.to_string(),
        };
        let ws_opt = (self.workspace != DEFAULT_WORKSPACE).then_some(self.workspace.as_str());
        if let Err(e) = self.log.append(ws_opt, ev) {
            eprintln!("[escalation-notify] bell SSE emit failed: {e}");
        }
    }

    /// Best-effort email into the existing outbox (the scheduling drain + EmailTransport
    /// deliver it). No-op when no operator address is configured.
    async fn enqueue_email(&self, subject: &str, body: &str) {
        let Some(recipient) = &self.email else {
            return;
        };
        let outbox = PgEmailOutbox::new(self.pool.clone());
        let new = NewEmail {
            id: ulid::Ulid::new().to_string(),
            workspace: self.workspace.clone(),
            recipient: recipient.clone(),
            cc: vec![],
            subject: subject.to_string(),
            body: body.to_string(),
            template_ref: None,
            send_at: None, // now
            created_by: FROM_ROLE.to_string(),
        };
        if let Err(e) = outbox.enqueue(new).await {
            eprintln!("[escalation-notify] email enqueue failed: {e}");
        }
    }
}

// ── notification rendering ──────────────────────────────────────────────────────

/// What an escalation renders to before any I/O — pure, so the wording is unit-testable
/// without Postgres. `is_reminder` distinguishes the first ping from a nag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationDraft {
    pub title: String,
    pub body: String,
    /// `decision` — an escalation asks the operator to decide (unlike `workflow_notify`, which
    /// only informs). The bell renders these in the "needs you" lane.
    pub kind: &'static str,
}

/// Render an escalation (request or reminder) into the operator notification it warrants. The
/// `link` is appended verbatim so the body is self-contained in both bell and email.
pub fn draft_for(e: &Escalation, link: &str, is_reminder: bool) -> EscalationDraft {
    let subject = e.bead.clone().unwrap_or_else(|| e.session.clone());
    let title = if is_reminder {
        format!("Escalación {subject} sigue esperando tu decisión")
    } else {
        format!("Escalación: {subject} requiere tu decisión")
    };
    let lead = if is_reminder {
        "Recordatorio: una sesión sigue bloqueada esperando tu respuesta."
    } else {
        "Una sesión se bloqueó y necesita tu decisión."
    };
    let body = format!(
        "{lead}\n\nSesión: {session}\nMotivo: {reason}\n\nResponde aquí: {link}",
        session = e.session,
        reason = e.reason,
    );
    EscalationDraft {
        title,
        body,
        kind: "decision",
    }
}

// ── request observer ────────────────────────────────────────────────────────────

/// Hub observer (A6): an `escalation.requested.v1` reaches the operator's bell + email with a
/// direct link. Registered by the bin on the same relay as the polecat supervisor and the
/// delegation callback. Built only when `GT_PG_URL` is set; without it there is no bell, so the
/// plugin is simply not registered.
pub struct EscalationNotifyPlugin {
    notifier: OperatorNotifier,
}

impl EscalationNotifyPlugin {
    pub fn new(notifier: OperatorNotifier) -> Self {
        Self { notifier }
    }
}

#[async_trait]
impl Plugin for EscalationNotifyPlugin {
    fn name(&self) -> &'static str {
        "escalation-notify"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        // Cheap kind gate before decoding.
        if record.kind != "escalation.requested.v1" {
            return Ok(());
        }
        let Ok(EscalationEvent::Requested {
            id,
            session,
            bead,
            reason,
            ..
        }) = record.decode::<EscalationEvent>()
        else {
            return Ok(()); // a foreign/malformed payload is the producer's bug, not ours
        };
        // Build a minimal projection for the renderer (the request carries everything we need).
        let view = Escalation {
            id: id.clone(),
            session,
            bead,
            reason,
            mode: Default::default(),
            state: crate::mcp::escalate::EscalationState::Pending,
            response: None,
            approved: None,
            at: String::new(),
            reminded_at: None,
        };
        let link = self.notifier.link(&id);
        let draft = draft_for(&view, &link, false);
        self.notifier.ring(&draft.title, &draft.body, draft.kind).await;
        self.notifier
            .enqueue_email(&format!("[gt] {}", draft.title), &draft.body)
            .await;
        eprintln!("[escalation-notify] operator notified for escalation {id}");
        Ok(())
    }
}

// ── reminder ticker ───────────────────────────────────────────────────────────

/// Seconds elapsed since an RFC3339 stamp, or `None` if it cannot be parsed (a pre-A6 request
/// with an empty `at` reads as "age unknown" → never reminded, the safe default).
fn elapsed_secs(at: &str) -> Option<i64> {
    use time::{format_description::well_known::Rfc3339, OffsetDateTime};
    let started = OffsetDateTime::parse(at, &Rfc3339).ok()?;
    Some((OffsetDateTime::now_utc() - started).whole_seconds())
}

/// Decide whether one pending escalation is due for a (re-)reminder, measuring from the most
/// recent of `reminded_at`/`at`. Pure → the threshold logic is unit-testable. `reminder_secs == 0`
/// disables reminders entirely.
pub fn is_due(e: &Escalation, reminder_secs: u64) -> bool {
    if reminder_secs == 0 {
        return false;
    }
    let clock = e.reminded_at.as_deref().unwrap_or(e.at.as_str());
    elapsed_secs(clock).is_some_and(|s| s >= 0 && (s as u64) >= reminder_secs)
}

/// Periodic sweep that re-pings the operator about escalations still pending past
/// `reminder_secs`. Driven by an interval loop in `gt-orch-server` (alongside the delegation
/// timeout sweep). Holds the same notifier the request observer does.
pub struct EscalationReminderTicker {
    log: Arc<EventLog>,
    workspace: Option<String>,
    notifier: OperatorNotifier,
    reminder_secs: u64,
}

impl EscalationReminderTicker {
    pub fn new(
        log: Arc<EventLog>,
        workspace: Option<String>,
        notifier: OperatorNotifier,
        reminder_secs: u64,
    ) -> Self {
        Self {
            log,
            workspace,
            notifier,
            reminder_secs,
        }
    }

    fn registry(&self) -> Result<EscalationRegistry, StoreError> {
        self.log.replay_domain(
            self.workspace.as_deref(),
            NS,
            EscalationRegistry::default(),
            EscalationRegistry::apply,
        )
    }

    /// One sweep: re-ping every pending escalation past its reminder window and record the
    /// reminder so the clock resets. Returns the number reminded (for logging/tests).
    /// Best-effort — a single append/notify failure is logged and the sweep continues.
    pub async fn tick(&self) -> usize {
        if self.reminder_secs == 0 {
            return 0;
        }
        let reg = match self.registry() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[escalation-reminder] registry replay failed: {e}");
                return 0;
            }
        };
        let due: Vec<Escalation> = reg
            .pending()
            .into_iter()
            .filter(|e| is_due(e, self.reminder_secs))
            .cloned()
            .collect();

        let mut reminded = 0;
        for e in due {
            let link = self.notifier.link(&e.id);
            let draft = draft_for(&e, &link, true);
            self.notifier.ring(&draft.title, &draft.body, draft.kind).await;
            self.notifier
                .enqueue_email(&format!("[gt] {}", draft.title), &draft.body)
                .await;
            // Reset the clock so the next sweep waits another full window.
            if let Err(err) = self.log.append(
                self.workspace.as_deref(),
                EscalationEvent::Reminded {
                    id: e.id.clone(),
                    at: rfc3339_now(),
                },
            ) {
                eprintln!("[escalation-reminder] reminded append failed for {}: {err}", e.id);
                continue;
            }
            eprintln!("[escalation-reminder] re-pinged operator for escalation {}", e.id);
            reminded += 1;
        }
        reminded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::escalate::{EscalationMode, EscalationState};

    fn escalation(at: &str, reminded_at: Option<&str>) -> Escalation {
        Escalation {
            id: "esc-1".into(),
            session: "gtcore-abc".into(),
            bead: Some("gtcore-abc".into()),
            reason: "destructive migration".into(),
            mode: EscalationMode::Kill,
            state: EscalationState::Pending,
            response: None,
            approved: None,
            at: at.into(),
            reminded_at: reminded_at.map(String::from),
        }
    }

    #[test]
    fn link_is_a_clean_dashboard_url() {
        assert_eq!(
            dashboard_link("https://gt.example.com/", "esc-9"),
            "https://gt.example.com/?escalation=esc-9"
        );
        // Idempotent on a base without a trailing slash.
        assert_eq!(
            dashboard_link("https://gt.example.com", "esc-9"),
            "https://gt.example.com/?escalation=esc-9"
        );
    }

    #[test]
    fn request_draft_asks_for_a_decision_and_carries_the_link() {
        let e = escalation("2026-06-15T00:00:00Z", None);
        let d = draft_for(&e, "https://gt/?escalation=esc-1", false);
        assert_eq!(d.kind, "decision");
        assert!(d.title.contains("gtcore-abc"));
        assert!(d.body.contains("destructive migration"));
        assert!(d.body.contains("https://gt/?escalation=esc-1"));
        assert!(!d.title.to_lowercase().contains("recordatorio"));
    }

    #[test]
    fn reminder_draft_is_phrased_as_a_nag() {
        let e = escalation("2026-06-15T00:00:00Z", None);
        let d = draft_for(&e, "https://gt/?escalation=esc-1", true);
        assert!(d.title.contains("sigue esperando"));
        assert!(d.body.contains("Recordatorio"));
    }

    #[test]
    fn draft_falls_back_to_session_when_no_bead() {
        let mut e = escalation("2026-06-15T00:00:00Z", None);
        e.bead = None;
        let d = draft_for(&e, "link", false);
        assert!(d.title.contains("gtcore-abc"), "session names the request: {}", d.title);
    }

    #[test]
    fn fresh_request_is_not_yet_due() {
        let e = escalation(&rfc3339_now(), None);
        assert!(!is_due(&e, 3600), "a just-created escalation must not reminder-fire");
    }

    #[test]
    fn old_request_is_due() {
        let e = escalation("2000-01-01T00:00:00Z", None);
        assert!(is_due(&e, 3600), "a request older than the window is due");
    }

    #[test]
    fn recent_reminder_suppresses_the_next() {
        // Old request, but reminded just now → not due again until another window passes.
        let e = escalation("2000-01-01T00:00:00Z", Some(&rfc3339_now()));
        assert!(!is_due(&e, 3600), "a fresh reminder resets the clock");
    }

    #[test]
    fn zero_window_disables_reminders() {
        let e = escalation("2000-01-01T00:00:00Z", None);
        assert!(!is_due(&e, 0), "reminder_secs=0 disables reminders");
    }

    #[test]
    fn unparseable_age_is_never_due() {
        // Pre-A6 requests carry an empty `at`; they must not reminder-fire.
        let e = escalation("", None);
        assert!(!is_due(&e, 3600));
    }
}
