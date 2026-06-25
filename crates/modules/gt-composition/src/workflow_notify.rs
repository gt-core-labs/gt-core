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
//! - `scheduling.dispatched.v1`  → `info`  — a polecat was assigned to the bead.
//! - `merge.merged.v1`           → `info`  — the bead's work landed on main (short sha).
//! - `merge.failed.v1`           → `alert` — the merge failed, with the reason (actionable from
//!   the bell, which can open a tracker issue from a notification).
//!
//! `gtcore-7e19fe` added the operational events that previously only reached `eprintln`, so the
//! operator no longer has to tail daemon logs to learn the autonomous loop is stuck. `gtcore-bc7dd2`
//! then corrected the quota mapping: the per-account predictive/reactive events
//! (`quota.block_predicted` / `quota.account_limited`) are ROUTINE — rotation self-heals by moving
//! the keychain pointer to a healthy account — so notifying (and emailing!) on each one was exactly
//! the noise the AC forbids. What the operator actually needs is the high-level *saturation* fact:
//! every account is exhausted at once, which is its own event. The operational set is therefore:
//!
//! - `quota.all_exhausted.v1`    → `alert` — EVERY account is blocked; the in-flight polecats were
//!   SIGSTOP'd to preserve their context. No capacity remains — the operator must add/unblock an
//!   account. This is the "todas las cuentas bloqueadas" signal the bead calls out.
//! - `quota.account_recovered.v1`→ `info`  — an exhausted account recovered and the paused polecats
//!   resumed (SIGCONT); closes the all-exhausted loop in the bell without paging.
//! - `patrol.lease-expired.v1`   → `alert` — an agent crashed; the patrol freed its claim.
//! - `polecat.sling-skipped.v1`  → `info`  — the pool/host cap is reached; the bead waits (bell-only
//!   backpressure — routine under load, deliberately off the email path).
//! - `polecat.sling-failed.v1`   → `info`  — `spawn_tmux` errored; the slot freed but the bead made
//!   no progress.
//!
//! Routine rotations deliberately warrant NO notification — they self-heal and would only be noise
//! (gtcore-bc7dd2 AC): `quota.block_predicted`, `quota.account_limited`, `quota.rotated`,
//! `quota.soft_drain` and a single-account `quota.blocked` all stay silent; only the aggregate
//! all-exhausted fact (and its recovery) reach the bell.
//!
//! Severity follows the bead's AC: the capacity faults (all-exhausted/lease) are `alert` and ride
//! the optional `GT_NOTIFY_EMAIL` path (like `merge.failed`); recovery and the sling outcomes are
//! `info` (bell).
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
use gt_patrol::PatrolEvent;
use gt_plugin::Plugin;
use gt_quota::QuotaEvent;
use gt_scheduling::SchedEvent;

use crate::mcp::eventlog::EventLog;
use crate::polecat_event::PolecatEvent;

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
        // A merge failed — in CI-gated mode this is the PR's CI going red (gtcore-3a1bd4). It is now
        // auto-recovered: the bead is re-slung to fix and re-push, up to the retry cap. So a single
        // `merge.failed` is no longer the operator's call to action — it's routine, a bell (`info`),
        // not an alert. The actionable escalation is `polecat.ci-retries-exhausted.v1` (below), which
        // fires only when the auto-retry loop gives up. This keeps the loop self-healing and quiet
        // (the gtcore-bc7dd2 "no noise" principle) while still surfacing the give-up to a human.
        "merge.failed.v1" => {
            let MergeEvent::Failed { bead, reason } = record.decode::<MergeEvent>().ok()? else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("CI de {bead} falló — reintentando"),
                body: reason,
                kind: "info",
            })
        }
        // Every claude account is exhausted at once (gtcore-6f449f): a rotation away from `account`
        // found no healthy alternative, so the in-flight polecats were SIGSTOP'd to preserve their
        // context instead of dying against the rate limit. This is the high-level "todas las cuentas
        // bloqueadas" fact the operator MUST act on (add/unblock an account) — an alert, bell + email.
        // The routine per-account predictive (`block_predicted`) / reactive (`account_limited`)
        // rotations that precede it self-heal and stay silent, so this is signal, not noise
        // (gtcore-bc7dd2).
        "quota.all_exhausted.v1" => {
            let QuotaEvent::AllExhausted {
                account,
                paused_sessions,
                ..
            } = record.decode::<QuotaEvent>().ok()?
            else {
                return None;
            };
            let paused = paused_sessions.len();
            Some(NotificationDraft {
                title: "Todas las cuentas de quota bloqueadas".to_string(),
                body: format!(
                    "La rotación no encontró ninguna cuenta sana al agotarse {account}; {paused} polecat(s) en vuelo quedaron suspendidos (SIGSTOP) para preservar su contexto. No hay capacidad — añade o desbloquea una cuenta."
                ),
                kind: "alert",
            })
        }
        // A previously-exhausted account recovered (gtcore-6f449f): the paused polecats resumed
        // (SIGCONT) and continue from where they froze. A bell (`info`) that closes the all-exhausted
        // loop without paging — the operator sees the saturation cleared on its own.
        "quota.account_recovered.v1" => {
            let QuotaEvent::AccountRecovered {
                account,
                resumed_sessions,
                ..
            } = record.decode::<QuotaEvent>().ok()?
            else {
                return None;
            };
            let resumed = resumed_sessions.len();
            Some(NotificationDraft {
                title: format!("Cuenta {account} recuperada"),
                body: format!(
                    "{account} volvió a estado sano; {resumed} polecat(s) suspendidos por agotamiento total se reanudaron (SIGCONT) y continúan desde donde quedaron."
                ),
                kind: "info",
            })
        }
        // An agent crashed without closing its session: the patrol detected the stale lease and
        // released the claim (gtcore-7e19fe). The bead is re-enqueued, but the crash is worth a look.
        "patrol.lease-expired.v1" => {
            let PatrolEvent::LeaseExpired { bead, worker, .. } =
                record.decode::<PatrolEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Agente {bead} crash — lease expirado"),
                body: format!(
                    "El patrol liberó el claim de {bead} (worker {worker}) tras un lease expirado; se re-encolará para otro polecat."
                ),
                kind: "alert",
            })
        }
        // The supervisor refused a sling because the pool/host cap is reached (gtcore-7e19fe): the
        // bead waits in limbo until a slot frees. This is recoverable backpressure — capacity frees
        // as live polecats finish — so it's a bell (`info`), not an alert: a pool-full skip is
        // routine under load and must NOT page/email the operator on every occurrence.
        "polecat.sling-skipped.v1" => {
            let PolecatEvent::SlingSkipped { bead, workspace } =
                record.decode::<PolecatEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Sling de {bead} omitido: pool lleno"),
                body: format!(
                    "No hay slots libres en el workspace {workspace}; {bead} espera a que se libere capacidad."
                ),
                kind: "info",
            })
        }
        // The sling spawn errored (gtcore-7e19fe): the slot was released but the bead made no
        // progress — needs a human (dead tmux, corrupt worktree, …). A bell (`info`) per the AC:
        // the operator sees it in the bell, but it stays off the alert/email path that the capacity
        // faults (quota/lease) ride.
        "polecat.sling-failed.v1" => {
            let PolecatEvent::SlingFailed { bead, reason } = record.decode::<PolecatEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Sling de {bead} falló"),
                body: reason,
                kind: "info",
            })
        }
        // A claude account's stored credentials are expired/dead (gtcore-bf4acd): selection skipped
        // it (validating credential expiry, not just quota status). An ALERT (bell + email) so the
        // operator re-onboards/rotates the account before more slings hit it — exactly the signal
        // that was missing during the incident where polecats were born in 401 for ~1h.
        "polecat.credential-dead.v1" => {
            let PolecatEvent::CredentialDead { account, reason } =
                record.decode::<PolecatEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Cuenta {account}: credenciales muertas"),
                body: format!(
                    "Las credenciales de {account} están {reason}; la selección la saltó para no slingear en 401. Re-onboardea o rota la cuenta."
                ),
                kind: "alert",
            })
        }
        // No account could authenticate, so the bead's sling was blocked rather than born in 401
        // (gtcore-bf4acd). An ALERT (bell + email): the autonomous loop is stuck on this bead until
        // a valid credential exists — the operator must act.
        "polecat.sling-auth-blocked.v1" => {
            let PolecatEvent::SlingAuthBlocked { bead, workspace } =
                record.decode::<PolecatEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("Sling de {bead} bloqueado: sin credenciales válidas"),
                body: format!(
                    "Ninguna cuenta del keychain tiene credenciales válidas en el workspace {workspace}; {bead} no se slingó para evitar un 401. Onboardea o refresca una cuenta."
                ),
                kind: "alert",
            })
        }
        // A bead's PR exhausted the CI-failure retry budget (gtcore-3a1bd4): the auto fix-and-re-push
        // loop ran out of retries with CI still red, so it was abandoned to avoid an infinite
        // quota-burning loop. An ALERT (bell + email): a human must read the CI log and unblock the
        // PR — this is the explicit escalation the bead's AC requires when retries run out.
        "polecat.ci-retries-exhausted.v1" => {
            let PolecatEvent::CiRetriesExhausted {
                bead,
                reason,
                attempts,
            } = record.decode::<PolecatEvent>().ok()?
            else {
                return None;
            };
            Some(NotificationDraft {
                title: format!("{bead}: CI sigue roja tras {attempts} reintentos"),
                body: format!(
                    "El re-despacho automático por fallo de CI agotó {attempts} reintentos y la CI de {bead} sigue fallando ({reason}). Se abandonó el lazo para no quemar quota; revisa el log de CI y desbloquea el PR a mano."
                ),
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
        // Phase 2 (hq-80e92c) + gtcore-7e19fe: an ALERT also lands in the operator's inbox via the
        // existing email_outbox (the scheduling drain + EmailTransport deliver it). Only the alert
        // kind emails — merge.failed plus the quota/lease capacity faults; the info-kind events
        // (dispatched/merged, sling skipped/failed) stay bell-only. Best-effort like the rest.
        if draft.kind == "alert" {
            if let Some(recipient) = &self.failure_email {
                use gt_store_pg::{EmailOutboxRepository, NewEmail, PgEmailOutbox};
                let outbox = PgEmailOutbox::new(self.pool.clone());
                let new = NewEmail {
                    id: ulid::Ulid::new().to_string(),
                    workspace: self.workspace.clone(),
                    recipient: recipient.clone(),
                    cc: vec![],
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
    fn failed_renders_an_info_since_it_auto_retries() {
        // gtcore-3a1bd4: a CI failure is auto-recovered (re-sling to fix), so a single merge.failed
        // is a bell (`info`, "reintentando"), not an alert. The alert is ci-retries-exhausted.
        let d = draft_for(&record(MergeEvent::Failed {
            bead: "gtweb-1".into(),
            reason: "rebase onto origin/main conflicted: f".into(),
        }))
        .expect("failed warrants a notification");
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("gtweb-1"));
        assert!(d.body.contains("conflicted"));
    }

    #[test]
    fn all_exhausted_renders_an_alert_counting_paused_polecats() {
        let d = draft_for(&record(QuotaEvent::AllExhausted {
            account: "acct-a".into(),
            paused_sessions: vec!["s1".into(), "s2".into()],
            now_secs: 0,
        }))
        .expect("all-exhausted warrants a notification");
        // The high-level "todas bloqueadas" fact — alert (bell + email), the operator must add capacity.
        assert_eq!(d.kind, "alert");
        assert!(d.title.to_lowercase().contains("todas"), "title flags the all-blocked fact: {}", d.title);
        assert!(d.body.contains("acct-a"));
        assert!(d.body.contains('2'), "body counts the suspended polecats: {}", d.body);
    }

    #[test]
    fn account_recovered_renders_an_info_counting_resumed_polecats() {
        let d = draft_for(&record(QuotaEvent::AccountRecovered {
            account: "acct-b".into(),
            resumed_sessions: vec!["s1".into()],
            now_secs: 0,
        }))
        .expect("account-recovered warrants a notification");
        // Bell (`info`): the saturation cleared on its own — no need to page the operator.
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("acct-b"));
        assert!(d.body.contains('1'), "body counts the resumed polecats: {}", d.body);
    }

    #[test]
    fn routine_quota_rotations_warrant_no_notification() {
        // gtcore-bc7dd2 AC: the per-account predictive/reactive rotations self-heal (rotation moves
        // to a healthy account), so notifying on each one is noise — only the aggregate
        // all-exhausted fact alerts. A single-account block precedes a rotation, so it stays silent
        // too.
        assert!(draft_for(&record(QuotaEvent::BlockPredicted {
            account: "a".into(),
            eta_to_block_secs: 120,
            consumed: 900,
            limit: 1000,
            rate_per_min: 8.0,
            now_secs: 0,
        }))
        .is_none());
        assert!(draft_for(&record(QuotaEvent::AccountLimited {
            account: "a".into(),
            now_secs: 0,
        }))
        .is_none());
        assert!(draft_for(&record(QuotaEvent::Blocked {
            account: "a".into(),
            until_secs: None,
            now_secs: 0,
        }))
        .is_none());
    }

    #[test]
    fn lease_expired_renders_an_alert_naming_the_bead_and_worker() {
        let d = draft_for(&record(PatrolEvent::LeaseExpired {
            bead: "gtcore-9".into(),
            worker: "w7".into(),
            priority: 1,
        }))
        .expect("lease-expired warrants a notification");
        assert_eq!(d.kind, "alert");
        assert!(d.title.contains("gtcore-9"));
        assert!(d.body.contains("w7"), "body names the crashed worker: {}", d.body);
    }

    #[test]
    fn sling_skipped_renders_a_bell_naming_the_bead() {
        let d = draft_for(&record(PolecatEvent::SlingSkipped {
            bead: "gtweb-3".into(),
            workspace: "default".into(),
        }))
        .expect("sling-skipped warrants a notification");
        // Bell (`info`), not alert: recoverable backpressure stays off the email path.
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("gtweb-3"));
        assert!(d.title.contains("pool"), "title says why it was skipped: {}", d.title);
    }

    #[test]
    fn sling_failed_renders_a_bell_carrying_the_reason() {
        let d = draft_for(&record(PolecatEvent::SlingFailed {
            bead: "gtweb-4".into(),
            reason: "tmux: no server running".into(),
        }))
        .expect("sling-failed warrants a notification");
        // Bell (`info`) per the AC — visible in the bell, off the alert/email path.
        assert_eq!(d.kind, "info");
        assert!(d.title.contains("gtweb-4"));
        assert!(d.body.contains("no server running"));
    }

    #[test]
    fn credential_dead_renders_an_alert_naming_the_account() {
        let d = draft_for(&record(PolecatEvent::CredentialDead {
            account: "acct-z".into(),
            reason: "credenciales expiradas sin refresh token".into(),
        }))
        .expect("credential-dead warrants a notification");
        // Alert (bell + email) — the operator must re-onboard/rotate.
        assert_eq!(d.kind, "alert");
        assert!(d.title.contains("acct-z"));
        assert!(d.body.contains("expiradas"), "body carries the reason: {}", d.body);
    }

    #[test]
    fn sling_auth_blocked_renders_an_alert_naming_the_bead() {
        let d = draft_for(&record(PolecatEvent::SlingAuthBlocked {
            bead: "gtcore-5".into(),
            workspace: "default".into(),
        }))
        .expect("sling-auth-blocked warrants a notification");
        assert_eq!(d.kind, "alert");
        assert!(d.title.contains("gtcore-5"));
        assert!(d.title.contains("sin credenciales"), "title says why: {}", d.title);
    }

    #[test]
    fn ci_retries_exhausted_renders_an_alert_with_attempts_and_reason() {
        let d = draft_for(&record(PolecatEvent::CiRetriesExhausted {
            bead: "gtcore-3a1bd4".into(),
            reason: "CI failed: failure".into(),
            attempts: 3,
        }))
        .expect("ci-retries-exhausted warrants a notification");
        // Alert (bell + email) — a human must unblock the PR.
        assert_eq!(d.kind, "alert");
        assert!(d.title.contains("gtcore-3a1bd4"));
        assert!(d.title.contains('3'), "title counts the retries: {}", d.title);
        assert!(d.body.contains("CI failed: failure"), "body carries the reason: {}", d.body);
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
