//! Scheduled report digest — service + daemon (hq-84f93b, epic hq-efc379).
//!
//! Composes the pure pieces hq-562e0b shipped: the bitácora projection
//! (`build_report`), the analytics KPIs (`summarize`) and the HTML render
//! (`render_digest`) into one send: an outbox row per ENABLED subscriber
//! (`report_subscriptions`, the operator's selection switch). Delivery is the
//! outbox drain's job — this module never touches a transport (ADR hq-423a4b
//! D8).
//!
//! Scheduling is fixed-time, not interval: [`ReportScheduler`] ticks every
//! minute and fires when the configured local wall-clock time has passed and
//! today's digest has not been sent (`last_sent_date` guard — idempotent
//! across restarts, catch-up after downtime). Manual sends
//! ([`ReportService::send_digest`] via `report.send-now` / the System UI) do
//! NOT touch the guard: a manual send never cancels the scheduled one.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use gt_issues::analytics::summarize;
use gt_issues::report::build_report;
use gt_issues::report_html::render_digest;
use gt_store_dolt::{DoltIssues, IssueFilter};
use gt_store_pg::{
    EmailOutboxRepository, NewEmail, PgEmailOutbox, PgReportSubscriptions,
    ReportSubscriptionsRepository,
};

use crate::system::SharedArchiveConfig;

/// When the digest goes out, and over which board scope. Lives inside the
/// persisted system config (`GT_SYSTEM_CONFIG_PATH`) next to the archive
/// knobs; `#[serde(default)]` everywhere so pre-existing config files parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportScheduleConfig {
    /// Master switch — off by default until the operator configures it.
    #[serde(default)]
    pub enabled: bool,
    /// Local wall-clock hour (0-23) the digest fires at.
    #[serde(default = "default_hour")]
    pub hour: u8,
    /// Local wall-clock minute (0-59).
    #[serde(default)]
    pub minute: u8,
    /// Minutes added to UTC to get the operator's wall clock. Default −300
    /// (UTC-5, Colombia — the deployment's operator timezone).
    #[serde(default = "default_tz_offset")]
    pub tz_offset_minutes: i64,
    /// Board scope the digest projects.
    #[serde(default = "default_rig")]
    pub rig: String,
    #[serde(default = "default_workspace")]
    pub workspace: String,
    /// Last LOCAL date (`YYYY-MM-DD`) the scheduler sent — the at-most-once-
    /// per-day guard. Maintained by the daemon, read-only through the API.
    #[serde(default)]
    pub last_sent_date: Option<String>,
}

fn default_hour() -> u8 {
    8
}
fn default_tz_offset() -> i64 {
    -300
}
fn default_rig() -> String {
    "hq".into()
}
fn default_workspace() -> String {
    "default".into()
}

impl Default for ReportScheduleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            hour: default_hour(),
            minute: 0,
            tz_offset_minutes: default_tz_offset(),
            rig: default_rig(),
            workspace: default_workspace(),
            last_sent_date: None,
        }
    }
}

/// `(local_date, minutes_since_midnight)` of `now_utc` shifted by `offset`.
fn local_now(offset_minutes: i64) -> (String, i64) {
    let now = time::OffsetDateTime::now_utc() + time::Duration::minutes(offset_minutes);
    let date = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
    (date, i64::from(now.hour()) * 60 + i64::from(now.minute()))
}

/// The digest pipeline + subscriber CRUD, shared by the daemon, the MCP
/// `report.*` tools and the System REST surface.
pub struct ReportService {
    dolt: Arc<DoltIssues>,
    pool: PgPool,
    /// The persisted system config (the `report` half is ours).
    pub config: SharedArchiveConfig,
    config_path: Option<PathBuf>,
}

impl ReportService {
    pub fn new(
        dolt: Arc<DoltIssues>,
        pool: PgPool,
        config: SharedArchiveConfig,
        config_path: Option<PathBuf>,
    ) -> Self {
        Self { dolt, pool, config, config_path }
    }

    /// The subscribers store over the shared public pool.
    pub fn subscribers(&self) -> PgReportSubscriptions {
        PgReportSubscriptions::new(self.pool.clone())
    }

    /// Persist the current config snapshot to disk (same file the archive
    /// knobs live in).
    pub async fn persist(&self) {
        if let Some(path) = &self.config_path {
            let snapshot = self.config.read().await.clone();
            crate::system::persist_config(path, &snapshot);
        }
    }

    /// Build + render today's digest and enqueue one outbox row per ENABLED
    /// subscriber. Returns how many were queued (0 when no enabled
    /// subscribers — not an error). Never touches `last_sent_date`; that is
    /// the daemon's bookkeeping.
    pub async fn send_digest(&self, created_by: &str) -> Result<usize, String> {
        let report_cfg = self.config.read().await.report.clone();
        let (rig, workspace) = (report_cfg.rig.clone(), report_cfg.workspace.clone());

        let recipients = self
            .subscribers()
            .enabled_emails(&workspace)
            .await
            .map_err(|e| format!("subscribers: {e}"))?;
        if recipients.is_empty() {
            return Ok(0);
        }

        // The same rows board.list / report.generate read (full=true for Notas).
        let rows = self
            .dolt
            .list(&IssueFilter {
                rig: Some(rig.clone()),
                workspace: Some(workspace.clone()),
                full: true,
                limit: Some(gt_store_dolt::issues_max_limit()),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("tracker rows: {e}"))?;
        let report = build_report(&rig, &workspace, &rows);
        let (today, _) = local_now(report_cfg.tz_offset_minutes);
        // reopens=0: the audit-derived count needs the audit sink the
        // analytics handler owns; the digest's KPI strip tolerates the
        // conservative zero (defects still counted).
        let summary = summarize(&rig, &workspace, &rows, 0, &today, 7, 30);
        let html = render_digest(&report, &summary, &today);

        let outbox = PgEmailOutbox::new(self.pool.clone());
        let subject = format!("Reporte de planning {rig}/{workspace} — {today}");
        let mut queued = 0;
        for to in recipients {
            match outbox
                .enqueue(NewEmail {
                    id: ulid::Ulid::new().to_string(),
                    workspace: workspace.clone(),
                    recipient: to,
                    subject: subject.clone(),
                    body: html.clone(),
                    template_ref: Some("report-digest".into()),
                    send_at: None,
                    created_by: created_by.to_string(),
                })
                .await
            {
                Ok(_) => queued += 1,
                Err(e) => eprintln!("[report-scheduler] outbox enqueue failed: {e}"),
            }
        }
        Ok(queued)
    }
}

/// Fire decision, pure for tests: send when enabled, the wall clock reached
/// the configured time, and today's digest hasn't gone out.
pub fn is_due(cfg: &ReportScheduleConfig, local_date: &str, minutes_now: i64) -> bool {
    cfg.enabled
        && minutes_now >= i64::from(cfg.hour) * 60 + i64::from(cfg.minute)
        && cfg.last_sent_date.as_deref() != Some(local_date)
}

/// The fixed-time daemon. Spawn via `tokio::spawn(ReportScheduler::new(svc).run())`.
pub struct ReportScheduler {
    service: Arc<ReportService>,
}

impl ReportScheduler {
    pub fn new(service: Arc<ReportService>) -> Self {
        Self { service }
    }

    pub async fn run(self) {
        loop {
            let report_cfg = self.service.config.read().await.report.clone();
            let (local_date, minutes_now) = local_now(report_cfg.tz_offset_minutes);
            if is_due(&report_cfg, &local_date, minutes_now) {
                match self.service.send_digest("report-scheduler").await {
                    Ok(queued) => {
                        eprintln!(
                            "[report-scheduler] digest queued to {queued} subscriber(s) \
                             ({local_date} {:02}:{:02})",
                            report_cfg.hour, report_cfg.minute
                        );
                        // Mark today as sent even when queued=0 (no enabled
                        // subscribers): the schedule fired; re-firing every
                        // minute would spam logs, and a subscriber added later
                        // today gets tomorrow's digest (or send-now).
                        self.service.config.write().await.report.last_sent_date =
                            Some(local_date);
                        self.service.persist().await;
                    }
                    Err(e) => eprintln!("[report-scheduler] digest failed (will retry next tick): {e}"),
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(enabled: bool, hour: u8, minute: u8, last: Option<&str>) -> ReportScheduleConfig {
        ReportScheduleConfig {
            enabled,
            hour,
            minute,
            last_sent_date: last.map(Into::into),
            ..Default::default()
        }
    }

    #[test]
    fn fires_once_per_day_after_the_configured_time() {
        let today = "2026-06-12";
        // Before the configured time: not due.
        assert!(!is_due(&cfg(true, 8, 30, None), today, 8 * 60 + 29));
        // At/after: due.
        assert!(is_due(&cfg(true, 8, 30, None), today, 8 * 60 + 30));
        assert!(is_due(&cfg(true, 8, 30, Some("2026-06-11")), today, 23 * 60));
        // Already sent today: never again (idempotent restarts).
        assert!(!is_due(&cfg(true, 8, 30, Some(today)), today, 9 * 60));
        // Disabled: never.
        assert!(!is_due(&cfg(false, 8, 30, None), today, 9 * 60));
    }

    #[test]
    fn config_defaults_are_off_and_colombia_morning() {
        let c = ReportScheduleConfig::default();
        assert!(!c.enabled);
        assert_eq!((c.hour, c.minute), (8, 0));
        assert_eq!(c.tz_offset_minutes, -300);
        assert_eq!((c.rig.as_str(), c.workspace.as_str()), ("hq", "default"));
        // Old config files (no `report` key) must keep parsing.
        let parsed: ReportScheduleConfig = serde_json::from_str("{}").expect("empty object");
        assert!(!parsed.enabled);
    }
}
