//! Scheduled report digests — multi-schedule service + daemon (hq-7d50e4,
//! epic hq-2ef0a3; supersedes the single daily schedule of hq-84f93b).
//!
//! The system config carries a LIST of [`ReportSchedule`]s. Each one scopes a
//! board (rig, workspace), names a report [`kind`](ReportSchedule::kind) from
//! the render REGISTRY, fires under one of three modes —
//! [`Daily`](ScheduleMode::Daily), [`EveryNDays`](ScheduleMode::EveryNDays),
//! [`Once`](ScheduleMode::Once) (auto-disables after sending) — and can carry
//! its OWN recipient list (`subscribers`), falling back to the workspace's
//! global enabled `report_subscriptions` when absent.
//!
//! Adding a new report type = registering one render fn in [`render_for`];
//! the daemon, CRUD surface and UI pick it up from [`kinds`] untouched.
//!
//! Delivery stays outbox-first (ADR hq-423a4b D8): one row per recipient, the
//! drain owns the transport. Manual sends never touch `last_sent_date`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use gt_issues::analytics::{summarize, AnalyticsSummary};
use gt_issues::report::{build_report, OperatorReport};
use gt_issues::report_html::render_digest;
use gt_store_dolt::{DoltIssues, IssueFilter};
use gt_store_pg::{
    EmailOutboxRepository, NewEmail, PgEmailOutbox, PgReportSubscriptions,
    ReportSubscriptionsRepository,
};

use crate::system::SharedArchiveConfig;

/// When a schedule fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleMode {
    /// Every day at `hour:minute`.
    Daily,
    /// When at least `n_days` local days passed since the last send (first
    /// send = first tick with the time reached).
    EveryNDays,
    /// Exactly once on `date` at `hour:minute` (catch-up if the process was
    /// down that day); auto-disables after sending.
    Once,
}

/// One scheduled report send.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSchedule {
    /// Stable id (ulid) — the CRUD/run-now handle.
    #[serde(default = "new_id")]
    pub id: String,
    /// Render-registry key ([`kinds`]). v1: `planning-digest`.
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default = "default_mode")]
    pub mode: ScheduleMode,
    /// `EveryNDays` interval; min 1.
    #[serde(default = "default_n_days")]
    pub n_days: u32,
    /// `Once` target local date (`YYYY-MM-DD`).
    #[serde(default)]
    pub date: Option<String>,
    /// Local wall-clock send time.
    #[serde(default = "default_hour")]
    pub hour: u8,
    #[serde(default)]
    pub minute: u8,
    /// Minutes added to UTC for this schedule's wall clock. Default −300
    /// (UTC-5, Colombia).
    #[serde(default = "default_tz_offset")]
    pub tz_offset_minutes: i64,
    /// Board scope the report projects.
    #[serde(default = "default_rig")]
    pub rig: String,
    #[serde(default = "default_workspace")]
    pub workspace: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Last LOCAL date sent (the at-most-once guard). Daemon bookkeeping;
    /// read-only through the API.
    #[serde(default)]
    pub last_sent_date: Option<String>,
    /// Per-schedule recipients. `None` ⇒ the workspace's global enabled
    /// `report_subscriptions` list.
    #[serde(default)]
    pub subscribers: Option<Vec<String>>,
}

fn new_id() -> String {
    ulid::Ulid::new().to_string()
}
fn default_kind() -> String {
    "planning-digest".into()
}
fn default_mode() -> ScheduleMode {
    ScheduleMode::Daily
}
fn default_n_days() -> u32 {
    1
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
fn default_true() -> bool {
    true
}

impl Default for ReportSchedule {
    fn default() -> Self {
        Self {
            id: new_id(),
            kind: default_kind(),
            mode: default_mode(),
            n_days: default_n_days(),
            date: None,
            hour: default_hour(),
            minute: 0,
            tz_offset_minutes: default_tz_offset(),
            rig: default_rig(),
            workspace: default_workspace(),
            enabled: true,
            last_sent_date: None,
            subscribers: None,
        }
    }
}

/// The PRE-multi-schedule scalar config (hq-84f93b). Still deserialized from
/// the persisted file's `report` key so a deployed `system_config.json`
/// migrates losslessly into one Daily schedule on load.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LegacyReportConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_hour")]
    pub hour: u8,
    #[serde(default)]
    pub minute: u8,
    #[serde(default = "default_tz_offset")]
    pub tz_offset_minutes: i64,
    #[serde(default = "default_rig")]
    pub rig: String,
    #[serde(default = "default_workspace")]
    pub workspace: String,
    #[serde(default)]
    pub last_sent_date: Option<String>,
}

impl LegacyReportConfig {
    /// The equivalent Daily schedule.
    pub fn into_schedule(self) -> ReportSchedule {
        ReportSchedule {
            mode: ScheduleMode::Daily,
            enabled: self.enabled,
            hour: self.hour,
            minute: self.minute,
            tz_offset_minutes: self.tz_offset_minutes,
            rig: self.rig,
            workspace: self.workspace,
            last_sent_date: self.last_sent_date,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Render registry — kind → (subject, html). Adding a report type happens HERE
// and nowhere else.
// ---------------------------------------------------------------------------

/// A registered report renderer.
pub type RenderFn = fn(&OperatorReport, &AnalyticsSummary, &str) -> (String, String);

fn render_planning_digest(
    report: &OperatorReport,
    summary: &AnalyticsSummary,
    fecha: &str,
) -> (String, String) {
    (
        format!("Reporte de planning {}/{} — {fecha}", report.rig, report.workspace),
        render_digest(report, summary, fecha),
    )
}

/// Registry lookup. `None` = unknown kind (validation rejects it; a stale
/// schedule with a retired kind is skipped with a log, never a panic).
pub fn render_for(kind: &str) -> Option<RenderFn> {
    match kind {
        "planning-digest" => Some(render_planning_digest),
        _ => None,
    }
}

/// The registered kinds, for validation and the UI dropdown.
pub fn kinds() -> Vec<&'static str> {
    vec!["planning-digest"]
}

// ---------------------------------------------------------------------------
// CRUD patch + validation — ONE shape shared by the MCP tools and the System
// REST surface (hq-c4f920), so both reject the same garbage the same way.
// ---------------------------------------------------------------------------

/// Partial schedule write. `None` leaves the field untouched. `last_sent_date`
/// and `id` are deliberately absent — daemon bookkeeping / immutable handle.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct SchedulePatch {
    pub kind: Option<String>,
    pub mode: Option<ScheduleMode>,
    pub n_days: Option<u32>,
    pub date: Option<String>,
    pub hour: Option<u8>,
    pub minute: Option<u8>,
    pub tz_offset_minutes: Option<i64>,
    pub rig: Option<String>,
    pub workspace: Option<String>,
    pub enabled: Option<bool>,
    /// `Some(list)` sets the per-schedule recipients (trimmed, empties
    /// dropped; an empty result clears back to the global fallback).
    pub subscribers: Option<Vec<String>>,
}

/// Apply `patch` onto `s`, then validate the result. The schedule is only
/// mutated on success (validation runs on a candidate).
pub fn apply_patch(s: &mut ReportSchedule, patch: SchedulePatch) -> Result<(), String> {
    let mut c = s.clone();
    if let Some(v) = patch.kind {
        c.kind = v;
    }
    if let Some(v) = patch.mode {
        c.mode = v;
    }
    if let Some(v) = patch.n_days {
        c.n_days = v;
    }
    if let Some(v) = patch.date {
        c.date = if v.trim().is_empty() { None } else { Some(v.trim().to_string()) };
    }
    if let Some(v) = patch.hour {
        c.hour = v;
    }
    if let Some(v) = patch.minute {
        c.minute = v;
    }
    if let Some(v) = patch.tz_offset_minutes {
        c.tz_offset_minutes = v;
    }
    if let Some(v) = patch.rig {
        c.rig = v.trim().to_string();
    }
    if let Some(v) = patch.workspace {
        c.workspace = v.trim().to_string();
    }
    if let Some(v) = patch.enabled {
        c.enabled = v;
    }
    if let Some(list) = patch.subscribers {
        let cleaned: Vec<String> =
            list.iter().map(|e| e.trim().to_string()).filter(|e| !e.is_empty()).collect();
        c.subscribers = (!cleaned.is_empty()).then_some(cleaned);
    }
    validate(&c)?;
    *s = c;
    Ok(())
}

/// The closed validation every write path runs.
pub fn validate(s: &ReportSchedule) -> Result<(), String> {
    if render_for(&s.kind).is_none() {
        return Err(format!("unknown report kind `{}` (known: {:?})", s.kind, kinds()));
    }
    if s.hour > 23 || s.minute > 59 {
        return Err("hour 0-23, minute 0-59".into());
    }
    if !(-14 * 60..=14 * 60).contains(&s.tz_offset_minutes) {
        return Err("tz_offset_minutes out of range (±840)".into());
    }
    if s.rig.is_empty() || s.workspace.is_empty() {
        return Err("rig and workspace are required".into());
    }
    match s.mode {
        ScheduleMode::Once => match s.date.as_deref() {
            Some(d) if epoch_days(d).is_some() => {}
            Some(d) => return Err(format!("`date` must be YYYY-MM-DD, got `{d}`")),
            None => return Err("mode `once` requires `date` (YYYY-MM-DD)".into()),
        },
        ScheduleMode::EveryNDays => {
            if s.n_days == 0 {
                return Err("mode `every_n_days` requires n_days >= 1".into());
            }
        }
        ScheduleMode::Daily => {}
    }
    if let Some(list) = &s.subscribers {
        if let Some(bad) = list.iter().find(|e| !e.contains('@')) {
            return Err(format!("`{bad}` is not an email address"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Clock math (no chrono dep: proleptic-Gregorian day arithmetic suffices for
// schedule dates).
// ---------------------------------------------------------------------------

/// `(local_date, minutes_since_midnight)` of now-UTC shifted by `offset`.
fn local_now(offset_minutes: i64) -> (String, i64) {
    let now = time::OffsetDateTime::now_utc() + time::Duration::minutes(offset_minutes);
    let date = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
    (date, i64::from(now.hour()) * 60 + i64::from(now.minute()))
}

/// Days since the civil epoch of a `YYYY-MM-DD` string (Howard Hinnant's
/// days_from_civil — same arithmetic the analytics projection uses).
fn epoch_days(date: &str) -> Option<i64> {
    let mut it = date.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

/// Fire decision, pure for tests.
pub fn is_due(s: &ReportSchedule, local_date: &str, minutes_now: i64) -> bool {
    if !s.enabled {
        return false;
    }
    let time_reached = minutes_now >= i64::from(s.hour) * 60 + i64::from(s.minute);
    let sent_today = s.last_sent_date.as_deref() == Some(local_date);
    match s.mode {
        ScheduleMode::Daily => time_reached && !sent_today,
        ScheduleMode::EveryNDays => {
            if !time_reached || sent_today {
                return false;
            }
            match (&s.last_sent_date, epoch_days(local_date)) {
                (None, _) => true,
                (Some(last), Some(today)) => epoch_days(last)
                    .map(|l| today - l >= i64::from(s.n_days.max(1)))
                    .unwrap_or(true),
                _ => false,
            }
        }
        ScheduleMode::Once => {
            // Fires on the target date once the time is reached; a LATER day
            // still fires (catch-up after downtime). Never twice: the send
            // both stamps last_sent_date and disables the schedule.
            if s.last_sent_date.is_some() {
                return false;
            }
            match (s.date.as_deref(), epoch_days(local_date)) {
                (Some(target), Some(today)) => match epoch_days(target) {
                    Some(t) if today > t => true,
                    Some(t) if today == t => time_reached,
                    _ => false,
                },
                _ => false,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Service + daemon
// ---------------------------------------------------------------------------

/// The digest pipeline + subscriber store, shared by the daemon, the MCP
/// `report.*` tools and the System REST surface.
pub struct ReportService {
    dolt: Arc<DoltIssues>,
    pool: PgPool,
    /// The persisted system config (`report_schedules` is ours).
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

    /// The global subscribers store over the shared public pool.
    pub fn subscribers(&self) -> PgReportSubscriptions {
        PgReportSubscriptions::new(self.pool.clone())
    }

    /// Persist the current config snapshot to disk.
    pub async fn persist(&self) {
        if let Some(path) = &self.config_path {
            let snapshot = self.config.read().await.clone();
            crate::system::persist_config(path, &snapshot);
        }
    }

    /// All schedules (the CRUD list).
    pub async fn list_schedules(&self) -> Vec<ReportSchedule> {
        self.config.read().await.report_schedules.clone()
    }

    /// Create a schedule from a patch over the defaults; persisted on success.
    pub async fn create_schedule(&self, patch: SchedulePatch) -> Result<ReportSchedule, String> {
        let mut s = ReportSchedule::default();
        apply_patch(&mut s, patch)?;
        self.config.write().await.report_schedules.push(s.clone());
        self.persist().await;
        Ok(s)
    }

    /// Patch one schedule by id; `last_sent_date`/`id` untouched by design.
    pub async fn update_schedule(
        &self,
        id: &str,
        patch: SchedulePatch,
    ) -> Result<ReportSchedule, String> {
        let updated = {
            let mut cfg = self.config.write().await;
            let s = cfg
                .report_schedules
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| format!("unknown schedule `{id}`"))?;
            apply_patch(s, patch)?;
            s.clone()
        };
        self.persist().await;
        Ok(updated)
    }

    /// Remove one schedule by id; persisted on success.
    pub async fn delete_schedule(&self, id: &str) -> Result<(), String> {
        {
            let mut cfg = self.config.write().await;
            let before = cfg.report_schedules.len();
            cfg.report_schedules.retain(|s| s.id != id);
            if cfg.report_schedules.len() == before {
                return Err(format!("unknown schedule `{id}`"));
            }
        }
        self.persist().await;
        Ok(())
    }

    /// Resolve the send target: an explicit id, or the single existing
    /// schedule when unambiguous.
    pub async fn resolve_schedule(&self, id: Option<&str>) -> Result<ReportSchedule, String> {
        let schedules = self.config.read().await.report_schedules.clone();
        match id {
            Some(id) => schedules
                .into_iter()
                .find(|s| s.id == id)
                .ok_or_else(|| format!("unknown schedule `{id}`")),
            None => match schedules.len() {
                0 => Err("no schedules configured".into()),
                1 => Ok(schedules.into_iter().next().expect("len checked")),
                n => Err(format!("{n} schedules exist — pass schedule_id")),
            },
        }
    }

    /// Build + render ONE schedule's report and enqueue an outbox row per
    /// recipient (the schedule's own list, else the workspace's enabled
    /// globals). Returns how many were queued. Never touches
    /// `last_sent_date` — that is the daemon's bookkeeping.
    pub async fn send_schedule(
        &self,
        schedule: &ReportSchedule,
        created_by: &str,
    ) -> Result<usize, String> {
        let render = render_for(&schedule.kind)
            .ok_or_else(|| format!("unknown report kind `{}`", schedule.kind))?;

        let recipients: Vec<String> = match &schedule.subscribers {
            Some(own) => own.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            None => self
                .subscribers()
                .enabled_emails(&schedule.workspace)
                .await
                .map_err(|e| format!("subscribers: {e}"))?,
        };
        if recipients.is_empty() {
            return Ok(0);
        }

        // The same rows board.list / report.generate read (full=true for Notas).
        let rows = self
            .dolt
            .list(&IssueFilter {
                rig: Some(schedule.rig.clone()),
                workspace: Some(schedule.workspace.clone()),
                full: true,
                limit: Some(gt_store_dolt::issues_max_limit()),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("tracker rows: {e}"))?;
        let report = build_report(&schedule.rig, &schedule.workspace, &rows);
        let (today, _) = local_now(schedule.tz_offset_minutes);
        // reopens=0: the audit-derived count lives with the analytics handler;
        // the digest tolerates the conservative zero (defects still counted).
        let summary = summarize(&schedule.rig, &schedule.workspace, &rows, 0, &today, 7, 30);
        let (subject, html) = render(&report, &summary, &today);

        let outbox = PgEmailOutbox::new(self.pool.clone());
        let mut queued = 0;
        for to in recipients {
            match outbox
                .enqueue(NewEmail {
                    id: ulid::Ulid::new().to_string(),
                    workspace: schedule.workspace.clone(),
                    recipient: to,
                    subject: subject.clone(),
                    body: html.clone(),
                    template_ref: Some(format!("report:{}", schedule.kind)),
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

/// The fixed-time daemon: ticks every minute, fires every due schedule.
pub struct ReportScheduler {
    service: Arc<ReportService>,
}

impl ReportScheduler {
    pub fn new(service: Arc<ReportService>) -> Self {
        Self { service }
    }

    pub async fn run(self) {
        loop {
            let schedules = self.service.config.read().await.report_schedules.clone();
            for schedule in &schedules {
                let (local_date, minutes_now) = local_now(schedule.tz_offset_minutes);
                if !is_due(schedule, &local_date, minutes_now) {
                    continue;
                }
                match self.service.send_schedule(schedule, "report-scheduler").await {
                    Ok(queued) => {
                        eprintln!(
                            "[report-scheduler] `{}` ({}, {:?}) queued to {queued} recipient(s) \
                             ({local_date} {:02}:{:02})",
                            schedule.kind, schedule.rig, schedule.mode, schedule.hour,
                            schedule.minute
                        );
                        // Stamp even when queued=0 (the schedule DID fire; a
                        // recipient added later gets the next cycle or
                        // send-now). Once auto-disables: exactly-one send.
                        let mut cfg = self.service.config.write().await;
                        if let Some(live) =
                            cfg.report_schedules.iter_mut().find(|s| s.id == schedule.id)
                        {
                            live.last_sent_date = Some(local_date);
                            if live.mode == ScheduleMode::Once {
                                live.enabled = false;
                            }
                        }
                        drop(cfg);
                        self.service.persist().await;
                    }
                    Err(e) => eprintln!(
                        "[report-scheduler] `{}` failed (retry next tick): {e}",
                        schedule.id
                    ),
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sched(mode: ScheduleMode) -> ReportSchedule {
        ReportSchedule { mode, hour: 8, minute: 30, ..Default::default() }
    }

    const TODAY: &str = "2026-06-12";
    const AT: i64 = 8 * 60 + 30;

    #[test]
    fn daily_fires_once_per_day_after_the_time() {
        let mut s = sched(ScheduleMode::Daily);
        assert!(!is_due(&s, TODAY, AT - 1));
        assert!(is_due(&s, TODAY, AT));
        s.last_sent_date = Some(TODAY.into());
        assert!(!is_due(&s, TODAY, 23 * 60));
        s.enabled = false;
        s.last_sent_date = None;
        assert!(!is_due(&s, TODAY, AT));
    }

    #[test]
    fn every_n_days_respects_the_interval() {
        let mut s = sched(ScheduleMode::EveryNDays);
        s.n_days = 3;
        // Never sent: first reached tick fires.
        assert!(is_due(&s, TODAY, AT));
        // Sent today: no.
        s.last_sent_date = Some(TODAY.into());
        assert!(!is_due(&s, TODAY, 23 * 60));
        // n-1 days ago: no. n days ago: yes.
        s.last_sent_date = Some("2026-06-10".into());
        assert!(!is_due(&s, TODAY, AT));
        s.last_sent_date = Some("2026-06-09".into());
        assert!(is_due(&s, TODAY, AT));
        // n=0 clamps to 1: yesterday ⇒ due.
        s.n_days = 0;
        s.last_sent_date = Some("2026-06-11".into());
        assert!(is_due(&s, TODAY, AT));
        // Time not reached blocks regardless.
        assert!(!is_due(&s, TODAY, AT - 1));
    }

    #[test]
    fn once_fires_exactly_once_with_catch_up() {
        let mut s = sched(ScheduleMode::Once);
        // No date: never.
        assert!(!is_due(&s, TODAY, AT));
        // Future date: no.
        s.date = Some("2026-06-20".into());
        assert!(!is_due(&s, TODAY, AT));
        // Target day: only once the time is reached.
        s.date = Some(TODAY.into());
        assert!(!is_due(&s, TODAY, AT - 1));
        assert!(is_due(&s, TODAY, AT));
        // Process was down on the date: a later day catches up at any time.
        s.date = Some("2026-06-10".into());
        assert!(is_due(&s, TODAY, 0));
        // Already sent: never again, any day.
        s.last_sent_date = Some("2026-06-10".into());
        assert!(!is_due(&s, "2026-07-01", 23 * 60));
    }

    #[test]
    fn legacy_scalar_config_migrates_to_one_daily_schedule() {
        let legacy: LegacyReportConfig = serde_json::from_str(
            r#"{"enabled":true,"hour":9,"minute":15,"tz_offset_minutes":-300,
                "rig":"hq","workspace":"default","last_sent_date":"2026-06-11"}"#,
        )
        .expect("legacy parses");
        let s = legacy.into_schedule();
        assert_eq!(s.mode, ScheduleMode::Daily);
        assert!(s.enabled);
        assert_eq!((s.hour, s.minute), (9, 15));
        assert_eq!(s.last_sent_date.as_deref(), Some("2026-06-11"));
        assert_eq!(s.kind, "planning-digest");
        assert!(s.subscribers.is_none());
        assert!(!s.id.is_empty());
    }

    #[test]
    fn new_schedule_list_round_trips_and_defaults_hold() {
        let s = ReportSchedule { mode: ScheduleMode::EveryNDays, n_days: 7, ..Default::default() };
        let json = serde_json::to_string(&vec![s.clone()]).expect("encode");
        let back: Vec<ReportSchedule> = serde_json::from_str(&json).expect("decode");
        assert_eq!(back[0].id, s.id);
        assert_eq!(back[0].mode, ScheduleMode::EveryNDays);
        assert_eq!(back[0].n_days, 7);
        // Defaults for an empty object.
        let d: ReportSchedule = serde_json::from_str("{}").expect("empty");
        assert_eq!(d.kind, "planning-digest");
        assert_eq!(d.mode, ScheduleMode::Daily);
        assert!(d.enabled);
        assert_eq!(d.tz_offset_minutes, -300);
    }

    #[test]
    fn patch_validation_rejects_the_documented_garbage() {
        let base = ReportSchedule::default();
        let try_patch = |p: SchedulePatch| {
            let mut s = base.clone();
            apply_patch(&mut s, p)
        };
        // Unknown kind.
        assert!(try_patch(SchedulePatch { kind: Some("nope".into()), ..Default::default() })
            .unwrap_err()
            .contains("unknown report kind"));
        // once without date / bad date.
        assert!(try_patch(SchedulePatch {
            mode: Some(ScheduleMode::Once),
            ..Default::default()
        })
        .unwrap_err()
        .contains("requires `date`"));
        assert!(try_patch(SchedulePatch {
            mode: Some(ScheduleMode::Once),
            date: Some("12/06/2026".into()),
            ..Default::default()
        })
        .unwrap_err()
        .contains("YYYY-MM-DD"));
        // every_n_days with n_days=0.
        assert!(try_patch(SchedulePatch {
            mode: Some(ScheduleMode::EveryNDays),
            n_days: Some(0),
            ..Default::default()
        })
        .unwrap_err()
        .contains("n_days >= 1"));
        // Hour out of range; bad subscriber.
        assert!(try_patch(SchedulePatch { hour: Some(24), ..Default::default() }).is_err());
        assert!(try_patch(SchedulePatch {
            subscribers: Some(vec!["sin-arroba".into()]),
            ..Default::default()
        })
        .unwrap_err()
        .contains("not an email"));
        // Failure leaves the original untouched.
        let mut s = base.clone();
        let _ = apply_patch(&mut s, SchedulePatch { hour: Some(24), ..Default::default() });
        assert_eq!(s.hour, base.hour);
    }

    #[test]
    fn patch_applies_and_normalizes_subscribers() {
        let mut s = ReportSchedule::default();
        apply_patch(&mut s, SchedulePatch {
            mode: Some(ScheduleMode::Once),
            date: Some(" 2026-06-20 ".into()),
            hour: Some(7),
            subscribers: Some(vec![" ana@x.com ".into(), "".into()]),
            ..Default::default()
        })
        .expect("valid patch");
        assert_eq!(s.mode, ScheduleMode::Once);
        assert_eq!(s.date.as_deref(), Some("2026-06-20"));
        assert_eq!(s.subscribers.as_deref(), Some(&["ana@x.com".to_string()][..]));
        // Empty subscriber list clears back to the global fallback.
        apply_patch(&mut s, SchedulePatch {
            subscribers: Some(vec![]),
            ..Default::default()
        })
        .expect("clear");
        assert!(s.subscribers.is_none());
    }

    #[test]
    fn registry_knows_planning_digest_and_rejects_unknown() {
        assert!(render_for("planning-digest").is_some());
        assert!(render_for("nope").is_none());
        assert_eq!(kinds(), vec!["planning-digest"]);
    }
}
