//! Scheduled report digests — multi-schedule service + daemon (hq-7d50e4,
//! epic hq-2ef0a3; supersedes the single daily schedule of hq-84f93b).
//!
//! The system config carries a LIST of [`ReportSchedule`]s. Each one scopes a
//! board (rig, workspace), names a report [`kind`](ReportSchedule::kind) from
//! the render REGISTRY, fires under one of five modes —
//! [`Daily`](ScheduleMode::Daily), [`EveryNDays`](ScheduleMode::EveryNDays),
//! [`Weekly`](ScheduleMode::Weekly), [`Monthly`](ScheduleMode::Monthly),
//! [`Once`](ScheduleMode::Once) (auto-disables after sending) — and can carry
//! its OWN recipient list (`subscribers`), falling back to the workspace's
//! global enabled `report_subscriptions` when absent.
//!
//! Adding a new report type = registering one render fn in [`render_for`];
//! the daemon, CRUD surface and UI pick it up from [`kinds`] untouched.
//!
//! Delivery stays outbox-first (ADR hq-423a4b D8): ONE row per send — To: the
//! configured sender, every registered recipient in CC (gtcore-ecf70d) — and
//! the drain owns the transport. Manual sends never touch `last_sent_date`.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use gt_issues::analytics::{summarize, AnalyticsSummary};
use gt_issues::report::{build_report, OperatorReport, ReportComment};
use gt_issues::report_html::render_digest;
use gt_store_dolt::{DoltIssues, IssueFilter, WorkspacePools};
use gt_store_pg::{
    EmailOutboxRepository, NewEmail, PgComments, PgEmailOutbox, PgReportSchedules,
    PgReportSubscriptions, ReportScheduleError, ReportScheduleRow, ReportSchedulesRepository,
    ReportSubscriptionsRepository,
};

use crate::mcp::WsPools;

/// When a schedule fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleMode {
    /// Every day at `hour:minute`.
    Daily,
    /// When at least `n_days` local days passed since the last send (first
    /// send = first tick with the time reached).
    EveryNDays,
    /// Every week on `weekday` (0=Sunday..6=Saturday) at `hour:minute`.
    Weekly,
    /// Every month on `day_of_month` (1..=31, clamped to the month's last day)
    /// at `hour:minute`.
    Monthly,
    /// Exactly once on `date` at `hour:minute` (catch-up if the process was
    /// down that day); auto-disables after sending.
    Once,
}

impl ScheduleMode {
    /// The persisted `mode` token (matches the snake_case serde wire form).
    pub fn as_str(self) -> &'static str {
        match self {
            ScheduleMode::Daily => "daily",
            ScheduleMode::EveryNDays => "every_n_days",
            ScheduleMode::Weekly => "weekly",
            ScheduleMode::Monthly => "monthly",
            ScheduleMode::Once => "once",
        }
    }

    /// Parse a persisted `mode` token; an unknown value falls back to `Daily`
    /// (the same default the serde decoder applies), so a corrupt row never
    /// panics the daemon.
    pub fn from_token(s: &str) -> ScheduleMode {
        match s {
            "every_n_days" => ScheduleMode::EveryNDays,
            "weekly" => ScheduleMode::Weekly,
            "monthly" => ScheduleMode::Monthly,
            "once" => ScheduleMode::Once,
            _ => ScheduleMode::Daily,
        }
    }
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
    /// `Weekly` send day, 0=Sunday..6=Saturday.
    #[serde(default = "default_weekday")]
    pub weekday: u8,
    /// `Monthly` send day, 1..=31 (clamped to the month's last day).
    #[serde(default = "default_day_of_month")]
    pub day_of_month: u8,
    /// Optional start gate (`YYYY-MM-DD`): recurring modes stay silent while
    /// the local date is before it.
    #[serde(default)]
    pub start_date: Option<String>,
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
fn default_weekday() -> u8 {
    1 // Monday
}
fn default_day_of_month() -> u8 {
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
            weekday: default_weekday(),
            day_of_month: default_day_of_month(),
            start_date: None,
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

impl ReportSchedule {
    /// Project to the serialization-free DB row. `subscribers` JSON-encodes to
    /// text here (the kernel store stays serde-free); `None` ⇒ NULL ⇒ the
    /// global recipient fallback.
    fn to_row(&self) -> ReportScheduleRow {
        ReportScheduleRow {
            id: self.id.clone(),
            workspace: self.workspace.clone(),
            kind: self.kind.clone(),
            mode: self.mode.as_str().to_string(),
            n_days: self.n_days as i32,
            date: self.date.clone(),
            weekday: i16::from(self.weekday),
            day_of_month: i16::from(self.day_of_month),
            start_date: self.start_date.clone(),
            hour: i16::from(self.hour),
            minute: i16::from(self.minute),
            tz_offset_minutes: self.tz_offset_minutes as i32,
            rig: self.rig.clone(),
            enabled: self.enabled,
            last_sent_date: self.last_sent_date.clone(),
            subscribers: self
                .subscribers
                .as_ref()
                .map(|list| serde_json::to_string(list).unwrap_or_else(|_| "[]".into())),
        }
    }

    /// Rebuild from a DB row. Tolerant of out-of-range scalars (clamped to the
    /// field width) and a corrupt `subscribers` blob (treated as the global
    /// fallback) so one bad row never poisons the daemon's whole tick.
    fn from_row(row: ReportScheduleRow) -> ReportSchedule {
        ReportSchedule {
            id: row.id,
            kind: row.kind,
            mode: ScheduleMode::from_token(&row.mode),
            n_days: row.n_days.max(0) as u32,
            date: row.date,
            weekday: row.weekday.clamp(0, 255) as u8,
            day_of_month: row.day_of_month.clamp(0, 255) as u8,
            start_date: row.start_date,
            hour: row.hour.clamp(0, 255) as u8,
            minute: row.minute.clamp(0, 255) as u8,
            tz_offset_minutes: i64::from(row.tz_offset_minutes),
            rig: row.rig,
            workspace: row.workspace,
            enabled: row.enabled,
            last_sent_date: row.last_sent_date,
            subscribers: row
                .subscribers
                .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok()),
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
    pub weekday: Option<u8>,
    pub day_of_month: Option<u8>,
    pub start_date: Option<String>,
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

/// Stamp the tenant scope onto a write patch (gtcore-00325f H3). On the
/// tenant-bound MCP surface (`Some(ws)`) the patch's `workspace` is forced to
/// the session's: a missing one defaults to it, and a different one is rejected
/// as a cross-tenant write rather than silently honored. `None` (the admin REST
/// surface) leaves the patch untouched so an operator can target any workspace.
fn stamp_scope(scope: Option<&str>, mut patch: SchedulePatch) -> Result<SchedulePatch, String> {
    if let Some(ws) = scope {
        match patch.workspace.as_deref().map(str::trim) {
            Some(req) if !req.is_empty() && req != ws => {
                return Err(format!(
                    "cross-tenant write rejected: schedule workspace `{req}` ≠ session `{ws}`"
                ));
            }
            _ => patch.workspace = Some(ws.to_string()),
        }
    }
    Ok(patch)
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
    if let Some(v) = patch.weekday {
        c.weekday = v;
    }
    if let Some(v) = patch.day_of_month {
        c.day_of_month = v;
    }
    if let Some(v) = patch.start_date {
        c.start_date = if v.trim().is_empty() { None } else { Some(v.trim().to_string()) };
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
        ScheduleMode::Weekly => {
            if s.weekday > 6 {
                return Err("mode `weekly` requires weekday 0-6 (0=Sunday)".into());
            }
        }
        ScheduleMode::Monthly => {
            if !(1..=31).contains(&s.day_of_month) {
                return Err("mode `monthly` requires day_of_month 1-31".into());
            }
        }
        ScheduleMode::Daily => {}
    }
    if let Some(start) = s.start_date.as_deref() {
        if epoch_days(start).is_none() {
            return Err(format!("`start_date` must be YYYY-MM-DD, got `{start}`"));
        }
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

/// Local weekday of a `YYYY-MM-DD` date, 0=Sunday..6=Saturday. Epoch
/// 1970-01-01 was a Thursday (=4), so `(epoch_days + 4) mod 7` indexes it.
fn weekday_of(date: &str) -> Option<u8> {
    epoch_days(date).map(|d| ((d + 4).rem_euclid(7)) as u8)
}

/// Number of days in `month` (1..=12) of `year`, honoring leap years
/// (proleptic Gregorian).
fn days_in_month(year: i64, month: u32) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Parse the `(year, month, day)` of a `YYYY-MM-DD` date.
fn ymd(date: &str) -> Option<(i64, u32, u32)> {
    let mut it = date.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    Some((y, m, d))
}

/// Fire decision, pure for tests.
pub fn is_due(s: &ReportSchedule, local_date: &str, minutes_now: i64) -> bool {
    if !s.enabled {
        return false;
    }
    let time_reached = minutes_now >= i64::from(s.hour) * 60 + i64::from(s.minute);
    let sent_today = s.last_sent_date.as_deref() == Some(local_date);
    // Optional start gate, applied to the recurring modes only (`once` carries
    // its own target date). Silent while the local date is before it.
    let before_start = |local_date: &str| match s.start_date.as_deref() {
        Some(start) => match (epoch_days(local_date), epoch_days(start)) {
            (Some(today), Some(s0)) => today < s0,
            _ => false,
        },
        None => false,
    };
    match s.mode {
        ScheduleMode::Daily => time_reached && !sent_today && !before_start(local_date),
        ScheduleMode::EveryNDays => {
            if before_start(local_date) {
                return false;
            }
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
        ScheduleMode::Weekly => {
            if !time_reached || sent_today || before_start(local_date) {
                return false;
            }
            // Fire when today's weekday matches the configured one (the
            // sent-today guard above prevents a second send the same day).
            weekday_of(local_date) == Some(s.weekday)
        }
        ScheduleMode::Monthly => {
            if !time_reached || sent_today || before_start(local_date) {
                return false;
            }
            let Some((year, month, day)) = ymd(local_date) else {
                return false;
            };
            // Clamp the target day to the month's length: day 31 in a 30-day
            // month (or 29/30/31 in February) fires on the last day instead.
            let last = days_in_month(year, month);
            let target = u32::from(s.day_of_month.min(last));
            if day != target {
                return false;
            }
            // Guard against a second send within the same calendar month
            // (e.g. clamp made several days "the target"): compare year-month.
            match s.last_sent_date.as_deref().and_then(ymd) {
                Some((ly, lm, _)) => (ly, lm) != (year, month),
                None => true,
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
    /// Shared single-tenant tracker AND the multi-tenant fallback (the row
    /// source for a schedule whose workspace has no dedicated pool).
    dolt: Arc<DoltIssues>,
    /// Per-workspace Dolt pools for multi-tenant routing (mirrors
    /// `ReportHandler`'s `dolt_workspaces`). `Some` ⇒ each schedule reads its
    /// own workspace DB; `None` ⇒ single-tenant, everything reads `dolt`.
    dolt_workspaces: Option<Arc<WorkspacePools>>,
    pool: PgPool,
    /// Per-workspace Postgres pools (gtcore-01bcf2) — the source of the bead/epic
    /// comments the digest folds into the report. `None` ⇒ no PG (comments are
    /// skipped, the digest renders without them rather than failing).
    ws_pools: Option<Arc<WsPools>>,
    /// The durable, DB-backed schedule store (gtcore-915232): the schedule LIST
    /// now lives in `report_schedules` (Postgres), NOT `system_config.json` on
    /// an ephemeral path — so a redeploy keeps schedules and `last_sent_date`.
    schedules: Arc<dyn ReportSchedulesRepository>,
}

impl ReportService {
    pub fn new(
        dolt: Arc<DoltIssues>,
        dolt_workspaces: Option<Arc<WorkspacePools>>,
        pool: PgPool,
        ws_pools: Option<Arc<WsPools>>,
    ) -> Self {
        let schedules = Arc::new(PgReportSchedules::new(pool.clone()));
        Self::with_schedules(dolt, dolt_workspaces, pool, ws_pools, schedules)
    }

    /// Build over an explicit schedule store. Production wires the Postgres
    /// `PgReportSchedules` (via [`ReportService::new`]); the CRUD-scoping tests
    /// inject an in-memory `ReportSchedulesRepository` so they exercise the edge
    /// without a Postgres socket (gtcore-915232).
    pub fn with_schedules(
        dolt: Arc<DoltIssues>,
        dolt_workspaces: Option<Arc<WorkspacePools>>,
        pool: PgPool,
        ws_pools: Option<Arc<WsPools>>,
        schedules: Arc<dyn ReportSchedulesRepository>,
    ) -> Self {
        Self { dolt, dolt_workspaces, pool, ws_pools, schedules }
    }

    /// Load the bead/epic comments for the report rows from the schedule's
    /// per-workspace Postgres schema, mapped to [`ReportComment`] keyed by bead
    /// id (gtcore-01bcf2). Best-effort: with no PG pools, or on a query error,
    /// returns an empty map so the digest still sends (comments are additive).
    async fn comments_for(
        &self,
        workspace: &str,
        rows: &[gt_store_dolt::IssueRow],
    ) -> std::collections::HashMap<String, Vec<ReportComment>> {
        let Some(ws_pools) = &self.ws_pools else {
            return std::collections::HashMap::new();
        };
        let ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let pool = match ws_pools.get(Some(workspace)).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[report-scheduler] comments pool for `{workspace}`: {e}");
                return std::collections::HashMap::new();
            }
        };
        let loaded = match PgComments::new(pool).list_for_cards(&ids).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[report-scheduler] load comments for `{workspace}`: {e}");
                return std::collections::HashMap::new();
            }
        };
        loaded
            .into_iter()
            .map(|(id, cs)| {
                let mapped = cs
                    .into_iter()
                    .map(|c| ReportComment {
                        author: c.author,
                        fecha: c.created_at.format("%Y-%m-%d").to_string(),
                        body: c.body,
                    })
                    .collect();
                (id, mapped)
            })
            .collect()
    }

    /// Resolve the Dolt tracker for a schedule's workspace — the per-workspace
    /// pool when multi-tenant routing is on, else the shared `dolt`. This makes
    /// the digest read the SAME database `report.generate` routes to
    /// (gtcore-252885: scheduled/test digests for a non-default workspace were
    /// reading the shared default DB → empty).
    ///
    /// Uses the lazy [`WorkspacePools::pool_for`] (not `ensured_pool`): the
    /// digest is a READ over an existing board, whose schema was already
    /// provisioned by the `report.generate`/`board.list` path — so there is no
    /// schema to self-heal here, and the lazy handle keeps this offline-safe.
    fn tracker(&self, workspace: &str) -> Result<Arc<DoltIssues>, String> {
        match &self.dolt_workspaces {
            Some(pools) => {
                let pool = pools
                    .pool_for(workspace)
                    .map_err(|e| format!("dolt pool for `{workspace}`: {e}"))?;
                Ok(Arc::new(DoltIssues::new(pool)))
            }
            None => Ok(self.dolt.clone()),
        }
    }

    /// The global subscribers store over the shared public pool.
    pub fn subscribers(&self) -> PgReportSubscriptions {
        PgReportSubscriptions::new(self.pool.clone())
    }

    /// All schedules visible to `scope` (gtcore-00325f H3 multi-tenant):
    /// `Some(ws)` restricts to that workspace's schedules; `None` is the
    /// unscoped admin view (every workspace), used by the System REST surface.
    /// Reads straight from the DB (gtcore-915232) on every call — no in-memory
    /// snapshot — so a peer process's write is visible immediately. A store
    /// error surfaces as an empty list with a log (the surfaces tolerate it the
    /// same way the pre-DB read of an absent config did).
    pub async fn list_schedules(&self, scope: Option<&str>) -> Vec<ReportSchedule> {
        match self.schedules.list(scope).await {
            Ok(rows) => rows.into_iter().map(ReportSchedule::from_row).collect(),
            Err(e) => {
                eprintln!("[report-scheduler] list schedules ({scope:?}): {e}");
                Vec::new()
            }
        }
    }

    /// One-shot migration (gtcore-8ff13e): seed the durable DB-backed store from
    /// a legacy `system_config.json` schedule list, ONCE, when the table is still
    /// empty. Pre-DB deployments persisted the schedule list to that file on an
    /// ephemeral path (the bead's root cause), so on the first boot against the
    /// Postgres store we import whatever the file still holds rather than silently
    /// lose it. Gated on an EMPTY store, which makes it:
    ///   - idempotent across redeploys — every boot after the first finds rows and
    ///     does nothing, so it never double-inserts;
    ///   - non-destructive — it never resurrects a schedule an operator later
    ///     deleted (that leaves the store non-empty, or empty-by-intent which an
    ///     absent/empty file then leaves alone).
    /// Each schedule is inserted verbatim — `id`, `workspace` and `last_sent_date`
    /// preserved — so the per-workspace placement and the at-most-once send guard
    /// migrate intact. Unscoped (`None`): the operator file may span tenants.
    /// Returns the number imported (0 when the store already holds rows or the
    /// file carried none).
    pub async fn import_file_schedules(
        &self,
        schedules: &[ReportSchedule],
    ) -> Result<usize, String> {
        if schedules.is_empty() {
            return Ok(0);
        }
        // One-shot gate: only ever seed a still-empty store.
        let existing = self
            .schedules
            .list(None)
            .await
            .map_err(|e| format!("import: probe store: {e}"))?;
        if !existing.is_empty() {
            return Ok(0);
        }
        let mut imported = 0;
        for s in schedules {
            self.schedules
                .insert(&s.to_row())
                .await
                .map_err(|e| format!("import schedule `{}`: {e}", s.id))?;
            imported += 1;
        }
        Ok(imported)
    }

    /// Create a schedule from a patch over the defaults; persisted to the DB on
    /// success. When `scope` is `Some(ws)` (the tenant-bound MCP surface) the
    /// schedule's workspace is STAMPED to the session's — a cross-tenant
    /// `workspace` in the patch is rejected rather than silently honored, so a
    /// schedule can only ever land in the actor's own tenant.
    pub async fn create_schedule(
        &self,
        scope: Option<&str>,
        patch: SchedulePatch,
    ) -> Result<ReportSchedule, String> {
        let patch = stamp_scope(scope, patch)?;
        let mut s = ReportSchedule::default();
        apply_patch(&mut s, patch)?;
        self.schedules
            .insert(&s.to_row())
            .await
            .map_err(|e| format!("persist schedule: {e}"))?;
        Ok(s)
    }

    /// Patch one schedule by id; `last_sent_date`/`id` untouched by design.
    /// Tenant-scoped (`Some(ws)`): a schedule outside the actor's workspace is
    /// invisible (reads as "unknown schedule"), and the patch cannot move it to
    /// another workspace. Read-modify-write against the DB.
    pub async fn update_schedule(
        &self,
        scope: Option<&str>,
        id: &str,
        patch: SchedulePatch,
    ) -> Result<ReportSchedule, String> {
        let patch = stamp_scope(scope, patch)?;
        // Load the current row within scope (an out-of-scope id is invisible).
        let mut current = self
            .list_schedules(scope)
            .await
            .into_iter()
            .find(|s| s.id == id)
            .ok_or_else(|| format!("unknown schedule `{id}`"))?;
        apply_patch(&mut current, patch)?;
        self.schedules
            .update(scope, &current.to_row())
            .await
            .map_err(|e| match e {
                ReportScheduleError::NotFound(id) => format!("unknown schedule `{id}`"),
                other => format!("persist schedule: {other}"),
            })?;
        Ok(current)
    }

    /// Remove one schedule by id; persisted on success. Tenant-scoped: a
    /// schedule outside the actor's workspace is not deletable (and reads as
    /// unknown).
    pub async fn delete_schedule(&self, scope: Option<&str>, id: &str) -> Result<(), String> {
        self.schedules.delete(scope, id).await.map_err(|e| match e {
            ReportScheduleError::NotFound(id) => format!("unknown schedule `{id}`"),
            other => format!("delete schedule: {other}"),
        })
    }

    /// Daemon bookkeeping after a fire: stamp `last_sent_date` and, when
    /// `disable` (a `once` schedule that just sent), flip `enabled` off — both in
    /// one DB statement (gtcore-915232) so the at-most-once guard and the
    /// auto-disable persist together and survive a redeploy. Unscoped: the
    /// daemon spans every tenant.
    pub async fn stamp_sent(
        &self,
        id: &str,
        last_sent_date: &str,
        disable: bool,
    ) -> Result<(), String> {
        self.schedules
            .stamp_sent(id, last_sent_date, disable)
            .await
            .map_err(|e| format!("stamp last_sent_date: {e}"))
    }

    /// Resolve the send target: an explicit id, or the single existing
    /// schedule when unambiguous. Tenant-scoped: only the actor's workspace
    /// schedules are visible, so the "exactly one" shortcut is per-tenant and an
    /// id outside the tenant reads as unknown.
    pub async fn resolve_schedule(
        &self,
        scope: Option<&str>,
        id: Option<&str>,
    ) -> Result<ReportSchedule, String> {
        let schedules = self.list_schedules(scope).await;
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

    /// Build + render ONE schedule's report and enqueue a SINGLE outbox row —
    /// To: the configured sender, every recipient (the schedule's own list,
    /// else the workspace's enabled globals) in CC. Returns the number of
    /// emails queued (0 with no recipients, else 1). Never touches
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

        // The same rows board.list / report.generate read (full=true for Notas),
        // from the SAME per-workspace Dolt DB report.generate routes to.
        let tracker = self.tracker(&schedule.workspace)?;
        let rows = tracker
            .list(&IssueFilter {
                rig: Some(schedule.rig.clone()),
                workspace: Some(schedule.workspace.clone()),
                full: true,
                limit: Some(gt_store_dolt::issues_max_limit()),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("tracker rows: {e}"))?;
        let parent_map = tracker
            .parent_map(&schedule.rig, &schedule.workspace)
            .await
            .map_err(|e| format!("parent_map: {e}"))?;
        // Bead/epic comments live in the per-workspace PG schema (gtcore-01bcf2),
        // folded into the report so the digest's Notas column carries them.
        let comments = self.comments_for(&schedule.workspace, &rows).await;
        let report =
            build_report(&schedule.rig, &schedule.workspace, &rows, &parent_map, &comments);
        let (today, _) = local_now(schedule.tz_offset_minutes);
        // reopens=0: the audit-derived count lives with the analytics handler;
        // the digest tolerates the conservative zero (defects still counted).
        let summary = summarize(&schedule.rig, &schedule.workspace, &rows, 0, &today, 7, 30, &parent_map);
        let (subject, html) = render(&report, &summary, &today);

        // One email To: the configured sender (`GT_SMTP_FROM`) with every
        // registered recipient in CC (gtcore-ecf70d) — instead of one outbox row
        // per recipient. Without an SMTP sender (dev/log transport) fall back to
        // the first recipient as To and the rest in CC, so the single-email
        // shape still holds.
        let (to, cc): (String, Vec<String>) = match std::env::var("GT_SMTP_FROM") {
            Ok(from) if !from.trim().is_empty() => (from.trim().to_string(), recipients),
            _ => {
                let mut it = recipients.into_iter();
                let first = it.next().expect("recipients checked non-empty above");
                (first, it.collect())
            }
        };

        let outbox = PgEmailOutbox::new(self.pool.clone());
        outbox
            .enqueue(NewEmail {
                id: ulid::Ulid::new().to_string(),
                workspace: schedule.workspace.clone(),
                recipient: to,
                cc,
                subject,
                body: html,
                template_ref: Some(format!("report:{}", schedule.kind)),
                send_at: None,
                created_by: created_by.to_string(),
            })
            .await
            .map_err(|e| format!("outbox enqueue failed: {e}"))?;
        // One email queued (carrying the whole CC list), not one per recipient.
        Ok(1)
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
            // Read every tenant's schedules straight from the DB each tick
            // (gtcore-915232) — unscoped (`None`), so the daemon spans all
            // workspaces, and a CRUD edit by any process lands on the next tick
            // without a restart.
            let schedules = self.service.list_schedules(None).await;
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
                        // send-now). Once auto-disables: exactly-one send. Both
                        // persist to the DB in one statement (gtcore-915232), so
                        // the at-most-once guard survives a redeploy.
                        let disable = schedule.mode == ScheduleMode::Once;
                        if let Err(e) =
                            self.service.stamp_sent(&schedule.id, &local_date, disable).await
                        {
                            eprintln!(
                                "[report-scheduler] `{}` stamp last_sent_date failed: {e}",
                                schedule.id
                            );
                        }
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
    fn weekly_fires_on_its_weekday_once() {
        // TODAY (2026-06-12) is a Friday → weekday 5 (0=Sunday).
        assert_eq!(weekday_of(TODAY), Some(5));
        let mut s = sched(ScheduleMode::Weekly);
        s.weekday = 5;
        // Matching weekday, time reached: fires.
        assert!(!is_due(&s, TODAY, AT - 1)); // time not reached
        assert!(is_due(&s, TODAY, AT));
        // Already sent today: no second send.
        s.last_sent_date = Some(TODAY.into());
        assert!(!is_due(&s, TODAY, 23 * 60));
        // Wrong weekday: silent (2026-06-14 is a Sunday → weekday 0).
        s.last_sent_date = None;
        assert_eq!(weekday_of("2026-06-14"), Some(0));
        assert!(!is_due(&s, "2026-06-14", 23 * 60));
        // Configure for Sunday: now that day fires.
        s.weekday = 0;
        assert!(is_due(&s, "2026-06-14", AT));
    }

    #[test]
    fn monthly_fires_on_day_with_end_of_month_clamp_and_same_month_guard() {
        let mut s = sched(ScheduleMode::Monthly);
        // Normal day: fires only on that day, once time is reached.
        s.day_of_month = 12;
        assert!(!is_due(&s, TODAY, AT - 1));
        assert!(is_due(&s, TODAY, AT)); // 2026-06-12
        assert!(!is_due(&s, "2026-06-11", AT));
        // Same-month guard: already sent this month → no second send even on
        // the target day.
        s.last_sent_date = Some("2026-06-01".into());
        assert!(!is_due(&s, TODAY, AT));
        s.last_sent_date = None;
        // End-of-month clamp: day 31 in a 30-day month (April) → fires on the
        // 30th, not the (nonexistent) 31st.
        s.day_of_month = 31;
        assert!(is_due(&s, "2026-04-30", AT));
        assert!(!is_due(&s, "2026-04-29", AT));
        // February clamp (non-leap 2026 has 28 days): day 31 → the 28th.
        assert!(is_due(&s, "2026-02-28", AT));
        // Leap February (2024 has 29 days): day 31 → the 29th.
        assert!(is_due(&s, "2024-02-29", AT));
        assert!(!is_due(&s, "2024-02-28", AT));
    }

    #[test]
    fn start_date_gates_the_recurring_modes() {
        // Daily silent before start_date, due on/after it.
        let mut s = sched(ScheduleMode::Daily);
        s.start_date = Some("2026-06-15".into());
        assert!(!is_due(&s, TODAY, AT)); // 2026-06-12 < start
        assert!(is_due(&s, "2026-06-15", AT)); // == start
        assert!(is_due(&s, "2026-06-16", AT)); // > start
        // every_n_days honors the gate too.
        let mut s = sched(ScheduleMode::EveryNDays);
        s.n_days = 1;
        s.start_date = Some("2026-06-15".into());
        assert!(!is_due(&s, TODAY, AT));
        assert!(is_due(&s, "2026-06-15", AT));
        // Weekly honors the gate: matching weekday but before start → silent.
        let mut s = sched(ScheduleMode::Weekly);
        s.weekday = 5; // Friday, matches 2026-06-12
        s.start_date = Some("2026-06-15".into());
        assert!(!is_due(&s, TODAY, AT));
        // Monthly honors the gate.
        let mut s = sched(ScheduleMode::Monthly);
        s.day_of_month = 12;
        s.start_date = Some("2026-07-01".into());
        assert!(!is_due(&s, TODAY, AT));
        assert!(is_due(&s, "2026-07-12", AT));
    }

    #[test]
    fn weekly_monthly_validate_and_round_trip() {
        // Validation rejects out-of-range weekday / day_of_month.
        let base = ReportSchedule::default();
        let try_patch = |p: SchedulePatch| {
            let mut s = base.clone();
            apply_patch(&mut s, p)
        };
        assert!(try_patch(SchedulePatch {
            mode: Some(ScheduleMode::Weekly),
            weekday: Some(7),
            ..Default::default()
        })
        .unwrap_err()
        .contains("weekday 0-6"));
        assert!(try_patch(SchedulePatch {
            mode: Some(ScheduleMode::Monthly),
            day_of_month: Some(0),
            ..Default::default()
        })
        .unwrap_err()
        .contains("day_of_month 1-31"));
        // Bad start_date format is rejected.
        assert!(try_patch(SchedulePatch {
            start_date: Some("06/2026".into()),
            ..Default::default()
        })
        .unwrap_err()
        .contains("start_date"));
        // Serde round-trip of the new fields.
        let s = ReportSchedule {
            mode: ScheduleMode::Weekly,
            weekday: 3,
            day_of_month: 28,
            start_date: Some("2026-06-15".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&s).expect("encode");
        let back: ReportSchedule = serde_json::from_str(&json).expect("decode");
        assert_eq!(back.mode, ScheduleMode::Weekly);
        assert_eq!(back.weekday, 3);
        assert_eq!(back.day_of_month, 28);
        assert_eq!(back.start_date.as_deref(), Some("2026-06-15"));
        // Wire values are snake_case.
        assert!(serde_json::to_string(&ScheduleMode::Weekly).unwrap().contains("weekly"));
        assert!(serde_json::to_string(&ScheduleMode::Monthly).unwrap().contains("monthly"));
        // Defaults for an empty object hold the new fields.
        let d: ReportSchedule = serde_json::from_str("{}").expect("empty");
        assert_eq!(d.weekday, 1);
        assert_eq!(d.day_of_month, 1);
        assert!(d.start_date.is_none());
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

    // --- per-workspace tracker routing (gtcore-252885) -----------------------
    // Both pure: mysql_async + sqlx pools are lazy, so no socket opens — the
    // same "lazy pool handle" seam multitenant_rbac.rs exercises.

    /// In-memory `ReportSchedulesRepository` for the CRUD-scoping tests — the
    /// same `scope` semantics as `PgReportSchedules` (gtcore-915232) with no
    /// Postgres socket. Insertion order stands in for the `created_at` ordering.
    #[derive(Default)]
    struct InMemorySchedules {
        rows: tokio::sync::Mutex<Vec<ReportScheduleRow>>,
    }

    #[async_trait::async_trait]
    impl ReportSchedulesRepository for InMemorySchedules {
        async fn list(
            &self,
            scope: Option<&str>,
        ) -> Result<Vec<ReportScheduleRow>, ReportScheduleError> {
            let rows = self.rows.lock().await;
            Ok(rows
                .iter()
                .filter(|r| scope.map_or(true, |ws| r.workspace == ws))
                .cloned()
                .collect())
        }

        async fn insert(&self, row: &ReportScheduleRow) -> Result<(), ReportScheduleError> {
            self.rows.lock().await.push(row.clone());
            Ok(())
        }

        async fn update(
            &self,
            scope: Option<&str>,
            row: &ReportScheduleRow,
        ) -> Result<(), ReportScheduleError> {
            let mut rows = self.rows.lock().await;
            let slot = rows
                .iter_mut()
                .find(|r| r.id == row.id && scope.map_or(true, |ws| r.workspace == ws))
                .ok_or_else(|| ReportScheduleError::NotFound(row.id.clone()))?;
            // `last_sent_date` is daemon bookkeeping the CRUD path never writes
            // (mirrors the SQL UPDATE column set, which omits it).
            let keep = slot.last_sent_date.clone();
            *slot = row.clone();
            slot.last_sent_date = keep;
            Ok(())
        }

        async fn delete(&self, scope: Option<&str>, id: &str) -> Result<(), ReportScheduleError> {
            let mut rows = self.rows.lock().await;
            let before = rows.len();
            rows.retain(|r| !(r.id == id && scope.map_or(true, |ws| r.workspace == ws)));
            if rows.len() == before {
                return Err(ReportScheduleError::NotFound(id.to_string()));
            }
            Ok(())
        }

        async fn stamp_sent(
            &self,
            id: &str,
            last_sent_date: &str,
            disable: bool,
        ) -> Result<(), ReportScheduleError> {
            let mut rows = self.rows.lock().await;
            if let Some(r) = rows.iter_mut().find(|r| r.id == id) {
                r.last_sent_date = Some(last_sent_date.to_string());
                if disable {
                    r.enabled = false;
                }
            }
            Ok(())
        }
    }

    fn service(dolt_workspaces: Option<Arc<WorkspacePools>>) -> (ReportService, Arc<DoltIssues>) {
        let dolt = Arc::new(
            DoltIssues::connect("mysql://root@127.0.0.1:3307/hq").expect("lazy dolt pool"),
        );
        let svc = ReportService::with_schedules(
            dolt.clone(),
            dolt_workspaces,
            PgPool::connect_lazy("postgres://gt@127.0.0.1:1/none").expect("lazy pg pool"),
            None,
            Arc::new(InMemorySchedules::default()),
        );
        (svc, dolt)
    }

    // `#[tokio::test]`: sqlx's `connect_lazy` spawns the pool keeper, so it
    // needs a runtime in scope even though no socket is opened.
    #[tokio::test]
    async fn tracker_without_pools_reuses_the_shared_dolt() {
        let (svc, dolt) = service(None);
        let got = svc.tracker("cotrafa").expect("tracker");
        // Single-tenant: every workspace reads the one shared tracker.
        assert!(Arc::ptr_eq(&got, &dolt));
    }

    #[tokio::test]
    async fn tracker_with_pools_routes_per_workspace_not_the_shared_default() {
        let pools = Arc::new(
            WorkspacePools::from_url("mysql://root@127.0.0.1:3307/").expect("base url"),
        );
        let (svc, dolt) = service(Some(pools));
        // The gtcore-252885 bug: a non-default workspace fell through to the
        // shared default DB → empty digest. It must now route to its own pool.
        let got = svc.tracker("cotrafa").expect("tracker");
        assert!(!Arc::ptr_eq(&got, &dolt));
    }

    // --- one-shot file→DB migration (gtcore-8ff13e) --------------------------

    #[tokio::test]
    async fn import_seeds_empty_store_then_is_idempotent_across_redeploys() {
        let (svc, _dolt) = service(None);
        let legacy = vec![
            ReportSchedule {
                id: "sched-a".into(),
                workspace: "default".into(),
                last_sent_date: Some("2026-06-20".into()),
                ..Default::default()
            },
            ReportSchedule { id: "sched-b".into(), workspace: "cotrafa".into(), ..Default::default() },
        ];
        // First boot: empty store ⇒ both imported.
        assert_eq!(svc.import_file_schedules(&legacy).await.expect("import"), 2);
        let all = svc.list_schedules(None).await;
        assert_eq!(all.len(), 2);
        // The at-most-once guard migrated with the row…
        let a = all.iter().find(|s| s.id == "sched-a").expect("sched-a present");
        assert_eq!(a.last_sent_date.as_deref(), Some("2026-06-20"));
        // …and each schedule landed in its OWN tenant (multi-tenant preserved).
        assert_eq!(svc.list_schedules(Some("cotrafa")).await.len(), 1);
        assert_eq!(svc.list_schedules(Some("default")).await.len(), 1);
        // Redeploy: store non-empty ⇒ no-op, no duplicates.
        assert_eq!(svc.import_file_schedules(&legacy).await.expect("re-import"), 0);
        assert_eq!(svc.list_schedules(None).await.len(), 2);
    }

    #[tokio::test]
    async fn import_with_no_legacy_schedules_is_a_noop() {
        let (svc, _dolt) = service(None);
        assert_eq!(svc.import_file_schedules(&[]).await.expect("noop"), 0);
        assert!(svc.list_schedules(None).await.is_empty());
    }

    // --- per-workspace CRUD scoping (gtcore-00325f H3) -----------------------
    // The service runs on an in-memory `ArchiveConfig` with no `config_path`, so
    // `persist()` is a no-op and the CRUD operates purely on the config list.

    fn create_patch(ws: &str) -> SchedulePatch {
        SchedulePatch { workspace: Some(ws.into()), ..Default::default() }
    }

    #[tokio::test]
    async fn create_stamps_session_workspace_over_a_blank_patch() {
        let (svc, _) = service(None);
        // No workspace in the patch: the session's is stamped, not the default.
        let s = svc
            .create_schedule(Some("cotrafa"), SchedulePatch::default())
            .await
            .expect("create");
        assert_eq!(s.workspace, "cotrafa");
    }

    #[tokio::test]
    async fn create_rejects_a_cross_tenant_workspace() {
        let (svc, _) = service(None);
        let err = svc
            .create_schedule(Some("cotrafa"), create_patch("default"))
            .await
            .expect_err("cross-tenant create must be rejected");
        assert!(err.contains("cross-tenant"), "got: {err}");
        // Nothing leaked into the config.
        assert!(svc.list_schedules(None).await.is_empty());
    }

    #[tokio::test]
    async fn list_is_isolated_by_workspace() {
        let (svc, _) = service(None);
        svc.create_schedule(Some("cotrafa"), SchedulePatch::default()).await.expect("c");
        svc.create_schedule(Some("default"), SchedulePatch::default()).await.expect("d");

        // The cotrafa schedule appears only in the cotrafa scope, never default
        // (the incident this bead closes), and vice versa.
        let cotrafa = svc.list_schedules(Some("cotrafa")).await;
        assert_eq!(cotrafa.len(), 1);
        assert_eq!(cotrafa[0].workspace, "cotrafa");
        let default = svc.list_schedules(Some("default")).await;
        assert_eq!(default.len(), 1);
        assert_eq!(default[0].workspace, "default");
        // The admin (unscoped) view sees both.
        assert_eq!(svc.list_schedules(None).await.len(), 2);
    }

    #[tokio::test]
    async fn update_and_delete_cannot_cross_the_tenant_boundary() {
        let (svc, _) = service(None);
        let other = svc
            .create_schedule(Some("cotrafa"), SchedulePatch::default())
            .await
            .expect("create cotrafa");

        // A default-scoped actor cannot see, patch, or delete a cotrafa schedule.
        let upd = svc
            .update_schedule(Some("default"), &other.id, SchedulePatch {
                hour: Some(9),
                ..Default::default()
            })
            .await;
        assert!(upd.unwrap_err().contains("unknown schedule"));
        let del = svc.delete_schedule(Some("default"), &other.id).await;
        assert!(del.unwrap_err().contains("unknown schedule"));
        // The schedule survived untouched.
        let live = svc.list_schedules(Some("cotrafa")).await;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].hour, other.hour);

        // The owning tenant CAN patch it, but not relocate it to another tenant.
        svc.update_schedule(Some("cotrafa"), &other.id, SchedulePatch {
            hour: Some(9),
            ..Default::default()
        })
        .await
        .expect("owner update");
        let relocate = svc
            .update_schedule(Some("cotrafa"), &other.id, create_patch("default"))
            .await;
        assert!(relocate.unwrap_err().contains("cross-tenant"));
        // In scope, the patched hour took; the workspace never moved.
        let live = svc.list_schedules(Some("cotrafa")).await;
        assert_eq!(live[0].hour, 9);
        assert_eq!(live[0].workspace, "cotrafa");
    }

    #[tokio::test]
    async fn resolve_is_per_tenant_unambiguous() {
        let (svc, _) = service(None);
        let c = svc.create_schedule(Some("cotrafa"), SchedulePatch::default()).await.expect("c");
        svc.create_schedule(Some("default"), SchedulePatch::default()).await.expect("d");

        // Two schedules exist globally, but each tenant has exactly one — so the
        // "single schedule" send-now shortcut resolves per-tenant.
        let r = svc.resolve_schedule(Some("cotrafa"), None).await.expect("resolve cotrafa");
        assert_eq!(r.id, c.id);
        // An explicit id from another tenant is invisible.
        assert!(svc
            .resolve_schedule(Some("default"), Some(&c.id))
            .await
            .unwrap_err()
            .contains("unknown schedule"));
        // Unscoped admin sees the ambiguity.
        assert!(svc.resolve_schedule(None, None).await.unwrap_err().contains("pass schedule_id"));
    }
}
