//! `ReportSchedulesRepository` — the DB-backed home of the scheduled-report
//! schedule LIST (gtcore-915232, epic gtcore-68bcf7).
//!
//! Before this, the list lived in `system_config.json` on an ephemeral path:
//! every redeploy lost it, and it was single-file (not multi-tenant). This is
//! the durable, workspace-keyed store the MCP `report.schedules.*` tools, the
//! System REST CRUD, and the scheduler daemon all read/write through the
//! composition `ReportService`.
//!
//! Serialization-free like its `report_subscriptions` sibling: the row carries
//! plain scalars, and the composition edge maps it to/from the wire
//! `ReportSchedule`. The per-schedule `subscribers` list rides as a
//! JSON-encoded `String` the edge owns (NULL/None ⇒ global fallback), so the
//! kernel store needs no serde.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::PgPool;

/// One persisted schedule row — every column of `report_schedules`, in the
/// `ReportSchedule` field order. `subscribers` is the raw text the composition
/// edge JSON-encodes; `None` ⇒ NULL ⇒ the global recipient fallback.
#[derive(Debug, Clone)]
pub struct ReportScheduleRow {
    pub id: String,
    pub workspace: String,
    pub kind: String,
    pub mode: String,
    pub n_days: i32,
    pub date: Option<String>,
    pub weekday: i16,
    pub day_of_month: i16,
    pub start_date: Option<String>,
    pub hour: i16,
    pub minute: i16,
    pub tz_offset_minutes: i32,
    pub rig: String,
    pub enabled: bool,
    pub last_sent_date: Option<String>,
    pub subscribers: Option<String>,
}

/// What can go wrong against the report-schedules store.
#[derive(Debug, thiserror::Error)]
pub enum ReportScheduleError {
    /// No matching schedule (update/delete of an unknown — or out-of-scope —
    /// id). The string is the id the caller passed.
    #[error("unknown schedule `{0}`")]
    NotFound(String),
    /// The backend failed.
    #[error("report schedules db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// The report-schedules port. `scope` mirrors the composition `ReportService`:
/// `Some(ws)` restricts to that workspace's rows (the tenant-bound MCP
/// surface); `None` is the unscoped admin view (the System REST surface) that
/// spans every workspace.
#[async_trait]
pub trait ReportSchedulesRepository: Send + Sync {
    /// Schedules visible to `scope`, stable `created_at` order.
    async fn list(&self, scope: Option<&str>) -> Result<Vec<ReportScheduleRow>, ReportScheduleError>;
    /// Insert a new schedule (the row's `id` is the caller's ulid).
    async fn insert(&self, row: &ReportScheduleRow) -> Result<(), ReportScheduleError>;
    /// Overwrite an existing schedule in-place by id, honoring `scope`: an
    /// out-of-scope (or absent) id is `NotFound`, never a silent no-op. The
    /// row's own `workspace` is written (the edge already enforced the tenant
    /// stamp), but the WHERE matches the prior scope so a cross-tenant id stays
    /// invisible.
    async fn update(
        &self,
        scope: Option<&str>,
        row: &ReportScheduleRow,
    ) -> Result<(), ReportScheduleError>;
    /// Delete one schedule by id within `scope`; `NotFound` when nothing matched.
    async fn delete(&self, scope: Option<&str>, id: &str) -> Result<(), ReportScheduleError>;
    /// The daemon's at-most-once bookkeeping: stamp `last_sent_date` and, when
    /// `disable` (a `once` schedule that just fired), flip `enabled` off — in one
    /// statement so the guard and the auto-disable persist together. Unscoped:
    /// the daemon spans all tenants.
    async fn stamp_sent(
        &self,
        id: &str,
        last_sent_date: &str,
        disable: bool,
    ) -> Result<(), ReportScheduleError>;
}

/// Postgres adapter over the shared public-schema pool (the same pool
/// `report_subscriptions` uses).
pub struct PgReportSchedules {
    pool: PgPool,
}

impl PgReportSchedules {
    /// Wrap the shared pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// The full column list, in `ReportScheduleRow` field order — shared by SELECT
/// projection and the row decoder so they never drift.
const COLS: &str = "id, workspace, kind, mode, n_days, date, weekday, day_of_month, \
     start_date, hour, minute, tz_offset_minutes, rig, enabled, last_sent_date, subscribers";

type RowTuple = (
    String,
    String,
    String,
    String,
    i32,
    Option<String>,
    i16,
    i16,
    Option<String>,
    i16,
    i16,
    i32,
    String,
    bool,
    Option<String>,
    Option<String>,
);

fn from_tuple(t: RowTuple) -> ReportScheduleRow {
    ReportScheduleRow {
        id: t.0,
        workspace: t.1,
        kind: t.2,
        mode: t.3,
        n_days: t.4,
        date: t.5,
        weekday: t.6,
        day_of_month: t.7,
        start_date: t.8,
        hour: t.9,
        minute: t.10,
        tz_offset_minutes: t.11,
        rig: t.12,
        enabled: t.13,
        last_sent_date: t.14,
        subscribers: t.15,
    }
}

#[async_trait]
impl ReportSchedulesRepository for PgReportSchedules {
    async fn list(
        &self,
        scope: Option<&str>,
    ) -> Result<Vec<ReportScheduleRow>, ReportScheduleError> {
        let rows: Vec<RowTuple> = match scope {
            Some(ws) => {
                sqlx::query_as(&format!(
                    "SELECT {COLS} FROM report_schedules WHERE workspace = $1 ORDER BY created_at, id"
                ))
                .bind(ws)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(&format!(
                    "SELECT {COLS} FROM report_schedules ORDER BY created_at, id"
                ))
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.into_iter().map(from_tuple).collect())
    }

    async fn insert(&self, row: &ReportScheduleRow) -> Result<(), ReportScheduleError> {
        sqlx::query(
            "INSERT INTO report_schedules \
             (id, workspace, kind, mode, n_days, date, weekday, day_of_month, start_date, \
              hour, minute, tz_offset_minutes, rig, enabled, last_sent_date, subscribers) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
        )
        .bind(&row.id)
        .bind(&row.workspace)
        .bind(&row.kind)
        .bind(&row.mode)
        .bind(row.n_days)
        .bind(&row.date)
        .bind(row.weekday)
        .bind(row.day_of_month)
        .bind(&row.start_date)
        .bind(row.hour)
        .bind(row.minute)
        .bind(row.tz_offset_minutes)
        .bind(&row.rig)
        .bind(row.enabled)
        .bind(&row.last_sent_date)
        .bind(&row.subscribers)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update(
        &self,
        scope: Option<&str>,
        row: &ReportScheduleRow,
    ) -> Result<(), ReportScheduleError> {
        // `last_sent_date` is daemon bookkeeping, never moved by a CRUD patch —
        // the edge leaves it as the loaded value, but we deliberately do NOT
        // write it here so a concurrent daemon stamp is not clobbered.
        let sql = "UPDATE report_schedules SET \
             workspace = $2, kind = $3, mode = $4, n_days = $5, date = $6, weekday = $7, \
             day_of_month = $8, start_date = $9, hour = $10, minute = $11, \
             tz_offset_minutes = $12, rig = $13, enabled = $14, subscribers = $15 \
             WHERE id = $1";
        let mut q = sqlx::query(sql)
            .bind(&row.id)
            .bind(&row.workspace)
            .bind(&row.kind)
            .bind(&row.mode)
            .bind(row.n_days)
            .bind(&row.date)
            .bind(row.weekday)
            .bind(row.day_of_month)
            .bind(&row.start_date)
            .bind(row.hour)
            .bind(row.minute)
            .bind(row.tz_offset_minutes)
            .bind(&row.rig)
            .bind(row.enabled)
            .bind(&row.subscribers);
        // Tenant guard: a scoped actor can only update rows already in its
        // workspace (so it cannot reach across, nor resurrect a stale id).
        let sql_scoped;
        if let Some(ws) = scope {
            sql_scoped = format!("{sql} AND workspace = $16");
            q = sqlx::query(&sql_scoped)
                .bind(&row.id)
                .bind(&row.workspace)
                .bind(&row.kind)
                .bind(&row.mode)
                .bind(row.n_days)
                .bind(&row.date)
                .bind(row.weekday)
                .bind(row.day_of_month)
                .bind(&row.start_date)
                .bind(row.hour)
                .bind(row.minute)
                .bind(row.tz_offset_minutes)
                .bind(&row.rig)
                .bind(row.enabled)
                .bind(&row.subscribers)
                .bind(ws);
        }
        let res = q.execute(&self.pool).await?;
        if res.rows_affected() == 0 {
            return Err(ReportScheduleError::NotFound(row.id.clone()));
        }
        Ok(())
    }

    async fn delete(&self, scope: Option<&str>, id: &str) -> Result<(), ReportScheduleError> {
        let res = match scope {
            Some(ws) => {
                sqlx::query("DELETE FROM report_schedules WHERE id = $1 AND workspace = $2")
                    .bind(id)
                    .bind(ws)
                    .execute(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("DELETE FROM report_schedules WHERE id = $1")
                    .bind(id)
                    .execute(&self.pool)
                    .await?
            }
        };
        if res.rows_affected() == 0 {
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
        // One statement: stamp the guard date, and (for `once`) flip enabled off
        // so the auto-disable and the at-most-once guard persist together.
        sqlx::query(
            "UPDATE report_schedules \
             SET last_sent_date = $2, enabled = CASE WHEN $3 THEN FALSE ELSE enabled END \
             WHERE id = $1",
        )
        .bind(id)
        .bind(last_sent_date)
        .bind(disable)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
