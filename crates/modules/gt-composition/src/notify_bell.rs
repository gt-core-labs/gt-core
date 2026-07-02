//! Shared dedup-aware operator-bell writer (gtcore-7a707a).
//!
//! Every notification writer (`notify.send`, the workflow observer, the escalation
//! reminder ticker, delegation, …) used to hand-roll the same `INSERT INTO
//! notifications … RETURNING id` + SSE-append pair. When a *daemon* re-emitted the
//! same finding on every tick that meant one bell row per tick — the audit's 13
//! identical rows for a single stuck slot.
//!
//! [`ring_bell`] is the one writer that knows about dedup. Given a `fingerprint`,
//! it collapses re-emissions of the same `(workspace, from_role, fingerprint)`
//! within a sliding window into ONE row: the first call inserts (state `new`) and
//! emits the SSE badge; later calls only bump `count` + `last_seen_at` and stay
//! silent, regardless of whether the operator has acked/resolved the row — so an
//! already-handled finding is never re-paged. A `None` fingerprint keeps the old
//! one-shot behaviour (always insert, always emit).

use std::sync::Arc;

use serde::Serialize;
use sqlx::PgPool;

use gt_events::EventKind;
use gt_store_dolt::AppError;

use crate::mcp::EventLog;
use crate::notify_kind::NotificationKind;

/// The unscoped workspace whose SSE feed is the `None` log — mirrors the other
/// writers so the dashboard's `notification.created.v1` listener treats us the same.
const DEFAULT_WORKSPACE: &str = "default";

/// Default dedup window when a caller does not pin one: 24h. Comfortably wider than
/// the 4h default escalation-reminder cadence, so consecutive reminders for the same
/// escalation land inside the window and collapse into one row.
pub const DEFAULT_DEDUP_WINDOW_SECS: i64 = 24 * 60 * 60;

/// The SSE half of a bell write, identical in shape to `mcp/notify.rs`,
/// `workflow_notify.rs` and `escalation_notify.rs` so the dashboard listener treats
/// every writer the same.
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

/// One request to ring the operator bell.
pub struct BellWrite<'a> {
    pub workspace: &'a str,
    pub from_role: &'a str,
    pub title: &'a str,
    pub body: &'a str,
    pub kind: NotificationKind,
    /// The dedup key. `Some` collapses re-emissions of the same finding within the
    /// window; `None` always inserts a fresh row (one-shot notifications).
    pub fingerprint: Option<&'a str>,
}

/// What [`ring_bell`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BellOutcome {
    /// The row's id (the freshly inserted one, or the existing row that was bumped).
    pub id: String,
    /// `true` when an existing row was bumped instead of a new one inserted — the
    /// caller stayed silent (no SSE, no email).
    pub deduped: bool,
    /// The row's repeat counter after this call (1 on a fresh insert).
    pub count: i32,
}

/// Insert (or dedup-bump) an operator notification, emitting the SSE badge only on a
/// fresh insert.
///
/// Dedup contract (only when `fingerprint` is `Some`): if a row with the same
/// `(workspace, from_role, fingerprint)` exists whose `last_seen_at` is within
/// `window_secs`, its `count` is incremented and `last_seen_at` bumped to now — no
/// new row, no SSE — and `deduped: true` is returned. The lookup + update run in one
/// `FOR UPDATE` transaction so two near-simultaneous ticks cannot both insert.
pub async fn ring_bell(
    pool: &PgPool,
    log: &Arc<EventLog>,
    write: BellWrite<'_>,
    window_secs: i64,
) -> Result<BellOutcome, AppError> {
    let kind = write.kind.as_str();

    if let Some(fingerprint) = write.fingerprint {
        let mut tx = pool.begin().await.map_err(|e| AppError::Other(e.to_string()))?;

        // Newest live row for this finding within the window, locked so a concurrent
        // tick serialises behind us instead of racing a second insert.
        let existing: Option<(String, i32)> = sqlx::query_as(
            "SELECT id::text, count FROM notifications \
             WHERE workspace = $1 AND from_role = $2 AND fingerprint = $3 \
               AND last_seen_at > now() - make_interval(secs => $4) \
             ORDER BY last_seen_at DESC LIMIT 1 FOR UPDATE",
        )
        .bind(write.workspace)
        .bind(write.from_role)
        .bind(fingerprint)
        .bind(window_secs.max(0) as f64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| AppError::Other(e.to_string()))?;

        if let Some((id, count)) = existing {
            sqlx::query(
                "UPDATE notifications SET count = count + 1, last_seen_at = now() \
                 WHERE id = $1::uuid",
            )
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::Other(e.to_string()))?;
            tx.commit().await.map_err(|e| AppError::Other(e.to_string()))?;
            // Silent: the operator already has this row on their bell.
            return Ok(BellOutcome { id, deduped: true, count: count + 1 });
        }

        let (id,): (String,) = sqlx::query_as(
            "INSERT INTO notifications \
             (workspace, from_role, title, body, kind, fingerprint, state, count, last_seen_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 'new', 1, now()) RETURNING id::text",
        )
        .bind(write.workspace)
        .bind(write.from_role)
        .bind(write.title)
        .bind(write.body)
        .bind(kind)
        .bind(fingerprint)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| AppError::Other(e.to_string()))?;
        tx.commit().await.map_err(|e| AppError::Other(e.to_string()))?;

        emit_sse(log, &write, &id, kind);
        return Ok(BellOutcome { id, deduped: false, count: 1 });
    }

    // One-shot: no fingerprint, always a fresh row + badge.
    let (id,): (String,) = sqlx::query_as(
        "INSERT INTO notifications (workspace, from_role, title, body, kind) \
         VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
    )
    .bind(write.workspace)
    .bind(write.from_role)
    .bind(write.title)
    .bind(write.body)
    .bind(kind)
    .fetch_one(pool)
    .await
    .map_err(|e| AppError::Other(e.to_string()))?;

    emit_sse(log, &write, &id, kind);
    Ok(BellOutcome { id, deduped: false, count: 1 })
}

/// Best-effort SSE badge for a freshly-inserted row (logged, never fatal).
fn emit_sse(log: &Arc<EventLog>, write: &BellWrite<'_>, id: &str, kind: &str) {
    let ev = NotificationCreated {
        id: id.to_string(),
        workspace: write.workspace.to_string(),
        from_role: write.from_role.to_string(),
        title: write.title.to_string(),
        body: write.body.to_string(),
        kind: kind.to_string(),
    };
    let ws_opt = (write.workspace != DEFAULT_WORKSPACE).then_some(write.workspace);
    if let Err(e) = log.append(ws_opt, ev) {
        eprintln!("[notify-bell] SSE emit failed: {e}");
    }
}
