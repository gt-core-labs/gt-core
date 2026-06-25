//! `EmailOutboxRepository` — the transport-agnostic email outbox port + its
//! Postgres adapter (hq-f24599, epic hq-56b5ee).
//!
//! Backs the public-schema `email_outbox` table (email #0001). Producers
//! enqueue; the drain daemon claims due rows ([`claim_due`]
//! uses `FOR UPDATE SKIP LOCKED`, so multiple replicas never double-send) and
//! settles each via [`mark_sent`] / [`mark_retry`] / [`mark_failed`]. The
//! transport itself lives behind `gt_notify::EmailTransport` — this port never
//! touches a mail server.
//!
//! [`claim_due`]: EmailOutboxRepository::claim_due
//! [`mark_sent`]: EmailOutboxRepository::mark_sent
//! [`mark_retry`]: EmailOutboxRepository::mark_retry
//! [`mark_failed`]: EmailOutboxRepository::mark_failed

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

/// What can go wrong against the outbox.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// No row with that id (in that workspace), or it is not in a cancellable/
    /// settleable state.
    #[error("outbox entry not found (or not in an eligible state): {0}")]
    NotFound(String),
    /// The backend failed.
    #[error("email outbox db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// One outbox row.
#[derive(Debug, Clone, FromRow)]
pub struct OutboxEntry {
    /// Opaque id (ULID minted by the caller).
    pub id: String,
    /// The workspace that scheduled it.
    pub workspace: String,
    /// Primary recipient address (the To: header).
    pub recipient: String,
    /// Carbon-copy recipients, comma-separated (empty = none). The report
    /// digest puts every registered subscriber here (gtcore-ecf70d).
    pub cc: String,
    /// Subject line.
    pub subject: String,
    /// Rendered body.
    pub body: String,
    /// Optional template/document provenance pointer.
    pub template_ref: Option<String>,
    /// When the send becomes due.
    pub send_at: DateTime<Utc>,
    /// `pending` | `sending` | `sent` | `retry` | `failed` | `cancelled`.
    pub status: String,
    /// Delivery attempts so far.
    pub attempts: i32,
    /// Last transport error, when any attempt failed.
    pub last_error: Option<String>,
    /// Who scheduled it.
    pub created_by: String,
    /// When it was scheduled.
    pub created_at: DateTime<Utc>,
    /// When it was delivered; `None` until `sent`.
    pub sent_at: Option<DateTime<Utc>>,
}

/// A new email to enqueue.
#[derive(Debug, Clone)]
pub struct NewEmail {
    /// Opaque id (ULID minted by the caller).
    pub id: String,
    /// The scheduling workspace.
    pub workspace: String,
    /// Primary recipient address (the To: header).
    pub recipient: String,
    /// Carbon-copy recipients (the To: gets the message, these get a copy).
    /// Empty = a plain single-recipient send.
    pub cc: Vec<String>,
    /// Subject line.
    pub subject: String,
    /// Rendered body.
    pub body: String,
    /// Optional template/document provenance pointer.
    pub template_ref: Option<String>,
    /// When to send; `None` = now.
    pub send_at: Option<DateTime<Utc>>,
    /// Who scheduled it.
    pub created_by: String,
}

const COLS: &str = "id, workspace, recipient, cc, subject, body, template_ref, send_at, \
                    status, attempts, last_error, created_by, created_at, sent_at";

/// The outbox port the email handler + drain daemon depend on.
#[async_trait]
pub trait EmailOutboxRepository: Send + Sync {
    /// Enqueue a new email (status `pending`). Returns the stored row.
    async fn enqueue(&self, new: NewEmail) -> Result<OutboxEntry, OutboxError>;
    /// The workspace's outbox, newest first, optionally narrowed to one status.
    async fn list(
        &self,
        workspace: &str,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<OutboxEntry>, OutboxError>;
    /// Cancel a `pending`/`retry` row of this workspace. Any other state is
    /// `NotFound` (already sent/sending rows cannot be recalled).
    async fn cancel(&self, id: &str, workspace: &str) -> Result<(), OutboxError>;
    /// Atomically claim up to `limit` due rows (`send_at <= now`, status
    /// `pending`|`retry`) by flipping them to `sending`. `FOR UPDATE SKIP
    /// LOCKED` keeps concurrent drainers disjoint.
    async fn claim_due(&self, limit: i64) -> Result<Vec<OutboxEntry>, OutboxError>;
    /// Settle a claimed row as delivered.
    async fn mark_sent(&self, id: &str) -> Result<(), OutboxError>;
    /// Settle a claimed row as retryable: bump `attempts`, record the error,
    /// push `send_at` to `next_at`.
    async fn mark_retry(
        &self,
        id: &str,
        error: &str,
        next_at: DateTime<Utc>,
    ) -> Result<(), OutboxError>;
    /// Settle a claimed row as permanently failed.
    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), OutboxError>;
}

/// Postgres adapter over the shared public-schema pool.
pub struct PgEmailOutbox {
    pool: PgPool,
}

impl PgEmailOutbox {
    /// Wrap the shared pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EmailOutboxRepository for PgEmailOutbox {
    async fn enqueue(&self, new: NewEmail) -> Result<OutboxEntry, OutboxError> {
        let row = sqlx::query_as::<_, OutboxEntry>(&format!(
            "INSERT INTO email_outbox \
                (id, workspace, recipient, cc, subject, body, template_ref, send_at, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, now()), $9) \
             RETURNING {COLS}"
        ))
        .bind(&new.id)
        .bind(&new.workspace)
        .bind(&new.recipient)
        .bind(new.cc.join(","))
        .bind(&new.subject)
        .bind(&new.body)
        .bind(&new.template_ref)
        .bind(new.send_at)
        .bind(&new.created_by)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list(
        &self,
        workspace: &str,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<OutboxEntry>, OutboxError> {
        let rows = match status {
            Some(status) => {
                sqlx::query_as::<_, OutboxEntry>(&format!(
                    "SELECT {COLS} FROM email_outbox \
                     WHERE workspace = $1 AND status = $2 \
                     ORDER BY created_at DESC LIMIT $3"
                ))
                .bind(workspace)
                .bind(status)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, OutboxEntry>(&format!(
                    "SELECT {COLS} FROM email_outbox \
                     WHERE workspace = $1 \
                     ORDER BY created_at DESC LIMIT $2"
                ))
                .bind(workspace)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn cancel(&self, id: &str, workspace: &str) -> Result<(), OutboxError> {
        let res = sqlx::query(
            "UPDATE email_outbox SET status = 'cancelled' \
             WHERE id = $1 AND workspace = $2 AND status IN ('pending', 'retry')",
        )
        .bind(id)
        .bind(workspace)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(OutboxError::NotFound(id.to_string()));
        }
        Ok(())
    }

    async fn claim_due(&self, limit: i64) -> Result<Vec<OutboxEntry>, OutboxError> {
        // Claim = flip to `sending` inside one statement; SKIP LOCKED keeps a
        // second drainer's claim set disjoint from ours.
        let rows = sqlx::query_as::<_, OutboxEntry>(&format!(
            "UPDATE email_outbox SET status = 'sending' \
             WHERE id IN ( \
                 SELECT id FROM email_outbox \
                 WHERE status IN ('pending', 'retry') AND send_at <= now() \
                 ORDER BY send_at ASC \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING {COLS}"
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn mark_sent(&self, id: &str) -> Result<(), OutboxError> {
        let res = sqlx::query(
            "UPDATE email_outbox SET status = 'sent', sent_at = now(), \
             attempts = attempts + 1, last_error = NULL \
             WHERE id = $1 AND status = 'sending'",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(OutboxError::NotFound(id.to_string()));
        }
        Ok(())
    }

    async fn mark_retry(
        &self,
        id: &str,
        error: &str,
        next_at: DateTime<Utc>,
    ) -> Result<(), OutboxError> {
        let res = sqlx::query(
            "UPDATE email_outbox SET status = 'retry', attempts = attempts + 1, \
             last_error = $2, send_at = $3 \
             WHERE id = $1 AND status = 'sending'",
        )
        .bind(id)
        .bind(error)
        .bind(next_at)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(OutboxError::NotFound(id.to_string()));
        }
        Ok(())
    }

    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), OutboxError> {
        let res = sqlx::query(
            "UPDATE email_outbox SET status = 'failed', attempts = attempts + 1, \
             last_error = $2 \
             WHERE id = $1 AND status = 'sending'",
        )
        .bind(id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(OutboxError::NotFound(id.to_string()));
        }
        Ok(())
    }
}
