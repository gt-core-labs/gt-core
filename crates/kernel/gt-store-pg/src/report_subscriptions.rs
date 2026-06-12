//! `ReportSubscriptionsRepository` — scheduled-report recipient list port +
//! Postgres adapter (hq-562e0b, epic hq-efc379).
//!
//! Recipients of the periodic HTML report digest. Flat list (no targets,
//! unlike `email_subscriptions`): the System scheduler mails every `enabled`
//! row of a workspace at the configured time. `enabled` is the operator's
//! send-selection switch — toggling off keeps the row (and its history) but
//! excludes it from sends.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::PgPool;

/// One report subscriber row. (No serde here — the composition edge shapes
/// the wire form, keeping this kernel crate serialization-free like its
/// siblings.)
#[derive(Debug, Clone)]
pub struct ReportSubscriber {
    /// Row id (ulid).
    pub id: String,
    /// Recipient address.
    pub email: String,
    /// Included in sends? The selection switch.
    pub enabled: bool,
    /// When subscribed (RFC 3339).
    pub created_at: String,
}

/// What can go wrong against the report-subscriptions store.
#[derive(Debug, thiserror::Error)]
pub enum ReportSubscriptionError {
    /// No matching subscriber (remove/toggle of an unknown email).
    #[error("report subscriber not found: {0}")]
    NotFound(String),
    /// The backend failed.
    #[error("report subscriptions db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// The report-subscribers port.
#[async_trait]
pub trait ReportSubscriptionsRepository: Send + Sync {
    /// All subscribers of a workspace (enabled and disabled), oldest first.
    async fn list(&self, workspace: &str) -> Result<Vec<ReportSubscriber>, ReportSubscriptionError>;
    /// Only the addresses the scheduler should mail (enabled rows).
    async fn enabled_emails(&self, workspace: &str)
        -> Result<Vec<String>, ReportSubscriptionError>;
    /// Add a recipient (idempotent — re-adding an existing email is a no-op
    /// success and does NOT flip its `enabled` state).
    async fn add(&self, workspace: &str, email: &str) -> Result<(), ReportSubscriptionError>;
    /// Remove a recipient; `NotFound` when nothing matched.
    async fn remove(&self, workspace: &str, email: &str) -> Result<(), ReportSubscriptionError>;
    /// Flip the send-selection switch; `NotFound` when nothing matched.
    async fn set_enabled(
        &self,
        workspace: &str,
        email: &str,
        enabled: bool,
    ) -> Result<(), ReportSubscriptionError>;
}

/// Postgres adapter over the shared public-schema pool.
pub struct PgReportSubscriptions {
    pool: PgPool,
}

impl PgReportSubscriptions {
    /// Wrap the shared pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ReportSubscriptionsRepository for PgReportSubscriptions {
    async fn list(
        &self,
        workspace: &str,
    ) -> Result<Vec<ReportSubscriber>, ReportSubscriptionError> {
        let rows: Vec<(String, String, bool, String)> = sqlx::query_as(
            "SELECT id, email, enabled, to_char(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF') \
             FROM report_subscriptions WHERE workspace = $1 ORDER BY created_at",
        )
        .bind(workspace)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, email, enabled, created_at)| ReportSubscriber {
                id,
                email,
                enabled,
                created_at,
            })
            .collect())
    }

    async fn enabled_emails(
        &self,
        workspace: &str,
    ) -> Result<Vec<String>, ReportSubscriptionError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT email FROM report_subscriptions \
             WHERE workspace = $1 AND enabled ORDER BY created_at",
        )
        .bind(workspace)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(e,)| e).collect())
    }

    async fn add(&self, workspace: &str, email: &str) -> Result<(), ReportSubscriptionError> {
        sqlx::query(
            "INSERT INTO report_subscriptions (id, workspace, email) VALUES ($1, $2, $3) \
             ON CONFLICT (workspace, email) DO NOTHING",
        )
        .bind(ulid::Ulid::new().to_string())
        .bind(workspace)
        .bind(email)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn remove(&self, workspace: &str, email: &str) -> Result<(), ReportSubscriptionError> {
        let res =
            sqlx::query("DELETE FROM report_subscriptions WHERE workspace = $1 AND email = $2")
                .bind(workspace)
                .bind(email)
                .execute(&self.pool)
                .await?;
        if res.rows_affected() == 0 {
            return Err(ReportSubscriptionError::NotFound(email.to_string()));
        }
        Ok(())
    }

    async fn set_enabled(
        &self,
        workspace: &str,
        email: &str,
        enabled: bool,
    ) -> Result<(), ReportSubscriptionError> {
        let res = sqlx::query(
            "UPDATE report_subscriptions SET enabled = $3 WHERE workspace = $1 AND email = $2",
        )
        .bind(workspace)
        .bind(email)
        .bind(enabled)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ReportSubscriptionError::NotFound(email.to_string()));
        }
        Ok(())
    }
}
