//! `SubscriptionsRepository` — seguimiento email subscriptions port + Postgres
//! adapter (hq-8a521a, epic hq-56b5ee).
//!
//! A workspace member subscribes to a card / doc / board; the mailbox daemon
//! fans matching feed events out through the email outbox. Public schema,
//! workspace-keyed, like the outbox itself.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::PgPool;

/// What can go wrong against the subscriptions store.
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// No matching subscription (unsubscribe of something not subscribed).
    #[error("subscription not found: {0}")]
    NotFound(String),
    /// The backend failed.
    #[error("subscriptions db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// The subscriptions port.
#[async_trait]
pub trait SubscriptionsRepository: Send + Sync {
    /// Subscribe (idempotent — re-subscribing is a no-op success).
    async fn subscribe(
        &self,
        workspace: &str,
        member_email: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<(), SubscriptionError>;
    /// Unsubscribe; `NotFound` when nothing matched.
    async fn unsubscribe(
        &self,
        workspace: &str,
        member_email: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<(), SubscriptionError>;
    /// The member emails watching one target.
    async fn subscribers(
        &self,
        workspace: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<String>, SubscriptionError>;
    /// One member's subscriptions, `(target_kind, target_id)` pairs.
    async fn list_for_member(
        &self,
        workspace: &str,
        member_email: &str,
    ) -> Result<Vec<(String, String)>, SubscriptionError>;
}

/// Postgres adapter over the shared public-schema pool.
pub struct PgSubscriptions {
    pool: PgPool,
}

impl PgSubscriptions {
    /// Wrap the shared pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SubscriptionsRepository for PgSubscriptions {
    async fn subscribe(
        &self,
        workspace: &str,
        member_email: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<(), SubscriptionError> {
        sqlx::query(
            "INSERT INTO email_subscriptions (id, workspace, member_email, target_kind, target_id) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (workspace, member_email, target_kind, target_id) DO NOTHING",
        )
        .bind(ulid::Ulid::new().to_string())
        .bind(workspace)
        .bind(member_email)
        .bind(target_kind)
        .bind(target_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn unsubscribe(
        &self,
        workspace: &str,
        member_email: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<(), SubscriptionError> {
        let res = sqlx::query(
            "DELETE FROM email_subscriptions \
             WHERE workspace = $1 AND member_email = $2 AND target_kind = $3 AND target_id = $4",
        )
        .bind(workspace)
        .bind(member_email)
        .bind(target_kind)
        .bind(target_id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(SubscriptionError::NotFound(format!("{target_kind}:{target_id}")));
        }
        Ok(())
    }

    async fn subscribers(
        &self,
        workspace: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<String>, SubscriptionError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT member_email FROM email_subscriptions \
             WHERE workspace = $1 AND target_kind = $2 AND target_id = $3",
        )
        .bind(workspace)
        .bind(target_kind)
        .bind(target_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(e,)| e).collect())
    }

    async fn list_for_member(
        &self,
        workspace: &str,
        member_email: &str,
    ) -> Result<Vec<(String, String)>, SubscriptionError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT target_kind, target_id FROM email_subscriptions \
             WHERE workspace = $1 AND member_email = $2 ORDER BY created_at",
        )
        .bind(workspace)
        .bind(member_email)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}
