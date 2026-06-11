//! `InvitesRepository` — collaborator-invite store port + Postgres adapter
//! (hq-4231c1, epic hq-56b5ee).
//!
//! Backs the public-schema `workspace_invites` table (invites #0001). The
//! invite is a one-shot capability: minted by an admin, mailed via the email
//! outbox, consumed exactly once by the gt-login-authenticated accept (which
//! then binds the membership through `gt_auth::PgUsers::add_workspace_member`
//! — never here; this port owns only the token lifecycle).

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

/// What can go wrong against the invites store.
#[derive(Debug, thiserror::Error)]
pub enum InviteError {
    /// No invite matches (or it is not in the state the operation needs).
    #[error("invite not found: {0}")]
    NotFound(String),
    /// The token exists but cannot be consumed: already accepted/revoked, or
    /// past its expiry. The label carries which.
    #[error("invite not consumable: {0}")]
    NotConsumable(String),
    /// The backend failed.
    #[error("invites store db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// One invite row.
#[derive(Debug, Clone, FromRow)]
pub struct Invite {
    /// Opaque row id.
    pub id: String,
    /// The one-shot capability token (only surfaced to the inviter/email).
    pub token: String,
    /// The workspace the invite grants membership of.
    pub workspace: String,
    /// The invited identity's email.
    pub email: String,
    /// The membership role the accept binds: viewer|commenter|editor|admin.
    pub role: String,
    /// pending | accepted | revoked | expired.
    pub status: String,
    /// Hard expiry; a pending token past this is dead.
    pub expires_at: DateTime<Utc>,
    /// The admin that minted it.
    pub created_by: String,
    /// Mint time.
    pub created_at: DateTime<Utc>,
    /// Consume time; `None` until accepted.
    pub accepted_at: Option<DateTime<Utc>>,
    /// The identity that consumed it.
    pub accepted_by: Option<String>,
}

/// A new invite to mint.
#[derive(Debug, Clone)]
pub struct NewInvite {
    /// Opaque row id (ULID).
    pub id: String,
    /// The one-shot token (CSPRNG, minted by the handler).
    pub token: String,
    /// Target workspace.
    pub workspace: String,
    /// Invited email.
    pub email: String,
    /// Membership role to bind on accept.
    pub role: String,
    /// Hard expiry.
    pub expires_at: DateTime<Utc>,
    /// Minting admin.
    pub created_by: String,
}

const COLS: &str = "id, token, workspace, email, role, status, expires_at, \
                    created_by, created_at, accepted_at, accepted_by";

/// The invites port.
#[async_trait]
pub trait InvitesRepository: Send + Sync {
    /// Mint a new pending invite.
    async fn create(&self, new: NewInvite) -> Result<Invite, InviteError>;
    /// A workspace's invites, newest first.
    async fn list(&self, workspace: &str) -> Result<Vec<Invite>, InviteError>;
    /// Revoke a still-pending invite of this workspace.
    async fn revoke(&self, id: &str, workspace: &str) -> Result<(), InviteError>;
    /// Consume a token exactly once: flips `pending` → `accepted` and stamps
    /// the consumer. A second accept, a revoked token, an expired one, or an
    /// unknown token all fail — each with its own message so the caller can
    /// tell the user what happened.
    async fn accept(&self, token: &str, accepted_by: &str) -> Result<Invite, InviteError>;
}

/// Postgres adapter over the shared public-schema pool.
pub struct PgInvites {
    pool: PgPool,
}

impl PgInvites {
    /// Wrap the shared pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InvitesRepository for PgInvites {
    async fn create(&self, new: NewInvite) -> Result<Invite, InviteError> {
        let row = sqlx::query_as::<_, Invite>(&format!(
            "INSERT INTO workspace_invites \
                (id, token, workspace, email, role, expires_at, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             RETURNING {COLS}"
        ))
        .bind(&new.id)
        .bind(&new.token)
        .bind(&new.workspace)
        .bind(&new.email)
        .bind(&new.role)
        .bind(new.expires_at)
        .bind(&new.created_by)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list(&self, workspace: &str) -> Result<Vec<Invite>, InviteError> {
        // Lazy expiry on read: a listed pending invite past its expiry shows as
        // expired (and is flipped so the accept path agrees).
        sqlx::query(
            "UPDATE workspace_invites SET status = 'expired' \
             WHERE workspace = $1 AND status = 'pending' AND expires_at <= now()",
        )
        .bind(workspace)
        .execute(&self.pool)
        .await?;
        let rows = sqlx::query_as::<_, Invite>(&format!(
            "SELECT {COLS} FROM workspace_invites \
             WHERE workspace = $1 ORDER BY created_at DESC"
        ))
        .bind(workspace)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn revoke(&self, id: &str, workspace: &str) -> Result<(), InviteError> {
        let res = sqlx::query(
            "UPDATE workspace_invites SET status = 'revoked' \
             WHERE id = $1 AND workspace = $2 AND status = 'pending'",
        )
        .bind(id)
        .bind(workspace)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(InviteError::NotFound(id.to_string()));
        }
        Ok(())
    }

    async fn accept(&self, token: &str, accepted_by: &str) -> Result<Invite, InviteError> {
        // One-shot CAS consume: only a live pending token flips.
        let row = sqlx::query_as::<_, Invite>(&format!(
            "UPDATE workspace_invites \
             SET status = 'accepted', accepted_at = now(), accepted_by = $2 \
             WHERE token = $1 AND status = 'pending' AND expires_at > now() \
             RETURNING {COLS}"
        ))
        .bind(token)
        .bind(accepted_by)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(invite) = row {
            return Ok(invite);
        }
        // Disambiguate the rejection so the user learns what actually happened.
        let state: Option<(String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT status, expires_at FROM workspace_invites WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        match state {
            None => Err(InviteError::NotFound("unknown token".into())),
            Some((status, expires_at)) => {
                let reason = match status.as_str() {
                    "accepted" => "already accepted (an invite is one-shot)".to_string(),
                    "revoked" => "revoked by an admin".to_string(),
                    "expired" => "expired".to_string(),
                    _ if expires_at <= Utc::now() => {
                        // Pending but past expiry: flip lazily so the table agrees.
                        sqlx::query(
                            "UPDATE workspace_invites SET status = 'expired' WHERE token = $1",
                        )
                        .bind(token)
                        .execute(&self.pool)
                        .await?;
                        "expired".to_string()
                    }
                    other => format!("in state `{other}`"),
                };
                Err(InviteError::NotConsumable(reason))
            }
        }
    }
}
