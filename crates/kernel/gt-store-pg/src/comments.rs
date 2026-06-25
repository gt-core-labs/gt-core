//! `CommentsRepository` — the per-workspace threaded-comments port + its Postgres
//! adapter (hq-57042e, epic hq-56b5ee).
//!
//! Backs the `ws_default.comments` template table (gt-comments #0001). One small
//! polymorphic store: a comment targets a Kanban card (bead) or a document
//! (`target_kind` ∈ `card`|`doc`), threads via `parent_id`, soft-deletes like the
//! document store. The [`PgComments`] adapter runs over a
//! [`WorkspacePool`](crate::WorkspacePool), whose `search_path` resolves the
//! caller's tenant schema — every query reads an unqualified `comments` that
//! lands in `ws_<slug>`.
//!
//! Target existence (a `card` lives in the Dolt tracker, a `doc` in
//! `documents`) is validated by the domain handler, not here — the port stays a
//! plain row store.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::WorkspacePool;

/// What can go wrong against the comments store.
#[derive(Debug, thiserror::Error)]
pub enum CommentError {
    /// No live comment with the given id (in this tenant).
    #[error("comment not found: {0}")]
    NotFound(String),
    /// The backend failed.
    #[error("comments store db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// A comment row.
#[derive(Debug, Clone, FromRow)]
pub struct Comment {
    /// Opaque comment id.
    pub id: String,
    /// `card` | `doc`.
    pub target_kind: String,
    /// The bead id (`card`) or document id (`doc`) this comment hangs on.
    pub target_id: String,
    /// The authoring actor (server-injected, never body-supplied).
    pub author: String,
    /// The comment text (markdown allowed; rendered by the consumer).
    pub body: String,
    /// Threading parent; `None` = top-level.
    pub parent_id: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last body edit; `None` = never edited.
    pub edited_at: Option<DateTime<Utc>>,
    /// Soft-delete marker; `None` = live.
    pub deleted_at: Option<DateTime<Utc>>,
}

/// A new comment to insert.
#[derive(Debug, Clone)]
pub struct NewComment {
    /// Opaque id (ULID/uuid minted by the caller).
    pub id: String,
    /// `card` | `doc`.
    pub target_kind: String,
    /// Target id.
    pub target_id: String,
    /// Authoring actor.
    pub author: String,
    /// Comment text.
    pub body: String,
    /// Optional threading parent.
    pub parent_id: Option<String>,
}

/// The comments port the `gt-comments` domain depends on.
#[async_trait]
pub trait CommentsRepository: Send + Sync {
    /// Insert a new comment and return the stored row.
    async fn insert(&self, new: NewComment) -> Result<Comment, CommentError>;
    /// One live comment by id; `NotFound` for absent or soft-deleted rows.
    async fn get(&self, id: &str) -> Result<Comment, CommentError>;
    /// The live comments of one target in chronological order (the thread tree
    /// is reassembled by the consumer from `parent_id`).
    async fn list_for_target(
        &self,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<Comment>, CommentError>;
    /// Overwrite a live comment's body + stamp `edited_at`. Returns the updated row.
    async fn update_body(&self, id: &str, body: &str) -> Result<Comment, CommentError>;
    /// Soft-delete a live comment (idempotent failure: deleting a missing/dead
    /// row is `NotFound`). Replies stay anchored to the dead parent.
    async fn soft_delete(&self, id: &str) -> Result<(), CommentError>;
}

/// Postgres adapter over the tenant-scoped [`WorkspacePool`].
pub struct PgComments {
    pool: WorkspacePool,
}

const COLS: &str = "id, target_kind, target_id, author, body, parent_id, \
                    created_at, edited_at, deleted_at";

impl PgComments {
    /// Wrap a tenant-scoped pool.
    pub fn new(pool: WorkspacePool) -> Self {
        Self { pool }
    }

    /// Bulk-load the live comments of many `card` targets at once
    /// (gtcore-01bcf2) — the report digest needs every bead/epic's comments in
    /// one round trip instead of N `list_for_target` calls. Returns a map keyed
    /// by `target_id`, each value chronological (`created_at ASC, id ASC`).
    /// Absent/comment-less ids simply do not appear in the map. An empty input
    /// short-circuits without a query.
    pub async fn list_for_cards(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, Vec<Comment>>, CommentError> {
        if ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let rows = sqlx::query_as::<_, Comment>(&format!(
            "SELECT {COLS} FROM comments
             WHERE target_kind = 'card' AND target_id = ANY($1) AND deleted_at IS NULL
             ORDER BY target_id ASC, created_at ASC, id ASC"
        ))
        .bind(ids)
        .fetch_all(self.pool.pool())
        .await?;
        let mut map: std::collections::HashMap<String, Vec<Comment>> =
            std::collections::HashMap::new();
        for row in rows {
            map.entry(row.target_id.clone()).or_default().push(row);
        }
        Ok(map)
    }

    /// Resolve an `@mention` handle to a member of this workspace: matches the
    /// tenant's `users` mirror (`ws_<slug>.users`, hq-platform-hardening.2) by
    /// full email or by the email's local part. Returns the member's email when
    /// the handle names exactly one member; `None` otherwise (unknown or
    /// ambiguous handles notify nobody — the caller treats mention dispatch as
    /// best-effort).
    pub async fn resolve_mention(&self, handle: &str) -> Result<Option<String>, CommentError> {
        let emails: Vec<(String,)> = sqlx::query_as(
            "SELECT email FROM users WHERE email = $1 OR split_part(email, '@', 1) = $1 LIMIT 2",
        )
        .bind(handle)
        .fetch_all(self.pool.pool())
        .await?;
        Ok((emails.len() == 1).then(|| emails[0].0.clone()))
    }
}

#[async_trait]
impl CommentsRepository for PgComments {
    async fn insert(&self, new: NewComment) -> Result<Comment, CommentError> {
        let row = sqlx::query_as::<_, Comment>(&format!(
            "INSERT INTO comments (id, target_kind, target_id, author, body, parent_id)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING {COLS}"
        ))
        .bind(&new.id)
        .bind(&new.target_kind)
        .bind(&new.target_id)
        .bind(&new.author)
        .bind(&new.body)
        .bind(&new.parent_id)
        .fetch_one(self.pool.pool())
        .await?;
        Ok(row)
    }

    async fn get(&self, id: &str) -> Result<Comment, CommentError> {
        sqlx::query_as::<_, Comment>(&format!(
            "SELECT {COLS} FROM comments WHERE id = $1 AND deleted_at IS NULL"
        ))
        .bind(id)
        .fetch_optional(self.pool.pool())
        .await?
        .ok_or_else(|| CommentError::NotFound(id.to_string()))
    }

    async fn list_for_target(
        &self,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<Comment>, CommentError> {
        let rows = sqlx::query_as::<_, Comment>(&format!(
            "SELECT {COLS} FROM comments
             WHERE target_kind = $1 AND target_id = $2 AND deleted_at IS NULL
             ORDER BY created_at ASC, id ASC"
        ))
        .bind(target_kind)
        .bind(target_id)
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows)
    }

    async fn update_body(&self, id: &str, body: &str) -> Result<Comment, CommentError> {
        sqlx::query_as::<_, Comment>(&format!(
            "UPDATE comments SET body = $2, edited_at = now()
             WHERE id = $1 AND deleted_at IS NULL
             RETURNING {COLS}"
        ))
        .bind(id)
        .bind(body)
        .fetch_optional(self.pool.pool())
        .await?
        .ok_or_else(|| CommentError::NotFound(id.to_string()))
    }

    async fn soft_delete(&self, id: &str) -> Result<(), CommentError> {
        let res = sqlx::query("UPDATE comments SET deleted_at = now() WHERE id = $1 AND deleted_at IS NULL")
            .bind(id)
            .execute(self.pool.pool())
            .await?;
        if res.rows_affected() == 0 {
            return Err(CommentError::NotFound(id.to_string()));
        }
        Ok(())
    }
}
