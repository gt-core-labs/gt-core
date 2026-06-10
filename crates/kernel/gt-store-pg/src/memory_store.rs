//! `MemoryRepository` — the per-workspace semantic memory store port + its Postgres
//! adapter (hq-memory-mcp.2).
//!
//! Backs the `ws_default.memories` table (hq-memory-mcp.1). The port is the inversion
//! seam the memory MCP namespace depends on; the [`PgMemory`] adapter implements it over
//! a [`WorkspacePool`](crate::WorkspacePool), whose `search_path` already resolves the
//! caller's tenant schema, so every query reads an unqualified `memories` that lands in
//! `ws_<slug>`.
//!
//! Mirrors [`PgDocuments`](crate::documents::PgDocuments) (hq-docs-store.3): a durable
//! note an agent writes once and later retrieves BY MEANING via the same hybrid keyword
//! (`tsv`) + vector (`embedding`) path documents use. Unlike documents this store is
//! keyed by a stable `name` slug and is upsert-by-name — re-writing a `name` replaces the
//! prior body and bumps the optimistic `version`, so an agent accretes a small, named,
//! queryable knowledge base rather than a growing append log. There is no soft-delete:
//! [`MemoryRepository::forget`] hard-deletes by `name` (the table carries no `deleted_at`).

#![cfg(feature = "pg")]

use async_trait::async_trait;
use gt_memory::{Memory, MemoryKind};
use pgvector::Vector;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::WorkspacePool;

/// What can go wrong against the memory store. Mirror of
/// [`DocError`](crate::documents::DocError).
#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    /// No memory with the given name (in this tenant).
    #[error("memory not found: {0}")]
    NotFound(String),
    /// The `expected` version did not match the row's current `version` — a concurrent
    /// write moved it. The caller should re-read and retry.
    #[error("version conflict on memory {name}: expected {expected}, found {found}")]
    VersionConflict {
        /// The memory name.
        name: String,
        /// The version the caller believed was current.
        expected: i64,
        /// The version actually stored.
        found: i64,
    },
    /// The backend failed.
    #[error("memory store db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// A memory row (the current state of a named memory). The generated `tsv` column and the
/// `embedding` vector are not projected back — `tsv` is search-only and an embedding is
/// large/opaque; callers re-derive both from the content if needed.
#[derive(Debug, Clone, FromRow)]
pub struct MemoryRow {
    /// Slug naming the memory; the natural key (upsert-by-name).
    pub name: String,
    /// Short human/agent summary surfaced in `list`.
    pub description: String,
    /// Recall class: `feedback` | `project` | `reference` | `user`.
    pub kind: String,
    /// The memory content, model-readable as-is.
    pub body: String,
    /// Optimistic-concurrency token; bumped on every upsert rewrite.
    pub version: i64,
    /// Who first wrote the memory (empty string = unattributed).
    pub created_by: String,
    /// When the row was last written.
    pub updated_at: DateTime<Utc>,
}

impl MemoryRow {
    /// View the row as the `gt-memory` domain [`Memory`] (drops `version`/`updated_at`/
    /// `created_by`). The stored `kind` is always one of the four CHECK-constrained tokens,
    /// so the parse cannot fail in practice; an unexpected value falls back to `Reference`.
    pub fn to_memory(&self) -> Memory {
        Memory {
            name: self.name.clone(),
            description: self.description.clone(),
            kind: MemoryKind::parse(&self.kind).unwrap_or(MemoryKind::Reference),
            body: self.body.clone(),
        }
    }
}

/// A memory to write (insert or replace-by-name). `version` starts at 0 on first insert
/// and is incremented by the store on every subsequent rewrite.
#[derive(Debug, Clone)]
pub struct NewMemory {
    /// Slug naming the memory; the upsert key.
    pub name: String,
    /// Short summary.
    pub description: String,
    /// Recall class (`feedback` | `project` | `reference` | `user`).
    pub kind: String,
    /// The memory content.
    pub body: String,
    /// Who wrote it (empty string = unattributed).
    pub created_by: String,
}

impl NewMemory {
    /// Build from a `gt-memory` domain [`Memory`], attributing it to `created_by`.
    pub fn from_memory(mem: &Memory, created_by: impl Into<String>) -> Self {
        Self {
            name: mem.name.clone(),
            description: mem.description.clone(),
            kind: mem.kind.as_str().to_string(),
            body: mem.body.clone(),
            created_by: created_by.into(),
        }
    }
}

/// The per-workspace semantic memory store port (hq-memory-mcp.2). The memory MCP
/// namespace depends on this trait, not on Postgres.
#[async_trait]
pub trait MemoryRepository: Send + Sync {
    /// Upsert a memory by `name`: insert at version 0, or replace an existing memory's
    /// description/kind/body, bumping its `version` and stamping `updated_at`. Returns the
    /// post-write row.
    async fn upsert(&self, mem: NewMemory) -> Result<MemoryRow, MemoryError>;
    /// Fetch one memory by `name`, or `None`.
    async fn get(&self, name: &str) -> Result<Option<MemoryRow>, MemoryError>;
    /// All memories of one recall `kind`, ordered by `name` for deterministic iteration.
    async fn by_kind(&self, kind: &str) -> Result<Vec<MemoryRow>, MemoryError>;
    /// Every memory in the tenant, ordered by `name`.
    async fn list(&self) -> Result<Vec<MemoryRow>, MemoryError>;
    /// **RECALL** (full-text): the memories whose `tsv` matches `query`
    /// (`websearch_to_tsquery`, forgiving user syntax), ranked by `ts_rank`, capped at
    /// `limit`. Optionally narrowed to one `kind`.
    async fn recall(
        &self,
        query: &str,
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>, MemoryError>;
    /// Store (or replace) a memory's semantic-search embedding. `embedding.len()` must
    /// match the column dimension (384). An absent `name` is simply not updated.
    async fn set_embedding(&self, name: &str, embedding: Vec<f32>) -> Result<(), MemoryError>;
    /// **RECALL** (hybrid): fuse the full-text rank with vector cosine similarity against
    /// `query_embedding`, ranked by their weighted sum, capped at `limit`, optionally
    /// narrowed to one `kind`. Rows lacking an embedding still surface on the text side
    /// (the vector term coalesces to 0). Mirror of
    /// [`search_hybrid`](crate::documents::DocumentsRepository::search_hybrid).
    async fn recall_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>, MemoryError>;
    /// **FORGET**: hard-delete a memory by `name`. Idempotent — deleting an absent memory
    /// is a successful no-op (the store has no soft-delete; upsert-by-name means a name is
    /// either present or gone).
    async fn forget(&self, name: &str) -> Result<(), MemoryError>;
}

/// Postgres adapter over a tenant-scoped [`WorkspacePool`]. Mirror of
/// [`PgDocuments`](crate::documents::PgDocuments).
#[derive(Clone)]
pub struct PgMemory {
    pool: WorkspacePool,
}

impl PgMemory {
    /// Wrap a tenant-scoped pool. Its `search_path` already targets `ws_<slug>`.
    pub fn new(pool: WorkspacePool) -> Self {
        Self { pool }
    }
}

/// The non-generated, projected columns — `tsv` and `embedding` are search-only and not
/// read back into [`MemoryRow`].
const COLS: &str = "name, description, kind, body, version, created_by, updated_at";

#[async_trait]
impl MemoryRepository for PgMemory {
    async fn upsert(&self, mem: NewMemory) -> Result<MemoryRow, MemoryError> {
        // Upsert-by-name: insert at version 0, or on conflict replace the mutable fields and
        // bump `version` so a concurrent reader sees the rewrite. `created_by` is overwritten
        // with the latest writer (the store keeps no authorship history).
        let row: MemoryRow = sqlx::query_as(&format!(
            "INSERT INTO memories (name, description, kind, body, created_by) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (name) DO UPDATE SET \
               description = EXCLUDED.description, \
               kind = EXCLUDED.kind, \
               body = EXCLUDED.body, \
               version = memories.version + 1, \
               created_by = EXCLUDED.created_by, \
               updated_at = now() \
             RETURNING {COLS}"
        ))
        .bind(&mem.name)
        .bind(&mem.description)
        .bind(&mem.kind)
        .bind(&mem.body)
        .bind(&mem.created_by)
        .fetch_one(self.pool.pool())
        .await?;
        Ok(row)
    }

    async fn get(&self, name: &str) -> Result<Option<MemoryRow>, MemoryError> {
        let row = sqlx::query_as(&format!("SELECT {COLS} FROM memories WHERE name = $1"))
            .bind(name)
            .fetch_optional(self.pool.pool())
            .await?;
        Ok(row)
    }

    async fn by_kind(&self, kind: &str) -> Result<Vec<MemoryRow>, MemoryError> {
        let rows = sqlx::query_as(&format!(
            "SELECT {COLS} FROM memories WHERE kind = $1 ORDER BY name"
        ))
        .bind(kind)
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows)
    }

    async fn list(&self) -> Result<Vec<MemoryRow>, MemoryError> {
        let rows = sqlx::query_as(&format!("SELECT {COLS} FROM memories ORDER BY name"))
            .fetch_all(self.pool.pool())
            .await?;
        Ok(rows)
    }

    async fn recall(
        &self,
        query: &str,
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>, MemoryError> {
        // `$1` is the query; `$2` the limit. The optional kind filter binds `$3`.
        let kind_clause = if kind.is_some() { "AND kind = $3" } else { "" };
        let sql = format!(
            "SELECT {COLS} FROM memories \
             WHERE tsv @@ websearch_to_tsquery('english', $1) {kind_clause} \
             ORDER BY ts_rank(tsv, websearch_to_tsquery('english', $1)) DESC \
             LIMIT $2"
        );
        let mut q = sqlx::query_as(&sql).bind(query).bind(limit);
        if let Some(k) = kind {
            q = q.bind(k);
        }
        let rows = q.fetch_all(self.pool.pool()).await?;
        Ok(rows)
    }

    async fn set_embedding(&self, name: &str, embedding: Vec<f32>) -> Result<(), MemoryError> {
        sqlx::query("UPDATE memories SET embedding = $2 WHERE name = $1")
            .bind(name)
            .bind(Vector::from(embedding))
            .execute(self.pool.pool())
            .await?;
        Ok(())
    }

    async fn recall_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
        kind: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>, MemoryError> {
        // $1 query text, $2 query vector, $3 limit; optional kind binds $4. Score fuses text
        // rank and (1 - cosine distance), each coalesced to 0 so a row matching on only one
        // side still ranks. Candidates: text match OR an embedding present.
        let kind_clause = if kind.is_some() { "AND kind = $4" } else { "" };
        let sql = format!(
            "SELECT {COLS} FROM memories \
             WHERE (tsv @@ websearch_to_tsquery('english', $1) OR embedding IS NOT NULL) \
               {kind_clause} \
             ORDER BY ( \
                 COALESCE(ts_rank(tsv, websearch_to_tsquery('english', $1)), 0) \
                 + COALESCE(1 - (embedding <=> $2), 0) \
             ) DESC \
             LIMIT $3"
        );
        let mut q = sqlx::query_as(&sql)
            .bind(query)
            .bind(Vector::from(query_embedding.to_vec()))
            .bind(limit);
        if let Some(k) = kind {
            q = q.bind(k);
        }
        let rows = q.fetch_all(self.pool.pool()).await?;
        Ok(rows)
    }

    async fn forget(&self, name: &str) -> Result<(), MemoryError> {
        // Hard delete; idempotent (no soft-delete column — a name is present or gone).
        sqlx::query("DELETE FROM memories WHERE name = $1")
            .bind(name)
            .execute(self.pool.pool())
            .await?;
        Ok(())
    }
}
