//! `DocChunksRepository` — the chunk-level RAG index port + Postgres adapter
//! (hq-c488cb, epic hq-56b5ee).
//!
//! Backs `ws_default.doc_chunks` (gt-docs #0005): overlapping text chunks of a
//! document, each with its own `vector(384)` embedding (the same
//! gt-docs-embed runtime as the whole-doc search column — ADR D9, no new model
//! stack). Workspace isolation is structural ([`WorkspacePool`] `search_path`),
//! so retrieval can never see another tenant's chunks; the rig half of the
//! scope filters on the denormalized `owner_id` prefix.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use pgvector::Vector;
use sqlx::FromRow;

use crate::WorkspacePool;

/// What can go wrong against the chunk index.
#[derive(Debug, thiserror::Error)]
pub enum ChunkError {
    /// The backend failed.
    #[error("doc chunks db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// One chunk to upsert.
#[derive(Debug, Clone)]
pub struct NewChunk {
    /// Position within the document (0-based).
    pub chunk_idx: i32,
    /// The chunk text (overlapping segments, model-ready).
    pub content: String,
    /// Its `vector(384)` embedding.
    pub embedding: Vec<f32>,
}

/// One retrieved chunk, best match first.
#[derive(Debug, Clone, FromRow)]
pub struct RetrievedChunk {
    /// The owning document.
    pub doc_id: String,
    /// Position within it.
    pub chunk_idx: i32,
    /// Owner bead class the doc hangs on.
    pub owner_type: String,
    /// Owner bead id (rig-prefixed for tracker owners).
    pub owner_id: String,
    /// The document's display filename.
    pub filename: String,
    /// The chunk text — what an agent injects into its prompt.
    pub content: String,
    /// Cosine similarity to the query (1.0 = identical direction).
    pub similarity: f64,
}

/// The chunk-index port.
#[async_trait]
pub trait DocChunksRepository: Send + Sync {
    /// Replace a document's chunk set atomically (delete + insert in one
    /// transaction) — the re-embed path on every create/update/attach.
    async fn replace_chunks(
        &self,
        doc_id: &str,
        owner_type: &str,
        owner_id: &str,
        filename: &str,
        chunks: &[NewChunk],
    ) -> Result<(), ChunkError>;
    /// Purge a deleted document's chunks.
    async fn purge(&self, doc_id: &str) -> Result<(), ChunkError>;
    /// Top-`k` chunks by cosine similarity to `query_embedding`, optionally
    /// narrowed to owners whose id carries `rig` as its `-` prefix (the rig
    /// half of the scope key; the workspace half is the pool's schema).
    async fn retrieve(
        &self,
        query_embedding: &[f32],
        k: i64,
        rig: Option<&str>,
    ) -> Result<Vec<RetrievedChunk>, ChunkError>;
    /// Doc ids that have NO chunks yet (the backfill frontier), capped.
    async fn unindexed_doc_ids(&self, limit: i64) -> Result<Vec<String>, ChunkError>;
}

/// Postgres adapter over the tenant-scoped pool.
pub struct PgDocChunks {
    pool: WorkspacePool,
}

impl PgDocChunks {
    /// Wrap a tenant-scoped pool.
    pub fn new(pool: WorkspacePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DocChunksRepository for PgDocChunks {
    async fn replace_chunks(
        &self,
        doc_id: &str,
        owner_type: &str,
        owner_id: &str,
        filename: &str,
        chunks: &[NewChunk],
    ) -> Result<(), ChunkError> {
        let mut tx = self.pool.pool().begin().await?;
        sqlx::query("DELETE FROM doc_chunks WHERE doc_id = $1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        for c in chunks {
            sqlx::query(
                "INSERT INTO doc_chunks \
                    (doc_id, chunk_idx, owner_type, owner_id, filename, content, embedding) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(doc_id)
            .bind(c.chunk_idx)
            .bind(owner_type)
            .bind(owner_id)
            .bind(filename)
            .bind(&c.content)
            .bind(Vector::from(c.embedding.clone()))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn purge(&self, doc_id: &str) -> Result<(), ChunkError> {
        sqlx::query("DELETE FROM doc_chunks WHERE doc_id = $1")
            .bind(doc_id)
            .execute(self.pool.pool())
            .await?;
        Ok(())
    }

    async fn retrieve(
        &self,
        query_embedding: &[f32],
        k: i64,
        rig: Option<&str>,
    ) -> Result<Vec<RetrievedChunk>, ChunkError> {
        let vec = Vector::from(query_embedding.to_vec());
        let rows = match rig {
            Some(rig) => {
                sqlx::query_as::<_, RetrievedChunk>(
                    "SELECT doc_id, chunk_idx, owner_type, owner_id, filename, content, \
                            1 - (embedding <=> $1)::float8 AS similarity \
                     FROM doc_chunks \
                     WHERE owner_id LIKE $3 \
                     ORDER BY embedding <=> $1 \
                     LIMIT $2",
                )
                .bind(&vec)
                .bind(k)
                .bind(format!("{rig}-%"))
                .fetch_all(self.pool.pool())
                .await?
            }
            None => {
                sqlx::query_as::<_, RetrievedChunk>(
                    "SELECT doc_id, chunk_idx, owner_type, owner_id, filename, content, \
                            1 - (embedding <=> $1)::float8 AS similarity \
                     FROM doc_chunks \
                     ORDER BY embedding <=> $1 \
                     LIMIT $2",
                )
                .bind(&vec)
                .bind(k)
                .fetch_all(self.pool.pool())
                .await?
            }
        };
        Ok(rows)
    }

    async fn unindexed_doc_ids(&self, limit: i64) -> Result<Vec<String>, ChunkError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT d.id FROM documents d \
             WHERE d.deleted_at IS NULL \
               AND (d.body_md IS NOT NULL OR d.extracted_text IS NOT NULL) \
               AND NOT EXISTS (SELECT 1 FROM doc_chunks c WHERE c.doc_id = d.id) \
             ORDER BY d.uploaded_at ASC \
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}
