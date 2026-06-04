//! `DocumentsRepository` — the per-workspace document store port + its Postgres adapter
//! (hq-docs-store.3, docs/11).
//!
//! Backs the `ws_default.documents` / `document_versions` tables (hq-docs-store.1). The
//! port is the inversion seam the `gt-documents` domain (hq-docs-api.1) depends on; the
//! [`PgDocuments`] adapter implements it over a [`WorkspacePool`](crate::WorkspacePool),
//! whose `search_path` already resolves the caller's tenant schema, so every query reads
//! an unqualified `documents` that lands in `ws_<slug>`.
//!
//! Realizes the locked design decisions (docs/11): optimistic `version` guard
//! (`expected_version` → [`DocError::VersionConflict`] on a stale write), `sha256` dedup
//! ([`DocumentsRepository::find_by_sha`]), and soft-delete ([`DocumentsRepository::soft_delete`]).
//! Every mutation appends an immutable snapshot to `document_versions`.

#![cfg(feature = "pg")]

use async_trait::async_trait;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{FromRow, Row};

use crate::WorkspacePool;

/// What can go wrong against the document store.
#[derive(Debug, thiserror::Error)]
pub enum DocError {
    /// No document with the given id (in this tenant).
    #[error("document not found: {0}")]
    NotFound(String),
    /// The `expected_version` did not match the row's current `version` — a concurrent
    /// write moved it. The caller should re-read and retry.
    #[error("version conflict on document {id}: expected {expected}, found {found}")]
    VersionConflict {
        /// The document id.
        id: String,
        /// The version the caller believed was current.
        expected: i64,
        /// The version actually stored.
        found: i64,
    },
    /// The backend failed.
    #[error("documents store db error: {0}")]
    Db(#[from] sqlx::Error),
}

/// A document row (current version). `body_md` is set for `kind='md'`; `bucket`/`key`/
/// `extracted_text` for `kind='blob'`.
#[derive(Debug, Clone, FromRow)]
pub struct Document {
    /// Opaque document id.
    pub id: String,
    /// Bead class this is attached to (`epic`/`skill`/`spec`).
    pub owner_type: String,
    /// Id of the owning bead.
    pub owner_id: String,
    /// `md` | `blob`.
    pub kind: String,
    /// Display filename.
    pub filename: String,
    /// MIME type, when known.
    pub content_type: Option<String>,
    /// Byte size, when known.
    pub size: Option<i64>,
    /// Content hash (dedup key).
    pub sha256: Option<String>,
    /// `kind='md'`: the markdown body.
    pub body_md: Option<String>,
    /// `kind='blob'`: object-store bucket.
    pub bucket: Option<String>,
    /// `kind='blob'`: object-store key.
    pub key: Option<String>,
    /// `kind='blob'`: text pulled out for the model + search.
    pub extracted_text: Option<String>,
    /// Optimistic-concurrency token.
    pub version: i64,
    /// Soft-delete marker; `None` = live.
    pub deleted_at: Option<DateTime<Utc>>,
    /// Who uploaded it.
    pub uploaded_by: String,
    /// When it was uploaded.
    pub uploaded_at: DateTime<Utc>,
}

/// A new document to insert (version starts at 0).
#[derive(Debug, Clone)]
pub struct NewDocument {
    /// Opaque id (uuid/ULID minted by the caller).
    pub id: String,
    /// Owning bead class.
    pub owner_type: String,
    /// Owning bead id.
    pub owner_id: String,
    /// `md` | `blob`.
    pub kind: String,
    /// Display filename.
    pub filename: String,
    /// MIME type.
    pub content_type: Option<String>,
    /// Byte size.
    pub size: Option<i64>,
    /// Content hash.
    pub sha256: Option<String>,
    /// `kind='md'` body.
    pub body_md: Option<String>,
    /// `kind='blob'` bucket pointer.
    pub bucket: Option<String>,
    /// `kind='blob'` key pointer.
    pub key: Option<String>,
    /// `kind='blob'` extracted text.
    pub extracted_text: Option<String>,
    /// Uploader (empty string = unattributed).
    pub uploaded_by: String,
}

/// Mutable fields of a document. `None` leaves the column unchanged.
#[derive(Debug, Clone, Default)]
pub struct DocumentPatch {
    /// New filename.
    pub filename: Option<String>,
    /// New `kind='md'` body.
    pub body_md: Option<String>,
    /// New extracted text.
    pub extracted_text: Option<String>,
    /// Who made the edit (recorded on the version snapshot).
    pub edited_by: String,
}

/// The per-workspace document store port (hq-docs-store.3). The `gt-documents` domain
/// depends on this trait, not on Postgres.
#[async_trait]
pub trait DocumentsRepository: Send + Sync {
    /// Insert a new document (version 0) and seed its first version snapshot.
    async fn create(&self, doc: NewDocument) -> Result<Document, DocError>;
    /// Fetch one document by id (live or soft-deleted), or `None`.
    async fn get(&self, id: &str) -> Result<Option<Document>, DocError>;
    /// List the live documents attached to an owner, newest first.
    async fn list_by_owner(
        &self,
        owner_type: &str,
        owner_id: &str,
    ) -> Result<Vec<Document>, DocError>;
    /// List the live documents attached to an owner id, regardless of `owner_type`,
    /// newest first. Backs the `gt://issue/{id}` resource inline (hq-docs-api.3): an
    /// issue's documents are those carrying its id, whatever class the attacher chose.
    async fn list_by_owner_id(&self, owner_id: &str) -> Result<Vec<Document>, DocError>;
    /// Dedup probe: the first live document whose content hash matches, or `None`.
    async fn find_by_sha(&self, sha256: &str) -> Result<Option<Document>, DocError>;
    /// Phase-1 full-text search (hq-docs-search.1) over `body_md`/`extracted_text` via the
    /// generated `tsv` column, ranked by `ts_rank`. `query` is parsed with
    /// `websearch_to_tsquery` (forgiving, user-style syntax). Live rows only, capped at
    /// `limit`. Optionally narrowed to one owner.
    async fn search(
        &self,
        query: &str,
        owner: Option<(&str, &str)>,
        limit: i64,
    ) -> Result<Vec<Document>, DocError>;
    /// Apply `patch` under an optimistic `expected_version` guard; bumps `version` and
    /// appends a version snapshot. [`DocError::VersionConflict`] on a stale guard.
    async fn update(
        &self,
        id: &str,
        expected_version: i64,
        patch: DocumentPatch,
    ) -> Result<Document, DocError>;
    /// Soft-delete (sets `deleted_at`) under the same version guard. The backing blob, if
    /// any, is purged later by a reconciler — not here.
    async fn soft_delete(&self, id: &str, expected_version: i64) -> Result<(), DocError>;
}

/// Postgres adapter over a tenant-scoped [`WorkspacePool`].
#[derive(Clone)]
pub struct PgDocuments {
    pool: WorkspacePool,
}

impl PgDocuments {
    /// Wrap a tenant-scoped pool. Its `search_path` already targets `ws_<slug>`.
    pub fn new(pool: WorkspacePool) -> Self {
        Self { pool }
    }
}

const COLS: &str = "id, owner_type, owner_id, kind, filename, content_type, size, sha256, \
                    body_md, bucket, key, extracted_text, version, deleted_at, uploaded_by, \
                    uploaded_at";

#[async_trait]
impl DocumentsRepository for PgDocuments {
    async fn create(&self, doc: NewDocument) -> Result<Document, DocError> {
        let mut tx = self.pool.pool().begin().await?;

        let row: Document = sqlx::query_as(&format!(
            "INSERT INTO documents \
             (id, owner_type, owner_id, kind, filename, content_type, size, sha256, \
              body_md, bucket, key, extracted_text, uploaded_by) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
             RETURNING {COLS}"
        ))
        .bind(&doc.id)
        .bind(&doc.owner_type)
        .bind(&doc.owner_id)
        .bind(&doc.kind)
        .bind(&doc.filename)
        .bind(&doc.content_type)
        .bind(doc.size)
        .bind(&doc.sha256)
        .bind(&doc.body_md)
        .bind(&doc.bucket)
        .bind(&doc.key)
        .bind(&doc.extracted_text)
        .bind(&doc.uploaded_by)
        .fetch_one(&mut *tx)
        .await?;

        insert_version(&mut tx, &row, &row.uploaded_by).await?;
        tx.commit().await?;
        Ok(row)
    }

    async fn get(&self, id: &str) -> Result<Option<Document>, DocError> {
        let row = sqlx::query_as(&format!("SELECT {COLS} FROM documents WHERE id = $1"))
            .bind(id)
            .fetch_optional(self.pool.pool())
            .await?;
        Ok(row)
    }

    async fn list_by_owner(
        &self,
        owner_type: &str,
        owner_id: &str,
    ) -> Result<Vec<Document>, DocError> {
        let rows = sqlx::query_as(&format!(
            "SELECT {COLS} FROM documents \
             WHERE owner_type = $1 AND owner_id = $2 AND deleted_at IS NULL \
             ORDER BY uploaded_at DESC"
        ))
        .bind(owner_type)
        .bind(owner_id)
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows)
    }

    async fn list_by_owner_id(&self, owner_id: &str) -> Result<Vec<Document>, DocError> {
        let rows = sqlx::query_as(&format!(
            "SELECT {COLS} FROM documents \
             WHERE owner_id = $1 AND deleted_at IS NULL \
             ORDER BY uploaded_at DESC"
        ))
        .bind(owner_id)
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows)
    }

    async fn find_by_sha(&self, sha256: &str) -> Result<Option<Document>, DocError> {
        let row = sqlx::query_as(&format!(
            "SELECT {COLS} FROM documents \
             WHERE sha256 = $1 AND deleted_at IS NULL LIMIT 1"
        ))
        .bind(sha256)
        .fetch_optional(self.pool.pool())
        .await?;
        Ok(row)
    }

    async fn search(
        &self,
        query: &str,
        owner: Option<(&str, &str)>,
        limit: i64,
    ) -> Result<Vec<Document>, DocError> {
        // `$1` is the query; `$2` the limit. The optional owner filter binds `$3`/`$4`.
        let owner_clause = if owner.is_some() {
            "AND owner_type = $3 AND owner_id = $4"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {COLS} FROM documents \
             WHERE deleted_at IS NULL \
               AND tsv @@ websearch_to_tsquery('english', $1) {owner_clause} \
             ORDER BY ts_rank(tsv, websearch_to_tsquery('english', $1)) DESC \
             LIMIT $2"
        );
        let mut q = sqlx::query_as(&sql).bind(query).bind(limit);
        if let Some((ot, oid)) = owner {
            q = q.bind(ot).bind(oid);
        }
        let rows = q.fetch_all(self.pool.pool()).await?;
        Ok(rows)
    }

    async fn update(
        &self,
        id: &str,
        expected_version: i64,
        patch: DocumentPatch,
    ) -> Result<Document, DocError> {
        let mut tx = self.pool.pool().begin().await?;

        // Version-guarded update: COALESCE leaves unspecified columns untouched.
        let updated: Option<Document> = sqlx::query_as(&format!(
            "UPDATE documents SET \
               filename = COALESCE($3, filename), \
               body_md = COALESCE($4, body_md), \
               extracted_text = COALESCE($5, extracted_text), \
               version = version + 1 \
             WHERE id = $1 AND version = $2 AND deleted_at IS NULL \
             RETURNING {COLS}"
        ))
        .bind(id)
        .bind(expected_version)
        .bind(&patch.filename)
        .bind(&patch.body_md)
        .bind(&patch.extracted_text)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match updated {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(conflict_or_not_found(self, id, expected_version).await);
            }
        };

        insert_version(&mut tx, &row, &patch.edited_by).await?;
        tx.commit().await?;
        Ok(row)
    }

    async fn soft_delete(&self, id: &str, expected_version: i64) -> Result<(), DocError> {
        let affected = sqlx::query(
            "UPDATE documents SET deleted_at = now(), version = version + 1 \
             WHERE id = $1 AND version = $2 AND deleted_at IS NULL",
        )
        .bind(id)
        .bind(expected_version)
        .execute(self.pool.pool())
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(conflict_or_not_found(self, id, expected_version).await);
        }
        Ok(())
    }
}

/// Append the current state of `row` as an immutable `document_versions` snapshot.
async fn insert_version(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &Document,
    edited_by: &str,
) -> Result<(), DocError> {
    sqlx::query(
        "INSERT INTO document_versions \
         (document_id, version, kind, filename, content_type, sha256, body_md, bucket, key, \
          extracted_text, edited_by) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(&row.id)
    .bind(row.version)
    .bind(&row.kind)
    .bind(&row.filename)
    .bind(&row.content_type)
    .bind(&row.sha256)
    .bind(&row.body_md)
    .bind(&row.bucket)
    .bind(&row.key)
    .bind(&row.extracted_text)
    .bind(edited_by)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A guarded write that affected no rows is either a missing id or a stale version;
/// distinguish them with one extra read so the caller gets the precise error.
async fn conflict_or_not_found(
    repo: &PgDocuments,
    id: &str,
    expected: i64,
) -> DocError {
    match sqlx::query("SELECT version FROM documents WHERE id = $1 AND deleted_at IS NULL")
        .bind(id)
        .fetch_optional(repo.pool.pool())
        .await
    {
        Ok(Some(r)) => {
            let found: i64 = r.get::<i64, _>("version");
            DocError::VersionConflict { id: id.to_string(), expected, found }
        }
        Ok(None) => DocError::NotFound(id.to_string()),
        Err(e) => DocError::Db(e),
    }
}
