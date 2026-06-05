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
use pgvector::Vector;
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
    /// Flat, paged browse of the workspace's live documents, newest first (hq-web-extras.11).
    /// All filters are optional: `owner` narrows to one bead (back-compat with `list_by_owner`),
    /// `content_type` narrows by MIME. Fetches `limit + 1` rows from `offset` so the caller can
    /// derive `has_more` without a separate COUNT; the caller truncates to `limit`.
    async fn browse(
        &self,
        owner: Option<(&str, &str)>,
        content_type: Option<&str>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Document>, DocError>;
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
    /// Store (or replace) the semantic-search embedding of a live document (hq-docs-search.2).
    /// `embedding.len()` must match the column dimension (384). A no-op count is fine — a
    /// soft-deleted/absent row simply isn't updated.
    async fn set_embedding(&self, id: &str, embedding: Vec<f32>) -> Result<(), DocError>;
    /// Phase-2 hybrid search (hq-docs-search.2): fuse the phase-1 full-text rank with vector
    /// cosine similarity against `query_embedding`, ranked by their weighted sum. Live rows
    /// only, capped at `limit`, optionally narrowed to one owner. Rows lacking an embedding
    /// still surface on the text side (the vector term coalesces to 0).
    async fn search_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
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

    async fn browse(
        &self,
        owner: Option<(&str, &str)>,
        content_type: Option<&str>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Document>, DocError> {
        // Build the optional predicates with stable bind positions: $1 offset, $2 limit (+1 for
        // the has_more probe). Owner binds $3/$4; content_type binds the next free slot.
        let mut clauses = String::new();
        if owner.is_some() {
            clauses.push_str(" AND owner_type = $3 AND owner_id = $4");
        }
        if content_type.is_some() {
            // $5 when an owner is also bound, else $3.
            let pos = if owner.is_some() { 5 } else { 3 };
            clauses.push_str(&format!(" AND content_type = ${pos}"));
        }
        let sql = format!(
            "SELECT {COLS} FROM documents \
             WHERE deleted_at IS NULL{clauses} \
             ORDER BY uploaded_at DESC, id DESC \
             OFFSET $1 LIMIT $2"
        );
        let mut q = sqlx::query_as(&sql).bind(offset).bind(limit + 1);
        if let Some((ot, oid)) = owner {
            q = q.bind(ot).bind(oid);
        }
        if let Some(ct) = content_type {
            q = q.bind(ct);
        }
        let rows = q.fetch_all(self.pool.pool()).await?;
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

    async fn set_embedding(&self, id: &str, embedding: Vec<f32>) -> Result<(), DocError> {
        sqlx::query("UPDATE documents SET embedding = $2 WHERE id = $1 AND deleted_at IS NULL")
            .bind(id)
            .bind(Vector::from(embedding))
            .execute(self.pool.pool())
            .await?;
        Ok(())
    }

    async fn search_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
        owner: Option<(&str, &str)>,
        limit: i64,
    ) -> Result<Vec<Document>, DocError> {
        // $1 query text, $2 query vector, $3 limit; optional owner binds $4/$5. Score fuses
        // text rank and (1 - cosine distance), each coalesced to 0 so a row matching on only
        // one side still ranks. Candidates: text match OR an embedding present.
        let owner_clause = if owner.is_some() {
            "AND owner_type = $4 AND owner_id = $5"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {COLS} FROM documents \
             WHERE deleted_at IS NULL \
               AND (tsv @@ websearch_to_tsquery('english', $1) OR embedding IS NOT NULL) \
               {owner_clause} \
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

/// A document share-link row (hq-web-extras.9): a public capability URL onto a LIVE document.
/// The `hash` is the only credential a reader presents; `expires_at`/`revoked` gate access,
/// evaluated lazily at read time so no GC job is required.
#[derive(Debug, Clone, FromRow)]
pub struct DocumentShare {
    /// The capability token (128-bit url-safe), PK and public credential.
    pub hash: String,
    /// The document this share exposes (live content).
    pub document_id: String,
    /// Who minted the share.
    pub created_by: String,
    /// When it was minted.
    pub created_at: DateTime<Utc>,
    /// Expiry instant; `None` = no limit (until revoked).
    pub expires_at: Option<DateTime<Utc>>,
    /// Hard revocation: a revoked share is dead regardless of expiry.
    pub revoked: bool,
    /// Last public read (informational; never gates access).
    pub last_accessed_at: Option<DateTime<Utc>>,
}

impl DocumentShare {
    /// The derived lifecycle state at `now`: `revoked` wins, then a past `expires_at` is
    /// `expired`, else `active`. The string matches the wire contract (active|expired|revoked).
    pub fn state_at(&self, now: DateTime<Utc>) -> &'static str {
        if self.revoked {
            "revoked"
        } else if self.expires_at.is_some_and(|e| now > e) {
            "expired"
        } else {
            "active"
        }
    }
}

/// The per-workspace document-share store port (hq-web-extras.9). Backs the public capability-URL
/// surface; the adapter resolves the tenant by the same `search_path` the documents port does.
#[async_trait]
pub trait SharesRepository: Send + Sync {
    /// Mint a share for a live document. `hash` is the caller-minted capability token;
    /// `expires_at` is the optional TTL deadline. [`DocError::NotFound`] if the document is
    /// absent or soft-deleted (a share onto a dead doc is never created).
    async fn create_share(
        &self,
        hash: &str,
        document_id: &str,
        created_by: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<DocumentShare, DocError>;
    /// List every share whose document is still live, newest first. Backs the owner's management
    /// view (`GET /shares`); the derived state is computed by the caller from each row.
    async fn list_shares(&self) -> Result<Vec<DocumentShare>, DocError>;
    /// Fetch one share by its hash (any state), or `None`.
    async fn get_share(&self, hash: &str) -> Result<Option<DocumentShare>, DocError>;
    /// Reset a share's expiry deadline (extend / shorten / lift): `Some(t)` sets the deadline,
    /// `None` lifts the limit. [`DocError::NotFound`] when no such share exists. Returns the
    /// post-update row.
    async fn set_share_expiry(
        &self,
        hash: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<DocumentShare, DocError>;
    /// Revoke a share (idempotent): a missing or already-revoked share is a no-op success, so a
    /// double-DELETE still returns OK.
    async fn revoke_share(&self, hash: &str) -> Result<(), DocError>;
    /// Read the LIVE document behind an *active* share by hash, touching `last_accessed_at`.
    /// Returns the share row paired with its document only when the share resolves to `active`
    /// at `now`; otherwise the share's derived state (so the caller maps revoked→404,
    /// expired→410). [`DocError::NotFound`] when the hash is unknown or the doc is gone.
    async fn read_active_share(
        &self,
        hash: &str,
        now: DateTime<Utc>,
    ) -> Result<(DocumentShare, Option<Document>), DocError>;
}

#[async_trait]
impl SharesRepository for PgDocuments {
    async fn create_share(
        &self,
        hash: &str,
        document_id: &str,
        created_by: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<DocumentShare, DocError> {
        // Guard: only mint a share onto a live document.
        let exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM documents WHERE id = $1 AND deleted_at IS NULL")
                .bind(document_id)
                .fetch_optional(self.pool.pool())
                .await?;
        if exists.is_none() {
            return Err(DocError::NotFound(document_id.to_string()));
        }
        let row: DocumentShare = sqlx::query_as(
            "INSERT INTO document_shares (hash, document_id, created_by, expires_at) \
             VALUES ($1, $2, $3, $4) \
             RETURNING hash, document_id, created_by, created_at, expires_at, revoked, \
                       last_accessed_at",
        )
        .bind(hash)
        .bind(document_id)
        .bind(created_by)
        .bind(expires_at)
        .fetch_one(self.pool.pool())
        .await?;
        Ok(row)
    }

    async fn list_shares(&self) -> Result<Vec<DocumentShare>, DocError> {
        let rows = sqlx::query_as(
            "SELECT s.hash, s.document_id, s.created_by, s.created_at, s.expires_at, \
                    s.revoked, s.last_accessed_at \
             FROM document_shares s \
             JOIN documents d ON d.id = s.document_id AND d.deleted_at IS NULL \
             ORDER BY s.created_at DESC",
        )
        .fetch_all(self.pool.pool())
        .await?;
        Ok(rows)
    }

    async fn get_share(&self, hash: &str) -> Result<Option<DocumentShare>, DocError> {
        let row = sqlx::query_as(
            "SELECT hash, document_id, created_by, created_at, expires_at, revoked, \
                    last_accessed_at FROM document_shares WHERE hash = $1",
        )
        .bind(hash)
        .fetch_optional(self.pool.pool())
        .await?;
        Ok(row)
    }

    async fn set_share_expiry(
        &self,
        hash: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<DocumentShare, DocError> {
        // A NULL `expires_at` lifts the limit; a value resets it. The `$2 IS NULL` arm makes the
        // "lift" explicit rather than a no-op COALESCE would-be.
        let row: Option<DocumentShare> = sqlx::query_as(
            "UPDATE document_shares SET expires_at = $2 WHERE hash = $1 \
             RETURNING hash, document_id, created_by, created_at, expires_at, revoked, \
                       last_accessed_at",
        )
        .bind(hash)
        .bind(expires_at)
        .fetch_optional(self.pool.pool())
        .await?;
        row.ok_or_else(|| DocError::NotFound(hash.to_string()))
    }

    async fn revoke_share(&self, hash: &str) -> Result<(), DocError> {
        // Idempotent: a missing/already-revoked share is a successful no-op.
        sqlx::query("UPDATE document_shares SET revoked = TRUE WHERE hash = $1")
            .bind(hash)
            .execute(self.pool.pool())
            .await?;
        Ok(())
    }

    async fn read_active_share(
        &self,
        hash: &str,
        now: DateTime<Utc>,
    ) -> Result<(DocumentShare, Option<Document>), DocError> {
        let share = match self.get_share(hash).await? {
            Some(s) => s,
            None => return Err(DocError::NotFound(hash.to_string())),
        };
        // Only resolve the document when the share is active; the caller maps the derived state
        // (revoked→404, expired→410) without ever leaking the content.
        if share.state_at(now) != "active" {
            return Ok((share, None));
        }
        let doc = self.get(&share.document_id).await?;
        // A soft-deleted/absent doc behind an otherwise-active share reads as gone.
        let doc = doc.filter(|d| d.deleted_at.is_none());
        if doc.is_some() {
            // Best-effort touch; a failure here never blocks the read.
            let _ = sqlx::query("UPDATE document_shares SET last_accessed_at = $2 WHERE hash = $1")
                .bind(hash)
                .bind(now)
                .execute(self.pool.pool())
                .await;
        }
        Ok((share, doc))
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
