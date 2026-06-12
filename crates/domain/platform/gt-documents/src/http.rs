//! The `axum` REST adapter for the `documents.*` surface (`hq-fe-api-platform.3`).
//!
//! Copies the gt-issues template (`hq-auth-routes.2`): it exposes the document subsystem as REST
//! routes that dispatch to the **same** stores the MCP `documents.*` tools use — the
//! per-workspace [`PgDocuments`] repository, the blob store, the text extractor, and the optional
//! embedder — so no domain logic is duplicated, only the wire shape differs. The dispatch body is
//! ported verbatim from the MCP `DocumentsHandler` (`gt-composition/src/mcp/documents.rs`): a
//! `kind="md"` attach lands its body in Postgres; a `kind="blob"` attach decodes the base64
//! payload, content-addresses it (sha256), uploads it to the object store (dedup), and runs it
//! through the extractor so `extracted_text` is searchable. It folds into `gt-documents` behind
//! the off-by-default `axum` feature (docs/03 Rule 4), never a sibling crate.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/documents` and wraps it with the capability-derived scope guard; the composition
//!   root layers the auth middleware in front. A handler here only runs once the caller already
//!   holds `documents.read` / `documents.write` for the request's verb-class.
//! - **It never reads the workspace from the path or body** (docs/04 §15, docs/03 Rule 6). The
//!   tenant is the server-injected `X-Workspace` header (resolved upstream from the auth context,
//!   reconciled against the JWT `workspace` claim), exactly as the SSE feed + MCP dispatch key on
//!   it. A document id is a plain resource identifier on the path; the workspace is never a route
//!   parameter or a body field.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::{json, Value};

use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_store_blob::{sha256_hex, BlobStore};
use chrono::{DateTime, Duration, Utc};
use gt_store_pg::{
    DocError, Document, DocumentPatch, DocumentShare, DocumentsRepository, NewDocument,
    PgDocuments, SharesRepository, VectorStore, WorkspacePool,
};

use crate::commands::{
    AttachDoc, CreateShare, ListDocs, PatchShare, RemoveDoc, SearchDocs, UpdateDoc, ValidationError,
};

/// The header carrying the request's tenant — server-injected by the auth/workspace middleware,
/// never read from a URL or body field (the same `X-Workspace` convention the dispatch + SSE
/// feed key on).
const WORKSPACE_HEADER: &str = "x-workspace";

/// The workspace a request resolves to when it carries no `X-Workspace` header — the
/// single-tenant default, schema `ws_default` (matches the MCP dispatch default).
const DEFAULT_WORKSPACE: &str = "default";

/// Everything the documents REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`documents_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract). Cheap to clone — an `Arc` over
/// the shared inner state.
#[derive(Clone)]
pub struct DocumentsApiState {
    inner: Arc<Inner>,
}

/// The shared backing handles: a lazily-populated per-workspace pool cache (mirroring
/// `WsPools`), the optional object store + its bucket, the extraction pipeline, and the optional
/// embedding engine.
struct Inner {
    /// The base Postgres URL; the schema is selected per workspace on checkout.
    url: String,
    /// One `search_path`-scoped pool per workspace slug, opened on first use and cached so a
    /// handler does not reconnect on every call (same shape as the dispatch tier's `WsPools`).
    cache: Mutex<HashMap<String, WorkspacePool>>,
    /// `None` when no object store is configured — `kind="blob"` attach then errors, but `.md`
    /// documents (the differentiator) still work.
    blob: Option<Arc<BlobStore>>,
    /// The bucket name recorded on a blob row's pointer.
    bucket: String,
    extractor: Extractor,
    /// Optional semantic-search engine (hq-docs-search.2). `Some` ⇒ documents are embedded on
    /// attach/update and `search` uses the hybrid (text + vector) path; `None` ⇒ full-text only.
    embedder: Option<Arc<dyn Embedder>>,
}

impl DocumentsApiState {
    /// Wire the REST state over the base Postgres URL, the optional blob store + its bucket, the
    /// extraction pipeline, and the optional embedding engine. The binary calls this and hands
    /// the module the result via [`DocumentsModule::with_http`](crate::DocumentsModule::with_http).
    pub fn new(
        pg_url: impl Into<String>,
        blob: Option<Arc<BlobStore>>,
        bucket: impl Into<String>,
        extractor: Extractor,
        embedder: Option<Arc<dyn Embedder>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                url: pg_url.into(),
                cache: Mutex::new(HashMap::new()),
                blob,
                bucket: bucket.into(),
                extractor,
                embedder,
            }),
        }
    }

    /// A document repository scoped to `workspace` (or the default workspace when `None`),
    /// reusing a cached pool when present.
    async fn repo(&self, workspace: Option<&str>) -> Result<PgDocuments, ApiError> {
        Ok(PgDocuments::new(self.pool(workspace).await?))
    }

    /// The cached (or freshly connected) `search_path`-scoped pool for `workspace`.
    async fn pool(&self, workspace: Option<&str>) -> Result<WorkspacePool, ApiError> {
        let slug = workspace.unwrap_or(DEFAULT_WORKSPACE);
        if let Some(pool) = self.inner.cache.lock().unwrap().get(slug).cloned() {
            return Ok(pool);
        }
        let pool = WorkspacePool::connect(&self.inner.url, slug)
            .await
            .map_err(|e| ApiError::Internal(format!("workspace pool connect ({slug}): {e}")))?;
        self.inner.cache.lock().unwrap().insert(slug.to_string(), pool.clone());
        Ok(pool)
    }

    /// The `documents.attach.execute` body: persist a `.md` body directly, or content-address +
    /// upload a binary and extract its text. Ported verbatim from the MCP `DocumentsHandler`.
    async fn attach(&self, workspace: Option<&str>, cmd: AttachDoc) -> Result<Document, ApiError> {
        let repo = self.repo(workspace).await?;
        let id = ulid::Ulid::new().to_string();

        let new = if cmd.kind == "md" {
            let body = cmd.body_md.unwrap_or_default();
            NewDocument {
                id: id.clone(),
                owner_type: cmd.owner_type,
                owner_id: cmd.owner_id,
                kind: "md".into(),
                filename: cmd.filename,
                content_type: cmd.content_type.or_else(|| Some("text/markdown".into())),
                size: Some(body.len() as i64),
                sha256: Some(sha256_hex(body.as_bytes())),
                body_md: Some(body),
                bucket: None,
                key: None,
                extracted_text: None,
                uploaded_by: cmd.created_by,
            }
        } else {
            // kind == "blob": decode, content-address, upload (dedup), extract.
            let bytes = B64
                .decode(cmd.data_base64.unwrap_or_default())
                .map_err(|e| ApiError::Validation(format!("data_base64 is not valid base64: {e}")))?;
            let content_type = cmd.content_type.clone().unwrap_or_default();
            let blob = self
                .inner
                .blob
                .as_ref()
                .ok_or_else(|| ApiError::Internal("no object store configured for blob attachments".into()))?;
            let sha = sha256_hex(&bytes);
            let ws_slug = workspace.unwrap_or(DEFAULT_WORKSPACE);
            let key = BlobStore::key_for(ws_slug, &cmd.owner_type, &cmd.owner_id, &sha, &cmd.filename);

            // Dedup: only upload if this exact object is absent.
            if !blob.exists(&key).await.map_err(blob_err)? {
                blob.put(&key, bytes.clone(), Some(&content_type)).await.map_err(blob_err)?;
            }
            // Extraction is best-effort: an unsupported/garbled binary still attaches; only the
            // text index misses it.
            let extracted = self.inner.extractor.extract(&content_type, &bytes).await.ok();

            NewDocument {
                id: id.clone(),
                owner_type: cmd.owner_type,
                owner_id: cmd.owner_id,
                kind: "blob".into(),
                filename: cmd.filename,
                content_type: Some(content_type),
                size: Some(bytes.len() as i64),
                sha256: Some(sha),
                body_md: None,
                bucket: Some(self.inner.bucket.clone()),
                key: Some(key),
                extracted_text: extracted,
                uploaded_by: cmd.created_by,
            }
        };

        let doc = repo.create(new).await.map_err(doc_err)?;
        self.embed_doc(&repo, &doc).await;
        Ok(doc)
    }

    /// Best-effort: embed a document's text and store the vector (hq-docs-search.2). A missing
    /// embedder, empty text, or an embed/store failure is silently skipped — semantic search then
    /// just misses this row; attach/update never fail on it. Generic over [`VectorStore`]
    /// (hq-vectorstore-abstraction) rather than the concrete [`PgDocuments`], so a future
    /// non-Postgres vector backend is a swap at the call site, not here.
    async fn embed_doc(&self, repo: &impl VectorStore, doc: &Document) {
        let Some(emb) = &self.inner.embedder else { return };
        let text = doc.body_md.as_deref().or(doc.extracted_text.as_deref()).unwrap_or("");
        if text.trim().is_empty() {
            return;
        }
        if let Ok(vec) = emb.embed(text).await {
            let _ = repo.set_embedding(&doc.id, vec).await;
        }
    }
}

/// Build the documents REST router with `state` baked in (`hq-fe-api-platform.3`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/documents` and applies the
/// scope guard. [`register_routes`](crate::DocumentsModule) returns exactly this router when the
/// module carries HTTP state.
///
/// | Method + path           | Maps to MCP tool            |
/// |-------------------------|-----------------------------|
/// | `GET /?owner_type&owner_id&content_type&offset&limit` | `documents.list.execute` |
/// | `POST /`                | `documents.attach.execute`  |
/// | `GET /search?query`     | `documents.search.execute`  |
/// | `GET /:id`              | `gt://doc/{id}` read        |
/// | `PATCH /:id`            | `documents.update.execute`  |
/// | `DELETE /:id?expected_version` | `documents.remove.execute` |
/// | `POST /:id/share`       | REST-only (hq-web-extras.9)  |
/// | `GET /shares`           | REST-only (hq-web-extras.9)  |
/// | `PATCH /shares/:hash`   | REST-only (hq-web-extras.9)  |
/// | `DELETE /shares/:hash`  | REST-only (hq-web-extras.9)  |
pub fn documents_router(state: DocumentsApiState) -> Router {
    Router::new()
        .route("/", get(list_documents).post(attach_document))
        .route("/search", get(search_documents))
        .route("/shares", get(list_shares))
        .route("/shares/:hash", axum::routing::patch(patch_share).delete(revoke_share))
        .route("/:id/share", axum::routing::post(create_share))
        .route("/:id/download", get(download_document))
        .route("/:id", get(get_document).patch(update_document).delete(remove_document))
        .with_state(state)
}

/// Build the PUBLIC share-read router (hq-web-extras.9): `GET /share/:hash` served OUTSIDE the
/// `/api/v1/<ns>` chain — unauthenticated, like `/openapi.json`. The composition bin mounts this
/// on the ops/public router, never behind the RS256 auth + scope guard. It exposes the LIVE
/// content of a document behind an *active* share; a revoked share is a `404`, an expired one a
/// `410 Gone`.
pub fn public_share_router(state: DocumentsApiState) -> Router {
    Router::new().route("/share/:hash", get(read_public_share)).with_state(state)
}

/// `POST /` — attach a document to a bead (`documents.attach.execute`). The id is minted
/// server-side (a ULID); `created_by` rides in the body (the agent's attribution, as on the MCP
/// path). `201` on success with the stored document.
#[utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Document attached"),
        (status = 422, description = "Validation failed (shape / bad base64)"),
        (status = 500, description = "No object store configured for a blob attach, or a store error"),
    ),
)]
async fn attach_document(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Json(cmd): Json<AttachDoc>,
) -> Result<Response, ApiError> {
    cmd.validate()?;
    let doc = st.attach(workspace_of(&headers).as_deref(), cmd).await?;
    Ok((StatusCode::CREATED, Json(doc_json(&doc))).into_response())
}

/// `GET /?owner_type&owner_id&content_type&offset&limit` — a flat, paged browse of the
/// workspace's live documents, newest first (`documents.list.execute`, hq-web-extras.11).
///
/// Owner is optional: with both `owner_type`+`owner_id` it filters to one bead (back-compat);
/// with neither it browses the whole workspace. `content_type` narrows by MIME. The response is
/// the paged envelope `{ documents, offset, limit, has_more, next_offset }` (the `gt://issues`
/// shape) — `next_offset` is `null` on the last page.
#[utoipa::path(
    get, path = "/",
    params(
        ("owner_type" = Option<String>, Query, description = "Optional owner narrowing (paired with owner_id)"),
        ("owner_id" = Option<String>, Query, description = "Optional owner narrowing (paired with owner_type)"),
        ("content_type" = Option<String>, Query, description = "Optional MIME-type filter"),
        ("offset" = Option<i64>, Query, description = "Zero-based page offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max rows per page (default 50)"),
    ),
    responses((status = 200, description = "A page of workspace documents (paged envelope)")),
)]
async fn list_documents(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Query(cmd): Query<ListDocs>,
) -> Result<Json<Value>, ApiError> {
    cmd.validate()?;
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let owner = match (&cmd.owner_type, &cmd.owner_id) {
        (Some(t), Some(i)) => Some((t.as_str(), i.as_str())),
        _ => None,
    };
    // browse fetches limit+1 to derive has_more without a COUNT; truncate to the page.
    let mut docs = repo
        .browse(owner, cmd.content_type.as_deref(), cmd.offset, cmd.limit)
        .await
        .map_err(doc_err)?;
    let has_more = docs.len() as i64 > cmd.limit;
    if has_more {
        docs.truncate(cmd.limit as usize);
    }
    let next_offset = has_more.then_some(cmd.offset + cmd.limit);
    Ok(Json(json!({
        "documents": docs.iter().map(doc_json).collect::<Vec<_>>(),
        "offset": cmd.offset,
        "limit": cmd.limit,
        "has_more": has_more,
        "next_offset": next_offset,
    })))
}

/// `GET /search?query&owner_type&owner_id&limit` — hybrid (text + vector) search when an embedder
/// is wired, else phase-1 full-text (`documents.search.execute`). An embed failure degrades to
/// full-text rather than erroring, exactly as the MCP path does.
#[utoipa::path(
    get, path = "/search",
    params(
        ("query" = String, Query, description = "Free-text query (websearch syntax)"),
        ("owner_type" = Option<String>, Query, description = "Optional owner narrowing (paired with owner_id)"),
        ("owner_id" = Option<String>, Query, description = "Optional owner narrowing (paired with owner_type)"),
        ("limit" = Option<i64>, Query, description = "Max hits (default 20)"),
    ),
    responses((status = 200, description = "Ranked document hits")),
)]
async fn search_documents(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Query(cmd): Query<SearchDocs>,
) -> Result<Json<Value>, ApiError> {
    cmd.validate()?;
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let owner = match (&cmd.owner_type, &cmd.owner_id) {
        (Some(t), Some(i)) => Some((t.as_str(), i.as_str())),
        _ => None,
    };
    let docs = match &st.inner.embedder {
        Some(emb) => match emb.embed(&cmd.query).await {
            Ok(vec) => repo.search_hybrid(&cmd.query, &vec, owner, cmd.limit).await.map_err(doc_err)?,
            Err(_) => repo.search(&cmd.query, owner, cmd.limit).await.map_err(doc_err)?,
        },
        None => repo.search(&cmd.query, owner, cmd.limit).await.map_err(doc_err)?,
    };
    Ok(Json(json!({ "documents": docs.iter().map(doc_json).collect::<Vec<_>>() })))
}

/// `GET /:id` — one document with its body/extracted text + `version`; `404` when no live row
/// matches (`gt://doc/{id}` read).
#[utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Document id")),
    responses(
        (status = 200, description = "The document"),
        (status = 404, description = "No document with that id"),
    ),
)]
async fn get_document(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    match repo.get(&id).await.map_err(doc_err)? {
        Some(doc) => Ok(Json(doc_json(&doc))),
        None => Err(ApiError::NotFound(format!("document not found: {id}"))),
    }
}

/// `GET /:id/download` — the document's raw bytes as a file download (hq-039316): a `md` row
/// serves its `body_md` as `text/markdown`; a `blob` row proxies the object-store bytes through
/// the backend (works on every backend — the `fs` dev store cannot presign). The
/// `Content-Disposition: attachment` header carries the stored filename so the browser saves
/// e.g. a generated xlsx report under its real name.
#[utoipa::path(
    get, path = "/{id}/download",
    params(("id" = String, Path, description = "Document id")),
    responses(
        (status = 200, description = "The raw document bytes (Content-Disposition: attachment)"),
        (status = 404, description = "No document with that id"),
    ),
)]
async fn download_document(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let doc = repo
        .get(&id)
        .await
        .map_err(doc_err)?
        .ok_or_else(|| ApiError::NotFound(format!("document not found: {id}")))?;

    let (bytes, content_type) = if doc.kind == "md" {
        (
            doc.body_md.clone().unwrap_or_default().into_bytes(),
            doc.content_type.clone().unwrap_or_else(|| "text/markdown".into()),
        )
    } else {
        let key = doc
            .key
            .as_deref()
            .ok_or_else(|| ApiError::Internal(format!("blob document {id} has no object key")))?;
        let blob = st
            .inner
            .blob
            .as_ref()
            .ok_or_else(|| ApiError::Internal("no object store configured for blob attachments".into()))?;
        (
            blob.get(key).await.map_err(blob_err)?,
            doc.content_type.clone().unwrap_or_else(|| "application/octet-stream".into()),
        )
    };

    // RFC 6266/5987: ASCII fallback + UTF-8 form so any stored filename survives the header.
    let ascii: String = doc
        .filename
        .chars()
        .map(|c| if c.is_ascii_graphic() && c != '"' { c } else { '_' })
        .collect();
    let utf8: String = doc
        .filename
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let disposition = format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{utf8}");

    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_TYPE, content_type),
            (axum::http::header::CONTENT_DISPOSITION, disposition),
        ],
        bytes,
    )
        .into_response())
}

/// `PATCH /:id` — edit a document under its optimistic version guard (`documents.update.execute`).
/// The id always comes from the path and overwrites any `id` in the body (docs/03 Rule 6).
/// Returns the post-bump document so a follow-up edit can chain `expected_version`.
#[utoipa::path(
    patch, path = "/{id}",
    params(("id" = String, Path, description = "Document id")),
    responses(
        (status = 200, description = "Patched; returns the post-bump document"),
        (status = 404, description = "No document with that id"),
        (status = 409, description = "Version conflict (stale expected_version)"),
        (status = 422, description = "Validation failed"),
    ),
)]
async fn update_document(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: UpdateDoc = with_path_id(body, id)?;
    cmd.validate()?;
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let doc = repo
        .update(
            &cmd.id,
            cmd.expected_version,
            DocumentPatch {
                filename: cmd.filename,
                body_md: cmd.body_md,
                extracted_text: None,
                edited_by: cmd.edited_by,
            },
        )
        .await
        .map_err(doc_err)?;
    // Re-embed: the body may have changed (hq-docs-search.2). Best-effort.
    st.embed_doc(&repo, &doc).await;
    Ok(Json(doc_json(&doc)))
}

/// `DELETE /:id?expected_version` — soft-delete a document under its version guard
/// (`documents.remove.execute`). The id is the path; `expected_version` is the querystring (a
/// DELETE carries no body). `200` echoes the removed id.
#[utoipa::path(
    delete, path = "/{id}",
    params(
        ("id" = String, Path, description = "Document id"),
        ("expected_version" = i64, Query, description = "The version the caller last read"),
    ),
    responses(
        (status = 200, description = "Document soft-deleted"),
        (status = 404, description = "No document with that id"),
        (status = 409, description = "Version conflict (stale expected_version)"),
        (status = 422, description = "Validation failed"),
    ),
)]
async fn remove_document(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<RemoveQuery>,
) -> Result<Json<Value>, ApiError> {
    let cmd = RemoveDoc { id, expected_version: q.expected_version };
    cmd.validate()?;
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    repo.soft_delete(&cmd.id, cmd.expected_version).await.map_err(doc_err)?;
    Ok(Json(json!({ "ok": true, "id": cmd.id, "removed": true })))
}

/// Querystring for `DELETE /:id` — the version guard a DELETE cannot carry in a body.
#[derive(Debug, Deserialize)]
struct RemoveQuery {
    /// The `version` the caller last read; a stale value is rejected at the store.
    expected_version: i64,
}

// ---------------------------------------------------------------------------------------------
// Share lifecycle (hq-web-extras.9): public capability URLs onto a live document.
// ---------------------------------------------------------------------------------------------

/// `POST /:id/share` — mint a public capability URL onto a document (`documents.write`). The doc
/// id rides on the path. `expires_in` (seconds, optional) sets the TTL; omit it for no limit.
/// `201` with the share `{ hash, url, state, expires_at, ... }`. Several shares per document are
/// allowed, each with its own TTL.
#[utoipa::path(
    post, path = "/{id}/share",
    params(("id" = String, Path, description = "Document id to share")),
    responses(
        (status = 201, description = "Share minted"),
        (status = 404, description = "No live document with that id"),
        (status = 422, description = "Validation failed (non-positive TTL)"),
    ),
)]
async fn create_share(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Option<Json<CreateShare>>,
) -> Result<Response, ApiError> {
    let cmd = body.map(|Json(c)| c).unwrap_or_default();
    cmd.validate()?;
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let hash = mint_hash();
    let expires_at = cmd.expires_in.map(|secs| Utc::now() + Duration::seconds(secs));
    let created_by = cmd.created_by.unwrap_or_default();
    let share = repo
        .create_share(&hash, &id, &created_by, expires_at)
        .await
        .map_err(doc_err)?;
    Ok((StatusCode::CREATED, Json(share_json(&share, Utc::now()))).into_response())
}

/// `GET /shares` — list the workspace's shares whose document is still live, with each share's
/// derived state (`active|expired|revoked`) computed now (`documents.read`).
#[utoipa::path(
    get, path = "/shares",
    responses((status = 200, description = "The workspace's shares with derived state")),
)]
async fn list_shares(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let now = Utc::now();
    let shares = SharesRepository::list_shares(&repo).await.map_err(doc_err)?;
    Ok(Json(json!({
        "shares": shares.iter().map(|s| share_json(s, now)).collect::<Vec<_>>(),
    })))
}

/// `PATCH /shares/:hash` — dynamically reset a share's limit: extend/shorten via `expires_in`
/// (seconds from now) or `expires_at` (RFC3339), or lift it entirely by sending neither
/// (`documents.write`). Returns the post-update share. `404` when the hash is unknown.
#[utoipa::path(
    patch, path = "/shares/{hash}",
    params(("hash" = String, Path, description = "Share capability hash")),
    responses(
        (status = 200, description = "Share limit reset; returns the share"),
        (status = 404, description = "No share with that hash"),
        (status = 422, description = "Validation failed (both forms set / bad timestamp)"),
    ),
)]
async fn patch_share(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(hash): Path<String>,
    body: Option<Json<PatchShare>>,
) -> Result<Json<Value>, ApiError> {
    let cmd = body.map(|Json(c)| c).unwrap_or_default();
    cmd.validate()?;
    // Resolve the new deadline: relative (expires_in) → absolute now+TTL; absolute (expires_at)
    // → parse RFC3339; neither present → None (lift the limit).
    let expires_at = if let Some(secs) = cmd.expires_in {
        Some(Utc::now() + Duration::seconds(secs))
    } else if let Some(ts) = &cmd.expires_at {
        Some(
            DateTime::parse_from_rfc3339(ts)
                .map_err(|e| ApiError::Validation(format!("expires_at is not RFC3339: {e}")))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let share = repo.set_share_expiry(&hash, expires_at).await.map_err(doc_err)?;
    Ok(Json(share_json(&share, Utc::now())))
}

/// `DELETE /shares/:hash` — revoke a share (`documents.write`). Idempotent: a missing or
/// already-revoked share is still a `200`, so a double-DELETE is safe.
#[utoipa::path(
    delete, path = "/shares/{hash}",
    params(("hash" = String, Path, description = "Share capability hash")),
    responses((status = 200, description = "Share revoked (idempotent)")),
)]
async fn revoke_share(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    SharesRepository::revoke_share(&repo, &hash).await.map_err(doc_err)?;
    Ok(Json(json!({ "ok": true, "hash": hash, "revoked": true })))
}

/// `GET /share/:hash` — PUBLIC, unauthenticated read of the LIVE document behind an *active*
/// share (hq-web-extras.9). Served outside the `/api/v1` auth chain. Lazy expiry: the state is
/// computed at read time. `active` ⇒ `200` with the document (markdown + metadata); `revoked` ⇒
/// `404`; `expired` ⇒ `410 Gone`; unknown hash / vanished doc ⇒ `404`. Never leaks session data.
#[utoipa::path(
    get, path = "/share/{hash}",
    params(("hash" = String, Path, description = "Share capability hash (the public credential)")),
    responses(
        (status = 200, description = "The live document behind an active share"),
        (status = 404, description = "Unknown / revoked share, or the document is gone"),
        (status = 410, description = "The share has expired"),
    ),
)]
async fn read_public_share(
    State(st): State<DocumentsApiState>,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Result<Response, ApiError> {
    let repo = st.repo(workspace_of(&headers).as_deref()).await?;
    let now = Utc::now();
    let (share, doc) = repo.read_active_share(&hash, now).await.map_err(doc_err)?;
    match share.state_at(now) {
        "active" => match doc {
            // Public payload: the document content + meta, plus the share fields the renderer
            // shows. No owner/session data beyond what the document row already carries.
            Some(d) => Ok((StatusCode::OK, Json(json!({
                "share": share_json(&share, now),
                "document": doc_json(&d),
            })))
            .into_response()),
            // Active share but the doc vanished (soft-deleted) ⇒ gone.
            None => Err(ApiError::NotFound(format!("document behind share {hash} is gone"))),
        },
        // A revoked share is indistinguishable from an unknown one to the public (no oracle).
        "revoked" => Err(ApiError::NotFound(format!("share not found: {hash}"))),
        // Expired ⇒ 410 Gone (the capability existed but its window closed).
        _ => Err(ApiError::Gone(format!("share expired: {hash}"))),
    }
}

/// Mint a 128-bit url-safe capability token: 16 bytes of OS entropy, base64-url (no padding) →
/// 22 chars. Brute-forcing the keyspace is computationally infeasible.
fn mint_hash() -> String {
    let mut bytes = [0u8; 16];
    // `getrandom` reads the OS CSPRNG; a failure here is unrecoverable (no entropy source).
    getrandom::getrandom(&mut bytes).expect("OS CSPRNG available for share hash");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Render a [`DocumentShare`] as the JSON the API returns: the hash + derived `state` at `now`,
/// the absolute `expires_at`, and the relative public capability path (`/share/{hash}`) the FE
/// resolves against its origin.
fn share_json(s: &DocumentShare, now: DateTime<Utc>) -> Value {
    json!({
        "hash": s.hash,
        "document_id": s.document_id,
        "url": format!("/share/{}", s.hash),
        "state": s.state_at(now),
        "created_by": s.created_by,
        "created_at": s.created_at.to_rfc3339(),
        "expires_at": s.expires_at.map(|t| t.to_rfc3339()),
        "revoked": s.revoked,
        "last_accessed_at": s.last_accessed_at.map(|t| t.to_rfc3339()),
    })
}

/// The combined OpenAPI document for the documents REST surface (`hq-fe-api-platform.3`). The
/// builder mounts it under the module prefix and rewrites its relative paths to
/// `/api/v1/documents/...`, so the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    attach_document,
    list_documents,
    search_documents,
    get_document,
    download_document,
    update_document,
    remove_document,
    create_share,
    list_shares,
    patch_share,
    revoke_share,
))]
pub struct ApiDoc;

/// The OpenAPI document for the PUBLIC share-read surface (`GET /share/{hash}`,
/// hq-web-extras.9). Kept SEPARATE from [`ApiDoc`] because it is mounted outside the
/// `/api/v1/documents` prefix — directly at the app root, unauthenticated, like `/openapi.json`.
/// The composition bin merges this into the fused `/openapi.json` so codegen sees it.
#[derive(utoipa::OpenApi)]
#[openapi(paths(read_public_share))]
pub struct PublicApiDoc;

/// The rendered OpenAPI for the public share-read surface, as the concrete
/// [`utoipa::openapi::OpenApi`] — so a consumer (the composition bin) can `merge` it into the
/// fused `/openapi.json` without importing the `utoipa::OpenApi` derive trait itself.
pub fn public_openapi() -> utoipa::openapi::OpenApi {
    <PublicApiDoc as utoipa::OpenApi>::openapi()
}

/// The request's tenant from the server-injected `X-Workspace` header, or `None` (the default
/// workspace) when absent/blank. Never read from the path or body (docs/04 §15).
fn workspace_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(WORKSPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// Deserialize a path-addressed command body, forcing the `id` to the path segment so it is never
/// trusted from the payload (docs/03 Rule 6). The path id overwrites any `id` in the body and
/// supplies it when absent. A malformed body surfaces as a `422`.
fn with_path_id<T: serde::de::DeserializeOwned>(mut body: Value, id: String) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert("id".to_string(), Value::String(id));
        }
        // A non-object body (e.g. `null` from an empty request) becomes just the id.
        other => *other = json!({ "id": id }),
    }
    serde_json::from_value(body)
        .map_err(|e| ApiError::Validation(format!("invalid arguments: {e}")))
}

/// HTTP error space for the documents surface, mapping the store/shape error kinds onto the right
/// status. The body is the bare error message — the same text the MCP path surfaces — so a client
/// sees an identical reason across transports.
#[derive(Debug)]
enum ApiError {
    /// `404` — no such document.
    NotFound(String),
    /// `422` — a shape/validation rejection (incl. bad base64).
    Validation(String),
    /// `409` — an optimistic-concurrency conflict (stale `expected_version`).
    Conflict(String),
    /// `410` — the resource existed but its window has closed (an expired share).
    Gone(String),
    /// `500` — a backend/store failure or missing object store.
    Internal(String),
}

impl From<ValidationError> for ApiError {
    fn from(e: ValidationError) -> Self {
        ApiError::Validation(e.0)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Validation(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Gone(m) => (StatusCode::GONE, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, msg).into_response()
    }
}

/// Map a [`DocError`] onto the HTTP error space: missing id ⇒ `404`, stale version ⇒ `409`,
/// backend failure ⇒ `500`.
fn doc_err(e: DocError) -> ApiError {
    match &e {
        DocError::NotFound(_) => ApiError::NotFound(e.to_string()),
        DocError::VersionConflict { .. } => ApiError::Conflict(e.to_string()),
        DocError::Db(_) => ApiError::Internal(e.to_string()),
    }
}

/// Map a blob-store error onto the HTTP error space (`500`).
fn blob_err(e: gt_store_blob::BlobError) -> ApiError {
    ApiError::Internal(e.to_string())
}

/// Render a [`Document`] as the JSON payload returned to the client. `body_md`/`extracted_text`
/// are inlined so a model reading a list gets the content directly (matches the MCP `doc_json`).
fn doc_json(d: &Document) -> Value {
    json!({
        "id": d.id,
        "owner_type": d.owner_type,
        "owner_id": d.owner_id,
        "kind": d.kind,
        "filename": d.filename,
        "content_type": d.content_type,
        "size": d.size,
        "sha256": d.sha256,
        "body_md": d.body_md,
        "extracted_text": d.extracted_text,
        "bucket": d.bucket,
        "key": d.key,
        "version": d.version,
        "deleted_at": d.deleted_at.map(|t| t.to_rfc3339()),
        "uploaded_by": d.uploaded_by,
        "uploaded_at": d.uploaded_at.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        // The spec the builder mounts: paths are relative (no `/api/v1/documents`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in
            ["/", "/search", "/{id}", "/{id}/share", "/shares", "/shares/{hash}"]
        {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/documents`.
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn public_apidoc_carries_only_the_share_read_route() {
        use utoipa::OpenApi;
        let doc = PublicApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        assert_eq!(paths, ["/share/{hash}"], "public doc is just the share read: {paths:?}");
    }

    #[test]
    fn mint_hash_is_22_char_urlsafe_and_unique() {
        let a = mint_hash();
        let b = mint_hash();
        assert_eq!(a.len(), 22, "16 bytes base64url-no-pad = 22 chars: {a}");
        assert_ne!(a, b, "two mints differ");
        assert!(
            a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "url-safe alphabet only: {a}",
        );
    }

    #[test]
    fn share_state_derivation_revoked_wins_then_expiry() {
        let now = Utc::now();
        let base = DocumentShare {
            hash: "h".into(),
            document_id: "d".into(),
            created_by: "u".into(),
            created_at: now,
            expires_at: None,
            revoked: false,
            last_accessed_at: None,
        };
        assert_eq!(base.state_at(now), "active", "no expiry, not revoked");
        let mut expired = base.clone();
        expired.expires_at = Some(now - Duration::seconds(1));
        assert_eq!(expired.state_at(now), "expired");
        let mut revoked = expired.clone();
        revoked.revoked = true;
        assert_eq!(revoked.state_at(now), "revoked", "revoked beats expiry");
    }

    #[test]
    fn error_status_mapping_matches_kinds() {
        let cases = [
            (ApiError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (ApiError::Validation("x".into()), StatusCode::UNPROCESSABLE_ENTITY),
            (ApiError::Conflict("x".into()), StatusCode::CONFLICT),
            (ApiError::Gone("x".into()), StatusCode::GONE),
            (ApiError::Internal("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (err, want) in cases {
            assert_eq!(err.into_response().status(), want);
        }
    }

    #[test]
    fn doc_err_maps_store_kinds_to_http() {
        assert!(matches!(doc_err(DocError::NotFound("d".into())), ApiError::NotFound(_)));
        assert!(matches!(
            doc_err(DocError::VersionConflict { id: "d".into(), expected: 1, found: 2 }),
            ApiError::Conflict(_)
        ));
    }

    #[test]
    fn with_path_id_forces_path_over_body() {
        // The body names `decoy`; the path `keep` must win and land in the parsed command.
        let parsed: UpdateDoc = with_path_id(
            json!({ "id": "decoy", "expected_version": 0, "filename": "f", "edited_by": "e" }),
            "keep".into(),
        )
        .expect("parses");
        assert_eq!(parsed.id, "keep");
    }

    #[test]
    fn workspace_of_reads_header_or_defaults_none() {
        let mut h = HeaderMap::new();
        assert_eq!(workspace_of(&h), None);
        h.insert(WORKSPACE_HEADER, "acme".parse().unwrap());
        assert_eq!(workspace_of(&h).as_deref(), Some("acme"));
        // A blank header is treated as absent (⇒ the default workspace).
        h.insert(WORKSPACE_HEADER, "".parse().unwrap());
        assert_eq!(workspace_of(&h), None);
    }
}
