//! `documents.*` domain dispatch (hq-docs-api.2, docs/11).
//!
//! Routes the document subsystem tools — `documents.{attach,update,remove,list,search}` in
//! `validate`/`execute` pairs — onto the per-workspace [`PgDocuments`] repository, the blob
//! store ([`BlobStore`]), and the text extractor ([`Extractor`]).
//!
//! `kind="md"` content lands directly in Postgres (`body_md`); `kind="blob"` bytes are
//! decoded from base64, content-addressed (sha256), uploaded to the object store (dedup:
//! an identical blob is not re-uploaded), and run through the extractor so `extracted_text`
//! is searchable. Every write is scoped to the caller's `ws_<slug>` schema by the
//! [`WsPools`] connection's `search_path`, so tenants never see each other's documents.

use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::{json, Value};

use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_documents::{AttachDoc, DocumentsModule, ListDocs, RemoveDoc, SearchDocs, UpdateDoc};
use gt_mcp_server::{DocumentsResource, DomainCtx, DomainHandler};
use gt_module::{GtModule, McpRegistry, McpTool};
use gt_store_blob::{sha256_hex, BlobStore};
use gt_store_dolt::AppError;
use gt_store_pg::{
    DocError, Document, DocumentPatch, DocumentsRepository, NewDocument, PgDocuments, VectorStore,
};

use super::pools::WsPools;

/// PG + blob backed handler for the `documents.*` tool namespace.
pub struct DocumentsHandler {
    pools: Arc<WsPools>,
    /// `None` when no object store is configured — `kind="blob"` attach then errors, but
    /// `.md` documents (the differentiator) still work.
    blob: Option<Arc<BlobStore>>,
    /// The bucket name recorded on a blob row's pointer (the operator the `BlobStore` wraps
    /// already targets it; the row stores it for resolution).
    bucket: String,
    extractor: Extractor,
    /// Optional semantic-search engine (hq-docs-search.2). `Some` ⇒ documents are embedded on
    /// attach and `documents.search` uses the hybrid (text + vector) path; `None` ⇒ phase-1
    /// full-text only.
    embedder: Option<Arc<dyn Embedder>>,
}

impl DocumentsHandler {
    /// Wire the per-workspace pool cache, the optional blob store + its bucket, the extraction
    /// pipeline, and the optional embedding engine.
    pub fn new(
        pools: Arc<WsPools>,
        blob: Option<Arc<BlobStore>>,
        bucket: impl Into<String>,
        extractor: Extractor,
        embedder: Option<Arc<dyn Embedder>>,
    ) -> Self {
        Self {
            pools,
            blob,
            bucket: bucket.into(),
            extractor,
            embedder,
        }
    }

    async fn repo(&self, ws: Option<&str>) -> Result<PgDocuments, AppError> {
        Ok(PgDocuments::new(self.pools.get(ws).await?))
    }
}

#[async_trait]
impl DomainHandler for DocumentsHandler {
    fn namespace(&self) -> &'static str {
        "documents"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        // Single source of truth: the `documents.*` tool contract is the GtModule's
        // (hq-docs-api.1). Harvesting it here keeps the advertised tools (the 10
        // validate/execute pairs) exactly aligned with what `dispatch` below matches —
        // no drift between the schema clients see and the verbs the handler runs.
        let mut reg = McpRegistry::new();
        // `DocumentsModule::default()` (not the bare value): under the `axum` feature the
        // composition crate now enables (hq-fe-api-mount.1), the module carries an optional
        // HTTP-state field, so it is no longer a unit struct. The default-constructed module
        // (no HTTP state) registers the same `documents.*` tool contract.
        DocumentsModule::default().register_mcp_tools(&mut reg);
        reg.tools().to_vec()
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "documents.attach.validate" => {
                parse::<AttachDoc>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "documents.attach.execute" => {
                let cmd = parse::<AttachDoc>(ctx.args)?;
                cmd.validate().map_err(val)?;
                self.attach(ctx.workspace, cmd).await
            }
            "documents.update.validate" => {
                parse::<UpdateDoc>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "documents.update.execute" => {
                let cmd = parse::<UpdateDoc>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let repo = self.repo(ctx.workspace).await?;
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
                self.embed_doc(&repo, &doc).await;
                Ok(doc_json(&doc))
            }
            "documents.remove.validate" => {
                parse::<RemoveDoc>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "documents.remove.execute" => {
                let cmd = parse::<RemoveDoc>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let repo = self.repo(ctx.workspace).await?;
                repo.soft_delete(&cmd.id, cmd.expected_version)
                    .await
                    .map_err(doc_err)?;
                Ok(json!({ "ok": true, "id": cmd.id, "removed": true }))
            }
            "documents.list.validate" => {
                parse::<ListDocs>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "documents.list.execute" => {
                let cmd = parse::<ListDocs>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let repo = self.repo(ctx.workspace).await?;
                // Flat, paged browse (hq-web-extras.11): owner is optional (back-compat when both
                // owner_type+owner_id are set), plus content_type filter + offset/limit paging.
                let owner = match (&cmd.owner_type, &cmd.owner_id) {
                    (Some(t), Some(i)) => Some((t.as_str(), i.as_str())),
                    _ => None,
                };
                let mut docs = repo
                    .browse(owner, cmd.content_type.as_deref(), cmd.offset, cmd.limit)
                    .await
                    .map_err(doc_err)?;
                let has_more = docs.len() as i64 > cmd.limit;
                if has_more {
                    docs.truncate(cmd.limit as usize);
                }
                let next_offset = has_more.then_some(cmd.offset + cmd.limit);
                Ok(json!({
                    "documents": docs.iter().map(doc_json).collect::<Vec<_>>(),
                    "offset": cmd.offset,
                    "limit": cmd.limit,
                    "has_more": has_more,
                    "next_offset": next_offset,
                }))
            }
            "documents.search.validate" => {
                parse::<SearchDocs>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "documents.search.execute" => {
                let cmd = parse::<SearchDocs>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let repo = self.repo(ctx.workspace).await?;
                let owner = match (&cmd.owner_type, &cmd.owner_id) {
                    (Some(t), Some(i)) => Some((t.as_str(), i.as_str())),
                    _ => None,
                };
                // Hybrid (text + vector) when an embedder is wired (hq-docs-search.2); else
                // phase-1 full-text. An embed failure degrades to full-text rather than erroring.
                let docs = match &self.embedder {
                    Some(emb) => match emb.embed(&cmd.query).await {
                        Ok(vec) => repo
                            .search_hybrid(&cmd.query, &vec, owner, cmd.limit)
                            .await
                            .map_err(doc_err)?,
                        Err(_) => repo
                            .search(&cmd.query, owner, cmd.limit)
                            .await
                            .map_err(doc_err)?,
                    },
                    None => repo
                        .search(&cmd.query, owner, cmd.limit)
                        .await
                        .map_err(doc_err)?,
                };
                Ok(json!({ "documents": docs.iter().map(doc_json).collect::<Vec<_>>() }))
            }
            other => Err(AppError::Validation(format!(
                "unknown documents tool `{other}`"
            ))),
        }
    }
}

impl DocumentsHandler {
    /// The `documents.attach.execute` body: persist a `.md` body directly, or content-address
    /// + upload a binary and extract its text.
    async fn attach(&self, ws: Option<&str>, cmd: AttachDoc) -> Result<Value, AppError> {
        let repo = self.repo(ws).await?;
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
                .map_err(|e| {
                    AppError::Validation(format!("data_base64 is not valid base64: {e}"))
                })?;
            let content_type = cmd.content_type.clone().unwrap_or_default();
            let blob = self.blob.as_ref().ok_or_else(|| {
                AppError::Other("no object store configured for blob attachments".into())
            })?;
            let sha = sha256_hex(&bytes);
            let ws_slug = ws.unwrap_or("default");
            let key =
                BlobStore::key_for(ws_slug, &cmd.owner_type, &cmd.owner_id, &sha, &cmd.filename);

            // Dedup: only upload if this exact object is absent.
            if !blob.exists(&key).await.map_err(blob_err)? {
                blob.put(&key, bytes.clone(), Some(&content_type))
                    .await
                    .map_err(blob_err)?;
            }
            // Extraction is best-effort: an unsupported/garbled binary still attaches (the
            // human can read the original via a presigned URL); only the text index misses it.
            let extracted = self.extractor.extract(&content_type, &bytes).await.ok();

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
                bucket: Some(self.bucket.clone()),
                key: Some(key),
                extracted_text: extracted,
                uploaded_by: cmd.created_by,
            }
        };

        let doc = repo.create(new).await.map_err(doc_err)?;
        self.embed_doc(&repo, &doc).await;
        Ok(doc_json(&doc))
    }

    /// Best-effort: embed a document's text and store the vector (hq-docs-search.2). A missing
    /// embedder, empty text, or an embed/store failure is silently skipped — semantic search
    /// then just misses this row; attach/update never fail on it. Generic over [`VectorStore`]
    /// (hq-vectorstore-abstraction) rather than the concrete [`PgDocuments`], so a future
    /// non-Postgres vector backend is a swap at the call site, not here.
    async fn embed_doc(&self, repo: &impl VectorStore, doc: &Document) {
        let Some(emb) = &self.embedder else { return };
        let text = doc
            .body_md
            .as_deref()
            .or(doc.extracted_text.as_deref())
            .unwrap_or("");
        if text.trim().is_empty() {
            return;
        }
        if let Ok(vec) = emb.embed(text).await {
            let _ = repo.set_embedding(&doc.id, vec).await;
        }
    }
}

/// Per-workspace document reader for the `gt://doc/{id}` + `gt://issue/{id}` resource reads
/// (hq-docs-api.3). Implements the orchestration-tier [`DocumentsResource`] seam over the
/// same per-workspace pool cache the dispatch handler uses.
pub struct PgDocumentsResource {
    pools: Arc<WsPools>,
}

impl PgDocumentsResource {
    /// Wire the per-workspace pool cache.
    pub fn new(pools: Arc<WsPools>) -> Self {
        Self { pools }
    }
}

#[async_trait]
impl DocumentsResource for PgDocumentsResource {
    async fn get(&self, workspace: Option<&str>, id: &str) -> Result<Option<Value>, AppError> {
        let repo = PgDocuments::new(self.pools.get(workspace).await?);
        Ok(repo.get(id).await.map_err(doc_err)?.as_ref().map(doc_json))
    }

    async fn list_for_owner(
        &self,
        workspace: Option<&str>,
        owner_id: &str,
    ) -> Result<Vec<Value>, AppError> {
        let repo = PgDocuments::new(self.pools.get(workspace).await?);
        let docs = repo.list_by_owner_id(owner_id).await.map_err(doc_err)?;
        Ok(docs.iter().map(doc_json).collect())
    }
}

/// Parse tool args into `T`, mapping a shape mismatch onto a validation error.
fn parse<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Map a `gt-documents` shape-validation error onto the MCP error space.
fn val(e: gt_documents::ValidationError) -> AppError {
    AppError::Validation(e.0)
}

/// Map a [`DocError`] onto the MCP error space: client-fixable conditions (missing id, stale
/// version) as `Validation`, backend failures as `Other`.
fn doc_err(e: DocError) -> AppError {
    match e {
        DocError::NotFound(_) | DocError::VersionConflict { .. } => {
            AppError::Validation(e.to_string())
        }
        DocError::Db(_) => AppError::Other(e.to_string()),
    }
}

/// Map a blob-store error onto the MCP error space.
fn blob_err(e: gt_store_blob::BlobError) -> AppError {
    AppError::Other(e.to_string())
}

/// Render a [`Document`] as the JSON payload returned to the client. `body_md`/`extracted_text`
/// are included so a model reading a list gets the content inline.
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
