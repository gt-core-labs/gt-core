//! `report.*` domain dispatch — the operator-report export engine
//! (hq-fc7d6a, epic hq-56b5ee).
//!
//! `report.generate` projects one (rig, workspace) board into the tracker
//! mockup (per-module sections, Modulo/Tarea/…/TOTAL HORAS — the pure
//! [`gt_issues::report`] projection), serializes it (xlsx primary, csv), and
//! delivers it as a DOCUMENT: csv lands as `md`-class text, xlsx as a blob in
//! the object store. Optionally the report is announced by email THROUGH the
//! outbox (hq-f24599) — never a direct transport call. No new storage: rows in,
//! document out.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;

use gt_issues::report::{build_report, to_csv, to_xlsx};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_blob::{sha256_hex, BlobStore};
use gt_store_dolt::{AppError, DoltIssues, IssueFilter, WorkspacePools};
use gt_store_pg::{
    DocumentsRepository, EmailOutboxRepository, NewDocument, NewEmail, PgDocuments, PgEmailOutbox,
};

use super::pools::WsPools;
use super::util::{descriptor, opt, req, str_arg};

/// PG/Dolt/blob-backed handler for the `report.*` tool namespace.
pub struct ReportHandler {
    /// Per-workspace PG pools (the documents store the report attaches into).
    pools: Arc<WsPools>,
    /// The default-workspace Dolt tracker (row source fallback).
    dolt: Arc<DoltIssues>,
    /// Per-workspace Dolt pools when multi-tenant routing is on.
    dolt_workspaces: Option<Arc<WorkspacePools>>,
    /// Object store for the binary xlsx bytes; `None` ⇒ csv only.
    blob: Option<Arc<BlobStore>>,
    /// The bucket recorded on blob rows.
    bucket: String,
    /// Shared public-schema pool (the email outbox).
    pool: PgPool,
    /// Base public URL for the download link in the announce email.
    public_url: Option<String>,
}

impl ReportHandler {
    /// Wire the stores the generate path composes.
    pub fn new(
        pools: Arc<WsPools>,
        dolt: Arc<DoltIssues>,
        dolt_workspaces: Option<Arc<WorkspacePools>>,
        blob: Option<Arc<BlobStore>>,
        bucket: impl Into<String>,
        pool: PgPool,
        public_url: Option<String>,
    ) -> Self {
        Self {
            pools,
            dolt,
            dolt_workspaces,
            blob,
            bucket: bucket.into(),
            pool,
            public_url,
        }
    }

    async fn tracker(&self, ws: Option<&str>) -> Result<Arc<DoltIssues>, AppError> {
        match (&self.dolt_workspaces, ws) {
            (Some(pools), Some(ws)) => Ok(Arc::new(DoltIssues::new(pools.ensured_pool(ws).await?))),
            _ => Ok(self.dolt.clone()),
        }
    }
}

#[async_trait]
impl DomainHandler for ReportHandler {
    fn namespace(&self) -> &'static str {
        "report"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![descriptor(
            "report.generate",
            "Generate the operator tracker report for one (rig, workspace) board: \
             per-module sections (module = epic, ADR D5) with Modulo/Tarea/Proceso/\
             Nivel/Horas Est./Estado/Responsable/Fecha Inicio/Fecha Fin/Notas and a \
             TOTAL HORAS footer. format=xlsx (default; needs the object store) or csv. \
             The output is attached as a document (owner report:{rig}:{workspace}) for \
             download; pass email_to to announce it through the email outbox. Pure \
             projection — no new card storage.",
            &[
                req("rig", "string"),
                req("workspace", "string"),
                opt("epic", "string"),
                opt("format", "string"),
                opt("email_to", "string"),
            ],
        )]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "report.generate" => {
                let rig = str_arg(&ctx.args, "rig")?.trim().to_string();
                let workspace = str_arg(&ctx.args, "workspace")?.trim().to_string();
                if rig.is_empty() || workspace.is_empty() {
                    return Err(AppError::Validation(
                        "rig and workspace are required (board scope key)".into(),
                    ));
                }
                let format = ctx
                    .args
                    .get("format")
                    .and_then(Value::as_str)
                    .unwrap_or("xlsx");
                if !["xlsx", "csv"].contains(&format) {
                    return Err(AppError::Validation(format!(
                        "unknown format `{format}` (xlsx|csv)"
                    )));
                }
                let epic = ctx.args.get("epic").and_then(Value::as_str).map(str::to_string);

                // The same rows board.list projects (full=true for the Notas column).
                let tracker = self.tracker(ctx.workspace).await?;
                let rows = tracker
                    .list(&IssueFilter {
                        rig: Some(rig.clone()),
                        workspace: Some(workspace.clone()),
                        external_ref: epic,
                        full: true,
                        limit: Some(gt_store_dolt::issues_max_limit()),
                        ..Default::default()
                    })
                    .await?;
                let report = build_report(&rig, &workspace, &rows);

                // Serialize + attach as a document of the board's report owner.
                let docs = PgDocuments::new(self.pools.get(ctx.workspace).await?);
                let stamp = sqlx::types::chrono::Utc::now().format("%Y%m%d-%H%M%S");
                let doc_id = ulid::Ulid::new().to_string();
                let owner_id = format!("{rig}:{workspace}");
                let doc = match format {
                    "csv" => {
                        let csv = to_csv(&report);
                        docs.create(NewDocument {
                            id: doc_id.clone(),
                            owner_type: "report".into(),
                            owner_id,
                            kind: "md".into(),
                            filename: format!("tracker-{rig}-{stamp}.csv"),
                            content_type: Some("text/csv".into()),
                            size: Some(csv.len() as i64),
                            sha256: Some(sha256_hex(csv.as_bytes())),
                            body_md: Some(csv),
                            bucket: None,
                            key: None,
                            extracted_text: None,
                            uploaded_by: ctx.actor.to_string(),
                        })
                        .await
                        .map_err(|e| AppError::Other(format!("attach report: {e}")))?
                    }
                    _ => {
                        let blob = self.blob.as_ref().ok_or_else(|| {
                            AppError::Validation(
                                "xlsx needs the object store (GT_BLOB_*); use format=csv on this deploy"
                                    .into(),
                            )
                        })?;
                        let bytes = to_xlsx(&report)
                            .map_err(|e| AppError::Other(format!("xlsx serialize: {e}")))?;
                        let sha = sha256_hex(&bytes);
                        let filename = format!("tracker-{rig}-{stamp}.xlsx");
                        let key = BlobStore::key_for(
                            ctx.workspace.unwrap_or("default"),
                            "report",
                            &owner_id,
                            &sha,
                            &filename,
                        );
                        if !blob
                            .exists(&key)
                            .await
                            .map_err(|e| AppError::Other(format!("blob: {e}")))?
                        {
                            blob.put(
                                &key,
                                bytes.clone(),
                                Some(
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                ),
                            )
                            .await
                            .map_err(|e| AppError::Other(format!("blob: {e}")))?;
                        }
                        docs.create(NewDocument {
                            id: doc_id.clone(),
                            owner_type: "report".into(),
                            owner_id,
                            kind: "blob".into(),
                            filename,
                            content_type: Some(
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    .into(),
                            ),
                            size: Some(bytes.len() as i64),
                            sha256: Some(sha),
                            body_md: None,
                            bucket: Some(self.bucket.clone()),
                            key: Some(key),
                            extracted_text: None,
                            uploaded_by: ctx.actor.to_string(),
                        })
                        .await
                        .map_err(|e| AppError::Other(format!("attach report: {e}")))?
                    }
                };

                // Optional delivery announce — through the outbox, never a transport.
                let mut email_queued = false;
                if let Some(to) = ctx.args.get("email_to").and_then(Value::as_str) {
                    if !to.contains('@') {
                        return Err(AppError::Validation(format!(
                            "`email_to` must be an email address, got `{to}`"
                        )));
                    }
                    let link = match &self.public_url {
                        Some(base) => format!(
                            "{}/api/v1/documents/{} (documento {})",
                            base.trim_end_matches('/'),
                            doc.id,
                            doc.filename
                        ),
                        None => format!("documento {} ({})", doc.id, doc.filename),
                    };
                    let outbox = PgEmailOutbox::new(self.pool.clone());
                    match outbox
                        .enqueue(NewEmail {
                            id: ulid::Ulid::new().to_string(),
                            workspace: ctx.workspace.unwrap_or("default").to_string(),
                            recipient: to.to_string(),
                            subject: format!("Reporte tracker {rig}/{workspace}"),
                            body: format!(
                                "Reporte generado ({} módulos, TOTAL HORAS {}).\nDescarga: {link}",
                                report.sections.len(),
                                report.total_horas
                            ),
                            template_ref: Some(doc.id.clone()),
                            send_at: None,
                            created_by: ctx.actor.to_string(),
                        })
                        .await
                    {
                        Ok(_) => email_queued = true,
                        Err(e) => eprintln!("[report] outbox enqueue failed: {e}"),
                    }
                }

                Ok(json!({
                    "ok": true,
                    "document_id": doc.id,
                    "filename": doc.filename,
                    "format": format,
                    "sections": report.sections.len(),
                    "rows": report.sections.iter().map(|s| s.rows.len()).sum::<usize>(),
                    "total_horas": report.total_horas,
                    "email_queued": email_queued,
                }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
