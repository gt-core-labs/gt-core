//! `documents.*` dispatch + resource-read integration (epic `hq-docs-test`, docs/12).
//!
//! Drives the production [`DocumentsHandler`] (PG repo + fs blob store + extractor) and the
//! [`PgDocumentsResource`] resource reader over a real per-workspace Postgres, mirroring the
//! `mcp_dispatch_namespaces` gating: no-op without `GT_PG_URL`, the full proof under the CI
//! `contract` job's pgvector service.
//!
//! Bead coverage:
//! - `hq-docs-test-api.1` — every `documents.*` tool: `validate` accepts/rejects shapes,
//!   `execute` performs the write (attach → list → search → update → remove).
//! - `hq-docs-test-api.2` — `attach` kind=md persists `body_md`; kind=blob decodes base64 →
//!   blob store object + populated `extracted_text`.
//! - `hq-docs-test-api.3` — `gt://doc/{id}` resolves one doc, `gt://issue/{id}` inlines the
//!   owner's `documents[]`, an absent id is `None`.
//! - `hq-docs-test-mt.1` — a doc attached in workspace A is invisible to list/resource reads
//!   in workspace B (schema isolation), and its blob key is prefixed by the tenant slug.

use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use gt_composition::mcp::{DocumentsHandler, PgDocumentsResource, WsPools};
use gt_docs_extract::Extractor;
use gt_mcp_server::{DocumentsResource, DomainCtx, DomainHandler};
use gt_store_blob::BlobStore;
use serde_json::{json, Value};
use tempfile::TempDir;

const DOCX_MIME: &str = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A minimal but real .docx: a zip whose `word/document.xml` carries the given text run.
/// The extractor pulls visible text from `word/`-prefixed XML parts, so this attaches as a
/// blob whose `extracted_text` is non-empty.
fn docx_with_text(text: &str) -> Vec<u8> {
    use zip::write::SimpleFileOptions;
    let mut cursor = std::io::Cursor::new(Vec::new());
    {
        let mut zw = zip::ZipWriter::new(&mut cursor);
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        zw.start_file("[Content_Types].xml", opts).unwrap();
        zw.write_all(br#"<?xml version="1.0"?><Types/>"#).unwrap();
        zw.start_file("word/document.xml", opts).unwrap();
        let body = format!(
            r#"<?xml version="1.0"?><w:document><w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>"#
        );
        zw.write_all(body.as_bytes()).unwrap();
        zw.finish().unwrap();
    }
    cursor.into_inner()
}

/// The wired handler + resource reader over a real PG, or `None` when `GT_PG_URL` is unset.
/// The returned `TempDir` roots the fs blob store and must be kept alive for the test.
async fn harness(
    test: &str,
) -> Option<(
    DocumentsHandler,
    PgDocumentsResource,
    Arc<WsPools>,
    sqlx::PgPool,
    TempDir,
)> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let admin = sqlx::PgPool::connect(&url)
        .await
        .expect("connect admin pool");
    // Catalog schema + the per-workspace provisioning fn, then the docs template tables.
    // `CREATE ... IF NOT EXISTS` is not atomic under concurrent DDL, so a session advisory
    // lock serializes the apply across the parallel tests (the test bodies use a nonce).
    let mut conn = admin.acquire().await.expect("acquire admin conn");
    sqlx::query("SELECT pg_advisory_lock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("take migration lock");
    for m in gt_store_pg::workspace_migrations() {
        sqlx::raw_sql(&m.sql)
            .execute(&mut *conn)
            .await
            .expect("apply workspace migration");
    }
    for m in gt_store_pg::docs_migrations() {
        sqlx::raw_sql(&m.sql)
            .execute(&mut *conn)
            .await
            .expect("apply docs migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("release migration lock");
    drop(conn);

    let pools = Arc::new(WsPools::new(url));
    let dir = TempDir::new().expect("tempdir");
    let blob = Arc::new(BlobStore::from_fs(dir.path().to_str().unwrap()).expect("fs blob store"));
    let handler = DocumentsHandler::new(
        pools.clone(),
        Some(blob),
        "test-bucket",
        Extractor::without_ocr(),
        None,
    );
    let resource = PgDocumentsResource::new(pools.clone());
    Some((handler, resource, pools, admin, dir))
}

/// Dispatch a `documents.*` tool in `ws`, returning the handler's JSON result.
async fn call(
    h: &DocumentsHandler,
    ws: Option<&str>,
    tool: &str,
    args: Value,
) -> Result<Value, gt_store_dolt::AppError> {
    h.dispatch(
        tool,
        DomainCtx {
            workspace: ws,
            actor: "doc-test",
            args,
        },
    )
    .await
}

/// hq-docs-test-api.1 + api.2(md) — the full md lifecycle through validate/execute, and that
/// each verb's `validate` accepts a good shape and rejects a bad one.
#[tokio::test]
async fn documents_tool_lifecycle_validate_and_execute() {
    let Some((h, _res, _pools, _admin, _dir)) =
        harness("documents_tool_lifecycle_validate_and_execute").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");

    // validate accepts a well-formed attach, rejects a malformed one (md without body).
    call(
        &h,
        None,
        "documents.attach.validate",
        json!({
            "owner_type": "epic", "owner_id": owner, "kind": "md",
            "filename": "spec.md", "body_md": "# hello", "created_by": "tester"
        }),
    )
    .await
    .expect("valid attach passes validate");
    assert!(
        call(
            &h,
            None,
            "documents.attach.validate",
            json!({
                "owner_type": "epic", "owner_id": owner, "kind": "md",
                "filename": "spec.md", "created_by": "tester"
            })
        )
        .await
        .is_err(),
        "md attach without body_md is rejected by validate"
    );

    // execute performs the write; the returned doc carries an id + body_md (api.2 md).
    let created = call(
        &h,
        None,
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": owner, "kind": "md",
            "filename": "spec.md", "body_md": "the gastown rollout plan", "created_by": "tester"
        }),
    )
    .await
    .expect("attach executes");
    let id = created["id"].as_str().expect("doc id").to_string();
    assert_eq!(
        created["body_md"], "the gastown rollout plan",
        "md body persisted on the row"
    );
    assert_eq!(created["kind"], "md");

    // list.execute returns the freshly-written doc (the write is durable).
    let listed = call(
        &h,
        None,
        "documents.list.execute",
        json!({
            "owner_type": "epic", "owner_id": owner
        }),
    )
    .await
    .expect("list executes");
    assert!(
        listed["documents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["id"] == created["id"]),
        "attached doc appears in the owner listing"
    );

    // search.execute finds it by a body term; validate rejects unpaired owner fields.
    let hits = call(
        &h,
        None,
        "documents.search.execute",
        json!({
            "query": "gastown", "owner_type": "epic", "owner_id": owner, "limit": 10
        }),
    )
    .await
    .expect("search executes");
    assert!(
        hits["documents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["id"] == created["id"]),
        "full-text search finds the doc by a body term"
    );
    assert!(
        call(
            &h,
            None,
            "documents.search.validate",
            json!({ "query": "x", "owner_type": "epic" })
        )
        .await
        .is_err(),
        "search validate rejects owner_type without owner_id"
    );

    // update.execute bumps version + changes the body; validate rejects a no-op update.
    assert!(
        call(
            &h,
            None,
            "documents.update.validate",
            json!({
                "id": id, "expected_version": 0, "edited_by": "e"
            })
        )
        .await
        .is_err(),
        "update with no fields is a no-op and rejected"
    );
    let updated = call(
        &h,
        None,
        "documents.update.execute",
        json!({
            "id": id, "expected_version": 0, "body_md": "revised plan", "edited_by": "editor"
        }),
    )
    .await
    .expect("update executes");
    assert_eq!(updated["version"], 1, "version bumped");
    assert_eq!(updated["body_md"], "revised plan");

    // remove.execute soft-deletes (version is now 1); it then drops from the listing.
    call(
        &h,
        None,
        "documents.remove.execute",
        json!({ "id": id, "expected_version": 1 }),
    )
    .await
    .expect("remove executes");
    let after = call(
        &h,
        None,
        "documents.list.execute",
        json!({
            "owner_type": "epic", "owner_id": owner
        }),
    )
    .await
    .unwrap();
    assert!(
        !after["documents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["id"] == id),
        "soft-deleted doc gone from the listing"
    );
}

/// hq-docs-test-api.2 (blob) — kind=blob decodes base64 → the object lands in the blob store
/// and the extractor populates `extracted_text`.
#[tokio::test]
async fn attach_blob_uploads_and_extracts_text() {
    let Some((h, _res, _pools, _admin, dir)) =
        harness("attach_blob_uploads_and_extracts_text").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");
    let docx = docx_with_text("quarterly objectives and key results");

    let created = call(
        &h,
        None,
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": owner, "kind": "blob",
            "filename": "okrs.docx", "content_type": DOCX_MIME,
            "data_base64": B64.encode(&docx), "created_by": "tester"
        }),
    )
    .await
    .expect("blob attach executes");

    assert_eq!(created["kind"], "blob");
    assert_eq!(
        created["bucket"], "test-bucket",
        "blob row records the configured bucket"
    );
    let key = created["key"].as_str().expect("blob key").to_string();
    assert!(
        key.contains("/epic/"),
        "key is per-owner content-addressed: {key}"
    );
    assert!(
        created["extracted_text"]
            .as_str()
            .unwrap_or("")
            .contains("objectives"),
        "the extractor pulled text out of the docx: {:?}",
        created["extracted_text"]
    );

    // The bytes really landed in the (fs) blob store at the recorded key.
    let on_disk = dir.path().join(&key);
    assert!(
        on_disk.exists(),
        "blob object written to the store at {key}"
    );
}

/// hq-docs-test-api.3 — resource reads: `gt://doc/{id}` resolves one doc, the owner inline
/// returns its `documents[]`, an absent id is `None`.
#[tokio::test]
async fn resource_reads_resolve_doc_and_owner_inline() {
    let Some((h, res, _pools, _admin, _dir)) =
        harness("resource_reads_resolve_doc_and_owner_inline").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");
    let created = call(
        &h,
        None,
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": owner, "kind": "md",
            "filename": "design.md", "body_md": "inline me", "created_by": "tester"
        }),
    )
    .await
    .expect("attach");
    let id = created["id"].as_str().unwrap().to_string();

    // gt://doc/{id} — one document by id.
    let one = res
        .get(None, &id)
        .await
        .expect("resource get")
        .expect("doc present");
    assert_eq!(one["id"], created["id"]);
    assert_eq!(one["body_md"], "inline me");

    // gt://issue/{id} inline — the owner's live documents.
    let inline = res
        .list_for_owner(None, &owner)
        .await
        .expect("list_for_owner");
    assert!(
        inline.iter().any(|d| d["id"] == created["id"]),
        "owner inline carries the doc"
    );

    // An absent id resolves to None (not-found).
    assert!(
        res.get(None, &format!("missing-{n}"))
            .await
            .expect("resource get")
            .is_none(),
        "absent doc id resolves to None"
    );
}

/// hq-docs-test-mt.1 — a doc attached in workspace A is invisible to reads in workspace B,
/// and its blob key is namespaced under the tenant slug (schema + key-prefix isolation).
#[tokio::test]
async fn documents_are_isolated_per_workspace() {
    let Some((h, res, _pools, admin, _dir)) = harness("documents_are_isolated_per_workspace").await
    else {
        return;
    };
    let n = nonce();
    let (ws_a, ws_b) = (format!("a{n}"), format!("b{n}"));
    // Provision both tenant schemas by cloning the ws_default template.
    for ws in [&ws_a, &ws_b] {
        sqlx::query("SELECT gt_create_workspace_schema($1)")
            .bind(ws)
            .execute(&admin)
            .await
            .expect("provision workspace schema");
    }

    let owner = format!("epic-{n}");
    let created = call(
        &h,
        Some(&ws_a),
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": owner, "kind": "blob",
            "filename": "secret.docx", "content_type": DOCX_MIME,
            "data_base64": B64.encode(docx_with_text("tenant a private")), "created_by": "tester"
        }),
    )
    .await
    .expect("attach in workspace A");

    // The blob key is prefixed by workspace A's slug.
    let key = created["key"].as_str().unwrap();
    assert!(
        key.starts_with(&format!("{ws_a}/")),
        "blob key namespaced under the tenant: {key}"
    );

    // Workspace A sees it; workspace B does not (separate schema).
    let in_a = call(
        &h,
        Some(&ws_a),
        "documents.list.execute",
        json!({
            "owner_type": "epic", "owner_id": owner
        }),
    )
    .await
    .unwrap();
    assert!(in_a["documents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["id"] == created["id"]));

    let in_b = call(
        &h,
        Some(&ws_b),
        "documents.list.execute",
        json!({
            "owner_type": "epic", "owner_id": owner
        }),
    )
    .await
    .unwrap();
    assert!(
        in_b["documents"].as_array().unwrap().is_empty(),
        "workspace B cannot see workspace A's documents"
    );

    // Resource reads honour the same isolation.
    let id = created["id"].as_str().unwrap();
    assert!(
        res.get(Some(&ws_a), id).await.unwrap().is_some(),
        "doc visible in its own tenant"
    );
    assert!(
        res.get(Some(&ws_b), id).await.unwrap().is_none(),
        "doc invisible in another tenant"
    );
}
