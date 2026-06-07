//! Full-stack documents e2e (epic `hq-docs-test`, docs/12 — `hq-docs-test-e2e.*`).
//!
//! Exercises the document subsystem against the **real** engines the compose stack deploys:
//! per-workspace Postgres (`GT_PG_URL`) + an S3/MinIO object store (`GT_BLOB_*`). It drives the
//! production [`DocumentsHandler`] + [`PgDocumentsResource`] — the same wiring the
//! `gt-mcp-server` binary builds — so the assertions hold over the live stores, not an fs
//! stand-in. No-op unless BOTH `GT_PG_URL` and `GT_BLOB_ENDPOINT` are set (the stack is up):
//! run it against the compose deploy or a local `docker run` of pgvector + MinIO.
//!
//! Beads:
//! - `hq-docs-test-e2e.1` — attach a `.md` to an epic → `gt://issue/{id}` inline returns it and
//!   `documents.search` finds it.
//! - `hq-docs-test-e2e.2` — attach a PDF → the object lands in the bucket (fetchable bytes +
//!   a presigned URL), and its `extracted_text` is searchable.
//! - `hq-docs-test-e2e.3` — the model-context loop: one owner read surfaces both attachments'
//!   text inline (the headline assertion — a model resolving the bead reads its docs in one call).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use gt_composition::mcp::{DocumentsHandler, PgDocumentsResource, WsPools};
use gt_docs_extract::Extractor;
use gt_mcp_server::{DocumentsResource, DomainCtx, DomainHandler};
use gt_store_blob::BlobStore;
use serde_json::{json, Value};

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A single-page PDF whose text stream shows `text` (lopdf — same lib the reader uses).
fn pdf_with_text(text: &str) -> Vec<u8> {
    use lopdf::content::{Content, Operation};
    use lopdf::{dictionary, Document, Object, Stream};

    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font", "Subtype" => "Type1", "BaseFont" => "Helvetica",
    });
    let resources_id = doc.add_object(dictionary! { "Font" => dictionary! { "F1" => font_id } });
    let content = Content {
        operations: vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 24.into()]),
            Operation::new("Td", vec![72.into(), 700.into()]),
            Operation::new("Tj", vec![Object::string_literal(text)]),
            Operation::new("ET", vec![]),
        ],
    };
    let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
    let page_id = doc.add_object(dictionary! {
        "Type" => "Page", "Parent" => pages_id, "Contents" => content_id,
        "Resources" => resources_id, "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages", "Kids" => vec![page_id.into()], "Count" => 1,
        }),
    );
    let catalog_id = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
    doc.trailer.set("Root", catalog_id);
    let mut buf = Vec::new();
    doc.save_to(&mut buf).expect("serialize pdf");
    buf
}

/// The wired handler + reader + blob handle over the live PG + S3 stack, or `None` when the
/// stack env is absent (skip). The bucket must already exist (the compose stack / MinIO init
/// provisions it, mirroring the deploy).
async fn stack() -> Option<(
    DocumentsHandler,
    PgDocumentsResource,
    Arc<BlobStore>,
    String,
)> {
    let (Ok(pg), Ok(endpoint)) = (
        std::env::var("GT_PG_URL"),
        std::env::var("GT_BLOB_ENDPOINT"),
    ) else {
        eprintln!("GT_PG_URL / GT_BLOB_ENDPOINT unset; skipping documents e2e (stack not up)");
        return None;
    };
    let bucket = std::env::var("GT_BLOB_BUCKET").unwrap_or_else(|_| "gt-documents".into());
    let region = std::env::var("GT_BLOB_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("GT_BLOB_ACCESS_KEY").unwrap_or_default();
    let secret = std::env::var("GT_BLOB_SECRET_KEY").unwrap_or_default();

    // Provision the docs template tables (serialized — see documents_dispatch for the why).
    let admin = sqlx::PgPool::connect(&pg)
        .await
        .expect("connect admin pool");
    let mut conn = admin.acquire().await.expect("acquire admin conn");
    sqlx::query("SELECT pg_advisory_lock(4915623003)")
        .execute(&mut *conn)
        .await
        .unwrap();
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
    sqlx::query("SELECT pg_advisory_unlock(4915623003)")
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);

    let blob = Arc::new(
        BlobStore::from_s3(&endpoint, &bucket, &region, &access, &secret).expect("S3 blob store"),
    );
    let pools = Arc::new(WsPools::new(pg));
    let handler = DocumentsHandler::new(
        pools.clone(),
        Some(blob.clone()),
        bucket.clone(),
        Extractor::without_ocr(),
        None,
    );
    let resource = PgDocumentsResource::new(pools);
    Some((handler, resource, blob, bucket))
}

async fn call(h: &DocumentsHandler, tool: &str, args: Value) -> Value {
    h.dispatch(
        tool,
        DomainCtx {
            workspace: None,
            actor: "e2e",
            args,
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{tool}: {e}"))
}

#[tokio::test]
async fn documents_full_stack_attach_inline_search_and_blob_roundtrip() {
    let Some((h, res, blob, _bucket)) = stack().await else {
        return;
    };
    let n = nonce();
    let epic = format!("epic-{n}");

    // --- e2e.1: a .md attached to an epic is inlined by the owner read + found by search.
    let md = call(
        &h,
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": epic, "kind": "md",
            "filename": "plan.md", "body_md": "the gastown launch runbook", "created_by": "e2e"
        }),
    )
    .await;
    let md_id = md["id"].as_str().unwrap().to_string();

    let inline = res.list_for_owner(None, &epic).await.expect("owner inline");
    assert!(
        inline.iter().any(|d| d["id"] == md["id"]),
        "e2e.1: gt://issue inline returns the .md"
    );

    let hits = call(
        &h,
        "documents.search.execute",
        json!({
            "query": "runbook", "owner_type": "epic", "owner_id": epic, "limit": 10
        }),
    )
    .await;
    assert!(
        hits["documents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["id"] == md["id"]),
        "e2e.1: documents.search finds the attached .md"
    );

    // --- e2e.2: a PDF lands in the bucket (bytes fetchable + presigned URL) and is searchable.
    let pdf = pdf_with_text("DEPLOYMENT TOPOLOGY DIAGRAM");
    let blob_doc = call(
        &h,
        "documents.attach.execute",
        json!({
            "owner_type": "epic", "owner_id": epic, "kind": "blob",
            "filename": "topology.pdf", "content_type": "application/pdf",
            "data_base64": B64.encode(&pdf), "created_by": "e2e"
        }),
    )
    .await;
    let key = blob_doc["key"].as_str().expect("blob key").to_string();

    // The object really landed in the object store: bytes roundtrip, and a presigned GET mints.
    assert_eq!(
        blob.get(&key).await.expect("object present in bucket"),
        pdf,
        "stored bytes match"
    );
    let url = blob
        .presign_read(&key, Duration::from_secs(120))
        .await
        .expect("S3 backend mints a presigned URL");
    assert!(url.starts_with("http"), "presigned URL is http(s): {url}");

    // Its extracted text is searchable (the PDF text reached the index).
    let pdf_hits = call(
        &h,
        "documents.search.execute",
        json!({
            "query": "topology", "owner_type": "epic", "owner_id": epic, "limit": 10
        }),
    )
    .await;
    assert!(
        pdf_hits["documents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["id"] == blob_doc["id"]),
        "e2e.2: the PDF's extracted_text is searchable"
    );

    // --- e2e.3: one owner read surfaces BOTH attachments' text inline (the model-context loop).
    let all = res.list_for_owner(None, &epic).await.expect("owner inline");
    let body_md_seen = all
        .iter()
        .any(|d| d["id"] == md["id"] && d["body_md"].as_str().unwrap_or("").contains("runbook"));
    let extracted_seen = all.iter().any(|d| {
        d["id"] == blob_doc["id"]
            && d["extracted_text"]
                .as_str()
                .unwrap_or("")
                .to_uppercase()
                .contains("TOPOLOGY")
    });
    assert!(
        body_md_seen,
        "e2e.3: the .md body is inline in the owner read"
    );
    assert!(
        extracted_seen,
        "e2e.3: the PDF extracted text is inline in the owner read"
    );
    assert!(
        md_id != blob_doc["id"].as_str().unwrap(),
        "two distinct attachments surfaced in one read"
    );
}
