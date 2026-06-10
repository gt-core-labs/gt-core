//! Archive interceptor e2e (hq-docs-archive-sync, Phase B).
//!
//! Drives the real cross-store path the production archive sweep takes: an epic is closed and
//! backdated in Dolt, a `documents` row is attached to it in per-workspace Postgres, the sweep
//! ([`DoltIssues::archive_old_closed`]) stamps `archived_at` and reports the archived epic, and
//! [`purge_archived_epic_documents`] soft-deletes that epic's docs so it stops surfacing in
//! `documents.search`. No-op unless BOTH `GT_DOLT_URL` and `GT_PG_URL` are set (the stores are up).

use std::sync::Arc;

use gt_composition::mcp::WsPools;
use gt_composition::system::purge_archived_epic_documents;
use gt_store_dolt::{DoltIssues, NewIssue};
use gt_store_pg::{DocumentsRepository, NewDocument, PgDocuments};
use mysql_async::prelude::*;

const DOLT_TEST_DB: &str = "gt_rs_archive_docs_test";

/// Provision the per-workspace docs tables in `ws_default` (serialized under an advisory lock,
/// mirroring documents_e2e — concurrent migration runs race on the schema otherwise).
async fn provision_pg(pg: &str) {
    let admin = sqlx::PgPool::connect(pg).await.expect("connect admin pool");
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
}

#[tokio::test]
async fn archived_epic_docs_are_soft_deleted_by_the_sweep() {
    let (Ok(dolt_base), Ok(pg)) = (std::env::var("GT_DOLT_URL"), std::env::var("GT_PG_URL")) else {
        eprintln!("GT_DOLT_URL / GT_PG_URL unset; skipping archive-docs-sync e2e (stores not up)");
        return;
    };
    let dolt_base = dolt_base.trim_end_matches('/').to_string();

    // --- Dolt: an epic, closed and backdated past the cutoff. ---
    let admin_pool = gt_store_dolt::connect(&dolt_base).expect("dolt admin pool");
    let mut admin = admin_pool.get_conn().await.expect("dolt admin conn");
    admin
        .query_drop(format!("CREATE DATABASE IF NOT EXISTS {DOLT_TEST_DB}"))
        .await
        .expect("create test db");

    let db_url = format!("{dolt_base}/{DOLT_TEST_DB}");
    let repo = DoltIssues::connect(&db_url).expect("connect issues");
    repo.ensure_schema().await.expect("ensure schema");

    let epic_id = format!("hq-arcdoc-{}", ulid::Ulid::new());
    repo.insert(&NewIssue {
        id: epic_id.clone(),
        title: "archive-docs subject".into(),
        issue_type: "epic".into(),
        priority: 2,
        created_by: "test".into(),
        ..Default::default()
    })
    .await
    .expect("insert epic");
    repo.close(&epic_id, "claude-host")
        .await
        .expect("close epic");

    let pool = gt_store_dolt::connect(&db_url).expect("dolt pool");
    let mut conn = pool.get_conn().await.expect("dolt conn");
    conn.exec_drop(
        "UPDATE issues SET closed_at = DATE_SUB(NOW(), INTERVAL 10 DAY) WHERE id = :id",
        mysql_async::params! { "id" => &epic_id },
    )
    .await
    .expect("backdate closed_at");

    // --- Postgres: a documents row attached to that epic. ---
    provision_pg(&pg).await;
    let pools = Arc::new(WsPools::new(pg));
    let docs = PgDocuments::new(pools.get(None).await.expect("default ws pool"));
    let doc = docs
        .create(NewDocument {
            id: format!("doc-{}", ulid::Ulid::new()),
            owner_type: "epic".into(),
            owner_id: epic_id.clone(),
            kind: "md".into(),
            filename: "epic.md".into(),
            content_type: None,
            size: None,
            sha256: None,
            body_md: Some("# archive-docs subject\n\nshould vanish on archive".into()),
            bucket: None,
            key: None,
            extracted_text: None,
            uploaded_by: "test".into(),
        })
        .await
        .expect("attach epic doc");
    assert_eq!(
        docs.list_by_owner_id(&epic_id)
            .await
            .expect("pre-list")
            .len(),
        1,
        "the epic has one live doc before the sweep"
    );

    // --- The sweep: archive in Dolt, then purge the archived epic's docs in PG. ---
    let archived = repo.archive_old_closed(1).await.expect("archive sweep");
    assert!(
        archived
            .iter()
            .any(|a| a.id == epic_id && a.issue_type == "epic"),
        "the sweep reports the archived epic"
    );
    purge_archived_epic_documents(&pools, &archived).await;

    // --- The epic's docs are gone (soft-deleted): the index no longer surfaces them. ---
    assert!(
        docs.list_by_owner_id(&epic_id)
            .await
            .expect("post-list")
            .is_empty(),
        "the archived epic's documents are soft-deleted after the sweep"
    );
    // The row still exists, just marked deleted (soft delete bumps the version).
    let after = docs.get(&doc.id).await.expect("get doc");
    assert!(
        after.map(|d| d.deleted_at.is_some()).unwrap_or(false),
        "the doc is soft-deleted, not hard-deleted"
    );

    // Cleanup the Dolt scratch DB (PG rows are soft-deleted + owner-id-scoped, so harmless).
    conn.query_drop(format!("DROP DATABASE IF EXISTS {DOLT_TEST_DB}"))
        .await
        .expect("cleanup dolt db");
}
