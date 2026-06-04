//! Contract test for the `DocumentsRepository` Postgres adapter (hq-docs-store.3, docs/11).
//!
//! Exercises the locked decisions end-to-end against a real Postgres: create + version
//! snapshot, owner listing, `sha256` dedup, optimistic `version` guard (conflict on a
//! stale write), and soft-delete. No-op without `GT_PG_URL` (developer box / CI without
//! Postgres), mirroring the `WorkspacePool` contract test.
//!
//! Requires the `pg` feature (the adapter is gated on it); CI runs
//! `cargo test -p gt-store-pg --features pg`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{docs_migrations, DocError, DocumentPatch, DocumentsRepository, NewDocument, PgDocuments, WorkspacePool};

/// Unique suffix so repeated runs against the same ephemeral DB never collide.
fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

#[tokio::test]
async fn documents_crud_version_dedup_softdelete() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping documents contract test");
        return;
    };

    // Provision the ws_default template tables (the migration creates the schema too).
    let admin = sqlx::PgPool::connect(&url).await.expect("connect admin pool");
    for m in docs_migrations() {
        sqlx::raw_sql(&m.sql).execute(&admin).await.expect("apply docs migration");
    }

    // The repository reads/writes through a tenant-scoped pool (search_path = ws_default).
    let wp = WorkspacePool::connect(&url, "default").await.expect("connect ws pool");
    let repo = PgDocuments::new(wp);

    let n = nonce();
    let id = format!("doc-{n}");
    let owner = format!("epic-{n}");
    let sha = format!("{n:064x}");

    // create → version 0, first snapshot.
    let created = repo
        .create(NewDocument {
            id: id.clone(),
            owner_type: "epic".into(),
            owner_id: owner.clone(),
            kind: "md".into(),
            filename: "spec.md".into(),
            content_type: Some("text/markdown".into()),
            size: None,
            sha256: Some(sha.clone()),
            body_md: Some("# spec v0".into()),
            bucket: None,
            key: None,
            extracted_text: None,
            uploaded_by: "tester".into(),
        })
        .await
        .expect("create");
    assert_eq!(created.version, 0);
    assert_eq!(created.body_md.as_deref(), Some("# spec v0"));
    assert!(created.deleted_at.is_none());

    // get + list_by_owner see the live row.
    assert!(repo.get(&id).await.unwrap().is_some());
    let listed = repo.list_by_owner("epic", &owner).await.unwrap();
    assert!(listed.iter().any(|d| d.id == id), "owner listing includes the doc");

    // dedup probe by content hash.
    let found = repo.find_by_sha(&sha).await.unwrap();
    assert_eq!(found.map(|d| d.id), Some(id.clone()));

    // phase-1 full-text (hq-docs-search.1): a body term hits via the generated tsv column.
    let hits = repo.search("spec", Some(("epic", &owner)), 10).await.unwrap();
    assert!(hits.iter().any(|d| d.id == id), "full-text search finds the doc by body term");
    let miss = repo.search("zzznomatchxyz", None, 10).await.unwrap();
    assert!(!miss.iter().any(|d| d.id == id), "unrelated query does not match");

    // stale version guard → VersionConflict (current is 0, claim 9).
    let stale = repo
        .update(&id, 9, DocumentPatch { body_md: Some("nope".into()), ..Default::default() })
        .await;
    assert!(matches!(stale, Err(DocError::VersionConflict { found: 0, .. })), "stale update rejected");

    // correct guard → version bumps, body changes, snapshot appended.
    let updated = repo
        .update(
            &id,
            0,
            DocumentPatch {
                body_md: Some("# spec v1".into()),
                edited_by: "editor".into(),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    assert_eq!(updated.version, 1);
    assert_eq!(updated.body_md.as_deref(), Some("# spec v1"));

    // soft-delete: stale guard rejected, correct guard hides from listing.
    assert!(repo.soft_delete(&id, 0).await.is_err(), "stale soft-delete rejected");
    repo.soft_delete(&id, 1).await.expect("soft delete");

    let listed_after = repo.list_by_owner("epic", &owner).await.unwrap();
    assert!(!listed_after.iter().any(|d| d.id == id), "soft-deleted doc gone from listing");
    let got = repo.get(&id).await.unwrap().expect("row still present");
    assert!(got.deleted_at.is_some(), "soft-delete stamped deleted_at");

    // dedup no longer matches a soft-deleted row.
    assert!(repo.find_by_sha(&sha).await.unwrap().is_none(), "dedup skips soft-deleted");
}
