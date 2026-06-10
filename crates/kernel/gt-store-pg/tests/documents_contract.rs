//! Contract test for the `DocumentsRepository` Postgres adapter (hq-docs-store.3, docs/11).
//!
//! Exercises the locked decisions end-to-end against a real Postgres: create + version
//! snapshot, owner listing, `sha256` dedup, optimistic `version` guard (conflict on a
//! stale write), soft-delete, and the phase-2 hybrid (text + vector) search path. No-op
//! without `GT_PG_URL` (developer box / CI without Postgres), mirroring the `WorkspacePool`
//! contract test.
//!
//! Bead coverage (epic `hq-docs-test`, docs/12):
//! - `hq-docs-test-store.1` — CRUD + version snapshot + soft-delete round trip.
//! - `hq-docs-test-store.2` — `sha256` dedup across owners; soft-deleted drops out of `find_by_sha`.
//! - `hq-docs-test-store.3` — optimistic `version` guard (stale → `VersionConflict`).
//! - `hq-docs-test-store.4` — `list_by_owner_id` spans every `owner_type`; excludes soft-deleted.
//! - `hq-docs-test-search.1` — phase-1 full-text hit/miss + owner narrowing.
//! - `hq-docs-test-search.2` — hybrid ranks a semantically-close (lexically-distant) doc first.
//! - `hq-docs-test-search.3` — a row with no embedding still surfaces on the text side of `search_hybrid`.
//! - `hq-vectorstore-abstraction.1` — `VectorStore::delete_embedding` clears a row's embedding
//!   (idempotent on a row with none), and the row drops out of the vector side of `search_hybrid`.
//!
//! Requires the `pg` feature (the adapter is gated on it); CI runs
//! `cargo test -p gt-store-pg --features pg`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{docs_migrations, DocError, DocumentPatch, DocumentsRepository, NewDocument, PgDocuments, VectorStore, WorkspacePool};

/// Unique suffix so repeated runs against the same ephemeral DB never collide.
fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// Provision the `ws_default` template tables (idempotent migrations) and hand back a
/// tenant-scoped repository, or `None` when `GT_PG_URL` is unset (skip the gated test).
///
/// The migrations are `CREATE ... IF NOT EXISTS`, but that check is not atomic under
/// concurrent DDL — parallel test threads applying them at once race on `pg_type`. A
/// session advisory lock serializes the apply so the first thread creates and the rest
/// see the objects already present; the test bodies stay parallel (each uses a nonce).
async fn repo_or_skip(test: &str) -> Option<PgDocuments> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let admin = sqlx::PgPool::connect(&url).await.expect("connect admin pool");
    let mut conn = admin.acquire().await.expect("acquire admin conn");
    sqlx::query("SELECT pg_advisory_lock(4915623001)")
        .execute(&mut *conn)
        .await
        .expect("take migration lock");
    for m in docs_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply docs migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623001)")
        .execute(&mut *conn)
        .await
        .expect("release migration lock");

    let wp = WorkspacePool::connect(&url, "default").await.expect("connect ws pool");
    Some(PgDocuments::new(wp))
}

/// A 384-dim unit vector along `axis` — matches the `vector(384)` column so two such vectors
/// on distinct axes are orthogonal (cosine similarity 0) and a vector equals itself (1.0).
fn unit_vec(axis: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[axis] = 1.0;
    v
}

/// A minimal `md` document for one owner, with the given content hash + body.
fn new_md(id: &str, owner_type: &str, owner_id: &str, sha: &str, body: &str) -> NewDocument {
    NewDocument {
        id: id.into(),
        owner_type: owner_type.into(),
        owner_id: owner_id.into(),
        kind: "md".into(),
        filename: "spec.md".into(),
        content_type: Some("text/markdown".into()),
        size: None,
        sha256: Some(sha.into()),
        body_md: Some(body.into()),
        bucket: None,
        key: None,
        extracted_text: None,
        uploaded_by: "tester".into(),
    }
}

#[tokio::test]
async fn documents_crud_version_dedup_softdelete() {
    let Some(repo) = repo_or_skip("documents_crud_version_dedup_softdelete").await else {
        return;
    };

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
    // hq-docs-api.3: list-by-owner-id (any owner_type) backs the gt://issue/{id} inline.
    let by_id = repo.list_by_owner_id(&owner).await.unwrap();
    assert!(by_id.iter().any(|d| d.id == id), "owner-id listing includes the doc");

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

/// hq-docs-test-store.2 — one content hash, many rows across owners; `find_by_sha` returns a
/// live row and a soft-deleted row drops out of the probe.
#[tokio::test]
async fn dedup_by_sha_spans_owners_and_skips_soft_deleted() {
    let Some(repo) = repo_or_skip("dedup_by_sha_spans_owners_and_skips_soft_deleted").await else {
        return;
    };
    let n = nonce();
    let sha = format!("{n:064x}");
    let (id_a, id_b) = (format!("doc-a-{n}"), format!("doc-b-{n}"));
    let (own_a, own_b) = (format!("epic-{n}"), format!("spec-{n}"));

    // Same content hash, two different owners: dedup is by content, not by owner.
    repo.create(new_md(&id_a, "epic", &own_a, &sha, "# shared")).await.expect("create a");
    repo.create(new_md(&id_b, "spec", &own_b, &sha, "# shared")).await.expect("create b");

    // A live row for that hash exists (LIMIT 1 — either of the two).
    let first = repo.find_by_sha(&sha).await.unwrap().expect("a live row for the hash");
    assert!(first.id == id_a || first.id == id_b, "probe returns one of the two rows");

    // Soft-delete the one the probe returned; the other live row still answers the probe.
    repo.soft_delete(&first.id, first.version).await.expect("soft delete the matched row");
    let survivor = repo.find_by_sha(&sha).await.unwrap().expect("the other row still matches");
    assert_ne!(survivor.id, first.id, "the soft-deleted row dropped out; the sibling remains");

    // Soft-delete the survivor too: now no live row carries the hash.
    repo.soft_delete(&survivor.id, survivor.version).await.expect("soft delete the survivor");
    assert!(repo.find_by_sha(&sha).await.unwrap().is_none(), "no live row left for the hash");
}

/// hq-docs-test-store.4 — `list_by_owner_id` returns every `owner_type` carrying an id (backs the
/// `gt://issue/{id}` inline) and excludes soft-deleted rows.
#[tokio::test]
async fn list_by_owner_id_spans_owner_types_and_excludes_deleted() {
    let Some(repo) = repo_or_skip("list_by_owner_id_spans_owner_types_and_excludes_deleted").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("hq-{n}"); // one bead id, two attachments of different classes
    let (id_epic, id_spec) = (format!("doc-epic-{n}"), format!("doc-spec-{n}"));

    repo.create(new_md(&id_epic, "epic", &owner, &format!("{n:064x}"), "# epic doc"))
        .await
        .expect("create epic-class doc");
    let spec = repo
        .create(new_md(&id_spec, "spec", &owner, &format!("{:064x}", n + 1), "# spec doc"))
        .await
        .expect("create spec-class doc");

    // Both classes surface under the shared id, regardless of owner_type.
    let listed = repo.list_by_owner_id(&owner).await.unwrap();
    assert!(listed.iter().any(|d| d.id == id_epic), "epic-class doc listed");
    assert!(listed.iter().any(|d| d.id == id_spec), "spec-class doc listed");

    // Soft-deleting one drops it from the inline; the other remains.
    repo.soft_delete(&spec.id, spec.version).await.expect("soft delete the spec doc");
    let after = repo.list_by_owner_id(&owner).await.unwrap();
    assert!(after.iter().any(|d| d.id == id_epic), "live doc still listed");
    assert!(!after.iter().any(|d| d.id == id_spec), "soft-deleted doc excluded from owner-id list");
}

/// hq-docs-test-search.2 — hybrid search ranks a doc that is semantically close to the query
/// (but shares no keyword with it) above a doc that only matches the keyword lexically.
#[tokio::test]
async fn hybrid_search_ranks_semantic_match_above_keyword_only() {
    let Some(repo) = repo_or_skip("hybrid_search_ranks_semantic_match_above_keyword_only").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");
    let keyword = format!("zk{n}word"); // a coined token only the lexical doc contains

    // The semantic doc: no occurrence of `keyword`, but its embedding equals the query's.
    let semantic = repo
        .create(new_md(
            &format!("doc-sem-{n}"),
            "epic",
            &owner,
            &format!("{n:064x}"),
            "deployment architecture and rollout strategy",
        ))
        .await
        .expect("create semantic doc");
    repo.set_embedding(&semantic.id, unit_vec(0)).await.expect("embed semantic doc");

    // The lexical doc: contains `keyword` (text match) but its embedding is orthogonal.
    let lexical = repo
        .create(new_md(
            &format!("doc-lex-{n}"),
            "epic",
            &owner,
            &format!("{:064x}", n + 1),
            &format!("an unrelated note mentioning {keyword} once"),
        ))
        .await
        .expect("create lexical doc");
    repo.set_embedding(&lexical.id, unit_vec(1)).await.expect("embed lexical doc");

    // Query carries the keyword (so the lexical doc matches on text) but its vector points at
    // the semantic doc. Fusion: semantic ≈ (0 text + 1.0 vector) beats lexical ≈ (small text + 0).
    let hits = repo
        .search_hybrid(&keyword, &unit_vec(0), Some(("epic", &owner)), 10)
        .await
        .expect("hybrid search");

    let pos = |id: &str| hits.iter().position(|d| d.id == id);
    let sem_pos = pos(&semantic.id).expect("semantic doc returned");
    let lex_pos = pos(&lexical.id).expect("lexical doc returned");
    assert!(
        sem_pos < lex_pos,
        "semantic match (pos {sem_pos}) must outrank keyword-only match (pos {lex_pos})"
    );
}

/// hq-docs-test-search.3 — a row with no embedding still surfaces on the text side of
/// `search_hybrid` (the vector term coalesces to 0 rather than excluding the row).
#[tokio::test]
async fn hybrid_search_surfaces_rows_without_embedding() {
    let Some(repo) = repo_or_skip("hybrid_search_surfaces_rows_without_embedding").await else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");
    let keyword = format!("qx{n}term");

    // No `set_embedding`: this row's `embedding` is NULL.
    let doc = repo
        .create(new_md(
            &format!("doc-noemb-{n}"),
            "epic",
            &owner,
            &format!("{n:064x}"),
            &format!("a plain note containing {keyword} and nothing else special"),
        ))
        .await
        .expect("create un-embedded doc");

    // Hybrid search with any query vector still finds it via the text term.
    let hits = repo
        .search_hybrid(&keyword, &unit_vec(0), Some(("epic", &owner)), 10)
        .await
        .expect("hybrid search");
    assert!(
        hits.iter().any(|d| d.id == doc.id),
        "a row with no embedding must still surface on the text side of hybrid search"
    );
}

/// hq-vectorstore-abstraction.1 — `VectorStore::delete_embedding` clears a row's embedding:
/// idempotent on a row with none, and once cleared the row drops out of the vector side of
/// `search_hybrid` (it no longer matches `embedding IS NOT NULL`, and its text doesn't match
/// either).
#[tokio::test]
async fn delete_embedding_clears_vector_and_is_idempotent() {
    let Some(repo) = repo_or_skip("delete_embedding_clears_vector_and_is_idempotent").await
    else {
        return;
    };
    let n = nonce();
    let owner = format!("epic-{n}");
    let unrelated_query = format!("zz{n}nomatch");

    let doc = repo
        .create(new_md(
            &format!("doc-del-{n}"),
            "epic",
            &owner,
            &format!("{n:064x}"),
            "content with no relation to the query keyword",
        ))
        .await
        .expect("create doc");

    // delete_embedding on a row with no embedding yet is a no-op success.
    repo.delete_embedding(&doc.id)
        .await
        .expect("delete_embedding on absent embedding");

    // Embed it, then confirm it surfaces via the vector side of hybrid search (text doesn't match).
    repo.set_embedding(&doc.id, unit_vec(2)).await.expect("embed doc");
    let hits = repo
        .search_hybrid(&unrelated_query, &unit_vec(2), Some(("epic", &owner)), 10)
        .await
        .expect("hybrid search before delete");
    assert!(
        hits.iter().any(|d| d.id == doc.id),
        "embedded doc must surface via the vector side of hybrid search"
    );

    // Clear the embedding: the row drops out of hybrid search entirely (no text or vector match).
    repo.delete_embedding(&doc.id).await.expect("delete_embedding");
    let hits = repo
        .search_hybrid(&unrelated_query, &unit_vec(2), Some(("epic", &owner)), 10)
        .await
        .expect("hybrid search after delete");
    assert!(
        !hits.iter().any(|d| d.id == doc.id),
        "doc with cleared embedding and no text match must not surface"
    );
}
