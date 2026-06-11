//! Contract test for the `DocChunksRepository` Postgres adapter (hq-c488cb).
//!
//! Exercises the chunk-index lifecycle against a real Postgres + pgvector:
//! atomic replace, cosine retrieve (best match first, rig-prefix narrowing),
//! purge, and the backfill frontier. Vectors are synthetic orthogonal axes so
//! similarity is deterministic without a model. No-op without `GT_PG_URL`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{
    docs_migrations, DocChunksRepository, DocumentsRepository, NewChunk, NewDocument,
    PgDocChunks, PgDocuments, WorkspacePool,
};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

fn unit_vec(axis: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[axis] = 1.0;
    v
}

async fn repo_or_skip(test: &str) -> Option<(PgDocChunks, PgDocuments)> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let admin = sqlx::PgPool::connect(&url).await.expect("connect");
    let mut conn = admin.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock(4915623005)")
        .execute(&mut *conn)
        .await
        .expect("lock");
    for m in docs_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply docs migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623005)")
        .execute(&mut *conn)
        .await
        .expect("unlock");
    let wp = WorkspacePool::connect(&url, "default").await.expect("ws pool");
    Some((PgDocChunks::new(wp.clone()), PgDocuments::new(wp)))
}

fn chunk(i: i32, content: &str, axis: usize) -> NewChunk {
    NewChunk { chunk_idx: i, content: content.into(), embedding: unit_vec(axis) }
}

#[tokio::test]
async fn replace_retrieve_rig_filter_and_purge() {
    let Some((chunks, _docs)) = repo_or_skip("doc chunks contract").await else { return };
    let n = nonce();
    let doc_a = format!("doc-{n}-a");
    let doc_b = format!("doc-{n}-b");

    // Two docs on different rigs; axis 0/1/2 vectors make ranking deterministic.
    chunks
        .replace_chunks(&doc_a, "epic", &format!("hq-{n}"), "a.md", &[
            chunk(0, "sobre tableros kanban", 0),
            chunk(1, "sobre lexorank", 1),
        ])
        .await
        .expect("replace a");
    chunks
        .replace_chunks(&doc_b, "epic", &format!("gtweb-{n}"), "b.md", &[chunk(0, "sobre svelte", 2)])
        .await
        .expect("replace b");

    // Query along axis 1: the lexorank chunk wins with similarity ~1.
    let hits = chunks.retrieve(&unit_vec(1), 2, None).await.expect("retrieve");
    assert_eq!(hits[0].doc_id, doc_a);
    assert_eq!(hits[0].chunk_idx, 1);
    assert!(hits[0].similarity > 0.99, "{}", hits[0].similarity);

    // Rig narrowing: an hq-scoped retrieve never sees the gtweb doc.
    let hits = chunks.retrieve(&unit_vec(2), 5, Some("hq")).await.expect("rig retrieve");
    assert!(hits.iter().all(|c| c.owner_id.starts_with("hq-")));
    assert!(!hits.iter().any(|c| c.doc_id == doc_b));

    // Replace is atomic: re-indexing doc_a with ONE chunk drops the old pair.
    chunks
        .replace_chunks(&doc_a, "epic", &format!("hq-{n}"), "a.md", &[chunk(0, "nuevo", 3)])
        .await
        .expect("re-replace");
    let hits = chunks.retrieve(&unit_vec(1), 5, None).await.expect("post-replace");
    assert!(!hits.iter().any(|c| c.doc_id == doc_a && c.similarity > 0.9));

    // Purge: gone entirely.
    chunks.purge(&doc_a).await.expect("purge");
    let hits = chunks.retrieve(&unit_vec(3), 5, None).await.expect("post-purge");
    assert!(!hits.iter().any(|c| c.doc_id == doc_a));
}

#[tokio::test]
async fn backfill_frontier_lists_text_docs_without_chunks() {
    let Some((chunks, docs)) = repo_or_skip("doc chunks backfill contract").await else { return };
    let n = nonce();
    let id = format!("doc-{n}-bf");
    docs.create(NewDocument {
        id: id.clone(),
        owner_type: "epic".into(),
        owner_id: format!("hq-{n}"),
        kind: "md".into(),
        filename: "bf.md".into(),
        content_type: Some("text/markdown".into()),
        size: Some(4),
        sha256: None,
        body_md: Some("hola".into()),
        bucket: None,
        key: None,
        extracted_text: None,
        uploaded_by: "tester".into(),
    })
    .await
    .expect("create doc");

    let frontier = chunks.unindexed_doc_ids(10_000).await.expect("frontier");
    assert!(frontier.contains(&id), "text doc without chunks is backfillable");

    chunks
        .replace_chunks(&id, "epic", &format!("hq-{n}"), "bf.md", &[chunk(0, "hola", 0)])
        .await
        .expect("index");
    let frontier = chunks.unindexed_doc_ids(10_000).await.expect("frontier 2");
    assert!(!frontier.contains(&id), "indexed doc leaves the frontier");
}
