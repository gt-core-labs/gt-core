//! Contract test for the `MemoryRepository` Postgres adapter (hq-memory-mcp.2).
//!
//! Exercises the locked decisions end-to-end against a real Postgres: upsert-by-name
//! (version bump on rewrite), get / by_kind / list, full-text `recall`, the hybrid
//! (text + vector) `recall_hybrid` path, and `forget`. No-op without `GT_PG_URL`
//! (developer box / CI without Postgres), mirroring `documents_contract.rs`.
//!
//! Requires the `pg` feature (the adapter is gated on it); CI runs
//! `cargo test -p gt-store-pg --features pg`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{
    memory_migrations, MemoryRepository, NewMemory, PgMemory, WorkspacePool,
};

/// Unique suffix so repeated runs against the same ephemeral DB never collide.
fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// Provision the `ws_default` template table (idempotent migrations) and hand back a
/// tenant-scoped repository, or `None` when `GT_PG_URL` is unset (skip the gated test).
///
/// A session advisory lock serializes the apply so parallel test threads don't race on
/// concurrent DDL; the test bodies stay parallel (each uses a nonce).
async fn repo_or_skip(test: &str) -> Option<PgMemory> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let admin = sqlx::PgPool::connect(&url).await.expect("connect admin pool");
    let mut conn = admin.acquire().await.expect("acquire admin conn");
    sqlx::query("SELECT pg_advisory_lock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("take migration lock");
    for m in memory_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply memory migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("release migration lock");

    let wp = WorkspacePool::connect(&url, "default").await.expect("connect ws pool");
    Some(PgMemory::new(wp))
}

/// A 384-dim unit vector along `axis` — matches the `vector(384)` column so two such vectors
/// on distinct axes are orthogonal (cosine similarity 0) and a vector equals itself (1.0).
fn unit_vec(axis: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[axis] = 1.0;
    v
}

fn new_mem(name: &str, kind: &str, desc: &str, body: &str) -> NewMemory {
    NewMemory {
        name: name.into(),
        description: desc.into(),
        kind: kind.into(),
        body: body.into(),
        created_by: "tester".into(),
    }
}

#[tokio::test]
async fn memory_upsert_get_list_recall_forget() {
    let Some(repo) = repo_or_skip("memory_upsert_get_list_recall_forget").await else {
        return;
    };
    let n = nonce();
    let name = format!("mem-{n}");
    let body = format!("deployment rollout strategy zk{n}word note");

    // upsert → insert at version 0.
    let created = repo
        .upsert(new_mem(&name, "feedback", "a summary", &body))
        .await
        .expect("upsert insert");
    assert_eq!(created.version, 0);
    assert_eq!(created.kind, "feedback");
    assert_eq!(created.body, body);

    // get sees the row; to_memory maps to the domain type.
    let got = repo.get(&name).await.unwrap().expect("row present");
    assert_eq!(got.name, name);
    assert_eq!(got.to_memory().kind.as_str(), "feedback");

    // re-upsert same name → version bumps, fields replaced.
    let rewritten = repo
        .upsert(new_mem(&name, "project", "new summary", &body))
        .await
        .expect("upsert rewrite");
    assert_eq!(rewritten.version, 1, "rewrite bumps version");
    assert_eq!(rewritten.kind, "project", "kind replaced on rewrite");

    // by_kind / list see it under the new kind.
    let by_kind = repo.by_kind("project").await.unwrap();
    assert!(by_kind.iter().any(|m| m.name == name), "by_kind includes the memory");
    assert!(repo.list().await.unwrap().iter().any(|m| m.name == name), "list includes it");

    // full-text recall: a body term hits via the generated tsv column.
    let hits = repo.recall(&format!("zk{n}word"), None, 10).await.unwrap();
    assert!(hits.iter().any(|m| m.name == name), "recall finds the memory by body term");
    let miss = repo.recall("zzznomatchxyz", None, 10).await.unwrap();
    assert!(!miss.iter().any(|m| m.name == name), "unrelated query does not match");

    // kind-narrowed recall: a non-matching kind excludes the row.
    let narrowed = repo.recall(&format!("zk{n}word"), Some("reference"), 10).await.unwrap();
    assert!(!narrowed.iter().any(|m| m.name == name), "kind filter excludes other-kind memory");

    // forget hard-deletes; idempotent.
    repo.forget(&name).await.expect("forget");
    assert!(repo.get(&name).await.unwrap().is_none(), "forgotten memory is gone");
    repo.forget(&name).await.expect("forget is idempotent");
}

/// Hybrid recall ranks a semantically-close (lexically-distant) memory above a
/// keyword-only match, and surfaces a memory with no embedding via the text side.
#[tokio::test]
async fn memory_recall_hybrid_fuses_text_and_vector() {
    let Some(repo) = repo_or_skip("memory_recall_hybrid_fuses_text_and_vector").await else {
        return;
    };
    let n = nonce();
    let keyword = format!("zk{n}word"); // coined token only the lexical memory contains

    let sem = format!("mem-sem-{n}");
    repo.upsert(new_mem(&sem, "reference", "s", "deployment architecture and rollout strategy"))
        .await
        .expect("create semantic");
    repo.set_embedding(&sem, unit_vec(0)).await.expect("embed semantic");

    let lex = format!("mem-lex-{n}");
    repo.upsert(new_mem(&lex, "reference", "l", &format!("unrelated note mentioning {keyword}")))
        .await
        .expect("create lexical");
    repo.set_embedding(&lex, unit_vec(1)).await.expect("embed lexical");

    // No embedding: this row's `embedding` is NULL but still surfaces on the text side.
    let plain = format!("mem-plain-{n}");
    repo.upsert(new_mem(&plain, "reference", "p", &format!("plain note with {keyword} only")))
        .await
        .expect("create un-embedded");

    let hits = repo
        .recall_hybrid(&keyword, &unit_vec(0), Some("reference"), 10)
        .await
        .expect("hybrid recall");

    let pos = |name: &str| hits.iter().position(|m| m.name == name);
    let sem_pos = pos(&sem).expect("semantic returned");
    let lex_pos = pos(&lex).expect("lexical returned");
    assert!(sem_pos < lex_pos, "semantic match must outrank keyword-only match");
    assert!(pos(&plain).is_some(), "un-embedded row surfaces on the text side");
}
