//! Contract test for the `CommentsRepository` Postgres adapter (hq-57042e).
//!
//! Exercises CRUD + threading + soft-delete + the `@mention` member resolution
//! against a real Postgres. No-op without `GT_PG_URL`, mirroring the documents
//! contract. Run: `cargo test -p gt-store-pg --features pg --test comments_contract`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{
    assert_ephemeral_pg_url, comments_migrations, CommentError, CommentsRepository, NewComment,
    PgComments, WorkspacePool,
};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// Provision the template table (idempotent, advisory-locked like the documents
/// contract) and hand back a tenant-scoped repository, or `None` to skip.
async fn repo_or_skip(test: &str) -> Option<PgComments> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    assert_ephemeral_pg_url(&url);
    let admin = sqlx::PgPool::connect(&url).await.expect("connect admin pool");
    let mut conn = admin.acquire().await.expect("acquire admin conn");
    sqlx::query("SELECT pg_advisory_lock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("take migration lock");
    for m in comments_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply comments migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623002)")
        .execute(&mut *conn)
        .await
        .expect("release migration lock");

    let wp = WorkspacePool::connect(&url, "default").await.expect("connect ws pool");
    Some(PgComments::new(wp))
}

fn new_comment(id: &str, target_id: &str, body: &str, parent: Option<&str>) -> NewComment {
    NewComment {
        id: id.into(),
        target_kind: "card".into(),
        target_id: target_id.into(),
        author: "tester".into(),
        body: body.into(),
        parent_id: parent.map(str::to_string),
    }
}

#[tokio::test]
async fn crud_threading_and_soft_delete_round_trip() {
    let Some(repo) = repo_or_skip("comments CRUD contract").await else { return };
    let n = nonce();
    let target = format!("hq-card-{n}");
    let root_id = format!("c-{n}-root");
    let reply_id = format!("c-{n}-reply");

    // Create a top-level comment + a threaded reply.
    let root = repo
        .insert(new_comment(&root_id, &target, "primer comentario", None))
        .await
        .expect("insert root");
    assert_eq!(root.author, "tester");
    assert!(root.parent_id.is_none() && root.edited_at.is_none());
    let reply = repo
        .insert(new_comment(&reply_id, &target, "respuesta", Some(&root_id)))
        .await
        .expect("insert reply");
    assert_eq!(reply.parent_id.as_deref(), Some(root_id.as_str()));

    // The thread lists chronologically, live rows only.
    let thread = repo.list_for_target("card", &target).await.expect("list");
    let ids: Vec<&str> = thread.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, [root_id.as_str(), reply_id.as_str()]);

    // Edit stamps edited_at and overwrites the body.
    let edited = repo.update_body(&root_id, "editado").await.expect("edit");
    assert_eq!(edited.body, "editado");
    assert!(edited.edited_at.is_some());

    // Soft-delete hides the root but keeps the reply anchored in the thread.
    repo.soft_delete(&root_id).await.expect("delete");
    let thread = repo.list_for_target("card", &target).await.expect("relist");
    let ids: Vec<&str> = thread.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, [reply_id.as_str()]);
    assert!(matches!(repo.get(&root_id).await, Err(CommentError::NotFound(_))));
    // Idempotent failure: a second delete is NotFound, not silent success.
    assert!(matches!(repo.soft_delete(&root_id).await, Err(CommentError::NotFound(_))));
    // Editing a dead comment is NotFound too.
    assert!(matches!(repo.update_body(&root_id, "zombi").await, Err(CommentError::NotFound(_))));
}

#[tokio::test]
async fn mention_resolution_matches_unique_member_handles() {
    let Some(repo) = repo_or_skip("comments mention contract").await else { return };
    let Ok(url) = std::env::var("GT_PG_URL") else { return };
    let n = nonce();

    // Seed two members into the tenant mirror (ws_default.users) — the table the
    // resolver matches against. Shape per gt-auth's per-ws login mirror; create a
    // minimal compatible table when the auth migrations haven't run in this DB.
    let admin = sqlx::PgPool::connect(&url).await.expect("connect");
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS ws_default.users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL DEFAULT '',
            scopes TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
            roles TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
            created_at BIGINT NOT NULL DEFAULT 0,
            updated_at BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(&admin)
    .await
    .expect("ensure users mirror");
    let ana = format!("ana{n}@example.com");
    let bob = format!("bob{n}@example.com");
    for (id, email) in [(format!("u-{n}-a"), &ana), (format!("u-{n}-b"), &bob)] {
        // Full column set: the REAL gt-auth mirror (when its migrations ran in this
        // DB) carries NOT NULL columns without defaults (e.g. password_hash).
        sqlx::query(
            "INSERT INTO ws_default.users \
                (id, email, password_hash, scopes, roles, created_at, updated_at) \
             VALUES ($1, $2, '', ARRAY[]::text[], ARRAY['viewer']::text[], 0, 0) \
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(email)
        .execute(&admin)
        .await
        .expect("seed member");
    }

    // Full-email handle resolves; local-part handle resolves; unknown is None.
    let hit = repo.resolve_mention(&ana).await.expect("resolve full");
    assert_eq!(hit.as_deref(), Some(ana.as_str()));
    let local = ana.split('@').next().unwrap();
    let hit = repo.resolve_mention(local).await.expect("resolve local");
    assert_eq!(hit.as_deref(), Some(ana.as_str()));
    let miss = repo.resolve_mention(&format!("nadie{n}")).await.expect("resolve miss");
    assert!(miss.is_none());
}
