//! Contract test for the `InvitesRepository` Postgres adapter (hq-4231c1).
//!
//! Exercises the one-shot token lifecycle against a real Postgres: mint, list
//! (token-lifecycle lazy expiry), revoke, the CAS accept, and the
//! double-accept / revoked / expired / unknown rejections — each with its own
//! reason. No-op without `GT_PG_URL`.
//! Run: `cargo test -p gt-store-pg --features pg --test invites_contract`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Duration;
use sqlx::types::chrono::Utc;

use gt_store_pg::{invites_migrations, InviteError, InvitesRepository, NewInvite, PgInvites};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

async fn repo_or_skip(test: &str) -> Option<PgInvites> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let mut conn = pool.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock(4915623004)")
        .execute(&mut *conn)
        .await
        .expect("lock");
    for m in invites_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623004)")
        .execute(&mut *conn)
        .await
        .expect("unlock");
    Some(PgInvites::new(pool))
}

fn new_invite(n: u128, suffix: &str, ws: &str, ttl_hours: i64) -> NewInvite {
    NewInvite {
        id: format!("inv-{n}-{suffix}"),
        token: format!("tok-{n}-{suffix}"),
        workspace: ws.into(),
        email: format!("ana{n}@example.com"),
        role: "editor".into(),
        expires_at: Utc::now() + Duration::hours(ttl_hours),
        created_by: "admin".into(),
    }
}

#[tokio::test]
async fn mint_accept_once_and_reject_double_accept() {
    let Some(repo) = repo_or_skip("invite accept contract").await else { return };
    let n = nonce();
    let ws = format!("wstest{n}");

    let minted = repo.create(new_invite(n, "a", &ws, 2)).await.expect("create");
    assert_eq!(minted.status, "pending");

    // First accept wins and stamps the consumer.
    let accepted = repo.accept(&minted.token, "ana-login").await.expect("accept");
    assert_eq!(accepted.status, "accepted");
    assert_eq!(accepted.accepted_by.as_deref(), Some("ana-login"));
    assert_eq!(accepted.role, "editor");

    // Second accept loses with the one-shot reason.
    let err = repo.accept(&minted.token, "intruso").await.expect_err("double accept");
    assert!(
        matches!(&err, InviteError::NotConsumable(m) if m.contains("already accepted")),
        "got {err:?}"
    );

    // Unknown token names itself unknown.
    assert!(matches!(
        repo.accept("tok-nope", "x").await,
        Err(InviteError::NotFound(_))
    ));
}

#[tokio::test]
async fn revoked_and_expired_tokens_are_dead() {
    let Some(repo) = repo_or_skip("invite revoke/expiry contract").await else { return };
    let n = nonce();
    let ws = format!("wstest{n}");

    // Revoke kills a pending token.
    let revoked = repo.create(new_invite(n, "r", &ws, 2)).await.expect("create");
    repo.revoke(&revoked.id, &ws).await.expect("revoke");
    let err = repo.accept(&revoked.token, "x").await.expect_err("revoked accept");
    assert!(matches!(&err, InviteError::NotConsumable(m) if m.contains("revoked")), "got {err:?}");
    // A revoked invite cannot be revoked again; wrong workspace cannot revoke.
    assert!(matches!(repo.revoke(&revoked.id, &ws).await, Err(InviteError::NotFound(_))));

    // Expired: mint already-dead and watch both accept + list agree.
    let expired = repo.create(new_invite(n, "e", &ws, -1)).await.expect("create expired");
    let err = repo.accept(&expired.token, "x").await.expect_err("expired accept");
    assert!(matches!(&err, InviteError::NotConsumable(m) if m.contains("expired")), "got {err:?}");
    let listed = repo.list(&ws).await.expect("list");
    let row = listed.iter().find(|i| i.id == expired.id).expect("listed");
    assert_eq!(row.status, "expired", "lazy expiry flips the listed status");
    // Cross-workspace revoke of someone else's invite fails.
    let foreign = repo.create(new_invite(n, "f", &ws, 2)).await.expect("create");
    assert!(matches!(
        repo.revoke(&foreign.id, "otherws").await,
        Err(InviteError::NotFound(_))
    ));
}
