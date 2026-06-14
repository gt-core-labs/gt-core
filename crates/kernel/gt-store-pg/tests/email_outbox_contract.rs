//! Contract test for the `EmailOutboxRepository` Postgres adapter (hq-f24599).
//!
//! Exercises the schedule → claim → settle pipeline against a real Postgres:
//! enqueue (immediate + future send_at), due-claiming (future rows stay
//! untouched, claimed rows flip to `sending`), the sent/retry/failed
//! settlements, and operator cancel. No-op without `GT_PG_URL`.
//! Run: `cargo test -p gt-store-pg --features pg --test email_outbox_contract`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::types::chrono::Utc;
use chrono::Duration;

use gt_store_pg::{
    email_migrations, EmailOutboxRepository, NewEmail, OutboxError, PgEmailOutbox,
};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

async fn repo_or_skip(test: &str) -> Option<(PgEmailOutbox, sqlx::PgPool)> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let mut conn = pool.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock(4915623003)")
        .execute(&mut *conn)
        .await
        .expect("lock");
    for m in email_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623003)")
        .execute(&mut *conn)
        .await
        .expect("unlock");
    Some((PgEmailOutbox::new(pool.clone()), pool))
}

fn new_email(id: &str, ws: &str, send_at: Option<sqlx::types::chrono::DateTime<Utc>>) -> NewEmail {
    NewEmail {
        id: id.into(),
        workspace: ws.into(),
        recipient: "ops@example.com".into(),
        subject: "reporte".into(),
        body: "hola".into(),
        template_ref: None,
        send_at,
        created_by: "tester".into(),
    }
}

#[tokio::test]
async fn schedule_claim_and_settlements_round_trip() {
    let Some((repo, _pool)) = repo_or_skip("outbox pipeline contract").await else { return };
    let n = nonce();
    let ws = format!("wstest{n}");
    let due_id = format!("e-{n}-due");
    let future_id = format!("e-{n}-future");

    // One immediately-due row, one scheduled into the future.
    let due = repo.enqueue(new_email(&due_id, &ws, None)).await.expect("enqueue due");
    // Force send_at into the past using PG's own clock to avoid Rust/PG clock skew.
    sqlx::query("UPDATE email_outbox SET send_at = now() - interval '2 seconds' WHERE id = $1")
        .bind(&due_id).execute(&_pool).await.expect("force past");
    assert_eq!(due.status, "pending");
    assert_eq!(due.attempts, 0);
    repo.enqueue(new_email(&future_id, &ws, Some(Utc::now() + Duration::hours(2))))
        .await
        .expect("enqueue future");

    // Claiming flips only the due row to `sending`; the future one stays queued.
    let claimed = repo.claim_due(500).await.expect("claim");
    assert!(claimed.iter().any(|e| e.id == due_id), "due row claimed");
    assert!(
        !claimed.iter().any(|e| e.id == future_id),
        "future row must not be claimed before send_at"
    );

    // Settle as retry: attempts bump, error recorded, send_at pushed forward.
    let next = Utc::now() + Duration::minutes(2);
    repo.mark_retry(&due_id, "smtp pending", next).await.expect("retry");
    let listed = repo.list(&ws, Some("retry"), 10).await.expect("list retry");
    let row = listed.iter().find(|e| e.id == due_id).expect("listed");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.last_error.as_deref(), Some("smtp pending"));
    assert!(row.send_at > Utc::now(), "backoff pushed send_at forward");

    // A retry row whose send_at is future is NOT due; pull it back due and
    // settle as sent.
    assert!(!repo.claim_due(500).await.expect("claim2").iter().any(|e| e.id == due_id));
    sqlx::query("UPDATE email_outbox SET send_at = now() WHERE id = $1")
        .bind(&due_id)
        .execute(&_pool)
        .await
        .expect("force due");
    let claimed = repo.claim_due(500).await.expect("claim3");
    assert!(claimed.iter().any(|e| e.id == due_id));
    repo.mark_sent(&due_id).await.expect("sent");
    let listed = repo.list(&ws, Some("sent"), 10).await.expect("list sent");
    let row = listed.iter().find(|e| e.id == due_id).expect("sent listed");
    assert_eq!(row.attempts, 2);
    assert!(row.sent_at.is_some() && row.last_error.is_none());

    // Settling a non-claimed row fails loud (state machine, not silent no-op).
    assert!(matches!(repo.mark_sent(&due_id).await, Err(OutboxError::NotFound(_))));
}

#[tokio::test]
async fn cancel_recalls_pending_but_never_sent() {
    let Some((repo, _pool)) = repo_or_skip("outbox cancel contract").await else { return };
    let n = nonce();
    let ws = format!("wstest{n}");
    let id = format!("e-{n}-cancel");

    repo.enqueue(new_email(&id, &ws, Some(Utc::now() + Duration::hours(1))))
        .await
        .expect("enqueue");
    // Wrong workspace cannot cancel another tenant's mail.
    assert!(matches!(
        repo.cancel(&id, "otherws").await,
        Err(OutboxError::NotFound(_))
    ));
    repo.cancel(&id, &ws).await.expect("cancel");
    let listed = repo.list(&ws, Some("cancelled"), 10).await.expect("list");
    assert!(listed.iter().any(|e| e.id == id));
    // A cancelled row is no longer claimable nor cancellable again.
    assert!(!repo.claim_due(500).await.expect("claim").iter().any(|e| e.id == id));
    assert!(matches!(repo.cancel(&id, &ws).await, Err(OutboxError::NotFound(_))));
}

#[tokio::test]
async fn failed_settlement_records_the_terminal_state() {
    let Some((repo, _pool)) = repo_or_skip("outbox failed contract").await else { return };
    let n = nonce();
    let ws = format!("wstest{n}");
    let id = format!("e-{n}-fail");

    repo.enqueue(new_email(&id, &ws, None)).await.expect("enqueue");
    sqlx::query("UPDATE email_outbox SET send_at = now() - interval '2 seconds' WHERE id = $1")
        .bind(&id).execute(&_pool).await.expect("force past");
    let claimed = repo.claim_due(500).await.expect("claim");
    assert!(claimed.iter().any(|e| e.id == id));
    repo.mark_failed(&id, "attempts exhausted").await.expect("fail");
    let listed = repo.list(&ws, Some("failed"), 10).await.expect("list");
    let row = listed.iter().find(|e| e.id == id).expect("failed listed");
    assert_eq!(row.last_error.as_deref(), Some("attempts exhausted"));
    // Terminal: never claimed again.
    assert!(!repo.claim_due(500).await.expect("reclaim").iter().any(|e| e.id == id));
}
