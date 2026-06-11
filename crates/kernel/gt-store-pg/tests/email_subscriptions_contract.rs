//! Contract test for the `SubscriptionsRepository` Postgres adapter (hq-8a521a).
//! No-op without `GT_PG_URL`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{email_migrations, PgSubscriptions, SubscriptionError, SubscriptionsRepository};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

#[tokio::test]
async fn subscribe_is_idempotent_and_fanout_lists_watchers() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping subscriptions contract");
        return;
    };
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let mut conn = pool.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock(4915623006)").execute(&mut *conn).await.expect("lock");
    for m in email_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623006)").execute(&mut *conn).await.expect("unlock");

    let subs = PgSubscriptions::new(pool);
    let n = nonce();
    let ws = format!("wstest{n}");
    let ana = format!("ana{n}@x.com");
    let bob = format!("bob{n}@x.com");

    // Idempotent subscribe; both watchers fan out; member listing works.
    subs.subscribe(&ws, &ana, "card", "hq-1").await.expect("sub ana");
    subs.subscribe(&ws, &ana, "card", "hq-1").await.expect("re-sub is a no-op");
    subs.subscribe(&ws, &bob, "card", "hq-1").await.expect("sub bob");
    subs.subscribe(&ws, &ana, "board", "hq").await.expect("sub board");

    let mut watchers = subs.subscribers(&ws, "card", "hq-1").await.expect("watchers");
    watchers.sort();
    assert_eq!(watchers, vec![ana.clone(), bob.clone()]);
    let mine = subs.list_for_member(&ws, &ana).await.expect("mine");
    assert_eq!(mine.len(), 2);

    // Tenant isolation: another workspace sees nothing.
    assert!(subs.subscribers("otherws", "card", "hq-1").await.expect("other").is_empty());

    // Unsubscribe removes exactly one edge; a second attempt is NotFound.
    subs.unsubscribe(&ws, &ana, "card", "hq-1").await.expect("unsub");
    assert_eq!(subs.subscribers(&ws, "card", "hq-1").await.expect("after"), vec![bob]);
    assert!(matches!(
        subs.unsubscribe(&ws, &ana, "card", "hq-1").await,
        Err(SubscriptionError::NotFound(_))
    ));
}
