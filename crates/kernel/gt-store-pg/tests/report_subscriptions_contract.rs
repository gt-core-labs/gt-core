//! Contract test for the `ReportSubscriptionsRepository` Postgres adapter
//! (hq-562e0b). No-op without `GT_PG_URL`.

#![cfg(feature = "pg")]

use std::time::{SystemTime, UNIX_EPOCH};

use gt_store_pg::{
    email_migrations, PgReportSubscriptions, ReportSubscriptionError,
    ReportSubscriptionsRepository,
};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

#[tokio::test]
async fn add_select_toggle_remove_lifecycle() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping report subscriptions contract");
        return;
    };
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let mut conn = pool.acquire().await.expect("acquire");
    sqlx::query("SELECT pg_advisory_lock(4915623007)").execute(&mut *conn).await.expect("lock");
    for m in email_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623007)").execute(&mut *conn).await.expect("unlock");

    let subs = PgReportSubscriptions::new(pool);
    let n = nonce();
    let ws = format!("wstest{n}");
    let ana = format!("ana{n}@x.com");
    let bob = format!("bob{n}@x.com");

    // Add is idempotent and defaults to enabled.
    subs.add(&ws, &ana).await.expect("add ana");
    subs.add(&ws, &ana).await.expect("re-add is a no-op");
    subs.add(&ws, &bob).await.expect("add bob");
    let listed = subs.list(&ws).await.expect("list");
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|s| s.enabled), "default enabled");

    // The selection switch: disabled rows stay listed but leave the send set.
    subs.set_enabled(&ws, &bob, false).await.expect("disable bob");
    assert_eq!(subs.enabled_emails(&ws).await.expect("enabled"), vec![ana.clone()]);
    assert_eq!(subs.list(&ws).await.expect("list").len(), 2, "disabled still listed");
    // Re-add of a disabled row must NOT silently re-enable it.
    subs.add(&ws, &bob).await.expect("re-add disabled");
    assert_eq!(subs.enabled_emails(&ws).await.expect("enabled"), vec![ana.clone()]);
    subs.set_enabled(&ws, &bob, true).await.expect("re-enable bob");
    assert_eq!(subs.enabled_emails(&ws).await.expect("enabled").len(), 2);

    // Tenant isolation.
    assert!(subs.list("otherws").await.expect("other").is_empty());

    // Remove; second attempt + toggle of unknown are NotFound.
    subs.remove(&ws, &ana).await.expect("remove");
    assert!(matches!(subs.remove(&ws, &ana).await, Err(ReportSubscriptionError::NotFound(_))));
    assert!(matches!(
        subs.set_enabled(&ws, &ana, false).await,
        Err(ReportSubscriptionError::NotFound(_))
    ));
}
