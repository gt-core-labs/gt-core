//! Contract test for the dedup-aware operator-bell writer (gtcore-7a707a).
//!
//! Exercises `ring_bell` against a real Postgres: fingerprint dedup inside the
//! window, silence over acked/resolved rows, the one-shot (no fingerprint) path,
//! and window expiry. No-op without `GT_PG_URL`, mirroring the store contracts.
//! Run: `cargo test -p gt-composition --test notify_bell_contract`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use gt_composition::mcp::eventlog::EventLog;
use gt_composition::mcp::notify::NotifyHandler;
use gt_composition::notify_bell::{ring_bell, BellWrite, DEFAULT_DEDUP_WINDOW_SECS};
use gt_composition::notify_kind::NotificationKind;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_store_pg::{assert_ephemeral_pg_url, notifications_migrations};

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// Provision the notifications table (idempotent, advisory-locked like the other
/// contracts) and hand back the admin pool + a temp-dir SSE log, or `None` to skip.
async fn pool_or_skip(test: &str) -> Option<(sqlx::PgPool, Arc<EventLog>)> {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset; skipping {test}");
        return None;
    };
    assert_ephemeral_pg_url(&url);
    let pool = sqlx::PgPool::connect(&url).await.expect("connect pg");
    let mut conn = pool.acquire().await.expect("acquire conn");
    sqlx::query("SELECT pg_advisory_lock(4915623077)")
        .execute(&mut *conn)
        .await
        .expect("take migration lock");
    for m in notifications_migrations() {
        sqlx::raw_sql(&m.sql).execute(&mut *conn).await.expect("apply notifications migration");
    }
    sqlx::query("SELECT pg_advisory_unlock(4915623077)")
        .execute(&mut *conn)
        .await
        .expect("release migration lock");
    let dir = std::env::temp_dir().join(format!("notify-bell-{}", nonce()));
    Some((pool, Arc::new(EventLog::new(Some(dir)))))
}

fn write<'a>(fingerprint: Option<&'a str>, title: &'a str) -> BellWrite<'a> {
    BellWrite {
        workspace: "default",
        from_role: "deacon",
        title,
        body: "the same finding, re-emitted every tick",
        kind: NotificationKind::Alert,
        fingerprint,
    }
}

#[tokio::test]
async fn refire_inside_the_window_bumps_instead_of_inserting() {
    let Some((pool, log)) = pool_or_skip("refire_inside_the_window").await else { return };
    let fp = format!("contract-{}", nonce());

    let first = ring_bell(&pool, &log, write(Some(&fp), "stuck slot"), DEFAULT_DEDUP_WINDOW_SECS)
        .await
        .expect("first ring");
    assert!(!first.deduped, "first emission inserts");
    assert_eq!(first.count, 1);

    // AC (a): re-sending the same fingerprint in the window creates NO new row —
    // it bumps the counter on the existing one and stays silent.
    let second = ring_bell(&pool, &log, write(Some(&fp), "stuck slot"), DEFAULT_DEDUP_WINDOW_SECS)
        .await
        .expect("second ring");
    assert!(second.deduped, "re-emission dedups");
    assert_eq!(second.id, first.id, "same bell row");
    assert_eq!(second.count, 2, "repeat counter bumped");

    let (rows,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM notifications WHERE fingerprint = $1")
            .bind(&fp)
            .fetch_one(&pool)
            .await
            .expect("count rows");
    assert_eq!(rows, 1, "two ticks over the same finding produce ONE live notification");
}

#[tokio::test]
async fn acked_and_resolved_rows_are_not_repaged() {
    let Some((pool, log)) = pool_or_skip("acked_rows_are_not_repaged").await else { return };
    let fp = format!("contract-{}", nonce());

    let first = ring_bell(&pool, &log, write(Some(&fp), "handled finding"), DEFAULT_DEDUP_WINDOW_SECS)
        .await
        .expect("first ring");

    // AC (b): once the operator acks (or resolves) the row, the same emitter +
    // fingerprint must NOT page again — the re-emission bumps the SAME row.
    for state in ["acked", "resolved"] {
        sqlx::query("UPDATE notifications SET state = $1 WHERE id = $2::uuid")
            .bind(state)
            .bind(&first.id)
            .execute(&pool)
            .await
            .expect("flip state");
        let again =
            ring_bell(&pool, &log, write(Some(&fp), "handled finding"), DEFAULT_DEDUP_WINDOW_SECS)
                .await
                .expect("re-ring");
        assert!(again.deduped, "{state} row still absorbs the re-emission");
        assert_eq!(again.id, first.id);
    }
}

/// Drive one MCP tool through the real handler (gtcore-73d4ab surface).
async fn call(handler: &NotifyHandler, tool: &str, args: Value) -> Result<Value, gt_store_dolt::AppError> {
    handler
        .dispatch(tool, DomainCtx { workspace: None, actor: "contract-test", args })
        .await
}

#[tokio::test]
async fn mcp_lifecycle_send_list_ack_resolve() {
    let Some((pool, log)) = pool_or_skip("mcp_lifecycle").await else { return };
    let handler = NotifyHandler::new(pool.clone(), log);
    let title = format!("lifecycle finding {}", nonce());

    // send twice → deduped into one row with count=2 (the derived fingerprint kicks in).
    let first = call(&handler, "notify.send", json!({ "title": title, "kind": "alert" }))
        .await
        .expect("send");
    let id = first["id"].as_str().expect("id").to_string();
    let second = call(&handler, "notify.send", json!({ "title": title, "kind": "alert" }))
        .await
        .expect("re-send");
    assert_eq!(second["deduped"], json!(true));
    assert_eq!(second["id"].as_str(), Some(id.as_str()));
    assert_eq!(second["count"], json!(2));

    // Out-of-set kind is an explicit, listing validation error (AC c).
    let bad = call(&handler, "notify.send", json!({ "title": "x", "kind": "warning" })).await;
    let err = format!("{bad:?}");
    assert!(err.contains("decision, info, alert"), "explicit listing error: {err}");

    // list state=new surfaces the row with its counter.
    let listed = call(&handler, "notify.list", json!({ "state": "new" })).await.expect("list");
    let row = listed["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .find(|r| r["id"].as_str() == Some(id.as_str()))
        .expect("sent row is listed as new")
        .clone();
    assert_eq!(row["count"], json!(2));
    assert_eq!(row["state"], json!("new"));

    // ack → acked (idempotent); resolve → resolved; ack-after-resolve is a usable error.
    let acked = call(&handler, "notify.ack", json!({ "id": id })).await.expect("ack");
    assert_eq!(acked["state"], json!("acked"));
    let re_acked = call(&handler, "notify.ack", json!({ "id": id })).await.expect("re-ack");
    assert_eq!(re_acked["state"], json!("acked"), "ack is idempotent");
    let resolved = call(&handler, "notify.resolve", json!({ "id": id })).await.expect("resolve");
    assert_eq!(resolved["state"], json!("resolved"));
    let regress = call(&handler, "notify.ack", json!({ "id": id })).await;
    assert!(regress.is_err(), "ack must not regress a resolved row");

    // AC (b) end-to-end: the resolved row still absorbs the same finding — no new page.
    let after = call(&handler, "notify.send", json!({ "title": title, "kind": "alert" }))
        .await
        .expect("send over resolved");
    assert_eq!(after["deduped"], json!(true), "resolved row absorbs the re-emission");

    // Unknown id → NotFound.
    let missing = call(
        &handler,
        "notify.resolve",
        json!({ "id": "00000000-0000-0000-0000-000000000000" }),
    )
    .await;
    assert!(missing.is_err(), "unknown id errors");
}

#[tokio::test]
async fn one_shot_and_expired_window_insert_fresh_rows() {
    let Some((pool, log)) = pool_or_skip("one_shot_inserts_fresh_rows").await else { return };

    // No fingerprint ⇒ the legacy one-shot behaviour: every call is a fresh row.
    let a = ring_bell(&pool, &log, write(None, "one-shot"), DEFAULT_DEDUP_WINDOW_SECS)
        .await
        .expect("one-shot a");
    let b = ring_bell(&pool, &log, write(None, "one-shot"), DEFAULT_DEDUP_WINDOW_SECS)
        .await
        .expect("one-shot b");
    assert!(!a.deduped && !b.deduped);
    assert_ne!(a.id, b.id, "one-shot writes never collapse");

    // A zero-second window means the prior emission is already outside it: a
    // recurring finding re-pages once the window truly expires.
    let fp = format!("contract-{}", nonce());
    let first = ring_bell(&pool, &log, write(Some(&fp), "expiring"), 0).await.expect("first");
    let second = ring_bell(&pool, &log, write(Some(&fp), "expiring"), 0).await.expect("second");
    assert!(!second.deduped, "expired window inserts a fresh row");
    assert_ne!(second.id, first.id);
}
