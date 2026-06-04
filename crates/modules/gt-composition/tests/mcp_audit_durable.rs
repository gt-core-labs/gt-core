//! Durable audit gate (`hq-mcp-test.6`): the PG sink survives a restart + `audit.tail`.
//!
//! The MCP server records every dispatch through a [`PgAuditSink`] whose writes
//! land in the cross-tenant `mcp_audit` table. Two properties this gate proves
//! against a live Postgres:
//!
//! 1. **Durability across a restart** — records written through one sink are
//!    readable by a *fresh* sink connected to the same database after the first is
//!    dropped (the process restarted), because the trail lives in Postgres, not
//!    in memory.
//! 2. **`audit.tail`** — the [`AuditHandler`] reading that same sink serves the
//!    most-recent-first window, per-tenant (a tenant never sees another's calls)
//!    and honouring the `outcome` / `limit` filters.
//!
//! Gated on `GT_PG_URL` (a sandbox Postgres). `read_all` bridges its async query
//! through `block_in_place`, so the test runs on the multi-thread runtime, like
//! the server.
//!
//! Run: `GT_PG_URL=postgres://postgres@127.0.0.1:PORT/postgres \
//!   cargo test -p gt-composition --test mcp_audit_durable`

use std::sync::Arc;
use std::time::Duration;

use gt_audit::{AuditRecord, AuditSink};
use gt_composition::mcp::AuditHandler;
use gt_mcp_server::{DomainCtx, DomainHandler, PgAuditSink};
use serde_json::json;

/// Poll the sink until its trail reaches `want` rows (the drain task inserts
/// asynchronously after `record` returns), or fail after a bounded wait.
async fn wait_for_rows(sink: &PgAuditSink, want: usize) {
    for _ in 0..100 {
        if sink.read_all().expect("read_all").len() >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("audit drain never reached {want} rows");
}

/// Dispatch `audit.tail` for `ws` with the given args, returning the decoded
/// `{count, records}` payload.
async fn tail(handler: &AuditHandler, ws: &str, args: serde_json::Value) -> serde_json::Value {
    handler
        .dispatch("audit.tail", DomainCtx { workspace: Some(ws), actor: "tester", args })
        .await
        .expect("audit.tail dispatch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_audit_survives_restart_and_tails_per_tenant() {
    let Ok(url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset — skipping hq-mcp-test.6 durable audit gate");
        return;
    };

    // Isolate from any prior run sharing the cross-tenant table.
    {
        let pool = sqlx::PgPool::connect(&url).await.expect("connect for cleanup");
        sqlx::query("DROP TABLE IF EXISTS mcp_audit").execute(&pool).await.expect("drop");
    }

    // --- First "process": write the trail through one sink, then drop it. ---
    {
        let sink = PgAuditSink::connect(&url).await.expect("connect sink 1");
        sink.record(AuditRecord::invoked("alice", "issues.read", json!({})).in_workspace("acme"))
            .expect("record acme read");
        sink.record(
            AuditRecord::unauthorized("mallory", "issues.create", json!({}))
                .in_workspace("acme"),
        )
        .expect("record acme denied");
        sink.record(AuditRecord::invoked("bob", "issues.read", json!({})).in_workspace("globex"))
            .expect("record globex read");
        wait_for_rows(&sink, 3).await;
        // sink dropped here — the drain task ends, the pool closes: a restart.
    }

    // --- Second "process": a fresh sink over the same DB sees the durable trail. ---
    let sink = Arc::new(PgAuditSink::connect(&url).await.expect("reconnect sink 2"));
    assert_eq!(
        sink.read_all().expect("read_all after restart").len(),
        3,
        "the trail written by the first sink survives a restart"
    );

    // --- audit.tail over that durable trail (the live handler path). ---
    let handler = AuditHandler::new(sink.clone() as Arc<dyn AuditSink + Send + Sync>);

    // acme sees its two calls; globex's is invisible (per-tenant gate).
    let acme = tail(&handler, "acme", json!({})).await;
    assert_eq!(acme["count"], 2, "acme tail returns only acme's records");
    for r in acme["records"].as_array().unwrap() {
        assert_eq!(r["workspace_id"], "acme", "no cross-tenant leak");
    }

    // globex sees exactly its one call.
    let globex = tail(&handler, "globex", json!({})).await;
    assert_eq!(globex["count"], 1);
    assert_eq!(globex["records"][0]["workspace_id"], "globex");

    // outcome filter narrows to the single denied acme call.
    let denied = tail(&handler, "acme", json!({ "outcome": "unauthorized" })).await;
    assert_eq!(denied["count"], 1);
    assert_eq!(denied["records"][0]["actor"], "mallory");

    // limit windows the most-recent records.
    let one = tail(&handler, "acme", json!({ "limit": 1 })).await;
    assert_eq!(one["count"], 1, "limit caps the window");
}
