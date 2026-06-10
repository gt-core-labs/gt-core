//! End-to-end custodian loop (`hq-graphrig.14`): drive the whole per-rig graph cycle over a
//! real event log with the in-memory indexer — register on attach, go stale (as a merge
//! would mark it), batch-refresh as the custodian tick, and query for context throughout.
//!
//! The merge→stale arm in production resolves the rig via `PgRigs::prefix_owner` (needs PG);
//! here we append the same `graphwarden.marked-stale.v1` the merge handler would, so the loop
//! is exercised end-to-end without a database.

use std::sync::Arc;

use serde_json::json;

use gt_composition::mcp::{EventLog, GraphHandler};
use gt_graphindex::InMemoryGraphIndexer;
use gt_graphwarden::WardenEvent;
use gt_mcp_server::{DomainCtx, DomainHandler};
use tempfile::TempDir;

fn ctx(args: serde_json::Value) -> DomainCtx<'static> {
    DomainCtx {
        workspace: None,
        actor: "custodian",
        args,
    }
}

#[tokio::test]
async fn full_per_rig_loop_register_stale_refresh_query() {
    // graph.refresh derives <GT_GRAPH_ROOT>/<ws>/<rig> server-side (no caller repo_dir anymore);
    // pin the root at a writable temp dir so the no-provisioner path can `create_dir_all` it.
    let graph_root = TempDir::new().unwrap();
    std::env::set_var("GT_GRAPH_ROOT", graph_root.path());
    let dir = TempDir::new().unwrap();
    let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
    let h = GraphHandler::new(log.clone(), Arc::new(InMemoryGraphIndexer::new()));

    // 1. ATTACH — the deploy edge calls graph.refresh {rig}; the server derives the checkout path.
    let reg = h
        .dispatch("graph.refresh", ctx(json!({ "rig": "alpha" })))
        .await
        .unwrap();
    assert_eq!(reg["ok"], true);

    // Queryable right after the initial build.
    let q1 = h
        .dispatch(
            "graph.query",
            ctx(json!({ "rig": "alpha", "question": "auth flow" })),
        )
        .await
        .unwrap();
    assert!(q1["text"].as_str().unwrap().contains("auth flow"));

    // Fresh, nothing pending.
    let list1 = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
    assert_eq!(list1["rigs"][0]["stale"], false);

    // 2. MERGE LANDS — the merge handler would append marked-stale for the owning rig.
    log.append(
        None,
        WardenEvent::MarkedStale {
            rig: "alpha".into(),
            changed: 3,
            now_secs: 200,
        },
    )
    .unwrap();

    let list2 = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
    assert_eq!(list2["rigs"][0]["stale"], true);
    assert_eq!(list2["rigs"][0]["pending_changes"], 3);

    // 3. CUSTODIAN TICK — refresh every stale rig in one batch.
    let ticked = h
        .dispatch("graph.refresh-stale", ctx(json!({})))
        .await
        .unwrap();
    let refreshed = ticked["refreshed"].as_array().unwrap();
    assert_eq!(refreshed.len(), 1);
    assert_eq!(refreshed[0]["rig"], "alpha");

    // Back to fresh, pending cleared.
    let list3 = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
    assert_eq!(list3["rigs"][0]["stale"], false);
    assert_eq!(list3["rigs"][0]["pending_changes"], 0);

    // 4. STILL QUERYABLE after the refresh.
    let q2 = h
        .dispatch(
            "graph.query",
            ctx(json!({ "rig": "alpha", "question": "auth flow" })),
        )
        .await
        .unwrap();
    assert!(q2["text"].is_string());

    // A tick with nothing stale is a no-op.
    let noop = h
        .dispatch("graph.refresh-stale", ctx(json!({})))
        .await
        .unwrap();
    assert!(noop["refreshed"].as_array().unwrap().is_empty());
}
