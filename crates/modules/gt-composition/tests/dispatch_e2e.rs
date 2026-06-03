//! End-to-end MCP dispatch across every ported domain (`hq-mcp-dispatch.8`).
//!
//! The earlier slices (`.2..7`) each proved one domain handler in isolation. This
//! test proves the **whole seam**: the unified [`DomainRouter`] — assembled exactly
//! as the `gt-mcp-server` binary assembles it — dispatches a tool into every domain
//! and each domain's state mutates durably. It is the structural guarantee behind
//! the epic's thesis: *MCP operates every domain function, not just issue tracking.*
//!
//! `DomainRouter::dispatch` is the same routing seam the server's `call_tool`
//! delegates non-`issues`/`meta` tools to (see `gt-mcp-server/src/domain.rs`), so
//! driving tools through it here is the end-to-end dispatch path.
//!
//! PG-gated: the workspace + rig handlers are Postgres-backed, so the suite is a
//! no-op without `GT_PG_URL` (a developer box / CI without the sidecar). The
//! event-sourced domains (merge, convoy, agent, quota) write to a throwaway event
//! log under a `TempDir`, so they need no external state.

use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::PgPool;
use tempfile::TempDir;

use gt_composition::mcp::{
    AgentHandler, ConvoyHandler, EventLog, MergeHandler, QuotaHandler, RigHandler, WorkspaceHandler,
    WsPools,
};
use gt_mcp_server::{DomainCtx, DomainRouter};
use gt_module::GtModule;

/// Connect to the contract-test Postgres, or `None` when `GT_PG_URL` is unset.
async fn pg_or_skip() -> Option<(PgPool, String)> {
    let url = std::env::var("GT_PG_URL").ok()?;
    let pool = PgPool::connect(&url)
        .await
        .expect("GT_PG_URL must point at a reachable Postgres");
    Some((pool, url))
}

/// Dispatch a fully-qualified tool through the router, asserting a handler ran.
async fn call(router: &DomainRouter, tool: &str, args: Value) -> Value {
    router
        .dispatch(tool, DomainCtx { workspace: None, actor: "e2e", args })
        .await
        .unwrap_or_else(|e| panic!("{tool} dispatch errored: {e}"))
        .unwrap_or_else(|| panic!("no handler owns {tool}"))
}

/// workspace.create → rig.add → convoy.launch → agent.spawn → quota.probe →
/// merge.submit, every call routed through the one [`DomainRouter`] and read back
/// to prove the domain mutated. Proves the server operates all domain functions.
#[tokio::test]
async fn mcp_operates_every_domain_end_to_end() {
    let Some((pool, url)) = pg_or_skip().await else {
        eprintln!("GT_PG_URL unset; skipping dispatch E2E");
        return;
    };

    // The PG-backed domains own their tables via migrations: the workspace catalog
    // (public.workspaces) and the rig catalog (ws_default.rigs template).
    let ws_sql = &gt_store_pg::workspace_migrations()[0].sql;
    sqlx::raw_sql(ws_sql).execute(&pool).await.expect("apply workspaces migration");
    let rig_sql = &gt_rig::RigsModule.migrations()[0].sql;
    sqlx::raw_sql(rig_sql).execute(&pool).await.expect("apply rigs migration");
    // Idempotent re-run: drop anything this test left behind previously.
    sqlx::query("DELETE FROM workspaces WHERE id = $1").bind("e2e-ws").execute(&pool).await.ok();
    sqlx::raw_sql("DELETE FROM ws_default.rigs WHERE name = 'e2erig'").execute(&pool).await.ok();

    // The event-sourced domains share one throwaway per-workspace event log.
    let dir = TempDir::new().unwrap();
    let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));

    // Assemble the router exactly as crates/modules/gt-composition/src/bin sets up
    // build_domain_router — the unified seam under test.
    let router = DomainRouter::new()
        .register(Arc::new(WorkspaceHandler::new(pool.clone())))
        .register(Arc::new(RigHandler::new(Arc::new(WsPools::new(url)))))
        .register(Arc::new(MergeHandler::new(log.clone())))
        .register(Arc::new(ConvoyHandler::new(log.clone())))
        .register(Arc::new(AgentHandler::new(log.clone())))
        .register(Arc::new(QuotaHandler::new(log.clone())));

    // Every ported domain is wired — the structural proof the server is all-domain.
    assert_eq!(
        router.namespaces(),
        vec!["agent", "convoy", "merge", "quota", "rig", "workspace"],
        "all six domain namespaces registered"
    );

    // 1) workspace.* (PG public catalog) ------------------------------------
    let created = call(&router, "workspace.create", json!({ "id": "e2e-ws", "name": "E2E" })).await;
    assert_eq!(created["status"], "active");
    let ws = call(&router, "workspace.info", json!({ "id": "e2e-ws" })).await;
    assert_eq!(ws["name"], "E2E", "workspace persisted to Postgres");

    // 2) rig.* (PG per-workspace schema, ws_default) ------------------------
    let rig = call(
        &router,
        "rig.add",
        json!({ "name": "e2erig", "prefix": "e2", "git_url": "git@x:y/e2e.git", "default_branch": "main" }),
    )
    .await;
    assert_eq!(rig["ok"], true);
    let rigs = call(&router, "rig.list", json!({})).await;
    assert!(
        rigs["rigs"].as_array().unwrap().iter().any(|r| r["name"] == "e2erig"),
        "rig persisted to the ws_default schema"
    );

    // 3) convoy.* (event-sourced orchestration) -----------------------------
    call(&router, "convoy.launch", json!({ "convoy": "e2e-c", "members": ["b1", "b2"] })).await;
    let convoy = call(&router, "convoy.info", json!({ "convoy": "e2e-c" })).await;
    assert_eq!(convoy["state"], "launched");
    assert_eq!(
        convoy["members"][0]["state"], "active",
        "first convoy member dispatched on launch"
    );

    // 4) agent.* (event-sourced session lifecycle) --------------------------
    call(&router, "agent.spawn", json!({ "session": "e2e-s", "rig": "e2erig" })).await;
    let session = call(&router, "agent.info", json!({ "session": "e2e-s" })).await;
    assert_eq!(session["id"], "e2e-s", "session materialized in the registry");
    assert_eq!(session["rig"], "e2erig", "session bound to the rig added above");

    // 5) quota.* (event-sourced account registry) ---------------------------
    let probed = call(
        &router,
        "quota.probe",
        json!({ "account": "e2e-a", "remaining": 500, "resets_at_secs": 20_000 }),
    )
    .await;
    assert_eq!(probed["event"], "quota.usage_probed.v1");
    let acct = call(&router, "quota.info", json!({ "account": "e2e-a" })).await;
    assert_eq!(acct["id"], "e2e-a", "account materialized from the probe event");

    // 6) merge.* (event-sourced merge board) --------------------------------
    call(
        &router,
        "merge.submit",
        json!({ "bead": "e2e-bead", "branch": "feat/e2e", "channel_msg_id": "m1" }),
    )
    .await;
    let slot = call(&router, "merge.info", json!({ "bead": "e2e-bead" })).await;
    assert_eq!(slot["state"], "ready", "merge slot recorded on the board");

    // A tool in an unregistered namespace routes to no handler (the server would
    // report it as unknown), distinct from a handler's own error.
    let unrouted = router
        .dispatch("nope.verb", DomainCtx { workspace: None, actor: "e2e", args: json!({}) })
        .await
        .unwrap();
    assert!(unrouted.is_none(), "unknown namespace returns None, not an error");

    // Clean up the durable PG rows (the event log dies with the TempDir).
    sqlx::query("DELETE FROM workspaces WHERE id = $1").bind("e2e-ws").execute(&pool).await.ok();
    sqlx::raw_sql("DELETE FROM ws_default.rigs WHERE name = 'e2erig'").execute(&pool).await.ok();
}
