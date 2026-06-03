//! End-to-end domain dispatch (`hq-mcp-dispatch.8`).
//!
//! Drives a [`DomainRouter`] wired with the three cross-tier handlers the epic
//! delivered — workspace (PG, shared schema), rig (PG, per-workspace schema),
//! agent (event log) — exactly as the `gt-mcp-server` binary composes them, and
//! exercises the headline path: **create a workspace, register a rig in it, spawn
//! a session in it, and read all three back**. This is the integration proof that
//! the dispatch seam (`hq-mcp-dispatch.1`) operates every domain through one
//! router, with per-workspace isolation (the rig + session land in the new
//! tenant's schema / log, not the default).
//!
//! `call_tool` itself cannot be driven in-process (its rmcp `RequestContext`
//! wraps a crate-private `Peer`), so the test targets the same `DomainRouter`
//! `call_tool` delegates to — the seam under test.
//!
//! Gated on `GT_PG_URL` (the workspace + rig handlers are Postgres-backed); a
//! no-op without it, like the other contract suites.

use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::PgPool;

use gt_composition::mcp::{AgentHandler, EventLog, RigHandler, WorkspaceHandler, WsPools};
use gt_mcp_server::{DomainCtx, DomainRouter};
use gt_module::GtModule;
use tempfile::TempDir;

/// The tenant this E2E provisions and routes the rig + session into.
const WS: &str = "e2e-dispatch";

/// Dispatch one tool through the router, asserting a handler owned it.
async fn call(router: &DomainRouter, tool: &str, workspace: Option<&str>, args: Value) -> Value {
    router
        .dispatch(tool, DomainCtx { workspace, actor: "e2e", args })
        .await
        .unwrap_or_else(|e| panic!("{tool} dispatch errored: {e}"))
        .unwrap_or_else(|| panic!("{tool}: no handler owned the namespace"))
}

#[tokio::test]
async fn create_workspace_rig_and_session_via_dispatch() {
    let Some(url) = std::env::var("GT_PG_URL").ok() else {
        eprintln!("GT_PG_URL unset; skipping hq-mcp-dispatch.8 E2E");
        return;
    };
    let pool = PgPool::connect(&url).await.expect("connect postgres");

    // --- Schema preconditions (a deploy/migration concern; applied inline here) ---
    // workspaces catalog + the per-workspace schema provisioning function.
    for m in gt_store_pg::workspace_migrations() {
        sqlx::raw_sql(&m.sql).execute(&pool).await.expect("apply workspace migration");
    }
    // The rigs table in the ws_default template, so the clone carries it.
    let rig_mig = gt_rig::RigsModule.migrations();
    sqlx::raw_sql(&rig_mig[0].sql).execute(&pool).await.expect("apply rigs migration");
    // Clean any leak from a prior run, then provision the tenant schema (clones
    // ws_default's structure — including rigs — into ws_e2e_dispatch).
    let schema = gt_store_pg::schema_for(WS);
    sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM workspaces WHERE id = $1").bind(WS).execute(&pool).await.ok();
    sqlx::query("SELECT gt_create_workspace_schema($1)")
        .bind(WS)
        .execute(&pool)
        .await
        .expect("provision tenant schema");

    // --- The router, wired exactly as the binary does ---
    let event_dir = TempDir::new().unwrap();
    let router = DomainRouter::new()
        .register(Arc::new(WorkspaceHandler::new(pool.clone())))
        .register(Arc::new(RigHandler::new(Arc::new(WsPools::new(url)))))
        .register(Arc::new(AgentHandler::new(Arc::new(EventLog::new(Some(
            event_dir.path().to_path_buf(),
        ))))));

    // --- 1. Create the workspace (shared public.workspaces catalog) ------------
    let created = call(
        &router,
        "workspace.create",
        None,
        json!({ "id": WS, "name": "E2E Dispatch" }),
    )
    .await;
    assert_eq!(created["status"], "active");

    // --- 2. Register a rig IN that workspace (ws_e2e_dispatch.rigs) -------------
    let rig = call(
        &router,
        "rig.add",
        Some(WS),
        json!({ "name": "plane", "prefix": "pl", "git_url": "git@x:y/plane.git", "default_branch": "main" }),
    )
    .await;
    assert_eq!(rig["ok"], true);

    // --- 3. Spawn a session IN that workspace (event log under <ws>/) ----------
    let session = call(
        &router,
        "agent.spawn",
        Some(WS),
        json!({ "session": "p1", "rig": "plane" }),
    )
    .await;
    assert_eq!(session["event"], "agent.spawned");

    // --- 4. Read all three back through the same router ------------------------
    let ws_info = call(&router, "workspace.info", None, json!({ "id": WS })).await;
    assert_eq!(ws_info["name"], "E2E Dispatch");

    let rig_info = call(&router, "rig.info", Some(WS), json!({ "name": "plane" })).await;
    assert_eq!(rig_info["prefix"], "pl");

    let sess_info = call(&router, "agent.info", Some(WS), json!({ "session": "p1" })).await;
    assert_eq!(sess_info["rig"], "plane");
    assert_eq!(sess_info["state"], "spawned");

    // --- Isolation: the rig landed in the tenant schema, not the default -------
    let default_rigs = call(&router, "rig.list", None, json!({})).await;
    assert!(
        !default_rigs["rigs"].as_array().unwrap().iter().any(|r| r["name"] == "plane"),
        "the rig must live in the tenant schema, not ws_default",
    );

    // --- Teardown ---
    sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).execute(&pool).await.ok();
    sqlx::query("DELETE FROM workspaces WHERE id = $1").bind(WS).execute(&pool).await.ok();
}
