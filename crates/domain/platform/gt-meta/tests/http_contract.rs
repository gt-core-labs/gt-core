//! Contract for the meta REST adapter (hq-fe-api-platform.4).
//!
//! Drives [`gt_meta::http::meta_router`] in-process with `tower`'s `oneshot`,
//! proving the REST surface dispatches to the same logic as the MCP tools: `GET
//! /help` echoes the carried tool descriptors in MCP shape, and `POST /report-gap`
//! mints a `hq-gap-<slug>-<ts>` bead in the issues store (visible back through the
//! store) parented under the gaps catalog. The wire mapping (error → status,
//! prefix-free OpenAPI) is unit-covered in `http.rs`; this is the integration proof
//! that the routes are wired.
//!
//! `GET /help` needs no database (it serializes carried state), so it runs
//! unconditionally over a never-connected store. The `report-gap` roundtrip needs a
//! live Dolt server and is skipped unless `GT_DOLT_URL` is set, so host CI without a
//! Dolt sidecar still compiles and runs the help test. Compiled only under the
//! `axum` feature (the adapter's gate).
#![cfg(feature = "axum")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mysql_async::prelude::Queryable;
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_meta::http::{meta_router, MetaApiState};
use gt_module::McpTool;
use gt_store_dolt::DoltIssues;

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

/// A never-connected store is fine for the help route: the handler reads no store.
fn unconnected_store() -> Arc<DoltIssues> {
    Arc::new(DoltIssues::connect("mysql://x@127.0.0.1:1/none").expect("lazy connect"))
}

#[tokio::test]
async fn help_echoes_the_carried_tools_in_mcp_shape() {
    let tools = vec![
        McpTool::new("meta.help.execute", "self-description"),
        McpTool::with_schema(
            "issues.create.execute",
            "create a bead",
            json!({ "type": "object", "properties": { "id": { "type": "string" } } }),
        ),
    ];
    let app = meta_router(MetaApiState::new(unconnected_store(), "test-actor", tools));

    let (status, body) = send(&app, get("/help")).await;
    assert_eq!(status, StatusCode::OK);
    let listed = body["tools"].as_array().expect("tools array");
    assert_eq!(listed.len(), 2);
    // MCP-canonical shape: { name, description, inputSchema }.
    assert_eq!(listed[0]["name"], "meta.help.execute");
    assert_eq!(listed[0]["description"], "self-description");
    assert!(listed[0]["inputSchema"].is_object());
    assert_eq!(listed[1]["name"], "issues.create.execute");
    assert_eq!(listed[1]["inputSchema"]["properties"]["id"]["type"], "string");
}

#[tokio::test]
async fn scopes_derives_the_grantable_catalog_from_advertised_tool_namespaces() {
    // hq-scope-catalog: GET /scopes derives the grantable catalog from SCOPE_VERBS × the namespaces
    // of the SAME carried tool descriptors meta.help serves — via the real router. A domain namespace
    // (memory) appears without anyone hand-enumerating it; the unauthenticated `ping` (no namespace)
    // is skipped.
    let tools = vec![
        McpTool::new("meta.help.execute", "self-description"),
        McpTool::new("memory.save.execute", "save a memory"),
        McpTool::new("issues.create.execute", "create a bead"),
        McpTool::new("ping", "readiness probe"),
    ];
    let app = meta_router(MetaApiState::new(unconnected_store(), "test-actor", tools));

    let (status, body) = send(&app, get("/scopes")).await;
    assert_eq!(status, StatusCode::OK);
    let listed = body["scopes"].as_array().expect("scopes array");

    let scope_strings: Vec<&str> = listed
        .iter()
        .map(|e| e["scope"].as_str().expect("scope string"))
        .collect();
    // The memory namespace surfaces read+write (the regression case) without a hand-written map.
    assert!(scope_strings.contains(&"memory.read"), "{scope_strings:?}");
    assert!(scope_strings.contains(&"memory.write"), "{scope_strings:?}");
    // issues + meta natives are in too; the dot-less `ping` is not a namespace.
    assert!(scope_strings.contains(&"issues.read"));
    assert!(scope_strings.contains(&"meta.read"));
    assert!(!scope_strings.iter().any(|s| s.starts_with("ping.")), "ping has no namespace");

    // Each entry carries the backend-derived label + namespace the UI consumes.
    let mem_read = listed
        .iter()
        .find(|e| e["scope"] == "memory.read")
        .expect("memory.read entry");
    assert_eq!(mem_read["label"], "Read memory");
    assert_eq!(mem_read["namespace"], "memory");
}

/// Create a throwaway DB so a `report-gap` roundtrip never shares a table with a
/// parallel run, then return a router over a freshly-schema'd store on it.
async fn router_on_fresh_db(base: &str, db: &str) -> axum::Router {
    let pool = gt_store_dolt::connect(base).expect("connect");
    let mut conn = pool.get_conn().await.expect("conn");
    conn.query_drop(format!("DROP DATABASE IF EXISTS {db}")).await.expect("drop");
    conn.query_drop(format!("CREATE DATABASE {db}")).await.expect("create");
    let repo = DoltIssues::connect(&format!("{base}/{db}")).expect("connect db");
    repo.ensure_schema().await.expect("schema present");
    meta_router(MetaApiState::new(Arc::new(repo), "test-actor", Vec::<McpTool>::new()))
}

#[tokio::test]
async fn report_gap_mints_a_gap_bead_in_the_store() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping report-gap roundtrip");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_meta_report_gap";
    let app = router_on_fresh_db(&base, db).await;

    // POST /report-gap mints a bead; with no external_ref it parents under the
    // hq-gaps catalog sub-epic (auto-created), so the response names that ref.
    let (status, body) = send(
        &app,
        post_json(
            "/report-gap",
            json!({ "operation": "issues.archive.execute", "notes": "needed for cleanup", "surface": ["crates/gt-issues"] }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "report-gap returns 201: {body}");
    assert_eq!(body["operation"], "issues.archive.execute");
    assert_eq!(body["external_ref"], "hq-gaps");
    let bead = body["bead"].as_str().expect("bead id");
    assert!(bead.starts_with("hq-gap-issues-archive-execute-"), "slug id: {bead}");

    // The bead is really in the store with the seeded fields + the server actor.
    let repo = DoltIssues::connect(&format!("{base}/{db}")).expect("connect");
    let detail = repo.get_detail(bead).await.expect("read").expect("bead present");
    assert_eq!(detail.title, "gap: issues.archive.execute");
    assert_eq!(detail.external_ref.as_deref(), Some("hq-gaps"));
    // The catalog epic was auto-created so the parent ref is never dangling.
    assert!(repo.get_detail("hq-gaps").await.expect("read").is_some(), "catalog epic exists");
}

#[tokio::test]
async fn report_gap_rejects_empty_operation() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping report-gap validation");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_meta_report_gap_empty";
    let app = router_on_fresh_db(&base, db).await;

    let (status, _) = send(&app, post_json("/report-gap", json!({ "operation": "  " }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "empty operation is a 422");
}
