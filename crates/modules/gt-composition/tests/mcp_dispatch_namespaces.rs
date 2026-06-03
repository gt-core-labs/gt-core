//! Domain dispatch coverage (`hq-mcp-test.2`): the 7 domain namespaces respond.
//!
//! The server routes any non-`meta.*`/`issues.*` tool to the [`DomainRouter`];
//! `Ok(None)` from the router is the "unknown tool" signal (no handler owns the
//! namespace). This suite proves every ported domain namespace is *owned* — a
//! dispatch returns `Ok(Some(_))` (the handler ran) or `Err(_)` (the handler ran
//! and failed), never `Ok(None)`.
//!
//! Two layers:
//! - **Env-free** — the four event-log-backed namespaces (merge/convoy/agent/
//!   quota) build over a tempdir log and a representative `*.list` read replays
//!   the empty log end to end. Runs in host CI with no sidecar.
//! - **`GT_PG_URL`-gated** — the full production router (all 7, mirroring the
//!   `gt-mcp-server` binary's `build_domain_router`) asserts the exact namespace
//!   set and that each namespace responds.

use std::sync::Arc;

use gt_composition::mcp::{
    AgentHandler, ConvoyHandler, EventLog, GraphHandler, MergeHandler, QuotaHandler, RigHandler,
    WorkspaceHandler, WsPools,
};
use gt_graphindex::GraphifyIndexer;
use gt_mcp_server::{DomainCtx, DomainRouter};
use serde_json::json;
use tempfile::TempDir;

/// The 7 domain dispatch namespaces the production server registers.
const EXPECTED_NAMESPACES: &[&str] =
    &["agent", "convoy", "graph", "merge", "quota", "rig", "workspace"];

/// The four namespaces whose state is a replayed event stream — buildable
/// without a database, so they anchor the env-free coverage.
const EVENT_LOG_NAMESPACES: &[&str] = &["agent", "convoy", "merge", "quota"];

fn ctx(args: serde_json::Value) -> DomainCtx<'static> {
    DomainCtx { workspace: None, actor: "mcp-test", args }
}

/// A dispatch that proves the namespace is *owned*: not `Ok(None)`.
async fn responds(router: &DomainRouter, tool: &str) -> bool {
    !matches!(router.dispatch(tool, ctx(json!({}))).await, Ok(None))
}

#[tokio::test]
async fn event_log_namespaces_respond_over_an_empty_log() {
    let tmp = TempDir::new().expect("tempdir");
    let log = Arc::new(EventLog::new(Some(tmp.path().to_path_buf())));

    let router = DomainRouter::new()
        .register(Arc::new(MergeHandler::new(log.clone())))
        .register(Arc::new(ConvoyHandler::new(log.clone())))
        .register(Arc::new(AgentHandler::new(log.clone())))
        .register(Arc::new(QuotaHandler::new(log.clone())));

    assert_eq!(
        router.namespaces(),
        EVENT_LOG_NAMESPACES,
        "the four event-log handlers own exactly their namespaces"
    );

    // Each `*.list` replays the (empty) log and returns a payload — the namespace
    // responds end to end, not merely registered.
    for ns in EVENT_LOG_NAMESPACES {
        let tool = format!("{ns}.list");
        let out = router
            .dispatch(&tool, ctx(json!({})))
            .await
            .unwrap_or_else(|e| panic!("{tool} errored over an empty log: {e}"));
        assert!(out.is_some(), "{tool}: namespace unowned (Ok(None))");
    }

    // A namespace no handler owns is still the unknown-tool signal.
    assert!(
        !responds(&router, "workspace.list").await,
        "unregistered namespace must report Ok(None)"
    );
}

/// hq-mcp-native.3: every tool a handler *advertises* (`descriptors()`) is one the
/// router *dispatches* — the discovery↔dispatch bridge the old contract QA missed.
/// That QA round-tripped a reconstructed `issues+meta` harvest against itself
/// (`help_names == served_names`, both from the same `harvest_tools()`), so it could
/// never catch a domain tool that dispatched but was never advertised. Here both
/// sides come from one router instance, so they cannot be reconciled by construction.
#[tokio::test]
async fn advertised_domain_tools_are_dispatchable() {
    let tmp = TempDir::new().expect("tempdir");
    let log = Arc::new(EventLog::new(Some(tmp.path().to_path_buf())));
    let router = DomainRouter::new()
        .register(Arc::new(MergeHandler::new(log.clone())))
        .register(Arc::new(ConvoyHandler::new(log.clone())))
        .register(Arc::new(AgentHandler::new(log.clone())))
        .register(Arc::new(QuotaHandler::new(log.clone())));

    let descriptors = router.descriptors();
    assert!(!descriptors.is_empty(), "the registered handlers must advertise tools");

    for tool in &descriptors {
        // The advertised tool's namespace is owned — dispatch is not Ok(None). An
        // empty payload makes a mutation fail validation (Err, owned), never appends.
        assert!(
            responds(&router, &tool.name).await,
            "advertised `{}` is not dispatchable (Ok(None))",
            tool.name
        );
        // A complete tools/list / meta.help entry: non-empty description + object schema.
        assert!(!tool.description.is_empty(), "`{}` has no description", tool.name);
        assert_eq!(
            tool.input_schema["type"], "object",
            "`{}` input schema is not an object",
            tool.name
        );
    }

    // The four event-log namespaces each surface their representative tools.
    let names: std::collections::BTreeSet<&str> =
        descriptors.iter().map(|t| t.name.as_str()).collect();
    for expected in ["merge.submit", "convoy.launch", "agent.spawn", "quota.sample"] {
        assert!(names.contains(expected), "`{expected}` missing from the advertised set");
    }
}

#[tokio::test]
async fn all_seven_domain_namespaces_registered_and_respond() {
    let Some(pg_url) = std::env::var("GT_PG_URL").ok() else {
        eprintln!("GT_PG_URL unset — skipping hq-mcp-test.2 full 7-namespace proof");
        return;
    };
    let pool = sqlx::PgPool::connect(&pg_url).await.expect("connect postgres");
    // Catalog + per-workspace schema scaffolding the PG handlers read against.
    for m in gt_store_pg::workspace_migrations() {
        sqlx::raw_sql(&m.sql).execute(&pool).await.expect("apply workspace migration");
    }

    let tmp = TempDir::new().expect("tempdir");
    let log = Arc::new(EventLog::new(Some(tmp.path().to_path_buf())));
    let ws_pools = Arc::new(WsPools::new(pg_url));

    // Mirror the binary's `build_domain_router` registration set.
    let router = DomainRouter::new()
        .register(Arc::new(WorkspaceHandler::new(pool.clone())))
        .register(Arc::new(RigHandler::new(ws_pools.clone())))
        .register(Arc::new(MergeHandler::new(log.clone())))
        .register(Arc::new(ConvoyHandler::new(log.clone())))
        .register(Arc::new(AgentHandler::new(log.clone())))
        .register(Arc::new(QuotaHandler::new(log.clone())))
        .register(Arc::new(GraphHandler::new(
            log.clone(),
            Arc::new(GraphifyIndexer::new()),
        )));

    assert_eq!(
        router.namespaces(),
        EXPECTED_NAMESPACES,
        "the production router owns exactly the 7 domain namespaces"
    );

    // Every namespace responds to a representative read verb (owned, not Ok(None)).
    for ns in EXPECTED_NAMESPACES {
        let tool = format!("{ns}.list");
        assert!(
            responds(&router, &tool).await,
            "namespace `{ns}` did not respond to `{tool}` (Ok(None))"
        );
    }
}
