//! `gt-mcp-server` — the gt-core MCP server binary (hq-core-host.3).
//!
//! The transport pillar of the MVP to host the issues tracker inside gt-core and
//! retire the gastown `gt-mcp` bin. It composes the issues module's tool
//! descriptors (through the kernel [`RootBuilder`]) + the lifted Dolt store, and
//! serves them over the rmcp streamable-HTTP transport — pointed at the SAME
//! Dolt the gastown gt-mcp uses, so cutover is a transport swap on shared data.
//!
//! Env:
//! - `GT_DOLT_URL` (required) — e.g. `mysql://gastown@127.0.0.1:3307/hq`.
//! - `GT_MCP_HTTP_BIND` — listen address, default `127.0.0.1:8765`.
//! - `GT_MCP_ACTOR` — scope actor, default `mcp-local`.
//! - `GT_MCP_SCOPE_CONFIG` — RBAC TOML/JSON path; unset ⇒ deny-by-default.

mod dispatch;
mod pg_audit;
mod server;

use std::sync::Arc;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use gt_audit::{AuditSink, InMemoryAudit};
use gt_issues::IssuesModule;
use gt_meta::MetaModule;
use gt_module::RootBuilder;
use gt_rbac::{RbacConfig, Scope};
use gt_store_dolt::DoltIssues;

use server::IssuesServer;

/// Path the MCP endpoint mounts at (mirrors the gastown gt-mcp).
const MCP_PATH: &str = "/mcp";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dolt_url = std::env::var("GT_DOLT_URL")
        .context("GT_DOLT_URL is required (e.g. mysql://gastown@127.0.0.1:3307/hq)")?;
    let bind = std::env::var("GT_MCP_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:8765".into());
    let actor = std::env::var("GT_MCP_ACTOR").unwrap_or_else(|_| "mcp-local".into());

    // Store: the lifted Dolt issues adapter (hq-core-host.1), on the shared Dolt.
    let store = Arc::new(DoltIssues::connect(&dolt_url)?);
    store.ensure_schema().await?;
    eprintln!("[gt-mcp-server] issues: Dolt @ {dolt_url}");

    // Tools: harvest the issues module's descriptors through the kernel builder —
    // the composition root never hand-lists tools (docs/03 rule 3).
    let root = RootBuilder::new()
        .module(IssuesModule)
        .module(MetaModule)
        .build()
        .map_err(|e| anyhow::anyhow!("module build failed: {e:?}"))?;
    let tools: Vec<_> = root.mcp_tools().cloned().collect();
    eprintln!("[gt-mcp-server] {} tools registered", tools.len());

    // Scope: resolve the actor against the RBAC config, or deny-by-default.
    let scope = match std::env::var("GT_MCP_SCOPE_CONFIG") {
        Ok(path) => Scope::from_rbac(&RbacConfig::load(&path)?, &actor),
        Err(_) => {
            eprintln!(
                "[gt-mcp-server] GT_MCP_SCOPE_CONFIG unset; actor '{actor}' gets a closed scope (deny all)"
            );
            Scope::denied(&actor)
        }
    };

    // Audit: durable Postgres sink when GT_PG_AUDIT_URL is set (survives restart);
    // in-memory otherwise. A PG connect failure degrades to in-memory rather than
    // taking the server down.
    let audit: Arc<dyn AuditSink + Send + Sync> = match std::env::var("GT_PG_AUDIT_URL") {
        Ok(url) => match pg_audit::PgAuditSink::connect(&url).await {
            Ok(sink) => {
                eprintln!("[gt-mcp-server] audit: Postgres @ {url}");
                Arc::new(sink)
            }
            Err(e) => {
                eprintln!("[gt-mcp-server] PG audit disabled — {e}; falling back to in-memory");
                Arc::new(InMemoryAudit::new())
            }
        },
        Err(_) => {
            eprintln!("[gt-mcp-server] GT_PG_AUDIT_URL unset; audit is in-memory (lost on restart)");
            Arc::new(InMemoryAudit::new())
        }
    };
    let service = IssuesServer::new(store, scope, audit, tools);

    let http = StreamableHttpService::new(
        move || Ok(service.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    let app = Router::new()
        .nest_service(MCP_PATH, http)
        .route("/health", get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!(
        "[gt-mcp-server] http transport on http://{}{MCP_PATH}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}
