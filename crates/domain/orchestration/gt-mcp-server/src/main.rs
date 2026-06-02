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
//! - `GT_REPO_DIR` — gt-core checkout whose `main` tree backs surface validation
//!   (S3, hq-core-mcp.9); unset ⇒ surface existence checks are skipped.

mod dispatch;
mod git_tree;
mod pg_audit;
mod server;
mod workspace;

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
use workspace::WorkspaceStores;

/// Path the MCP endpoint mounts at (mirrors the gastown gt-mcp).
const MCP_PATH: &str = "/mcp";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dolt_url = std::env::var("GT_DOLT_URL")
        .context("GT_DOLT_URL is required (e.g. mysql://gastown@127.0.0.1:3307/hq)")?;
    let bind = std::env::var("GT_MCP_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:8765".into());
    let actor = std::env::var("GT_MCP_ACTOR").unwrap_or_else(|_| "mcp-local".into());
    // S3 surface validation (hq-core-mcp.9): the gt-core checkout whose `main`
    // tree create/update validate `planned:false` surface paths against. Unset ⇒
    // no checkout (e.g. the live container), so surface validation is skipped.
    let repo_dir = std::env::var("GT_REPO_DIR").ok().map(std::path::PathBuf::from);

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

    // Scope: the boot actor's scope is the default; when an RBAC config is wired
    // the server also resolves per-connection scopes from the X-Actor header
    // (hq-core-mcp.6), so several actors share one server with distinct allow-lists.
    let (default_scope, rbac) = match std::env::var("GT_MCP_SCOPE_CONFIG") {
        Ok(path) => {
            let cfg = Arc::new(RbacConfig::load(&path)?);
            eprintln!(
                "[gt-mcp-server] RBAC config loaded; per-connection X-Actor scope resolution on (default actor '{actor}')"
            );
            (Scope::from_rbac(&cfg, &actor), Some(cfg))
        }
        Err(_) => {
            eprintln!(
                "[gt-mcp-server] GT_MCP_SCOPE_CONFIG unset; actor '{actor}' gets a closed scope (deny all), no per-connection resolution"
            );
            (Scope::denied(&actor), None)
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
    match &repo_dir {
        Some(dir) => eprintln!(
            "[gt-mcp-server] surface validation on; main git tree from {}",
            dir.display()
        ),
        None => eprintln!(
            "[gt-mcp-server] GT_REPO_DIR unset; surface existence checks skipped (accept-all)"
        ),
    }
    let mut service = IssuesServer::new(store, default_scope, rbac, audit, tools, repo_dir);

    // Multi-tenant routing (hq-mt-routing.5): when GT_DOLT_BASE_URL is set, a
    // request's X-Workspace header resolves that tenant's own `hq_<ws>` store per
    // call. Unset ⇒ single-tenant on GT_DOLT_URL exactly as before (the live
    // server's default), so enabling tenancy is an opt-in env, not a behaviour
    // change. The base URL carries server coordinates only; the per-workspace
    // database is selected per tenant.
    match std::env::var("GT_DOLT_BASE_URL") {
        Ok(base) => {
            let stores = WorkspaceStores::from_base_url(&base)
                .context("GT_DOLT_BASE_URL is malformed")?;
            service = service.with_workspaces(Arc::new(stores));
            eprintln!(
                "[gt-mcp-server] multi-tenant routing on; X-Workspace selects hq_<ws> via {base}"
            );
        }
        Err(_) => eprintln!(
            "[gt-mcp-server] GT_DOLT_BASE_URL unset; single-tenant on GT_DOLT_URL (X-Workspace ignored)"
        ),
    }

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
