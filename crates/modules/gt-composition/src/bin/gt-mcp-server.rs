//! `gt-mcp-server` — the gt-core MCP server binary (hq-core-host.3, relocated
//! here by `hq-mcp-dispatch`).
//!
//! The binary lives in `gt-composition` (the `modules` tier) because it wires the
//! per-domain [`DomainHandler`](gt_mcp_server::DomainHandler)s into the server's
//! [`DomainRouter`](gt_mcp_server::DomainRouter), and only the `modules` tier may
//! depend on every `domain/*` crate (docs/03 Rule 4). The orchestration-tier
//! `gt-mcp-server` library owns the transport + the issues/meta dispatch + the
//! router contract; this crate composes it with the domain handlers.
//!
//! Env:
//! - `GT_DOLT_URL` (required) — e.g. `mysql://gastown@127.0.0.1:3307/hq`.
//! - `GT_PG_URL` — Postgres backing the domain handlers (workspace.*, …). Unset ⇒
//!   the domain router is empty, so the server serves issues + meta only.
//! - `GT_MCP_HTTP_BIND` — listen address, default `127.0.0.1:8765`.
//! - `GT_MCP_ACTOR` — scope actor, default `mcp-local`.
//! - `GT_MCP_SCOPE_CONFIG` — RBAC TOML/JSON path; unset ⇒ deny-by-default.
//! - `GT_REPO_DIR` — gt-core checkout whose `main` tree backs surface validation
//!   (S3, hq-core-mcp.9); unset ⇒ surface existence checks are skipped.
//! - `GT_DOLT_BASE_URL` — multi-tenant routing (hq-mt-routing.5); unset ⇒
//!   single-tenant on `GT_DOLT_URL`.
//! - `GT_PG_AUDIT_URL` — durable Postgres audit sink; unset ⇒ in-memory.

use std::sync::Arc;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use gt_audit::{AuditSink, InMemoryAudit};
use gt_composition::mcp::{
    AgentHandler, AuditHandler, ConvoyHandler, DocumentsHandler, EventLog, GraphHandler,
    MergeHandler, PgDocumentsResource, PgRigPrefixes, PgWorkspaceStatus, QuotaHandler, RigHandler,
    WorkspaceHandler, WsPools,
};
use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_store_blob::BlobStore;
use gt_graphindex::GraphifyIndexer;
use gt_composition::stream::{feed_router, FeedState};
use gt_issues::IssuesModule;
use gt_mcp_server::{
    health, DocumentsResource, DomainRouter, HealthState, IssuesServer, PgAuditSink,
    WorkspaceRigPrefixes, WorkspaceStatusGate, WorkspaceStores,
};
use gt_meta::MetaModule;
use gt_module::RootBuilder;
use gt_rbac::{RbacConfig, Scope};
use gt_store_dolt::DoltIssues;

/// Path the MCP endpoint mounts at (mirrors the gastown gt-mcp).
const MCP_PATH: &str = "/mcp";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register the process-global Prometheus counters (hq-mt-deploy.8) before any
    // dispatch can fire, so the per-workspace cost counters (gt_workspace_requests_total,
    // gt_workspace_quota_consumed) record from the first call and surface on /metrics.
    gt_telemetry::metrics::ensure_registered();

    let dolt_url = std::env::var("GT_DOLT_URL")
        .context("GT_DOLT_URL is required (e.g. mysql://gastown@127.0.0.1:3307/hq)")?;
    let bind = std::env::var("GT_MCP_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:8765".into());
    let actor = std::env::var("GT_MCP_ACTOR").unwrap_or_else(|_| "mcp-local".into());
    // S3 surface validation (hq-core-mcp.9): the gt-core checkout whose `main`
    // tree create/update validate `planned:false` surface paths against. Unset ⇒
    // no checkout (e.g. the live container), so surface validation is skipped.
    let repo_dir = std::env::var("GT_REPO_DIR").ok().map(std::path::PathBuf::from);

    // Store: the lifted Dolt issues adapter (hq-core-host.1), on the shared Dolt. gt-core owns
    // the bootstrap (hq-docs follow-up): ensure the target database exists before the pool
    // binds to it (a fresh Dolt volume ships none), so the deploy needs no dolt-init.sql.
    DoltIssues::ensure_database(&dolt_url).await?;
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

    // Scope: the boot actor's scope is the default; when an RBAC config is wired the server
    // also resolves per-connection scopes from the X-Actor header (hq-core-mcp.6). Resolution
    // precedence: an explicit GT_MCP_SCOPE_CONFIG file (operator's bespoke policy) wins; else a
    // built-in named profile via GT_MCP_SCOPE_PROFILE (dev/readonly/closed — policy ships in
    // core, the deploy just picks one); else deny-by-default.
    let rbac_config = resolve_rbac_config()?;
    let (default_scope, rbac) = match rbac_config {
        Some(cfg) => {
            let cfg = Arc::new(cfg);
            eprintln!(
                "[gt-mcp-server] RBAC active; per-connection X-Actor scope resolution on (default actor '{actor}')"
            );
            (Scope::from_rbac(&cfg, &actor), Some(cfg))
        }
        None => {
            eprintln!(
                "[gt-mcp-server] no RBAC config/profile; actor '{actor}' gets a closed scope (deny all)"
            );
            (Scope::denied(&actor), None)
        }
    };

    // Audit: durable Postgres sink when GT_PG_AUDIT_URL is set (survives restart);
    // in-memory otherwise. A PG connect failure degrades to in-memory rather than
    // taking the server down.
    let audit: Arc<dyn AuditSink + Send + Sync> = match std::env::var("GT_PG_AUDIT_URL") {
        Ok(url) => match PgAuditSink::connect(&url).await {
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
    let mut service =
        IssuesServer::new(store, default_scope, rbac, audit.clone(), tools, repo_dir);

    // The per-workspace event log (the event-sourced domains' durable store +
    // the SSE feed's source) is path-partitioned under GT_EVENTLOG_ROOT (default
    // /var/lib/gt-core). Built once, shared by the domain dispatch handlers and the
    // streaming feed route.
    let event_root = std::env::var("GT_EVENTLOG_ROOT").ok().map(std::path::PathBuf::from);
    let event_log = Arc::new(EventLog::new(event_root));

    // Domain dispatch (hq-mcp-dispatch): tool namespaces beyond issues.*/meta.*
    // (workspace.*, rig.*, …) route to PG-backed handlers. Wired only when
    // GT_PG_URL is set; unset ⇒ an empty router, so the server serves issues +
    // meta exactly as before.
    let (domains, rig_prefixes, ws_status, documents) =
        build_domain_router(event_log.clone()).await?;
    // audit.* tails the same audit sink the server records into (hq-mt-ops.3).
    // Registered unconditionally — it reads the in-memory or Postgres sink, so it
    // works even when GT_PG_URL is unset and the rest of the router is empty.
    let domains = domains.register(Arc::new(AuditHandler::new(audit.clone())));
    eprintln!("[gt-mcp-server] audit.tail dispatch on (per-workspace audit trail)");
    service = service.with_domains(Arc::new(domains));
    // Wire issues.create rig-prefix routing (hq-mt-rigs.6) when the PG rig catalog
    // is present; without it the server accepts any bead-id prefix as before.
    if let Some(prefixes) = rig_prefixes {
        service = service.with_rig_prefixes(prefixes);
        eprintln!("[gt-mcp-server] issues.create rig-prefix routing on (per-workspace)");
    }
    // Wire the suspend/archive enforcement gate (hq-mt-bootstrap.8) when the PG
    // workspace catalog is present; without it a suspended/archived tenant's
    // mutations are not blocked, exactly as before.
    if let Some(gate) = ws_status {
        service = service.with_workspace_status(gate);
        eprintln!("[gt-mcp-server] suspend/archive enforcement on (mutations gated by ws status)");
    }
    // Wire the document resource reads (hq-docs-api.3): gt://doc/{id} + the `documents`
    // inline on gt://issue/{id}. Present whenever the PG document store is (GT_PG_URL set).
    if let Some(docs) = documents {
        service = service.with_documents(docs);
        eprintln!("[gt-mcp-server] document resources on (gt://doc/{{id}} + gt://issue docs inline)");
    }

    // Multi-tenant routing (hq-mt-routing.5): when GT_DOLT_BASE_URL is set, a
    // request's X-Workspace header resolves that tenant's own `hq_<ws>` store per
    // call. Unset ⇒ single-tenant on GT_DOLT_URL exactly as before (the live
    // server's default), so enabling tenancy is an opt-in env, not a behaviour
    // change.
    match std::env::var("GT_DOLT_BASE_URL") {
        Ok(base) => {
            let stores =
                WorkspaceStores::from_base_url(&base).context("GT_DOLT_BASE_URL is malformed")?;
            service = service.with_workspaces(Arc::new(stores));
            eprintln!(
                "[gt-mcp-server] multi-tenant routing on; X-Workspace selects hq_<ws> via {base}"
            );
        }
        Err(_) => eprintln!(
            "[gt-mcp-server] GT_DOLT_BASE_URL unset; single-tenant on GT_DOLT_URL (X-Workspace ignored)"
        ),
    }

    // Ops endpoints (hq-mt-routing.8) share the server's workspace resolver to
    // report workspaces_loaded + per-workspace Dolt readiness. Capture it before
    // `service` is moved into the transport factory below.
    let health_state = HealthState::new(service.workspaces());

    let http = StreamableHttpService::new(
        move || Ok(service.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    // Per-workspace SSE event feed (hq-mcp-dispatch.10): GET /stream fans the
    // caller's workspace log out as Server-Sent Events, keyed per (workspace,
    // channel) with Last-Event-ID resume + KeepAlive (docs/02). Merged as its own
    // sub-router so it carries FeedState without disturbing the health state.
    let feed = feed_router(FeedState::new(event_log.clone()));
    eprintln!("[gt-mcp-server] SSE feed on GET /stream (per-workspace, X-Workspace keyed)");

    let app = Router::new()
        .route("/health", get(health::health))
        .route("/readyz", get(health::readyz))
        // Prometheus scrape endpoint (hq-mt-deploy.8): the per-workspace cost
        // counters + the golden event/dead-letter metrics in text exposition format.
        // Ignores the health state, so it composes with the `with_state` below.
        .route("/metrics", get(metrics_text))
        .with_state(health_state)
        .merge(feed)
        .nest_service(MCP_PATH, http);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!(
        "[gt-mcp-server] http transport on http://{}{MCP_PATH}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Prometheus scrape endpoint (hq-mt-deploy.8): render the process-global registry
/// — the per-workspace cost counters plus the golden event/dead-letter metrics — in
/// the text exposition format. An encode failure surfaces as a 500 rather than a
/// silent empty scrape.
async fn metrics_text() -> axum::response::Response {
    use axum::response::IntoResponse;
    match gt_telemetry::metrics::render_text() {
        Ok(body) => (axum::http::StatusCode::OK, body).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Apply the public-schema PG catalog migrations on boot: the `workspaces` table
/// (+ bootstrap default row + the `gt_create_workspace_schema` provisioning
/// function), the `feature` flag-overrides table, and the per-workspace projection
/// tables seeded into the `ws_default` template (currently `rig`). These are the
/// tables the domain dispatch handlers read on the very first call, so a fresh
/// deploy must seed them before serving. `gt_module_migrate::apply` records each
/// migration in the tracking table and skips ones already applied, so this is safe
/// to run on every boot.
///
/// The `rig` migrations are schema-qualified (`CREATE SCHEMA IF NOT EXISTS
/// ws_default` + `ws_default.rigs`), so they bootstrap the very template
/// `gt_create_workspace_schema` clones from. Without them the template has no `rigs`
/// table, every tenant schema cloned from it lacks one, and `rig.*` dispatch fails
/// with `relation "rigs" does not exist` — the gap this seed closes (hq-mcp-test.2).
/// They belong here, not in a per-tenant step: the template is global and the
/// provisioner is structure-only (it never clones a table absent from `ws_default`).
///
/// The module ids (`workspace`, `feature`, `rig`) match the owning modules'
/// namespaces so the tracking rows line up with any future module-driven apply (the
/// workspace schema currently has no `GtModule`, so its id is named explicitly here).
async fn apply_pg_catalog(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    use gt_module::{GtModule, ModuleId};
    use gt_rig::RigsModule;

    let workspace_id = ModuleId::new("workspace").expect("`workspace` is a valid module id");
    let feature_id = ModuleId::new("feature").expect("`feature` is a valid module id");
    let rig_id = ModuleId::new("rig").expect("`rig` is a valid module id");
    let docs_id = ModuleId::new("docs").expect("`docs` is a valid module id");
    let workspace_migs = gt_store_pg::workspace_migrations();
    let feature_migs = gt_store_pg::feature_flags_migrations();
    let rig_migs = RigsModule.migrations();
    // hq-docs-store.1: the per-workspace `documents` template tables (docs/11). Like `rig`,
    // they seed the `ws_default` template so `gt_create_workspace_schema` clones them per tenant.
    let docs_migs = gt_store_pg::docs_migrations();

    let plan: Vec<_> = workspace_migs
        .iter()
        .map(|m| (&workspace_id, m))
        .chain(feature_migs.iter().map(|m| (&feature_id, m)))
        .chain(rig_migs.iter().map(|m| (&rig_id, m)))
        .chain(docs_migs.iter().map(|m| (&docs_id, m)))
        .collect();

    let report = gt_module_migrate::apply(pool, &plan)
        .await
        .context("apply public-schema PG catalog migrations")?;
    eprintln!(
        "[gt-mcp-server] PG catalog migrations: {} applied, {} already present",
        report.applied.len(),
        report.skipped
    );
    Ok(())
}

/// Build the domain dispatch router from `GT_PG_URL`. Unset ⇒ an empty router
/// (issues + meta only). The per-domain `DomainHandler`s (`hq-mcp-dispatch.2..7`)
/// are registered here as they land. `event_log` (the shared, path-partitioned
/// per-workspace log) backs the event-sourced domains — it is the same handle the
/// SSE feed streams from.
async fn build_domain_router(
    event_log: Arc<EventLog>,
) -> anyhow::Result<(
    DomainRouter,
    Option<Arc<dyn WorkspaceRigPrefixes>>,
    Option<Arc<dyn WorkspaceStatusGate>>,
    Option<Arc<dyn DocumentsResource>>,
)> {
    let Ok(pg_url) = std::env::var("GT_PG_URL") else {
        eprintln!(
            "[gt-mcp-server] GT_PG_URL unset; domain dispatch disabled (issues + meta only)"
        );
        return Ok((DomainRouter::new(), None, None, None));
    };
    // The workspace catalog lives in the shared `public` schema, so it uses a
    // plain pool; the per-workspace domains (rig, …) resolve their `ws_<slug>`
    // schema through a search_path-scoped WorkspacePool cache over the same URL.
    let pool = sqlx::PgPool::connect(&pg_url)
        .await
        .context("GT_PG_URL must point at a reachable Postgres")?;
    eprintln!("[gt-mcp-server] domain dispatch: Postgres @ {pg_url}");

    // Self-seed the public-schema catalog on boot (hq-mcp-deploy): the workspace.*
    // handler and the suspend/archive gate need the `workspaces` table (+ its
    // bootstrap default row) and `feature` flag overrides to exist. A fresh deploy
    // ships an empty Postgres, so the server runs the migrations itself rather than
    // depending on an operator step. Idempotent — `apply` skips already-recorded
    // migrations, so a restart against a seeded DB is a no-op.
    apply_pg_catalog(&pool).await?;
    let ws_pools = Arc::new(WsPools::new(pg_url));
    let router = DomainRouter::new()
        .register(Arc::new(WorkspaceHandler::new(pool.clone())))
        .register(Arc::new(RigHandler::new(ws_pools.clone())))
        // A completed merge marks the owning rig's graph stale (hq-graphrig.7).
        .register(Arc::new(
            MergeHandler::new(event_log.clone()).with_rig_pools(ws_pools.clone()),
        ))
        .register(Arc::new(ConvoyHandler::new(event_log.clone())))
        .register(Arc::new(AgentHandler::new(event_log.clone())))
        .register(Arc::new(QuotaHandler::new(event_log.clone())))
        // graph.* read-only queries (hq-graphrig.10): graphify-backed indexer; the
        // warden state (replayed from event_log) resolves rig -> repo_dir.
        .register(Arc::new(GraphHandler::new(
            event_log.clone(),
            Arc::new(GraphifyIndexer::new()),
        )));

    // documents.* dispatch (hq-docs-api.2, docs/11): .md content + binary attachments a model
    // reads as context. The blob store is wired from GT_BLOB_* when set; unset ⇒ md-only
    // (blob attach errors, .md still works). Extraction runs without OCR in the default build
    // (the tesseract OcrEngine is behind the `ocr-tesseract` feature, docs/11).
    let (blob, bucket) = build_blob_store();
    let router = router.register(Arc::new(DocumentsHandler::new(
        ws_pools.clone(),
        blob,
        bucket,
        Extractor::without_ocr(),
        build_embedder(),
    )));
    eprintln!(
        "[gt-mcp-server] domain namespaces: {:?}",
        router.namespaces()
    );
    // The same per-workspace rig catalog backs issues.create prefix routing
    // (hq-mt-rigs.6): a new bead's id prefix must be a registered rig prefix in the
    // caller's workspace (or a reserved prefix).
    let rig_prefixes: Arc<dyn WorkspaceRigPrefixes> = Arc::new(PgRigPrefixes::new(ws_pools.clone()));
    // The suspend/archive gate reads the same `public`-schema workspace catalog the
    // `workspace.*` handler mutates (hq-mt-bootstrap.8), behind a short TTL cache.
    let ws_status: Arc<dyn WorkspaceStatusGate> = Arc::new(PgWorkspaceStatus::new(pool));
    // The same per-workspace pool cache backs the document resource reads (hq-docs-api.3):
    // gt://doc/{id} + the documents inline on gt://issue/{id}.
    let documents: Arc<dyn DocumentsResource> = Arc::new(PgDocumentsResource::new(ws_pools));
    Ok((router, Some(rig_prefixes), Some(ws_status), Some(documents)))
}

/// Resolve the RBAC config from the environment (scope-profiles feature). Precedence:
/// `GT_MCP_SCOPE_CONFIG` (an operator's file — bespoke policy) over `GT_MCP_SCOPE_PROFILE`
/// (a built-in named policy shipped in core) over `None` (deny-by-default). An unknown
/// profile name fails closed with the valid set, rather than silently denying.
fn resolve_rbac_config() -> anyhow::Result<Option<RbacConfig>> {
    if let Ok(path) = std::env::var("GT_MCP_SCOPE_CONFIG") {
        eprintln!("[gt-mcp-server] RBAC from file: {path}");
        return Ok(Some(RbacConfig::load(&path)?));
    }
    if let Ok(profile) = std::env::var("GT_MCP_SCOPE_PROFILE") {
        match RbacConfig::from_profile(&profile)? {
            Some(cfg) => {
                eprintln!("[gt-mcp-server] RBAC from built-in profile: {profile}");
                return Ok(Some(cfg));
            }
            None => {
                anyhow::bail!(
                    "unknown GT_MCP_SCOPE_PROFILE `{profile}`; valid: {:?}",
                    RbacConfig::available_profiles()
                );
            }
        }
    }
    Ok(None)
}

/// Build the semantic-search embedder (hq-docs-search.2). Only the `embeddings-fastembed`
/// build can produce one, and only when `GT_EMBEDDINGS` is set truthy (model load/download is
/// heavy, so it is opt-in). `None` ⇒ `documents.search` stays phase-1 full-text only.
fn build_embedder() -> Option<Arc<dyn Embedder>> {
    #[cfg(feature = "embeddings-fastembed")]
    {
        let on = std::env::var("GT_EMBEDDINGS").map(|v| v != "0" && !v.is_empty()).unwrap_or(false);
        if on {
            match gt_docs_embed::fastembed::FastEmbedder::new() {
                Ok(e) => {
                    eprintln!("[gt-mcp-server] documents semantic search on (fastembed local)");
                    return Some(Arc::new(e));
                }
                Err(err) => {
                    eprintln!("[gt-mcp-server] embedder init failed — {err}; search is full-text only");
                }
            }
        }
    }
    eprintln!("[gt-mcp-server] embeddings off; documents.search is full-text only");
    None
}

/// Build the document blob store from `GT_BLOB_*` (hq-docs-api.2 / hq-docs-deploy.1). Returns
/// `(None, bucket)` when `GT_BLOB_ENDPOINT` is unset — the server then serves `.md` documents
/// but rejects `kind="blob"` attachments. The bucket name is always returned (recorded on a
/// blob row's pointer); it defaults to `gt-documents`.
fn build_blob_store() -> (Option<Arc<BlobStore>>, String) {
    let bucket = std::env::var("GT_BLOB_BUCKET").unwrap_or_else(|_| "gt-documents".into());
    let Ok(endpoint) = std::env::var("GT_BLOB_ENDPOINT") else {
        eprintln!("[gt-mcp-server] GT_BLOB_ENDPOINT unset; documents are .md-only (no blob store)");
        return (None, bucket);
    };
    let region = std::env::var("GT_BLOB_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("GT_BLOB_ACCESS_KEY").unwrap_or_default();
    let secret = std::env::var("GT_BLOB_SECRET_KEY").unwrap_or_default();
    match BlobStore::from_s3(&endpoint, &bucket, &region, &access, &secret) {
        Ok(store) => {
            eprintln!("[gt-mcp-server] documents blob store: S3 @ {endpoint} (bucket {bucket})");
            (Some(Arc::new(store)), bucket)
        }
        Err(e) => {
            eprintln!("[gt-mcp-server] blob store init failed — {e}; documents are .md-only");
            (None, bucket)
        }
    }
}
