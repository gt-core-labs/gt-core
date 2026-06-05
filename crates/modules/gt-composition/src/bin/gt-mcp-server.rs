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
//! - `GT_MCP_ALLOWED_HOSTS` — extra `Host` authorities the /mcp transport accepts, appended to
//!   the loopback defaults (comma-separated). Set the served domain here for a public deploy
//!   behind a reverse proxy; unset ⇒ loopback-only.
//! - `GT_MCP_ACTOR` — scope actor, default `mcp-local`.
//! - `GT_MCP_SCOPE_CONFIG` — RBAC TOML/JSON path; unset ⇒ deny-by-default.
//! - `GT_REPO_DIR` — gt-core checkout whose `main` tree backs surface validation
//!   (S3, hq-core-mcp.9); unset ⇒ surface existence checks are skipped.
//! - `GT_DOLT_BASE_URL` — multi-tenant routing (hq-mt-routing.5); unset ⇒
//!   single-tenant on `GT_DOLT_URL`.
//! - `GT_PG_AUDIT_URL` — durable Postgres audit sink; unset ⇒ in-memory.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use gt_audit::{AuditSink, InMemoryAudit};
use gt_composition::mcp::{
    AgentHandler, AuditHandler, ConvoyHandler, DocumentsHandler, EventLog, EventLogMerges,
    EventLogQuota, GraphHandler, MergeHandler, PgDocumentsResource, PgRigPrefixes,
    PgWorkspaceStatus, QuotaHandler, RigHandler, WorkspaceHandler, WsPoolRigs, WsPools,
};
use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_store_blob::BlobStore;
use gt_graphindex::GraphifyIndexer;
use gt_composition::auth::{authenticate, AuthState, SharedAuthenticator};
use gt_composition::denial_audit::audit_denials;
use gt_composition::scope_bridge::bridge_scopes;
use gt_composition::stream::{feed_router, FeedState};
use gt_auth::{
    auth_router, AuthState as LoginState, InMemoryRefreshStore, JwtAuthenticator, JwtMinter, PgUsers,
    SameSite,
};
use gt_store_pg::WorkspacePool;
// Domain REST modules + their `with_http` state (hq-fe-api-mount.1): the bin mounts each
// crate's `register_routes` so the FE reaches every namespace over authenticated HTTP.
use gt_agent::{AgentApiState, AgentModule};
use gt_documents::{DocumentsApiState, DocumentsModule};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_issues::{IssuesApiState, IssuesModule};
use gt_merge::{MergeApiState, MergeModule};
use gt_mcp_server::{
    health, DocumentsResource, DomainRouter, HealthState, IssuesServer, PgAuditSink,
    WorkspaceRigPrefixes, WorkspaceStatusGate, WorkspaceStores,
};
use gt_meta::{MetaApiState, MetaModule};
use gt_module::RootBuilder;
use gt_quota::{QuotaApiState, QuotaModule};
use gt_rig::{RigApiState, RigsModule};
use gt_workspace::{PgWorkspaces, WorkspaceApiState, WorkspaceModule};
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

    // Tools + routes: harvest the issues module's descriptors AND its REST router through the
    // kernel builder — the composition root never hand-lists tools or hand-wires a module's
    // routes (docs/03 rule 3). The module carries the live store + attribution actor so its
    // `register_routes` (hq-auth-routes.2) can dispatch to the same handlers as the MCP tools;
    // the builder mounts them at `/api/v1/issues` behind the issues.read/issues.write guard.
    let issues_api = IssuesApiState::new(store.clone(), actor.clone());
    let root = RootBuilder::new()
        .module(IssuesModule::with_http(issues_api))
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
    // Clones for the meta REST surface (hq-fe-api-mount.2), captured before `store`/`tools`
    // move into the server: `meta.report-gap` mints into the same Dolt store, and `meta.help`
    // serves the full tools/list — the issues+meta descriptors here, extended below with the
    // domain handlers' descriptors so the REST `/help` matches the MCP `meta.help` exactly.
    let meta_store = store.clone();
    let meta_base_tools = tools.clone();
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
    // The full served tools/list for meta.help (hq-fe-api-mount.2): issues+meta descriptors
    // plus every domain handler's, the same set `with_domains` folds into the MCP `tools/list`
    // just below — captured here, before `domains` moves into the server, so REST `/help` and
    // MCP `meta.help` return the identical payload.
    let meta_tools: Vec<_> = {
        let mut t = meta_base_tools;
        t.extend(domains.descriptors());
        t
    };
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

    // Streamable-HTTP Host allow-list (rmcp's DNS-rebinding guard). The default only
    // accepts loopback authorities (localhost/127.0.0.1/::1), so a public deploy behind a
    // reverse proxy — where the inbound `Host` is the served domain — would have every /mcp
    // request rejected. GT_MCP_ALLOWED_HOSTS (comma-separated host or host:port authorities)
    // is APPENDED to the loopback defaults, so local clients keep working and the deploy adds
    // its own domain (e.g. `gt.codecsrayo.com`). Unset ⇒ loopback-only, exactly as before.
    // The RS256 verifier, built once and shared by the MCP transport's auth, the SSE feed's
    // cookie auth, and the REST auth chain below. Env-gated (GT_JWT_RS256_KEYS /
    // GT_JWT_RS256_PUBLIC_KEY_FILE): a deploy that configures no public key gets no auth at all
    // (legacy X-Actor MCP + MCP/ops only), exactly as before — enabling auth is an opt-in env.
    let verifier: Option<SharedAuthenticator> = match JwtAuthenticator::from_env() {
        Ok(v) => Some(Arc::new(v)),
        Err(e) => {
            eprintln!("[gt-mcp-server] no RS256 verifier configured ({e}); MCP/REST/cookie auth off");
            None
        }
    };

    // Authenticate the /mcp transport too (hq-mcp-cookie-auth): with a verifier wired, every MCP
    // call must carry a valid RS256 access JWT — `Authorization: Bearer` OR the `gt_web_token`
    // cookie — and its scope is derived from the token's claims, not the open X-Actor/dev path.
    // Without a verifier the server keeps the legacy X-Actor behaviour (loopback dev).
    if let Some(v) = &verifier {
        service = service.with_authenticator(v.clone());
        eprintln!(
            "[gt-mcp-server] /mcp requires auth (RS256 bearer or gt_web_token cookie; claim-scoped)"
        );
    }

    // Streamable-HTTP Host allow-list (rmcp's DNS-rebinding guard). The default only
    // accepts loopback authorities (localhost/127.0.0.1/::1), so a public deploy behind a
    // reverse proxy — where the inbound `Host` is the served domain — would have every /mcp
    // request rejected. GT_MCP_ALLOWED_HOSTS (comma-separated host or host:port authorities)
    // is APPENDED to the loopback defaults, so local clients keep working and the deploy adds
    // its own domain (e.g. `gt.codecsrayo.com`). Unset ⇒ loopback-only, exactly as before.
    let mut http_config = StreamableHttpServerConfig::default();
    if let Ok(raw) = std::env::var("GT_MCP_ALLOWED_HOSTS") {
        let extra: Vec<String> = raw
            .split(',')
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .collect();
        if !extra.is_empty() {
            eprintln!("[gt-mcp-server] allowed_hosts += {extra:?} (public reverse-proxy deploy)");
            http_config.allowed_hosts.extend(extra);
        }
    }
    let http = StreamableHttpService::new(
        move || Ok(service.clone()),
        Arc::new(LocalSessionManager::default()),
        http_config,
    );

    // Per-workspace SSE event feed (hq-mcp-dispatch.10): GET /stream fans the
    // caller's workspace log out as Server-Sent Events, keyed per (workspace,
    // channel) with Last-Event-ID resume + KeepAlive (docs/02). Merged as its own
    // sub-router so it carries FeedState without disturbing the health state.
    //
    // Cookie auth (hq-fe-api-stream.1): with a verifier, a browser EventSource authenticates
    // via the `gt_web_token` cookie and the feed is keyed by the JWT `workspace` claim; without
    // one, the legacy `X-Workspace` header keys it (no auth), exactly as before.
    let feed = match &verifier {
        Some(v) => {
            eprintln!("[gt-mcp-server] SSE feed on GET /stream (gt_web_token cookie auth, claim-keyed)");
            feed_router(FeedState::with_cookie_auth(event_log.clone(), v.clone(), audit.clone()))
        }
        None => {
            eprintln!("[gt-mcp-server] SSE feed on GET /stream (no verifier; X-Workspace keyed)");
            feed_router(FeedState::new(event_log.clone()))
        }
    };

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

    // REST surface (hq-auth-routes.2): the module routers the kernel builder mounted
    // (`/api/v1/<module>/...`, currently issues) behind the auth + scope-bridge + audit chain.
    // The per-route scope guard is already baked in by `into_router` from each module's
    // Capability; this only adds authentication (RS256 bearer → verified claims) and the
    // claims→CallerScopes bridge the guard consumes, with denials audited.
    //
    // Env-gated on the same RS256 verifier built above (shared with the SSE cookie auth): a
    // deploy that configures no public key serves no REST surface (MCP + ops only), exactly as
    // before — enabling it is an opt-in env, not a behaviour change.
    //
    // Domain REST surface (hq-fe-api-mount.1, .2): mount the modules' `register_routes`
    // (meta/workspace/rig/documents/agent/quota) so the frontend reaches those namespaces over
    // the same authenticated HTTP path as issues — dispatching the identical domain commands the
    // MCP `DomainRouter` serves. Built from a SEPARATE `RootBuilder` than `root`: each module
    // re-registers its MCP tools (RigsHttpModule delegates to RigsModule, MetaHttpModule to
    // MetaModule, &c.), and the server's `with_domains` (+ `root`'s MetaModule) already folded
    // those namespaces into `tools/list`; harvesting this builder's tools too would double-list
    // them. So this builder feeds ONLY `into_router()`/`openapi()` — its `mcp_tools()` is never read.
    //
    // The store-/event-log-backed modules (meta, agent, quota) mount whenever the REST surface is on; the
    // PG-backed ones (workspace, rig, documents) mount only with GT_PG_URL, mirroring
    // `build_domain_router`'s gating — without Postgres they have no backing. The REST backings
    // are independent handles (their own pool / pool-cache / event-log provider) over the same
    // stores, so the MCP dispatch wired above is untouched.
    let agent_root = std::env::var("GT_EVENTLOG_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
    let mut rest = RootBuilder::new()
        // meta REST (hq-fe-api-mount.2): GET /help (full tools/list) + POST /report-gap, both
        // backed by the Dolt store + server actor — always on, like agent/quota, since they need
        // no Postgres. Mounted under /api/v1/meta behind the meta.read/meta.write guard.
        .module(MetaModule::with_http(MetaApiState::new(
            meta_store,
            actor.clone(),
            meta_tools,
        )))
        .module(AgentModule::with_http(AgentApiState::new(agent_root)))
        .module(QuotaModule::with_http(QuotaApiState::new(Arc::new(EventLogQuota::new(
            event_log.clone(),
        )))))
        // merge.* (hq-fe-api-mount.3): the durable backing replays + appends the caller's
        // `merge.*` stream — the same event-sourced board the MCP MergeHandler folds into, so the
        // board survives restart (the actor's in-memory projection does not).
        .module(MergeModule::with_http(MergeApiState::new(Arc::new(EventLogMerges::new(
            event_log.clone(),
        )))));
    if let Ok(pg_url) = std::env::var("GT_PG_URL") {
        let pool = sqlx::PgPool::connect(&pg_url)
            .await
            .context("GT_PG_URL must point at a reachable Postgres (REST backings)")?;
        rest = rest
            .module(WorkspaceModule::with_http(WorkspaceApiState::new(Arc::new(
                PgWorkspaces::new(pool),
            ))))
            .module(RigsModule::with_http(RigApiState::new(Arc::new(WsPoolRigs::new(Arc::new(
                WsPools::new(pg_url.clone()),
            ))))));
        let (blob, bucket) = build_blob_store();
        rest = rest.module(DocumentsModule::with_http(DocumentsApiState::new(
            pg_url,
            blob,
            bucket,
            Extractor::without_ocr(),
            build_embedder(),
        )));
        eprintln!(
            "[gt-mcp-server] REST domain modules: meta + workspace + rig + documents + agent + quota + merge"
        );
    } else {
        eprintln!(
            "[gt-mcp-server] REST domain modules: meta + agent + quota + merge (GT_PG_URL unset → no workspace/rig/documents)"
        );
    }
    let rest_root = rest
        .build()
        .map_err(|e| anyhow::anyhow!("REST module build failed: {e:?}"))?;

    // Fused OpenAPI (hq-fe-api-spec.1): the kernel mounts each loaded module's route spec under
    // its `/api/v1/<module>` prefix and merges them per builder; combine the two builders' docs —
    // `root` (issues; meta is descriptor-only here so it adds no spec) and `rest` (meta + the
    // domain crates) — into one and serve it at the public
    // `/openapi.json`, so a frontend / codegen reads the whole REST surface from a single URL.
    // Rendered once at boot, before `into_router` consumes either Root. Public by design: it
    // describes the contract (a `401` without a token is part of it) and carries no tenant data,
    // so it mounts on the ops router, never behind the RS256 auth chain the `/api/v1/*` routes do.
    let mut openapi_doc = root.openapi();
    openapi_doc.merge(rest_root.openapi());
    let openapi_json: Arc<str> = Arc::from(
        serde_json::to_string(&openapi_doc).context("serialize the merged OpenAPI document")?,
    );
    let app = app.merge(openapi_router(openapi_json));
    eprintln!("[gt-mcp-server] fused OpenAPI on GET /openapi.json (public spec)");

    // Public login surface (hq-web-extras.7): mount `/auth/*` UNGUARDED so login is reachable
    // without a bearer. Gated on a verifier (for the JWKS), a minter (the RS256 signing key),
    // and Postgres (the `users` store). The `authenticate` layer here only *injects* claims when
    // a token IS present — it passes anonymous requests straight through (see `auth::authenticate`)
    // — so `/auth/me` sees the caller while `/auth/login` stays open. Distinct from the
    // `/api/v1/*` chain below, which additionally enforces RBAC scopes.
    let access_ttl = env_u64("GT_AUTH_ACCESS_TTL_SECS", 900);
    let refresh_ttl = env_u64("GT_AUTH_REFRESH_TTL_SECS", 2_592_000);
    let app = match (
        verifier.clone(),
        JwtMinter::from_env().ok(),
        std::env::var("GT_PG_URL").ok(),
    ) {
        (Some(verifier), Some(minter), Some(pg_url)) => {
            // ws_default-scoped pool: `PgUsers` issues unqualified `users`, resolved by search_path.
            let ws_pool = WorkspacePool::connect(&pg_url, "default")
                .await
                .context("auth: connect ws_default Postgres pool")?;
            let pool = ws_pool.pool().clone();
            // Apply the auth migrations (idempotent CREATE ... IF NOT EXISTS) so `users` +
            // `refresh_tokens` exist before the first login. `raw_sql` uses the simple-query
            // protocol, so each multi-statement migration file runs in one round-trip.
            for sql in [
                gt_auth::migrations::CREATE_USERS,
                gt_auth::migrations::CREATE_REFRESH_TOKENS,
            ] {
                sqlx::raw_sql(sql)
                    .execute(&pool)
                    .await
                    .context("auth: apply gt-auth migration")?;
            }
            seed_admin(&pool).await?;
            let jwks = Arc::new(
                JwtAuthenticator::from_env()
                    .context("auth: build JWKS from the public verifier keys")?
                    .jwk_set(),
            );
            // One PgUsers backs both the login port and the user-admin store (hq-web-extras.5).
            let pg_users = Arc::new(PgUsers::new(pool.clone(), "default"));
            let login_state = LoginState {
                login: pg_users.clone(),
                users: Some(pg_users.clone() as Arc<dyn gt_auth::UserStore>),
                minter: Arc::new(minter),
                // MVP refresh store (hq-web-extras.7): in-memory, so refresh tokens do not
                // survive a restart. Durable PgRefreshStore is async-only and does not implement
                // the sync `RefreshStore` trait — wiring it is the follow-up (hq-web-extras.2).
                refresh: Arc::new(InMemoryRefreshStore::new()),
                access_ttl,
                refresh_ttl,
                now: Arc::new(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system clock before the Unix epoch")
                        .as_secs()
                }),
                // Cookie attributes (hq-web-extras.1): Secure on by default (HTTPS deploy); a
                // cross-site SSR frontend sets GT_AUTH_COOKIE_SAMESITE=None (which forces Secure).
                cookie_secure: cookie_secure(),
                cookie_same_site: cookie_same_site(),
                jwks,
            };
            let auth_app = auth_router(login_state).layer(axum::middleware::from_fn_with_state(
                AuthState::new(verifier, audit.clone()),
                authenticate,
            ));
            eprintln!(
                "[gt-mcp-server] login surface on /auth/* (public; access {access_ttl}s / refresh {refresh_ttl}s, in-memory refresh)"
            );
            app.merge(auth_app)
        }
        _ => {
            eprintln!(
                "[gt-mcp-server] login surface off (needs RS256 verifier + GT_JWT_RS256_PRIVATE_KEY_FILE + GT_PG_URL)"
            );
            app
        }
    };

    let module_routes = root.into_router().merge(rest_root.into_router());
    let app = match verifier {
        Some(verifier) => {
            let guarded = module_routes
                // Innermost-first: the kernel scope guard (inside `module_routes`) runs last;
                // bridge_scopes feeds it CallerScopes; audit_denials observes the guard's verdict;
                // authenticate (outermost) verifies the bearer token and injects the claims.
                .layer(axum::middleware::from_fn(bridge_scopes))
                .layer(axum::middleware::from_fn_with_state(audit.clone(), audit_denials))
                .layer(axum::middleware::from_fn_with_state(
                    AuthState::new(verifier, audit.clone()),
                    authenticate,
                ));
            eprintln!(
                "[gt-mcp-server] REST surface on /api/v1/* behind RS256 auth + RBAC scope guard"
            );
            app.merge(guarded)
        }
        // No verifier (the reason was logged where it was built): MCP + ops only.
        None => app,
    };

    // CORS (hq-web-extras.3): off by default — a same-origin proxy needs none. When
    // GT_CORS_ALLOWED_ORIGINS lists origins, a credentialed CORS layer wraps the whole app so a
    // cross-site browser can call /api/v1/* and open the /stream EventSource with cookies.
    let app = match cors_layer() {
        Some(cors) => {
            eprintln!("[gt-mcp-server] CORS on (credentialed) for GT_CORS_ALLOWED_ORIGINS");
            app.layer(cors)
        }
        None => app,
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!(
        "[gt-mcp-server] http transport on http://{}{MCP_PATH}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Serve the kernel-merged OpenAPI document as the public `GET /openapi.json`
/// (hq-fe-api-spec.1). `spec` is the JSON pre-rendered once at boot and shared across
/// requests, so each call is a cheap clone, not a re-serialize. Public by design — it
/// describes the REST contract and holds no tenant data — so it mounts on the ops router
/// beside `/health`, never behind the RS256 auth chain the `/api/v1/*` surface carries.
fn openapi_router(spec: Arc<str>) -> Router {
    Router::new().route(
        "/openapi.json",
        get(move || {
            let spec = spec.clone();
            async move {
                ([(axum::http::header::CONTENT_TYPE, "application/json")], spec.to_string())
            }
        }),
    )
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

/// Ensure the default admin user exists in `ws_default.users` (hq-web-extras.4). Idempotent:
/// `ON CONFLICT (email) DO NOTHING`, and the password is argon2-hashed at boot — never stored
/// plain. Skipped (with a log line) unless BOTH `GT_ADMIN_EMAIL` and `GT_ADMIN_PASSWORD` are
/// set, so no insecure default credential is ever baked into a deploy. Scopes are `["*"]`: the
/// RBAC scope guard treats the wildcard as allow-all, so the seeded admin reaches every route.
async fn seed_admin(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let (email, password) = match (
        std::env::var("GT_ADMIN_EMAIL"),
        std::env::var("GT_ADMIN_PASSWORD"),
    ) {
        (Ok(e), Ok(p)) if !e.is_empty() && !p.is_empty() => (e, p),
        _ => {
            eprintln!(
                "[gt-mcp-server] admin seed skipped (set GT_ADMIN_EMAIL + GT_ADMIN_PASSWORD)"
            );
            return Ok(());
        }
    };
    let hash = gt_auth::password::hash_password(&password)
        .map_err(|e| anyhow::anyhow!("hash admin password: {e}"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the Unix epoch")
        .as_secs() as i64;
    let scopes = vec!["*".to_string()];
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, scopes, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $5) ON CONFLICT (email) DO NOTHING",
    )
    .bind("admin")
    .bind(&email)
    .bind(&hash)
    .bind(&scopes)
    .bind(now)
    .execute(pool)
    .await
    .context("seed default admin user")?;
    eprintln!("[gt-mcp-server] default admin ensured: {email}");
    Ok(())
}

/// Parse a `u64` env var, falling back to `default` when it is unset or unparseable.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The `Secure` flag for the auth cookies (hq-web-extras.1). Defaults to `true` (HTTPS deploy);
/// `GT_AUTH_COOKIE_SECURE=false` opts a plain-http local dev out. `SameSite=None` forces it on
/// regardless, since browsers drop a `None` cookie that is not also `Secure`.
fn cookie_secure() -> bool {
    if cookie_same_site() == SameSite::None {
        return true;
    }
    !matches!(
        std::env::var("GT_AUTH_COOKIE_SECURE").ok().as_deref(),
        Some("false" | "0" | "no")
    )
}

/// The `SameSite` attribute for the auth cookies. `GT_AUTH_COOKIE_SAMESITE` ∈ {strict,lax,none},
/// default `lax` (good for a same-origin deploy); `none` is for a cross-site SSR frontend.
fn cookie_same_site() -> SameSite {
    match std::env::var("GT_AUTH_COOKIE_SAMESITE")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "strict" => SameSite::Strict,
        "none" => SameSite::None,
        _ => SameSite::Lax,
    }
}

/// Build the credentialed CORS layer (hq-web-extras.3) from `GT_CORS_ALLOWED_ORIGINS` — a
/// comma-separated list of exact origins. Returns `None` when unset/empty so a same-origin
/// proxy deploy carries no CORS. Credentialed CORS forbids `*`, so origins/methods/headers are
/// all explicit lists.
fn cors_layer() -> Option<tower_http::cors::CorsLayer> {
    use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
    use axum::http::{HeaderValue, Method};
    use tower_http::cors::{AllowOrigin, CorsLayer};

    let raw = std::env::var("GT_CORS_ALLOWED_ORIGINS").ok()?;
    let origins: Vec<HeaderValue> = raw
        .split(',')
        .map(str::trim)
        .filter(|o| !o.is_empty())
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    if origins.is_empty() {
        return None;
    }
    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_credentials(true)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([AUTHORIZATION, CONTENT_TYPE, ACCEPT]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use tower::ServiceExt; // `oneshot`

    /// `GET /openapi.json` serves the pre-rendered spec verbatim, as `application/json`,
    /// with no auth — it is the public REST contract a frontend / codegen consumes.
    #[tokio::test]
    async fn openapi_json_served_public_as_json() {
        let spec: Arc<str> = Arc::from(r#"{"openapi":"3.1.0","paths":{}}"#);
        let resp = openapi_router(spec.clone())
            .oneshot(Request::builder().uri("/openapi.json").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "application/json");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        // Served byte-for-byte, and still valid JSON.
        assert_eq!(&bytes[..], spec.as_bytes());
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_ok());
    }
}
