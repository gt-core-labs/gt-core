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
use gt_auth::{
    auth_router, AuthState as LoginState, GlobalLogin, JwtAuthenticator, JwtMinter, PgPatStore,
    PgRefreshStore, PgUsers, SameSite,
};
use gt_claude_hooks::HooksStore;
use gt_composition::auth::{authenticate, AuthState, PatVerifier, SharedAuthenticator};
use gt_composition::denial_audit::audit_denials;
use gt_composition::hooks::{hooks_router, HooksApiState};
use gt_composition::notifications::{notifications_router, NotificationsApiState};
use gt_composition::mcp::{
    AgentHandler, AuditHandler, CompositionTenantProvisioner, ConvoyHandler, DocumentsHandler,
    EventLog, EventLogConvoy, EventLogFeed, EventLogHooks, EventLogIssueSink, EventLogMerges,
    EventLogQuota, EventLogSkills, FsAccountCatalog, GraphHandler, IdentityDoltMeStats,
    MergeHandler, NotifyHandler, PgDocumentsResource, PgRigPrefixes, PgWorkspaceStatus,
    QuotaHandler, RigHandler, WorkspaceHandler, WsPoolRigs, WsPools,
};
use gt_composition::onboard::{onboard_router, OnboardState};
use gt_composition::operator_resource::EventLogOperatorResource;
use gt_composition::scope_bridge::bridge_scopes;
use gt_composition::stream::{feed_router, FeedState};
use gt_composition::terminal::{terminal_router, TerminalState};
use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_graphindex::GraphifyIndexer;
use gt_store_blob::BlobStore;
use gt_store_pg::{schema_for, WorkspacePool};
use sqlx::Row;
// Domain REST modules + their `with_http` state (hq-fe-api-mount.1): the bin mounts each
// crate's `register_routes` so the FE reaches every namespace over authenticated HTTP.
use gt_agent::{AgentApiState, AgentModule};
use gt_composition::system::{load_config, system_router, ArchiveDaemon, SystemApiState};
use gt_documents::{DocumentsApiState, DocumentsModule};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_feed::{FeedApiState, FeedModule};
use gt_issues::{IssuesApiState, IssuesModule, MeApiState, MeModule};
use gt_mcp_server::{
    health, DocumentsResource, DomainRouter, HealthState, IssuesServer, PatAuthenticator,
    PgAuditSink, WorkspaceRigPrefixes, WorkspaceStatusGate, WorkspaceStores,
};
use gt_merge::{MergeApiState, MergeModule};
use gt_meta::{MetaApiState, MetaModule};
use gt_module::RootBuilder;
use gt_orchestration::{ConvoyApiState, ConvoyModule};
use gt_quota::{QuotaApiState, QuotaModule};
use gt_rbac::{RbacConfig, Scope};
use gt_rig::{RigApiState, RigsModule};
use gt_skills::{SkillsApiState, SkillsModule};
use gt_store_dolt::{DoltIssues, WorkspacePools};
use gt_workspace::{PgWorkspaces, WorkspaceApiState, WorkspaceModule};
use tokio::sync::RwLock;

/// Path the MCP endpoint mounts at (mirrors the upstream gt-mcp).
const MCP_PATH: &str = "/mcp";

/// Adapt the composition-tier [`PatVerifier`] to the `/mcp` transport's
/// [`PatAuthenticator`] port (gt-core#6). Both resolve a `gtpat_…` bearer to
/// [`JwtClaims`](gt_auth::JwtClaims) via the same Postgres lookup; the transport just needs
/// the trait from `gt-mcp-server` (the domain crate cannot depend on this `modules` crate, so
/// the adapter lives here). With it wired, a PAT authenticates an MCP `tools/call` /
/// `resources/read` exactly as it already does on the REST surface — instead of being
/// RS256-decoded into `-32600: malformed token: InvalidToken`.
struct McpPatAuth(Arc<dyn PatVerifier>);

#[async_trait::async_trait]
impl PatAuthenticator for McpPatAuth {
    async fn verify(&self, token: &str, now: u64) -> Result<gt_auth::JwtClaims, ()> {
        self.0.verify(token, now).await
    }
}

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
    let repo_dir = std::env::var("GT_REPO_DIR")
        .ok()
        .map(std::path::PathBuf::from);

    // Store: the lifted Dolt issues adapter (hq-core-host.1), on the shared Dolt. gt-core owns
    // the bootstrap (hq-docs follow-up): ensure the target database exists before the pool
    // binds to it (a fresh Dolt volume ships none), so the deploy needs no dolt-init.sql.
    DoltIssues::ensure_database(&dolt_url).await?;
    let store = Arc::new(DoltIssues::connect(&dolt_url)?);
    store.ensure_schema().await?;
    eprintln!("[gt-mcp-server] issues: Dolt @ {dolt_url}");

    // The per-workspace event log (the event-sourced domains' durable store AND the SSE feed's
    // source) is path-partitioned under GT_EVENTLOG_ROOT (default /var/lib/gt-core). Built here
    // — before the issues REST state + the MCP service — so both can emit issue-mutation events
    // into the same log the `GET /stream` feed fans out (hq-issues-sse). The event-sourced
    // domain handlers + the feed route below share this one handle.
    let event_root = std::env::var("GT_EVENTLOG_ROOT")
        .ok()
        .map(std::path::PathBuf::from);
    let event_log = Arc::new(EventLog::new(event_root));
    // The issues tracker is Dolt-backed, not event-sourced, so its mutations never reached the
    // event log — the SSE feed never carried issue movement. This sink closes that gap: it
    // appends every `issues.*` mutation (REST or MCP) to the workspace log, so the tracker moves
    // on `GET /stream?channel=issues`. One sink, shared by both surfaces, so REST and MCP emit the
    // identical event (parity).
    let issue_sink = Arc::new(EventLogIssueSink::new(event_log.clone()));

    // Tools + routes: harvest the issues module's descriptors AND its REST router through the
    // kernel builder — the composition root never hand-lists tools or hand-wires a module's
    // routes (docs/03 rule 3). The module carries the live store + attribution actor so its
    // `register_routes` (hq-auth-routes.2) can dispatch to the same handlers as the MCP tools;
    // the builder mounts them at `/api/v1/issues` behind the issues.read/issues.write guard.
    // Multi-tenant REST routing for `/api/v1/issues` (hq-gap-issues-rest-workspace-routing):
    // when `GT_DOLT_BASE_URL` is set, give the issues REST state its own per-workspace Dolt pool
    // cache so each request resolves its tenant's `hq_<ws>` store from the verified workspace —
    // the REST mirror of the MCP path's claim-scoped routing. Its own cache (not the MCP
    // `WorkspaceStores` instance) keeps this change off the live MCP routing path. Unset ⇒ the
    // REST surface stays single-tenant on `store`, exactly as before.
    let issues_workspaces = std::env::var("GT_DOLT_BASE_URL")
        .ok()
        .map(|base| WorkspacePools::from_url(&base))
        .transpose()
        .context("GT_DOLT_BASE_URL is malformed")?
        .map(Arc::new);
    let mut issues_api = match &issues_workspaces {
        Some(pools) => {
            eprintln!("[gt-mcp-server] issues REST: per-workspace routing on (X-Workspace/claim selects hq_<ws>)");
            IssuesApiState::new(store.clone(), actor.clone()).with_workspaces(pools.clone())
        }
        None => IssuesApiState::new(store.clone(), actor.clone()),
    }
    // SSE feed (hq-issues-sse): a REST mutation publishes its event into the per-workspace log.
    .with_event_sink(issue_sink.clone())
    // operated_by overlay (hq-agent-observability.3): GET /api/v1/issues[/{id}] inlines which
    // agent operates each bead + its skills/hooks, folded from the same per-workspace log the
    // polecat supervisor emits issues.operated/cleared into, so the FE shows the live agent chip.
    .with_operators(Arc::new(EventLogOperatorResource::new(event_log.clone())));
    // S2/S3 git verification on the REST surface (hq-platform-hardening.5) + the
    // `?ready` frontier's git tree (hq-platform-hardening.4): when GT_REPO_DIR is
    // set, wire the same git-backed surface tree + commit inspector the MCP path
    // uses so REST create/update reject a non-existent surface (S3) and REST close
    // verifies the delivering commit (S2). Unset ⇒ the state keeps its accept-all
    // S3 + skipped-S2 defaults, the degraded mode the MCP path runs in with no repo.
    if let Some((surfaces, inspectors)) =
        gt_mcp_server::git_tree::rest_verification_providers(repo_dir.as_deref())
    {
        issues_api = issues_api.with_git_verification(surfaces, inspectors);
        eprintln!(
            "[gt-mcp-server] issues REST: git S2/S3 verification on (surface existence + delivered-commit)"
        );
    } else {
        eprintln!(
            "[gt-mcp-server] issues REST: GT_REPO_DIR unset; S3 accept-all, S2 skipped (degraded)"
        );
    }
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
            eprintln!(
                "[gt-mcp-server] GT_PG_AUDIT_URL unset; audit is in-memory (lost on restart)"
            );
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
    // Clone for the system config surface (hq-system-config): the archive daemon and
    // POST /api/v1/system/archive/run need the same Dolt store the MCP server uses.
    let system_store = store.clone();
    let meta_base_tools = tools.clone();
    // `with_issue_sink` (hq-issues-sse): an `issues.*.execute` over MCP — the agent-driven
    // movement the REST surface alone would miss — publishes its event into the same per-workspace
    // log the SSE feed fans out, so the tracker moves on `GET /stream?channel=issues`.
    let mut service = IssuesServer::new(store, default_scope, rbac, audit.clone(), tools, repo_dir)
        .with_issue_sink(issue_sink.clone());

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
        eprintln!(
            "[gt-mcp-server] document resources on (gt://doc/{{id}} + gt://issue docs inline)"
        );
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
            eprintln!(
                "[gt-mcp-server] no RS256 verifier configured ({e}); MCP/REST/cookie auth off"
            );
            None
        }
    };

    // Authenticate the /mcp transport too (hq-mcp-cookie-auth): with a verifier wired, every MCP
    // call must carry a valid RS256 access JWT — `Authorization: Bearer` OR the `gt_web_token`
    // cookie — and its scope is derived from the token's claims, not the open X-Actor/dev path.
    // Without a verifier the server keeps the legacy X-Actor behaviour (loopback dev).
    if let Some(v) = &verifier {
        service = service.with_authenticator(v.clone());
        // Personal Access Token verifier for the transport (gt-core#6): a `gtpat_…` bearer
        // authenticates the SAME on `/mcp` as it already does on `/api/v1/*` — verified through the
        // PgPatStore PAT port instead of being RS256-decoded into `malformed token: InvalidToken`.
        // The service is finalized + moved into the transport below, well before the `/auth/*` arm
        // builds its own ws_default pool, so this opens a dedicated ws_default pool here (the same
        // lazy pattern /me/stats uses). Gated on GT_PG_URL — without Postgres there is no PAT store,
        // so a PAT is denied (never tried as a JWT), exactly like the REST chain.
        match std::env::var("GT_PG_URL") {
            Ok(pg_url) => match WorkspacePool::connect(&pg_url, "default").await {
                Ok(pat_pool) => {
                    let pat: Arc<dyn PatVerifier> =
                        Arc::new(PgPatStore::new(pat_pool.pool().clone()));
                    service = service.with_pat_authenticator(Arc::new(McpPatAuth(pat)));
                    eprintln!(
                        "[gt-mcp-server] /mcp requires auth (RS256 bearer / gt_web_token cookie / gtpat_ PAT; claim-scoped)"
                    );
                }
                Err(e) => eprintln!(
                    "[gt-mcp-server] /mcp PAT auth off (ws_default pool connect failed: {e}); RS256/cookie only"
                ),
            },
            Err(_) => eprintln!(
                "[gt-mcp-server] /mcp requires auth (RS256 bearer or gt_web_token cookie; claim-scoped; no GT_PG_URL → no PAT)"
            ),
        }
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
            eprintln!(
                "[gt-mcp-server] SSE feed on GET /stream (gt_web_token cookie auth, claim-keyed)"
            );
            feed_router(FeedState::with_cookie_auth(
                event_log.clone(),
                v.clone(),
                audit.clone(),
            ))
        }
        None => {
            eprintln!("[gt-mcp-server] SSE feed on GET /stream (no verifier; X-Workspace keyed)");
            feed_router(FeedState::new(event_log.clone()))
        }
    };

    // Interactive PTY terminal (hq-terminal): GET /api/v1/terminal/ws opens a `/bin/sh` on a
    // pseudo-terminal *inside this process* and bridges it to a browser xterm. It is REMOTE
    // SHELL EXECUTION, so it is mounted only when BOTH a verifier is configured (cookie/bearer
    // auth + `terminal.exec` scope gate) AND `GT_TERMINAL_ENABLE` is truthy — a default deploy
    // serves no terminal (the route 404s). See `gt_composition::terminal`.
    let terminal = match (&verifier, std::env::var("GT_TERMINAL_ENABLE")) {
        (Some(v), Ok(flag)) if matches!(flag.trim(), "1" | "true" | "yes" | "on") => {
            eprintln!(
                "[gt-mcp-server] terminal WS on GET /api/v1/terminal/ws (cookie/bearer auth, scope terminal.exec)"
            );
            // Wire the event log so an interactive session terminal launches `claude` under the
            // active claude account's CLAUDE_CONFIG_DIR (hq-term-dock.4). Also wire the RS256 minter +
            // this server's URL so each role session gets a least-privilege `.gt-config` and its
            // `gt mcp` proxy authenticates as the role (hq-role-mcp). `GT_SELF_URL` overrides the
            // loopback default the role's `gt mcp` dials back into.
            let role_minter = JwtMinter::from_env().ok().map(Arc::new);
            let self_url =
                std::env::var("GT_SELF_URL").unwrap_or_else(|_| format!("http://{bind}"));
            Some(terminal_router(
                TerminalState::new(v.clone(), audit.clone())
                    .with_active_accounts(event_log.clone())
                    .with_role_auth(role_minter, Some(self_url)),
            ))
        }
        _ => {
            eprintln!(
                "[gt-mcp-server] terminal WS off (needs RS256 verifier + GT_TERMINAL_ENABLE=1)"
            );
            None
        }
    };

    // Web claude-account onboarding (hq-quota-onboard-web.4): POST /api/v1/quota/onboard/{start,
    // complete} drives the real `claude auth login` lifecycle (claude now lives IN the image) and
    // registers the captured account into the workspace quota log — the same event the daemon
    // hydrates its rotation keychain from. Mounted only with an RS256 verifier; each call requires
    // the `quota.write` scope (audited on denial). See `gt_composition::onboard`.
    let onboard = verifier.as_ref().map(|v| {
        eprintln!(
            "[gt-mcp-server] onboarding on POST /api/v1/quota/onboard/{{start,complete}} (cookie/bearer auth, scope quota.write)"
        );
        let onboard_eventlog_root = std::env::var("GT_EVENTLOG_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
        let accounts_root = gt_composition::account_dirs::accounts_root(&onboard_eventlog_root);
        onboard_router(OnboardState::new(
            v.clone(),
            audit.clone(),
            event_log.clone(),
            accounts_root,
        ))
    });

    // Global Claude Code hook registry (hq-hooks): GET/POST/DELETE /api/v1/hooks list/register/
    // retire the global hook set the terminal materialises into a launching session's
    // .claude/settings.json (filtered by the session's workspace/rig/role target). Mounted with an
    // RS256 verifier; reads need `hooks.read`, writes `hooks.write` (the admin `*` satisfies both).
    // The portable safety guards (rm -rf /, git push --force/-f) seed the GLOBAL log once, when the
    // registry is empty. See `gt_composition::hooks`.
    let hooks = verifier.as_ref().map(|v| {
        let store = Arc::new(EventLogHooks::new(event_log.clone()));
        if let Ok(reg) = store.registry() {
            if reg.is_empty() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut seeded = 0usize;
                for ev in gt_claude_hooks::safety_guard_hooks(now) {
                    if store.append(ev).is_ok() {
                        seeded += 1;
                    }
                }
                eprintln!("[gt-mcp-server] hooks: seeded {seeded} safety guard(s) into empty global registry");
            }
        }
        eprintln!(
            "[gt-mcp-server] hooks REST on /api/v1/hooks (cookie/bearer auth, scope hooks.read/write)"
        );
        hooks_router(HooksApiState::new(v.clone(), audit.clone(), store))
    });

    // Notifications REST surface: GET/POST /api/v1/notifications + mark-read / delete.
    // Agents (e.g. mayor) POST here to send notifications to the human operator; the web UI
    // polls or streams these via the bell-icon panel. Gated on RS256 verifier + GT_PG_URL.
    // GET = notifications.read, POST/DELETE = notifications.write (admin `*` satisfies both).
    let notifications = match (verifier.as_ref(), std::env::var("GT_PG_URL").ok()) {
        (Some(v), Some(pg_url)) => {
            match sqlx::PgPool::connect(&pg_url).await {
                Ok(notif_pool) => {
                    eprintln!(
                        "[gt-mcp-server] notifications REST on /api/v1/notifications (scope notifications.read/write)"
                    );
                    Some(notifications_router(NotificationsApiState::new(
                        v.clone(),
                        audit.clone(),
                        notif_pool,
                        event_log.clone(),
                    )))
                }
                Err(e) => {
                    eprintln!("[gt-mcp-server] notifications REST off (PG connect failed: {e})");
                    None
                }
            }
        }
        _ => {
            eprintln!(
                "[gt-mcp-server] notifications REST off (needs RS256 verifier + GT_PG_URL)"
            );
            None
        }
    };

    // System config REST surface (hq-system-config): GET/PUT /api/v1/system/config and
    // POST /api/v1/system/archive/run. Scoped to system.read/system.write (admin `*` satisfies both).
    // Spawns the background archive daemon that sweeps old closed issues on a configurable interval.
    let system = verifier.as_ref().map(|v| {
        let config_path = std::env::var("GT_SYSTEM_CONFIG_PATH")
            .map(std::path::PathBuf::from)
            .ok()
            .or_else(|| {
                std::env::var("GT_EVENTLOG_ROOT")
                    .ok()
                    .map(|r| std::path::PathBuf::from(r).with_file_name("system_config.json"))
            });
        let initial_cfg = config_path.as_ref().map(load_config).unwrap_or_default();
        let config = std::sync::Arc::new(RwLock::new(initial_cfg.clone()));
        tokio::spawn(ArchiveDaemon::new(system_store.clone(), config.clone()).run());
        eprintln!(
            "[gt-mcp-server] archive daemon on (interval {}min, archive_after {}d)",
            initial_cfg.interval_minutes,
            initial_cfg.archive_after_days,
        );
        eprintln!(
            "[gt-mcp-server] system config REST on /api/v1/system/* (scope system.read/write)"
        );
        system_router(SystemApiState::new(
            v.clone(),
            audit.clone(),
            system_store.clone(),
            config,
            config_path,
        ))
    });

    // Orphan claude-credential GC (hq-quota-onboard-web.6): the backend is the only always-on
    // process, so it sweeps the accounts root on a timer and reaps dirs no live account points at
    // (a re-onboarded email replaces its dir, a retire drops one, an abandoned /start leaves an
    // empty one). Off when GT_ACCOUNTS_GC_TICK_SECS=0. The grace window spares onboards in flight.
    let gc_tick = env_u64("GT_ACCOUNTS_GC_TICK_SECS", 21_600); // 6h
    if gc_tick > 0 {
        let gc_grace = std::time::Duration::from_secs(env_u64("GT_ACCOUNTS_GC_GRACE_SECS", 7_200));
        let gc_root = std::env::var("GT_EVENTLOG_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
        let gc_accounts_root = gt_composition::account_dirs::accounts_root(&gc_root);
        let gc_log = event_log.clone();
        eprintln!(
            "[gt-mcp-server] account-dir GC on (every {gc_tick}s, grace {}s) at {}",
            gc_grace.as_secs(),
            gc_accounts_root.display()
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(gc_tick));
            loop {
                tick.tick().await;
                // Live accounts = the basenames (<id>) of the registered config_dirs. Single-tenant
                // "default" workspace, where the onboarding flow registers (see onboard::complete).
                let state =
                    gt_composition::replay_quota_state(&gc_log, "default").unwrap_or_default();
                let live: std::collections::HashSet<String> = state
                    .registered
                    .values()
                    .filter_map(|d| {
                        std::path::Path::new(d)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(str::to_string)
                    })
                    .collect();
                let removed = gt_composition::account_dirs::gc_orphan_account_dirs(
                    &gc_accounts_root,
                    &live,
                    gc_grace,
                );
                if !removed.is_empty() {
                    eprintln!(
                        "[gt-mcp-server] account GC reaped {} orphan credential dir(s)",
                        removed.len()
                    );
                }
            }
        });
    }

    let mut app = Router::new()
        .route("/health", get(health::health))
        .route("/readyz", get(health::readyz))
        // Prometheus scrape endpoint (hq-mt-deploy.8): the per-workspace cost
        // counters + the golden event/dead-letter metrics in text exposition format.
        // Ignores the health state, so it composes with the `with_state` below.
        .route("/metrics", get(metrics_text))
        .with_state(health_state)
        .merge(feed)
        .nest_service(MCP_PATH, http);
    if let Some(terminal) = terminal {
        app = app.merge(terminal);
    }
    if let Some(onboard) = onboard {
        app = app.merge(onboard);
    }
    if let Some(hooks) = hooks {
        app = app.merge(hooks);
    }
    if let Some(notifications) = notifications {
        app = app.merge(notifications);
    }
    if let Some(system) = system {
        app = app.merge(system);
    }

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
        // agent.*: wire the orchd dispatch channel (hq-agent-auto-dispatch.1) when GT_CHANNEL_ROOT
        // is set so POST /api/v1/agent with role=polecat+crew drops a dispatch request on the
        // channel the orchd scheduler consumes — making the spawn actually sling a polecat.
        .module(AgentModule::with_http({
            let agent_state = AgentApiState::new(agent_root);
            match std::env::var("GT_CHANNEL_ROOT") {
                Ok(root) => {
                    let name = std::env::var("GT_DISPATCH_CHANNEL")
                        .unwrap_or_else(|_| "dispatch".to_string());
                    let channel_dir = std::path::PathBuf::from(&root).join(&name);
                    eprintln!(
                        "[gt-mcp-server] agent→scheduler bridge on — dispatch channel {root}/{name}"
                    );
                    agent_state.with_dispatch_channel(channel_dir)
                }
                Err(_) => {
                    eprintln!(
                        "[gt-mcp-server] agent→scheduler bridge off — GT_CHANNEL_ROOT unset"
                    );
                    agent_state
                }
            }
        }))
        // quota.*: per-workspace assignment (EventLogQuota) PLUS the deploy-global account pool
        // (FsAccountCatalog over the accounts root) so `/api/v1/quota/catalog` lists onboarded
        // accounts and `/:account/assign` attaches one to the active workspace (hq-quota-ws-accounts).
        .module(QuotaModule::with_http(
            QuotaApiState::new(Arc::new(EventLogQuota::new(event_log.clone()))).with_catalog(
                Arc::new(FsAccountCatalog::new(
                    gt_composition::account_dirs::accounts_root(
                        &std::env::var("GT_EVENTLOG_ROOT")
                            .map(std::path::PathBuf::from)
                            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT)),
                    ),
                )),
            ),
        ))
        // merge.* (hq-fe-api-mount.3): the durable backing replays + appends the caller's
        // `merge.*` stream — the same event-sourced board the MCP MergeHandler folds into, so the
        // board survives restart (the actor's in-memory projection does not).
        .module(MergeModule::with_http(MergeApiState::new(Arc::new(
            EventLogMerges::new(event_log.clone()),
        ))))
        // skills.read + skills.write (hq-web-extras.13 + hq-agent-observability.7): the catalog the
        // FE hydrates, replayed from the caller's `skills.*` stream; with a writer wired, POST /
        // DELETE register/retire skills (skills.write), appending events into that same log so the
        // Knowledge skills tab can be populated. One backing serves both read + write.
        .module({
            let skills = Arc::new(EventLogSkills::new(event_log.clone()));
            // Seed the canonical role catalog into the default workspace's `skills.*` log when it is
            // empty (hq-role-mcp), so a clean deploy on a new machine gives each role a working
            // least-privilege MCP grant out of the box — without it every minted per-role token is
            // scopeless. Empty-check makes it idempotent and never clobbers an operator-curated
            // catalog (the prod machine, already populated via the Knowledge REST surface, is a
            // no-op). See `gt_skills::presets::workspace_seed_events`.
            {
                use gt_skills::{SkillWriter, WorkspaceSkills};
                let ws = std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".to_string());
                if let Ok(cat) = skills.catalog(&ws).await {
                    if cat.is_empty() {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let mut seeded = 0usize;
                        for ev in gt_skills::presets::workspace_seed_events(now) {
                            if skills.append(&ws, ev).await.is_ok() {
                                seeded += 1;
                            }
                        }
                        eprintln!("[gt-mcp-server] skills: seeded {seeded} role-catalog event(s) into empty `{ws}` catalog");
                    }
                }
            }
            SkillsModule::with_http(SkillsApiState::new(skills.clone()).with_writer(skills))
        })
        // feed.read (hq-web-extras.14): read-only activity feed, folded from the caller's whole
        // workspace log. Mounted at /api/v1/feed behind the feed.read guard.
        .module(FeedModule::with_http(FeedApiState::new(Arc::new(
            EventLogFeed::new(event_log.clone()),
        ))))
        // convoy.* (hq-web-extras.16): read + mutate REST surface — the durable backing replays +
        // appends the caller's `convoy.*` stream, the same event-sourced board the MCP
        // ConvoyHandler folds into. Mounted at /api/v1/convoy behind the convoy.read/convoy.write
        // guard (board read + complete-member/fail-member mutations).
        .module(ConvoyModule::with_http(ConvoyApiState::new(Arc::new(
            EventLogConvoy::new(event_log.clone()),
        ))));
    // The public, unauthenticated share-read surface (hq-web-extras.9): set when documents mount,
    // mounted OUTSIDE the /api/v1 auth chain (like /openapi.json). `None` without Postgres.
    let mut public_share: Option<axum::Router> = None;
    if let Ok(pg_url) = std::env::var("GT_PG_URL") {
        let pool = sqlx::PgPool::connect(&pg_url)
            .await
            .context("GT_PG_URL must point at a reachable Postgres (REST backings)")?;
        rest = rest
            // The REST `POST /api/v1/workspace` provisions a fully-usable tenant — PG schema/RBAC
            // + Dolt — exactly as the MCP `workspace.create` tool, via the shared provisioner over
            // the same PG pool + per-workspace Dolt pools (hq-gap-workspace-rest-create-provision).
            .module(WorkspaceModule::with_http(
                WorkspaceApiState::new(Arc::new(PgWorkspaces::new(pool.clone()))).with_provisioner(
                    Arc::new(CompositionTenantProvisioner::new(
                        pool,
                        issues_workspaces.clone(),
                    )),
                ),
            ))
            .module(RigsModule::with_http(RigApiState::new(Arc::new(
                WsPoolRigs::new(Arc::new(WsPools::new(pg_url.clone()))),
            ))));
        let (blob, bucket) = build_blob_store();
        // Capture the PG url before it moves into the documents state — the cross-workspace
        // /me/stats surface (hq-web-extras.15) below opens its own ws_default pool for the
        // membership directory.
        let pg_url_for_me = pg_url.clone();
        // Build the documents REST state once; the authenticated module router and the public
        // share-read router (hq-web-extras.9) share the same store handles.
        let docs_state = DocumentsApiState::new(
            pg_url,
            blob,
            bucket,
            Extractor::without_ocr(),
            build_embedder(),
        );
        public_share = Some(gt_documents::public_share_router(docs_state.clone()));
        rest = rest.module(DocumentsModule::with_http(docs_state));
        // Cross-workspace self-view (hq-web-extras.15): GET /api/v1/me/stats rolls up issue progress
        // across every workspace the caller is a member of. It needs BOTH the global identity
        // directory (`public.user_workspaces`, the membership N:N from hq-identity) over the PG pool
        // here, AND each tenant's own `hq_<ws>` tracker via the per-workspace Dolt store cache. The
        // latter only exists when GT_DOLT_BASE_URL configures multi-tenant routing, so the surface
        // mounts only then; without it, single-tenant Dolt has no per-workspace stores to aggregate.
        match std::env::var("GT_DOLT_BASE_URL") {
            Ok(base) => {
                let me_pool = WorkspacePool::connect(&pg_url_for_me, "default")
                    .await
                    .context("me/stats: connect ws_default Postgres pool")?;
                let memberships = Arc::new(PgUsers::new(me_pool.pool().clone(), "default"))
                    as Arc<dyn gt_auth::MembershipDirectory>;
                let stores = Arc::new(
                    WorkspaceStores::from_base_url(&base)
                        .context("me/stats: GT_DOLT_BASE_URL is malformed")?,
                );
                let me_source = Arc::new(IdentityDoltMeStats::new(memberships, stores));
                rest = rest.module(MeModule::with_http(MeApiState::new(me_source)));
                eprintln!(
                    "[gt-mcp-server] REST domain modules: meta + workspace + rig + documents + agent + quota + merge + skills + feed + convoy + me (cross-workspace stats)"
                );
            }
            Err(_) => eprintln!(
                "[gt-mcp-server] REST domain modules: meta + workspace + rig + documents + agent + quota + merge + skills + feed + convoy (GT_DOLT_BASE_URL unset → no /me/stats cross-workspace surface)"
            ),
        }
    } else {
        eprintln!(
            "[gt-mcp-server] REST domain modules: meta + agent + quota + merge + skills + feed + convoy (GT_PG_URL unset → no workspace/rig/documents)"
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
    // Fold in the PUBLIC share-read route (hq-web-extras.9): it lives outside the /api/v1/<ns>
    // module prefixes, so it is not in either builder's docs — merge its prefix-free spec
    // directly (its `#[utoipa::path]` already names the absolute `/share/{hash}`).
    if public_share.is_some() {
        openapi_doc.merge(gt_documents::public_openapi());
    }
    // Fold in the public `/auth/*` login + RBAC surface (hq-web-extras.10): it mounts at the
    // server ROOT (below, beside /openapi.json), not under an /api/v1/<ns> module prefix, so it is
    // in neither builder's docs — its `#[utoipa::path]`s already name the absolute `/auth/...`
    // paths. Merged unconditionally: it describes the platform's auth contract (the gt-web codegen
    // consumes it), and the compiled feature set (`pg`) advertises the admin/RBAC routes too.
    openapi_doc.merge(gt_auth::auth_openapi());
    let openapi_json: Arc<str> = Arc::from(
        serde_json::to_string(&openapi_doc).context("serialize the merged OpenAPI document")?,
    );
    let app = app.merge(openapi_router(openapi_json));
    eprintln!("[gt-mcp-server] fused OpenAPI on GET /openapi.json (public spec)");

    // Mount the public, unauthenticated share-read surface on the ops/public router — beside
    // /openapi.json and /health, never behind the RS256 auth chain the /api/v1/* routes carry
    // (hq-web-extras.9). A capability hash is the only credential; no session data leaks.
    let app = match public_share {
        Some(router) => {
            eprintln!("[gt-mcp-server] public share read on GET /share/:hash (unauthenticated)");
            app.merge(router)
        }
        None => app,
    };

    // Public login surface (hq-web-extras.7): mount `/auth/*` UNGUARDED so login is reachable
    // without a bearer. Gated on a verifier (for the JWKS), a minter (the RS256 signing key),
    // and Postgres (the `users` store). The `authenticate` layer here only *injects* claims when
    // a token IS present — it passes anonymous requests straight through (see `auth::authenticate`)
    // — so `/auth/me` sees the caller while `/auth/login` stays open. Distinct from the
    // `/api/v1/*` chain below, which additionally enforces RBAC scopes.
    let access_ttl = env_u64("GT_AUTH_ACCESS_TTL_SECS", 900);
    let refresh_ttl = env_u64("GT_AUTH_REFRESH_TTL_SECS", 2_592_000);
    // The Personal Access Token verifier (hq-security-pat.1), hoisted so BOTH the `/auth/*` chain
    // (built in the match arm below) and the `/api/v1/*` chain (built afterwards) authenticate a
    // `gtpat_…` bearer through it. Set inside the arm where the Postgres pool is built; `None`
    // without a verifier + minter + PG, so a deploy without login carries no PAT surface either.
    let mut pat_verifier: Option<Arc<dyn PatVerifier>> = None;
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
                // hq-rbac.3: the role catalog + the users.roles assignment column, so login can
                // expand roles → scopes from the first request.
                gt_auth::migrations::CREATE_ROLES,
                gt_auth::migrations::ADD_USER_ROLES,
                // hq-identity.1: the global identity + N:N membership tables, so global login
                // (hq-identity.2) and the migration below have somewhere to land.
                gt_auth::migrations::CREATE_GLOBAL_IDENTITY,
                // hq-idp-db.1/.2: the GLOBAL `public.oauth_providers` store, so the DB-backed
                // OAuth/OIDC login resolver (`DbOauthLogin`) has rows to resolve a `provider_id`
                // against. Idempotent CREATE ... IF NOT EXISTS like the rest.
                gt_auth::migrations::CREATE_OAUTH_PROVIDERS,
                // hq-idp-db.3: the ephemeral authorize-state + PKCE store, so the public
                // `/authorize`→`/callback` redirect flow has somewhere durable to park the
                // per-login `state`+`code_verifier` (one-shot, ~10 min TTL).
                gt_auth::migrations::CREATE_OAUTH_AUTHZ_STATE,
                // hq-gt-login-oauth.1: the nullable `cli_redirect` column on oauth_authz_state, so a
                // `gt login` browser handshake can park the loopback URL the callback 302s a one-shot
                // code back to. ALTER ... ADD COLUMN IF NOT EXISTS, idempotent like the rest.
                gt_auth::migrations::ADD_CLI_REDIRECT,
                // hq-gt-login-oauth.2: the one-shot CLI hand-off code store, where /auth/callback
                // parks a `gt login` token pair under an opaque code for /auth/cli/exchange to
                // redeem. CREATE TABLE IF NOT EXISTS, idempotent like the rest.
                gt_auth::migrations::CREATE_OAUTH_CLI_CODE,
                // hq-security-pat.1: the per-workspace Personal Access Token store, defined in the
                // ws_default template so gt_create_workspace_schema clones it into every tenant —
                // the table the PAT verifier + the self-service /auth/tokens surface read.
                gt_auth::migrations::CREATE_PERSONAL_ACCESS_TOKENS,
            ] {
                sqlx::raw_sql(sql)
                    .execute(&pool)
                    .await
                    .context("auth: apply gt-auth migration")?;
            }
            // hq-identity.4: lift every existing per-workspace user into the global identity +
            // membership tables, THEN (re)seed the global admin. Order matters — the seed is the
            // authority for admin, so it runs last and wins. Both are idempotent.
            migrate_users_to_global(&pool).await?;
            seed_admin(&pool).await?;
            let jwks = Arc::new(
                JwtAuthenticator::from_env()
                    .context("auth: build JWKS from the public verifier keys")?
                    .jwk_set(),
            );
            // One PgUsers backs the login port, the user-admin store (hq-web-extras.5), and the
            // role-admin store (hq-rbac.4) — one adapter over the ws_default pool.
            let pg_users = Arc::new(PgUsers::new(pool.clone(), "default"));
            // hq-security-pat: one PgPatStore over the same ws_default pool backs BOTH the
            // request-time PAT verifier (composition's `PatVerifier`, wired into the auth
            // middleware) AND the self-service `/auth/tokens` admin surface (`PatAdmin`). Hoist the
            // verifier so the `/api/v1/*` chain below uses the same store.
            let pg_pat = Arc::new(PgPatStore::new(pool.clone()));
            pat_verifier = Some(pg_pat.clone() as Arc<dyn PatVerifier>);
            // hq-idp-db.3: the public OAuth redirect-login flow. One DB-backed resolver
            // (`DbOauthLogin`) drives BOTH `oauth_login` (the JSON `/login` code path) and
            // `authz_flow` (the `/authorize`→`/callback` redirect), so they share the same provider
            // store + redirect URI. Built once here; `None` without the `oauth` feature.
            #[cfg(feature = "oauth")]
            let db_oauth = db_oauth_resolver(pool.clone());
            let login_state = LoginState {
                // hq-identity.4: login now authenticates against the GLOBAL identity and resolves
                // the active workspace from membership (default = first), replacing the per-ws
                // ws_default lookup. The migration + seed above guarantee admin@gt.local already
                // has a global row + a default membership, so the live login survives the cutover.
                login: Arc::new(GlobalLogin(pg_users.clone())),
                // OAuth/OIDC login provider. hq-idp-db.2 moved provider config from env to the DB:
                // when the `oauth` feature is built, this is the `DbOauthLogin` resolver over the
                // GLOBAL `public.oauth_providers` store (just migrated above), so a login's
                // `provider_id` selects the registered provider per request — no redeploy to add
                // one. Without the `oauth` feature there is no HTTP client, so it stays `None` and
                // an OAuth/OIDC login responds 501.
                #[cfg(feature = "oauth")]
                oauth_login: db_oauth
                    .clone()
                    .map(|r| r as Arc<dyn gt_auth::LoginProvider>),
                #[cfg(not(feature = "oauth"))]
                oauth_login: None,
                users: Some(pg_users.clone() as Arc<dyn gt_auth::UserStore>),
                roles: Some(pg_users.clone() as Arc<dyn gt_auth::RoleStore>),
                // hq-security-pat.2: the self-service PAT surface — the same store the verifier uses.
                pat: Some(pg_pat.clone() as Arc<dyn gt_auth::PatAdmin>),
                // Cross-workspace surface (hq-identity.3): list memberships + switch active
                // workspace, backed by the same global-identity adapter.
                memberships: Some(pg_users.clone() as Arc<dyn gt_auth::MembershipDirectory>),
                // Membership administration (hq-platform-hardening.2): a ws admin adds/removes
                // another user, backed by the same global-identity adapter.
                membership_admin: Some(pg_users.clone() as Arc<dyn gt_auth::MembershipAdmin>),
                // OAuth/OIDC provider administration (hq-idp-db.4): a SYSTEM admin manages the
                // GLOBAL login providers via `/auth/providers`, backed by PgProviderRepo over the
                // shared pool. The secret is AES-GCM-sealed at rest (GT_SECRET_KEY) and never
                // returned. Wired only with the `oauth` feature (the provider store + crypto).
                #[cfg(feature = "oauth")]
                providers: Some(Arc::new(gt_auth::PgProviderRepo::new(pool.clone()))
                    as Arc<dyn gt_auth::ProviderStore>),
                // hq-idp-db.3: the PUBLIC OAuth redirect-login flow. `authz_flow` is the same
                // `DbOauthLogin` resolver as `oauth_login` (it builds the IdP authorize URL +
                // runs the PKCE exchange); `authz_state` is the durable, one-shot state+PKCE store
                // (`public.oauth_authz_state`, migrated above), so an in-flight login survives a
                // redeploy. `fe_redirect_url` is where `/auth/callback` hands the tokens off after a
                // successful login (None ⇒ returns the token JSON, for a non-browser client).
                #[cfg(feature = "oauth")]
                authz_flow: db_oauth
                    .clone()
                    .map(|r| r as Arc<dyn gt_auth::OauthAuthzFlow>),
                #[cfg(feature = "oauth")]
                authz_state: db_oauth.as_ref().map(|_| {
                    Arc::new(gt_auth::PgAuthzStateRepo::new(pool.clone()))
                        as Arc<dyn gt_auth::AuthzStateStore>
                }),
                // hq-gt-login-oauth.2: the one-shot CLI hand-off code store
                // (`public.oauth_cli_code`, migrated above), so `/auth/callback` can park a
                // `gt login` token pair under an opaque code and `/auth/cli/exchange` redeem it.
                #[cfg(feature = "oauth")]
                cli_code: db_oauth.as_ref().map(|_| {
                    Arc::new(gt_auth::PgCliCodeRepo::new(pool.clone()))
                        as Arc<dyn gt_auth::CliCodeStore>
                }),
                #[cfg(feature = "oauth")]
                fe_redirect_url: std::env::var("GT_OAUTH_FE_REDIRECT_URL")
                    .ok()
                    .filter(|v| !v.trim().is_empty()),
                // JIT SSO provisioner (hq-epic.auth-refactor.4): wire PgUsers as the
                // SsoProvisioner so the /auth/callback auto-creates SSO user rows on first login.
                #[cfg(feature = "oauth")]
                sso_provisioner: Some(pg_users.clone() as Arc<dyn gt_auth::SsoProvisioner>),
                minter: Arc::new(minter),
                // Durable refresh store (hq-platform-hardening.1): PgRefreshStore over the same
                // ws_default pool, so a refresh token survives a gt-mcp-server redeploy instead of
                // being wiped with the process (the in-memory MVP forced re-login on every deploy).
                // It backs the async `AsyncRefreshStore` port directly, no block_on; the
                // `refresh_tokens` table was provisioned by the CREATE_REFRESH_TOKENS migration above.
                refresh: Arc::new(PgRefreshStore::new(pool.clone())),
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
                AuthState::new(verifier, audit.clone())
                    .with_pat(pg_pat.clone() as Arc<dyn PatVerifier>),
                authenticate,
            ));
            eprintln!(
                "[gt-mcp-server] login surface on /auth/* (public; access {access_ttl}s / refresh {refresh_ttl}s, durable PG refresh)"
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
                .layer(axum::middleware::from_fn_with_state(
                    audit.clone(),
                    audit_denials,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    {
                        // hq-security-pat: the API surface authenticates `gtpat_…` bearers through
                        // the same PAT verifier as `/auth/*` (set above when PG/login is configured).
                        let mut st = AuthState::new(verifier, audit.clone());
                        if let Some(pv) = pat_verifier.clone() {
                            st = st.with_pat(pv);
                        }
                        st
                    },
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
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    spec.to_string(),
                )
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
    let notifications_id = ModuleId::new("notifications").expect("`notifications` is a valid module id");
    let workspace_migs = gt_store_pg::workspace_migrations();
    let feature_migs = gt_store_pg::feature_flags_migrations();
    let rig_migs = RigsModule.migrations();
    // hq-docs-store.1: the per-workspace `documents` template tables (docs/11). Like `rig`,
    // they seed the `ws_default` template so `gt_create_workspace_schema` clones them per tenant.
    let docs_migs = gt_store_pg::docs_migrations();
    // notifications: the public-schema `notifications` table agents write to via notify.send.execute.
    let notifications_migs = gt_store_pg::notifications_migrations();

    let plan: Vec<_> = workspace_migs
        .iter()
        .map(|m| (&workspace_id, m))
        .chain(feature_migs.iter().map(|m| (&feature_id, m)))
        .chain(rig_migs.iter().map(|m| (&rig_id, m)))
        .chain(docs_migs.iter().map(|m| (&docs_id, m)))
        .chain(notifications_migs.iter().map(|m| (&notifications_id, m)))
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
        eprintln!("[gt-mcp-server] GT_PG_URL unset; domain dispatch disabled (issues + meta only)");
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
    // Per-workspace Dolt pools for `workspace.create`'s tenant provisioning
    // (hq-gap-workspace-provision-full): when multi-tenant Dolt routing is on
    // (GT_DOLT_BASE_URL), creating a workspace also `CREATE DATABASE hq_<slug>`s
    // and seeds its issues schema so the tracker works from creation. Unset ⇒
    // single-tenant Dolt on the shared `hq`, nothing per-tenant to provision.
    let dolt_pools = std::env::var("GT_DOLT_BASE_URL")
        .ok()
        .map(|base| WorkspacePools::from_url(&base))
        .transpose()
        .context("GT_DOLT_BASE_URL is malformed")?
        .map(Arc::new);
    let workspace_handler = match &dolt_pools {
        Some(dolt) => WorkspaceHandler::new(pool.clone()).with_dolt(dolt.clone()),
        None => WorkspaceHandler::new(pool.clone()),
    };
    // Convoy → scheduler bridge (hq-daemons-health-20260607.2): a convoy.launch runs in THIS
    // process, but the polecat sling lives in the orchd daemon and there is no cross-process
    // event bus — the dispatch gt-channel is the IPC. Wire the ConvoyHandler to drop a
    // {bead,priority} request onto the same channel the orchd dispatch loop consumes, so a
    // convoy.launch actually slings a polecat. Gated on GT_CHANNEL_ROOT (the shared channel
    // root both processes mount): unset ⇒ bridge off, convoy events are still recorded.
    let convoy_handler = {
        let handler = ConvoyHandler::new(event_log.clone());
        match std::env::var("GT_CHANNEL_ROOT") {
            Ok(root) => {
                let name =
                    std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
                match gt_channel::Channel::open(&root, &name) {
                    Ok(channel) => {
                        eprintln!(
                            "[gt-mcp-server] convoy→scheduler bridge on — dispatch channel {root}/{name}"
                        );
                        handler.with_dispatch_channel(Arc::new(channel))
                    }
                    Err(e) => {
                        eprintln!(
                            "[gt-mcp-server] convoy→scheduler bridge off — channel open failed at {root}/{name}: {e}"
                        );
                        handler
                    }
                }
            }
            Err(_) => {
                eprintln!("[gt-mcp-server] convoy→scheduler bridge off — GT_CHANNEL_ROOT unset");
                handler
            }
        }
    };
    let router = DomainRouter::new()
        .register(Arc::new(workspace_handler))
        .register(Arc::new(RigHandler::new(ws_pools.clone())))
        // A completed merge marks the owning rig's graph stale (hq-graphrig.7).
        .register(Arc::new(
            MergeHandler::new(event_log.clone()).with_rig_pools(ws_pools.clone()),
        ))
        .register(Arc::new(convoy_handler))
        .register(Arc::new(AgentHandler::new(event_log.clone())))
        .register(Arc::new(QuotaHandler::new(event_log.clone())))
        // notify.* — operator notification channel (hq-notifications): agents write
        // via notify.send; the browser bell polls/streams the same PG table.
        .register(Arc::new(NotifyHandler::new(pool.clone(), event_log.clone())))
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
    let rig_prefixes: Arc<dyn WorkspaceRigPrefixes> =
        Arc::new(PgRigPrefixes::new(ws_pools.clone()));
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
        let on = std::env::var("GT_EMBEDDINGS")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        if on {
            match gt_docs_embed::fastembed::FastEmbedder::new() {
                Ok(e) => {
                    eprintln!("[gt-mcp-server] documents semantic search on (fastembed local)");
                    return Some(Arc::new(e));
                }
                Err(err) => {
                    eprintln!(
                        "[gt-mcp-server] embedder init failed — {err}; search is full-text only"
                    );
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

/// Ensure the default admin exists as a GLOBAL identity (hq-identity.4, was hq-web-extras.4).
///
/// Seeds, all idempotently and all argon2-hashed at boot (never plaintext):
/// - the legacy `ws_default.users` admin row (kept for back-compat during the transition);
/// - the global `public.users` admin identity (id `admin`, the globally-unique email);
/// - an `admin` role in `ws_default.roles` granting `["*"]` (the RBAC wildcard = allow-all);
/// - a `public.user_workspaces` membership tying admin to the `default` workspace with that role.
///
/// So once login is global, `admin@gt.local` authenticates against `public.users`, resolves its
/// `default` membership, and expands `admin` → `["*"]` — the live admin login survives the
/// cutover. Skipped (with a log line) unless BOTH `GT_ADMIN_EMAIL` and `GT_ADMIN_PASSWORD` are
/// set, so no insecure default credential is ever baked into a deploy.
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
    // Legacy per-ws row (back-compat; harmless once global login is wired).
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

    // Global identity: the admin reachable from any workspace pool.
    sqlx::query(
        "INSERT INTO public.users (id, email, password_hash, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $4) ON CONFLICT (email) DO NOTHING",
    )
    .bind("admin")
    .bind(&email)
    .bind(&hash)
    .bind(now)
    .execute(pool)
    .await
    .context("seed global admin identity")?;
    // The `admin` role granting the wildcard, in the default workspace's catalog.
    sqlx::query(
        "INSERT INTO ws_default.roles (name, scopes, created_at, updated_at) \
         VALUES ('admin', $1, $2, $2) ON CONFLICT (name) DO UPDATE SET scopes = EXCLUDED.scopes",
    )
    .bind(&scopes)
    .bind(now)
    .execute(pool)
    .await
    .context("seed admin role")?;
    // Membership: admin@default holds `admin`, so global login expands it to `["*"]`. Resolve the
    // surviving global id by email in case a prior row already claimed it.
    let admin_id: String = sqlx::query_scalar("SELECT id FROM public.users WHERE email = $1")
        .bind(&email)
        .fetch_one(pool)
        .await
        .context("resolve global admin id")?;
    sqlx::query(
        "INSERT INTO public.user_workspaces (user_id, workspace_slug, role, created_at) \
         VALUES ($1, 'default', 'admin', $2) \
         ON CONFLICT (user_id, workspace_slug) DO UPDATE SET role = EXCLUDED.role",
    )
    .bind(&admin_id)
    .bind(now)
    .execute(pool)
    .await
    .context("seed admin membership")?;
    eprintln!("[gt-mcp-server] global admin ensured: {email} (default/admin → [*])");
    Ok(())
}

/// Lift every existing per-workspace user (`ws_<slug>.users`) into the global identity tables
/// (hq-identity.4): one `public.users` row per email plus a `public.user_workspaces` membership
/// for the workspace it came from, with the user's old direct scopes preserved through a role the
/// membership names ([`migrated_role`]). Idempotent — every write is an upsert, so a restart
/// against an already-migrated DB is a no-op. Best-effort per workspace: a tenant whose schema has
/// no `users` table is skipped, never fatal. Runs BEFORE [`seed_admin`], which is the authority for
/// the admin row.
async fn migrate_users_to_global(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the Unix epoch")
        .as_secs() as i64;
    // Drive off the workspace catalog: its slug is authoritative for both the schema name and the
    // membership's `workspace_slug` (reverse-mapping a schema name back to a slug would be lossy).
    // The catalog is self-seeded LATER in boot, so on a brand-new DB it may not exist yet — but a
    // brand-new DB has no pre-existing users to migrate either, so its absence is a clean skip.
    let has_catalog: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'workspaces')",
    )
    .fetch_one(pool)
    .await
    .context("migrate: probe workspace catalog")?;
    if !has_catalog {
        return Ok(());
    }
    let slugs: Vec<String> = sqlx::query_scalar("SELECT slug FROM public.workspaces ORDER BY slug")
        .fetch_all(pool)
        .await
        .context("migrate: list workspaces")?;
    let mut migrated = 0usize;
    for slug in slugs {
        let schema = schema_for(&slug);
        // Catalog slugs are system-minted, but the schema name is interpolated below — prove it is
        // a plain `ws_*` identifier before it ever reaches a SQL string.
        if !is_safe_schema_ident(&schema) {
            continue;
        }
        let has_users: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'users')",
        )
        .bind(&schema)
        .fetch_one(pool)
        .await
        .context("migrate: probe users table")?;
        if !has_users {
            continue;
        }
        // Ensure the per-ws role catalog exists (older tenants may predate it) so the migrated
        // role has somewhere to land and login can expand it.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.roles ( \
                name TEXT PRIMARY KEY, \
                scopes TEXT[] NOT NULL DEFAULT '{{}}', \
                created_at BIGINT NOT NULL, \
                updated_at BIGINT NOT NULL )"
        ))
        .execute(pool)
        .await
        .with_context(|| format!("migrate: ensure {schema}.roles"))?;

        let rows = sqlx::query(&format!(
            "SELECT id, email, password_hash, scopes FROM {schema}.users"
        ))
        .fetch_all(pool)
        .await
        .with_context(|| format!("migrate: read {schema}.users"))?;
        for row in &rows {
            let id: String = row.try_get("id").context("migrate: row id")?;
            let email: String = row.try_get("email").context("migrate: row email")?;
            let hash: String = row.try_get("password_hash").context("migrate: row hash")?;
            let scopes: Vec<String> = row.try_get("scopes").context("migrate: row scopes")?;

            sqlx::query(
                "INSERT INTO public.users (id, email, password_hash, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $4) ON CONFLICT (email) DO NOTHING",
            )
            .bind(&id)
            .bind(&email)
            .bind(&hash)
            .bind(now)
            .execute(pool)
            .await
            .context("migrate: upsert global user")?;
            // The surviving global id may differ if this email already had a global row.
            let global_id: String =
                sqlx::query_scalar("SELECT id FROM public.users WHERE email = $1")
                    .bind(&email)
                    .fetch_one(pool)
                    .await
                    .context("migrate: resolve global id")?;

            let (role_name, role_scopes) = migrated_role(&global_id, &scopes);
            sqlx::query(&format!(
                "INSERT INTO {schema}.roles (name, scopes, created_at, updated_at) \
                 VALUES ($1, $2, $3, $3) ON CONFLICT (name) DO UPDATE SET scopes = EXCLUDED.scopes"
            ))
            .bind(&role_name)
            .bind(&role_scopes)
            .bind(now)
            .execute(pool)
            .await
            .context("migrate: upsert migrated role")?;

            sqlx::query(
                "INSERT INTO public.user_workspaces (user_id, workspace_slug, role, created_at) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, workspace_slug) DO NOTHING",
            )
            .bind(&global_id)
            .bind(&slug)
            .bind(&role_name)
            .bind(now)
            .execute(pool)
            .await
            .context("migrate: upsert membership")?;
            migrated += 1;
        }
    }
    if migrated > 0 {
        eprintln!("[gt-mcp-server] migrated {migrated} per-workspace user(s) into global identity");
    }
    Ok(())
}

/// The (role name, scopes) a migrated user's membership references (hq-identity.4), carrying their
/// old direct scopes into the role→scopes model. A full-access user (`["*"]`) maps to the shared
/// `admin` role; everyone else gets a per-user role keyed by their global id, granting exactly the
/// scopes they had. Pure — unit-tested without a database.
fn migrated_role(global_id: &str, scopes: &[String]) -> (String, Vec<String>) {
    if scopes.len() == 1 && scopes[0] == "*" {
        ("admin".to_string(), vec!["*".to_string()])
    } else {
        (format!("migrated-{global_id}"), scopes.to_vec())
    }
}

/// Whether `s` is a plain `ws_*` Postgres schema identifier safe to interpolate (hq-identity.4):
/// lowercase letters, digits, and underscores only, ≤ the 63-byte identifier limit. Guards the
/// migration's interpolated schema names even though catalog slugs are system-minted.
fn is_safe_schema_ident(s: &str) -> bool {
    s.len() <= 63
        && s.starts_with("ws_")
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
/// The DB-backed OAuth/OIDC [`LoginProvider`](gt_auth::LoginProvider) for `AuthState::oauth_login`
/// (hq-idp-db.2). Unlike the retired env path (`OidcConfig::from_env`), provider config now lives in
/// the GLOBAL `public.oauth_providers` store: this returns a `DbOauthLogin` resolver over a
/// `PgProviderRepo` on `pool`, so a login's `provider_id` selects the registered provider per
/// request — an admin adds Google/GitHub/Microsoft or a generic OIDC provider with no redeploy.
///
/// Returns `None` (the email+password path only; an OAuth/OIDC login responds `501`) when the
/// `oauth` feature is not built — a default deploy carries no HTTP client. The per-deploy bits that
/// are NOT provider rows still come from env: `GT_OIDC_REDIRECT_URI` (the app's own callback URL,
/// echoed on every exchange; required) and `GT_OIDC_WORKSPACE` (the tenant a resolved OAuth identity
/// lands in; defaults to `default`). A missing redirect URI is fatal — it must not silently resolve
/// to a blank callback.
/// The concrete DB-backed OAuth resolver (`DbOauthLogin`) over `pool`, shared by `oauth_login` (the
/// JSON `/login` code path) and `authz_flow` (the public `/authorize`→`/callback` redirect, with
/// `state`+PKCE — hq-idp-db.3). Returning the CONCRETE type (not a trait object) lets the call site
/// cast the single `Arc` to both ports, so they agree on the provider store + redirect URI. The
/// per-deploy bits not in a provider row still come from env: `GT_OIDC_REDIRECT_URI` (the app's own
/// `/auth/callback` URL, echoed + recorded on every flow; REQUIRED) and `GT_OIDC_WORKSPACE` (the
/// tenant a resolved OAuth identity lands in; defaults to `default`).
#[cfg(feature = "oauth")]
fn db_oauth_resolver(pool: sqlx::PgPool) -> Option<Arc<gt_auth::DbOauthLogin>> {
    let redirect_uri = std::env::var(gt_auth::ENV_REDIRECT_URI)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .expect("GT_OIDC_REDIRECT_URI must be set for the DB-backed OAuth login");
    let workspace = std::env::var(gt_auth::ENV_WORKSPACE)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "default".to_owned());
    let repo = Arc::new(gt_auth::PgProviderRepo::new(pool)) as Arc<dyn gt_auth::ProviderRepo>;
    let resolver = gt_auth::DbOauthLogin::new(repo, workspace, redirect_uri)
        .expect("build the DB-backed OAuth login resolver");
    eprintln!(
        "[gt-mcp-server] oauth/oidc login enabled (DB-backed provider store; public /authorize+/callback with state+PKCE)"
    );
    Some(Arc::new(resolver))
}

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
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        // Served byte-for-byte, and still valid JSON.
        assert_eq!(&bytes[..], spec.as_bytes());
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_ok());
    }

    /// hq-identity.4: a full-access user maps to the shared `admin` role; everyone else keeps their
    /// exact scopes through a per-user role keyed by their global id.
    #[test]
    fn migrated_role_maps_wildcard_to_admin_else_per_user() {
        assert_eq!(
            migrated_role("admin", &["*".to_string()]),
            ("admin".to_string(), vec!["*".to_string()])
        );
        assert_eq!(
            migrated_role(
                "user-bob@x.test",
                &["rig.read".to_string(), "beads.read".to_string()]
            ),
            (
                "migrated-user-bob@x.test".to_string(),
                vec!["rig.read".to_string(), "beads.read".to_string()]
            )
        );
        // A user with no scopes still gets a (empty) per-user role, never the admin one.
        assert_eq!(
            migrated_role("u-empty", &[]),
            ("migrated-u-empty".to_string(), vec![])
        );
        // `*` alongside other scopes is NOT the wildcard-only case → per-user role.
        let (name, _) = migrated_role("u-mixed", &["*".to_string(), "rig.read".to_string()]);
        assert_eq!(name, "migrated-u-mixed");
    }

    /// hq-identity.4: only plain `ws_*` identifiers are safe to interpolate as a schema name.
    #[test]
    fn is_safe_schema_ident_accepts_ws_and_rejects_the_rest() {
        assert!(is_safe_schema_ident("ws_default"));
        assert!(is_safe_schema_ident("ws_team_1"));
        for bad in [
            "public",
            "ws-default",
            "ws_Default",
            "ws_a;b",
            "pg_catalog",
            "",
            &format!("ws_{}", "a".repeat(61)),
        ] {
            assert!(!is_safe_schema_ident(bad), "{bad:?} must be rejected");
        }
    }

    /// hq-identity.4 (GT_PG_URL-gated): the boot path end to end against a live Postgres — apply
    /// the auth migrations, lift a pre-existing per-workspace user into the global identity, seed
    /// the global admin, and prove BOTH log in globally with their scopes intact. A no-op without
    /// GT_PG_URL (same gate as gt-auth's pg contract tests). Serialised: it sets admin env vars.
    #[tokio::test]
    async fn boot_migration_and_seed_make_users_log_in_globally() {
        let Ok(url) = std::env::var("GT_PG_URL") else {
            eprintln!("GT_PG_URL unset; skipping global migration+seed e2e");
            return;
        };
        // The server hands seed_admin a ws_default-scoped pool (its legacy per-ws insert is
        // unqualified `users`); mirror that search_path so the test is faithful to boot.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET search_path TO ws_default, public")
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect(&url)
            .await
            .expect("connect GT_PG_URL");
        // The schema must exist before a connection sets search_path to it on checkout.
        sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS ws_default")
            .execute(&pool)
            .await
            .expect("ensure ws_default schema");

        // The boot migration set the server applies before migrating.
        for sql in [
            gt_auth::migrations::CREATE_USERS,
            gt_auth::migrations::CREATE_ROLES,
            gt_auth::migrations::ADD_USER_ROLES,
            gt_auth::migrations::CREATE_GLOBAL_IDENTITY,
        ] {
            sqlx::raw_sql(sql)
                .execute(&pool)
                .await
                .expect("apply migration");
        }
        // A workspace catalog with `default` (the migration drives off it).
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS public.workspaces (slug TEXT PRIMARY KEY); \
             INSERT INTO public.workspaces (slug) VALUES ('default') ON CONFLICT DO NOTHING;",
        )
        .execute(&pool)
        .await
        .expect("seed workspace catalog");

        // Clean prior runs, then plant a pre-existing per-ws user (bob, rig.read) to be migrated.
        for email in ["bob@id4.test", "admin@id4.test"] {
            sqlx::query("DELETE FROM public.users WHERE email = $1")
                .bind(email)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM ws_default.users WHERE email = $1")
                .bind(email)
                .execute(&pool)
                .await
                .unwrap();
        }
        let hash = gt_auth::password::hash_password("pw-bob").unwrap();
        sqlx::query(
            "INSERT INTO ws_default.users (id, email, password_hash, scopes, created_at, updated_at) \
             VALUES ('u-bob', 'bob@id4.test', $1, ARRAY['rig.read'], 0, 0)",
        )
        .bind(&hash)
        .execute(&pool)
        .await
        .expect("plant pre-existing user");

        // Run the real boot steps.
        migrate_users_to_global(&pool).await.expect("migrate");
        std::env::set_var("GT_ADMIN_EMAIL", "admin@id4.test");
        std::env::set_var("GT_ADMIN_PASSWORD", "pw-admin");
        seed_admin(&pool).await.expect("seed admin");

        let users = PgUsers::new(pool.clone(), "default");

        // Migrated user: logs in globally, lands on its origin workspace, keeps its scopes.
        let bob = users
            .authenticate_global(
                &gt_auth::Credentials::EmailPassword {
                    email: "bob@id4.test".into(),
                    password: "pw-bob".into(),
                },
                None,
            )
            .await
            .expect("bob logs in globally");
        assert_eq!(bob.workspace, "default");
        assert_eq!(bob.scopes, vec!["rig.read".to_string()]);

        // Seeded admin: global login, default workspace, wildcard scope.
        let admin = users
            .authenticate_global(
                &gt_auth::Credentials::EmailPassword {
                    email: "admin@id4.test".into(),
                    password: "pw-admin".into(),
                },
                None,
            )
            .await
            .expect("admin logs in globally");
        assert_eq!(admin.workspace, "default");
        assert_eq!(admin.scopes, vec!["*".to_string()]);

        // Idempotent: a second migrate + seed changes nothing and still logs both in.
        migrate_users_to_global(&pool).await.expect("re-migrate");
        seed_admin(&pool).await.expect("re-seed");
        assert_eq!(
            users
                .authenticate_global(
                    &gt_auth::Credentials::EmailPassword {
                        email: "bob@id4.test".into(),
                        password: "pw-bob".into(),
                    },
                    None,
                )
                .await
                .unwrap()
                .scopes,
            vec!["rig.read".to_string()]
        );
    }
}
