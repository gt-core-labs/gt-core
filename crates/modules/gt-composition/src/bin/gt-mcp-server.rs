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
//!   behind a reverse proxy; unset ⇒ loopback-only. The authorities of `GT_SELF_URL` and
//!   `GT_PUBLIC_URL` are auto-appended too (gtcore-2cc534), so an in-cluster `gt mcp call`
//!   against the URL the orchd writes into agents' `.mcp.json` is accepted without extra config.
//! - `GT_MCP_ACTOR` — scope actor, default `mcp-local`.
//! - `GT_MCP_SCOPE_CONFIG` — RBAC TOML/JSON path; unset ⇒ deny-by-default.
//! - `GT_REPO_DIR` — gt-core checkout whose `main` tree backs surface validation
//!   (S3, hq-core-mcp.9); unset ⇒ surface existence checks are skipped.
//! - `GT_DOLT_BASE_URL` — multi-tenant routing (hq-mt-routing.5); unset ⇒
//!   single-tenant on `GT_DOLT_URL`.
//! - `GT_PG_AUDIT_URL` — durable Postgres audit sink; unset ⇒ in-memory.
//! - `GT_A2A_DEFAULT_RIG` / `GT_A2A_INTAKE_EPIC` / `GT_A2A_ORCHD_URL` — the A2A
//!   ingress (B5, gtcore-9039b5): all three present (plus `GT_CHANNEL_ROOT` and
//!   the RS256 verifier) ⇒ `POST /a2a` + the public `/.well-known/agent.json`
//!   are served; any missing ⇒ no A2A surface. `GT_A2A_ORCHD_TOKEN` optionally
//!   pins the session-control bearer (else one is minted with the signing key,
//!   ttl `GT_A2A_ORCHD_TOKEN_TTL_SECS`); `GT_PUBLIC_URL` names the card's
//!   public origin (e.g. `https://gt-dev.codecsrayo.com`).
//! - `GT_A2A_CROSS_WS_GRANTS` — cross-workspace delegation allow-list (B4,
//!   gtcore-3f3c57): comma-separated `origin->dest` pairs (`*` wildcard on either
//!   side), e.g. `acme->platform, ops->*`. Deny-by-default — unset ⇒ `a2a.delegate`
//!   / `a2a.discover` only see the caller's own tenant. Cross-workspace minting
//!   also needs `GT_DOLT_BASE_URL` (the per-tenant `hq_<dest>` routing); without it
//!   a cross-workspace `workspace` arg is rejected.

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
use gt_composition::kanban_rest::{kanban_rest_router, KanbanRestState};
use gt_composition::notifications::{notifications_router, NotificationsApiState};
use gt_composition::mcp::{
    A2aDelegateHandler, AgentHandler, AnalyticsHandler, AuditHandler, CommentsHandler, ConvoyHandler, CrossWsGrants, DispatchHandler, DocumentsHandler, DomainCatalogHandler, EmailHandler, EscalateHandler, EventLog, EventLogHooks,
    EventLogIssueSink, EventLogRigSink, GraphHandler, IdentityDoltMeStats, InvitesHandler, MemoryHandler, MergeHandler, NotifyHandler, ReportHandler,
    PgDocumentsResource, PgRigPrefixes, PgWorkspaceStatus, QuotaBlockGuard, QuotaHandler, RigHandler,
    WorkspaceHandler, WsPools,
};
use gt_composition::delegation_http::{A2aPeerClient, PeerRegistry};
use gt_composition::onboard::{onboard_router, OnboardState};
use gt_composition::operator_resource::EventLogOperatorResource;
use gt_composition::scope_bridge::bridge_scopes;
use gt_composition::session_reconcile::{ReapScope, ReapSink, SessionReconciler};
use gt_composition::stream::{feed_router, FeedState};
use gt_composition::terminal::{terminal_router, TerminalState};
use gt_polecat::tmux::TmuxCli;
use gt_docs_embed::Embedder;
use gt_docs_extract::Extractor;
use gt_graphindex::GraphifyIndexer;
use gt_store_blob::BlobStore;
use gt_store_pg::{schema_for, WorkspacePool};
use gt_vcs::VcsConnectionRepo;
use sqlx::Row;
// Domain REST modules + their `with_http` state (hq-fe-api-mount.1): the bin mounts each
// crate's `register_routes` so the FE reaches every namespace over authenticated HTTP.
use gt_composition::system::{load_config, system_router, ArchiveDaemon, SystemApiState};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_issues::{BoardModule, IssuesApiState, IssuesModule, MeApiState, MeModule};
use gt_mcp_server::{
    health, DocumentsResource, DomainRouter, HealthState, IssuesServer, PatAuthenticator,
    PgAuditSink, WorkspaceRigPrefixes, WorkspaceStatusGate, WorkspaceStores,
};
use gt_meta::MetaModule;
use gt_module::RootBuilder;
use gt_rbac::{RbacConfig, Scope};
use gt_store_dolt::{DoltIssues, WorkspacePools};
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

    // Singleton-daemon master switch (hq-talos-migration.4): the API surface (/mcp, REST,
    // /auth/*, SSE feed, webhook) is stateless and scales to N replicas, but the background
    // loops below (session reaper, archive sweep, account-dir GC, graph drift-reconcile) are
    // SINGLETONs — running them on every replica duplicates ticks and races. A multi-replica
    // k8s deploy runs the API tier with GT_RUN_DAEMONS=0 and a single-replica daemons tier with
    // the default. DEFAULT = ON, so the existing single-instance compose deploy (which sets
    // nothing) runs every daemon exactly as before — back-compat. The per-daemon cadence/off
    // envs still apply on top; this is the one master flag the API tier flips off. Read ONCE here.
    let run_daemons = should_run_daemons(|k| std::env::var(k).ok());
    if run_daemons {
        eprintln!("[gt-mcp-server] daemons: ON (singleton tier)");
    } else {
        eprintln!("[gt-mcp-server] daemons: OFF (API tier, GT_RUN_DAEMONS=0)");
    }

    // Store: the lifted Dolt issues adapter (hq-core-host.1), on the shared Dolt. gt-core owns
    // the bootstrap (hq-docs follow-up): ensure the target database exists before the pool
    // binds to it (a fresh Dolt volume ships none), so the deploy needs no dolt-init.sql.
    DoltIssues::ensure_database(&dolt_url).await?;
    let store = Arc::new(DoltIssues::connect(&dolt_url)?);
    store.ensure_schema().await?;
    eprintln!("[gt-mcp-server] issues: Dolt @ {dolt_url}");

    // Per-workspace domain catalog (gtcore-55d5fb H1): ensure the `default`
    // workspace's `domain_catalog` table exists and is seeded with the technical
    // set (built from the `Domain` enum so it can't diverge). Idempotent — a
    // re-boot re-upserts by key. Business workspaces get the editable generic
    // template at provision time (see mcp::workspace::provision_tenant).
    gt_composition::domain_catalog::seed_default_workspace(store.as_ref()).await?;
    eprintln!("[gt-mcp-server] domain catalog: default workspace seeded (technical set)");

    // The per-workspace event log (the event-sourced domains' durable store AND the SSE feed's
    // source) is path-partitioned under GT_EVENTLOG_ROOT (default /var/lib/gt-core). Built here
    // — before the issues REST state + the MCP service — so both can emit issue-mutation events
    // into the same log the `GET /stream` feed fans out (hq-issues-sse). The event-sourced
    // domain handlers + the feed route below share this one handle.
    let event_root = std::env::var("GT_EVENTLOG_ROOT")
        .ok()
        .map(std::path::PathBuf::from);
    // Backend selection (hq-talos-migration.10): the event log uses Postgres ONLY on an EXPLICIT
    // opt-in — GT_EVENTLOG_PG truthy (1/true/yes/on), connecting via GT_PG_URL. It is deliberately
    // NOT triggered by GT_PG_URL alone: prod already sets GT_PG_URL for the domain handlers, and
    // silently switching the event log to an empty `public.events` would RESET every event-sourced
    // domain (graph custody, merge board, agent sessions, …) — there is no file→PG backfill yet
    // (that is the cutover/data-migration item, hq-talos-migration.7/.10 ACs). Unset/falsy ⇒ the
    // path-partitioned file log under GT_EVENTLOG_ROOT, exactly as before (the prod-safe default).
    // PG backs N mcp-server replicas + a separate orchd concurrently with no shared volume (the
    // horizontal-scale unlock); flipping GT_EVENTLOG_PG off is reversible. Sync API on BOTH backends.
    let want_pg = std::env::var("GT_EVENTLOG_PG")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    let event_log = match want_pg.then(|| std::env::var("GT_PG_URL").ok()).flatten() {
        Some(pg_url) => match EventLog::new_pg(&pg_url) {
            Ok(log) => {
                eprintln!(
                    "[gt-mcp-server] event log: Postgres-backed (public.events; concurrent writers, no shared volume)"
                );
                Arc::new(log)
            }
            Err(e) => {
                eprintln!(
                    "[gt-mcp-server] event log: PG backend init failed ({e}); falling back to file log under GT_EVENTLOG_ROOT"
                );
                Arc::new(EventLog::new(event_root))
            }
        },
        None => {
            if want_pg {
                eprintln!(
                    "[gt-mcp-server] event log: GT_EVENTLOG_PG set but GT_PG_URL unset — using file log under GT_EVENTLOG_ROOT"
                );
            } else {
                eprintln!(
                    "[gt-mcp-server] event log: file-backed (GT_EVENTLOG_ROOT; set GT_EVENTLOG_PG=1 + GT_PG_URL for the Postgres backend)"
                );
            }
            Arc::new(EventLog::new(event_root))
        }
    };
    // The issues tracker is Dolt-backed, not event-sourced, so its mutations never reached the
    // event log — the SSE feed never carried issue movement. This sink closes that gap: it
    // appends every `issues.*` mutation (REST or MCP) to the workspace log, so the tracker moves
    // on `GET /stream?channel=issues`. One sink, shared by both surfaces, so REST and MCP emit the
    // identical event (parity).
    let issue_sink = Arc::new(EventLogIssueSink::new(event_log.clone()));

    // Interactive session reaper (hq-flow-validation-20260609.5): the orchd reconciler cannot see
    // the mayor/dog tmux — it lives on this process's per-workspace `gt-<ws>` socket, in a
    // different container's $TMUX_TMPDIR (/tmp is not shared). So judging interactive liveness must
    // run HERE, where the socket is reachable. Each tick replays the agent log and appends
    // agent.killed for interactive sessions whose tmux is gone on the declared socket; the append
    // lands in the shared log the SSE feed reads, so the Sessions view drops the dead mayor.
    // Gated on the singleton master switch (hq-talos-migration.4): the API tier never reaps.
    if run_daemons {
        let reaper_root = std::env::var("GT_EVENTLOG_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
        let reaper_ws = std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".into());
        let reaper_tick = env_u64("GT_RECONCILE_TICK_SECS", 120);
        let reaper = SessionReconciler::new(
            reaper_root,
            reaper_ws,
            std::env::temp_dir(), // heartbeat dir unused for the Interactive scope
            std::time::Duration::from_secs(300), // stale_after unused for the Interactive scope
            Arc::new(TmuxCli::new()), // default adapter unused — interactive probes its own socket
            ReapScope::Interactive,
            ReapSink::Log(event_log.clone()),
        );
        eprintln!(
            "[gt-mcp-server] interactive session reaper on — sweep {reaper_tick}s; closes dead mayor/dog sessions on the gt-<ws> socket"
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(reaper_tick));
            tick.tick().await; // skip the immediate first fire
            loop {
                tick.tick().await;
                let n = reaper.sweep().await;
                if n > 0 {
                    eprintln!("[gt-mcp-server] interactive reaper closed {n} dead session(s)");
                }
            }
        });
    }

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
        .module(IssuesModule::with_http(issues_api.clone()))
        // hq-62130a: the Kanban board projection — board.* MCP tools + the
        // /api/v1/board REST surface, sharing the issues API state (same store
        // resolution, actor, and SSE event sink).
        .module(BoardModule::with_http(issues_api))
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
    // Keep a handle to the loaded RBAC config for the post-router reachability self-check
    // (hq-rbac-reachability.1) — `rbac` itself moves into the server just below. Cheap `Arc` clone.
    let rbac_for_reach = rbac.clone();
    // `with_issue_sink` (hq-issues-sse): an `issues.*.execute` over MCP — the agent-driven
    // movement the REST surface alone would miss — publishes its event into the same per-workspace
    // log the SSE feed fans out, so the tracker moves on `GET /stream?channel=issues`.
    let mut service = IssuesServer::new(store.clone(), default_scope, rbac, audit.clone(), tools, repo_dir)
        .with_issue_sink(issue_sink.clone())
        // Claim-context custodian's quota signal (hq-context-custodian.2): a `working`
        // transition with no context is deferred (debt note) instead of stop-the-line ONLY
        // when the workspace pool is freshly out of capacity — read from the same per-workspace
        // quota event log the quota.* handler replays. Always on (the log is always present).
        .with_quota_signal(Arc::new(QuotaBlockGuard::new(event_log.clone())))
        // Auto-complete merge slot on bead close (gtcore-71c575): when a bead closes with a
        // delivered_sha, the close hook advances its merge slot to Merged so stale `failed`
        // slots for work already in main are cleaned up automatically.
        .with_close_hook(Arc::new(MergeHandler::new(event_log.clone())));

    // System config (hq-system-config) is loaded BEFORE the domain router so the
    // report-digest service (hq-84f93b) inside it and the /api/v1/system surface
    // below share ONE SharedArchiveConfig — a PUT/schedule.set is visible to the
    // scheduler daemon and the MCP tools without a restart.
    let system_config_path = std::env::var("GT_SYSTEM_CONFIG_PATH")
        .map(std::path::PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("GT_EVENTLOG_ROOT")
                .ok()
                .map(|r| std::path::PathBuf::from(r).with_file_name("system_config.json"))
        });
    let system_initial_cfg = system_config_path.as_ref().map(load_config).unwrap_or_default();
    let system_config = std::sync::Arc::new(RwLock::new(system_initial_cfg.clone()));

    // Domain dispatch (hq-mcp-dispatch): tool namespaces beyond issues.*/meta.*
    // (workspace.*, rig.*, …) route to PG-backed handlers. Wired only when
    // GT_PG_URL is set; unset ⇒ an empty router, so the server serves issues +
    // meta exactly as before.
    let (domains, rig_prefixes, ws_status, documents, report_service) =
        build_domain_router(event_log.clone())
            .await?;
    // Report-digest scheduler (hq-84f93b): fixed-time daily send to the ENABLED
    // subscribers. Like the outbox drain/mailbox, NOT behind the singleton gate —
    // it must tick where the outbox lives; the `last_sent_date` guard in the
    // persisted config keeps the send at-most-once per day.
    if let Some(svc) = &report_service {
        // One-shot migration (gtcore-8ff13e): the schedule LIST now lives in the
        // durable Postgres store, but pre-DB deployments persisted it to
        // `system_config.json`. On the first boot against the DB store, seed it
        // from whatever the legacy file still holds so those schedules are not
        // lost; gated on an empty store, so this is a no-op on every later boot.
        match svc.import_file_schedules(&system_initial_cfg.report_schedules).await {
            Ok(0) => {}
            Ok(n) => eprintln!(
                "[gt-mcp-server] report scheduler: migrated {n} legacy schedule(s) from system_config.json into the DB store"
            ),
            Err(e) => eprintln!(
                "[gt-mcp-server] report scheduler: legacy schedule migration skipped ({e})"
            ),
        }
        tokio::spawn(gt_composition::report_scheduler::ReportScheduler::new(svc.clone()).run());
        eprintln!("[gt-mcp-server] report scheduler on (digest via /api/v1/system/report/*)");
    }
    // audit.* tails the same audit sink the server records into (hq-mt-ops.3).
    // Registered unconditionally — it reads the in-memory or Postgres sink, so it
    // works even when GT_PG_URL is unset and the rest of the router is empty.
    let domains = domains.register(Arc::new(AuditHandler::new(audit.clone())));
    // analytics.summary (hq-1cd840): the Kanban dashboard KPIs over the same
    // tracker rows board.list reads; reopens derive from this audit sink.
    let domains = domains.register(Arc::new(AnalyticsHandler::new(
        store.clone(),
        issues_workspaces.clone(),
        audit.clone(),
    )));
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
    // Reachability self-check (hq-rbac-reachability.1). A namespace can be REGISTERED in the
    // router yet UNREACHABLE because no least-privilege actor grant references it — exactly how
    // `memory.*` shipped live but was denied to every non-`*` actor (admin reached issues.*/merge.*
    // but not memory.*), the failure invisible because the autorecall hook swallowed the denial.
    // Cross every advertised tool against the loaded RBAC actors and shout per orphan; under the
    // strict env the boot refuses. Meaningful ONLY when a real least-privilege policy is in force —
    // a pure `*` dev profile intends blanket access, so the audit is skipped there. This validates
    // the actor allow-surface in THIS config; a JWT client's claim scopes (e.g. a polecat's
    // catalog-derived role) ride a separate store the server does not hold, so they are out of scope.
    if let Some(cfg) = &rbac_for_reach {
        if cfg.has_least_privilege_actor() {
            let orphans = cfg.least_privilege_orphans(meta_tools.iter().map(|t| t.name.as_str()));
            if orphans.is_empty() {
                eprintln!(
                    "[gt-mcp-server] RBAC reachability OK: all {} advertised tool(s) have a least-privilege grant",
                    meta_tools.len()
                );
            } else {
                let mut namespaces: Vec<&str> =
                    orphans.iter().map(|t| t.split('.').next().unwrap_or(t)).collect();
                namespaces.sort_unstable();
                namespaces.dedup();
                eprintln!(
                    "[gt-mcp-server] ⚠ RBAC reachability: {} tool(s) in namespace(s) {namespaces:?} are reachable ONLY by a `*` superuser — no least-privilege actor grants them. Add a grant in GT_MCP_SCOPE_CONFIG (e.g. `allow = [..., \"{}.*\"]`).",
                    orphans.len(),
                    namespaces.first().copied().unwrap_or("<ns>"),
                );
                let strict = std::env::var("GT_MCP_REQUIRE_REACHABLE")
                    .ok()
                    .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "on" | "ON" | "yes" | "YES"));
                if strict {
                    anyhow::bail!(
                        "GT_MCP_REQUIRE_REACHABLE: registered namespace(s) {namespaces:?} have no least-privilege grant — refusing to boot"
                    );
                }
            }
        }
    }
    let domains = Arc::new(domains);
    // Kanban bridge REST (hq-95c2bb): comments/report/invites over cookie-auth
    // HTTP, dispatching through this same router — clone the Arc before it
    // moves into the MCP service.
    let kanban_domains = domains.clone();
    service = service.with_domains(domains);
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

    // Server→agent push over the live MCP session (gtcore-d366ff). The registry binds each
    // authenticated connection's peer to its actor + `Mcp-Session-Id`; the push observer
    // (spawned below) tails the event log and delivers `notifications/resources/updated` to
    // the agent's open GET stream when an A2A message or a delegation result lands — replacing
    // the agent's `a2a.inbox` / `a2a.status` poll. Additive: an agent with no open session is
    // never in the registry, so the push is a no-op and polling still works.
    let push_registry = Arc::new(gt_mcp_server::SessionRegistry::with_default_ttl());
    service = service.with_session_registry(push_registry.clone());

    // Streamable-HTTP Host allow-list (rmcp's DNS-rebinding guard). The default only
    // accepts loopback authorities (localhost/127.0.0.1/::1), so a public deploy behind a
    // reverse proxy — where the inbound `Host` is the served domain — would have every /mcp
    // request rejected. GT_MCP_ALLOWED_HOSTS (comma-separated host or host:port authorities)
    // is APPENDED to the loopback defaults, so local clients keep working and the deploy adds
    // its own domain (e.g. `gt.codecsrayo.com`). Unset ⇒ loopback-only, exactly as before.
    let mut http_config = StreamableHttpServerConfig::default();
    // Auto-allow the authorities the deploy already advertises as "where to reach me": the
    // in-cluster `GT_SELF_URL` (e.g. `http://gt-mcp-server:8765`) and the public `GT_PUBLIC_URL`
    // (e.g. `https://gt-dev.codecsrayo.com`). gt-orch-server writes GT_SELF_URL verbatim into
    // every agent's `.mcp.json` on each sling (hq-polecat-rig-config.1), so without this an
    // in-cluster `gt mcp call` / native `mcp__gt__*` against that host is rejected 403 "Host
    // header is not allowed" and the only workaround — hand-editing server_url to the public
    // host — is reverted by the next sling (gtcore-2cc534). Deriving the allow-list from the
    // SAME env the orchd hands out keeps a freshly-spawned agent working with no config edit,
    // and survives an orchd restart (it is code + already-present env, not a mutable file).
    for (var, label) in [
        ("GT_SELF_URL", "in-cluster self URL"),
        ("GT_PUBLIC_URL", "public origin"),
    ] {
        if let Some(url) = std::env::var(var).ok().filter(|v| !v.trim().is_empty()) {
            let hosts = authority_allowed_hosts(&url);
            if hosts.is_empty() {
                eprintln!("[gt-mcp-server] {var}={url:?} has no parseable Host authority; not added to allow-list");
            } else {
                eprintln!("[gt-mcp-server] allowed_hosts += {hosts:?} (from {var}, {label})");
                http_config.allowed_hosts.extend(hosts);
            }
        }
    }
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

    // Push observer (gtcore-d366ff): poll-tail the event log and push inbox/delegation/
    // escalation notifications to open MCP sessions, plus reap idle sessions on a timer.
    // Shares the same `event_log` handle the SSE feed streams from, so it sees both this
    // server's `a2a.send` writes and the orchd daemon's `delegation.completed.v1`.
    gt_composition::mcp_push::spawn(event_log.clone(), push_registry);
    eprintln!("[gt-mcp-server] session push observer on (event-log tail → MCP resource notifications)");

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

    // Per-account relogin + cred-health (gtcore-1fe9b4): POST /api/v1/quota/relogin/{start,complete}
    // relogs an EXISTING keychain account's creds dir via `claude /login`, seeds the dir
    // onboarding-complete so the sling consumes it without a first-run TUI/OAuth wedge, and dedups
    // duplicate dirs for the same email. GET /api/v1/quota/cred-health reports per-account
    // {refresh, expiry, onboarding, needs_relogin}. Mounted only with an RS256 verifier; relogin
    // needs `quota.write`, cred-health `quota.read` (audited on denial). See `gt_composition::relogin`.
    let relogin = verifier.as_ref().map(|v| {
        eprintln!(
            "[gt-mcp-server] relogin on POST /api/v1/quota/relogin/{{start,complete}} + GET /api/v1/quota/cred-health (cookie/bearer auth, scope quota.write/read)"
        );
        let relogin_eventlog_root = std::env::var("GT_EVENTLOG_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
        let accounts_root = gt_composition::account_dirs::accounts_root(&relogin_eventlog_root);
        // Enumerate every quota-tracked account in cred-health (gtcore-e09320): replay the
        // workspace's quota log into the registry so an account in `quota.list` with NO on-disk dir
        // (brayanrayo/fsrbwowr) still surfaces as needs_relogin instead of vanishing → shown Healthy.
        let known_log = event_log.clone();
        let known_ws = std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".to_string());
        let known_accounts: gt_composition::relogin::KnownAccountsFn = Arc::new(move || {
            known_log
                .replay_domain(
                    Some(&known_ws),
                    "quota.",
                    gt_quota::QuotaState::default(),
                    gt_quota::QuotaState::apply,
                )
                .map(|st| {
                    gt_quota::AccountRegistry::from_state(&st)
                        .accounts()
                        // gtcore-62723a: carry credential_dead so cred-health forces needs_relogin
                        // for prober-confirmed-dead accounts (the FE then surfaces relogin).
                        .map(|a| (a.id.clone(), a.credential_dead))
                        .collect::<Vec<(String, bool)>>()
                })
                .unwrap_or_default()
        });
        gt_composition::relogin::relogin_router(
            gt_composition::relogin::ReloginState::new(v.clone(), audit.clone(), accounts_root)
                .with_known_accounts(known_accounts),
        )
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

    // Kanban bridge REST (hq-95c2bb): comments + report + invites for the web
    // Kanban, dispatched through the SAME domain router as MCP (zero logic
    // forks). Gated on the RS256 verifier (cookie/bearer auth like notifications).
    let kanban = match verifier.as_ref() {
        Some(v) => {
            let mut state =
                KanbanRestState::new(v.clone(), audit.clone(), kanban_domains.clone());
            // gtpat_… bearers (agents/CLIs) authenticate here exactly like the
            // global REST chain — through the PG-backed PAT port.
            if let Ok(pg_url) = std::env::var("GT_PG_URL") {
                if let Ok(pat_pool) = WorkspacePool::connect(&pg_url, "default").await {
                    state = state.with_pat(Arc::new(PgPatStore::new(pat_pool.pool().clone())));
                }
            }
            eprintln!(
                "[gt-mcp-server] kanban bridge REST on /api/v1/{{comments,report,invites,analytics}} (domain-router dispatch)"
            );
            Some(kanban_rest_router(state))
        }
        None => None,
    };

    // Command mailbox + seguimiento subscriptions daemon (hq-8a521a): polls the
    // inbound-mail seam (GT_INBOUND_MAIL_DIR file source until the IMAP server
    // exists), verifies senders against the member mirror, executes orders
    // through the SAME domain router, and fans subscription events out via the
    // outbox. Opt-in: needs GT_INBOUND_MAIL_DIR + GT_PG_URL + the daemons
    // master switch; tick via GT_MAILBOX_TICK_SECS (default 30).
    // Like the outbox drain, NOT gated by the singleton switch: the inbound
    // dir is a per-pod mount (consume-by-rename keeps a poll idempotent), and
    // the daemons pod runs a different binary. Opt-in by GT_INBOUND_MAIL_DIR.
    match (
        std::env::var("GT_INBOUND_MAIL_DIR").ok(),
        std::env::var("GT_PG_URL").ok(),
    ) {
        (Some(dir), Some(pg_url)) => match sqlx::PgPool::connect(&pg_url).await {
            Ok(mailbox_pool) => {
                let tick = env_u64("GT_MAILBOX_TICK_SECS", 30).max(1);
                let inbox: Arc<dyn gt_notify::InboundMail> =
                    Arc::new(gt_notify::FileInbox::new(&dir));
                eprintln!(
                    "[gt-mcp-server] command mailbox on (every {tick}s; inbound {} @ {dir})",
                    inbox.label()
                );
                let mailbox = Arc::new(gt_composition::mailbox::Mailbox::new(
                    inbox,
                    kanban_domains.clone(),
                    mailbox_pool,
                    Arc::new(WsPools::new(pg_url)),
                    audit.clone(),
                    event_log.clone(),
                    "default".to_string(),
                ));
                tokio::spawn(mailbox.run(std::time::Duration::from_secs(tick)));
            }
            Err(e) => eprintln!("[gt-mcp-server] command mailbox off (PG connect failed: {e})"),
        },
        _ => eprintln!(
            "[gt-mcp-server] command mailbox off (set GT_INBOUND_MAIL_DIR + GT_PG_URL)"
        ),
    }

    // Per-workspace pool cache for the archive interceptor (hq-docs-archive-sync): when the sweep
    // archives an epic, its `documents`/embeddings are soft-deleted so it drops out of
    // `documents.search`. Built from GT_PG_URL — `None` when unset (no docs store, nothing to clean).
    // A dedicated cache (the domain router holds its own); both are lazy Arc-over-pool handles over
    // the same Postgres, and the hourly sweep is low-frequency, so the extra cache is negligible.
    let archive_pools: Option<Arc<WsPools>> = std::env::var("GT_PG_URL")
        .ok()
        .map(|pg_url| Arc::new(WsPools::new(pg_url)));

    // Operator scope options for GET /api/v1/system/report/scopes (hq-00ed29):
    // the public-schema workspace catalog + the per-tenant rig provider. Both
    // ride GT_PG_URL; a connect failure just leaves the route beads-only.
    let scope_sources: Option<(
        Arc<dyn gt_workspace::WorkspaceRepository>,
        Arc<dyn gt_rig::WorkspaceRigs>,
    )> = match std::env::var("GT_PG_URL") {
        Ok(pg_url) => match sqlx::PgPool::connect(&pg_url).await {
            Ok(pool) => Some((
                Arc::new(gt_workspace::PgWorkspaces::new(pool)),
                Arc::new(gt_composition::mcp::WsPoolRigs::new(Arc::new(WsPools::new(pg_url)))),
            )),
            Err(e) => {
                eprintln!("[gt-mcp-server] report/scopes catalog off (PG connect failed: {e})");
                None
            }
        },
        Err(_) => None,
    };

    // System config REST surface (hq-system-config): GET/PUT /api/v1/system/config and
    // POST /api/v1/system/archive/run. Scoped to system.read/system.write (admin `*` satisfies both).
    // Spawns the background archive daemon that sweeps old closed issues on a configurable interval.
    let system = verifier.as_ref().map(|v| {
        // The shared config + path were built before the domain router (the
        // report-digest service shares them — see system_config above).
        let config = system_config.clone();
        let config_path = system_config_path.clone();
        let initial_cfg = system_initial_cfg.clone();
        // The background sweep is a SINGLETON (hq-talos-migration.4): gate the spawn on the master
        // switch so the API tier never sweeps, but ALWAYS build the system_router below — GET/PUT
        // /api/v1/system/* + the on-demand POST /archive/run stay served on every replica.
        if run_daemons {
            tokio::spawn(
                ArchiveDaemon::new(system_store.clone(), config.clone(), archive_pools.clone())
                    .run(),
            );
            eprintln!(
                "[gt-mcp-server] archive daemon on (interval {}min, archive_after {}d)",
                initial_cfg.interval_minutes, initial_cfg.archive_after_days,
            );
        }
        eprintln!(
            "[gt-mcp-server] system config REST on /api/v1/system/* (scope system.read/write)"
        );
        let mut state = SystemApiState::new(
            v.clone(),
            audit.clone(),
            system_store.clone(),
            config,
            config_path,
            archive_pools.clone(),
            report_service.clone(),
        );
        if let Some((workspaces, rigs)) = scope_sources.clone() {
            state = state.with_scope_sources(workspaces, rigs);
        }
        system_router(state)
    });

    // Orphan claude-credential GC (hq-quota-onboard-web.6): the backend is the only always-on
    // process, so it sweeps the accounts root on a timer and reaps dirs no live account points at
    // (a re-onboarded email replaces its dir, a retire drops one, an abandoned /start leaves an
    // empty one). Off when GT_ACCOUNTS_GC_TICK_SECS=0. The grace window spares onboards in flight.
    // Singleton sweep (hq-talos-migration.4): gated on the master switch on top of its own
    // GT_ACCOUNTS_GC_TICK_SECS off-switch, so the API tier never reaps credential dirs.
    let gc_tick = env_u64("GT_ACCOUNTS_GC_TICK_SECS", 21_600); // 6h
    if run_daemons && gc_tick > 0 {
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

    // GitHub App push webhook (hq-vcs-connections.7): POST /api/v1/connection/github/webhook. The
    // answer to "does the custodian hear an EXTERNAL push to origin?" — a signed `push` delivery
    // verifies its HMAC-SHA256 (X-Hub-Signature-256) against the App's webhook secret, maps
    // installation.id → workspace and repository.full_name → rig, and marks that rig's graph stale
    // (default-branch + head-moved only) so `graph.refresh-stale` reindexes it. Mounted OUTSIDE the
    // /api/v1/* RBAC chain — a webhook authenticates by its signature, not a workspace JWT — gated on
    // BOTH a configured App webhook secret (GT_GITHUB_APP_WEBHOOK_SECRET_FILE) AND GT_PG_URL (the
    // connection store + per-workspace rig catalog). Without either, no route is mounted.
    // hq-61ea43: the webhook is mounted whenever PG is available and resolves the App webhook secret
    // from the DB-backed config per delivery (env fallback). An unconfigured App / missing secret is
    // a 503 at request time (the route exists, it just can't verify yet) rather than an unmounted
    // route — so configuring the App from the UI lights it up with no redeploy.
    let github_webhook = match std::env::var("GT_PG_URL") {
        Ok(pg_url) => match sqlx::PgPool::connect(&pg_url).await {
            Ok(conn_pool) => {
                eprintln!(
                    "[gt-mcp-server] GitHub push webhook on POST /api/v1/connection/github/webhook (HMAC-SHA256 verified from DB config; marks rig stale)"
                );
                let source = gt_vcs::GithubAppSource::Db(gt_vcs::PgGithubAppConfig::new(
                    conn_pool.clone(),
                ));
                let connections: Arc<dyn VcsConnectionRepo> =
                    Arc::new(gt_vcs::PgVcsConnections::new(conn_pool));
                // CI-gate forwarding (gtcore-52c9ec): when GT_CI_GATE_URL points at orchd's
                // metrics/CI-gate listener (e.g. http://gt-orch-server:9099), `pull_request.closed`
                // / `check_suite.completed` deliveries are forwarded to /ci-gate/{merged,failed} so
                // orchd drives the merge slot to its terminal state — replacing the old 60s PR poll.
                // Unset ⇒ those events are acknowledged and ignored (backward-compatible).
                let mut webhook_state = gt_composition::webhook::GithubWebhookState::new(
                    source,
                    Arc::new(WsPools::new(pg_url.clone())),
                    connections,
                    event_log.clone(),
                );
                if let Ok(ci_gate_url) = std::env::var("GT_CI_GATE_URL") {
                    webhook_state = webhook_state
                        .with_ci_forward(ci_gate_url, std::env::var("GT_CI_GATE_TOKEN").ok());
                    eprintln!(
                        "[gt-mcp-server] CI-gate forwarding on (pull_request.closed / check_suite.completed → orchd /ci-gate)"
                    );
                }
                Some(gt_composition::webhook::github_webhook_router(
                    webhook_state,
                ))
            }
            Err(e) => {
                eprintln!("[gt-mcp-server] GitHub push webhook off (PG connect failed: {e})");
                None
            }
        },
        Err(_) => {
            eprintln!("[gt-mcp-server] GitHub push webhook off (GT_PG_URL unset)");
            None
        }
    };

    // Email-outbox drain daemon (hq-f24599): claims due email_outbox rows
    // (pending|retry, send_at <= now) and delivers them through the configured
    // EmailTransport (GT_EMAIL_TRANSPORT: log default; smtp once the server
    // exists). NOT gated by the singleton daemons switch: claim_due runs
    // FOR UPDATE SKIP LOCKED, so concurrent drainers (API replicas) are
    // disjoint by construction — and the daemons pod runs gt-orch-server, a
    // different binary, so gating here would leave the outbox undrained
    // (observed on gt-dev, hq-562fbd). Tick via GT_EMAIL_DRAIN_TICK_SECS
    // (default 30, 0 = off).
    let email_tick = env_u64("GT_EMAIL_DRAIN_TICK_SECS", 30);
    match (email_tick > 0)
        .then_some(())
        .and(std::env::var("GT_PG_URL").ok())
    {
        Some(pg_url) => match sqlx::PgPool::connect(&pg_url).await {
            Ok(outbox_pool) => {
                let transport = gt_notify::transport_from_env();
                eprintln!(
                    "[gt-mcp-server] email-outbox drain on (every {email_tick}s; transport {})",
                    transport.label()
                );
                let drain_notifier = Some(
                    gt_composition::email_outbox_drain::DrainNotifier::new(
                        outbox_pool.clone(),
                        event_log.clone(),
                    ),
                );
                tokio::spawn(gt_composition::email_outbox_drain::run(
                    std::time::Duration::from_secs(email_tick),
                    outbox_pool,
                    transport,
                    drain_notifier,
                ));
            }
            Err(e) => eprintln!("[gt-mcp-server] email-outbox drain off (PG connect failed: {e})"),
        },
        None => eprintln!(
            "[gt-mcp-server] email-outbox drain off (daemons disabled, tick=0, or GT_PG_URL unset)"
        ),
    }

    // Graph drift-reconcile daemon (hq-vcs-connections.8): the BACKSTOP for the deliveries the push
    // webhook (.7) misses (App downtime, a dropped delivery, the App reinstalled, a network blip). On
    // a low cadence it sweeps every workspace partition, and for each rig under graph custody runs a
    // cheap `git ls-remote <git_url> refs/heads/<default_branch>` (private repos via a JIT
    // installation token minted from the rig's connection, public repos anonymously) and compares the
    // remote tip to the warden's last-indexed commit — on divergence it marks the rig stale so
    // `graph.refresh-stale` reindexes it. It NEVER clones or indexes, only flips the freshness flag.
    // Opt-in + configurable: off unless GT_GRAPH_DRIFT_TICK_SECS > 0 (default 3600 = hourly), and
    // gated on GT_PG_URL (the per-workspace rig catalog + connection store). The GitHub App is
    // optional — without one, only connectionless (public) rigs are reconciled.
    // Singleton backstop (hq-talos-migration.4): the master switch is ANDed on top of the daemon's
    // own GT_GRAPH_DRIFT_TICK_SECS off-switch, so the API tier never ls-remotes / flips freshness.
    let drift_tick = env_u64("GT_GRAPH_DRIFT_TICK_SECS", 3600);
    match (run_daemons && drift_tick > 0)
        .then_some(())
        .and(std::env::var("GT_PG_URL").ok())
    {
        Some(pg_url) => match sqlx::PgPool::connect(&pg_url).await {
            Ok(conn_pool) => {
                let connections: Arc<dyn VcsConnectionRepo> =
                    Arc::new(gt_vcs::PgVcsConnections::new(conn_pool.clone()));
                // hq-61ea43: the App client mints JIT installation tokens for private rigs; resolved
                // from the DB-backed config (env fallback). Absent/unconfigured → public rigs only.
                let github = gt_vcs::GithubAppSource::Db(gt_vcs::PgGithubAppConfig::new(conn_pool))
                    .load()
                    .await
                    .ok()
                    .flatten()
                    .map(gt_vcs::GithubAppClient::new);
                eprintln!(
                    "[gt-mcp-server] graph drift-reconcile daemon on (every {drift_tick}s; \
                     ls-remote vs indexed commit; GitHub App {})",
                    if github.is_some() { "wired" } else { "absent (public rigs only)" }
                );
                tokio::spawn(gt_composition::drift_reconcile::run(
                    std::time::Duration::from_secs(drift_tick),
                    event_log.clone(),
                    Arc::new(WsPools::new(pg_url)),
                    connections,
                    github,
                ));
            }
            Err(e) => {
                eprintln!("[gt-mcp-server] graph drift-reconcile off (PG connect failed: {e})");
            }
        },
        None => {
            if !run_daemons {
                eprintln!("[gt-mcp-server] graph drift-reconcile off (API tier, GT_RUN_DAEMONS=0)");
            } else if drift_tick == 0 {
                eprintln!("[gt-mcp-server] graph drift-reconcile off (GT_GRAPH_DRIFT_TICK_SECS=0)");
            } else {
                eprintln!("[gt-mcp-server] graph drift-reconcile off (GT_PG_URL unset)");
            }
        }
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
    if let Some(webhook) = github_webhook {
        // Nest under /api/v1/connection so the absolute path is
        // /api/v1/connection/github/webhook (the route inside is /github/webhook). Outside the
        // auth chain merged below — the HMAC signature is the credential.
        app = app.nest("/api/v1/connection", webhook);
    }
    // Inbound-email webhook (hq-6c6d16): the mail provider POSTs received
    // messages here; the receiver writes them into the command-mailbox dir the
    // FileInbox daemon above polls. Same opt-in env as the mailbox, plus the
    // shared secret. Outside the auth chain — the secret header is the
    // credential. Absolute path: /api/v1/email/inbound.
    if let Ok(dir) = std::env::var("GT_INBOUND_MAIL_DIR") {
        let secret = std::env::var("GT_INBOUND_WEBHOOK_SECRET").ok();
        if secret.is_none() {
            eprintln!(
                "[gt-mcp-server] inbound-email webhook UNCONFIGURED \
                 (GT_INBOUND_WEBHOOK_SECRET unset — endpoint answers 503)"
            );
        } else {
            eprintln!("[gt-mcp-server] inbound-email webhook on (-> {dir})");
        }
        app = app.nest(
            "/api/v1/email",
            gt_composition::inbound_email::inbound_email_router(
                gt_composition::inbound_email::InboundEmailState::new(secret, dir),
            ),
        );
    }
    if let Some(terminal) = terminal {
        app = app.merge(terminal);
    }
    if let Some(onboard) = onboard {
        app = app.merge(onboard);
    }
    if let Some(relogin) = relogin {
        app = app.merge(relogin);
    }
    if let Some(hooks) = hooks {
        app = app.merge(hooks);
    }
    if let Some(notifications) = notifications {
        app = app.merge(notifications);
    }
    if let Some(kanban) = kanban {
        app = app.merge(kanban);
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
    let eventlog_root = std::env::var("GT_EVENTLOG_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT));
    // The orchd dispatch channel dir (hq-agent-auto-dispatch.1): a POST /api/v1/agent with
    // role=polecat+crew drops a dispatch request on the channel the orchd scheduler consumes —
    // making the spawn actually sling a polecat. Resolved here (env), threaded into the assembly.
    let agent_dispatch_channel = match std::env::var("GT_CHANNEL_ROOT") {
        Ok(root) => {
            let name =
                std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
            eprintln!("[gt-mcp-server] agent→scheduler bridge on — dispatch channel {root}/{name}");
            Some(std::path::PathBuf::from(&root).join(&name))
        }
        Err(_) => {
            eprintln!("[gt-mcp-server] agent→scheduler bridge off — GT_CHANNEL_ROOT unset");
            None
        }
    };
    // The A2A gateway (B5, gtcore-9039b5) drops its dispatch orders on the SAME
    // channel dir — captured before `agent_dispatch_channel` moves into the REST
    // module parts below.
    let a2a_dispatch_channel = agent_dispatch_channel.clone();
    let accounts_root = gt_composition::account_dirs::accounts_root(&eventlog_root);
    let skills_seed_workspace =
        std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".to_string());

    // The Postgres-gated module slice (workspace/rig/connection/graph/documents). Resolve the
    // eager pool + the env-derived GitHub App / blob store / embedder HERE (the bin owns the env),
    // then hand the pre-built parts to `build_rest_modules`. `None` ⇒ the GT_PG_URL-unset branch.
    // The eager `pool` (kept for parity with the prior behaviour) also backs the /me/stats membership
    // pool below. `pg_url_for_me` is captured before `pg_url` moves into the parts.
    //
    // The public, unauthenticated share-read surface (hq-web-extras.9) rides back from the assembly;
    // `me_pg` carries the eager PG url+pool the /me/stats surface needs (its connect is eager, so it
    // is mounted by main() after the pure assembly returns).
    let public_share: Option<axum::Router>;
    let mut me_pg_url: Option<String> = None;
    let rest_pg = if let Ok(pg_url) = std::env::var("GT_PG_URL") {
        let pool = sqlx::PgPool::connect(&pg_url)
            .await
            .context("GT_PG_URL must point at a reachable Postgres (REST backings)")?;
        // The platform GitHub App is now DB-backed (hq-61ea43): the install/config routes resolve it
        // from `public.github_app_config` per request (env fallback when no row), so it is configured
        // from the UI with no redeploy. Pass the source, not a pre-resolved client.
        let github = gt_vcs::GithubAppSource::Db(gt_vcs::PgGithubAppConfig::new(pool.clone()));
        let (blob, bucket) = build_blob_store();
        // Capture the PG url for the cross-workspace /me/stats membership pool (built below), which
        // needs an EAGER connect and so is mounted by main() after the assembly returns.
        me_pg_url = Some(pg_url.clone());
        Some(gt_composition::rest_modules::RestPgParts {
            pool,
            pg_url,
            issues_workspaces: issues_workspaces.clone(),
            github,
            blob,
            bucket,
            extractor: Extractor::without_ocr(),
            embedder: build_embedder(),
        })
    } else {
        eprintln!(
            "[gt-mcp-server] REST domain modules: meta + agent + quota + merge + skills + feed + convoy (GT_PG_URL unset → no workspace/rig/documents)"
        );
        None
    };

    // Assemble the SAME REST module set the boot-smoke test pins (hq-vcs-connections.12): one
    // `build_rest_modules` call, shared by main() and `tests/boot_smoke.rs`, so the test exercises
    // the exact wiring — the seam that would have caught the `.9` `graph.query` MalformedToolName
    // crash before it left CI. `public_share` rides back for the public ops router.
    let (mut rest, share) = gt_composition::rest_modules::build_rest_modules(
        gt_composition::rest_modules::RestModuleParts {
            meta_store,
            actor: actor.clone(),
            meta_tools,
            dispatch_channel: agent_dispatch_channel,
            event_log: event_log.clone(),
            accounts_root,
            sync_prober: Some(Arc::new(gt_composition::usage_probe::EventLogSyncProber::new())),
            skills_seed_workspace,
            pg: rest_pg,
        },
    )
    .await;
    public_share = share;

    // Cross-workspace self-view (hq-web-extras.15): GET /api/v1/me/stats rolls up issue progress
    // across every workspace the caller is a member of. It needs an EAGER `WorkspacePool::connect`,
    // so it is mounted here (not inside the pure `build_rest_modules`), only with GT_PG_URL +
    // GT_DOLT_BASE_URL — without the latter, single-tenant Dolt has no per-workspace stores to
    // aggregate.
    if let Some(pg_url_for_me) = me_pg_url {
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
                    "[gt-mcp-server] REST domain modules: meta + workspace + rig + connection + graph + documents + memory + agent + quota + merge + skills + feed + convoy + me (cross-workspace stats)"
                );
            }
            Err(_) => eprintln!(
                "[gt-mcp-server] REST domain modules: meta + workspace + rig + connection + graph + documents + memory + agent + quota + merge + skills + feed + convoy (GT_DOLT_BASE_URL unset → no /me/stats cross-workspace surface)"
            ),
        }
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
                // hq-epic.auth-refactor.2: the optional `workspace_id` column on
                // `public.oauth_providers`, so the per-workspace provider query (`COLS` includes
                // `workspace_id`) has a column to read — without it `GET /auth/providers` 500s with
                // "column workspace_id does not exist". ALTER ... ADD COLUMN IF NOT EXISTS, idempotent.
                gt_auth::migrations::ADD_PROVIDER_WORKSPACE,
                // hq-epic.auth-refactor.4: make `public.users.password_hash` nullable so JIT-provisioned
                // SSO users (no password) can be inserted. ALTER ... DROP NOT NULL, idempotent.
                gt_auth::migrations::SSO_USER_PASSWORD_NULLABLE,
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
            // hq-greenfield-seeds.3: replay the versioned, NON-SECRET OAuth/IdP provider config
            // (extracted from live prod, `seeds/oauth-providers.json`) into an EMPTY oauth_providers
            // table, so a greenfield deploy comes up with its login providers instead of a blank
            // login page. Idempotent (skips when the table is non-empty, never clobbers a curated
            // prod) and secret-gated (each provider's client_secret is read from its named env var;
            // unset => that provider is skipped). Only with the `oauth` feature (the provider store
            // + AES-GCM crypto that seals the secret at rest).
            #[cfg(feature = "oauth")]
            seed_oauth_providers(pool.clone()).await?;
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

    // A2A ingress (B5, gtcore-9039b5 / epic gtcore-155917): `POST /a2a` (JSON-RPC
    // tasks/send|get|cancel|sendSubscribe over the A2aGateway) + the public
    // `GET /.well-known/agent.json` discovery card. Opt-in wiring — mounted ONLY
    // when ALL of these are present (see gt_composition::a2a's module doc):
    //   - GT_A2A_DEFAULT_RIG + GT_A2A_INTAKE_EPIC — intake defaults for minted beads;
    //   - GT_A2A_ORCHD_URL (+ optional GT_A2A_ORCHD_TOKEN) — the orchd agent REST
    //     surface tasks/cancel kills sessions through; without a token one is
    //     minted with the platform RS256 key (agent.read/agent.write scopes);
    //   - GT_CHANNEL_ROOT — the dispatch channel the minted bead's order drops on;
    //   - the RS256 verifier — POST /a2a sits behind the SAME PAT/JWT authenticator
    //     as /mcp and /api/v1/* (the card endpoint stays public per spec).
    // GT_PUBLIC_URL names the card's served origin (default http://<bind>).
    let app = match (
        gt_composition::a2a::a2a_env(|k| std::env::var(k).ok()),
        &verifier,
        &a2a_dispatch_channel,
    ) {
        (Some(a2a), Some(v), Some(channel)) => {
            // One AgentSkill per catalog rig (single-tenant: the GT_WORKSPACE
            // schema). No PG / empty catalog ⇒ the default rig is the one skill,
            // so the card never advertises nothing.
            let a2a_ws = std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".to_string());
            let mut rigs: Vec<gt_rig::RigEntry> = match &scope_sources {
                Some((_, rig_provider)) => match rig_provider.repo(&a2a_ws).await {
                    Ok(repo) => repo.list().await.unwrap_or_default(),
                    Err(_) => vec![],
                },
                None => vec![],
            };
            if rigs.is_empty() {
                rigs.push(gt_rig::RigEntry::new(
                    a2a.rig.clone(),
                    a2a.rig.clone(),
                    String::new(),
                    "main",
                    0,
                ));
            }
            let public_url =
                std::env::var("GT_PUBLIC_URL").unwrap_or_else(|_| format!("http://{bind}"));
            let card = gt_composition::a2a::agent_card(&public_url, &rigs);
            // Sign with the deploy's existing RS256 signing key; a deploy without
            // one serves the card unsigned (discovery still works, just unattested).
            let opt_minter = JwtMinter::from_env().ok();
            let card = match &opt_minter {
                Some(minter) => match gt_composition::a2a::sign_card(card.clone(), minter) {
                    Ok(signed) => {
                        eprintln!("[gt-mcp-server] A2A agent card signed (RS256 JWS)");
                        signed
                    }
                    Err(e) => {
                        eprintln!("[gt-mcp-server] A2A agent card UNSIGNED (sign failed: {e})");
                        card
                    }
                },
                None => {
                    eprintln!("[gt-mcp-server] A2A agent card UNSIGNED (no signing key)");
                    card
                }
            };
            // Per-rig cards (A2, gtcore-4023de): GET /.well-known/agent/<rig>[.json].
            // Pre-sign each with the same key (or serve unsigned when no minter).
            let rig_cards: std::collections::HashMap<String, gt_a2a::AgentCard> = rigs
                .iter()
                .map(|r| {
                    let c = gt_composition::a2a::rig_agent_card(&public_url, r);
                    let c = match &opt_minter {
                        Some(m) => gt_composition::a2a::sign_card(c.clone(), m).unwrap_or(c),
                        None => c,
                    };
                    (r.name.clone(), c)
                })
                .collect();
            // tasks/cancel authenticates against the orchd agent REST surface with
            // the configured token, else a token minted with the platform key —
            // the same key that verifier accepts, so the kill rides the normal
            // auth chain. Boot-lifetime mint: ttl via GT_A2A_ORCHD_TOKEN_TTL_SECS
            // (default one year).
            let orchd_token = a2a.orchd_token.clone().or_else(|| {
                let minter = opt_minter.as_ref()?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let ttl = env_u64("GT_A2A_ORCHD_TOKEN_TTL_SECS", 31_536_000);
                minter
                    .mint(&gt_auth::JwtClaims {
                        sub: "a2a-gateway".into(),
                        workspace: a2a_ws.clone(),
                        scopes: vec!["agent.read".into(), "agent.write".into()],
                        exp: now + ttl,
                        nbf: None,
                        iat: now,
                    })
                    .ok()
            });
            // External intake lands as meta.gap — the "work arrived from outside
            // the planning loop" bucket — until the taxonomy grows a dedicated
            // a2a/intake discriminator.
            let gateway = gt_composition::a2a::A2aGateway::new(
                Arc::new(gt_composition::a2a::DoltIntake::new(
                    store.clone(),
                    "a2a".into(),
                    vec![gt_issues::Domain::MetaGap],
                )),
                Arc::new(gt_composition::a2a::ChannelDispatch::new(channel.clone())),
                Arc::new(gt_composition::a2a::DoltTaskStore::new(store.clone())),
                Arc::new(gt_composition::a2a::RestSessionControl::new(
                    a2a.orchd_url.clone(),
                    orchd_token,
                )),
                Arc::new(gt_composition::a2a::LogEventFeed::new(
                    event_log.clone(),
                    Some(a2a_ws.clone()),
                )),
                gt_composition::a2a::A2aGatewayConfig {
                    rig: a2a.rig.clone(),
                    parent_id: a2a.parent_id.clone(),
                    created_by: "a2a".into(),
                    domain: vec![gt_issues::Domain::MetaGap],
                    poll: std::time::Duration::from_secs(1),
                },
            )
            // A6 (gtcore-46c9dc): wire the human-escalation view over the SAME event log the
            // escalate.* tools write. tasks/get projects input-required while a session is
            // blocked; a tasks/send on a blocked task id resolves the escalation and re-activates
            // the SAME bead instead of minting a new one.
            .with_escalations(Arc::new(gt_composition::a2a::EventLogEscalations::new(
                event_log.clone(),
                Some(a2a_ws.clone()),
            )));
            // The SAME PAT port the /mcp transport and /api/v1/* authenticate
            // gtpat_… bearers through; the card route stays outside the guard.
            let mut a2a_auth = AuthState::new(v.clone(), audit.clone());
            if let Some(pv) = pat_verifier.clone() {
                a2a_auth = a2a_auth.with_pat(pv);
            }
            eprintln!(
                "[gt-mcp-server] A2A on POST /a2a (PAT/JWT guarded) + GET /.well-known/agent.json (public; {} skill(s); {} per-rig cards at /.well-known/agent/<rig>; intake {} → rig {}; orchd {})",
                rigs.len(),
                rig_cards.len(),
                a2a.parent_id,
                a2a.rig,
                a2a.orchd_url,
            );
            app.merge(gt_composition::a2a::a2a_app(card, Arc::new(gateway), a2a_auth, rig_cards))
        }
        (env_cfg, v, channel) => {
            let mut missing: Vec<&str> = vec![];
            if env_cfg.is_none() {
                missing.push("GT_A2A_DEFAULT_RIG/GT_A2A_INTAKE_EPIC/GT_A2A_ORCHD_URL");
            }
            if v.is_none() {
                missing.push("RS256 verifier");
            }
            if channel.is_none() {
                missing.push("GT_CHANNEL_ROOT");
            }
            eprintln!("[gt-mcp-server] A2A off (missing: {})", missing.join(", "));
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
    use gt_vcs::VcsModule;

    let workspace_id = ModuleId::new("workspace").expect("`workspace` is a valid module id");
    let feature_id = ModuleId::new("feature").expect("`feature` is a valid module id");
    let rig_id = ModuleId::new("rig").expect("`rig` is a valid module id");
    let docs_id = ModuleId::new("docs").expect("`docs` is a valid module id");
    let comments_id = ModuleId::new("comments").expect("`comments` is a valid module id");
    let email_id = ModuleId::new("email").expect("`email` is a valid module id");
    let invites_id = ModuleId::new("invites").expect("`invites` is a valid module id");
    let memory_id = ModuleId::new("memory").expect("`memory` is a valid module id");
    let notifications_id = ModuleId::new("notifications").expect("`notifications` is a valid module id");
    // hq-talos-migration.10: the GLOBAL `public.events` table — the Postgres-backed EventStore that
    // decouples mcp-server/orchd from the shared event-log volume. A `public` catalog (the
    // `workspace` column partitions it), NOT a per-tenant template table, so it seeds here in the
    // public-schema apply, never in the ws_default clone path — exactly like notifications.
    let events_id = ModuleId::new("events").expect("`events` is a valid module id");
    // hq-talos-migration.11: the GLOBAL `public.dispatch_jobs` queue — the Postgres-backed dispatch
    // channel that decouples mcp-server/orchd from the shared GT_CHANNEL_ROOT volume. A `public`
    // catalog (the `channel` column keys it), NOT a per-tenant template, so it seeds here.
    let dispatch_id = ModuleId::new("dispatch").expect("`dispatch` is a valid module id");
    // hq-vcs-connections.1: the GLOBAL `public.vcs_connections` store (mirrors public.oauth_providers
    // + a workspace_id column). A `public` catalog like notifications — NOT a per-tenant template
    // table — so it seeds here in the public-schema apply, never in the ws_default clone path.
    let connection_id = VcsModule::id();
    let workspace_migs = gt_store_pg::workspace_migrations();
    let feature_migs = gt_store_pg::feature_flags_migrations();
    let rig_migs = RigsModule.migrations();
    let connection_migs = VcsModule.migrations();
    // hq-docs-store.1: the per-workspace `documents` template tables (docs/11). Like `rig`,
    // they seed the `ws_default` template so `gt_create_workspace_schema` clones them per tenant.
    let docs_migs = gt_store_pg::docs_migrations();
    // hq-57042e: the per-workspace `comments` template table (threaded card|doc comments).
    let comments_migs = gt_store_pg::comments_migrations();
    // hq-f24599: the public-schema email_outbox the programmed-send pipeline drains.
    let email_migs = gt_store_pg::email_migrations();
    // hq-4231c1: the public-schema workspace_invites the collaborator flow consumes.
    let invites_migs = gt_store_pg::invites_migrations();
    // hq-memory-mcp.1: the per-workspace `memories` template table (semantic agent memory).
    // Like `documents`, it seeds the `ws_default` template so it is cloned per tenant.
    let memory_migs = gt_store_pg::memory_migrations();
    // notifications: the public-schema `notifications` table agents write to via notify.send.execute.
    let notifications_migs = gt_store_pg::notifications_migrations();
    // hq-talos-migration.10: the public-schema `events` table backing the PG EventStore. One
    // idempotent migration (CREATE TABLE/INDEX IF NOT EXISTS); the same DDL `PgEventStore::
    // ensure_schema` self-heals with, registered here so a fresh deploy needs no operator step.
    let events_migs = vec![gt_module::Migration::new(
        1,
        "0001_events",
        gt_eventlog::events_migration_sql(),
    )];
    // hq-talos-migration.11: the public-schema `dispatch_jobs` queue backing the PG dispatch
    // channel (convoy→scheduler). One idempotent migration (CREATE TABLE/INDEX IF NOT EXISTS); the
    // same DDL `PgQueue::ensure_schema` self-heals with, registered here so a fresh deploy needs no
    // operator step. A `public` catalog (the `channel` column keys it) like `events`/notifications.
    let dispatch_migs = vec![gt_module::Migration::new(
        1,
        "0001_dispatch_jobs",
        gt_channel::dispatch_migration_sql(),
    )];

    let plan: Vec<_> = workspace_migs
        .iter()
        .map(|m| (&workspace_id, m))
        .chain(feature_migs.iter().map(|m| (&feature_id, m)))
        .chain(rig_migs.iter().map(|m| (&rig_id, m)))
        .chain(docs_migs.iter().map(|m| (&docs_id, m)))
        .chain(comments_migs.iter().map(|m| (&comments_id, m)))
        .chain(email_migs.iter().map(|m| (&email_id, m)))
        .chain(invites_migs.iter().map(|m| (&invites_id, m)))
        .chain(memory_migs.iter().map(|m| (&memory_id, m)))
        .chain(notifications_migs.iter().map(|m| (&notifications_id, m)))
        .chain(events_migs.iter().map(|m| (&events_id, m)))
        .chain(dispatch_migs.iter().map(|m| (&dispatch_id, m)))
        .chain(connection_migs.iter().map(|m| (&connection_id, m)))
        .collect();

    // Self-heal the `ws_default` per-workspace TEMPLATE tables before applying the plan
    // (gtcore-a80f74 for `rigs`, gtcore-c9b292 for the projection tables). The migration tracking
    // table lives in `public` and survives a `DROP SCHEMA ws_default CASCADE` (the
    // tenant-reprovision / data-wipe path), so after such a drop `gt_module_migrate::apply` would
    // SKIP the already-recorded migrations (never recreating the dropped tables). For `rigs` that
    // then aborts boot when a pending follow-on `ALTER TABLE ws_default.rigs ADD COLUMN …` hits
    // `relation "ws_default.rigs" does not exist`, crashlooping the whole server; for the
    // projection tables (`comments`, the `documents` family, `memories`) it silently leaves the
    // feature 500ing with `relation "…" does not exist` forever — the desync the comments wipe of
    // 2026-06-28 surfaced. Replaying every template module's fully idempotent DDL here
    // UNCONDITIONALLY guarantees the tables are present and complete regardless of what the
    // tracking table claims — the same belt-and-suspenders self-heal `events`/`dispatch` already
    // do via `ensure_schema`. The DDL is `CREATE …/ALTER … IF NOT EXISTS` only, so it never
    // destroys data or clobbers existing rows, and a boot against an intact DB is a cheap
    // catalog-check no-op. (Non-`default` tenants are healed separately by
    // `reconcile_tenant_schemas`, which clones these freshly-ensured template tables into each
    // `ws_<slug>` via `gt_create_workspace_schema`.)
    //
    // Run under a transaction-scoped advisory lock: this fires on EVERY boot across N mcp-server
    // replicas (plus parallel tests), and concurrent `CREATE TABLE/ALTER … IF NOT EXISTS` against
    // the same object races in Postgres. The lock makes provisioning a single-writer critical
    // section; the key is arbitrary-but-fixed so every process contends on the same lock.
    const TEMPLATE_DDL_LOCK: i64 = 0x6774_7269_0001; // "gtri" + 1
    let rigs_ensure = gt_rig::RigsModule::template_ensure_sql();
    let projection_ensure = gt_store_pg::projection_template_ensure_sql();
    sqlx::raw_sql(&format!(
        "BEGIN; SELECT pg_advisory_xact_lock({TEMPLATE_DDL_LOCK}); {rigs_ensure}\n{projection_ensure} COMMIT;"
    ))
    .execute(pool)
    .await
    .context("self-heal ws_default template tables before migrations")?;

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

/// Heal per-tenant schema drift on boot (hq-vcs-connections.13).
///
/// The per-workspace template tables (`rig`, `documents`, `memories`, …) are migrated ONLY into
/// the `ws_default` template by [`apply_pg_catalog`]. `gt_create_workspace_schema` clones that
/// template into a tenant's `ws_<slug>` schema, but ONLY when the tenant is first provisioned —
/// so any template migration added AFTER a tenant was created never reaches that tenant. The
/// concrete failure: rig/0003 added `git_connection_ref` to `ws_default.rigs`, but the existing
/// `ws_confiar.rigs` never got the column, so `rig.list` for ws=confiar errored with
/// `column "git_connection_ref" does not exist`.
///
/// This reconciles every existing tenant schema against the `ws_default` template, generically:
///   1. `gt_create_workspace_schema(slug)` — re-run the cloner, which is `CREATE TABLE IF NOT
///      EXISTS ... LIKE`, so it adds any NEW template TABLE the tenant lacks (and is a no-op for
///      tables it already has).
///   2. For every template table the tenant also has, diff `information_schema.columns` and
///      `ALTER TABLE ... ADD COLUMN` each column present in the template but missing in the
///      tenant, copying the template column's type, nullability, and default. `LIKE` cannot add a
///      column to an already-existing table, so this closes the column-level drift.
///
/// Idempotent — it only adds what is missing, so a second boot against a reconciled DB is a no-op.
/// Automatic so a deploy heals drift, mirroring how the `ws_default` migrations already
/// auto-apply. We deliberately do NOT drop or alter columns the tenant has but the template lacks:
/// reconcile only adds, never destroys, so an in-flight schema is never truncated.
async fn reconcile_tenant_schemas(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // Drive off the workspace catalog (same source of truth as `migrate_users_to_global`). On a
    // brand-new DB the catalog may not exist yet — but then there are no tenants to reconcile.
    let has_catalog: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'workspaces')",
    )
    .fetch_one(pool)
    .await
    .context("reconcile: probe workspace catalog")?;
    if !has_catalog {
        return Ok(());
    }

    const TEMPLATE: &str = "ws_default";
    let slugs: Vec<String> = sqlx::query_scalar("SELECT slug FROM public.workspaces ORDER BY slug")
        .fetch_all(pool)
        .await
        .context("reconcile: list workspaces")?;

    let mut columns_added = 0usize;
    for slug in slugs {
        let schema = schema_for(&slug);
        // The schema name is interpolated into DDL below; catalog slugs are system-minted, but
        // prove it is a plain `ws_*` identifier first (same guard as the user migration).
        if !is_safe_schema_ident(&schema) {
            continue;
        }
        // `ws_default` IS the template — it is migrated directly by `apply_pg_catalog`, so
        // reconciling it against itself is pointless (and the `default` workspace maps to it).
        if schema == TEMPLATE {
            continue;
        }

        // Step 1: pick up any NEW template tables (no-op for tables the tenant already has).
        sqlx::query("SELECT gt_create_workspace_schema($1)")
            .bind(&slug)
            .execute(pool)
            .await
            .with_context(|| format!("reconcile: clone template into {schema}"))?;

        // Step 2: add columns present in the template table but missing from the tenant's copy.
        // `format_type(atttypid, atttypmod)` renders the canonical type (incl. precision/length),
        // and `pg_get_expr(adbin, adrelid)` the default expression, both straight from the
        // template's catalog so the tenant column matches byte-for-byte.
        let missing = sqlx::query(MISSING_COLUMNS_SQL)
            .bind(TEMPLATE)
            .bind(&schema)
            .fetch_all(pool)
            .await
            .with_context(|| format!("reconcile: diff columns for {schema}"))?;
        for row in &missing {
            let table: String = row.try_get("table_name").context("reconcile: table_name")?;
            let column: String = row.try_get("column_name").context("reconcile: column_name")?;
            let coltype: String = row.try_get("coltype").context("reconcile: coltype")?;
            let not_null: bool = row.try_get("not_null").context("reconcile: not_null")?;
            let default: Option<String> = row.try_get("default_expr").context("reconcile: default")?;

            // Identifiers come from the catalog (already validated for the schema); table/column
            // names are quoted, the type/default are catalog-rendered SQL fragments. A NOT NULL
            // column with no default cannot be added to a table that may hold rows, so we only
            // enforce NOT NULL when the template also supplies a default to backfill with.
            let mut ddl = format!(
                "ALTER TABLE {schema}.\"{table}\" ADD COLUMN IF NOT EXISTS \"{column}\" {coltype}"
            );
            if let Some(expr) = &default {
                ddl.push_str(" DEFAULT ");
                ddl.push_str(expr);
            }
            if not_null && default.is_some() {
                ddl.push_str(" NOT NULL");
            }
            sqlx::query(&ddl)
                .execute(pool)
                .await
                .with_context(|| format!("reconcile: add {schema}.{table}.{column}"))?;
            columns_added += 1;
        }
    }
    if columns_added > 0 {
        eprintln!(
            "[gt-mcp-server] tenant schema reconcile: {columns_added} column(s) backfilled from {TEMPLATE}"
        );
    }
    Ok(())
}

/// Columns that exist in a template table (`$1`) but are missing from the same table in a tenant
/// schema (`$2`), with the canonical type, NOT NULL flag, and default expression read straight
/// from the template's catalog so a re-added column matches the template exactly. Only considers
/// tables that exist in BOTH schemas — new tables are handled by `gt_create_workspace_schema`.
const MISSING_COLUMNS_SQL: &str = "\
SELECT c.relname AS table_name, \
       a.attname AS column_name, \
       format_type(a.atttypid, a.atttypmod) AS coltype, \
       a.attnotnull AS not_null, \
       pg_get_expr(ad.adbin, ad.adrelid) AS default_expr \
FROM pg_attribute a \
JOIN pg_class c ON c.oid = a.attrelid \
JOIN pg_namespace n ON n.oid = c.relnamespace \
LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum \
WHERE n.nspname = $1 \
  AND c.relkind = 'r' \
  AND a.attnum > 0 \
  AND NOT a.attisdropped \
  AND EXISTS ( \
      SELECT 1 FROM pg_class tc \
      JOIN pg_namespace tn ON tn.oid = tc.relnamespace \
      WHERE tn.nspname = $2 AND tc.relname = c.relname AND tc.relkind = 'r' \
  ) \
  AND NOT EXISTS ( \
      SELECT 1 FROM pg_attribute ta \
      JOIN pg_class tc ON tc.oid = ta.attrelid \
      JOIN pg_namespace tn ON tn.oid = tc.relnamespace \
      WHERE tn.nspname = $2 AND tc.relname = c.relname \
        AND ta.attname = a.attname AND ta.attnum > 0 AND NOT ta.attisdropped \
  ) \
ORDER BY c.relname, a.attnum";

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
    Option<Arc<gt_composition::report_scheduler::ReportService>>,
)> {
    let Ok(pg_url) = std::env::var("GT_PG_URL") else {
        eprintln!("[gt-mcp-server] GT_PG_URL unset; domain dispatch disabled (issues + meta only)");
        return Ok((DomainRouter::new(), None, None, None, None));
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
    // hq-vcs-connections.13: `apply_pg_catalog` migrates the `ws_default` TEMPLATE, but
    // `gt_create_workspace_schema` only clones it into a tenant when that tenant is FIRST
    // provisioned — so a template migration landed after a tenant was created (e.g. rig/0003's
    // `git_connection_ref`) never reaches the already-existing `ws_<slug>` schemas, and
    // `rig.list` against that tenant 500s with `column git_connection_ref does not exist`
    // (observed for ws=confiar). Heal that drift on every boot, idempotently, so a deploy is the
    // fix — generic over any future per-tenant template change, not a one-off ALTER.
    reconcile_tenant_schemas(&pool).await?;
    // hq-greenfield-seeds.5: replay the versioned, declarative rig catalog (extracted from live
    // prod, `seeds/rigs.json`) into an EMPTY per-workspace `rigs` table, so a greenfield deploy
    // comes up with its rigs (gt/gt_core/gtmcp/gtproxy/gtweb) instead of an empty catalog that
    // needs a manual `rig.add` per rig. Idempotent (skips when the table is non-empty, never
    // clobbers a curated prod) and connectionless (the live extract binds no vcs_connections ref —
    // SSH clone — so it depends on no runtime GitHub-App artifact). Scoped to the `ws_default`
    // template schema, exactly where the prod rigs live; a clean skip if its pool can't connect.
    seed_rigs(&pg_url).await?;
    // Clone the URL before it moves into WsPools — the convoy→scheduler PG queue (below) reuses it.
    let dispatch_pg_url = pg_url.clone();
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
    // event bus — the dispatch channel is the IPC. Wire the ConvoyHandler to drop a
    // {bead,priority} request onto the same channel the orchd dispatch loop consumes, so a
    // convoy.launch actually slings a polecat.
    //
    // hq-talos-migration.11 — REVERSIBLE BY ENV (same opt-in as the event log, .10): the PG-backed
    // queue (`public.dispatch_jobs`) is selected ONLY when GT_EVENTLOG_PG is truthy (1/true/yes/on);
    // it is deliberately NOT triggered by GT_PG_URL alone (prod sets GT_PG_URL for the domain
    // handlers but must stay on the file channel). Otherwise the existing file-based channel under
    // GT_CHANNEL_ROOT is used, EXACTLY as before — flipping GT_EVENTLOG_PG off restores it. With the
    // PG queue, mcp-server and orchd share NO filesystem for dispatch (the .11 decoupling).
    // Build the shared dispatch sink once — convoy AND agent both bridge through it so
    // convoy.launch members and agent.spawn(bead=…) both sling real polecats via orchd.
    let dispatch_sink: Option<Arc<gt_channel::DispatchSink>> = {
        let dispatch_name =
            std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
        let want_pg = std::env::var("GT_EVENTLOG_PG")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if want_pg {
            match gt_channel::PgQueue::connect(&dispatch_pg_url, &dispatch_name)
                .and_then(|q| q.ensure_schema().map(|()| q))
            {
                Ok(queue) => {
                    eprintln!(
                        "[gt-mcp-server] convoy/agent→scheduler bridge on (Postgres queue public.dispatch_jobs, channel {dispatch_name}) — GT_EVENTLOG_PG"
                    );
                    Some(Arc::new(gt_channel::DispatchSink::Pg(queue)))
                }
                Err(e) => {
                    eprintln!(
                        "[gt-mcp-server] convoy/agent→scheduler bridge off — PG queue init failed: {e}"
                    );
                    None
                }
            }
        } else {
            match std::env::var("GT_CHANNEL_ROOT") {
                Ok(root) => match gt_channel::Channel::open(&root, &dispatch_name) {
                    Ok(channel) => {
                        eprintln!(
                            "[gt-mcp-server] convoy/agent→scheduler bridge on (file channel {root}/{dispatch_name}; set GT_EVENTLOG_PG=1 for the Postgres queue)"
                        );
                        Some(Arc::new(gt_channel::DispatchSink::File(channel)))
                    }
                    Err(e) => {
                        eprintln!(
                            "[gt-mcp-server] convoy/agent→scheduler bridge off — channel open failed at {root}/{dispatch_name}: {e}"
                        );
                        None
                    }
                },
                Err(_) => {
                    eprintln!(
                        "[gt-mcp-server] convoy/agent→scheduler bridge off — GT_CHANNEL_ROOT unset"
                    );
                    None
                }
            }
        }
    };
    let convoy_handler = {
        let handler = ConvoyHandler::new(event_log.clone());
        match &dispatch_sink {
            Some(sink) => handler.with_dispatch_channel(sink.clone()),
            None => handler,
        }
    };
    // graph.* server-side provisioner (hq-vcs-connections.4): graph.refresh no longer trusts a
    // caller `repo_dir`. The handler derives <GT_GRAPH_ROOT>/<ws>/<rig>, resolves the rig's
    // git_connection_ref → public.vcs_connections, mints a JIT GitHub App installation token, and
    // clones/fetches the default branch into that managed-volume path before indexing. The vcs
    // connections live in the GLOBAL public table (the same PgVcsConnections the REST CRUD uses);
    // the GitHub App client is built only when GT_GITHUB_APP_* is configured (else the provisioner
    // degrades to indexing whatever already sits at the derived path — the legacy mounted rigs).
    let graph_provisioner = {
        let connections: Arc<dyn VcsConnectionRepo> =
            Arc::new(gt_vcs::PgVcsConnections::new(pool.clone()));
        // hq-61ea43: resolve the App client from the DB-backed config (env fallback) at boot. The
        // provisioner mints JIT tokens for private rigs; an unconfigured App ⇒ public-rig-only.
        let github = gt_vcs::GithubAppSource::Db(gt_vcs::PgGithubAppConfig::new(pool.clone()))
            .load()
            .await
            .ok()
            .flatten()
            .map(gt_vcs::GithubAppClient::new);
        gt_composition::mcp::RigProvisioner::new(ws_pools.clone(), connections, github)
    };
    let router = DomainRouter::new()
        .register(Arc::new(workspace_handler))
        // rig-hold H1: the rig handler emits `rig.held.v1` / `rig.resumed.v1` to the shared event
        // log so the operator's hold/resume is auditable + visible on the SSE feed.
        .register(Arc::new(
            RigHandler::new(ws_pools.clone())
                .with_event_sink(Arc::new(EventLogRigSink::new(event_log.clone()))),
        ))
        // A completed merge marks the owning rig's graph stale (hq-graphrig.7).
        .register(Arc::new(
            MergeHandler::new(event_log.clone()).with_rig_pools(ws_pools.clone()),
        ))
        .register(Arc::new(convoy_handler))
        .register(Arc::new({
            let handler = AgentHandler::new(event_log.clone());
            match &dispatch_sink {
                Some(sink) => handler.with_dispatch_channel(sink.clone()),
                None => handler,
            }
        }))
        .register(Arc::new(QuotaHandler::new(event_log.clone()).with_accounts_root(
            gt_composition::account_dirs::accounts_root(
                &std::env::var("GT_EVENTLOG_ROOT")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_EVENTLOG_ROOT)),
            ),
        )))
        // notify.* — operator notification channel (hq-notifications): agents write
        // via notify.send; the browser bell polls/streams the same PG table.
        .register(Arc::new(NotifyHandler::new(pool.clone(), event_log.clone())))
        // escalate.* — human escalation for autonomous agents (A6, gtcore-46c9dc):
        // agents call escalate.request when they need human input; operators respond
        // via escalate.respond. Approved beads are re-dispatched via the scheduler.
        .register(Arc::new({
            let handler = EscalateHandler::new(event_log.clone());
            match &dispatch_sink {
                Some(sink) => handler.with_dispatch_channel(sink.clone()),
                None => handler,
            }
        }))
        // email.* — the programmed-send outbox (hq-f24599): schedule/list/cancel;
        // the drain daemon (spawned in main, gated like the other daemons) delivers
        // through the gt-notify EmailTransport seam.
        .register(Arc::new(EmailHandler::new(pool.clone())))
        // invite.* — collaborator invites (hq-4231c1): mint/list/revoke/accept;
        // the token mails via the outbox, the accept binds the gt-login identity
        // to the membership (workspace_member-add under the hood).
        .register(Arc::new(InvitesHandler::new(
            pool.clone(),
            std::env::var("GT_PUBLIC_URL").ok(),
        )))
        // graph.* read-only queries (hq-graphrig.10) + server-side refresh (hq-vcs-connections.4):
        // graphify-backed indexer; the warden state (replayed from event_log) resolves rig ->
        // repo_dir, and the provisioner clones/fetches from the rig's VCS connection on refresh.
        .register(Arc::new(
            GraphHandler::new(event_log.clone(), Arc::new(GraphifyIndexer::new()))
                .with_provisioner(graph_provisioner),
        ));

    // documents.* dispatch (hq-docs-api.2, docs/11): .md content + binary attachments a model
    // reads as context. The blob store is wired from GT_BLOB_* when set; unset ⇒ md-only
    // (blob attach errors, .md still works). Extraction runs without OCR in the default build
    // (the tesseract OcrEngine is behind the `ocr-tesseract` feature, docs/11).
    let (blob, bucket) = build_blob_store();
    // Build the embedder ONCE and share it across documents.* and memory.*: the
    // fastembed model is heavy (an ONNX load), so loading it twice would double the
    // memory + startup cost for no benefit. `Option<Arc<dyn Embedder>>` is cheap to
    // clone (a refcount bump), so both handlers ride the same engine.
    let embedder = build_embedder();
    let documents_handler = Arc::new(
        DocumentsHandler::new(
            ws_pools.clone(),
            blob,
            bucket,
            Extractor::without_ocr(),
            embedder.clone(),
        )
        // hq-0c8fe1: document mutations broadcast documents.*.v1 frames the
        // doc:{id} SSE topic delivers (post-commit, best-effort).
        .with_event_log(event_log.clone()),
    );
    let router = router.register(documents_handler.clone());
    // hq-c488cb: one-shot RAG backfill — chunk + embed pre-existing docs that
    // have text but no chunk index yet (default workspace; per-tenant docs
    // backfill on their next write). Spawned so boot never blocks on it.
    if embedder.is_some() {
        let backfill = documents_handler.clone();
        tokio::spawn(async move {
            let n = backfill.backfill_chunks(None).await;
            if n > 0 {
                eprintln!("[gt-mcp-server] doc-chunk backfill indexed {n} document(s)");
            }
        });
    }

    // memory.* dispatch (hq-memory-mcp.4): the durable, named semantic-memory store an
    // agent writes once and recalls BY MEANING. Mirror of documents.* — PG-backed,
    // gated on GT_PG_URL, and riding the SAME shared embedder so a `memory.save` embeds
    // over the one fastembed engine documents already loaded. It HOLDS the per-workspace
    // pool cache and resolves `ws_<slug>.memories` per-request from the caller's tenant
    // (hq-memory-admin.4), exactly like documents.* — no longer bound to `ws_default`.
    let router = router.register(Arc::new(MemoryHandler::new(ws_pools.clone(), embedder)));

    // domain.catalog.* — operator-editable per-workspace domain catalog (gtcore-b37400
    // H4). Resolves the active workspace's catalog over the SAME Dolt store the issues
    // surface writes to (the shared `hq` default, or the tenant's `hq_<ws>` under
    // multi-tenant routing), so an edit here is read back by the bead-create domain
    // validation (gtcore-d81e77 H2). Needs Dolt (GT_DOLT_URL) for the default store;
    // skipped otherwise (no catalog to edit).
    let router = match std::env::var("GT_DOLT_URL")
        .ok()
        .and_then(|url| DoltIssues::connect(&url).ok())
    {
        Some(catalog_store) => {
            let handler = DomainCatalogHandler::new(Arc::new(catalog_store));
            let handler = match &dolt_pools {
                Some(dolt) => handler.with_dolt(dolt.clone()),
                None => handler,
            };
            router.register(Arc::new(handler))
        }
        None => {
            eprintln!("[gt-mcp-server] domain.catalog.* off — GT_DOLT_URL unset");
            router
        }
    };

    // dispatch.* — agent-dispatch frontier probe (gtcore-7bec8c — C3): exposes
    // ready_for_auto as an MCP tool so operators/agents can query which beads are
    // safe for autonomous dispatch right now. Needs Dolt for the ready predicate.
    let router = match std::env::var("GT_DOLT_URL")
        .ok()
        .and_then(|url| DoltIssues::connect(&url).ok())
    {
        Some(dispatch_dolt) => {
            let repo_dir = std::env::var("GT_REPO_DIR")
                .ok()
                .map(std::path::PathBuf::from);
            let mut handler = DispatchHandler::new(Arc::new(dispatch_dolt), repo_dir);
            // rig-hold H2 (gtcore-1f5e67): give the probe the per-workspace rig pools so it
            // excludes held rigs, matching the orchd frontier. Fail-soft without GT_PG_URL.
            if let Some(pg_url) = std::env::var("GT_PG_URL").ok().filter(|v| !v.is_empty()) {
                handler = handler.with_held_rigs(Arc::new(WsPools::new(pg_url)));
            }
            // gtcore-d24661: dispatch.request — the mayor's delegation edge. Shares the
            // convoy/agent sink; when it is absent the tool errors loudly instead of no-op'ing.
            if let Some(sink) = &dispatch_sink {
                handler = handler.with_dispatch_sink(sink.clone());
            }
            router.register(Arc::new(handler))
        }
        None => {
            eprintln!("[gt-mcp-server] dispatch.* off — GT_DOLT_URL unset");
            router
        }
    };

    // a2a.* — agent-to-agent delegation tool (Fase 2 — A3): lets a running
    // agent delegate a sub-task via the A2A intake pipeline without an HTTP
    // round-trip. Gated on the same envs as the HTTP A2A surface
    // (GT_A2A_DEFAULT_RIG + GT_A2A_INTAKE_EPIC) plus GT_DOLT_URL (intake)
    // and a live dispatch sink (same channel convoy/agent use).
    let router = match (
        std::env::var("GT_A2A_DEFAULT_RIG").ok().filter(|s| !s.trim().is_empty()),
        std::env::var("GT_A2A_INTAKE_EPIC").ok().filter(|s| !s.trim().is_empty()),
        std::env::var("GT_DOLT_URL").ok().and_then(|url| DoltIssues::connect(&url).ok()),
        &dispatch_sink,
    ) {
        (Some(a2a_rig), Some(a2a_parent), Some(delegate_dolt), Some(sink)) => {
            // B5 (gtcore-1bda00): default completion timeout for delegations
            // (0 disables). The daemon escalates a delegation stuck past this.
            let a2a_timeout_secs = std::env::var("GT_A2A_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(gt_composition::delegation::DEFAULT_TIMEOUT_SECS);
            // B4 (gtcore-3f3c57): cross-workspace delegation. Needs the per-tenant
            // Dolt routing (GT_DOLT_BASE_URL — same source `with_workspaces` uses)
            // to mint into another tenant's `hq_<dest>`, plus an explicit
            // deny-by-default grant list (GT_A2A_CROSS_WS_GRANTS). With either
            // absent, cross-workspace `workspace` args are rejected and only
            // same-workspace delegation works (the legacy behaviour).
            let cross_ws_grants = std::env::var("GT_A2A_CROSS_WS_GRANTS")
                .ok()
                .map(|spec| CrossWsGrants::parse(&spec))
                .unwrap_or_else(CrossWsGrants::empty);
            let cross_ws_stores: Option<Arc<WorkspaceStores>> = std::env::var("GT_DOLT_BASE_URL")
                .ok()
                .and_then(|base| WorkspaceStores::from_base_url(&base).ok())
                .map(Arc::new);
            eprintln!(
                "[gt-mcp-server] a2a.delegate on — rig {a2a_rig}, parent {a2a_parent}, push-callback tracking on (timeout {a2a_timeout_secs}s); cross-ws grants={} (stores {})",
                cross_ws_grants.len(),
                if cross_ws_stores.is_some() { "wired" } else { "off" },
            );
            let mut handler = A2aDelegateHandler::new(
                Arc::new(delegate_dolt),
                sink.clone(),
                a2a_rig,
                a2a_parent,
            )
            // A7 (gtcore-3a3557): wire the per-workspace pool cache so
            // a2a.discover can read the rig catalog for peer discovery.
            .with_pools(ws_pools.clone())
            // B5 (gtcore-1bda00): register each delegation on the event log
            // so the orchd callback plugin + timeout ticker push the outcome
            // back to the parent instead of the parent polling a2a.status.
            .with_delegation_log(event_log.clone(), a2a_timeout_secs)
            // Inter-agent messaging (a2a.send/inbox/ack)
            .with_event_log(event_log.clone());
            // B4: enable cross-workspace minting only when the per-tenant store
            // resolver is available; the grant list alone (without routing) cannot
            // reach another tenant's tracker.
            if let Some(stores) = cross_ws_stores {
                handler = handler.with_cross_ws(stores, cross_ws_grants);
            }

            // A5 (gtcore-f3a016): rig-level RBAC on the in-process intake path. With
            // GT_A2A_RIG_GRANTS set (same `origin->rig` syntax as the cross-ws/peer
            // grants), an agent may only delegate work onto a rig it holds a grant
            // for; an ungranted rig is rejected + audited with the origin identity.
            // Unset/empty ⇒ no rig gate (legacy: any rig is delegable).
            let rig_grants = std::env::var("GT_A2A_RIG_GRANTS")
                .ok()
                .map(|spec| CrossWsGrants::parse(&spec))
                .unwrap_or_else(CrossWsGrants::empty);
            eprintln!(
                "[gt-mcp-server] a2a.delegate rig RBAC: {} grant(s) ({})",
                rig_grants.len(),
                if rig_grants.is_empty() { "no rig gate" } else { "deny-by-default" },
            );
            handler = handler.with_rig_grants(rig_grants);

            // A7 (gtcore-3a3557): direct rig→rig HTTP delegation. A `peer` arg on
            // a2a.delegate routes the hop straight to that rig's own A2A endpoint
            // (discovered via its Agent Card), with orchd NOT relaying. Deny-by-
            // default: a hop needs an explicit `origin->peer` grant
            // (GT_A2A_PEER_GRANTS) and a resolvable peer (a name in GT_A2A_PEERS or
            // a bare http(s):// origin). With both envs absent, a `peer` arg is
            // rejected and only the in-process intake path runs.
            let peer_registry = std::env::var("GT_A2A_PEERS")
                .ok()
                .map(|spec| PeerRegistry::parse(&spec))
                .unwrap_or_else(PeerRegistry::empty);
            let peer_grants = std::env::var("GT_A2A_PEER_GRANTS")
                .ok()
                .map(|spec| CrossWsGrants::parse(&spec))
                .unwrap_or_else(CrossWsGrants::empty);
            if !peer_registry.is_empty() || !peer_grants.is_empty() {
                // The outbound bearer the peer's guarded POST /a2a requires. Prefer
                // an explicit GT_A2A_PEER_TOKEN; else mint a boot-lifetime token
                // with the platform RS256 key (peers share the verifier, so a token
                // minted here validates there). `None` only when neither is set.
                let peer_token = std::env::var("GT_A2A_PEER_TOKEN")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
                    .or_else(|| {
                        let minter = JwtMinter::from_env().ok()?;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let ttl = env_u64("GT_A2A_PEER_TOKEN_TTL_SECS", 31_536_000);
                        minter
                            .mint(&gt_auth::JwtClaims {
                                sub: "a2a-peer-delegation".into(),
                                workspace: std::env::var("GT_WORKSPACE")
                                    .unwrap_or_else(|_| "default".into()),
                                scopes: vec!["a2a.delegate".into()],
                                exp: now + ttl,
                                nbf: None,
                                iat: now,
                            })
                            .ok()
                    });
                eprintln!(
                    "[gt-mcp-server] a2a peer delegation on — {} peer(s), {} grant(s), outbound token {}",
                    peer_registry.len(),
                    peer_grants.len(),
                    if peer_token.is_some() { "wired" } else { "none" },
                );
                handler = handler.with_peers(
                    Arc::new(peer_registry),
                    peer_grants,
                    Arc::new(A2aPeerClient::new(peer_token)),
                );
            }

            router.register(Arc::new(handler))
        }
        _ => {
            eprintln!("[gt-mcp-server] a2a.delegate off — GT_A2A_DEFAULT_RIG / GT_A2A_INTAKE_EPIC / GT_DOLT_URL / dispatch channel required");
            router
        }
    };

    // comments.* dispatch (hq-57042e): threaded comments on cards (beads) + documents.
    // Card-target existence checks need the Dolt tracker, so the handler is wired only
    // when GT_DOLT_URL is set (it always is alongside the issues store); mention
    // notifications ride the same `notifications` table + SSE event notify.send uses.
    let router = match std::env::var("GT_DOLT_URL")
        .ok()
        .and_then(|url| DoltIssues::connect(&url).ok())
    {
        Some(comments_dolt) => router.register(Arc::new(CommentsHandler::new(
            ws_pools.clone(),
            Arc::new(comments_dolt),
            dolt_pools.clone(),
            pool.clone(),
            event_log.clone(),
        ))),
        None => {
            eprintln!("[gt-mcp-server] comments.* off — GT_DOLT_URL unset (card targets unverifiable)");
            router
        }
    };

    // report.* — the operator-report export engine (hq-fc7d6a): projects the
    // (rig, workspace) board into the tracker mockup, attaches it as a document
    // (xlsx via the blob store, csv as text), optionally announcing via the
    // email outbox. Needs the Dolt tracker for the row source.
    let (router, report_service) = match std::env::var("GT_DOLT_URL")
        .ok()
        .and_then(|url| DoltIssues::connect(&url).ok())
    {
        Some(report_dolt) => {
            let report_dolt = Arc::new(report_dolt);
            let (report_blob, report_bucket) = build_blob_store();
            // The digest service (hq-84f93b): same Dolt row source + outbox pool
            // as report.generate. The schedule LIST now lives in the DB-backed
            // `report_schedules` store (gtcore-915232) — durable across redeploys
            // — so a CRUD edit lands on the next daemon tick without a restart.
            let service = Arc::new(gt_composition::report_scheduler::ReportService::new(
                report_dolt.clone(),
                dolt_pools.clone(),
                pool.clone(),
                // Per-workspace PG pools: the source of the bead/epic comments the
                // digest folds into the report (gtcore-01bcf2).
                Some(ws_pools.clone()),
            ));
            let router = router.register(Arc::new(ReportHandler::new(
                ws_pools.clone(),
                report_dolt,
                dolt_pools.clone(),
                report_blob,
                report_bucket,
                pool.clone(),
                std::env::var("GT_PUBLIC_URL").ok(),
                Some(service.clone()),
            )));
            (router, Some(service))
        }
        None => {
            eprintln!("[gt-mcp-server] report.* off — GT_DOLT_URL unset (no row source)");
            (router, None)
        }
    };
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
    Ok((router, Some(rig_prefixes), Some(ws_status), Some(documents), report_service))
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

/// Seed the GLOBAL `public.oauth_providers` table from the versioned, NON-SECRET provider extract
/// (`gt-auth`'s `seeds/oauth-providers.json`, `hq-greenfield-seeds.3`), so a greenfield deploy comes
/// up with the login providers prod had — Google, … — instead of a blank login page. Mirrors
/// [`seed_admin`]'s discipline:
///
/// - **Idempotent / non-clobbering:** seeds ONLY when the table is EMPTY. A populated table (the
///   already-curated prod `default`, or any deploy where an admin has registered a provider) is left
///   exactly as-is — the live `/admin/providers` surface remains the source of truth there.
/// - **Secret-gated, never vendored:** the OAuth `client_secret` is NOT in the seed. Each entry names
///   the env var its cleartext secret is read from (`GT_OAUTH_SEED_SECRET_<ID>`); a provider whose env
///   is unset is SKIPPED cleanly (a log line, never fatal), exactly like `seed_admin` skips without
///   `GT_ADMIN_*`. The secret is then AES-256-GCM sealed at rest via `GT_SECRET_KEY` ([`crypto`]),
///   so it never reaches the database in clear — and is required: with `GT_SECRET_KEY` unset the seal
///   fails, so the whole step is short-circuited (logged, never fatal) rather than erroring boot.
///
/// `pool` is the ws_default pool; the table is `public`-qualified, so the global store is reached
/// regardless of search_path (the same handle `PgProviderRepo` uses elsewhere).
#[cfg(feature = "oauth")]
async fn seed_oauth_providers(pool: sqlx::PgPool) -> anyhow::Result<()> {
    use gt_auth::ProviderRepo;

    // The seal needs the master key; without it every provider would fail to seal. Short-circuit the
    // whole step (a log line, never fatal) so a deploy that has not provided GT_SECRET_KEY still boots
    // — it just has no seeded login providers, mirroring `seed_admin`'s skip.
    if std::env::var(gt_auth::ENV_SECRET_KEY)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .is_none()
    {
        eprintln!(
            "[gt-mcp-server] oauth provider seed skipped (set {} to seal the client secret at rest)",
            gt_auth::ENV_SECRET_KEY
        );
        return Ok(());
    }

    let repo = gt_auth::PgProviderRepo::new(pool);

    // Idempotency gate: never touch a non-empty table. A curated prod (or any deploy where an admin
    // already registered a provider) is left untouched — the live surface owns it there.
    let existing = repo
        .list()
        .await
        .context("oauth provider seed: list existing providers")?;
    if !existing.is_empty() {
        eprintln!(
            "[gt-mcp-server] oauth provider seed skipped ({} provider(s) already registered)",
            existing.len()
        );
        return Ok(());
    }

    let seed = gt_auth::seed_providers().context("oauth provider seed: parse embedded seed")?;
    let mut created = 0usize;
    for sp in &seed {
        // Resolve the cleartext secret from the entry's named env var; unset => clean skip.
        let np = match sp.resolve() {
            Ok(Some(np)) => np,
            Ok(None) => {
                eprintln!(
                    "[gt-mcp-server] oauth provider '{}' seed skipped (set {} to enable it)",
                    sp.id, sp.secret_env
                );
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "oauth provider seed: resolve '{}': {e}",
                    sp.id
                ));
            }
        };
        let id = np.id.clone();
        repo.create(np)
            .await
            .with_context(|| format!("oauth provider seed: create '{id}'"))?;
        created += 1;
        eprintln!("[gt-mcp-server] oauth provider seeded: {id}");
    }
    if created == 0 {
        eprintln!(
            "[gt-mcp-server] oauth provider seed: no provider secrets present (set GT_OAUTH_SEED_SECRET_* to seed)"
        );
    }
    Ok(())
}

/// Seed the versioned, declarative rig catalog into an EMPTY `rigs` table (hq-greenfield-seeds.5).
///
/// The rigs (`gt`/`gt_core`/`gtmcp`/`gtproxy`/`gtweb`) were registered by hand with `rig.add` and
/// lived ONLY in prod's per-tenant `ws_default.rigs` table, so a greenfield cluster came up with an
/// empty catalog — no prefix routing, no dispatch — until an operator re-ran every `rig.add`. This
/// replays the live-extracted, embedded seed (`gt-rig/seeds/rigs.json`) so a clean deploy brings its
/// rigs with no manual step. It follows the same discipline as [`seed_oauth_providers`]:
///
/// - **Idempotent / non-clobbering:** seeds ONLY when the table is EMPTY. A populated table (the
///   already-curated prod `default`, or any deploy where an operator has registered a rig) is left
///   exactly as-is — `rig.*` remains the source of truth there.
/// - **No runtime artifact vendored:** `registered_at_secs` is stamped from the boot clock (the seed
///   never copies prod's epochs), and `git_connection_ref` is carried only if the live extract had
///   it (in prod all five are `null` = SSH clone). A rig needing a private-repo token still depends
///   on its `vcs_connections` row (hq-greenfield-seeds.3) existing first; the seed binds none.
///
/// Scoped to the `ws_default` template schema — exactly where the prod rigs live, and the schema
/// `gt_create_workspace_schema` clones per tenant. A `WorkspacePool` connect failure is a clean skip
/// (a log line, never fatal), so a deploy whose tenant schema is not yet provisioned still boots.
async fn seed_rigs(pg_url: &str) -> anyhow::Result<()> {
    use gt_rig::RigRepository;

    // The rigs table lives in the per-workspace `ws_default` schema (not `public`), so it needs a
    // search_path-scoped pool — the same handle `PgRigs` uses elsewhere. A connect failure here is
    // non-fatal: the rest of the boot still runs, the catalog just stays whatever it already is.
    let ws_pool = match WorkspacePool::connect(pg_url, "default").await {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "[gt-mcp-server] rig catalog seed skipped (ws_default pool connect failed: {e})"
            );
            return Ok(());
        }
    };
    let repo = gt_rig::PgRigs::new(ws_pool.pool().clone());

    // Idempotency gate: never touch a non-empty catalog. A curated prod (or any deploy where an
    // operator already registered a rig) is left untouched — the live `rig.*` surface owns it there.
    //
    // A list failure here is NON-FATAL (gtcore-e07dd0): if `ws_default.rigs` is missing or the schema
    // is not yet provisioned, this seed must not crashloop the ENTIRE mcp-server (which takes the whole
    // platform down). The catalog just stays whatever it is; an operator can `rig.add`. Mirrors the
    // connect-failure skip above.
    let existing = match repo.list().await {
        Ok(rigs) => rigs,
        Err(e) => {
            eprintln!(
                "[gt-mcp-server] rig catalog seed skipped (list failed — schema not ready: {e})"
            );
            return Ok(());
        }
    };
    if !existing.is_empty() {
        eprintln!(
            "[gt-mcp-server] rig catalog seed skipped ({} rig(s) already registered)",
            existing.len()
        );
        return Ok(());
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let seed = gt_rig::seed_rigs().context("rig catalog seed: parse embedded seed")?;
    let mut created = 0usize;
    for sr in seed {
        let entry = sr.into_entry(now);
        let name = entry.name.clone();
        repo.upsert(&entry)
            .await
            .with_context(|| format!("rig catalog seed: upsert '{name}'"))?;
        created += 1;
        eprintln!("[gt-mcp-server] rig seeded: {name}");
    }
    eprintln!("[gt-mcp-server] rig catalog seeded: {created} rig(s)");
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

/// The `Host` allow-list entries implied by a service URL (`GT_SELF_URL` / `GT_PUBLIC_URL`).
///
/// rmcp's streamable-HTTP transport rejects any request whose `Host` is not in
/// `allowed_hosts` (a DNS-rebinding guard). gt-orch-server writes `GT_SELF_URL` verbatim into
/// every agent's `.mcp.json`, so the server must accept that exact authority or an in-cluster
/// `gt mcp call` is 403'd. Given `http://gt-mcp-server:8765` this returns
/// `["gt-mcp-server", "gt-mcp-server:8765"]` — the bare host (matches any port, per rmcp's
/// `host_is_allowed`) plus the explicit `host:port` for clarity. An empty/unparseable URL, or
/// one with no host authority, yields an empty vec (the caller logs and skips it).
fn authority_allowed_hosts(url: &str) -> Vec<String> {
    let Ok(uri) = url.trim().parse::<axum::http::Uri>() else {
        return Vec::new();
    };
    let Some(authority) = uri.authority() else {
        return Vec::new();
    };
    let host = authority.host();
    if host.is_empty() {
        return Vec::new();
    }
    let mut out = vec![host.to_string()];
    if let Some(port) = authority.port_u16() {
        out.push(format!("{host}:{port}"));
    }
    out
}

/// Parse a `u64` env var, falling back to `default` when it is unset or unparseable.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The master switch for the singleton background daemons (hq-talos-migration.4).
///
/// The API surface (`/mcp`, REST, `/auth/*`, the SSE feed, the GitHub webhook) is stateless and
/// scales to N replicas under k8s; the background loops (session reaper, archive sweep,
/// account-dir GC, graph drift-reconcile) are SINGLETONs — running them on every replica
/// duplicates ticks and races. So a multi-replica deploy splits the platform into an API
/// Deployment (`GT_RUN_DAEMONS=0`) and a single-replica daemons Deployment (the default).
///
/// DEFAULT = ON: only an explicit `GT_RUN_DAEMONS=0` turns them off, so the existing
/// single-instance compose deploy — which sets nothing — keeps running every daemon exactly as
/// before. The per-daemon cadence/off envs (`GT_RECONCILE_TICK_SECS`, `GT_ACCOUNTS_GC_TICK_SECS`,
/// `GT_GRAPH_DRIFT_TICK_SECS`, …) still apply on top; this is the one master flag the API tier
/// flips off.
///
/// Pure over its `(key) -> Option<value>` lookup, so it is unit-testable without mutating the
/// process environment.
fn should_run_daemons(lookup: impl Fn(&str) -> Option<String>) -> bool {
    !matches!(lookup("GT_RUN_DAEMONS").as_deref(), Some("0"))
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

    /// hq-talos-migration.4: the singleton-daemon master switch is ON by default (single-instance
    /// compose sets nothing) and turns OFF only on an explicit `GT_RUN_DAEMONS=0`. Anything else
    /// (unset, "1", "true", empty, "00") is treated as ON, so a fat-fingered value never silently
    /// disables the daemon tier.
    #[test]
    fn should_run_daemons_defaults_on_and_off_only_on_zero() {
        let on = |_: &str| None; // unset ⇒ ON
        assert!(should_run_daemons(on));
        assert!(!should_run_daemons(|_| Some("0".to_string())));
        for v in ["1", "true", "TRUE", "on", "", " ", "00", "no", "false"] {
            assert!(
                should_run_daemons(|_| Some(v.to_string())),
                "GT_RUN_DAEMONS={v:?} must run daemons (only \"0\" turns them off)"
            );
        }
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

    /// gtcore-2cc534: the `Host` allow-list derived from `GT_SELF_URL`/`GT_PUBLIC_URL` must yield
    /// the exact authority the orchd writes into every agent's `.mcp.json`, so an in-cluster
    /// `gt mcp call` is no longer 403'd. The bare host (port-agnostic, matching rmcp's
    /// `host_is_allowed`) plus the explicit `host:port` are emitted; junk yields nothing.
    #[test]
    fn authority_allowed_hosts_extracts_host_and_port() {
        // The in-cluster GT_SELF_URL the deploy hands agents — the case this bead fixes.
        assert_eq!(
            authority_allowed_hosts("http://gt-mcp-server:8765"),
            vec!["gt-mcp-server".to_string(), "gt-mcp-server:8765".to_string()]
        );
        // Public ingress, https default port (no explicit port in the URL).
        assert_eq!(
            authority_allowed_hosts("https://gt-dev.codecsrayo.com"),
            vec!["gt-dev.codecsrayo.com".to_string()]
        );
        // A path/trailing slash does not change the authority.
        assert_eq!(
            authority_allowed_hosts("http://gt-mcp-server:8765/mcp"),
            vec!["gt-mcp-server".to_string(), "gt-mcp-server:8765".to_string()]
        );
        // Surrounding whitespace is tolerated (env values can be sloppy).
        assert_eq!(
            authority_allowed_hosts("  http://gt-mcp-server:8765  "),
            vec!["gt-mcp-server".to_string(), "gt-mcp-server:8765".to_string()]
        );
        // Empty / authority-less / unparseable inputs yield nothing (caller logs + skips).
        for junk in ["", "   ", "not a url", "/just/a/path"] {
            assert!(
                authority_allowed_hosts(junk).is_empty(),
                "{junk:?} must yield no allow-list entries"
            );
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

    /// hq-vcs-connections.13 (GT_PG_URL-gated): `reconcile_tenant_schemas` heals per-tenant drift.
    /// Plant a tenant schema that pre-dates a template column (the exact `git_connection_ref` /
    /// ws=confiar shape), then prove reconcile (1) adds a NEW template table the tenant lacks,
    /// (2) backfills the missing column with the template's type/default, and (3) is idempotent.
    /// A no-op without GT_PG_URL (same gate as the migration e2e). Uses a private `ws_recon`
    /// schema so it never collides with the real `ws_default` template.
    #[tokio::test]
    async fn reconcile_backfills_drifted_tenant_columns_and_tables() {
        let Ok(url) = std::env::var("GT_PG_URL") else {
            eprintln!("GT_PG_URL unset; skipping tenant-reconcile e2e");
            return;
        };
        let pool = sqlx::PgPool::connect(&url).await.expect("connect GT_PG_URL");

        // Clean slate for a deterministic re-run.
        sqlx::raw_sql(
            "DROP SCHEMA IF EXISTS ws_recon CASCADE; \
             DROP SCHEMA IF EXISTS ws_default CASCADE;",
        )
        .execute(&pool)
        .await
        .expect("drop prior recon schemas");

        // The TEMPLATE: a `rigs` table WITH the post-migration column + a NEW table absent from the
        // (older) tenant, plus the provisioning function the reconcile re-runs.
        sqlx::raw_sql(
            "CREATE SCHEMA ws_default; \
             CREATE TABLE ws_default.rigs ( \
                 name TEXT PRIMARY KEY, \
                 default_branch TEXT NOT NULL DEFAULT 'main', \
                 git_connection_ref TEXT NULL ); \
             CREATE TABLE ws_default.newtable ( id TEXT PRIMARY KEY );",
        )
        .execute(&pool)
        .await
        .expect("seed template");
        // The real provisioning function (clones template tables into ws_<slug>).
        for mig in gt_store_pg::workspace_migrations() {
            sqlx::raw_sql(&mig.sql).execute(&pool).await.expect("apply ws migration");
        }

        // Catalog with a `recon` tenant; plant its schema as a STALE clone: `rigs` WITHOUT
        // `git_connection_ref`, and missing `newtable` entirely (the drift the bug describes).
        sqlx::raw_sql(
            "INSERT INTO public.workspaces (id, slug, name, status) \
             VALUES ('recon', 'recon', 'Recon', 'active') ON CONFLICT (id) DO NOTHING; \
             CREATE SCHEMA ws_recon; \
             CREATE TABLE ws_recon.rigs ( \
                 name TEXT PRIMARY KEY, \
                 default_branch TEXT NOT NULL DEFAULT 'main' );",
        )
        .execute(&pool)
        .await
        .expect("seed stale tenant");

        // Pre-condition: the column is genuinely absent (the production failure).
        let had_col: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema='ws_recon' AND table_name='rigs' AND column_name='git_connection_ref')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!had_col, "precondition: stale tenant must lack git_connection_ref");

        reconcile_tenant_schemas(&pool).await.expect("reconcile");

        // (1) the new template table was cloned in.
        let has_table: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema='ws_recon' AND table_name='newtable')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(has_table, "reconcile must clone the new template table into the tenant");

        // (2) the missing column was backfilled with the template's type.
        let coltype: Option<String> = sqlx::query_scalar(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema='ws_recon' AND table_name='rigs' AND column_name='git_connection_ref'",
        )
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(coltype.as_deref(), Some("text"), "git_connection_ref must be added as text");

        // (3) idempotent: a second pass does not error and changes nothing.
        reconcile_tenant_schemas(&pool).await.expect("reconcile again");

        // Cleanup.
        sqlx::raw_sql(
            "DROP SCHEMA IF EXISTS ws_recon CASCADE; \
             DROP SCHEMA IF EXISTS ws_default CASCADE; \
             DELETE FROM public.workspaces WHERE slug='recon';",
        )
        .execute(&pool)
        .await
        .ok();
    }
}
