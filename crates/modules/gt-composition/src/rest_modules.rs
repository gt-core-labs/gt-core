//! `build_rest_modules` — the REST domain module-set assembly, extracted from
//! `gt-mcp-server.rs main()` so a boot-smoke test can build the *exact same*
//! `RootBuilder` the server boots and assert it validates (hq-vcs-connections.12).
//!
//! ## Why this lives in the library, not the bin
//!
//! On 2026-06-10 the `.9` graph REST module shipped a `GtModule` whose
//! `register_mcp_tools` declared a 2-segment tool name (`graph.query`). The REST
//! `RootBuilder` rejects that at `build()` with
//! [`BuildError::MalformedToolName`](gt_module::BuildError::MalformedToolName)
//! (`NotThreeSegments`), so the embeddings server *crash-looped at boot in prod*.
//! `cargo check`, CI, and every unit test passed — nothing exercised the real
//! server's module-build. This function is the seam that closes that gap: `main()`
//! calls it to assemble its REST modules, and `tests/boot_smoke.rs` calls the same
//! function with offline (lazy / `None`) state and asserts `.build()` succeeds — a
//! regression that would have caught the `.9` crash before it left CI.
//!
//! ## Purity
//!
//! The function reads no environment and opens no socket: every env-derived choice
//! main() makes (the dispatch channel, the GitHub App client, the per-workspace
//! issues Dolt pools, the blob store / embedder) is threaded in as a parameter on
//! [`RestModuleParts`]. The PG-backed states it constructs are all *lazy* — they
//! store a pool/URL but never dial until a request — so the test feeds a
//! `connect_lazy` pool that never connects (the same trick `mcp_http_parity.rs`
//! uses). The skills-catalog seed is the one IO touch: it replays the event log
//! (a filesystem read), so the function is `async` and the test points it at a
//! throwaway temp event log.

use std::path::PathBuf;
use std::sync::Arc;

use gt_agent::{AgentApiState, AgentModule};
use gt_documents::{DocumentsApiState, DocumentsModule};
use gt_feed::{FeedApiState, FeedModule};
use gt_graphindex::GraphifyIndexer;
use gt_merge::{MergeApiState, MergeModule};
use gt_meta::{MetaApiState, MetaModule};
use gt_module::{McpTool, RootBuilder};
use gt_orchestration::{ConvoyApiState, ConvoyModule};
use gt_quota::{QuotaApiState, QuotaModule};
use gt_rig::{RigApiState, RigsModule};
use gt_skills::{SkillsApiState, SkillsModule};
use gt_store_blob::BlobStore;
use gt_workspace::{PgWorkspaces, WorkspaceApiState, WorkspaceModule};

use crate::mcp::{
    CompositionTenantProvisioner, EventLog, EventLogConvoy, EventLogFeed, EventLogGraph,
    EventLogMerges, EventLogQuota, EventLogSkills, FsAccountCatalog, GraphHandler,
    GraphHandlerRefresher, RigProvisioner, WsPoolRigs, WsPools,
};

/// The Postgres-gated slice of the REST module set (workspace / rig / connection /
/// graph / documents). `main()` builds this only when `GT_PG_URL` is set; the test
/// builds it from a never-dialed lazy pool so the offline build still exercises the
/// graph + connection modules — the `.9` crash site.
pub struct RestPgParts {
    /// A Postgres pool. The constructed states are lazy, so a `connect_lazy` pool
    /// that never dials is enough for the build-only path the smoke test walks.
    pub pool: sqlx::PgPool,
    /// The base Postgres URL the per-workspace pool caches connect against (lazily).
    pub pg_url: String,
    /// The per-workspace issues Dolt pools the workspace provisioner threads through
    /// (`GT_DOLT_BASE_URL`-derived in `main()`); `None` ⇒ single-tenant.
    pub issues_workspaces: Option<Arc<gt_store_dolt::WorkspacePools>>,
    /// The DB-backed GitHub App config source (hq-61ea43). The install/callback/repos/config routes
    /// are ALWAYS mounted and resolve the App from this source per request (so a UI config takes
    /// effect with no restart); the graph provisioner resolves a client from it at boot.
    pub github: gt_vcs::GithubAppSource,
    /// The document blob store (`GT_BLOB_*` in `main()`); `None` ⇒ `.md`-only.
    pub blob: Option<Arc<BlobStore>>,
    /// The blob bucket name (recorded on a blob pointer).
    pub bucket: String,
    /// The text extractor for uploaded attachments.
    pub extractor: gt_docs_extract::Extractor,
    /// The optional embedding engine (`GT_EMBEDDINGS` in `main()`); `None` ⇒
    /// full-text-only document search.
    pub embedder: Option<Arc<dyn gt_docs_embed::Embedder>>,
}

/// Every input `build_rest_modules` needs, all pre-resolved by the caller so the
/// function itself reads no environment and opens no socket. Mirrors the handles
/// `main()` already has in scope where the `rest` builder is assembled.
pub struct RestModuleParts {
    /// The Dolt issues store the meta REST surface mints `report-gap` into.
    pub meta_store: Arc<gt_store_dolt::DoltIssues>,
    /// The scope-attribution actor.
    pub actor: String,
    /// The full served `tools/list` for `meta.help` (issues+meta+domain descriptors).
    pub meta_tools: Vec<McpTool>,
    /// The agent event-log root the agent REST state replays sessions from.
    pub agent_root: PathBuf,
    /// The orchd dispatch-channel dir (`GT_CHANNEL_ROOT`/`GT_DISPATCH_CHANNEL` in
    /// `main()`); `Some` bridges `POST /api/v1/agent` to the scheduler, `None` not.
    pub dispatch_channel: Option<PathBuf>,
    /// The shared per-workspace event log backing the event-sourced REST domains.
    pub event_log: Arc<EventLog>,
    /// The claude-account credential root the quota catalog lists onboarded accounts
    /// from.
    pub accounts_root: PathBuf,
    /// Optional on-demand OAuth usage prober for `POST /api/v1/quota/probe/sweep`.
    /// `None` ⇒ the endpoint returns 503; set it from `gt-mcp-server` when the accounts
    /// root is available.
    pub sync_prober: Option<std::sync::Arc<dyn gt_quota::http::SyncProber>>,
    /// The workspace slug the skills catalog seeds its role preset into when empty.
    pub skills_seed_workspace: String,
    /// The Postgres-gated module slice; `None` ⇒ no workspace/rig/connection/graph/
    /// documents modules (exactly `main()`'s `GT_PG_URL`-unset branch).
    pub pg: Option<RestPgParts>,
}

/// Assemble the REST domain `RootBuilder` — the `rest_root` set `gt-mcp-server`
/// boots — without reading the environment or dialing a socket.
///
/// This is the SAME builder `main()` finalizes with `.build()`, so a smoke test that
/// calls this and asserts `.build()` is `Ok` exercises the real boot-time module
/// validation (tool-name 3-segment rule, scope-conflict, dependency order) that
/// only ran in production before — the gap that let `.9`'s `graph.query` crash the
/// server (hq-vcs-connections.12). The `me` module is intentionally NOT assembled
/// here: it requires an *eager* `WorkspacePool::connect().await`, so `main()` appends
/// it after this returns (and the smoke test does not need it — `me` carries no MCP
/// tools that could trip the validator).
///
/// Returns the builder plus the optional public share-read router (set when the
/// document store mounts), which `main()` merges onto the public ops router.
pub async fn build_rest_modules(
    parts: RestModuleParts,
) -> (RootBuilder, Option<axum::Router>) {
    let RestModuleParts {
        meta_store,
        actor,
        meta_tools,
        agent_root,
        dispatch_channel,
        event_log,
        accounts_root,
        skills_seed_workspace,
        sync_prober,
        pg,
    } = parts;

    let mut rest = RootBuilder::new()
        // meta REST: GET /help (full tools/list) + POST /report-gap.
        .module(MetaModule::with_http(MetaApiState::new(
            meta_store,
            actor.clone(),
            meta_tools,
        )))
        // agent.*: bridge POST /api/v1/agent to the orchd scheduler when a dispatch
        // channel is configured.
        .module(AgentModule::with_http({
            let agent_state = AgentApiState::new(agent_root);
            match dispatch_channel {
                Some(channel_dir) => agent_state.with_dispatch_channel(channel_dir),
                None => agent_state,
            }
        }))
        // quota.*: per-workspace assignment + the deploy-global account catalog.
        .module(QuotaModule::with_http({
            let mut quota_state = QuotaApiState::new(Arc::new(EventLogQuota::new(event_log.clone())))
                .with_catalog(Arc::new(FsAccountCatalog::new(accounts_root)));
            if let Some(prober) = sync_prober {
                quota_state = quota_state.with_sync_prober(prober);
            }
            quota_state
        }))
        // merge.*: the durable event-sourced board.
        .module(MergeModule::with_http(MergeApiState::new(Arc::new(
            EventLogMerges::new(event_log.clone()),
        ))));

    // skills.read + skills.write: seed the canonical role catalog into the seed
    // workspace's `skills.*` log when empty (idempotent), so a clean deploy gives
    // each role a working least-privilege MCP grant out of the box.
    let skills = Arc::new(EventLogSkills::new(event_log.clone()));
    {
        use gt_skills::{SkillWriter, WorkspaceSkills};
        if let Ok(cat) = skills.catalog(&skills_seed_workspace).await {
            if cat.is_empty() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut seeded = 0usize;
                for ev in gt_skills::presets::workspace_seed_events(now) {
                    if skills.append(&skills_seed_workspace, ev).await.is_ok() {
                        seeded += 1;
                    }
                }
                eprintln!("[gt-mcp-server] skills: seeded {seeded} role-catalog event(s) into empty `{skills_seed_workspace}` catalog");
            }
        }
    }
    rest = rest
        .module(SkillsModule::with_http(
            SkillsApiState::new(skills.clone()).with_writer(skills),
        ))
        // feed.read: read-only activity feed.
        .module(FeedModule::with_http(FeedApiState::new(Arc::new(
            EventLogFeed::new(event_log.clone()),
        ))))
        // convoy.*: read + mutate the event-sourced convoy board.
        .module(ConvoyModule::with_http(ConvoyApiState::new(Arc::new(
            EventLogConvoy::new(event_log.clone()),
        ))));

    let mut public_share: Option<axum::Router> = None;
    if let Some(pg) = pg {
        let RestPgParts {
            pool,
            pg_url,
            issues_workspaces,
            github,
            blob,
            bucket,
            extractor,
            embedder,
        } = pg;
        // Resolve a concrete App client ONCE for the graph provisioner (it consumes
        // `Option<GithubAppClient>`); the install/config routes keep the live source for per-request
        // resolution. A load fault / unconfigured App ⇒ `None` (public-rig graph path still works).
        let github_client = github
            .load()
            .await
            .ok()
            .flatten()
            .map(gt_vcs::GithubAppClient::new);
        rest = rest
            // workspace.*: REST create provisions a full tenant via the shared
            // provisioner (PG schema/RBAC + per-tenant Dolt).
            .module(WorkspaceModule::with_http(
                WorkspaceApiState::new(Arc::new(PgWorkspaces::new(pool.clone()))).with_provisioner(
                    Arc::new(CompositionTenantProvisioner::new(
                        pool.clone(),
                        issues_workspaces,
                    )),
                ),
            ))
            .module(RigsModule::with_http(RigApiState::new(Arc::new(
                WsPoolRigs::new(Arc::new(WsPools::new(pg_url.clone()))),
            ))))
            // connection.*: per-workspace VCS connections over the GLOBAL
            // `public.vcs_connections` table, + the GitHub App install flow when
            // an App is configured.
            .module({
                let connections = Arc::new(gt_vcs::PgVcsConnections::new(pool.clone()));
                let vcs = gt_vcs::VcsModule::with_http(gt_vcs::ConnectionApiState::new(
                    connections.clone(),
                ));
                // The GitHub surface (install/callback/repos/config) is ALWAYS mounted (hq-61ea43):
                // it resolves the App from the DB-backed source per request, so it works once the
                // admin sets the config — no env, no redeploy.
                vcs.with_github(gt_vcs::GithubApiState::new(github.clone(), connections))
            })
            // graph.* read-only REST + refresh. The REST wrap registers NO MCP tools
            // (the `graph.*` tools are 2-segment and served on the MCP DomainHandler
            // path) — the `.9` incident was this module declaring them, which the
            // builder rejects. This is the exact `.module()` call the smoke test
            // pins.
            .module({
                let read_custody: Arc<dyn gt_graphindex::WorkspaceGraph> =
                    Arc::new(EventLogGraph::new(event_log.clone()));
                let graph_provisioner = {
                    let conns: Arc<dyn gt_vcs::VcsConnectionRepo> =
                        Arc::new(gt_vcs::PgVcsConnections::new(pool.clone()));
                    RigProvisioner::new(
                        Arc::new(WsPools::new(pg_url.clone())),
                        conns,
                        github_client,
                    )
                };
                let refresh_handler = Arc::new(
                    GraphHandler::new(event_log.clone(), Arc::new(GraphifyIndexer::new()))
                        .with_provisioner(graph_provisioner),
                );
                let refresher: Arc<dyn gt_graphindex::GraphRefresher> =
                    Arc::new(GraphHandlerRefresher::new(refresh_handler));
                let graph_state = gt_graphindex::GraphApiState::new(
                    read_custody,
                    Arc::new(GraphifyIndexer::new()),
                )
                .with_refresher(refresher);
                gt_graphindex::GraphModule::with_http(graph_state)
            });
        // documents.*: the authenticated module router + the public share-read router
        // share the same store handles.
        let docs_state = DocumentsApiState::new(pg_url, blob, bucket, extractor, embedder);
        public_share = Some(gt_documents::public_share_router(docs_state.clone()));
        rest = rest.module(DocumentsModule::with_http(docs_state));
    }

    (rest, public_share)
}
