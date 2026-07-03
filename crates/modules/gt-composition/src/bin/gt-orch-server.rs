//! `gt-orch-server` — the gt-core autonomous orchestration daemon (`hq-orchd.1`).
//!
//! The long-lived daemon entrypoint replacing the upstream `bins/gt`. It boots the single
//! Tokio runtime (the domain crates never create one — `tokio::spawn` is forbidden in
//! the kernel; the bin owns the runtime, docs/03), resolves the **durable hydrated**
//! [`live_root`] for the configured workspace, and stays alive running the reactor
//! loops until SIGTERM/SIGINT, when it drains every in-flight worktree (a final commit + push of
//! pending work, `gtcore-0179f8`), then drains the actor stack and exits cleanly.
//!
//! Like `gt-mcp-server`, this bin lives in `gt-composition` (the `modules` tier)
//! because composing the per-workspace root names every `domain/*` crate, which only
//! the `modules` tier may depend on (docs/03 Rule 4).
//!
//! Durability (`hq-orchd.2` / `.5`): the root persists every hub record to the
//! path-partitioned per-workspace event log under `GT_EVENTLOG_ROOT`, and on boot
//! rehydrates the pending scheduler queue + the in-flight merge board by replaying
//! that log — so a restart resumes open work.
//!
//! Polecat supervision (`hq-orchd.3`): a [`PolecatSupervisorPlugin`] observes the hub and slings a
//! supervised tmux polecat for each dispatched bead (admitted by a [`PoolAllocator`]); a timer
//! drives [`PolecatSupervisor::tick`] to re-sling dead ones and refreshes the host admission cap
//! from live CPU + RAM via [`host_cap_from_metrics`]. The reactor loops (`.4`) and pipelines (`.6`)
//! wire in here as they land.
//!
//! Env:
//! - `GT_EVENTLOG_ROOT` — durable per-workspace event-log volume (default
//!   [`gt_eventlog::DEFAULT_EVENTLOG_ROOT`], `/var/lib/gt-core`).
//! - `GT_WORKSPACE` — the workspace the daemon boots (default `default`).
//! - `GT_POOL_SIZE` — per-workspace polecat pool size (default 4).
//! - `GT_POLECAT_MEM_MB` — per-polecat RAM budget for the host cap (default 1024).
//! - `GT_POLECAT_MAX_RESTARTS` — re-sling cap per session (default 64).
//! - `GT_POLECAT_TICK_SECS` — supervision + capacity timer interval (default 15).
//! - `GT_CHECKPOINT_PUSH_SECS` — checkpoint-push timer interval (default 120): every N seconds the
//!   daemon pushes each in-flight polecat branch to origin so a committed-but-unmerged branch is
//!   durable before merge-ready (`gtcore-4cea57`). `0` ⇒ disabled.
//! - `GT_DRAIN_ON_TERM` (default on) / `GT_DRAIN_TIMEOUT_SECS` (default 90) — on SIGTERM/preStop
//!   (redeploy), force a final checkpoint: commit any UNCOMMITTED changes in every active worktree
//!   and push the branch to origin before draining the actor stack, so a `Recreate` redeploy never
//!   loses committable work (`gtcore-0179f8`). Bounded by the timeout (keep it under the pod's
//!   `terminationGracePeriodSeconds`); `GT_DRAIN_ON_TERM=0` disables it.
//! - `GT_RIG` / `GT_RIG_PATH` / `GT_POLECAT_CMD` / `GT_POLECAT_PREFIX` / `GT_HEARTBEAT_DIR` —
//!   the rig's [`SpawnTemplate`] (see [`SpawnTemplate::from_env`]).
//! - `GT_PATROL_TICK_SECS` (30) / `GT_LEASE_TIMEOUT_SECS` (300) — patrol lease-expiry ticker.
//! - `GT_QUOTA_TICK_SECS` (60) / `GT_QUOTA_THRESHOLD_SECS` (300) — quota auto-rotation ticker.
//! - `GT_QUOTA_PLAN_LIMIT` (50_000_000) — synthetic-window budget (cost units) the quota-feed seeds
//!   for an account when a polecat reports token samples but no `anthropic-ratelimit-*` headers
//!   (`hq-agent-provisioning.8`); tune to the plan's real 5h budget.
//! - `GT_CHANNEL_ROOT` (`/gt/.channels`) / `GT_MERGE_READY_CHANNEL` (`merge-ready`) — the
//!   Refinery MERGE_READY gt-channel; absent/unopenable ⇒ the loop is disabled, the daemon boots.
//! - `GT_DISPATCH_CHANNEL` (`dispatch`, under `GT_CHANNEL_ROOT`) — the dispatch-request gt-channel
//!   (`hq-orchd-deploy.4`): a `{"bead","priority"}` JSON message seeds + enqueues the bead on the
//!   scheduler, which auto-dispatches it (`scheduling.dispatched.v1`) → the polecat supervisor
//!   slings an agent. Absent/unopenable ⇒ the loop is disabled, the daemon boots.
//! - `GT_METRICS_BIND` (`127.0.0.1:9099`) — Prometheus `/metrics` scrape endpoint (exposes
//!   `gt_workspace_session_minutes` + the golden counters from this daemon process).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gt_auth::JwtMinter;
use gt_channel::Channel;
use gt_composition::bead_close::BeadClosePlugin;
use gt_composition::checkpoint_push::{checkpoint_push_pass, drain_pass};
use gt_composition::ci_gate::CiGateState;
use gt_composition::git_merge::GitMergePlugin;
use gt_composition::patrol_bridge::PatrolBridgePlugin;
use gt_composition::mcp::eventlog::EventLog;
use gt_composition::polecat::{
    host_cap_from_metrics, rig_routing_from_catalog, AgentTokenMinter, PolecatSupervisorPlugin,
    RigConfig, ScopeResolver, DEFAULT_CAP_RETRY_SECS, DEFAULT_CI_MAX_RETRIES,
};
use gt_composition::quota_rotation::{self, QuotaRotationPlugin};
use gt_composition::role_agent::{
    RoleAgentDispatcher, RoleAgentPlugin, RoleTrigger, SpecRoleLauncher,
};
use gt_composition::session_reconcile::{ReapScope, ReapSink, SessionReconciler};
use gt_composition::witness_sweep::WitnessSweep;
use gt_composition::workflow_notify::WorkflowNotifyPlugin;
use gt_composition::{daemon_root_with_capacity, replay_quota_state, DaemonRoot};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_plugin::{spawn_plugin_relay, PluginRegistry};
use gt_polecat::{
    PolecatSupervisor, PoolAllocator, RestartConfig, RestartTracker, SpawnTemplate, Tmux, TmuxCli,
};
use gt_quota::{CredentialRecord, InMemoryKeychain, Keychain};
use gt_workspace::WorkspaceId;

/// Edge-stamped unix seconds for the supervisor clock.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse a positive `usize` env var, falling back to `default` when unset/empty/invalid.
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Resolve a stored claude-account `config_dir` to a path the daemon (HOST) can read
/// (`hq-quota-onboard-web.5`). Onboarding writes the dir from inside the backend container, so the
/// stored path is container-absolute; the daemon sees the same shared volume mounted elsewhere on
/// the host. When `stored` does not exist here but a dir with the same basename exists under
/// `host_accounts_root`, use the host one (same volume, same `<id>`). Otherwise return it unchanged:
/// host-native dirs (the `GT_CLAUDE_ACCOUNTS` env bootstrap) already exist and pass through, and a
/// path we cannot translate is left as-is (claude then falls back to its default, logged at sling).
fn resolve_host_account_dir(stored: &str, host_accounts_root: &std::path::Path) -> String {
    let p = std::path::Path::new(stored);
    if p.exists() {
        return stored.to_string();
    }
    if let Some(base) = p.file_name() {
        let candidate = host_accounts_root.join(base);
        if candidate.exists() {
            return candidate.display().to_string();
        }
    }
    stored.to_string()
}

/// Build the claude-account keychain from `GT_CLAUDE_ACCOUNTS` and seed the quota actor with the
/// same accounts (`hq-agent-provisioning.7`). The env is a comma list of `account=CLAUDE_CONFIG_DIR`
/// pairs; the first becomes the boot-active account (its creds the first polecat burns). Returns the
/// shared keychain + the account count, or `None` when the env is unset/empty (single-account mode).
/// A keychain with a single account is still returned — rotation just has nowhere to go (logged at
/// runtime when a prediction fires).
/// Build the claude-account keychain (`hq-quota-accounts.2`). Accounts come from TWO sources,
/// merged so a live-registered account survives restart without an env edit:
///
/// 1. **The quota log** (durable, the source of truth): `replay_quota_state(...).registered` maps
///    each onboarded account → its `CLAUDE_CONFIG_DIR`. These are seeded straight in; the quota
///    actor is already hydrated with the same accounts (`daemon_root` → `spawn_hydrated`), so no
///    re-emit.
/// 2. **`GT_CLAUDE_ACCOUNTS` env** (bootstrap): `account=dir` pairs. An env account NOT yet in the
///    log is promoted to a durable `AccountRegistered` (emitted ONCE — next boot it is in the log,
///    so no duplicate). This lets the operator seed the first accounts via env and have them
///    persist as events.
///
/// The active pointer follows the log's last rotation target when present (so a rotation survives
/// restart), else the first account. `None` when no account exists in either source.
async fn seed_claude_accounts(
    quota: &gt_quota::QuotaHandle,
    event_root: &std::path::Path,
    ws: &str,
) -> Option<(Arc<dyn Keychain>, usize)> {
    let log = EventLog::new(Some(event_root.to_path_buf()));
    let state = replay_quota_state(&log, ws).unwrap_or_default();
    let kc = InMemoryKeychain::new();
    let mut first: Option<String> = None;

    // 1) Durable accounts from the log. Translate each stored config_dir to a path the daemon (on
    // the HOST) can actually read: onboarding writes the dir from INSIDE the backend container
    // (hq-quota-onboard-web.4), so the stored path is container-absolute (/var/lib/gt-core/accounts/
    // <id>), but the same shared volume mounts elsewhere on the host (/var/lib/docker/volumes/
    // gt-app_gt-eventlog/_data/accounts/<id>). Resolve by basename under the host accounts root
    // (hq-quota-onboard-web.5); host-native dirs (env bootstrap below) pass through unchanged.
    let host_accounts_root = gt_composition::account_dirs::accounts_root(event_root);
    for (account, dir) in &state.registered {
        let host_dir = resolve_host_account_dir(dir, &host_accounts_root);
        if kc
            .put(CredentialRecord {
                account: account.clone(),
                secret: host_dir,
            })
            .is_ok()
        {
            first.get_or_insert_with(|| account.clone());
        }
    }

    // 2) Env bootstrap (promote new ones to durable events).
    if let Ok(raw) = std::env::var("GT_CLAUDE_ACCOUNTS") {
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((account, dir)) = entry.split_once('=') else {
                eprintln!(
                    "[gt-orch-server] GT_CLAUDE_ACCOUNTS entry '{entry}' is not account=dir — skipped"
                );
                continue;
            };
            let (account, dir) = (account.trim(), dir.trim());
            if account.is_empty() || dir.is_empty() {
                continue;
            }
            if kc
                .put(CredentialRecord {
                    account: account.to_string(),
                    secret: dir.to_string(),
                })
                .is_err()
            {
                continue;
            }
            // New to the log ⇒ persist it as an event (also upserts the live actor candidate).
            if !state.registered.contains_key(account) {
                quota.register_account(account, dir, now_secs()).await;
            }
            first.get_or_insert_with(|| account.to_string());
        }
    }

    let first = first?; // no account in either source ⇒ no keychain
                        // Last rotation target wins (log truth), else the first account. set_active fails closed if the
                        // target was deregistered — then the first account stands.
    let active = state
        .rotations
        .last()
        .map(|(_, to)| to.clone())
        .filter(|to| kc.get(to).ok().flatten().is_some())
        .unwrap_or(first);
    let _ = kc.set_active(&active);

    let n = kc.accounts().map(|a| a.len()).unwrap_or(0);
    if n == 0 {
        None
    } else {
        Some((Arc::new(kc), n))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register the process-global Prometheus counters before any actor can emit, so the
    // golden event/dead-letter metrics record from boot onward (mirrors gt-mcp-server).
    gt_telemetry::metrics::ensure_registered();

    // The Prometheus + CI-gate HTTP listener is spawned AFTER `daemon_root` below, because the
    // CI-gate routes need the merge handle it produces (gtcore-52c9ec). Resolve the bind here so an
    // invalid value surfaces early in the log.
    let metrics_bind = std::env::var("GT_METRICS_BIND").unwrap_or_else(|_| "127.0.0.1:9099".into());

    // The daemon always persists — durability is its whole point — so an unset
    // GT_EVENTLOG_ROOT falls back to the production volume, never to the in-memory
    // (`None`) mode `live_root` uses for tests.
    let event_root = PathBuf::from(
        std::env::var("GT_EVENTLOG_ROOT").unwrap_or_else(|_| DEFAULT_EVENTLOG_ROOT.into()),
    );
    let ws_slug = std::env::var("GT_WORKSPACE").unwrap_or_else(|_| "default".into());
    let ws = WorkspaceId::new(&ws_slug)
        .map_err(|e| anyhow::anyhow!("invalid GT_WORKSPACE '{ws_slug}': {e}"))?;

    eprintln!(
        "[gt-orch-server] booting workspace '{ws_slug}' — event log: {}",
        event_root.display()
    );

    // Build the durable, hydrated daemon root: hydrate scheduler + merge from the log, anchor the
    // scheduler/merge/patrol/quota actors, drain their events onto the hub, and register the
    // persistence sink + role observers + scheduler/merge reactor arms + the sheriff observer. The
    // returned handles drive the edge loops below (patrol/quota ticks + the Refinery channel).
    // Keep a copy of the log root for the keychain seed below: daemon_root consumes event_root.
    let event_root_for_seed = event_root.clone();
    // Keep a copy for the session reconciler (hq-orchd-deploy.23): it replays the same agent.* log.
    let event_root_for_reconcile = event_root.clone();
    // Keep a copy for the task custodian (gtcore-912043): it replays the same agent.* log to find
    // beads stuck `working` with no live session.
    let event_root_for_custodian = event_root.clone();
    // Keep a copy for the polecat Knowledge prompt reader (hq-polecat-knowledge.1).
    let event_root_for_polecat = event_root.clone();
    // Keep a copy for the polecat heartbeat emitter (hq-e5b288): appends AgentEvent::Heartbeat
    // for each watched session after every supervisor tick so the MCP audit trail reflects liveness.
    let event_root_for_heartbeat = event_root.clone();
    // Keep a copy for the convoy reactor (gtcore-e719c1): the slingability re-sling guard reads the
    // convoy board from here to recognise an explicit convoy dispatch, and the completion plugin
    // advances a convoy when a member bead closes.
    let event_root_for_convoy = event_root.clone();
    // A4 (gtcore-08a8be): read pool_size BEFORE daemon_root so the scheduler's capacity governor
    // matches the polecat pool — prevents over-dispatching beyond what the supervisor can sling.
    let pool_size = env_usize("GT_POOL_SIZE", 4);
    let DaemonRoot {
        handle,
        sched,
        merge,
        patrol,
        quota,
    } = daemon_root_with_capacity(ws, event_root, pool_size).await;
    eprintln!(
        "[gt-orch-server] daemon root up — scheduler(max={pool_size}) + merge + patrol + quota actors anchored; persistence + roles + reactor arms + sheriff observer running"
    );
    eprintln!(
        "[gt-orch-server] durable: hub records persisted to the per-workspace log; restart rehydrates pending queue + merge board"
    );

    // Prometheus scrape endpoint (hq-orchd.6) + CI-gate receiver (gtcore-52c9ec) on one listener.
    // The metrics half exposes THIS process's registry (gt_workspace_session_minutes etc.) for the
    // per-tenant cost dashboard; the CI-gate half receives the MCP-server webhook forward and drives
    // a merge slot to Merged/Failed via the merge handle — replacing orchd's old 60s PR poll.
    // Detached + best-effort: a bind failure logs but never aborts the orchestrator.
    let ci_gate_token = std::env::var("GT_CI_GATE_TOKEN").ok();
    let ci_gate_state = CiGateState::new(merge.clone(), ci_gate_token);
    let metrics_bind_owned = metrics_bind.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(&metrics_bind_owned, Some(ci_gate_state)).await {
            eprintln!("[gt-orch-server] metrics/ci-gate http server stopped: {e}");
        }
    });
    eprintln!("[gt-orch-server] CI-gate receiver on POST {metrics_bind}/ci-gate/{{merged,failed}}");

    // --- Autonomous polecat supervision (hq-orchd.3) ---
    // The shared admission core: pool_size (read above for scheduler alignment), host cap seeded
    // from live metrics. The sling observer claims here before spawning; the timer refreshes the
    // host cap.
    let max_restarts = env_usize("GT_POLECAT_MAX_RESTARTS", 64) as u32;
    let allocator = Arc::new(Mutex::new(PoolAllocator::new(
        host_cap_from_metrics(),
        pool_size,
    )));
    let tmux: Arc<dyn Tmux> = Arc::new(TmuxCli::new());
    let supervisor = Arc::new(PolecatSupervisor::new(
        tmux.clone(),
        RestartConfig::default(),
        max_restarts,
    ));
    let template = SpawnTemplate::from_env(&ws_slug);
    // The rig checkout (GT_RIG_PATH) the git-merge edge pushes from (hq-orchd-deploy.12). Captured
    // before `template` is moved into the polecat supervisor plugin below.
    let rig_path = template.workdir.clone();

    // Trigger-driven role agents (gtcore-999795): sheriff/witness/deacon as AGENTS WITH CRITERION,
    // slung single-shot only when their trigger fires. Gated on GT_ROLE_AGENTS. Capture the template
    // clone the role launcher needs BEFORE `template` is moved into the polecat supervisor plugin —
    // only when the feature is on, so an unused clone never lingers when it's off.
    let role_agents_on = std::env::var("GT_ROLE_AGENTS")
        .ok()
        .filter(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .is_some();
    let role_template = role_agents_on.then(|| template.clone());

    // Seed `gh` auth before the merge edge can fire (gtcore-4c9c85). `gh`'s login lives in
    // $HOME/.config/gh, but the orchd's HOME=/tmp is wiped on every pod restart — so without this,
    // the GitMergePlugin's `gh pr create`/`gh pr merge` fail with "please run: gh auth login" after
    // every redeploy and no merge closes until an operator re-seeds by hand. Setting GH_TOKEN (from
    // GT_RIG_GIT_TOKEN or the rig remote's embedded token) makes `gh` authenticated for the process
    // lifetime with no file on disk, surviving any restart. Best-effort: a missing token only logs.
    {
        let seed = gt_composition::gh_auth::seed_gh_auth(&rig_path);
        if seed.authenticated() {
            eprintln!("[gt-orch-server] {}", seed.describe());
        } else {
            eprintln!("[gt-orch-server] WARN: {}", seed.describe());
        }
    }

    // Install the polecat hook settings into the rig checkout (hq-agent-provisioning.2) so a slung
    // claude reports back: heartbeat touches + a merge-ready drop on Stop. Best-effort + marker-safe
    // — it never clobbers a human's `.claude/settings.json` (returns AlreadyExists, logged + skipped).
    match gt_polecat::install_polecat_hooks(&template.workdir) {
        Ok(path) => eprintln!("[gt-orch-server] polecat hooks installed at {}", path.display()),
        Err(e) => eprintln!(
            "[gt-orch-server] polecat hooks NOT installed ({}): {e} — slung polecats won't report back",
            template.workdir.display()
        ),
    }

    // Per-polecat git worktree root (hq-orchd-deploy.9), hoisted before the rig-catalog load so
    // every routed rig inherits the same per-bead worktree isolation (session names embed the rig
    // prefix, so one shared root never collides).
    let polecat_worktree_root: Option<PathBuf> = std::env::var("GT_POLECAT_WORKTREE_ROOT")
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from);

    // Multi-rig dispatch routing (hq-d15050, epic hq-554308): with GT_PG_URL set, read the
    // workspace's live rig catalog and build the per-prefix routing tables — bead prefix →
    // RigConfig (polecat supervisor) and bead prefix → rig checkout (git-merge edge). Each rig's
    // workdir is its resolved_worktree_root; per-rig templates inherit the boot template's shared
    // fields via SpawnTemplate::for_rig. Without GT_PG_URL (or on any load failure) both maps stay
    // empty and the daemon behaves exactly as the legacy single-rig deployment.
    let (rig_configs, rig_paths): (HashMap<String, RigConfig>, HashMap<String, PathBuf>) =
        match std::env::var("GT_PG_URL").ok().filter(|v| !v.is_empty()) {
            Some(pg_url) => match gt_store_pg::WorkspacePool::connect(&pg_url, &ws_slug).await {
                Ok(pool) => {
                    use gt_rig::RigRepository;
                    let repo = gt_rig::PgRigs::new(pool.pool().clone());
                    match repo.list().await {
                        Ok(mut rigs) => {
                            // Cross-workspace rig hydration (gtcore-c82d0f): rigs are a per-
                            // workspace catalog, but this daemon's frontier dispatches EVERY
                            // workspace's beads. Union the other tenants' rigs so a properly
                            // registered new-workspace rig routes to its OWN checkout instead of
                            // being refused (or, before the strict router, mis-slung into the
                            // boot template's repo). The boot workspace wins prefix/name
                            // collisions. Best-effort per tenant: one bad schema never blocks
                            // boot. Workspaces created AFTER boot hydrate on the next restart —
                            // until then their beads are refused loudly by the strict router.
                            match sqlx::query_scalar::<_, String>(
                                "SELECT id FROM workspaces ORDER BY id",
                            )
                            .fetch_all(pool.pool())
                            .await
                            {
                                Ok(slugs) => {
                                    for slug in slugs.into_iter().filter(|s| s != &ws_slug) {
                                        let ws_pool = match gt_store_pg::WorkspacePool::connect(
                                            &pg_url, &slug,
                                        )
                                        .await
                                        {
                                            Ok(p) => p,
                                            Err(e) => {
                                                eprintln!("[gt-orch-server] workspace '{slug}' pool connect failed: {e} — its rigs stay unroutable");
                                                continue;
                                            }
                                        };
                                        match gt_rig::PgRigs::new(ws_pool.pool().clone())
                                            .list()
                                            .await
                                        {
                                            Ok(extra) => {
                                                for rig in extra {
                                                    if rigs.iter().any(|r| {
                                                        r.prefix == rig.prefix
                                                            || r.name == rig.name
                                                    }) {
                                                        eprintln!("[gt-orch-server] rig '{}' (ws '{slug}') skipped — prefix/name collides with an earlier workspace's rig", rig.name);
                                                    } else {
                                                        eprintln!("[gt-orch-server] rig '{}' (prefix '{}') adopted from workspace '{slug}'", rig.name, rig.prefix);
                                                        rigs.push(rig);
                                                    }
                                                }
                                            }
                                            Err(e) => eprintln!("[gt-orch-server] rig list for workspace '{slug}' failed: {e} — its beads stay unroutable"),
                                        }
                                    }
                                }
                                Err(e) => eprintln!("[gt-orch-server] workspace catalog list failed: {e} — cross-workspace rigs not hydrated"),
                            }
                            eprintln!(
                                "[gt-orch-server] rig catalog loaded — {} rig(s) registered",
                                rigs.len()
                            );
                            let home = PathBuf::from(
                                std::env::var("HOME").unwrap_or_else(|_| "/root".into()),
                            );
                            // Provision any missing rig checkout from its OWN catalog git_url
                            // BEFORE routing (gtcore-d0ec4f). Without this, a rig whose checkout is
                            // absent on the host is skipped below and its beads fall back to the
                            // BOOT template — slinging a cross-rig bead (gtweb-*) into the gt-core
                            // checkout (the wrong repo). Cloning from the rig's git_url makes the
                            // route find a real checkout so the per-bead worktree carries the
                            // correct origin. Best-effort: a clone failure leaves the rig on the
                            // legacy skip.
                            let _ = gt_composition::polecat::provision_rig_checkouts(
                                &rigs, &ws_slug, &home,
                            );
                            rig_routing_from_catalog(
                                &rigs,
                                &template,
                                polecat_worktree_root.as_deref(),
                                &ws_slug,
                                &home,
                            )
                        }
                        Err(e) => {
                            eprintln!(
                                "[gt-orch-server] rig catalog list failed: {e} — single-rig dispatch (boot template only)"
                            );
                            (HashMap::new(), HashMap::new())
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[gt-orch-server] rig catalog PG connect failed: {e} — single-rig dispatch (boot template only)"
                    );
                    (HashMap::new(), HashMap::new())
                }
            },
            None => {
                eprintln!(
                    "[gt-orch-server] GT_PG_URL unset — single-rig dispatch (boot template only)"
                );
                (HashMap::new(), HashMap::new())
            }
        };

    // Per-agent least-privilege token (hq-agent-provisioning.3): mint GT_TOKEN scoped to the
    // polecat's role so it acts as ITSELF, not the operator. Needs an RS256 signing key
    // (GT_JWT_RS256_PRIVATE_KEY_FILE); absent ⇒ polecats sling without a token (logged). Scopes
    // resolve via the gt-skills catalog — empty until hq-agent-provisioning.4 seeds it, i.e.
    // least-privilege (no scopes) by default, never the operator's `*`.
    let agent_token = match JwtMinter::from_env() {
        Ok(minter) => {
            // Least-privilege role→scopes policy as a code preset (hq-agent-provisioning.4):
            // deterministic, versioned, never `*`. The daemon can't read the operator-driven
            // gt-skills catalog (separate process / store), so the agent policy lives in code.
            let catalog = gt_skills::agent_least_privilege_catalog();
            let resolver: ScopeResolver =
                Arc::new(move |role| catalog.scopes_for_roles(&[role.to_string()]));
            let ttl = env_usize("GT_AGENT_TOKEN_TTL_SECS", 3600) as u64;
            eprintln!("[gt-orch-server] per-agent token minting on (ttl {ttl}s)");
            Some(AgentTokenMinter::new(
                minter,
                resolver,
                ws_slug.clone(),
                ttl,
            ))
        }
        Err(e) => {
            eprintln!(
                "[gt-orch-server] per-agent token minting OFF ({e}) — polecats sling without GT_TOKEN"
            );
            None
        }
    };

    // The role-agent launcher mints the same kind of least-privilege per-agent token as the polecat
    // path; clone the configured minter before `agent_token` is moved into the polecat plugin below
    // (only when role agents are on, so the clone is never built needlessly).
    let role_agent_token = role_agents_on
        .then(|| agent_token.clone())
        .flatten();
    // The mayor waker mints the SAME least-privilege per-agent token as the polecat/role-agent paths
    // (gtcore-3f4d94), so the mayor's MCP/`gt` calls carry its role-scoped identity. Clone before
    // `agent_token` is moved into the polecat plugin below.
    let mayor_agent_token = agent_token.clone();

    // Claude-account keychain for predictive rotation (hq-agent-provisioning.7). GT_CLAUDE_ACCOUNTS
    // is a comma list of `account=CLAUDE_CONFIG_DIR` pairs; the first is the boot-active account.
    // Each is also seeded into the quota actor so the rotation observer has a candidate pool. Unset
    // / single account ⇒ no rotation possible (logged); the polecat uses the host default ~/.claude.
    let keychain: Option<Arc<dyn Keychain>> = match seed_claude_accounts(
        &quota,
        &event_root_for_seed,
        &ws_slug,
    )
    .await
    {
        Some((kc, n)) => {
            eprintln!("[gt-orch-server] claude keychain seeded with {n} account(s) — predictive rotation armed");
            Some(kc)
        }
        None => {
            eprintln!("[gt-orch-server] GT_CLAUDE_ACCOUNTS unset/empty — single-account mode, no rotation (polecats use host ~/.claude)");
            None
        }
    };

    // Server-side usage prober (hq-28b063 / hq-002cca): sweep every keychain account against the
    // OAuth /usage endpoint so rotation gates on the PROVIDER's window — exact resets_at, external
    // consumption included — instead of the locally-accumulated counters. Feeds the same
    // QuotaHandle::probe path the header probe uses. Only armed with a keychain (the secret is each
    // account's CLAUDE_CONFIG_DIR, where .credentials.json lives).
    let _usage_probe_timer = keychain.as_ref().map(|kc| {
        let prober =
            gt_composition::usage_probe::UsageProber::new(kc.clone(), quota.clone());
        let probe_secs = env_usize("GT_USAGE_PROBE_SECS", 300) as u64;
        eprintln!(
            "[gt-orch-server] usage prober on — sweep every {probe_secs}s against the OAuth /usage endpoint"
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(probe_secs));
            loop {
                tick.tick().await;
                prober.sweep().await;
            }
        })
    });

    // A5 (gtcore-f3a016): per-session HARD spend cap. When GT_SESSION_HARD_CAP_COST is set (in
    // cost units), a session whose cumulative spend crosses it is FROZEN at the anthropic proxy —
    // every further model call is refused, so a runaway agent stops itself ("hard gate, no
    // soft-fail silencioso"). Unset/0 ⇒ no hard gate: spend is still tracked + soft-alerted (B1),
    // just never enforced. Configured on the quota actor the proxy consults below.
    match std::env::var("GT_SESSION_HARD_CAP_COST")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
    {
        Some(cap) if cap > 0.0 => {
            quota.configure_budget_gate(Some(cap)).await;
            eprintln!(
                "[gt-orch-server] per-session hard spend cap armed at {cap} cost units (GT_SESSION_HARD_CAP_COST) — runaways freeze at the proxy"
            );
        }
        _ => {
            eprintln!(
                "[gt-orch-server] per-session hard spend cap disabled (GT_SESSION_HARD_CAP_COST unset) — spend tracked, not gated"
            );
        }
    }

    // Anthropic passthrough proxy (hq-284842): polecats' claude points here via
    // ANTHROPIC_BASE_URL; every response feeds per-call quota truth (unified-status verdicts,
    // tokens-family probe, API-reported usage samples) with zero extra requests. Listener is
    // always armed (harmless when nothing routes through it); GT_ANTHROPIC_PROXY_BIND="" disables.
    let anthropic_proxy_url: Option<String> = {
        let bind = std::env::var("GT_ANTHROPIC_PROXY_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8089".to_string());
        if bind.is_empty() {
            eprintln!("[gt-orch-server] anthropic proxy disabled (GT_ANTHROPIC_PROXY_BIND empty) — no per-call quota truth");
            None
        } else {
            let app =
                Arc::new(gt_composition::anthropic_proxy::AnthropicProxy::new(quota.clone()))
                    .router();
            match tokio::net::TcpListener::bind(&bind).await {
                Ok(listener) => {
                    let url = std::env::var("GT_ANTHROPIC_PROXY_URL")
                        .unwrap_or_else(|_| format!("http://{bind}"));
                    eprintln!(
                        "[gt-orch-server] anthropic proxy on http://{bind} (polecats see {url})"
                    );
                    tokio::spawn(async move {
                        if let Err(e) = axum::serve(listener, app).await {
                            eprintln!("[gt-orch-server] anthropic proxy serve failed: {e}");
                        }
                    });
                    Some(url)
                }
                Err(e) => {
                    eprintln!("[gt-orch-server] anthropic proxy bind {bind} failed: {e} — polecats go straight to the API");
                    None
                }
            }
        }
    };

    // Re-sling account re-resolution (hq-49198f): a dead polecat's stored spec may point at an
    // account that blocked/rotated since the original sling — re-slinging it verbatim burns
    // max_restarts against dead credentials. Rewrite the account-dependent env from the
    // keychain's CURRENT active pointer just before each re-sling; branch/worktree survive.
    if let Some(kc) = &keychain {
        let kc = kc.clone();
        let proxy = anthropic_proxy_url.clone();
        let respec_quota = quota.clone();
        let respec_handle = tokio::runtime::Handle::current();
        supervisor.set_respec(Box::new(move |mut spec| {
            fn set_env(env: &mut Vec<(String, String)>, key: &str, value: String) {
                match env.iter_mut().find(|(k, _)| k == key) {
                    Some((_, v)) => *v = value,
                    None => env.push((key.to_string(), value)),
                }
            }
            // gtcore-bf4acd: re-resolve through the credential guard, not a raw active()+get() —
            // a re-sling must VALIDATE the active account's creds (and rotate off a dead one) so a
            // polecat that died is not re-slung straight back into 401. A NoValidAccount / host
            // default outcome leaves the stored env untouched (legacy behaviour).
            // gtcore-2836bb: ALSO snapshot quota status so the re-sling rotates off a
            // Limited/Blocked active account — a re-sling onto a rate-limited account births the
            // polecat into the usage-limit dialog. The respec runs inside the supervisor's
            // spawn_blocking tick, so block_on the snapshot is safe (same posture as the slingability probe).
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let q = respec_quota.clone();
            let quota_status: std::collections::HashMap<String, gt_quota::AccountQuotaStatus> =
                respec_handle.block_on(async move {
                    q.accounts()
                        .await
                        .into_iter()
                        // gtcore-62723a: a prober-confirmed-dead credential 401s every sling even
                        // though its quota `status` reads Healthy (the budget is untouched). Report
                        // it as Blocked so `resolve_for_sling` never picks it — the guard rotates to
                        // a live account instead of slinging a polecat that can't authenticate.
                        .map(|a| {
                            let status = if a.credential_dead {
                                gt_quota::AccountQuotaStatus::Blocked
                            } else {
                                a.status
                            };
                            (a.id, status)
                        })
                        .collect()
                });
            if let gt_composition::credential_guard::CredOutcome::Resolved { resolved, .. } =
                gt_composition::credential_guard::resolve_for_sling(&kc, now_ms, |acc| {
                    quota_status.get(acc).copied()
                }, |_| 100.0)
            {
                set_env(&mut spec.env, "CLAUDE_CONFIG_DIR", resolved.config_dir);
                set_env(
                    &mut spec.env,
                    gt_polecat::GT_HOOK_ACCOUNT,
                    resolved.account.clone(),
                );
                if proxy.is_some() {
                    set_env(
                        &mut spec.env,
                        "ANTHROPIC_CUSTOM_HEADERS",
                        format!(
                            "x-gt-account: {}\nx-gt-session: {}",
                            resolved.account, spec.session
                        ),
                    );
                }
            }
            spec
        }));
        eprintln!("[gt-orch-server] re-sling account re-resolution armed (keychain-backed)");
    }

    // Slingability re-sling guard (gtcore-177770, unified by gtcore-db99e0): a polecat can die
    // after its bead became un-slingable — closed (operator or merge auto-close, work delivered),
    // flipped to an epic container, or set dispatch=manual. Re-slinging it then burns the restart
    // budget on work no autonomous agent should touch and keeps the slot occupied, starving new
    // dispatches. Wire a probe the supervisor consults before each re-sling: it re-reads the bead's
    // CURRENT state and applies the unified `should_sling` predicate via `bead_should_sling` — the
    // SAME decision the dispatch→sling path and the auto-dispatch frontier use. The supervisor's
    // `tick` runs inside `spawn_blocking`, so blocking on the current runtime handle is safe here.
    // Only a positive ¬slingable verdict drops; an unknown bead or a query error is treated as
    // slingable and falls through to the normal re-sling path. Env-gated on GT_DOLT_URL — without it
    // the guard is off and dead polecats re-sling as before.
    //
    // rig-hold H3 (gtcore-9a84e6): the held-rigs source, SHARED by the dead-polecat re-sling guard
    // (this closure) and the supervisor plugin's crash/CI-failure re-sling (`with_held_rigs` below) —
    // a `rig.hold` pauses the watchdogs too, else they restart exactly the work H2's hold paused.
    // Fail-soft: no GT_PG_URL ⇒ no rig is ever held (pre-feature behaviour).
    let held_rigs_source: Option<Arc<dyn gt_composition::auto_dispatch::HeldRigs>> =
        match std::env::var("GT_PG_URL").ok().filter(|v| !v.is_empty()) {
            Some(pg_url) => match gt_store_pg::WorkspacePool::connect(&pg_url, &ws_slug).await {
                Ok(pool) => Some(Arc::new(gt_composition::auto_dispatch::CatalogHeldRigs::new(
                    gt_rig::PgRigs::new(pool.pool().clone()),
                )) as Arc<dyn gt_composition::auto_dispatch::HeldRigs>),
                Err(e) => {
                    eprintln!("[gt-orch-server] rig-hold (watchdogs) OFF — held-rigs pool connect failed: {e}");
                    None
                }
            },
            None => None,
        };
    match std::env::var("GT_DOLT_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|url| gt_store_dolt::DoltIssues::connect(&url).ok())
    {
        Some(store) => {
            let store = Arc::new(store);
            let handle = tokio::runtime::Handle::current();
            let held_for_guard = held_rigs_source.clone();
            // gtcore-7d16f0: the crash / boot re-hydration re-sling guard must also recognise an
            // explicit convoy dispatch, else a crashed convoy member (always dispatch=manual) is
            // never re-slung. Read the convoy board from the shared log, same as the dispatch path.
            let convoy_log = Arc::new(EventLog::new(Some(event_root_for_convoy.clone())));
            let convoy_ws = ws_slug.clone();
            supervisor.set_bead_slingable(Box::new(move |bead: &str| {
                let store = store.clone();
                let bead = bead.to_string();
                let held_for_guard = held_for_guard.clone();
                let convoy_override = gt_composition::convoy_reactor::active_convoy_membership(
                    &convoy_log,
                    Some(&convoy_ws),
                    &bead,
                )
                .is_some();
                handle.block_on(async move {
                    let held = match &held_for_guard {
                        Some(s) => s.held().await,
                        None => std::collections::HashSet::new(),
                    };
                    gt_composition::polecat::bead_should_sling(&store, &bead, &held, convoy_override)
                        .await
                })
            }));
            eprintln!("[gt-orch-server] slingability re-sling guard armed (Dolt-backed: closed/epic/manual dropped)");
        }
        None => {
            eprintln!("[gt-orch-server] slingability re-sling guard OFF — GT_DOLT_URL unset (closed/epic/manual beads may re-sling)")
        }
    }

    // Wedge recovery hook (gtcore-2836bb, completed by gtcore-f396dc): the supervisor DETECTS an
    // alive-but-wedged polecat (trust/promo dialog, usage-limit modal, or a fresh session whose
    // positional kickoff prompt was eaten and now idles at an empty input box), but the recovery
    // belongs to composition — re-seed the onboarding/promo flags into the session's ACTUAL config
    // dir so the re-sling that follows boots past the dialog. Without this hook the detection is
    // off entirely ("no hook ⇒ no wedge detection"): the gtcore-2836bb machinery shipped dark,
    // which is how 4/4 re-slung polecats sat wedged unnoticed on 2026-07-02.
    supervisor.set_on_wedge(Box::new(|spec, dialog| {
        match dialog.recovery() {
            gt_polecat::WedgeRecovery::ReseedOnboarding => {
                // Same effective-config-dir resolution as the sling path: the stamped
                // CLAUDE_CONFIG_DIR when an account was resolved, else claude's default.
                let config_dir = spec
                    .env
                    .iter()
                    .find(|(k, _)| k == "CLAUDE_CONFIG_DIR")
                    .map(|(_, v)| std::path::PathBuf::from(v))
                    .or_else(|| {
                        std::env::var_os("HOME")
                            .map(|h| std::path::Path::new(&h).join(".claude"))
                    });
                match config_dir {
                    Some(cd) => {
                        gt_composition::worktree::seed_claude_onboarding(&cd, &spec.workdir);
                        eprintln!(
                            "[gt-orch-server] wedge recovery session={} ({}): re-seeded onboarding/promo flags at {}",
                            spec.session,
                            dialog.reason(),
                            cd.display()
                        );
                    }
                    None => eprintln!(
                        "[gt-orch-server] wedge recovery session={} ({}): no config dir resolvable — re-sling only",
                        spec.session,
                        dialog.reason()
                    ),
                }
            }
            gt_polecat::WedgeRecovery::RotateAccount => {
                // Nothing to mutate here: the re-sling path re-resolves the account through the
                // respec closure (keychain + quota snapshot), which rotates off the limited one.
                eprintln!(
                    "[gt-orch-server] wedge recovery session={} ({}): account re-resolves at re-sling",
                    spec.session,
                    dialog.reason()
                );
            }
        }
    }));
    eprintln!(
        "[gt-orch-server] wedge detection armed — trust/promo/usage-limit dialogs + fresh idle-empty-prompt sessions recover via kill + re-sling"
    );

    // Web onboarding (hq-quota-onboard-web) moved to the backend mcp-server in .4: claude now lives
    // IN the image, so onboarding rides the existing /api/v1/* auth chain instead of a host process
    // behind a docker→host firewall hole. The daemon no longer serves it — it only hydrates its
    // rotation keychain from the accounts the backend registers into the shared quota log.

    // Snapshot the boot template's shared launch fields BEFORE it is moved into the polecat
    // supervisor plugin below — mayor-mode auto-dispatch (gtcore-d72302) reuses them to launch the
    // per-rig MAYOR session the same way (same agent command/args/env, same checkout).
    let mayor_launch = (
        template.command.clone(),
        template.args.clone(),
        template.base_env.clone(),
        template.workdir.clone(),
    );
    // Same snapshot for the resident role sessions (gtcore-3246e8): the host launches
    // sheriff/witness/deacon/refinery with the same agent command/env/checkout as everyone else.
    let resident_launch = (
        template.rig.clone(),
        template.command.clone(),
        template.args.clone(),
        template.base_env.clone(),
        template.workdir.clone(),
    );

    // Observe the SAME hub the root drains actor output onto: a fresh broadcast receiver, so the
    // sling observer runs independently of the root's own plugin relay (durability/roles/reactor).
    let mut pol_plugin = PolecatSupervisorPlugin::new(
        ws_slug.clone(),
        tmux.clone(),
        template,
        supervisor.clone(),
        allocator.clone(),
    )
    // Emit agent.spawned/session-end onto the hub (hq-orchd.6) so the session-minutes
    // projector (registered inside daemon_root) feeds gt_workspace_session_minutes.
    .with_session_events(handle.events_sender())
    // Route dispatched beads to their rig by prefix (hq-0ecfec/hq-d15050): an empty map (no
    // GT_PG_URL / empty catalog) is a no-op — every bead keeps the boot template.
    .with_rig_configs(rig_configs);
    if let Some(tm) = agent_token {
        pol_plugin = pol_plugin.with_agent_token(tm);
    }
    if let Some(kc) = &keychain {
        pol_plugin = pol_plugin.with_keychain(kc.clone());
    }
    // Sling-time quota-status gate (gtcore-2836bb): the credential guard also rotates off an active
    // account that is quota-Limited/Blocked, so a polecat is never slung into the rate-limit dialog.
    pol_plugin = pol_plugin.with_quota(quota.clone());
    // CI-failure recovery (gtcore-3a1bd4): on `merge.failed.v1` the supervisor re-slings the bead
    // with a fix-and-re-push prompt (carrying the CI failure context) up to GT_CI_MAX_RETRIES times,
    // then escalates to the operator. The scheduler handle lets it free dispatch capacity when the
    // budget is exhausted (the slot is held across retries, like a crash re-sling).
    let ci_max_retries = std::env::var("GT_CI_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CI_MAX_RETRIES);
    pol_plugin = pol_plugin
        .with_scheduler(sched.clone())
        .with_ci_max_retries(ci_max_retries);
    eprintln!(
        "[gt-orch-server] CI-failure auto re-sling on — up to {ci_max_retries} retries before escalation (GT_CI_MAX_RETRIES)"
    );
    if let Some(url) = &anthropic_proxy_url {
        pol_plugin = pol_plugin.with_anthropic_proxy(url.clone());
    }
    // Per-polecat git worktree (hq-orchd-deploy.9): with GT_POLECAT_WORKTREE_ROOT set, each sling
    // gets its own worktree off the rig checkout (branch = bead) so concurrent polecats don't race
    // on a shared HEAD (CLAUDE.md). Unset ⇒ the legacy single shared checkout, unchanged.
    if let Some(root) = &polecat_worktree_root {
        eprintln!(
            "[gt-orch-server] per-polecat worktrees under {} (branch = bead, off the rig checkout)",
            root.display()
        );
        // gtcore-acacfb: a pod restart kills every in-pod polecat, so every worktree under the root
        // is an orphan from a prior life carrying a full cargo target/ (tens of GB). Sweep them at
        // boot — BEFORE dispatch resumes — else they accumulate and fill the disk (107 trees /
        // 372 GB found 2026-06-28, co-cause of the etcd I/O-saturation incident). Empty live set =
        // nothing is alive yet at boot.
        let swept = gt_composition::worktree::sweep_orphans(root, &std::collections::HashSet::new());
        if swept > 0 {
            eprintln!(
                "[gt-orch-server] worktree boot sweep: reclaimed {swept} orphan tree(s) under {}",
                root.display()
            );
        }
        pol_plugin = pol_plugin.with_worktree_root(root.clone());
    } else {
        eprintln!("[gt-orch-server] GT_POLECAT_WORKTREE_ROOT unset — polecats share the rig checkout (single-polecat safe only)");
    }
    // Dynamic .mcp.json per sling (hq-polecat-rig-config.1): when GT_SELF_URL is set the plugin
    // writes a fresh `.mcp.json` using the per-session token rather than copying the static
    // operator-placed file — so the agent's MCP auth survives rig changes and token rotation.
    if let Some(url) = std::env::var("GT_SELF_URL").ok().filter(|v| !v.is_empty()) {
        eprintln!("[gt-orch-server] dynamic .mcp.json per sling on (GT_SELF_URL={url})");
        pol_plugin = pol_plugin.with_server_url(url);
    } else {
        eprintln!("[gt-orch-server] GT_SELF_URL unset — polecats copy static .mcp.json from base checkout (token may expire)");
    }
    // Drop privileges per polecat (hq-quota-accounts.6): GT_POLECAT_RUN_AS re-execs the polecat as
    // a dedicated non-root user (the command wrap is in SpawnTemplate::from_env) and chowns its
    // worktree to that user here. The daemon stays root (for the eventlog volume); the polecat —
    // which holds account creds + skips permission prompts — does not. Unset ⇒ runs as root (legacy).
    if let Some(user) = std::env::var("GT_POLECAT_RUN_AS")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        eprintln!("[gt-orch-server] polecats run as non-root user '{user}' (privilege drop on)");
        pol_plugin = pol_plugin.with_run_as(user);
    } else {
        eprintln!("[gt-orch-server] GT_POLECAT_RUN_AS unset — polecats run as the daemon uid (root); see hq-quota-accounts.6");
    }
    // Knowledge role prompt for each sling (hq-polecat-knowledge.1): replay skills.* from the same
    // event log the backend writes to — the polecat role's prompt becomes CLAUDE.md in its worktree,
    // the same pattern terminal.rs uses for interactive sessions.
    let knowledge_log = Arc::new(EventLog::new(Some(event_root_for_polecat)));
    pol_plugin = pol_plugin.with_event_log(knowledge_log.clone());
    // Resident role sessions (gtcore-3246e8, epic gtcore-4c40b5): with GT_ROLE_SESSIONS=1 the
    // infra roles (sheriff/witness/deacon/refinery) live as LONG-LIVED tmux sessions on the
    // mayor pattern — spawned at boot, idle-blocked on their wake file (idle ≈ 0 tokens),
    // heartbeated and re-raised by a supervision pass when they die. Credential resolution,
    // onboarding seed, role-scoped tokens and role skills ride the same shared paths as the
    // mayor/polecat spawns. Default OFF: the single-shot role agents below stay byte-for-byte
    // the production behaviour until the rollout bead (gtcore-d58dff) flips the flag.
    let resident_host: Option<Arc<gt_composition::role_resident::ResidentRoleHost>> =
        if std::env::var("GT_ROLE_SESSIONS").ok().as_deref() == Some("1") {
            let (res_rig, res_cmd, res_args, res_env, res_workdir) = resident_launch;
            let channel_root = std::env::var("GT_CHANNEL_ROOT")
                .unwrap_or_else(|_| "/gt/.channels".to_string());
            let mut host = gt_composition::role_resident::ResidentRoleHost::new(
                tmux.clone(),
                ws_slug.clone(),
                res_rig,
                res_workdir,
                res_cmd,
                res_args,
                res_env,
                std::path::PathBuf::from(channel_root),
            );
            if let Some(kc) = &keychain {
                host = host.with_keychain(kc.clone());
            }
            host = host.with_quota(quota.clone());
            if let Some(url) = &anthropic_proxy_url {
                host = host.with_anthropic_proxy(url.clone());
            }
            host = host.with_event_log(knowledge_log.clone());
            if let Some(tm) = &mayor_agent_token {
                host = host.with_agent_token(tm.clone());
            }
            if let Some(url) = std::env::var("GT_SELF_URL").ok().filter(|v| !v.is_empty()) {
                host = host.with_server_url(url);
            }
            host = host.with_session_events(handle.events_sender());
            let host = Arc::new(host);
            // Boot + re-raise supervision: every resident ensured now and on each pass.
            let _resident_supervisor = host.clone().spawn_supervisor(Duration::from_secs(60));
            eprintln!(
                "[gt-orch-server] resident role sessions ON — sheriff/witness/deacon/refinery live in tmux, supervised every 60s (GT_ROLE_SESSIONS=1)"
            );
            Some(host)
        } else {
            None
        };
    // Consumed by the trigger rewiring (gtcore-865fb8); until then the supervision pass is the host's driver.
    let _ = &resident_host;
    // rig-hold H3 (gtcore-9a84e6): the supervisor plugin's crash re-sling (boot re-hydration /
    // stale dispatch) and CI-failure re-sling (sheriff) skip a bead whose rig is on hold — reusing
    // the same source the dead-polecat guard above uses.
    if let Some(source) = held_rigs_source.clone() {
        pol_plugin = pol_plugin.with_held_rigs(source);
        eprintln!(
            "[gt-orch-server] rig-hold watchdog guard on — supervisor skips re-sling of held rigs (crash + CI-failure)"
        );
    }
    eprintln!("[gt-orch-server] Knowledge role prompt on — polecat CLAUDE.md from skills.* log");
    // Dolt issues store for the polecat sling → working transition + bead auto-close. Resolved
    // once and shared across both plugins. Env-gated on GT_DOLT_URL — without it the bead stays
    // `open` until the polecat self-transitions and merged beads stay `working`.
    let dolt_issues: Option<Arc<gt_store_dolt::DoltIssues>> = std::env::var("GT_DOLT_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|url| gt_store_dolt::DoltIssues::connect(&url).ok())
        .map(Arc::new);
    // Transition beads open→working at sling time (gtcore-orchd-working): the frontend and the
    // auto-dispatch frontier see the state change immediately, not after the polecat self-transitions.
    if let Some(issues) = &dolt_issues {
        pol_plugin = pol_plugin.with_issues(issues.clone());
        eprintln!("[gt-orch-server] sling→working transition on — beads flip to working at spawn");
    } else {
        eprintln!("[gt-orch-server] sling→working transition OFF — GT_DOLT_URL unset (beads stay open until agent self-transitions)");
    }
    // Merge-board boot reconciliation phase 1 (gtcore-088db9): settle any hydrated slot whose bead
    // already closed with a `delivered_sha` — walk it to `Merged` with NO git attempt (boot-time
    // `try_complete_slot`). Run HERE, before the git-merge edge subscribes onto the hub (pol_relay,
    // below), so the `start`/`complete` it emits cannot trigger a real merge for work already on
    // main (the gtcore-4ad682 hazard). Phase 2 (`reconcile_inflight_slots`) runs after the edge is
    // live. Gated on GT_DOLT_URL: without the issues store there is no close-state to read.
    if let Some(issues) = &dolt_issues {
        let issues = issues.clone();
        let healed = gt_composition::merge_boot::settle_delivered_slots(&merge, |bead| {
            let issues = issues.clone();
            async move {
                match issues.get_detail(&bead).await {
                    Ok(Some(detail)) if detail.status == "closed" => detail
                        .delivered_sha
                        .filter(|s| !s.trim().is_empty()),
                    _ => None,
                }
            }
        })
        .await;
        if healed > 0 {
            eprintln!(
                "[gt-orch-server] merge boot reconcile (phase 1) — {healed} delivered slot(s) auto-completed"
            );
        }
    }
    // Cap-parked retry (gtcore-f527f6): a dispatch refused at pool/host cap is re-emitted onto the
    // dispatch channel after this many seconds instead of being dropped — the post-OOM
    // depressed-cap boot self-heals. The sink mirrors the consumer's backend selection below
    // (PG queue under GT_EVENTLOG_PG, else the file channel), so the retry always re-enters
    // through the consumer that re-seeds the bead `Pending`.
    let cap_retry_secs = env_usize("GT_CAP_RETRY_SECS", DEFAULT_CAP_RETRY_SECS as usize) as u64;
    pol_plugin = pol_plugin.with_cap_retry_secs(cap_retry_secs);
    let cap_retry_sink: Option<gt_channel::DispatchSink> = {
        let name = std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
        let want_pg = std::env::var("GT_EVENTLOG_PG")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        match (want_pg, std::env::var("GT_PG_URL").ok()) {
            (true, Some(pg_url)) => gt_channel::PgQueue::connect(&pg_url, &name)
                .and_then(|q| q.ensure_schema().map(|()| q))
                .map(gt_channel::DispatchSink::Pg)
                .map_err(|e| eprintln!("[gt-orch-server] cap-parked retry sink (PG) init failed: {e}"))
                .ok(),
            _ => std::env::var("GT_CHANNEL_ROOT")
                .ok()
                .and_then(|root| gt_channel::Channel::open(&root, &name).ok())
                .map(gt_channel::DispatchSink::File),
        }
    };
    match cap_retry_sink {
        Some(sink) => {
            pol_plugin = pol_plugin.with_dispatch_sink(Arc::new(sink));
            eprintln!(
                "[gt-orch-server] cap-parked sling retry on — a pool/host-cap refusal re-enqueues after {cap_retry_secs}s"
            );
        }
        None => eprintln!(
            "[gt-orch-server] cap-parked sling retry OFF — no dispatch sink (GT_CHANNEL_ROOT/GT_EVENTLOG_PG unset); cap refusals stay terminal"
        ),
    }
    // Register the polecat supervisor and — when a keychain exists — the predictive rotation
    // observer on the same relay: a `quota.block_predicted.v1` / `quota.account_limited.v1` flips
    // the keychain's active pointer so the NEXT sling lands on a healthy account.
    let mut pol_registry = PluginRegistry::new().register(pol_plugin);
    if let Some(kc) = &keychain {
        // Wire the supervisor so the rotation observer can detect and surface in-flight polecat
        // risk when an account is rotated (hq-quota-refinement.3).
        pol_registry = pol_registry.register(
            QuotaRotationPlugin::new(quota.clone(), kc.clone())
                .with_supervisor(supervisor.clone()),
        );
        eprintln!("[gt-orch-server] predictive account rotation observer on");
    }
    // Git-merge edge effect (hq-orchd-deploy.12): land a polecat branch on main. On
    // `merge.started.v1` it pushes <branch>:main from the rig checkout (rebasing on divergence) and
    // drives the slot to Merged/Failed — the arm that closes the autonomous loop. Registered on the
    // same hub relay as the polecat sling (both are real I/O, kept out of pure-state daemon_root).
    // Routed by bead prefix (hq-c846f5): a gtweb bead's ff-merge runs from the gtweb checkout;
    // unknown prefixes (or an empty catalog) fall back to the boot rig checkout.
    let ci_gated = std::env::var("GT_CI_GATED_MERGE")
        .ok()
        .filter(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .is_some();
    // The distinct rig checkouts the checkpoint-push timer (below) sweeps for in-flight branches:
    // every routed rig path plus the boot rig fallback. Captured before `rig_paths` is moved into
    // the merge plugin; the pass dedups, so an overlap with `rig_path` is harmless.
    let checkpoint_rigs: Vec<PathBuf> = rig_paths
        .values()
        .cloned()
        .chain(std::iter::once(rig_path.clone()))
        .collect();
    // The same rig set, captured for the final drain on SIGTERM/preStop (gtcore-0179f8): the periodic
    // checkpoint timer below MOVES `checkpoint_rigs` into its closure, so the shutdown path needs its
    // own copy to commit + push every worktree's pending work before the pod dies.
    let drain_rigs = checkpoint_rigs.clone();
    // Per-prefix rig routing for the phase-2 merge-board reconciler (gtcore-088db9), captured before
    // `rig_paths` is moved into the plugin: it resolves each orphaned `Merging` slot's checkout the
    // same way the edge does (bead prefix → rig, else the boot fallback) to probe origin/main.
    let reconcile_rig_paths = rig_paths.clone();
    let reconcile_rig_fallback = rig_path.clone();
    pol_registry = pol_registry.register(
        GitMergePlugin::with_rig_paths(merge.clone(), rig_paths, rig_path.clone())
            .with_ci_gated(ci_gated),
    );
    if ci_gated {
        eprintln!(
            "[gt-orch-server] git-merge edge on — CI-gated PRs from rig checkout {} (+ per-rig routing)",
            rig_path.display()
        );
    } else {
        eprintln!(
            "[gt-orch-server] git-merge edge on — branches land on main from rig checkout {} (+ per-rig routing)",
            rig_path.display()
        );
    }
    // Bead auto-close on merge (Fase 2 bug fix): when a branch lands on main
    // (`merge.merged.v1`), close the bead in Dolt so its surfaces are freed for
    // new dispatch. Without this, merged beads stay `status='working'` and block
    // `working_surfaces()` / `occupied_surfaces` indefinitely.
    match &dolt_issues {
        Some(store) => {
            pol_registry = pol_registry.register(BeadClosePlugin::new(store.clone()));
            eprintln!("[gt-orch-server] bead auto-close on — merged beads transition to closed");
        }
        None => {
            eprintln!("[gt-orch-server] bead auto-close OFF — GT_DOLT_URL unset (merged beads stay working)")
        }
    }
    // Workflow notifications (hq-b7f7c1, epic hq-bb12a2): mirror dispatch / merge-landed /
    // merge-failed onto the operator's notification bell — same write surfaces as notify.send
    // (public.notifications + the SSE event log). Armed only with GT_PG_URL; best-effort inside.
    match std::env::var("GT_PG_URL").ok().filter(|v| !v.is_empty()) {
        Some(pg_url) => match sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(&pg_url)
        {
            Ok(pool) => {
                let mut notify_plugin =
                    WorkflowNotifyPlugin::new(pool, knowledge_log.clone(), ws_slug.clone());
                // Phase 2 (hq-80e92c): GT_NOTIFY_EMAIL ⇒ merge failures also email the operator
                // via the existing email_outbox drain. Unset ⇒ bell only.
                match std::env::var("GT_NOTIFY_EMAIL").ok().filter(|v| !v.trim().is_empty()) {
                    Some(rcpt) => {
                        eprintln!("[gt-orch-server] merge-failure emails on → {rcpt}");
                        notify_plugin = notify_plugin.with_failure_email(rcpt);
                    }
                    None => eprintln!(
                        "[gt-orch-server] merge-failure emails off — GT_NOTIFY_EMAIL unset (bell only)"
                    ),
                }
                pol_registry = pol_registry.register(notify_plugin);
                eprintln!(
                    "[gt-orch-server] workflow notifications on — dispatch/merged/failed + operational alerts (all-accounts-exhausted, lease-expired, pool-cap sling-skipped, sling-failed) reach the operator bell"
                );
            }
            Err(e) => eprintln!("[gt-orch-server] workflow notifications OFF (pg pool: {e})"),
        },
        None => {
            eprintln!("[gt-orch-server] workflow notifications OFF — GT_PG_URL unset")
        }
    }
    // A2A delegation push-callbacks (B5, gtcore-1bda00): replace the parent's
    // `a2a.status` poll with a push. `DelegationCallbackPlugin` observes the hub's
    // terminal facts (merge.merged/merge.failed/agent.killed) and, for an OPEN
    // delegation (registered by a2a.delegate as `delegation.requested.v1`), emits
    // a durable `delegation.completed.v1` callback (+ operator bell when GT_PG_URL
    // is set). The timeout ticker auto-escalates a delegation stuck past its
    // `timeout_secs`. The plugin is always on (the registry lives on the shared
    // event log); the bell is the only GT_PG_URL-gated part.
    let deleg_ws = (ws_slug != "default").then(|| ws_slug.clone());
    let deleg_bell_pool = std::env::var("GT_PG_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|pg_url| {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .connect_lazy(&pg_url)
                .map_err(|e| eprintln!("[gt-orch-server] delegation bell OFF (pg pool: {e})"))
                .ok()
        });
    {
        let mut plugin = gt_composition::delegation::DelegationCallbackPlugin::new(
            knowledge_log.clone(),
            deleg_ws.clone(),
        );
        if let Some(pool) = &deleg_bell_pool {
            plugin = plugin.with_bell(pool.clone(), ws_slug.clone());
        }
        pol_registry = pol_registry.register(plugin);
        eprintln!(
            "[gt-orch-server] a2a delegation callbacks on — terminal beads push delegation.completed.v1{}",
            if deleg_bell_pool.is_some() { " + operator bell" } else { "" }
        );
    }
    // Built here, spawned alongside the other reactor ticks below.
    let delegation_ticker = {
        let mut ticker = gt_composition::delegation::DelegationTimeoutTicker::new(
            knowledge_log.clone(),
            deleg_ws.clone(),
        );
        if let Some(pool) = &deleg_bell_pool {
            ticker = ticker.with_bell(pool.clone(), ws_slug.clone());
        }
        Arc::new(ticker)
    };
    // Human-escalation operator notifications (A6, gtcore-46c9dc, epic hq-bb12a2): a blocked
    // agent's `escalation.requested.v1` reaches the operator bell + email with a direct link, and
    // a periodic ticker re-pings escalations still pending past N hours. Both ride the same
    // GT_PG_URL-gated pool the delegation bell uses (no bell ⇒ no operator surface ⇒ skip). The
    // reminder ticker is built here, spawned alongside the delegation timer below.
    let escalation_ticker = match &deleg_bell_pool {
        Some(pool) => {
            let notifier = gt_composition::escalation_notify::OperatorNotifier::new(
                pool.clone(),
                knowledge_log.clone(),
                ws_slug.clone(),
            )
            .with_email(std::env::var("GT_NOTIFY_EMAIL").unwrap_or_default())
            .with_public_url(std::env::var("GT_PUBLIC_URL").unwrap_or_default());
            // Observer: each new escalation rings the operator the moment it lands.
            pol_registry = pol_registry.register(
                gt_composition::escalation_notify::EscalationNotifyPlugin::new(notifier.clone()),
            );
            let reminder_secs = env_usize(
                "GT_ESCALATION_REMINDER_SECS",
                gt_composition::escalation_notify::DEFAULT_REMINDER_SECS as usize,
            ) as u64;
            eprintln!(
                "[gt-orch-server] escalation notifications on — operator bell/email on escalate.request; reminders every {reminder_secs}s ({})",
                if std::env::var("GT_NOTIFY_EMAIL").ok().filter(|v| !v.trim().is_empty()).is_some() {
                    "bell + email"
                } else {
                    "bell only"
                }
            );
            Some(Arc::new(
                gt_composition::escalation_notify::EscalationReminderTicker::new(
                    knowledge_log.clone(),
                    deleg_ws.clone(),
                    notifier,
                    reminder_secs,
                ),
            ))
        }
        None => {
            eprintln!("[gt-orch-server] escalation notifications OFF — GT_PG_URL unset (no operator bell)");
            None
        }
    };
    // Rig VCS-connection health sweep (gtcore-406b12, epic gtcore-0e095b): ring the operator bell
    // when a rig becomes unbound or its connection goes inactive (the dev data-wipe left the rigs
    // unbound silently). Same GT_PG_URL-gated bell as escalations; gated additionally on
    // GT_RIG_CONNECTION_CHECK_SECS > 0 (off by default). Self-contained loop, spawned here.
    {
        let check_secs = env_usize("GT_RIG_CONNECTION_CHECK_SECS", 0) as u64;
        match (&deleg_bell_pool, check_secs) {
            (Some(pool), secs) if secs > 0 => {
                match std::env::var("GT_PG_URL")
                    .ok()
                    .filter(|v| !v.is_empty())
                {
                    Some(pg_url) => {
                        match gt_composition::rig_connection_notify::PgRigHealthSource::connect(
                            &pg_url,
                            ws_slug.clone(),
                        )
                        .await
                        {
                            Ok(source) => {
                                let notifier =
                                    gt_composition::escalation_notify::OperatorNotifier::new(
                                        pool.clone(),
                                        knowledge_log.clone(),
                                        ws_slug.clone(),
                                    )
                                    .with_public_url(
                                        std::env::var("GT_PUBLIC_URL").unwrap_or_default(),
                                    );
                                let public_url =
                                    std::env::var("GT_PUBLIC_URL").unwrap_or_default();
                                eprintln!(
                                    "[gt-orch-server] rig connection-health sweep on — operator bell every {secs}s when a rig is unbound/inactive"
                                );
                                let ticker = gt_composition::rig_connection_notify::RigConnectionHealthTicker::new(
                                    std::sync::Arc::new(source),
                                    notifier,
                                    secs,
                                    public_url,
                                );
                                tokio::spawn(ticker.run());
                            }
                            Err(e) => eprintln!(
                                "[gt-orch-server] rig connection-health sweep OFF — pool connect failed: {e}"
                            ),
                        }
                    }
                    None => {}
                }
            }
            _ => {}
        }
    }
    // Patrol bridge (gtcore-a33952 — C2): agent.spawned → lease, session-end/killed → close,
    // patrol.lease-expired → release_claim (Dolt CAS). Env-gated on GT_DOLT_URL — without it,
    // the bridge is off and crashed agents stay working until manual reconciliation.
    match std::env::var("GT_DOLT_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|url| gt_store_dolt::DoltIssues::connect(&url).ok())
    {
        Some(store) => {
            pol_registry =
                pol_registry.register(PatrolBridgePlugin::new(patrol.clone(), Arc::new(store)));
            eprintln!(
                "[gt-orch-server] patrol bridge on — agent leases expire into release_claim"
            );
        }
        None => {
            eprintln!("[gt-orch-server] patrol bridge OFF — GT_DOLT_URL unset (lease expiry will not auto-release claims)")
        }
    }
    // Autonomous dispatch loop (Fase 2 — A1): polls the ready_for_auto frontier
    // and feeds eligible beads to the scheduler. Env-gated: GT_AUTO_DISPATCH=1 +
    // GT_DOLT_URL. The completion plugin is registered on the SAME hub relay so
    // slot-freeing events (issues.closed.v1, patrol.lease-expired.v1) are observed.
    //
    // Two dispatch shapes share the same env gate + ready frontier (gtcore-d72302):
    //
    //   * DIRECT (default, GT_DISPATCH_VIA_MAYOR unset/0): the orchd owns the
    //     bead-by-bead decision — every ready bead is seeded + enqueued on the
    //     scheduler, which slings a polecat per bead. Current behavior, untouched.
    //   * MAYOR (GT_DISPATCH_VIA_MAYOR=1): the orchd stops deciding which bead runs.
    //     It keeps one supervised MAYOR session alive per rig and wakes it with the
    //     ready frontier; the mayor decides the bead-by-bead dispatch. Wake-on-task:
    //     an idle rig (empty frontier) is never woken, so it burns ~0 tokens. A
    //     downed mayor is re-slung on the next tick its rig has ready work. The
    //     pool / host_cap arbitration stays with the orchd (the mayor asks, the
    //     polecat supervisor bounds), so this loop never sizes capacity itself.
    //
    // The mayor loop tracks no in-flight set (the mayor + pool own capacity), so it
    // registers no completion plugin and is spawned directly into `mayor_dispatch_task`.
    let auto_dispatch_tick_secs = env_usize("GT_AUTO_DISPATCH_TICK_SECS", 30) as u64;
    let dispatch_via_mayor = std::env::var("GT_DISPATCH_VIA_MAYOR").ok().as_deref() == Some("1");
    let mut mayor_dispatch_task: Option<tokio::task::JoinHandle<()>> = None;
    let auto_dispatch_handle: Option<Arc<gt_runtime::Dispatcher<
        gt_composition::auto_dispatch::FrontierSource,
        gt_composition::auto_dispatch::SchedWorker,
    >>> = if std::env::var("GT_AUTO_DISPATCH").ok().as_deref() == Some("1") {
        match std::env::var("GT_DOLT_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .and_then(|url| gt_store_dolt::DoltIssues::connect(&url).ok())
        {
            Some(store) => {
                let repo_dir = std::env::var("GT_REPO_DIR")
                    .ok()
                    .map(std::path::PathBuf::from);
                let mut source =
                    gt_composition::auto_dispatch::FrontierSource::new(Arc::new(store), repo_dir);
                // rig-hold H2 (gtcore-1f5e67): wire the rig catalog so a rig on `hold` has its
                // ready+auto beads excluded from the frontier (both DIRECT and MAYOR modes consume
                // this source). Fail-soft: no PG ⇒ holds simply never apply.
                if let Some(pg_url) =
                    std::env::var("GT_PG_URL").ok().filter(|v| !v.is_empty())
                {
                    match gt_store_pg::WorkspacePool::connect(&pg_url, &ws_slug).await {
                        Ok(pool) => {
                            source = source.with_held_rigs(Arc::new(
                                gt_composition::auto_dispatch::CatalogHeldRigs::new(
                                    gt_rig::PgRigs::new(pool.pool().clone()),
                                ),
                            ));
                            eprintln!(
                                "[gt-orch-server] rig-hold on — frontier excludes beads of rigs in dispatch_mode=hold"
                            );
                        }
                        Err(e) => eprintln!(
                            "[gt-orch-server] rig-hold OFF — held-rigs pool connect failed (holds not applied): {e}"
                        ),
                    }
                }
                if dispatch_via_mayor {
                    let (command, args, base_env, workdir) = mayor_launch;
                    let channel_root = std::env::var("GT_CHANNEL_ROOT")
                        .unwrap_or_else(|_| "/gt/.channels".to_string());
                    let mut waker = gt_composition::mayor_dispatch::TmuxMayorWaker::new(
                        tmux.clone(),
                        ws_slug.clone(),
                        gt_composition::mayor_dispatch::DEFAULT_MAYOR_PREFIX,
                        workdir,
                        command,
                        args,
                        base_env,
                        std::path::PathBuf::from(channel_root),
                    );
                    // Resolve + validate the mayor's claude account at spawn, exactly like the
                    // polecat sling (gtcore-559c50): without this the mayor inherits the static
                    // boot-template CLAUDE_CONFIG_DIR and is born in 401 once that account's creds
                    // expire. Quota gates rotation off a Limited/Blocked account; the proxy feeds
                    // its spend into per-call quota truth.
                    if let Some(kc) = &keychain {
                        waker = waker.with_keychain(kc.clone());
                    }
                    waker = waker.with_quota(quota.clone());
                    if let Some(url) = &anthropic_proxy_url {
                        waker = waker.with_anthropic_proxy(url.clone());
                    }
                    // Materialise the mayor's role skills + Knowledge from the `skills.*` catalog at
                    // spawn, the same shared path as the polecat sling (gtcore-ec24d2).
                    waker = waker.with_event_log(knowledge_log.clone());
                    // Role-scoped MCP token + .mcp.json/.gt-config for the mayor (gtcore-3f4d94), like
                    // the polecat/role-agent paths — so its MCP calls honour the mayor's /agents scopes.
                    if let Some(tm) = &mayor_agent_token {
                        waker = waker.with_agent_token(tm.clone());
                    }
                    if let Some(url) = std::env::var("GT_SELF_URL").ok().filter(|v| !v.is_empty()) {
                        waker = waker.with_server_url(url);
                    }
                    // Announce the mayor's session lifecycle on the hub (gtcore-a44568) so
                    // agent.list/console show it — before this the mayor was a tmux-only ghost.
                    waker = waker.with_session_events(handle.events_sender());
                    let dispatcher =
                        Arc::new(gt_composition::mayor_dispatch::MayorDispatcher::new(source, waker));
                    mayor_dispatch_task =
                        Some(dispatcher.spawn(Duration::from_secs(auto_dispatch_tick_secs)));
                    eprintln!(
                        "[gt-orch-server] auto-dispatch on — MAYOR mode (GT_DISPATCH_VIA_MAYOR=1), tick {auto_dispatch_tick_secs}s; per-rig mayor woken with the ready frontier — no direct polecat sling"
                    );
                    None
                } else {
                    let worker = gt_composition::auto_dispatch::SchedWorker::new(sched.clone());
                    let max = env_usize("GT_AUTO_DISPATCH_MAX", 4);
                    let dispatcher = Arc::new(gt_runtime::Dispatcher::new(source, worker, max));
                    pol_registry = pol_registry.register(
                        gt_composition::auto_dispatch::AutoDispatchCompletionPlugin::new(
                            dispatcher.clone(),
                        ),
                    );
                    eprintln!(
                        "[gt-orch-server] auto-dispatch on — DIRECT mode, tick {auto_dispatch_tick_secs}s, max_in_flight={max}"
                    );
                    Some(dispatcher)
                }
            }
            None => {
                eprintln!(
                    "[gt-orch-server] auto-dispatch OFF — GT_AUTO_DISPATCH=1 but GT_DOLT_URL unset"
                );
                None
            }
        }
    } else {
        eprintln!(
            "[gt-orch-server] auto-dispatch OFF — set GT_AUTO_DISPATCH=1 to enable"
        );
        None
    };

    // --- Trigger-driven role agents (gtcore-999795) ---
    // sheriff/witness/deacon run as AGENTS WITH CRITERION — slung single-shot ONLY when their trigger
    // fires (sheriff ← merge.failed.v1/merge.ready.v1; witness ← issues.closed.v1; deacon ← a health
    // tick), with single-flight per role so a burst can't sling a racing herd. Between triggers there
    // is no live session, so idle cost is ≈0 tokens. The launcher gives each agent its own per-session
    // workdir (under the polecat worktree root) with a least-privilege `.mcp.json`, so concurrent role
    // agents never race on a shared token. Registered on the SAME relay as the polecat sling so it
    // observes the same hub. GT_ROLE_AGENTS off ⇒ the legacy in-process loops + witness safety-net
    // stand unchanged. The returned dispatcher is shared with the deacon health-tick timer below.
    let role_dispatcher: Option<Arc<RoleAgentDispatcher>> = match (&resident_host, role_template) {
        // Resident mode (gtcore-865fb8): the SAME triggers deliver WAKES to the long-lived
        // sessions instead of single-shot slings — one plugin swap, ownership unchanged
        // (sheriff←merge.failed/ready, witness←issues.closed; deacon rides the timer below).
        (Some(host), _) => {
            pol_registry = pol_registry
                .register(gt_composition::role_resident::ResidentTriggerPlugin::new(host.clone()));
            eprintln!(
                "[gt-orch-server] role triggers → resident wakes (GT_ROLE_SESSIONS=1) — sheriff←merge.failed/ready, witness←issues.closed, deacon←health tick"
            );
            None
        }
        (None, Some(role_template)) => {
            let mut launcher = SpecRoleLauncher::new(role_template, tmux.clone())
                .with_workspace(ws_slug.clone())
                .with_session_events(handle.events_sender());
            if let Some(tm) = role_agent_token {
                launcher = launcher.with_agent_token(tm);
            }
            if let Some(kc) = &keychain {
                launcher = launcher.with_keychain(kc.clone());
            }
            match std::env::var("GT_SELF_URL").ok().filter(|v| !v.is_empty()) {
                Some(url) => launcher = launcher.with_server_url(url),
                None => eprintln!(
                    "[gt-orch-server] role agents: GT_SELF_URL unset — role sessions get no .mcp.json (gt MCP tools unavailable)"
                ),
            }
            if let Some(root) = &polecat_worktree_root {
                launcher = launcher.with_session_root(root.clone());
            }
            let dispatcher =
                Arc::new(RoleAgentDispatcher::new(ws_slug.clone(), Arc::new(launcher)));
            pol_registry = pol_registry.register(RoleAgentPlugin::new(dispatcher.clone()));
            eprintln!(
                "[gt-orch-server] role agents ON — sheriff←merge.failed/ready, witness←issues.closed, deacon←health tick (single-shot, single-flight, idle≈0 tokens)"
            );
            Some(dispatcher)
        }
        (None, None) => {
            eprintln!(
                "[gt-orch-server] role agents OFF — set GT_ROLE_AGENTS=1 (sheriff/witness/deacon stay in-process loops + witness safety-net)"
            );
            None
        }
    };

    // On-demand role spawn consumer (gtcore-b69087): agent.spawn with an infra role
    // (refinery/sheriff/witness/deacon/overseer/dog) rides the role-spawn channel from the
    // mcp-server; this loop materializes each request through the RoleAgentDispatcher — same
    // single-flight, launcher and session accounting as the trigger-driven slings. The mayor is
    // NOT spawnable here: it is the one role this orchestrator raises itself. Requires role
    // agents ON (the dispatcher is the materialization engine).
    let role_spawn_task: Option<tokio::task::JoinHandle<()>> = if let Some(host) = &resident_host {
        // Resident mode (gtcore-865fb8): the same channel + payload (mayor/polecat rejections
        // unchanged), each accepted request wakes the resident instead of slinging single-shot.
        let name = std::env::var("GT_ROLE_SPAWN_CHANNEL").unwrap_or_else(|_| "role-spawn".to_string());
        let role_spawn_root =
            std::env::var("GT_CHANNEL_ROOT").unwrap_or_else(|_| "/gt/.channels".to_string());
        match Channel::open(&role_spawn_root, &name) {
            Ok(channel) => {
                eprintln!(
                    "[gt-orch-server] on-demand role spawn on — resident wakes via file channel {role_spawn_root}/{name}"
                );
                let host = host.clone();
                Some(tokio::spawn(async move {
                    let mut tracker = RestartTracker::new(RestartConfig::default());
                    let make = || {
                        let channel = channel.clone();
                        let host = host.clone();
                        async move {
                            if let Err(e) =
                                gt_composition::role_resident::run_on_demand_resident(channel, host).await
                            {
                                eprintln!("[gt-orch-server] role-spawn resident consumer error: {e} — supervisor will restart");
                            }
                        }
                    };
                    gt_polecat::supervise_daemon("role-spawn", make, &mut tracker, u32::MAX, now_secs).await;
                }))
            }
            Err(e) => {
                eprintln!("[gt-orch-server] on-demand role spawn OFF — channel open failed: {e}");
                None
            }
        }
    } else { match &role_dispatcher {
        Some(dispatcher) => {
            let name = std::env::var("GT_ROLE_SPAWN_CHANNEL")
                .unwrap_or_else(|_| "role-spawn".to_string());
            let want_pg = std::env::var("GT_EVENTLOG_PG")
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false);
            let pg_queue = match (want_pg, std::env::var("GT_PG_URL").ok()) {
                (true, Some(pg_url)) => gt_channel::PgQueue::connect(&pg_url, &name)
                    .and_then(|q| q.ensure_schema().map(|()| q))
                    .map_err(|e| eprintln!("[gt-orch-server] role-spawn PG queue init failed: {e} — falling back to file channel"))
                    .ok(),
                _ => None,
            };
            if let Some(queue) = pg_queue {
                eprintln!("[gt-orch-server] on-demand role spawn on — Postgres queue (channel {name})");
                let dispatcher = dispatcher.clone();
                Some(tokio::spawn(async move {
                    let mut tracker = RestartTracker::new(RestartConfig::default());
                    let make = || {
                        let queue = queue.clone();
                        let dispatcher = dispatcher.clone();
                        async move {
                            if let Err(e) = gt_composition::role_agent::run_on_demand(queue, dispatcher).await {
                                eprintln!("[gt-orch-server] role-spawn PG consumer error: {e} — supervisor will restart");
                            }
                        }
                    };
                    gt_polecat::supervise_daemon("role-spawn", make, &mut tracker, u32::MAX, now_secs).await;
                }))
            } else {
                let role_spawn_root = std::env::var("GT_CHANNEL_ROOT")
                    .unwrap_or_else(|_| "/gt/.channels".to_string());
                match Channel::open(&role_spawn_root, &name) {
                    Ok(channel) => {
                        eprintln!("[gt-orch-server] on-demand role spawn on — file channel {role_spawn_root}/{name}");
                        let dispatcher = dispatcher.clone();
                        Some(tokio::spawn(async move {
                            let mut tracker = RestartTracker::new(RestartConfig::default());
                            let make = || {
                                let channel = channel.clone();
                                let dispatcher = dispatcher.clone();
                                async move {
                                    if let Err(e) = gt_composition::role_agent::run_on_demand(channel, dispatcher).await {
                                        eprintln!("[gt-orch-server] role-spawn consumer error: {e} — supervisor will restart");
                                    }
                                }
                            };
                            gt_polecat::supervise_daemon("role-spawn", make, &mut tracker, u32::MAX, now_secs).await;
                        }))
                    }
                    Err(e) => {
                        eprintln!("[gt-orch-server] on-demand role spawn OFF — channel open failed: {e}");
                        None
                    }
                }
            }
        }
        None => {
            eprintln!("[gt-orch-server] on-demand role spawn OFF — role agents disabled (GT_ROLE_AGENTS)");
            None
        }
    } };

    // Convoy completion reactor (gtcore-896a29): advance a convoy when one of its member beads
    // closes — complete the member, feed the next one onto the dispatch channel (the orchd's own
    // dispatch loop consumes it and slings) and close the convoy when all members are done. Sibling
    // of PatrolBridgePlugin on the same hub (both react to issues.closed.v1). The dispatch sink is
    // built the same way the mcp-server's convoy/agent bridge is (GT_EVENTLOG_PG ? PG queue : file
    // channel) so the handoff rides the SAME queue the launch bridge uses.
    let convoy_dispatch_sink: Option<Arc<gt_channel::DispatchSink>> = {
        let dispatch_name =
            std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
        let want_pg = std::env::var("GT_EVENTLOG_PG")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if want_pg {
            match std::env::var("GT_PG_URL").ok() {
                Some(pg_url) => gt_channel::PgQueue::connect(&pg_url, &dispatch_name)
                    .and_then(|q| q.ensure_schema().map(|()| q))
                    .map(|q| Arc::new(gt_channel::DispatchSink::Pg(q)))
                    .map_err(|e| {
                        eprintln!("[gt-orch-server] convoy completion: PG dispatch sink off — {e}")
                    })
                    .ok(),
                None => None,
            }
        } else {
            match std::env::var("GT_CHANNEL_ROOT").ok() {
                Some(root) => Channel::open(&root, &dispatch_name)
                    .map(|c| Arc::new(gt_channel::DispatchSink::File(c)))
                    .map_err(|e| {
                        eprintln!("[gt-orch-server] convoy completion: file dispatch sink off — {e}")
                    })
                    .ok(),
                None => None,
            }
        }
    };
    let mut convoy_plugin = gt_composition::convoy_reactor::ConvoyCompletionPlugin::new(
        Arc::new(EventLog::new(Some(event_root_for_convoy))),
        Some(ws_slug.clone()),
    );
    match convoy_dispatch_sink {
        Some(sink) => {
            convoy_plugin = convoy_plugin.with_dispatch_channel(sink);
            eprintln!("[gt-orch-server] convoy completion ON — issues.closed → complete-member + handoff (dispatch bridged)");
        }
        None => {
            eprintln!("[gt-orch-server] convoy completion ON — issues.closed → complete-member (no dispatch sink: next member won't auto-sling)");
        }
    }
    pol_registry = pol_registry.register(convoy_plugin);

    let pol_registry = Arc::new(pol_registry);
    let pol_relay = spawn_plugin_relay(handle.subscribe_events(), pol_registry);
    // All observers are live — kick the scheduler so hydrated beads pump Dispatched events.
    sched.kick().await;
    eprintln!(
        "[gt-orch-server] polecat supervision on — pool_size={pool_size}, host_cap={} (cpu+ram), max_restarts={max_restarts}",
        allocator.lock().expect("pool mutex").host_cap()
    );

    // Merge-board boot reconciliation phase 2 (gtcore-088db9, extending the gtcore-c15018 restart
    // recovery): now that the git-merge edge + sheriff + polecat supervisor are subscribed on
    // pol_relay, reconcile the in-flight slots boot hydration seeded silently. A `Ready` slot is
    // re-enqueued (→ Merging, so the edge lands it — the restart-lost `merge.ready.v1` never
    // re-fired). A `Merging` orphan is completed if its branch already reached origin/main with
    // delivery evidence, else failed so the sheriff / supervisor recover it (the old behaviour, now
    // gated on the not-on-main check instead of failing every orphan). Delivered slots were already
    // settled in phase 1, so they read as `Merged` here and are skipped. The origin/main probe
    // shells git, so it runs on a blocking thread, routed to each bead's rig checkout by prefix.
    {
        let reconcile_rig_paths = reconcile_rig_paths.clone();
        let reconcile_rig_fallback = reconcile_rig_fallback.clone();
        let (requeued, orphans) =
            gt_composition::merge_boot::reconcile_inflight_slots(&merge, |bead, branch| {
                let rig = reconcile_rig_paths
                    .get(gt_mcp_server::bead_prefix(&bead))
                    .cloned()
                    .unwrap_or_else(|| reconcile_rig_fallback.clone());
                async move {
                    tokio::task::spawn_blocking(move || {
                        gt_composition::git_merge::delivered_main_sha(&rig, &branch)
                    })
                    .await
                    .ok()
                    .flatten()
                }
            })
            .await;
        if requeued > 0 || orphans > 0 {
            eprintln!(
                "[gt-orch-server] merge boot reconcile (phase 2) — {requeued} ready slot(s) re-enqueued, {orphans} merging orphan(s) reconciled"
            );
        }
    }

    // Supervision + capacity timer: re-sling dead polecats (PolecatSupervisor::tick) and refresh
    // the host admission cap from live CPU + RAM, every GT_POLECAT_TICK_SECS (default 15s).
    let tick_secs = env_usize("GT_POLECAT_TICK_SECS", 15) as u64;
    let sup_timer = supervisor.clone();
    let alloc_timer = allocator.clone();
    let heartbeat_log = Arc::new(EventLog::new(Some(event_root_for_heartbeat)));
    // Shared with the refinery lifecycle emitter below (same log root, cheap Arc clone).
    let refinery_log = Arc::clone(&heartbeat_log);
    let heartbeat_ws = ws_slug.clone();
    let pol_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(tick_secs));
        tick.tick().await; // skip the immediate first fire
        loop {
            tick.tick().await;
            // Track real headroom: a smaller cap throttles new claims; running polecats finish.
            alloc_timer
                .lock()
                .expect("pool mutex")
                .set_host_cap(host_cap_from_metrics());
            // `tick` is blocking tmux I/O — keep it off the runtime workers.
            let sup = sup_timer.clone();
            let reslung = tokio::task::spawn_blocking(move || sup.tick(now_secs()))
                .await
                .unwrap_or(0);
            if reslung > 0 {
                eprintln!("[gt-orch-server] re-slung {reslung} dead polecat(s)");
            }
            // Emit MCP agent heartbeats for all still-watched polecats (hq-e5b288): after the
            // tick, the watched set only contains sessions that are alive or being re-slung.
            // Best-effort: a log failure never aborts the supervision loop.
            let ws_opt = Some(heartbeat_ws.as_str());
            let hb_ts = now_secs();
            for session in sup_timer.watched_sessions() {
                let ev = gt_agent::AgentEvent::Heartbeat { session, timestamp_secs: Some(hb_ts) };
                if let Err(e) = heartbeat_log.append(ws_opt, ev) {
                    eprintln!("[gt-orch-server] heartbeat append failed: {e}");
                }
            }
        }
    });

    // Checkpoint-push timer (gtcore-4cea57): every GT_CHECKPOINT_PUSH_SECS (default 120s) push each
    // in-flight polecat branch to origin, so a committed-but-unmerged branch is durable BEFORE
    // merge-ready. Closes the last-leg leak from the 2026-06-15 incident — an agent death after a
    // commit can no longer hide work on the node-local PVC. Idempotent + best-effort: a no-op when
    // nothing changed, a per-branch log on failure. `0` disables it. Blocking git → spawn_blocking.
    let checkpoint_secs = env_usize("GT_CHECKPOINT_PUSH_SECS", 120) as u64;
    let checkpoint_timer = if checkpoint_secs > 0 && !checkpoint_rigs.is_empty() {
        eprintln!(
            "[gt-orch-server] checkpoint-push on — every {checkpoint_secs}s over {} rig checkout(s)",
            checkpoint_rigs.len()
        );
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(checkpoint_secs));
            tick.tick().await; // skip the immediate first fire (no commits yet)
            loop {
                tick.tick().await;
                let rigs = checkpoint_rigs.clone();
                // Shelling `git push` is blocking I/O — keep it off the runtime workers.
                let _ = tokio::task::spawn_blocking(move || checkpoint_push_pass(&rigs)).await;
            }
        }))
    } else {
        eprintln!("[gt-orch-server] checkpoint-push OFF (GT_CHECKPOINT_PUSH_SECS=0 or no rig checkout)");
        None
    };

    // --- Reactor loops (hq-orchd.4) ---
    // Patrol lease-expiry ticker: a pure timer drives PatrolHandle::tick; an expired lease emits
    // patrol.lease-expired.v1 onto the hub, where the scheduler reactor arm re-enqueues the bead.
    let patrol_tick_secs = env_usize("GT_PATROL_TICK_SECS", 30) as u64;
    let lease_timeout = env_usize("GT_LEASE_TIMEOUT_SECS", 300) as u64;
    let patrol_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(patrol_tick_secs));
        tick.tick().await; // skip the immediate first fire (no leases yet)
        loop {
            tick.tick().await;
            patrol.tick(now_secs(), lease_timeout).await;
        }
    });

    // Quota predictive auto-rotation ticker: drives QuotaHandle::tick; an account whose projected
    // consumption crosses the threshold within its window emits the rotation chain on the hub.
    let quota_tick_secs = env_usize("GT_QUOTA_TICK_SECS", 60) as u64;
    let quota_threshold = env_usize("GT_QUOTA_THRESHOLD_SECS", 300) as u64;
    let quota_tick_handle = quota.clone();
    let quota_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(quota_tick_secs));
        tick.tick().await;
        loop {
            tick.tick().await;
            quota_tick_handle.tick(now_secs(), quota_threshold).await;
        }
    });
    // Spawn the auto-dispatch tick loop (if configured above).
    let auto_dispatch_task: Option<tokio::task::JoinHandle<()>> =
        auto_dispatch_handle.map(|d| d.spawn(Duration::from_secs(auto_dispatch_tick_secs)));

    // A2A delegation timeout sweep (B5, gtcore-1bda00): auto-escalate any open
    // delegation stuck past its `timeout_secs`. GT_A2A_TIMEOUT_TICK_SECS (default
    // 60) sets the cadence; the per-delegation timeout is carried on each event.
    let delegation_tick_secs = env_usize("GT_A2A_TIMEOUT_TICK_SECS", 60) as u64;
    let delegation_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(delegation_tick_secs));
        tick.tick().await; // skip the immediate first fire (no delegations yet)
        loop {
            tick.tick().await;
            delegation_ticker.tick().await;
        }
    });

    // Escalation reminder sweep (A6, gtcore-46c9dc): re-ping the operator about escalations
    // still pending past their window. GT_ESCALATION_TICK_SECS (default 300) sets the cadence;
    // the per-escalation window is GT_ESCALATION_REMINDER_SECS (read above). Only spawned when
    // the operator bell is wired (GT_PG_URL).
    let escalation_timer = escalation_ticker.map(|ticker| {
        let escalation_tick_secs = env_usize("GT_ESCALATION_TICK_SECS", 300) as u64;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(escalation_tick_secs));
            tick.tick().await; // skip the immediate first fire (no escalations yet)
            loop {
                tick.tick().await;
                ticker.tick().await;
            }
        })
    });

    // Deacon health-tick timer (gtcore-999795): the deacon trigger is time-driven, not event-driven,
    // so a timer drives it on the SAME shared dispatcher the relay plugin holds. Each fire slings a
    // single-shot deacon (single-flight absorbs a fire while one is still live), which scans flow
    // health read-only and escalates. Only spawned when role agents are on. Default 900s (15 min) —
    // a health sweep, not a hot loop — and the first fire is skipped (nothing to scan at boot).
    let deacon_timer = if let Some(host) = &resident_host {
        let host = host.clone();
        let deacon_tick_secs = env_usize("GT_DEACON_TICK_SECS", 900) as u64;
        eprintln!("[gt-orch-server] deacon health tick every {deacon_tick_secs}s (resident wake)");
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(deacon_tick_secs));
            tick.tick().await; // skip the immediate first fire (the boot survey wake covers it)
            loop {
                tick.tick().await;
                let (kind, payload) = gt_composition::role_resident::trigger_wake(
                    &gt_composition::role_agent::RoleTrigger::HealthTick,
                );
                if let Err(e) = host.wake(kind, &payload).await {
                    eprintln!("[role-resident] deacon health-tick wake failed: {e}");
                }
            }
        }))
    } else { role_dispatcher.as_ref().map(|dispatcher| {
        let dispatcher = dispatcher.clone();
        let deacon_tick_secs = env_usize("GT_DEACON_TICK_SECS", 900) as u64;
        eprintln!("[gt-orch-server] deacon health tick every {deacon_tick_secs}s");
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(deacon_tick_secs));
            tick.tick().await; // skip the immediate first fire (nothing to scan at boot)
            loop {
                tick.tick().await;
                dispatcher.on_trigger(&RoleTrigger::HealthTick);
            }
        })
    }) };

    eprintln!(
        "[gt-orch-server] reactor loops on — patrol tick {patrol_tick_secs}s (lease timeout {lease_timeout}s), quota tick {quota_tick_secs}s (threshold {quota_threshold}s)"
    );

    // The merge-ready channel coordinates (shared by the witness emitter + the refinery consumer).
    let channel_root =
        std::env::var("GT_CHANNEL_ROOT").unwrap_or_else(|_| "/gt/.channels".to_string());
    let merge_ready_channel =
        std::env::var("GT_MERGE_READY_CHANNEL").unwrap_or_else(|_| "merge-ready".to_string());

    // --- Witness completion safety-net (hq-orchd-deploy.6) ---
    // A periodic sweep that catches a polecat which committed its bead but died before its Stop hook
    // self-signaled merge-ready (OOM, kill, tmux crash). It scans the per-polecat worktrees and, for
    // any branch ahead of origin/main whose tmux session is gone / heartbeat stale and not already on
    // the merge board, emits merge-ready into the SAME channel the refinery consumes — so a finished
    // polecat's branch still lands on main. The polecat self-signal (hq-orchd-deploy.20) stays the
    // primary path; this is the net under it ("discover, don't track", gastown DiscoverCompletions).
    // Only active with per-polecat worktrees (GT_POLECAT_WORKTREE_ROOT) + an openable channel.
    let witness_task = match (
        std::env::var("GT_POLECAT_WORKTREE_ROOT")
            .ok()
            .filter(|v| !v.is_empty()),
        Channel::open(&channel_root, &merge_ready_channel),
    ) {
        (Some(wt_root), Ok(channel)) => {
            let witness_tick_secs = env_usize("GT_WITNESS_TICK_SECS", 60) as u64;
            let witness_stale_secs = env_usize("GT_WITNESS_STALE_SECS", 300) as u64;
            let heartbeat_dir = std::env::var("GT_HEARTBEAT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| std::env::temp_dir());
            let sweep = WitnessSweep::new(
                PathBuf::from(wt_root),
                rig_path.clone(),
                heartbeat_dir,
                Duration::from_secs(witness_stale_secs),
                tmux.clone(),
                merge.clone(),
                channel,
            );
            eprintln!(
                "[gt-orch-server] witness safety-net on — sweep {witness_tick_secs}s (stale {witness_stale_secs}s); emits merge-ready for completed polecats that didn't self-signal"
            );
            Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(witness_tick_secs));
                tick.tick().await; // skip the immediate first fire (no worktrees yet)
                loop {
                    tick.tick().await;
                    let n = sweep.sweep().await;
                    if n > 0 {
                        eprintln!(
                            "[gt-orch-server] witness emitted {n} missed merge-ready signal(s)"
                        );
                    }
                }
            }))
        }
        (None, _) => {
            eprintln!("[gt-orch-server] witness disabled — GT_POLECAT_WORKTREE_ROOT unset (no per-polecat worktrees to scan)");
            None
        }
        (_, Err(e)) => {
            eprintln!("[gt-orch-server] witness disabled — merge-ready channel open failed at {channel_root}/{merge_ready_channel}: {e}");
            None
        }
    };

    // --- Session reconciler (hq-orchd-deploy.23) ---
    // The backend's Sessions view folds the shared agent.* log; a polecat spawned but never closed
    // (daemon stopped/crashed before agent.session-end/killed) shows "spawned" forever. This timer
    // replays that log each tick and emits agent.killed for any still-open session whose tmux session
    // is gone AND heartbeat is stale — the daemon's event-log sink persists it, so the next backend
    // fold shows the ghost as killed. Always on (the daemon always persists).
    let reconcile_tick_secs = env_usize("GT_RECONCILE_TICK_SECS", 120) as u64;
    let reconcile_stale_secs = env_usize("GT_RECONCILE_STALE_SECS", 300) as u64;
    let reconcile_hb_dir = std::env::var("GT_HEARTBEAT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    // Scope = Heartbeat: this daemon owns polecats only — their tmux is on this container's default
    // server. Interactive mayor/dog sessions live on the mcp-server's `gt-<ws>` socket (another
    // container) and are reaped there; probing them from here always reports absent and would
    // false-kill a live mayor (hq-flow-validation-20260609.5).
    let reconciler = SessionReconciler::new(
        event_root_for_reconcile,
        ws_slug.clone(),
        reconcile_hb_dir,
        Duration::from_secs(reconcile_stale_secs),
        tmux.clone(),
        ReapScope::Heartbeat,
        ReapSink::Hub(handle.events_sender()),
    );
    eprintln!(
        "[gt-orch-server] session reconciler on — sweep {reconcile_tick_secs}s (stale {reconcile_stale_secs}s); closes orphaned polecat sessions"
    );
    let reconcile_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(reconcile_tick_secs));
        // A3 (gtcore-c0740a): sweep immediately at boot to kill orphaned sessions
        // from a prior crash. The interval's first tick fires instantly; previous
        // code skipped it, leaving ghosts for up to reconcile_tick_secs.
        loop {
            tick.tick().await;
            let n = reconciler.sweep().await;
            if n > 0 {
                eprintln!("[gt-orch-server] session reconciler closed {n} orphaned session(s)");
            }
        }
    });

    // --- Task custodian (gtcore-912043) ---
    // The task-side twin of the session reconciler: the sling flips beads open→working, but a
    // failed spawn / dead agent left them `working` forever (nothing reconciled the TASK after
    // the session died — "el dispatch se preocupa solo por los agentes, no por las tareas").
    // Each tick, a bead `working` with no active session in the agent.* registry and no merge
    // slot in flight, session-less past the grace window, is CAS re-opened: auto beads re-enter
    // the frontier, manual/epic beads become claimable instead of stranded. Requires the Dolt
    // issues store (same gate as the sling→working transition itself).
    let _custodian_timer: Option<tokio::task::JoinHandle<()>> = match &dolt_issues {
        Some(issues) => {
            let custodian_tick_secs = env_usize("GT_TASK_CUSTODIAN_TICK_SECS", 120) as u64;
            let custodian_grace_secs = env_usize("GT_TASK_CUSTODIAN_GRACE_SECS", 600) as u64;
            let custodian = gt_composition::task_custodian::TaskCustodian::new(
                event_root_for_custodian,
                ws_slug.clone(),
                issues.clone(),
                Some(merge.clone()),
                Duration::from_secs(custodian_grace_secs),
                Some(handle.events_sender()),
            );
            eprintln!(
                "[gt-orch-server] task custodian on — sweep {custodian_tick_secs}s (grace {custodian_grace_secs}s); re-opens working beads with no live session"
            );
            Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(custodian_tick_secs));
                loop {
                    tick.tick().await;
                    let recovered = custodian.sweep().await;
                    if !recovered.is_empty() {
                        eprintln!(
                            "[gt-orch-server] task custodian re-opened {} stranded bead(s): {}",
                            recovered.len(),
                            recovered.join(", ")
                        );
                    }
                }
            }))
        }
        None => {
            eprintln!("[gt-orch-server] task custodian OFF — GT_DOLT_URL unset (no issues store to reconcile)");
            None
        }
    };

    // Refinery MERGE_READY live loop: await MERGE_READY messages on a gt-channel and submit each to
    // the merge actor, under a restart+backoff supervisor (gt-core agents may instead submit via
    // the MCP merge.submit path — both feed the same event-sourced board). Absent/unopenable
    // channel ⇒ the loop is disabled and the daemon still boots.
    let refinery_ws = ws_slug.clone();
    let refinery_task = match Channel::open(&channel_root, &merge_ready_channel) {
        Ok(channel) => {
            eprintln!(
                "[gt-orch-server] refinery: MERGE_READY channel {}",
                channel.dir().display()
            );
            Some(tokio::spawn(async move {
                let session = format!("refinery-{refinery_ws}");
                let ws_opt = Some(refinery_ws.as_str());
                // Announce the refinery as a live role session (gtcore-cd9a14): emits
                // agent.spawned.v1 so it appears in agent_list/audit like any other role.
                // maintains_heartbeat=false → the session reconciler won't kill it on
                // missing heartbeat; it stays visible until session-end at shutdown.
                if let Err(e) = refinery_log.append(
                    ws_opt,
                    gt_agent::AgentEvent::Spawned {
                        session: session.clone(),
                        rig: refinery_ws.clone(),
                        role: gt_agent::SessionRole::Dog(gt_agent::DogKind::Refinery),
                        crew: None,
                        spawned_by: None,
                        skills: vec![],
                        hooks: vec![],
                        maintains_heartbeat: false,
                        tmux_socket: None,
                    },
                ) {
                    eprintln!("[gt-orch-server] refinery: agent.spawned append failed: {e}");
                }
                let mut tracker = RestartTracker::new(RestartConfig::default());
                let make = || {
                    let channel = channel.clone();
                    let merge = merge.clone();
                    async move {
                        if let Err(e) = gt_merge::refinery::run(channel, merge).await {
                            eprintln!("[gt-orch-server] refinery channel error: {e} — supervisor will restart");
                        }
                    }
                };
                gt_polecat::supervise_daemon("refinery", make, &mut tracker, u32::MAX, now_secs)
                    .await;
                // Emit session-end when the loop exits (channel closed or daemon shutdown).
                if let Err(e) = refinery_log.append(
                    ws_opt,
                    gt_agent::AgentEvent::session_end(session),
                ) {
                    eprintln!("[gt-orch-server] refinery: agent.session-end append failed: {e}");
                }
            }))
        }
        Err(e) => {
            eprintln!(
                "[gt-orch-server] refinery disabled — channel open failed at {channel_root}/{merge_ready_channel}: {e}"
            );
            None
        }
    };

    // Dispatch live loop (hq-orchd-deploy.4): await dispatch requests on a channel and seed +
    // enqueue each onto the scheduler, which auto-dispatches (emits scheduling.dispatched.v1) when
    // capacity allows — the event the polecat supervisor observes to sling an agent. This is the
    // concrete trigger for "a bead becomes dispatched": the convoy→scheduler bridge (mcp-server)
    // drops a {"bead","priority"} message in the channel. Sibling of the Refinery loop, same
    // restart+backoff supervisor. Absent/unopenable channel ⇒ disabled, the daemon still boots.
    //
    // hq-talos-migration.11 — REVERSIBLE BY ENV (same opt-in as the event log, .10): when
    // GT_EVENTLOG_PG is truthy AND GT_PG_URL is set, the loop consumes the Postgres queue
    // (`public.dispatch_jobs`, FOR UPDATE SKIP LOCKED claim — concurrency-safe across consumers) so
    // mcp-server and orchd share NO filesystem for dispatch. Otherwise it consumes the file-based
    // channel under GT_CHANNEL_ROOT, EXACTLY as before. Both feed the SAME `dispatch::run` (generic
    // over the backend); flipping GT_EVENTLOG_PG off restores the file path.
    let dispatch_channel =
        std::env::var("GT_DISPATCH_CHANNEL").unwrap_or_else(|_| "dispatch".to_string());
    let want_pg_dispatch = std::env::var("GT_EVENTLOG_PG")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    let pg_dispatch_queue = match (want_pg_dispatch, std::env::var("GT_PG_URL").ok()) {
        (true, Some(pg_url)) => {
            match gt_channel::PgQueue::connect(&pg_url, &dispatch_channel)
                .and_then(|q| q.ensure_schema().map(|()| q))
            {
                Ok(q) => Some(q),
                Err(e) => {
                    eprintln!("[gt-orch-server] dispatch PG queue init failed: {e} — falling back to file channel");
                    None
                }
            }
        }
        (true, None) => {
            eprintln!("[gt-orch-server] dispatch: GT_EVENTLOG_PG set but GT_PG_URL unset — using file channel");
            None
        }
        (false, _) => None,
    };
    let dispatch_task = if let Some(queue) = pg_dispatch_queue {
        eprintln!(
            "[gt-orch-server] dispatch: Postgres queue public.dispatch_jobs (channel {dispatch_channel}, FOR UPDATE SKIP LOCKED) — GT_EVENTLOG_PG"
        );
        let sched = sched.clone();
        Some(tokio::spawn(async move {
            let mut tracker = RestartTracker::new(RestartConfig::default());
            let make = || {
                let queue = queue.clone();
                let sched = sched.clone();
                async move {
                    if let Err(e) = gt_scheduling::dispatch::run(queue, sched).await {
                        eprintln!("[gt-orch-server] dispatch PG queue error: {e} — supervisor will restart");
                    }
                }
            };
            gt_polecat::supervise_daemon("dispatch", make, &mut tracker, u32::MAX, now_secs).await;
        }))
    } else {
        match Channel::open(&channel_root, &dispatch_channel) {
            Ok(channel) => {
                eprintln!(
                    "[gt-orch-server] dispatch: file channel {} — drop a {{\"bead\",\"priority\"}} JSON as a `<id>.event` file (atomic: write `.<id>.tmp` then rename; a bare `.json` is IGNORED — only the `.event` extension is consumed) to dispatch (set GT_EVENTLOG_PG=1 + GT_PG_URL for the Postgres queue)",
                    channel.dir().display()
                );
                let sched = sched.clone();
                Some(tokio::spawn(async move {
                    let mut tracker = RestartTracker::new(RestartConfig::default());
                    let make = || {
                        let channel = channel.clone();
                        let sched = sched.clone();
                        async move {
                            if let Err(e) = gt_scheduling::dispatch::run(channel, sched).await {
                                eprintln!("[gt-orch-server] dispatch channel error: {e} — supervisor will restart");
                            }
                        }
                    };
                    gt_polecat::supervise_daemon("dispatch", make, &mut tracker, u32::MAX, now_secs)
                        .await;
                }))
            }
            Err(e) => {
                eprintln!(
                    "[gt-orch-server] dispatch disabled — channel open failed at {channel_root}/{dispatch_channel}: {e}"
                );
                None
            }
        }
    };

    // Quota feed live loop (hq-agent-provisioning.7): await ratelimit/usage figures on a gt-channel
    // and fold them into the quota actor — the INPUT half of predictive rotation. An external edge
    // (a claude-session hook reporting `anthropic-ratelimit-*` headers + token usage, a sidecar
    // proxy, or a manual probe) drops a {"account","headers"|"sample"} JSON message; without this
    // feed the predictor stays flat (rate 0 ⇒ no BlockPredicted). Sibling of the dispatch loop,
    // same restart+backoff supervisor. Absent/unopenable channel ⇒ disabled, the daemon still boots.
    let quota_feed_channel =
        std::env::var("GT_QUOTA_FEED_CHANNEL").unwrap_or_else(|_| "quota-feed".to_string());
    let quota_feed_task = match Channel::open(&channel_root, &quota_feed_channel) {
        Ok(channel) => {
            eprintln!(
                "[gt-orch-server] quota feed: channel {} — drop {{\"account\",\"headers\"|\"sample\"}} to feed the predictor",
                channel.dir().display()
            );
            Some(tokio::spawn(async move {
                let mut tracker = RestartTracker::new(RestartConfig::default());
                let make = || {
                    let channel = channel.clone();
                    let quota = quota.clone();
                    async move {
                        if let Err(e) = quota_rotation::run(channel, quota).await {
                            eprintln!("[gt-orch-server] quota feed channel error: {e} — supervisor will restart");
                        }
                    }
                };
                gt_polecat::supervise_daemon("quota-feed", make, &mut tracker, u32::MAX, now_secs)
                    .await;
            }))
        }
        Err(e) => {
            eprintln!(
                "[gt-orch-server] quota feed disabled — channel open failed at {channel_root}/{quota_feed_channel}: {e}"
            );
            None
        }
    };

    wait_for_signal().await;

    eprintln!("[gt-orch-server] signal received — draining actor stack");
    // Stop the edge loops first: no new polecats slung/re-slung, no ticks, no MERGE_READY submits
    // during teardown. Live tmux polecats keep running — the daemon is going down, not the town.
    pol_timer.abort();
    pol_relay.abort();
    if let Some(task) = &checkpoint_timer {
        task.abort();
    }

    // --- Final drain on shutdown (gtcore-0179f8) ---
    // k8s redeploys orchd with a `Recreate` strategy: it SIGTERMs this pod (and every in-flight
    // polecat) here, then SIGKILLs after `terminationGracePeriodSeconds`. The periodic checkpoint
    // above only saved COMMITTED work; UNCOMMITTED edits in a live worktree would die with the pod.
    // Now that the slings + periodic push are stopped (the lines above), this is the sole writer to
    // the worktree branches: force a final checkpoint — commit pending changes in every active
    // worktree and push the branch to origin — so nothing committable is lost. Bounded by
    // GT_DRAIN_TIMEOUT_SECS (kept under the grace period) so it can never be SIGKILLed mid-push and
    // never wedges a clean shutdown. GT_DRAIN_ON_TERM=0 disables it. With no dirty worktrees the
    // drain is a couple of fast `git` calls, so a clean shutdown stays fast (the happy-path AC).
    let drain_on_term = std::env::var("GT_DRAIN_ON_TERM")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    if drain_on_term && !drain_rigs.is_empty() {
        let drain_secs = env_usize("GT_DRAIN_TIMEOUT_SECS", 90) as u64;
        eprintln!(
            "[gt-orch-server] final drain — commit+push pending work across {} rig checkout(s), ≤{drain_secs}s",
            drain_rigs.len()
        );
        let rigs = drain_rigs.clone();
        // Shelling `git add`/`commit`/`push` is blocking I/O — keep it off the runtime workers.
        let drain = tokio::task::spawn_blocking(move || drain_pass(&rigs));
        match tokio::time::timeout(Duration::from_secs(drain_secs), drain).await {
            Ok(Ok(())) => eprintln!("[gt-orch-server] final drain complete"),
            Ok(Err(e)) => eprintln!("[gt-orch-server] final drain task panicked: {e}"),
            Err(_) => eprintln!(
                "[gt-orch-server] final drain timed out after {drain_secs}s — proceeding to shutdown"
            ),
        }
    } else if drain_rigs.is_empty() {
        eprintln!("[gt-orch-server] final drain skipped — no rig checkout to sweep");
    } else {
        eprintln!("[gt-orch-server] final drain OFF (GT_DRAIN_ON_TERM=0)");
    }

    patrol_timer.abort();
    quota_timer.abort();
    if let Some(task) = &deacon_timer {
        task.abort();
    }
    delegation_timer.abort();
    if let Some(task) = &escalation_timer {
        task.abort();
    }
    reconcile_timer.abort();
    if let Some(task) = &witness_task {
        task.abort();
    }
    if let Some(task) = &refinery_task {
        task.abort();
    }
    if let Some(task) = &dispatch_task {
        task.abort();
    }
    if let Some(task) = &quota_feed_task {
        task.abort();
    }
    if let Some(task) = &auto_dispatch_task {
        task.abort();
    }
    if let Some(task) = &mayor_dispatch_task {
        task.abort();
    }
    let _ = pol_timer.await;
    let _ = pol_relay.await;
    if let Some(task) = checkpoint_timer {
        let _ = task.await;
    }
    let _ = patrol_timer.await;
    let _ = quota_timer.await;
    if let Some(task) = deacon_timer {
        let _ = task.await;
    }
    let _ = delegation_timer.await;
    if let Some(task) = escalation_timer {
        let _ = task.await;
    }
    let _ = reconcile_timer.await;
    if let Some(task) = witness_task {
        let _ = task.await;
    }
    if let Some(task) = refinery_task {
        let _ = task.await;
    }
    if let Some(task) = dispatch_task {
        let _ = task.await;
    }
    if let Some(task) = quota_feed_task {
        let _ = task.await;
    }
    if let Some(task) = auto_dispatch_task {
        let _ = task.await;
    }
    if let Some(task) = mayor_dispatch_task {
        let _ = task.await;
    }
    // Cancel the actor stack + stop the observer relay and the per-domain drains. The
    // durable log already holds every record appended up to this point.
    handle.shutdown().await;
    eprintln!("[gt-orch-server] shutdown complete");
    Ok(())
}

/// Serve the Prometheus text exposition of this process's registry on `GET /metrics`
/// (`hq-orchd.6`), and — when `ci_gate` is `Some` — the CI-gate receiver
/// (`POST /ci-gate/{merged,failed}`, gtcore-52c9ec) on the same listener. Bound to `GT_METRICS_BIND`
/// (default `127.0.0.1:9099`). The CI-gate routes are how the MCP-server webhook drives a merge slot
/// to its terminal state without orchd polling the PR.
async fn serve_metrics(bind: &str, ci_gate: Option<CiGateState>) -> anyhow::Result<()> {
    use axum::routing::get;
    use axum::Router;
    let mut app = Router::new().route("/metrics", get(metrics_text));
    if let Some(state) = ci_gate {
        app = app.merge(gt_composition::ci_gate::ci_gate_router(state));
    }
    let listener = tokio::net::TcpListener::bind(bind).await?;
    eprintln!(
        "[gt-orch-server] metrics on http://{}/metrics",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Render the process-global metric registry, or a 500 on encode failure (mirrors gt-mcp-server).
async fn metrics_text() -> axum::response::Response {
    use axum::response::IntoResponse;
    match gt_telemetry::metrics::render_text() {
        Ok(body) => (axum::http::StatusCode::OK, body).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Wait for SIGTERM or SIGINT. If signal install fails (non-Unix), the future never
/// resolves and the process keeps running until killed externally — better than
/// auto-exiting at startup.
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(mut term), Ok(mut int)) => {
            tokio::select! {
                _ = term.recv() => eprintln!("[gt-orch-server] SIGTERM received"),
                _ = int.recv() => eprintln!("[gt-orch-server] SIGINT received"),
            }
        }
        (Err(e), _) | (_, Err(e)) => {
            eprintln!(
                "[gt-orch-server] signal install failed: {e}; running until killed externally"
            );
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_translates_container_path_to_host_by_basename() {
        let host = tempfile::tempdir().unwrap();
        let id = "01ABCDEF";
        std::fs::create_dir(host.path().join(id)).unwrap();

        // A container-absolute path that does not exist on the host resolves to the host dir with
        // the same basename.
        let stored = format!("/var/lib/gt-core/accounts/{id}");
        assert_eq!(
            resolve_host_account_dir(&stored, host.path()),
            host.path().join(id).display().to_string()
        );

        // A path that already exists (host-native env bootstrap) passes through unchanged.
        let native = host.path().join(id).display().to_string();
        assert_eq!(resolve_host_account_dir(&native, host.path()), native);

        // Untranslatable (no matching basename under the host root) is left as-is.
        let unknown = "/var/lib/gt-core/accounts/NOPE".to_string();
        assert_eq!(resolve_host_account_dir(&unknown, host.path()), unknown);
    }
}
