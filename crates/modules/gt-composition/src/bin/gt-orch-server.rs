//! `gt-orch-server` — the gt-core autonomous orchestration daemon (`hq-orchd.1`).
//!
//! The long-lived daemon entrypoint replacing the upstream `bins/gt`. It boots the single
//! Tokio runtime (the domain crates never create one — `tokio::spawn` is forbidden in
//! the kernel; the bin owns the runtime, docs/03), resolves the **durable hydrated**
//! [`live_root`] for the configured workspace, and stays alive running the reactor
//! loops until SIGTERM/SIGINT, when it drains the actor stack and exits cleanly.
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
use gt_composition::git_merge::GitMergePlugin;
use gt_composition::patrol_bridge::PatrolBridgePlugin;
use gt_composition::mcp::eventlog::EventLog;
use gt_composition::polecat::{
    host_cap_from_metrics, rig_routing_from_catalog, AgentTokenMinter, PolecatSupervisorPlugin,
    RigConfig, ScopeResolver,
};
use gt_composition::quota_rotation::{self, QuotaRotationPlugin};
use gt_composition::session_reconcile::{ReapScope, ReapSink, SessionReconciler};
use gt_composition::witness_sweep::WitnessSweep;
use gt_composition::workflow_notify::WorkflowNotifyPlugin;
use gt_composition::{daemon_root, replay_quota_state, DaemonRoot};
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

    // Prometheus scrape endpoint (hq-orchd.6): expose THIS process's registry — including
    // gt_workspace_session_minutes, bumped by the session-minutes projector — so the per-tenant
    // cost dashboard scrapes the daemon (a separate process from gt-mcp-server's /metrics).
    // Detached + best-effort: a bind failure logs but never aborts the orchestrator.
    let metrics_bind = std::env::var("GT_METRICS_BIND").unwrap_or_else(|_| "127.0.0.1:9099".into());
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(&metrics_bind).await {
            eprintln!("[gt-orch-server] metrics http server stopped: {e}");
        }
    });

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
    // Keep a copy for the polecat Knowledge prompt reader (hq-polecat-knowledge.1).
    let event_root_for_polecat = event_root.clone();
    // Keep a copy for the polecat heartbeat emitter (hq-e5b288): appends AgentEvent::Heartbeat
    // for each watched session after every supervisor tick so the MCP audit trail reflects liveness.
    let event_root_for_heartbeat = event_root.clone();
    let DaemonRoot {
        handle,
        sched,
        merge,
        patrol,
        quota,
    } = daemon_root(ws, event_root).await;
    eprintln!(
        "[gt-orch-server] daemon root up — scheduler + merge + patrol + quota actors anchored; persistence + roles + reactor arms + sheriff observer running"
    );
    eprintln!(
        "[gt-orch-server] durable: hub records persisted to the per-workspace log; restart rehydrates pending queue + merge board"
    );

    // --- Autonomous polecat supervision (hq-orchd.3) ---
    // The shared admission core: per-workspace pool size from env, host cap seeded from live
    // metrics. The sling observer claims here before spawning; the timer refreshes the host cap.
    let pool_size = env_usize("GT_POOL_SIZE", 4);
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
                        Ok(rigs) => {
                            eprintln!(
                                "[gt-orch-server] rig catalog loaded — {} rig(s) registered",
                                rigs.len()
                            );
                            let home = PathBuf::from(
                                std::env::var("HOME").unwrap_or_else(|_| "/root".into()),
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
        supervisor.set_respec(Box::new(move |mut spec| {
            fn set_env(env: &mut Vec<(String, String)>, key: &str, value: String) {
                match env.iter_mut().find(|(k, _)| k == key) {
                    Some((_, v)) => *v = value,
                    None => env.push((key.to_string(), value)),
                }
            }
            let resolved = kc
                .active()
                .ok()
                .flatten()
                .and_then(|a| kc.get(&a).ok().flatten().map(|c| (a, c.secret)));
            if let Some((account, config_dir)) = resolved {
                set_env(&mut spec.env, "CLAUDE_CONFIG_DIR", config_dir);
                set_env(
                    &mut spec.env,
                    gt_polecat::GT_HOOK_ACCOUNT,
                    account.clone(),
                );
                if proxy.is_some() {
                    set_env(
                        &mut spec.env,
                        "ANTHROPIC_CUSTOM_HEADERS",
                        format!("x-gt-account: {account}\nx-gt-session: {}", spec.session),
                    );
                }
            }
            spec
        }));
        eprintln!("[gt-orch-server] re-sling account re-resolution armed (keychain-backed)");
    }

    // Web onboarding (hq-quota-onboard-web) moved to the backend mcp-server in .4: claude now lives
    // IN the image, so onboarding rides the existing /api/v1/* auth chain instead of a host process
    // behind a docker→host firewall hole. The daemon no longer serves it — it only hydrates its
    // rotation keychain from the accounts the backend registers into the shared quota log.

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
    eprintln!("[gt-orch-server] Knowledge role prompt on — polecat CLAUDE.md from skills.* log");
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
    pol_registry = pol_registry.register(GitMergePlugin::with_rig_paths(
        merge.clone(),
        rig_paths,
        rig_path.clone(),
    ));
    eprintln!(
        "[gt-orch-server] git-merge edge on — branches land on main from rig checkout {} (+ per-rig routing)",
        rig_path.display()
    );
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
                    "[gt-orch-server] workflow notifications on — dispatch/merged/failed reach the operator bell"
                );
            }
            Err(e) => eprintln!("[gt-orch-server] workflow notifications OFF (pg pool: {e})"),
        },
        None => {
            eprintln!("[gt-orch-server] workflow notifications OFF — GT_PG_URL unset")
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
    let auto_dispatch_tick_secs = env_usize("GT_AUTO_DISPATCH_TICK_SECS", 30) as u64;
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
                let source =
                    gt_composition::auto_dispatch::FrontierSource::new(Arc::new(store), repo_dir);
                let worker = gt_composition::auto_dispatch::SchedWorker::new(sched.clone());
                let max = env_usize("GT_AUTO_DISPATCH_MAX", 4);
                let dispatcher = Arc::new(gt_runtime::Dispatcher::new(source, worker, max));
                pol_registry = pol_registry.register(
                    gt_composition::auto_dispatch::AutoDispatchCompletionPlugin::new(
                        dispatcher.clone(),
                    ),
                );
                eprintln!(
                    "[gt-orch-server] auto-dispatch on — tick {auto_dispatch_tick_secs}s, max_in_flight={max}"
                );
                Some(dispatcher)
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

    let pol_registry = Arc::new(pol_registry);
    let pol_relay = spawn_plugin_relay(handle.subscribe_events(), pol_registry);
    eprintln!(
        "[gt-orch-server] polecat supervision on — pool_size={pool_size}, host_cap={} (cpu+ram), max_restarts={max_restarts}",
        allocator.lock().expect("pool mutex").host_cap()
    );

    // Supervision + capacity timer: re-sling dead polecats (PolecatSupervisor::tick) and refresh
    // the host admission cap from live CPU + RAM, every GT_POLECAT_TICK_SECS (default 15s).
    let tick_secs = env_usize("GT_POLECAT_TICK_SECS", 15) as u64;
    let sup_timer = supervisor.clone();
    let alloc_timer = allocator.clone();
    let heartbeat_log = Arc::new(EventLog::new(Some(event_root_for_heartbeat)));
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

    // Refinery MERGE_READY live loop: await MERGE_READY messages on a gt-channel and submit each to
    // the merge actor, under a restart+backoff supervisor (gt-core agents may instead submit via
    // the MCP merge.submit path — both feed the same event-sourced board). Absent/unopenable
    // channel ⇒ the loop is disabled and the daemon still boots.
    let refinery_task = match Channel::open(&channel_root, &merge_ready_channel) {
        Ok(channel) => {
            eprintln!(
                "[gt-orch-server] refinery: MERGE_READY channel {}",
                channel.dir().display()
            );
            Some(tokio::spawn(async move {
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
                    "[gt-orch-server] dispatch: file channel {} — drop {{\"bead\",\"priority\"}} to dispatch (set GT_EVENTLOG_PG=1 + GT_PG_URL for the Postgres queue)",
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
    patrol_timer.abort();
    quota_timer.abort();
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
    let _ = pol_timer.await;
    let _ = pol_relay.await;
    let _ = patrol_timer.await;
    let _ = quota_timer.await;
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
    // Cancel the actor stack + stop the observer relay and the per-domain drains. The
    // durable log already holds every record appended up to this point.
    handle.shutdown().await;
    eprintln!("[gt-orch-server] shutdown complete");
    Ok(())
}

/// Serve the Prometheus text exposition of this process's registry on `GET /metrics`
/// (`hq-orchd.6`). Bound to `GT_METRICS_BIND` (default `127.0.0.1:9099`).
async fn serve_metrics(bind: &str) -> anyhow::Result<()> {
    use axum::routing::get;
    use axum::Router;
    let app = Router::new().route("/metrics", get(metrics_text));
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
