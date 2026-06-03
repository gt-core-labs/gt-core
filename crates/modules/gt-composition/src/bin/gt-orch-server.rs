//! `gt-orch-server` — the gt-core autonomous orchestration daemon (`hq-orchd.1`).
//!
//! The long-lived daemon entrypoint replacing gastown `bins/gt`. It boots the single
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
//! - `GT_CHANNEL_ROOT` (`/gt/.channels`) / `GT_MERGE_READY_CHANNEL` (`merge-ready`) — the
//!   Refinery MERGE_READY gt-channel; absent/unopenable ⇒ the loop is disabled, the daemon boots.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gt_channel::Channel;
use gt_composition::polecat::{host_cap_from_metrics, PolecatSupervisorPlugin};
use gt_composition::{daemon_root, DaemonRoot};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_plugin::{spawn_plugin_relay, PluginRegistry};
use gt_polecat::{
    PoolAllocator, PolecatSupervisor, RestartConfig, RestartTracker, SpawnTemplate, Tmux, TmuxCli,
};
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register the process-global Prometheus counters before any actor can emit, so the
    // golden event/dead-letter metrics record from boot onward (mirrors gt-mcp-server).
    gt_telemetry::metrics::ensure_registered();

    // The daemon always persists — durability is its whole point — so an unset
    // GT_EVENTLOG_ROOT falls back to the production volume, never to the in-memory
    // (`None`) mode `live_root` uses for tests.
    let event_root =
        PathBuf::from(std::env::var("GT_EVENTLOG_ROOT").unwrap_or_else(|_| DEFAULT_EVENTLOG_ROOT.into()));
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
    let DaemonRoot { handle, merge, patrol, quota } = daemon_root(ws, event_root).await;
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
    let allocator = Arc::new(Mutex::new(PoolAllocator::new(host_cap_from_metrics(), pool_size)));
    let tmux: Arc<dyn Tmux> = Arc::new(TmuxCli::new());
    let supervisor = Arc::new(PolecatSupervisor::new(
        tmux.clone(),
        RestartConfig::default(),
        max_restarts,
    ));
    let template = SpawnTemplate::from_env(&ws_slug);

    // Observe the SAME hub the root drains actor output onto: a fresh broadcast receiver, so the
    // sling observer runs independently of the root's own plugin relay (durability/roles/reactor).
    let pol_registry = Arc::new(PluginRegistry::new().register(PolecatSupervisorPlugin::new(
        ws_slug.clone(),
        tmux.clone(),
        template,
        supervisor.clone(),
        allocator.clone(),
    )));
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
    let pol_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(tick_secs));
        tick.tick().await; // skip the immediate first fire
        loop {
            tick.tick().await;
            // Track real headroom: a smaller cap throttles new claims; running polecats finish.
            alloc_timer.lock().expect("pool mutex").set_host_cap(host_cap_from_metrics());
            // `tick` is blocking tmux I/O — keep it off the runtime workers.
            let sup = sup_timer.clone();
            let reslung = tokio::task::spawn_blocking(move || sup.tick(now_secs()))
                .await
                .unwrap_or(0);
            if reslung > 0 {
                eprintln!("[gt-orch-server] re-slung {reslung} dead polecat(s)");
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
    let quota_timer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(quota_tick_secs));
        tick.tick().await;
        loop {
            tick.tick().await;
            quota.tick(now_secs(), quota_threshold).await;
        }
    });
    eprintln!(
        "[gt-orch-server] reactor loops on — patrol tick {patrol_tick_secs}s (lease timeout {lease_timeout}s), quota tick {quota_tick_secs}s (threshold {quota_threshold}s)"
    );

    // Refinery MERGE_READY live loop: await MERGE_READY messages on a gt-channel and submit each to
    // the merge actor, under a restart+backoff supervisor (gt-core agents may instead submit via
    // the MCP merge.submit path — both feed the same event-sourced board). Absent/unopenable
    // channel ⇒ the loop is disabled and the daemon still boots.
    let channel_root =
        std::env::var("GT_CHANNEL_ROOT").unwrap_or_else(|_| "/gt/.channels".to_string());
    let merge_ready_channel =
        std::env::var("GT_MERGE_READY_CHANNEL").unwrap_or_else(|_| "merge-ready".to_string());
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

    wait_for_signal().await;

    eprintln!("[gt-orch-server] signal received — draining actor stack");
    // Stop the edge loops first: no new polecats slung/re-slung, no ticks, no MERGE_READY submits
    // during teardown. Live tmux polecats keep running — the daemon is going down, not the town.
    pol_timer.abort();
    pol_relay.abort();
    patrol_timer.abort();
    quota_timer.abort();
    if let Some(task) = &refinery_task {
        task.abort();
    }
    let _ = pol_timer.await;
    let _ = pol_relay.await;
    let _ = patrol_timer.await;
    let _ = quota_timer.await;
    if let Some(task) = refinery_task {
        let _ = task.await;
    }
    // Cancel the actor stack + stop the observer relay and the per-domain drains. The
    // durable log already holds every record appended up to this point.
    handle.shutdown().await;
    eprintln!("[gt-orch-server] shutdown complete");
    Ok(())
}

/// Wait for SIGTERM or SIGINT. If signal install fails (non-Unix), the future never
/// resolves and the process keeps running until killed externally — better than
/// auto-exiting at startup.
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) {
        (Ok(mut term), Ok(mut int)) => {
            tokio::select! {
                _ = term.recv() => eprintln!("[gt-orch-server] SIGTERM received"),
                _ = int.recv() => eprintln!("[gt-orch-server] SIGINT received"),
            }
        }
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("[gt-orch-server] signal install failed: {e}; running until killed externally");
            std::future::pending::<()>().await;
        }
    }
}
