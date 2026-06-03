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

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gt_composition::live_root;
use gt_composition::polecat::{host_cap_from_metrics, PolecatSupervisorPlugin};
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_plugin::{spawn_plugin_relay, PluginRegistry};
use gt_polecat::{PoolAllocator, PolecatSupervisor, RestartConfig, SpawnTemplate, Tmux, TmuxCli};
use gt_runtime::RootRegistry;
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

    // One registry owns the per-workspace root for the process lifetime. `live_root`
    // hydrates from the durable log, anchors the scheduler + merge actors to the
    // supervisor, drains their events onto the hub, and registers the persistence sink
    // + role observers + reactor arms. The daemon does not inspect the cascade, so the
    // probe sink is parked.
    let reg = RootRegistry::new();
    let probe = Arc::new(Mutex::new(Vec::new()));
    let handle = live_root(&reg, ws, Some(event_root), probe).await;
    eprintln!(
        "[gt-orch-server] live_root up — scheduler + merge actors anchored, role observers + reactor arms running"
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

    wait_for_signal().await;

    eprintln!("[gt-orch-server] signal received — draining actor stack");
    // Stop the polecat timer + the sling observer first: no new polecats are slung or re-slung
    // during teardown. Live tmux polecats keep running — the daemon is going down, not the town.
    pol_timer.abort();
    pol_relay.abort();
    let _ = pol_timer.await;
    let _ = pol_relay.await;
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
