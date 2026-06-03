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
//! The polecat supervisor (`hq-orchd.3`), the reactor loops (`.4`), and the pipelines
//! (`.6`) wire in here as they land.
//!
//! Env:
//! - `GT_EVENTLOG_ROOT` — durable per-workspace event-log volume (default
//!   [`gt_eventlog::DEFAULT_EVENTLOG_ROOT`], `/var/lib/gt-core`).
//! - `GT_WORKSPACE` — the workspace the daemon boots (default `default`).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use gt_composition::live_root;
use gt_eventlog::DEFAULT_EVENTLOG_ROOT;
use gt_runtime::RootRegistry;
use gt_workspace::WorkspaceId;

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

    wait_for_signal().await;

    eprintln!("[gt-orch-server] signal received — draining actor stack");
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
