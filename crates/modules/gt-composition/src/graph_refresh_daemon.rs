//! Scheduler graph-refresh: the tick that actually REBUILDS a rig's graph once it is marked
//! stale (hq-graphrig-autotick).
//!
//! [`crate::drift_reconcile`] and the push webhook (`crate::webhook`) both only flip the warden's
//! `stale` flag — neither clones nor indexes. Before this daemon existed, nothing did: a rig sat
//! `stale=true` (or, for a freshly-registered rig, `stale=true` with no `last_indexed_commit` at
//! all) until a human or agent happened to call `graph.refresh` / `graph.refresh-stale` by hand.
//! In practice that meant a newly-adopted rig (e.g. `authapp` in the `templates` workspace,
//! 2026-07-03) could sit fully un-indexed for days with no error, no alert, and no visible symptom
//! beyond "the graph isn't building" — because nothing was ever wired to build it.
//!
//! This module closes that loop: on each interval, for every workspace partition, it calls
//! [`gt_composition::mcp::GraphHandler::refresh_stale`] — the exact same batch-refresh the
//! `graph.refresh-stale` MCP tool runs, so there is one code path for "rebuild everything stale",
//! reachable either on demand or on a timer.
//!
//! ## Opt-in / configurable
//!
//! Wired in `gt-mcp-server` only when `GT_GRAPH_REFRESH_TICK_SECS > 0` (default 900 = 15min — more
//! frequent than the hourly drift backstop, since this is the step that removes user-visible
//! staleness, not just detects it). Mirrors [`crate::drift_reconcile`]'s opt-in shape exactly.

use std::sync::Arc;
use std::time::Duration;

use crate::mcp::{EventLog, GraphHandler};

/// One refresh pass over every workspace partition: for each, call
/// [`GraphHandler::refresh_stale`] and log a one-line summary. A per-workspace fault is caught by
/// `refresh_stale` itself (best-effort per rig), so this loop never needs to catch anything —
/// there is nothing left that can abort the sweep.
pub async fn refresh_pass(log: &EventLog, handler: &GraphHandler) {
    for ws in log.workspaces() {
        let workspace = Some(ws.as_str());
        let refreshed = match handler.refresh_stale(workspace).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[graph-refresh] ws={ws} refresh_stale failed: {e}");
                continue;
            }
        };
        if refreshed.is_empty() {
            continue;
        }
        let ok = refreshed.iter().filter(|r| r["ok"] == true).count();
        let failed = refreshed.len() - ok;
        if failed > 0 {
            eprintln!(
                "[graph-refresh] ws={ws} refreshed {ok}/{} stale rig(s), {failed} failed: {}",
                refreshed.len(),
                refreshed
                    .iter()
                    .filter(|r| r["ok"] != true)
                    .filter_map(|r| r["rig"].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else {
            eprintln!("[graph-refresh] ws={ws} refreshed {ok} stale rig(s)");
        }
    }
}

/// The daemon loop: a `tokio::time::interval` of `tick` that runs [`refresh_pass`] each cycle.
/// Awaitable (never returns) so it composes with `tokio::spawn` exactly like
/// [`crate::drift_reconcile::run`].
pub async fn run(tick: Duration, log: Arc<EventLog>, handler: Arc<GraphHandler>) {
    let mut interval = tokio::time::interval(tick);
    loop {
        interval.tick().await;
        refresh_pass(&log, &handler).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_graphindex::InMemoryGraphIndexer;
    use gt_graphwarden::WardenEvent;
    use gt_mcp_server::{DomainCtx, DomainHandler};
    use serde_json::json;
    use tempfile::TempDir;

    fn ctx(workspace: Option<&'static str>) -> DomainCtx<'static> {
        DomainCtx {
            workspace,
            actor: "graph-refresh-daemon-test",
            args: json!({}),
        }
    }

    /// A rig registered (and therefore stale, per `RegisterRig`'s initial state) with no
    /// `graph.refresh` ever called gets indexed by the very first pass — the exact `authapp` gap
    /// this daemon closes.
    #[tokio::test]
    async fn never_refreshed_rig_gets_built_on_the_first_pass() {
        let graph_root = TempDir::new().unwrap();
        std::env::set_var("GT_GRAPH_ROOT", graph_root.path());
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let handler = GraphHandler::new(log.clone(), Arc::new(InMemoryGraphIndexer::new()));

        // `acme` is the workspace partition `log.workspaces()` must discover from the directory
        // layout — appending under it is what creates the partition on a file-backed log.
        let ws = Some("acme");
        // Register-only, mirroring `ensure_custody`'s bare RegisterRig — no refresh ever ran.
        log.append(
            ws,
            WardenEvent::RigRegistered {
                rig: "authapp".into(),
                repo_dir: graph_root
                    .path()
                    .join("acme/authapp")
                    .display()
                    .to_string(),
                now_secs: 1,
            },
        )
        .unwrap();

        refresh_pass(&log, &handler).await;

        let list = handler
            .dispatch("graph.list", ctx(ws))
            .await
            .unwrap();
        assert_eq!(list["rigs"][0]["rig"], "authapp");
        assert_eq!(
            list["rigs"][0]["stale"], false,
            "the pass must build + un-stale it"
        );
        assert!(list["rigs"][0]["last_indexed_commit"].is_string());
    }

    /// No workspace partitions exist yet — the sweep must not panic over an empty catalog.
    #[tokio::test]
    async fn nothing_stale_is_a_silent_noop() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let handler = GraphHandler::new(log.clone(), Arc::new(InMemoryGraphIndexer::new()));
        refresh_pass(&log, &handler).await;
    }
}
