//! Operational `/health` and `/readyz` endpoints (hq-mt-routing.8).
//!
//! ## Reframe note
//!
//! The bead targeted the upstream `gt-web/src/health.rs`; that bin is being retired
//! and the live HTTP surface is this `gt-mcp-server`, which already mounts a
//! `/health` route. So the endpoints land here, on the multi-tenant resolver
//! introduced by hq-mt-routing.5.
//!
//! ## Scope
//!
//! - `/health` — liveness: `{ status, workspaces_loaded, uptime_s }`. Always
//!   `200`; cheap (no I/O), safe for a container liveness probe.
//! - `/readyz` — readiness: probes each *loaded* workspace's Dolt pool
//!   (`SELECT 1`) and reports `{ ready, workspaces: [{ id, dolt_ok }] }`, with a
//!   `503` when any probe fails so an orchestrator can gate traffic.
//!
//! The bead also sketches per-workspace `status`, `last_seen`, and `pg_ok`.
//! Those need substrate gt-core does not have yet — a workspace *catalog*
//! (loaded pools are lazy, so the set here is "seen so far", not "all tenants")
//! and per-workspace activity / Postgres tracking. They are deferred to the
//! beads that introduce that substrate (catalog: hq-mt routing/data; activity:
//! the autonomous-loop telemetry); reporting them now would be invented data.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::json;

use crate::workspace::WorkspaceStores;

/// Shared state for the health routes: the (optional) workspace resolver and the
/// process start instant for uptime. Cheap to clone (an `Arc` + a `Copy`).
#[derive(Clone)]
pub struct HealthState {
    /// `Some` when multi-tenant routing is wired (hq-mt-routing.5); `None` in
    /// single-tenant mode, where `workspaces_loaded` is reported as 0.
    pub workspaces: Option<Arc<WorkspaceStores>>,
    /// Process start, for the `uptime_s` gauge.
    pub started: Instant,
}

impl HealthState {
    /// Build the state from the composed server's resolver, stamping start now.
    pub fn new(workspaces: Option<Arc<WorkspaceStores>>) -> Self {
        Self { workspaces, started: Instant::now() }
    }
}

/// `GET /health` — liveness. Never fails: a reachable process is alive even with
/// no workspaces loaded yet.
pub async fn health(State(state): State<HealthState>) -> Json<serde_json::Value> {
    let workspaces_loaded = state.workspaces.as_ref().map_or(0, |w| w.live_pools());
    Json(json!({
        "status": "ok",
        "workspaces_loaded": workspaces_loaded,
        "uptime_s": state.started.elapsed().as_secs(),
    }))
}

/// `GET /readyz` — readiness. In single-tenant mode (no resolver) the process is
/// ready as soon as it is up. In multi-tenant mode it probes each *loaded*
/// workspace's Dolt pool and is ready only if every probe passes; otherwise `503`.
pub async fn readyz(State(state): State<HealthState>) -> (StatusCode, Json<serde_json::Value>) {
    let Some(stores) = state.workspaces.as_ref() else {
        return (
            StatusCode::OK,
            Json(json!({ "ready": true, "mode": "single-tenant", "workspaces": [] })),
        );
    };

    let mut entries = Vec::new();
    let mut all_ok = true;
    for id in stores.loaded_workspaces() {
        let dolt_ok = stores.health(&id).await.is_ok();
        all_ok &= dolt_ok;
        entries.push(json!({ "id": id, "dolt_ok": dolt_ok }));
    }

    let code = if all_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(json!({ "ready": all_ok, "workspaces": entries })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_reports_zero_loaded_in_single_tenant() {
        let Json(v) = health(State(HealthState::new(None))).await;
        assert_eq!(v["status"], "ok");
        assert_eq!(v["workspaces_loaded"], 0);
        assert!(v["uptime_s"].is_u64());
    }

    #[tokio::test]
    async fn health_counts_loaded_pools_in_multi_tenant() {
        let stores = WorkspaceStores::from_base_url("mysql://root@127.0.0.1:3307/").unwrap();
        // Priming a pool creates a (lazy, un-connected) pool — counted as loaded.
        // Use `pool_for` directly (not `store_for`, which now ensures the schema on
        // first access and would touch the dead 3307 server in this offline test).
        let _ = stores.pools().pool_for("acme").unwrap();
        let _ = stores.pools().pool_for("beta").unwrap();
        let state = HealthState::new(Some(Arc::new(stores)));
        let Json(v) = health(State(state)).await;
        assert_eq!(v["workspaces_loaded"], 2);
    }

    #[tokio::test]
    async fn readyz_is_ready_in_single_tenant() {
        let (code, Json(v)) = readyz(State(HealthState::new(None))).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["ready"], true);
        assert_eq!(v["mode"], "single-tenant");
    }

    #[tokio::test]
    async fn readyz_with_no_loaded_workspaces_is_ready() {
        // Multi-tenant but nothing loaded yet → vacuously ready, empty list.
        let stores = WorkspaceStores::from_base_url("mysql://root@127.0.0.1:3307/").unwrap();
        let (code, Json(v)) = readyz(State(HealthState::new(Some(Arc::new(stores))))).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["ready"], true);
        assert_eq!(v["workspaces"].as_array().unwrap().len(), 0);
    }
}
