//! Multi-tenant gate (`hq-mcp-test.5`): X-Workspace routing + status gate + the
//! spoof-proof channel.
//!
//! The server resolves a request's tenant from the connection — never from the
//! tool payload — then routes to that tenant's store and consults a status gate
//! before any mutation. This integration suite pins the **public** seams that
//! behaviour is built on (the cross-tenant JWT leak reject and the in-server
//! suspend enforcement are unit-tested inline in `server.rs` against the private
//! `authorize`/`enforce_workspace_active`; here we gate the contract surface a
//! consumer can construct):
//!
//! - `WorkspaceStores` routes each slug to its own lazily-created pool — distinct
//!   tenants get distinct pools, a malformed slug is rejected before a database is
//!   ever selected, and construction opens no socket.
//! - `workspace_from_ext` is the *only* channel for the tenant: the `x-workspace`
//!   header. There is no tool-argument path, so a payload cannot spoof a tenant.
//! - `GateStatus` / `WorkspaceStatusGate` — the port the server consults — admits
//!   mutations only for `Active`, and resolves per-workspace.

use std::collections::HashMap;

use async_trait::async_trait;
use gt_mcp_server::{workspace_from_ext, GateStatus, WorkspaceStatusGate, WorkspaceStores,
    WORKSPACE_HEADER};
use gt_store_dolt::AppError;
use rmcp::model::Extensions;

/// Build an `Extensions` carrying the HTTP `Parts` with the given headers, the
/// way rmcp injects them per call.
fn ext_with_headers(headers: &[(&str, &str)]) -> Extensions {
    let mut builder = axum::http::Request::builder();
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let parts = builder.body(()).unwrap().into_parts().0;
    let mut ext = Extensions::new();
    ext.insert(parts);
    ext
}

#[test]
fn workspace_stores_route_each_slug_to_its_own_pool() {
    let stores = WorkspaceStores::from_base_url("mysql://gastown@127.0.0.1:3307/")
        .expect("base url parses");

    // Two distinct tenants route to their own pool cleanly and offline (the pool is
    // lazy — no socket). `store_for` now ensures the schema on first access (a DB
    // hit), so the offline routing seam is the lower-level `pool_for`.
    assert!(stores.pools().pool_for("acme").is_ok());
    assert!(stores.pools().pool_for("globex").is_ok());

    // Each got its own pool: the routing registry now holds exactly the two.
    assert_eq!(stores.live_pools(), 2, "one pool per distinct tenant");
    assert_eq!(stores.loaded_workspaces(), vec!["acme", "globex"]);

    // Re-resolving a tenant reuses its pool (idempotent — no leak).
    assert!(stores.pools().pool_for("acme").is_ok());
    assert_eq!(stores.live_pools(), 2, "re-resolve reuses the cached pool");
}

#[test]
fn malformed_tenant_slug_is_rejected_before_a_database_is_selected() {
    let stores = WorkspaceStores::from_base_url("mysql://gastown@127.0.0.1:3307/").unwrap();
    // Rejection is synchronous, before any database is selected: `pool_for` (which
    // `store_for`/`ensured_pool` call first) validates the slug, so this stays
    // offline-safe even though the happy path now ensures the schema.
    assert!(stores.pools().pool_for("Bad_Slug").is_err(), "uppercase/underscore is invalid");
    assert!(stores.pools().pool_for("").is_err(), "empty slug is invalid");
    assert!(stores.pools().pool_for("a/b").is_err(), "a path separator can never select a db");
    // A rejected slug created no pool.
    assert_eq!(stores.live_pools(), 0);
}

#[test]
fn the_tenant_channel_is_the_x_workspace_header_only() {
    // The header is the single source of the tenant.
    assert_eq!(WORKSPACE_HEADER, "x-workspace");
    let ext = ext_with_headers(&[("x-workspace", "acme")]);
    assert_eq!(workspace_from_ext(&ext), Some("acme"));

    // A blank / whitespace header falls back to None (the default tenant), never a
    // partial value.
    assert_eq!(workspace_from_ext(&ext_with_headers(&[("x-workspace", "   ")])), None);

    // Crucially: no other header — and no tool argument — can name a tenant. A
    // request carrying a `workspace`-looking value anywhere but the canonical
    // header resolves to None, so a payload cannot spoof cross-tenant access
    // (docs/04 §15: server-injected from ctx, never the body).
    assert_eq!(
        workspace_from_ext(&ext_with_headers(&[("x-workspace-id", "victim"), ("workspace", "victim")])),
        None,
        "only the canonical x-workspace header is honoured"
    );
    assert_eq!(workspace_from_ext(&Extensions::new()), None, "no Parts ⇒ default tenant");
}

#[test]
fn gate_status_admits_mutations_only_when_active() {
    assert!(GateStatus::Active.allows_mutation());
    assert!(!GateStatus::Suspended.allows_mutation());
    assert!(!GateStatus::Archived.allows_mutation());
    assert_eq!(GateStatus::Active.label(), "active");
    assert_eq!(GateStatus::Suspended.label(), "suspended");
    assert_eq!(GateStatus::Archived.label(), "archived");
}

/// A catalog-backed gate fake: the port the server consults to decide whether a
/// tenant currently accepts mutations.
struct MapGate(HashMap<&'static str, GateStatus>);

#[async_trait]
impl WorkspaceStatusGate for MapGate {
    async fn status(&self, ws: &str) -> Result<Option<GateStatus>, AppError> {
        Ok(self.0.get(ws).copied())
    }
}

#[tokio::test]
async fn status_gate_resolves_per_workspace() {
    let gate = MapGate(HashMap::from([
        ("live", GateStatus::Active),
        ("paused", GateStatus::Suspended),
        ("gone", GateStatus::Archived),
    ]));

    // Each tenant resolves to its own status; the live one admits mutations, the
    // others do not.
    assert_eq!(gate.status("live").await.unwrap(), Some(GateStatus::Active));
    assert!(gate.status("live").await.unwrap().unwrap().allows_mutation());
    assert!(!gate.status("paused").await.unwrap().unwrap().allows_mutation());
    assert!(!gate.status("gone").await.unwrap().unwrap().allows_mutation());

    // An unknown slug is None — not a block (the legacy/default or not-yet-
    // provisioned tenant the gate does not govern).
    assert_eq!(gate.status("stranger").await.unwrap(), None);
}
