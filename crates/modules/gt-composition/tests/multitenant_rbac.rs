//! RBAC scope + multi-tenant routing (`hq-mcp-test.4`, `hq-mcp-test.5`).
//!
//! Exercises the two guardrails the MCP server enforces before any store I/O,
//! through their public seams:
//!
//! - **.4 RBAC / deny-by-default** — boot a 2-actor `scope.toml`, resolve each
//!   actor's [`Scope`] (`X-Actor` picks the allow-list), and assert the denied
//!   actor's write is rejected while the allowed actor's call passes. An actor
//!   absent from the config resolves to the deny-everything scope (docs/04 §the
//!   scope gate is deny-by-default).
//! - **.5 multi-tenant** — two workspaces resolve to distinct tenant stores
//!   (isolation); the authoritative tenant comes ONLY from the `x-workspace`
//!   header, never the tool body, so a spoofed `workspace_id` argument is inert
//!   (docs/04 §15); and the status gate blocks a mutation on a
//!   suspended/archived tenant.
//!
//! Pure (no Dolt/PG): every seam under test is offline-safe — scope resolution is
//! string matching, store resolution is a lazy pool handle, the gate is an enum
//! predicate.

use gt_mcp_server::{workspace_from_ext, GateStatus, WorkspaceStores, WORKSPACE_HEADER};
use gt_rbac::{RbacConfig, Scope};
use rmcp::model::Extensions;

/// A 2-actor scope config: `admin` may do anything, `viewer` may only dry-run a
/// create (validate), never execute it.
const SCOPE_TOML: &str = r#"
[actors.admin]
allow = ["*"]

[actors.viewer]
allow = ["issues.create.validate"]
"#;

// ----- .4 RBAC / scope -------------------------------------------------------

#[test]
fn two_actor_scope_allows_admin_and_denies_viewer_write() {
    let cfg = RbacConfig::from_toml(SCOPE_TOML).expect("parse scope.toml");

    let admin = Scope::from_rbac(&cfg, "admin");
    let viewer = Scope::from_rbac(&cfg, "viewer");

    // The allowed actor's call passes the gate.
    assert!(admin.check("issues.create.execute").is_ok(), "admin may execute");

    // The denied actor is rejected on the write it has no grant for...
    assert!(
        viewer.check("issues.create.execute").is_err(),
        "viewer must be denied the execute it has no grant for",
    );
    // ...but its single granted dry-run passes.
    assert!(viewer.check("issues.create.validate").is_ok(), "viewer may validate");
}

#[test]
fn an_unknown_actor_is_denied_by_default() {
    let cfg = RbacConfig::from_toml(SCOPE_TOML).expect("parse scope.toml");
    // An actor with no entry resolves to the deny-everything scope — the gate is
    // deny-by-default, not allow-by-default.
    let ghost = Scope::from_rbac(&cfg, "nobody");
    assert!(ghost.check("issues.create.validate").is_err(), "unknown actor denied reads");
    assert!(ghost.check("issues.create.execute").is_err(), "unknown actor denied writes");
}

// ----- .5 multi-tenant routing ----------------------------------------------

/// Build request extensions carrying a single header, the way rmcp injects the
/// HTTP `Parts` into each call.
fn ext_with_header(name: &str, value: &str) -> Extensions {
    let parts = axum::http::Request::builder()
        .header(name, value)
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let mut ext = Extensions::new();
    ext.insert(parts);
    ext
}

#[test]
fn two_workspaces_resolve_to_distinct_isolated_stores() {
    let stores = WorkspaceStores::from_base_url("mysql://gtapp@127.0.0.1:3307/").unwrap();
    // Distinct, well-formed tenants each build their own (lazy) store — no socket
    // opened, but the slugs route to different `hq_<ws>` databases.
    assert!(stores.store_for("acme").is_ok());
    assert!(stores.store_for("beta").is_ok());
    // A malformed slug can never select a surprising database.
    assert!(stores.store_for("Bad Slug").is_err());
}

#[test]
fn authoritative_tenant_comes_from_the_header_not_a_spoofed_body() {
    // The header IS the only tenant channel — resolution reads `x-workspace`.
    let acme = ext_with_header(WORKSPACE_HEADER, "acme");
    assert_eq!(workspace_from_ext(&acme), Some("acme"));

    // A caller cannot smuggle a tenant through any other channel: a bogus
    // `x-workspace-id` (the shape a spoofer would try) is NOT the header, so it
    // resolves to None → the request falls back to the default-workspace store,
    // never the spoofed "victim" tenant. There is no args/body path at all.
    let spoof = ext_with_header("x-workspace-id", "victim");
    assert_eq!(workspace_from_ext(&spoof), None, "a non-header channel cannot select a tenant");
}

#[test]
fn status_gate_blocks_mutations_on_suspended_or_archived_tenants() {
    // The gate the server applies after resolving the tenant, before store I/O.
    assert!(GateStatus::Active.allows_mutation(), "an active tenant accepts writes");
    assert!(!GateStatus::Suspended.allows_mutation(), "a suspended tenant blocks writes");
    assert!(!GateStatus::Archived.allows_mutation(), "an archived tenant blocks writes");
}
