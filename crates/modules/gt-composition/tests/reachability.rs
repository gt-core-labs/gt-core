//! Namespace-reachability contract (`hq-rbac-reachability.2`).
//!
//! The merge-time half of the guardrail the boot self-check enforces at deploy time: a tool
//! NAMESPACE can be registered in the server's [`DomainRouter`] yet be UNREACHABLE because no
//! least-privilege actor grant references it — exactly how `memory.*` shipped live but was denied
//! to every non-`*` actor (the operator's RBAC config granted `issues.*`/`merge.*` to `admin` but
//! never `memory.*`), with the failure invisible because the autorecall hook swallowed the denial.
//!
//! This test wires the REAL [`MemoryHandler`] descriptors through the REAL [`DomainRouter`] and
//! the REAL [`RbacConfig`] audit — no hardcoded tool names — so adding a namespace without a grant
//! in the reference policy turns this test red. Pure/offline: descriptors are static and the audit
//! is string matching, so the never-dialed `WsPools` URL is never connected.

use std::sync::Arc;

use gt_composition::mcp::{MemoryHandler, WsPools};
use gt_mcp_server::DomainRouter;
use gt_rbac::RbacConfig;

/// A router holding the memory namespace, built exactly as the server builds it (a never-dialed
/// pool cache + no embedder — descriptors don't touch either).
fn router_with_memory() -> DomainRouter {
    let pools = Arc::new(WsPools::new("postgres://unused"));
    DomainRouter::new().register(Arc::new(MemoryHandler::new(pools, None)))
}

/// The prod shape that shipped the bug: `admin` reaches issues/merge but NOT memory; `mcp-local`
/// is the `*` dev superuser. `memory.*` is therefore reachable ONLY by the superuser.
const PROD_LIKE_MISSING_MEMORY: &str = r#"
[actors.admin]
allow = ["issues.*", "merge.*"]

[actors.mcp-local]
allow = ["*"]
"#;

/// The deploy-time fix the contract forces: `admin` also gets `memory.*`.
const PROD_LIKE_WITH_MEMORY: &str = r#"
[actors.admin]
allow = ["issues.*", "merge.*", "memory.*"]

[actors.mcp-local]
allow = ["*"]
"#;

#[test]
fn registered_memory_namespace_without_a_grant_is_flagged() {
    let router = router_with_memory();
    let tool_names: Vec<String> = router.descriptors().into_iter().map(|t| t.name).collect();
    assert!(
        tool_names.iter().any(|n| n == "memory.recall"),
        "sanity: the router actually advertises the memory tools ({tool_names:?})"
    );

    let cfg = RbacConfig::from_toml(PROD_LIKE_MISSING_MEMORY).unwrap();
    assert!(cfg.has_least_privilege_actor(), "the reference policy is least-privilege");

    let orphans = cfg.least_privilege_orphans(tool_names.iter().map(String::as_str));
    // EVERY advertised memory tool is an orphan — no least-privilege actor can call it.
    assert_eq!(
        orphans, tool_names,
        "an ungranted namespace's whole tool surface is unreachable: {orphans:?}"
    );
}

#[test]
fn granting_the_namespace_clears_every_orphan() {
    let router = router_with_memory();
    let tool_names: Vec<String> = router.descriptors().into_iter().map(|t| t.name).collect();

    let cfg = RbacConfig::from_toml(PROD_LIKE_WITH_MEMORY).unwrap();
    let orphans = cfg.least_privilege_orphans(tool_names.iter().map(String::as_str));
    assert!(
        orphans.is_empty(),
        "with `memory.*` granted to a least-privilege actor, nothing is an orphan (left: {orphans:?})"
    );
}
