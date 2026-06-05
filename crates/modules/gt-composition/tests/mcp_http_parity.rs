//! MCP ↔ HTTP parity contract (`hq-fe-api-spec.2`).
//!
//! Every domain the gt-mcp-server exposes over **both** transports must offer the same surface
//! on each: a model driving it over MCP `tools/list` and a frontend driving it over REST reach
//! the *same* commands, dispatched to the *same* transport-free handlers (the `run_*` / domain
//! handler the `http.rs` adapter and the MCP `DomainHandler` both call — no logic is duplicated,
//! only the wire shape differs). This test pins that parity so neither surface can silently drift
//! from the other: a new MCP tool without a route, or a route naming a tool that no longer exists,
//! fails here.
//!
//! ## What "parity" means (and the exemptions)
//!
//! The server serves an MCP tool over HTTP **unless** the tool is intentionally MCP-only:
//!
//! - **`*.validate` dry-runs** — the check-then-commit `validate` sibling has no REST route; the
//!   REST `POST`/`PATCH` is the `execute`. Shape validation still runs inside the shared handler.
//! - **`issues.phase.advance`** — the operator-only frontier bump (`hq-core-mcp.7`), never an FE
//!   action.
//!
//! Read-only resource reads (`gt://issues`, `gt://issue/{id}`, `gt://doc/{id}`) surface as `GET`
//! routes with no tool sibling — listed below with `tool: None`.
//!
//! ## Scope: the REST-surfaced namespaces only
//!
//! The bin (`src/bin/gt-mcp-server.rs`) mounts REST modules for [`NAMESPACES`]: `issues` (the
//! `root` builder) plus `meta` / `agent` / `quota` / `merge` / `workspace` / `rig` / `documents`
//! (the `rest_root` builder). The other MCP namespaces — `convoy`, `graph`, `audit` — dispatch
//! over MCP only (no `with_http` module is mounted for them), so they are out of scope here: this
//! test asserts parity exactly for the namespaces the server serves on both transports.
//!
//! ## How it harvests the two surfaces
//!
//! - **MCP**: the *served* descriptor set, from the same source the running server advertises —
//!   the module registry for `issues` (harvested by `root.mcp_tools()`) and `documents` (the
//!   `DocumentsHandler` re-harvests `DocumentsModule`), and the live `DomainHandler::descriptors`
//!   for `workspace` / `rig` / `agent` / `quota` (whose tools live on the handler, not the
//!   module — `RigsModule`'s own MCP tools differ and are never what the server serves). The
//!   handlers construct over never-connected stores (a lazy PG pool, an empty event log);
//!   `descriptors()` reads no store, so this is pure and offline.
//! - **HTTP**: each module's `utoipa` `ApiDoc` — the exact OpenAPI the kernel mounts under
//!   `/api/v1/<ns>` and fuses into `GET /openapi.json`. Paths are prefix-free per module, so the
//!   table below is relative too.

use std::collections::BTreeSet;
use std::sync::Arc;

use gt_mcp_server::DomainHandler;
use gt_module::{GtModule, McpRegistry};
use utoipa::OpenApi;

use gt_composition::mcp::{
    AgentHandler, EventLog, MergeHandler, QuotaHandler, RigHandler, WorkspaceHandler, WsPools,
};
use gt_documents::DocumentsModule;
use gt_issues::IssuesModule;
use gt_meta::MetaModule;

/// The namespaces the server exposes over both MCP and HTTP — the parity scope.
const NAMESPACES: &[&str] =
    &["issues", "meta", "workspace", "rig", "documents", "agent", "quota", "merge"];

/// A never-connected Postgres URL: the PG-backed handlers store the pool/url but `descriptors()`
/// never touches it, so a lazy pool that never dials is enough to harvest the tool set offline.
const DUMMY_PG: &str = "postgres://parity@127.0.0.1:1/none";

/// One row of the canonical parity map: an HTTP route on a REST-surfaced domain and the MCP tool
/// it dispatches — or `None` for a read-only resource route that has no tool sibling.
struct Route {
    method: &'static str,
    /// Prefix-free, in `utoipa` path syntax (`{id}`), matching the module's `ApiDoc`.
    path: &'static str,
    tool: Option<&'static str>,
}

const fn rt(method: &'static str, path: &'static str, tool: Option<&'static str>) -> Route {
    Route { method, path, tool }
}

/// The canonical route↔tool map for one namespace — the single source of truth this test enforces
/// against both live surfaces. Mirrors each `http.rs`'s "Maps to MCP tool" doc table.
fn parity_map(ns: &str) -> Vec<Route> {
    match ns {
        "issues" => vec![
            rt("GET", "/", None),                                  // gt://issues
            rt("GET", "/{id}", None),                              // gt://issue/{id}
            rt("GET", "/stats", None),                             // REST-only aggregation (hq-web-extras.12)
            rt("POST", "/", Some("issues.create.execute")),
            rt("PATCH", "/{id}", Some("issues.update.execute")),
            rt("POST", "/{id}/transition", Some("issues.transition.execute")),
            rt("POST", "/{id}/close", Some("issues.close.execute")),
            rt("POST", "/{id}/claim", Some("issues.claim.execute")),
        ],
        "meta" => vec![
            rt("GET", "/help", Some("meta.help.execute")),
            rt("POST", "/report-gap", Some("meta.report-gap.execute")),
        ],
        "workspace" => vec![
            rt("GET", "/", Some("workspace.list")),
            rt("GET", "/{id}", Some("workspace.info")),
            rt("POST", "/", Some("workspace.create")),
            rt("POST", "/{id}/suspend", Some("workspace.suspend")),
            rt("POST", "/{id}/resume", Some("workspace.resume")),
            rt("POST", "/{id}/archive", Some("workspace.archive")),
        ],
        "rig" => vec![
            rt("GET", "/", Some("rig.list")),
            rt("GET", "/{name}", Some("rig.info")),
            rt("GET", "/lookup", Some("rig.lookup-by-prefix")),
            rt("POST", "/", Some("rig.add")),
            rt("POST", "/adopt", Some("rig.adopt")),
            rt("DELETE", "/{name}", Some("rig.remove")),
            rt("POST", "/{name}/set-prefix", Some("rig.set-prefix")),
            rt("POST", "/{name}/set-default-branch", Some("rig.set-default-branch")),
            rt("POST", "/{name}/set-worktree-root", Some("rig.set-worktree-root")),
        ],
        "documents" => vec![
            rt("POST", "/", Some("documents.attach.execute")),
            rt("GET", "/", Some("documents.list.execute")),
            rt("GET", "/search", Some("documents.search.execute")),
            rt("GET", "/{id}", None), // gt://doc/{id}
            rt("PATCH", "/{id}", Some("documents.update.execute")),
            rt("DELETE", "/{id}", Some("documents.remove.execute")),
            // Share lifecycle (hq-web-extras.9) — REST-only capability URLs, no MCP tool sibling.
            // The public GET /share/{hash} read lives in a SEPARATE PublicApiDoc mounted outside
            // /api/v1/documents, so it never enters this namespace's served OpenAPI (no row here).
            rt("POST", "/{id}/share", None),
            rt("GET", "/shares", None),
            rt("PATCH", "/shares/{hash}", None),
            rt("DELETE", "/shares/{hash}", None),
        ],
        "agent" => vec![
            rt("GET", "/", Some("agent.list")),
            rt("GET", "/{id}", Some("agent.info")),
            rt("POST", "/", Some("agent.spawn")),
            rt("POST", "/{id}/heartbeat", Some("agent.heartbeat")),
            rt("POST", "/{id}/end", Some("agent.end")),
            rt("POST", "/{id}/kill", Some("agent.kill")),
        ],
        "quota" => vec![
            rt("GET", "/", Some("quota.list")),
            rt("GET", "/{account}", Some("quota.info")),
            rt("POST", "/{account}/sample", Some("quota.sample")),
            rt("POST", "/{account}/probe", Some("quota.probe")),
            rt("POST", "/{account}/rotate", Some("quota.rotate")),
        ],
        "merge" => vec![
            rt("GET", "/", Some("merge.list")),
            rt("GET", "/{bead}", Some("merge.info")),
            rt("POST", "/", Some("merge.submit")),
            rt("POST", "/{bead}/start", Some("merge.start")),
            rt("POST", "/{bead}/complete", Some("merge.complete")),
            rt("POST", "/{bead}/fail", Some("merge.fail")),
        ],
        other => panic!("no parity map for namespace `{other}`"),
    }
}

/// The live OpenAPI the kernel mounts for `ns` (the module's `utoipa` `ApiDoc`).
fn served_openapi(ns: &str) -> utoipa::openapi::OpenApi {
    match ns {
        "issues" => gt_issues::ApiDoc::openapi(),
        "meta" => gt_meta::ApiDoc::openapi(),
        "workspace" => gt_workspace::ApiDoc::openapi(),
        "rig" => gt_rig::ApiDoc::openapi(),
        "documents" => gt_documents::ApiDoc::openapi(),
        "agent" => gt_agent::ApiDoc::openapi(),
        "quota" => gt_quota::ApiDoc::openapi(),
        "merge" => gt_merge::http::ApiDoc::openapi(),
        other => panic!("no OpenAPI for namespace `{other}`"),
    }
}

/// The live MCP tool names the server *advertises* for `ns` — harvested from the same source the
/// running server serves from (the module registry for `issues`/`documents`, the live
/// `DomainHandler::descriptors` for the dispatch-only namespaces).
fn served_mcp_tools(ns: &str) -> Vec<String> {
    fn from_registry(f: impl FnOnce(&mut McpRegistry)) -> Vec<String> {
        let mut reg = McpRegistry::new();
        f(&mut reg);
        reg.tools().iter().map(|t| t.name.clone()).collect()
    }
    fn from_handler(h: &dyn DomainHandler) -> Vec<String> {
        h.descriptors().iter().map(|t| t.name.clone()).collect()
    }
    let event_log = || Arc::new(EventLog::new(None));
    match ns {
        "issues" => from_registry(|r| IssuesModule::default().register_mcp_tools(r)),
        "meta" => from_registry(|r| MetaModule.register_mcp_tools(r)),
        "documents" => from_registry(|r| DocumentsModule::default().register_mcp_tools(r)),
        "workspace" => {
            let pool = sqlx::PgPool::connect_lazy(DUMMY_PG).expect("lazy pool never dials");
            from_handler(&WorkspaceHandler::new(pool))
        }
        "rig" => from_handler(&RigHandler::new(Arc::new(WsPools::new(DUMMY_PG)))),
        "agent" => from_handler(&AgentHandler::new(event_log())),
        "quota" => from_handler(&QuotaHandler::new(event_log())),
        "merge" => from_handler(&MergeHandler::new(event_log())),
        other => panic!("no MCP source for namespace `{other}`"),
    }
}

/// A tool the server is expected to also serve over HTTP — i.e. not one of the documented
/// MCP-only exemptions (`*.validate` dry-runs, the operator-only `issues.phase.advance`).
fn is_routable(tool: &str) -> bool {
    !tool.ends_with(".validate") && tool != "issues.phase.advance"
}

/// The `(METHOD, path)` set a module's OpenAPI actually declares.
fn openapi_routes(doc: &utoipa::openapi::OpenApi) -> BTreeSet<(String, String)> {
    let mut set = BTreeSet::new();
    for (path, item) in &doc.paths.paths {
        let methods = [
            ("GET", item.get.is_some()),
            ("POST", item.post.is_some()),
            ("PUT", item.put.is_some()),
            ("PATCH", item.patch.is_some()),
            ("DELETE", item.delete.is_some()),
        ];
        for (m, present) in methods {
            if present {
                set.insert((m.to_string(), path.clone()));
            }
        }
    }
    set
}

/// The `(METHOD, path)` set the parity map declares for `ns`.
fn map_routes(ns: &str) -> BTreeSet<(String, String)> {
    parity_map(ns)
        .iter()
        .map(|r| (r.method.to_string(), r.path.to_string()))
        .collect()
}

/// (1) The parity map and the served OpenAPI agree exactly, per namespace — no route in the
/// served spec is undocumented here, and no row here points at a route the module doesn't serve.
#[test]
fn parity_map_matches_served_openapi() {
    for &ns in NAMESPACES {
        let declared = map_routes(ns);
        let served = openapi_routes(&served_openapi(ns));
        let missing: Vec<_> = served.difference(&declared).collect();
        let stale: Vec<_> = declared.difference(&served).collect();
        assert!(
            missing.is_empty() && stale.is_empty(),
            "`{ns}` route drift — served-but-undocumented: {missing:?}; documented-but-not-served: {stale:?}",
        );
    }
}

/// (2) Every route that names a tool names a tool the server actually advertises over MCP — a
/// route can never dispatch to a tool that has been renamed or removed from the MCP surface.
#[tokio::test] // a never-dialing lazy PG pool still wants a Tokio context to exist
async fn every_route_tool_is_a_served_mcp_tool() {
    for &ns in NAMESPACES {
        let served: BTreeSet<String> = served_mcp_tools(ns).into_iter().collect();
        for route in parity_map(ns) {
            if let Some(tool) = route.tool {
                assert!(
                    tool.starts_with(&format!("{ns}.")),
                    "`{ns}` route {} {} maps to `{tool}`, which is not in the `{ns}.` namespace",
                    route.method,
                    route.path,
                );
                assert!(
                    served.contains(tool),
                    "`{ns}` route {} {} maps to `{tool}`, absent from the served MCP tools {served:?}",
                    route.method,
                    route.path,
                );
            }
        }
    }
}

/// (3) Every routable MCP tool (everything but the documented MCP-only exemptions) has exactly
/// one HTTP route — a new `workspace.*` / `rig.*` / … tool cannot land MCP-only by omission.
#[tokio::test] // a never-dialing lazy PG pool still wants a Tokio context to exist
async fn every_routable_mcp_tool_has_a_route() {
    for &ns in NAMESPACES {
        let routed: BTreeSet<&str> = parity_map(ns).iter().filter_map(|r| r.tool).collect();
        for tool in served_mcp_tools(ns) {
            if is_routable(&tool) {
                assert!(
                    routed.contains(tool.as_str()),
                    "`{ns}` MCP tool `{tool}` has no HTTP route (and is not an MCP-only exemption)",
                );
            }
        }
    }
}

/// Guards the exemption predicate itself: the `issues` namespace carries known MCP-only tools
/// (the five `.validate` dry-runs + `issues.phase.advance`) that must stay route-free, so the
/// test above genuinely exercises the exemption path rather than vacuously passing.
#[test]
fn issues_namespace_has_the_expected_mcp_only_tools() {
    let tools = served_mcp_tools("issues");
    let mcp_only: Vec<&String> = tools.iter().filter(|t| !is_routable(t)).collect();
    assert!(
        mcp_only.iter().any(|t| t.as_str() == "issues.phase.advance"),
        "expected issues.phase.advance among the MCP-only tools: {mcp_only:?}",
    );
    assert_eq!(
        mcp_only.iter().filter(|t| t.ends_with(".validate")).count(),
        5,
        "expected the five issues `.validate` dry-runs to be MCP-only: {mcp_only:?}",
    );
}
