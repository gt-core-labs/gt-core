//! Boot-smoke: the REST module set the server boots must `build()` (hq-vcs-connections.12).
//!
//! ## The gap this closes
//!
//! On 2026-06-10 the `.9` graph REST module (`GraphModule::with_http`) shipped a `GtModule` whose
//! `register_mcp_tools` declared 2-segment tool names (`graph.query`, …). The REST `RootBuilder`
//! rejects those at `build()` with [`MalformedToolName`]`(NotThreeSegments)`, so the embeddings
//! server **crash-looped at boot in production**. `cargo check`, CI, and every unit test passed —
//! NOTHING exercised the real server's module-build, so it only surfaced live.
//!
//! This test calls [`build_rest_modules`](gt_composition::rest_modules::build_rest_modules) — the
//! SAME function `gt-mcp-server.rs main()` calls to assemble its `rest_root` builder — and asserts
//! the builder `.build()`s. The builder's finalize runs the boot-time validation (the 3-segment
//! tool-name rule, scope-conflict, event-kind-conflict, dependency-order, migration-version), so a
//! module that registers a malformed tool name fails HERE, in CI, before it can crash prod. With
//! the `.9` fix (commit 556ba66, `GraphHttpModule` registers no MCP tools) this test PASSES;
//! reverting that fix makes it FAIL with `MalformedToolName { tool: "graph.query", … }`.
//!
//! ## Offline by construction
//!
//! The module-build validates pure-data accessors (`meta`/`capability`/`register_mcp_tools`) — it
//! never connects a store. So the test feeds a `connect_lazy` Postgres pool that never dials (the
//! same trick `mcp_http_parity.rs` uses), a throwaway temp event log, and `None` for the GitHub
//! App / blob store / embedder. It needs no live PG/Dolt/MinIO.
//!
//! [`MalformedToolName`]: gt_module::BuildError::MalformedToolName

use std::sync::Arc;

use gt_composition::mcp::EventLog;
use gt_composition::rest_modules::{build_rest_modules, RestModuleParts, RestPgParts};
use gt_docs_extract::Extractor;
use gt_store_dolt::DoltIssues;

/// A never-connected Postgres URL: the assembled REST states are all lazy (they cache a pool/url
/// but only dial on a request), so a lazy pool over this is enough for the build-only path.
const DUMMY_PG: &str = "postgres://boot-smoke@127.0.0.1:1/none";

/// Build the parts with the Postgres-gated slice present (workspace/rig/connection/graph/documents)
/// — including the graph + connection modules, the `.9` crash site — over offline state.
fn parts_with_pg(event_log: Arc<EventLog>) -> RestModuleParts {
    let pool = sqlx::PgPool::connect_lazy(DUMMY_PG).expect("lazy pool never dials");
    RestModuleParts {
        meta_store: dummy_dolt_store(),
        actor: "boot-smoke".to_string(),
        meta_tools: Vec::new(),
        agent_root: std::env::temp_dir().join("gt-boot-smoke-agent"),
        dispatch_channel: None,
        event_log,
        accounts_root: std::env::temp_dir().join("gt-boot-smoke-accounts"),
        skills_seed_workspace: "default".to_string(),
        pg: Some(RestPgParts {
            pool,
            pg_url: DUMMY_PG.to_string(),
            issues_workspaces: None,
            // No GitHub App: the connection CRUD + public-rig graph path still mount, which is the
            // module surface (the install routes are conditional and carry no MCP tools either way).
            github: None,
            blob: None,
            bucket: "gt-documents".to_string(),
            extractor: Extractor::without_ocr(),
            embedder: None,
        }),
    }
}

/// A Dolt issues store handle over an unreachable URL. `DoltIssues::connect` builds the pool lazily
/// (it does not dial), and the meta REST state only stores the handle — the module build reads no
/// store — so this is enough to assemble the meta module offline.
fn dummy_dolt_store() -> Arc<DoltIssues> {
    Arc::new(
        DoltIssues::connect("mysql://boot-smoke@127.0.0.1:1/none")
            .expect("lazy Dolt pool never dials"),
    )
}

/// A throwaway temp event log for the offline build. The only IO the assembly touches is the
/// skills-catalog seed (a filesystem replay of an empty log → it seeds the role preset); a fresh
/// temp dir keeps that isolated and side-effect-free across runs.
fn temp_event_log() -> Arc<EventLog> {
    let dir = std::env::temp_dir().join(format!("gt-boot-smoke-eventlog-{}", std::process::id()));
    Arc::new(EventLog::new(Some(dir)))
}

/// The PG-on path — the production embeddings deploy's shape. Asserts the full REST module set
/// (meta + agent + quota + merge + skills + feed + convoy + workspace + rig + connection + graph +
/// documents) builds. This is the assertion that would have caught the `.9` `graph.query` crash:
/// before commit 556ba66 the graph module registered 2-segment tool names and this `.build()`
/// returned `Err(MalformedToolName)`.
#[tokio::test]
async fn rest_module_set_builds_with_postgres() {
    let parts = parts_with_pg(temp_event_log());
    let (builder, _share) = build_rest_modules(parts).await;
    let root = builder
        .build()
        .expect("the production REST module set must build — a MalformedToolName / module-registration error here is the .9-class boot crash");
    // Sanity: the graph + connection modules — the .9 crash site — actually made it into the set.
    let ids: Vec<String> = root.modules().map(|m| m.id.as_str().to_string()).collect();
    for expected in [
        "meta",
        "agent",
        "quota",
        "merge",
        "skills",
        "feed",
        "convoy",
        "workspace",
        "rig",
        "connection",
        "graph",
        "documents",
    ] {
        assert!(
            ids.iter().any(|id| id == expected),
            "REST module `{expected}` missing from the built set; got {ids:?}",
        );
    }
}

/// The PG-off path (`GT_PG_URL` unset): only the store-/event-log-backed modules mount. Asserts the
/// minimal set still builds, so the validation guard holds in the degraded single-tenant deploy too.
#[tokio::test]
async fn rest_module_set_builds_without_postgres() {
    let parts = RestModuleParts {
        meta_store: dummy_dolt_store(),
        actor: "boot-smoke".to_string(),
        meta_tools: Vec::new(),
        agent_root: std::env::temp_dir().join("gt-boot-smoke-agent-nopg"),
        dispatch_channel: None,
        event_log: temp_event_log(),
        accounts_root: std::env::temp_dir().join("gt-boot-smoke-accounts-nopg"),
        skills_seed_workspace: "default".to_string(),
        pg: None,
    };
    let (builder, share) = build_rest_modules(parts).await;
    assert!(
        share.is_none(),
        "no documents store ⇒ no public share-read router without Postgres",
    );
    builder
        .build()
        .expect("the PG-off REST module set must build");
}
