//! End-to-end gate for `mod-hello` (`hq-mod-docs.2`).
//!
//! Proves the module on-ramp works as one flow rather than as isolated unit
//! tests: a single module is assembled through the real [`RootBuilder`], and
//! every contribution point is observed from the outside —
//!
//! 1. **register** — the builder accepts the module (validating its event-kind
//!    and MCP-tool namespaces) and lists it.
//! 2. **route** — the merged application router answers under the module's
//!    `/api/v1/hello` prefix, with the per-method scope guard enforced.
//! 3. **migration** — the module's SQL migration is harvested into the plan,
//!    owned by `hello`, at version 1.
//! 4. **MCP** — the module's tool is harvested under its namespace.
//! 5. **capability** — the module declares the versioned event it emits.
//! 6. **dog** — a worker claims a sample task, runs it through a
//!    [`PluginExecutor`] over a fake agent backend, and the run is turned into a
//!    digest receipt and recorded.
//!
//! Live event emission onto a bus and live migration apply against Postgres are
//! deliberately out of scope here: the bus (`gt-events`) migrates up in Phase 4,
//! and migration application is covered by `gt-module-migrate`'s own tests
//! against a real pool. This gate proves the kernel-side wiring is whole.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use mod_hello::{HelloModule, GREETED_V1};
use tower::ServiceExt; // `oneshot`

use gt_module::{CallerScopes, EventKind, GtModule, RootBuilder, Scope};

// --- 1. register --------------------------------------------------------------

#[test]
fn builder_registers_the_module() {
    let root = RootBuilder::new().module(HelloModule).build().unwrap();
    let ids: Vec<&str> = root.modules().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["hello"]);

    let meta = root.modules().next().unwrap();
    assert_eq!(meta.name, "Hello");
    assert_eq!(meta.version, semver::Version::new(1, 0, 0));
}

// --- 2. route (with scope enforcement) ---------------------------------------

/// Drive `method path` against the assembled application router, optionally
/// carrying caller scopes the way the auth layer would inject them.
async fn hit(method: Method, path: &str, scopes: Option<CallerScopes>) -> (StatusCode, String) {
    let app = RootBuilder::new()
        .module(HelloModule)
        .build()
        .unwrap()
        .into_router();
    let builder = Request::builder().method(method).uri(path);
    let req = match scopes {
        Some(s) => builder.extension(s).body(Body::empty()),
        None => builder.body(Body::empty()),
    }
    .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn scopes(labels: &[&str]) -> CallerScopes {
    CallerScopes::new(labels.iter().map(|s| Scope::new(*s).unwrap()))
}

#[tokio::test]
async fn read_route_answers_under_prefix_with_read_scope() {
    let (status, body) = hit(Method::GET, "/api/v1/hello/greeting", Some(scopes(&["hello.read"]))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("hello, gas town"), "unexpected body: {body}");
}

#[tokio::test]
async fn unauthenticated_request_is_rejected() {
    // No scopes extension at all → the module guard treats it as unauthenticated.
    let (status, _) = hit(Method::GET, "/api/v1/hello/greeting", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn write_route_needs_write_scope() {
    // Holding only read is forbidden on a mutating method...
    let (forbidden, _) =
        hit(Method::POST, "/api/v1/hello/greeting", Some(scopes(&["hello.read"]))).await;
    assert_eq!(forbidden, StatusCode::FORBIDDEN);

    // ...and write succeeds.
    let (ok, body) = hit(Method::POST, "/api/v1/hello/greeting", Some(scopes(&["hello.write"]))).await;
    assert_eq!(ok, StatusCode::OK);
    assert!(body.contains("greeted"), "unexpected body: {body}");
}

// --- 3. migration ------------------------------------------------------------

#[test]
fn migration_is_harvested_into_the_plan() {
    let root = RootBuilder::new().module(HelloModule).build().unwrap();
    let plan = root.migrations();
    assert_eq!(plan.len(), 1);
    let (owner, migration) = plan[0];
    assert_eq!(owner.as_str(), "hello");
    assert_eq!(migration.version, 1);
    assert_eq!(migration.name, "create_greetings");
    assert!(migration.sql.contains("hello_greetings"));
}

// --- 4. MCP ------------------------------------------------------------------

#[test]
fn mcp_tool_is_harvested_under_the_module_namespace() {
    let root = RootBuilder::new().module(HelloModule).build().unwrap();
    let tools: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
    assert_eq!(tools, ["hello.greeting.show"]);

    let (module, _action, _verb) = root.mcp_tools().next().unwrap().parse_name().unwrap();
    assert_eq!(module, "hello");
}

// --- 5. capability (event declaration) ---------------------------------------

#[test]
fn module_declares_the_versioned_event_it_emits() {
    let cap = HelloModule.capability();
    let kind = EventKind::new(GREETED_V1).unwrap();
    assert!(cap.emits().contains(&kind));
    assert_eq!(kind.module(), "hello");
    assert_eq!(kind.version(), 1);
}

// --- 6. dog (claim → run → digest receipt) -----------------------------------

mod dog_flow {
    use async_trait::async_trait;
    use gt_dog::{
        Digest, Dispatch, DogDispatcher, DogId, DogReport, ExecBackend, ExecutionType,
        InMemoryReceipts, PluginExecutor, ReceiptSink, TrackingLabels,
    };

    /// Stand-in for a spawned agent: validates nothing of its own (the executor
    /// did that) and reports success. The real backend spawns a session.
    struct FakeAgent;

    #[async_trait]
    impl ExecBackend for FakeAgent {
        async fn run(&self, _claim: &str, _execution: &ExecutionType) -> DogReport {
            DogReport::Completed
        }
    }

    #[tokio::test]
    async fn dog_claims_runs_and_emits_a_digest_receipt() {
        // A pool with one worker claims the sample task.
        let mut pool = DogDispatcher::with_capacity(1);
        let dog = DogId::new("hello-dog").unwrap();
        assert!(pool.register(dog.clone()));

        let claim = "hello-claim-1";
        assert_eq!(pool.dispatch(claim), Dispatch::Assigned(dog.clone()));

        // The plugin executor validates the request, then runs it on the backend.
        let executor = PluginExecutor::new(FakeAgent);
        let report = executor
            .execute(claim, &ExecutionType::Agent { persona: "hello".into() })
            .await
            .unwrap();
        assert_eq!(report, DogReport::Completed);

        // The finished run becomes a digest, injected into the receipt sink.
        let sink = InMemoryReceipts::new();
        let digest = Digest::from_report(dog, claim, &report, TrackingLabels::new());
        sink.emit(&digest).await.unwrap();

        let recorded = sink.recorded();
        assert_eq!(recorded.len(), 1);
        assert!(!recorded[0].is_failure());
        let labels = recorded[0].labels();
        assert_eq!(labels.get("claim").map(String::as_str), Some(claim));
        assert_eq!(labels.get("dog").map(String::as_str), Some("hello-dog"));
    }
}
