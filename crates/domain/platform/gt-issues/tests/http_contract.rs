//! Contract for the issues REST adapter (`hq-auth-routes.2`).
//!
//! Drives [`gt_issues::http::issues_router`] in-process with `tower`'s `oneshot` against a real
//! Dolt server, proving the REST surface dispatches to the **same** handlers as the MCP tools:
//! a `POST /` create is visible to `GET /:id` and `GET /`, a `PATCH /:id` takes the id from the
//! path, a `POST /:id/claim` flips status, and a missing bead is a `404`. The wire mapping
//! (querystring → filter, error → status) is unit-covered in `http.rs`; this is the integration
//! proof that the routes are wired to the store.
//!
//! Compiled only under the `axum` feature (the adapter's gate) and skipped unless `GT_DOLT_URL`
//! is set, so host CI without a Dolt sidecar still compiles. Each test uses its own throwaway
//! database so parallel runs never share a table.
#![cfg(feature = "axum")]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mysql_async::prelude::Queryable;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt; // oneshot

use gt_issues::http::{issues_router, IssuesApiState};
use gt_issues::resources::{filter_ready, read_issues};
use gt_issues::{
    CommitInfo, CommitInspector, InspectorProvider, SurfaceProvider, SurfaceTree,
};
use gt_store_dolt::{DoltIssues, IssueFilter};

/// Create a throwaway DB `db` + an empty `issues` table (full schema via `ensure_schema` on the
/// returned repo). Each test passes its own `db` so the parallel seeds never share a table.
async fn fresh_db(base: &str, db: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pool = gt_store_dolt::connect(base)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {db}")).await?;
    conn.query_drop(format!("CREATE DATABASE {db}")).await?;
    Ok(())
}

/// Build the REST router over a freshly-schema'd throwaway DB.
async fn router(base: &str, db: &str) -> axum::Router {
    let repo = DoltIssues::connect(&format!("{base}/{db}")).expect("connect");
    repo.ensure_schema().await.expect("schema present");
    issues_router(IssuesApiState::new(Arc::new(repo), "test-actor"))
}

/// Send a request and return `(status, parsed-json-or-null)`.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// The tenant header the `WorkspaceContext` extractor reads (`gt_workspace::WORKSPACE_HEADER`).
/// Every request carries it so the workspace-aware handlers resolve a tenant; the router here is
/// built single-tenant (no `with_workspaces`), so resolution falls back to the one test store.
const WS_HEADER: &str = "X-GT-Workspace";

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header(WS_HEADER, "test")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).header(WS_HEADER, "test").body(Body::empty()).unwrap()
}

#[tokio::test]
async fn create_then_read_roundtrips_through_the_store() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_roundtrip";
    fresh_db(&base, db).await.expect("db");
    let app = router(&base, db).await;

    // POST / creates an epic (epics are NN-16-exempt, so no external_ref is needed).
    let (status, body) = send(
        &app,
        post_json("/", json!({ "id": "hq-http", "title": "http epic", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create returns 201: {body}");
    assert_eq!(body["id"], "hq-http");

    // GET /:id sees the just-created bead with its heavy bodies + version.
    let (status, detail) = send(&app, get("/hq-http")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["id"], "hq-http");
    assert_eq!(detail["title"], "http epic");

    // GET / lists it in the page envelope.
    let (status, page) = send(&app, get("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 1);
    assert_eq!(page["rows"][0]["id"], "hq-http");
}

#[tokio::test]
async fn missing_bead_is_404() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_404";
    fresh_db(&base, db).await.expect("db");
    let app = router(&base, db).await;

    let (status, _) = send(&app, get("/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_takes_id_from_path_not_body() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_patch_id";
    fresh_db(&base, db).await.expect("db");
    let app = router(&base, db).await;

    // Two epics exist; the patch targets `keep` via the path while the body carries the OTHER
    // id. The path must win (docs/03 Rule 6) — `keep` is patched, `decoy` is untouched.
    for id in ["keep", "decoy"] {
        let (s, _) =
            send(&app, post_json("/", json!({ "id": id, "title": "orig", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })))
                .await;
        assert_eq!(s, StatusCode::CREATED);
    }

    let req = Request::builder()
        .method("PATCH")
        .uri("/keep")
        .header("content-type", "application/json")
        .header(WS_HEADER, "test")
        .body(Body::from(json!({ "id": "decoy", "title": "patched" }).to_string()))
        .unwrap();
    let (status, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    // `keep` got the new title; `decoy` (named in the body) was never touched.
    let (_, keep) = send(&app, get("/keep")).await;
    assert_eq!(keep["title"], "patched", "path id is patched");
    let (_, decoy) = send(&app, get("/decoy")).await;
    assert_eq!(decoy["title"], "orig", "body id must not be patched");
}

#[tokio::test]
async fn claim_flips_status_to_working() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_claim";
    fresh_db(&base, db).await.expect("db");
    let app = router(&base, db).await;

    send(&app, post_json("/", json!({ "id": "hq-claim", "title": "c", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })))
        .await;

    // First claim wins: the server actor owns it and status flips open -> working.
    let (status, body) = send(&app, post_json("/hq-claim/claim", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "won");
    assert_eq!(body["owner"], "test-actor");

    let (_, detail) = send(&app, get("/hq-claim")).await;
    assert_eq!(detail["status"], "working", "claim flips status to working");
}

#[tokio::test]
async fn stats_groups_by_status_with_progress_and_totals() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_stats";
    fresh_db(&base, db).await.expect("db");
    let app = router(&base, db).await;

    // Two epics under the same id prefix (rig = "hq"); one is then claimed -> working.
    for id in ["hq-stats-a", "hq-stats-b"] {
        let (s, _) = send(
            &app,
            post_json("/", json!({ "id": id, "title": "s", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
    }
    send(&app, post_json("/hq-stats-a/claim", json!({}))).await;

    // group_by=status: one open + one working bucket, totals over both.
    let (status, body) = send(&app, get("/stats?group_by=status")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["group_by"], json!(["status"]));
    assert_eq!(body["totals"]["total"], 2);
    let buckets = body["buckets"].as_array().expect("buckets array");
    let working = buckets
        .iter()
        .find(|b| b["key"]["status"] == "working")
        .expect("working bucket");
    assert_eq!(working["working"], 1);
    assert_eq!(working["total"], 1);

    // group_by=rig: both beads share the "hq" prefix => a single bucket of 2.
    let (status, body) = send(&app, get("/stats?group_by=rig")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let buckets = body["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"]["rig"], "hq");
    assert_eq!(buckets[0]["total"], 2);

    // Unknown/missing group_by is a 422 (client error), not a 500.
    let (status, _) = send(&app, get("/stats?group_by=bogus")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = send(&app, get("/stats")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// Build a request carrying an explicit `X-GT-Workspace` so the routed router resolves a tenant.
fn req_ws(method: &str, uri: &str, ws: &str, body: Option<Value>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri).header(WS_HEADER, ws);
    match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            b.body(Body::from(v.to_string())).unwrap()
        }
        None => b.body(Body::empty()).unwrap(),
    }
}

/// With `with_workspaces` wired, each request routes to its own tenant's `hq_<ws>` store: a bead
/// created under one `X-GT-Workspace` is invisible under another. This is the REST mirror of the
/// MCP path's claim-scoped routing (`hq-gap-issues-rest-workspace-routing`) — proof that the REST
/// surface isolates tenants, not just `/me/stats`.
#[tokio::test]
async fn multi_tenant_routes_each_request_to_its_own_workspace_store() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();

    // Two tenants, each with its own schema'd `hq_<ws>` DB (what the binary provisions per
    // workspace; here we seed them inline so the per-workspace pool can connect).
    let (ws_a, ws_b) = ("mtroutea", "mtrouteb");
    let (db_a, db_b) = (gt_store_dolt::dolt_db_name(ws_a), gt_store_dolt::dolt_db_name(ws_b));
    for db in [&db_a, &db_b] {
        fresh_db(&base, db).await.expect("tenant db");
        DoltIssues::connect(&format!("{base}/{db}"))
            .expect("connect tenant")
            .ensure_schema()
            .await
            .expect("tenant schema");
    }

    // Fallback store (never hit on a routed request) + the shared per-workspace pool cache.
    let fallback = Arc::new(DoltIssues::connect(&format!("{base}/{db_a}")).expect("fallback"));
    let pools =
        Arc::new(gt_store_dolt::WorkspacePools::from_url(&format!("{base}/")).expect("pools"));
    let app = issues_router(IssuesApiState::new(fallback, "test-actor").with_workspaces(pools));

    // Create a bead under tenant A only.
    let body = json!({ "id": "only-in-a", "title": "a-only", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] });
    let (status, _) = send(&app, req_ws("POST", "/", ws_a, Some(body))).await;
    assert_eq!(status, StatusCode::CREATED);

    // Tenant A sees exactly that bead; tenant B's tracker is untouched (isolation).
    let (_, a) = send(&app, req_ws("GET", "/", ws_a, None)).await;
    assert_eq!(a["total"], 1, "tenant A sees its own bead");
    assert_eq!(a["rows"][0]["id"], "only-in-a");
    let (_, b) = send(&app, req_ws("GET", "/", ws_b, None)).await;
    assert_eq!(b["total"], 0, "tenant B never sees tenant A's bead");

    // A direct read under B for that id is a 404, not a cross-tenant leak.
    let (status, _) = send(&app, req_ws("GET", "/only-in-a", ws_b, None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// `GET /?ready=1` returns the SAME sound set as the MCP `gt://issues?ready=1`
/// resource (`hq-platform-hardening.4`). Both gather the candidate rows then narrow
/// with the SAME `filter_ready` over the same phase frontier + delivered index + the
/// `main` git tree (accept-all here, no repo wired). The REST result must be a bare
/// array of only the ready ids — no pager envelope — equalling `filter_ready` applied
/// in-process, which is exactly what the resource serves.
#[tokio::test]
async fn ready_filter_matches_the_mcp_resource_sound_set() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_ready";
    fresh_db(&base, db).await.expect("db");
    let repo = Arc::new(DoltIssues::connect(&format!("{base}/{db}")).expect("connect"));
    repo.ensure_schema().await.expect("schema");
    let app = issues_router(IssuesApiState::new(repo.clone(), "test-actor"));

    // A mixed set: two OPEN epics (sound — no deps, accept-all surface, phase open),
    // plus one that gets claimed -> working (NOT sound: not open).
    for id in ["hq-ready-a", "hq-ready-b", "hq-claimed"] {
        let (s, _) = send(
            &app,
            post_json("/", json!({ "id": id, "title": "r", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED);
    }
    send(&app, post_json("/hq-claimed/claim", json!({}))).await;

    // REST `?ready=1`: a bare array (no `total`/`rows` envelope) of only the sound beads.
    let (status, ready) = send(&app, get("/?ready=1")).await;
    assert_eq!(status, StatusCode::OK, "{ready}");
    let rest_ids: Vec<String> = ready
        .as_array()
        .expect("?ready=1 is a bare array, not a page")
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();

    // Compute the same set the MCP resource would: read the candidate rows, then narrow
    // with the identical `filter_ready` over the gathered inputs + an accept-all tree.
    let filter = IssueFilter { ready: true, ..Default::default() };
    let rows = read_issues(&repo, &filter).await.expect("rows");
    let open_phase = repo.open_phase().await.expect("phase");
    let deps = repo.dep_index().await.expect("deps");
    let want: Vec<String> = filter_ready(rows, open_phase, &deps, &gt_issues::AllowAllTree)
        .into_iter()
        .map(|r| r.id)
        .collect();

    let mut rest_sorted = rest_ids.clone();
    rest_sorted.sort();
    let mut want_sorted = want.clone();
    want_sorted.sort();
    assert_eq!(rest_sorted, want_sorted, "REST ?ready=1 == MCP resource sound set");
    // The claimed (working) bead is excluded; the two open epics are present.
    assert!(!rest_ids.iter().any(|i| i == "hq-claimed"), "working bead is not ready");
    assert!(rest_ids.iter().any(|i| i == "hq-ready-a"));
    assert!(rest_ids.iter().any(|i| i == "hq-ready-b"));
}

/// Run `git` in `dir` and assert success (test-repo setup helper).
fn git(dir: &std::path::Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// A test-local git-backed [`SurfaceTree`]: the flat blob set of `main` via
/// `git ls-tree`, mirroring the orchestration `GitSurfaceTree` so the contract test
/// exercises the REST S3 path without depending on the orchestration crate.
struct TestGitTree {
    paths: std::collections::HashSet<String>,
}

impl TestGitTree {
    fn snapshot(repo: &std::path::Path) -> Self {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["ls-tree", "-r", "--name-only", "main"])
            .output()
            .expect("ls-tree");
        let paths = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        Self { paths }
    }
}

impl SurfaceTree for TestGitTree {
    fn contains(&self, path: &str) -> bool {
        let p = path.trim().trim_end_matches('/');
        if p.is_empty() {
            return false;
        }
        let prefix = format!("{p}/");
        self.paths.iter().any(|t| t == p || t.starts_with(&prefix))
    }
}

struct TestSurfaceProvider {
    repo: std::path::PathBuf,
}

impl SurfaceProvider for TestSurfaceProvider {
    fn surface_tree(&self) -> Box<dyn SurfaceTree + Send + Sync> {
        Box::new(TestGitTree::snapshot(&self.repo))
    }
}

/// Test-local git [`CommitInspector`]: resolves a sha on `main` + its changed paths,
/// mirroring the orchestration `GitCommitInspector`.
struct TestGitInspector {
    repo: std::path::PathBuf,
}

impl CommitInspector for TestGitInspector {
    fn inspect(&self, sha: &str) -> Option<CommitInfo> {
        let run = |args: &[&str]| {
            std::process::Command::new("git").arg("-C").arg(&self.repo).args(args).output().ok()
        };
        let rev = run(&["rev-parse", "--verify", &format!("{sha}^{{commit}}")])?;
        if !rev.status.success() {
            return None;
        }
        let full_sha = String::from_utf8_lossy(&rev.stdout).trim().to_string();
        if full_sha.is_empty() {
            return None;
        }
        let anc = run(&["merge-base", "--is-ancestor", &full_sha, "main"])?;
        if !anc.status.success() {
            return None;
        }
        let diff = run(&["diff-tree", "--no-commit-id", "--name-only", "-r", &full_sha])?;
        let changed_paths = String::from_utf8_lossy(&diff.stdout)
            .lines()
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        Some(CommitInfo { full_sha, changed_paths })
    }
}

struct TestInspectorProvider {
    repo: std::path::PathBuf,
}

impl InspectorProvider for TestInspectorProvider {
    fn commit_inspector(&self) -> Option<Box<dyn CommitInspector + Send + Sync>> {
        Some(Box::new(TestGitInspector { repo: self.repo.clone() }))
    }
}

/// With git verification wired (the REST mirror of `GT_REPO_DIR` set,
/// `hq-platform-hardening.5`): create with a non-existent `planned:false` surface is
/// REJECTED (S3), exactly as the MCP path rejects it; and a close whose `commit_sha`
/// does not resolve on `main` / does not touch a non-planned surface is REJECTED (S2),
/// not silently accepted as the permissive REST path did before.
#[tokio::test]
async fn git_verification_rejects_bad_surface_and_unverified_close() {
    let Ok(base) = std::env::var("GT_DOLT_URL") else {
        eprintln!("GT_DOLT_URL unset — skipping http contract");
        return;
    };
    let base = base.trim_end_matches('/').to_string();
    let db = "gt_http_gitverify";
    fresh_db(&base, db).await.expect("db");
    let repo_store = Arc::new(DoltIssues::connect(&format!("{base}/{db}")).expect("connect"));
    repo_store.ensure_schema().await.expect("schema");

    // A throwaway git repo whose `main` tree holds exactly one real file.
    let tmp = std::env::temp_dir().join(format!("gt-http-gitverify-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("src")).expect("mkdir");
    std::fs::write(tmp.join("src/real.rs"), "// real surface\n").expect("write");
    git(&tmp, &["init", "-q", "-b", "main"]);
    git(&tmp, &["config", "user.email", "t@t"]);
    git(&tmp, &["config", "user.name", "t"]);
    git(&tmp, &["add", "."]);
    git(&tmp, &["commit", "-q", "-m", "add real surface"]);

    let app = issues_router(
        IssuesApiState::new(repo_store.clone(), "test-actor").with_git_verification(
            Arc::new(TestSurfaceProvider { repo: tmp.clone() }),
            Arc::new(TestInspectorProvider { repo: tmp.clone() }),
        ),
    );

    // The parent epic the NN-16 taxonomy requires the beads to hang off of.
    let (s, _) = send(
        &app,
        post_json("/", json!({ "id": "hq-gv", "title": "epic", "issue_type": "epic", "created_by": "test", "domain": ["platform.rig"] })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    // S3: a bead naming a non-planned surface that does NOT exist on `main` is rejected
    // at create (422), exactly as the MCP create path rejects it.
    let bad = json!({
        "id": "hq-gv.1", "title": "bad surface", "issue_type": "bead",
        "external_ref": "hq-gv", "created_by": "test", "domain": ["platform.rig"],
        "surface": [{ "path": "src/does_not_exist.rs", "planned": false }],
    });
    let (status, body) = send(&app, post_json("/", bad)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "non-existent surface rejected: {body}");

    // A bead with a REAL non-planned surface is accepted (the tree contains src/real.rs).
    let good = json!({
        "id": "hq-gv.2", "title": "real surface", "issue_type": "bead",
        "external_ref": "hq-gv", "created_by": "test", "domain": ["platform.rig"],
        "surface": [{ "path": "src/real.rs", "planned": false }],
    });
    let (status, body) = send(&app, post_json("/", good)).await;
    assert_eq!(status, StatusCode::CREATED, "real surface accepted: {body}");

    // S2: closing the code-surface bead with a bogus sha (not on `main`) is rejected —
    // delivery verification denies the off-tree commit.
    let (status, body) = send(
        &app,
        post_json("/hq-gv.2/close", json!({ "commit_sha": "0".repeat(40), "closed_by_session": "test" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "unverified close rejected: {body}");

    let _ = std::fs::remove_dir_all(&tmp);
}
