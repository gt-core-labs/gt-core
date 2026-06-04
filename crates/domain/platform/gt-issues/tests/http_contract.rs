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
use gt_store_dolt::DoltIssues;

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

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
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
