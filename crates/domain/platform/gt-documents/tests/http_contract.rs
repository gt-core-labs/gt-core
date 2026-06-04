//! Contract for the documents REST adapter (`hq-fe-api-platform.3`).
//!
//! Drives [`gt_documents::documents_router`] in-process with `tower`'s `oneshot`, proving the
//! REST surface dispatches to the **same** per-workspace store the MCP `documents.*` tools use:
//! a `POST /` attach is visible to `GET /` and `GET /:id`, a `PATCH /:id` takes the id from the
//! path (not the body), a `DELETE /:id` soft-deletes under its version guard, and a missing doc
//! is a `404`. The wire mapping (validation → 422, error → status, path-id-wins) is unit-covered
//! in `http.rs`; this is the integration proof that the routes are wired to the store.
//!
//! Compiled only under the `axum` feature (the adapter's gate) and the live roundtrip is skipped
//! unless `GT_PG_URL` is set, so host CI without a Postgres sidecar still compiles + passes. The
//! shape/validation path (no DB) always runs.
#![cfg(feature = "axum")]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // oneshot

use gt_documents::{documents_router, DocumentsApiState};
use gt_docs_extract::Extractor;

/// Build the REST router over `pg_url`, no object store (md documents need none), the no-OCR
/// extractor, and no embedder (phase-1 full-text).
fn router(pg_url: &str) -> axum::Router {
    documents_router(DocumentsApiState::new(
        pg_url,
        None,
        "test-bucket",
        Extractor::without_ocr(),
        None,
    ))
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

/// No DB needed: shape validation runs before the repository is touched, so an unknown `kind`
/// is a `422` against a router whose Postgres URL never resolves. Proves routing + validation +
/// error mapping without a live store.
#[tokio::test]
async fn bad_attach_is_422_without_touching_the_db() {
    let app = router("postgres://nobody@127.0.0.1:1/none");

    // `kind="exe"` is rejected by AttachDoc::validate before any connection attempt.
    let (status, _) = send(
        &app,
        post_json(
            "/",
            json!({
                "owner_type": "epic", "owner_id": "hq-docs", "kind": "exe",
                "filename": "x", "body_md": "hi", "created_by": "agent"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // A md attach with no body is likewise a pre-DB 422.
    let (status, _) = send(
        &app,
        post_json(
            "/",
            json!({
                "owner_type": "epic", "owner_id": "hq-docs", "kind": "md",
                "filename": "x", "created_by": "agent"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// Best-effort schema bootstrap so the `ws_default` template + its documents tables exist. The
/// migration SQL is idempotent where it can be (`CREATE SCHEMA/TABLE IF NOT EXISTS`); applies are
/// best-effort (errors ignored) because parallel tests + a pre-populated DB both re-run these —
/// `CREATE SCHEMA IF NOT EXISTS` is not race-atomic in Postgres, and an already-present object is
/// fine. If the schema genuinely failed to materialize, the subsequent store calls surface it.
async fn ensure_schema(pg_url: &str) {
    use gt_store_pg::WorkspacePool;
    let pool = WorkspacePool::connect(pg_url, "default").await.expect("connect");
    for m in gt_store_pg::workspace_migrations().into_iter().chain(gt_store_pg::docs_migrations()) {
        let _ = sqlx::raw_sql(&m.sql).execute(pool.pool()).await;
    }
}

#[tokio::test]
async fn attach_list_get_update_remove_roundtrip_through_the_store() {
    let Ok(pg_url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset — skipping documents http roundtrip");
        return;
    };
    ensure_schema(&pg_url).await;
    let app = router(&pg_url);

    // POST / attaches a .md document; the id is minted server-side.
    let (status, attached) = send(
        &app,
        post_json(
            "/",
            json!({
                "owner_type": "epic", "owner_id": "hq-http-doc", "kind": "md",
                "filename": "spec.md", "body_md": "# original", "created_by": "test"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "attach 201: {attached}");
    let id = attached["id"].as_str().expect("doc id").to_string();
    assert_eq!(attached["body_md"], "# original");
    assert_eq!(attached["version"], 0);

    // GET /?owner lists it.
    let (status, page) = send(&app, get("/?owner_type=epic&owner_id=hq-http-doc")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["documents"][0]["id"], id);

    // GET /:id reads the single document back.
    let (status, one) = send(&app, get(&format!("/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["filename"], "spec.md");

    // PATCH /:id takes the id from the path even when the body names another; the body is edited.
    let patch = Request::builder()
        .method("PATCH")
        .uri(format!("/{id}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "id": "decoy", "expected_version": 0, "body_md": "# edited", "edited_by": "test" })
                .to_string(),
        ))
        .unwrap();
    let (status, patched) = send(&app, patch).await;
    assert_eq!(status, StatusCode::OK, "patch: {patched}");
    assert_eq!(patched["id"], id, "path id wins over body id");
    assert_eq!(patched["body_md"], "# edited");
    assert_eq!(patched["version"], 1, "version bumped");

    // A stale PATCH (expected_version=0 again) is a 409 conflict.
    let stale = Request::builder()
        .method("PATCH")
        .uri(format!("/{id}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "expected_version": 0, "body_md": "# nope", "edited_by": "test" }).to_string(),
        ))
        .unwrap();
    let (status, _) = send(&app, stale).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // DELETE /:id?expected_version soft-deletes; it then drops out of the owner list.
    let del = Request::builder()
        .method("DELETE")
        .uri(format!("/{id}?expected_version=1"))
        .body(Body::empty())
        .unwrap();
    let (status, removed) = send(&app, del).await;
    assert_eq!(status, StatusCode::OK, "delete: {removed}");
    assert_eq!(removed["removed"], true);

    let (_, page) = send(&app, get("/?owner_type=epic&owner_id=hq-http-doc")).await;
    assert!(
        page["documents"].as_array().map(|a| a.is_empty()).unwrap_or(false),
        "soft-deleted doc is gone from the live list: {page}"
    );
}

#[tokio::test]
async fn missing_document_is_404() {
    let Ok(pg_url) = std::env::var("GT_PG_URL") else {
        eprintln!("GT_PG_URL unset — skipping documents http 404");
        return;
    };
    ensure_schema(&pg_url).await;
    let app = router(&pg_url);

    let (status, _) = send(&app, get("/no-such-doc")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
