//! Contract for the read-only feed REST adapter (`hq-web-extras.14`).
//!
//! Drives [`gt_feed::feed_router`] in-process with `tower`'s `oneshot` against an **in-memory**
//! per-workspace page provider, proving the REST surface paginates the **same** per-workspace
//! activity log the MCP feed / `/stream` read: `GET /` returns a page (`items`, `offset`, `limit`,
//! `has_more`, `next_offset`), honors the `channel` filter, and — critically — the log is
//! per-tenant, so a tenant with no events gets an empty page (no cross-tenant leak). The wire
//! mapping (error → status) is unit-covered in `http.rs`; this is the integration proof that the
//! route is wired to a workspace-scoped read.
//!
//! Compiled only under the `axum` feature (the adapter's gate). It needs no database: the provider
//! holds a fixed list per workspace, so the contract runs on host CI with no Postgres sidecar.
#![cfg(feature = "axum")]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt; // oneshot

use gt_events::AppError;
use gt_feed::{feed_router, FeedApiState, FeedItem, FeedPage, WorkspaceFeed};
use gt_workspace::WORKSPACE_HEADER;

/// An in-memory [`WorkspaceFeed`] provider: a fixed newest-first item list per workspace slug, so
/// two tenants get genuinely separate feeds (the REST mirror of log-per-workspace). Paginates +
/// channel-filters in memory exactly as the real backing windows the log read.
struct MemProvider {
    by_ws: HashMap<String, Vec<FeedItem>>,
}

impl MemProvider {
    fn new() -> Self {
        Self { by_ws: HashMap::new() }
    }

    fn seed(mut self, workspace: &str, items: Vec<FeedItem>) -> Self {
        self.by_ws.insert(workspace.to_string(), items);
        self
    }
}

#[async_trait]
impl WorkspaceFeed for MemProvider {
    async fn page(
        &self,
        workspace: &str,
        channel: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<FeedPage, AppError> {
        let all = self.by_ws.get(workspace).cloned().unwrap_or_default();
        let filtered: Vec<FeedItem> = all
            .into_iter()
            .filter(|i| match channel {
                Some(c) if !c.is_empty() => {
                    i.kind == c || i.kind.starts_with(&format!("{c}."))
                }
                _ => true,
            })
            .collect();
        let total = filtered.len();
        let end = (offset + limit).min(total);
        let items = if offset < total { filtered[offset..end].to_vec() } else { Vec::new() };
        let has_more = end < total;
        Ok(FeedPage {
            items,
            offset,
            limit,
            has_more,
            next_offset: has_more.then_some(end),
        })
    }
}

fn item(id: &str, kind: &str, ts: &str) -> FeedItem {
    FeedItem {
        event_id: id.into(),
        kind: kind.into(),
        correlation_id: "c1".into(),
        causation_id: None,
        ts: ts.into(),
    }
}

fn router(provider: MemProvider) -> axum::Router {
    feed_router(FeedApiState::new(Arc::new(provider)))
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.expect("router responds");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn get_in(ws: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(WORKSPACE_HEADER, ws)
        .body(Body::empty())
        .unwrap()
}

fn seeded() -> MemProvider {
    MemProvider::new().seed(
        "acme",
        vec![
            item("e1", "merge.started.v1", "2026-06-01T10:00:00Z"),
            item("e2", "merge.merged.v1", "2026-06-01T10:05:00Z"),
            item("e3", "rig.added.v1", "2026-06-02T10:00:00Z"),
        ],
    )
}

#[tokio::test]
async fn feed_returns_a_page_with_the_cursor() {
    let app = router(seeded());
    let (status, body) = send(&app, get_in("acme", "/?limit=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(body["offset"], 0);
    assert_eq!(body["limit"], 2);
    assert_eq!(body["has_more"], true);
    assert_eq!(body["next_offset"], 2);
    // Page 2 picks up where page 1 left off and reports the end.
    let (_, p2) = send(&app, get_in("acme", "/?limit=2&offset=2")).await;
    assert_eq!(p2["items"].as_array().unwrap().len(), 1);
    assert_eq!(p2["has_more"], false);
    assert!(p2["next_offset"].is_null());
}

#[tokio::test]
async fn channel_filters_by_kind_namespace() {
    let app = router(seeded());
    let (status, body) = send(&app, get_in("acme", "/?channel=merge")).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "only the two merge.* events");
    assert!(items.iter().all(|i| i["kind"].as_str().unwrap().starts_with("merge.")));
}

#[tokio::test]
async fn feed_is_per_tenant() {
    let app = router(seeded());
    let (_, acme) = send(&app, get_in("acme", "/")).await;
    assert_eq!(acme["items"].as_array().unwrap().len(), 3);
    // Tenant `other` shares no log — empty page, no cross-tenant leak.
    let (status, other) = send(&app, get_in("other", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(other["items"].as_array().unwrap().is_empty());
    assert_eq!(other["has_more"], false);
}
