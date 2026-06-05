//! The `axum` REST adapter for the read-only `feed.*` surface (`hq-web-extras.14`).
//!
//! Exposes the workspace activity log — the historical, paginated read that complements the live
//! `/stream` SSE push — as a REST route over the **same** per-workspace event log that feeds the
//! MCP feed surface and `/stream`. No domain logic is duplicated, only the wire shape differs. It
//! folds into `gt-feed` behind the off-by-default `axum` feature (docs/03 Rule 4), exactly as
//! gt-rig/gt-workspace gate theirs, rather than living in a sibling crate.
//!
//! ## Read-only, paginated, per-workspace
//!
//! The single GET route returns a page of the activity feed: `offset` / `limit` windowing,
//! `has_more` + `next_offset` for the cursor, an optional `channel` (event-kind namespace) filter,
//! newest-first. The feed emits no events — it only reads them — so every route is a GET ⇒
//! `feed.read`.
//!
//! The log is **per-tenant**: each workspace owns its own append-only stream. The tenant comes from
//! the [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim / sanctioned
//! header), **never** from the URL or body (docs/03 Rule 6, docs/04 §15). The adapter holds a
//! [`WorkspaceFeed`] provider that hands back a workspace-scoped page per request — the REST mirror
//! of the MCP feed's / `/stream`'s per-workspace read.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under `/api/v1/feed`
//!   and wraps it with the capability-derived scope guard; the composition root layers the auth
//!   middleware in front. The single route is a GET, so the guard requires `feed.read`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use gt_events::AppError;
use gt_workspace::WorkspaceContext;

/// Default page size when the caller omits `limit` — the same cap the SSE feed reconnect uses.
fn default_limit() -> usize {
    50
}

/// One activity-feed item on the wire — the read-only projection of a stored event record. Tool-
/// neutral + serializable so the REST payload is stable for FE codegen, independent of the
/// internal `EventRecord` shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FeedItem {
    /// The event's stable id.
    pub event_id: String,
    /// The event kind (`<namespace>.<noun>.vN`) — the `channel` filter keys on its namespace.
    pub kind: String,
    /// The workflow lifeline this event belongs to.
    pub correlation_id: String,
    /// The event that caused this one, if any.
    pub causation_id: Option<String>,
    /// RFC3339 timestamp.
    pub ts: String,
}

/// One page of the activity feed, with the cursor the FE pages forward on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FeedPage {
    /// The items in this page, newest-first.
    pub items: Vec<FeedItem>,
    /// The offset this page started at.
    pub offset: usize,
    /// The page size requested.
    pub limit: usize,
    /// Whether more items exist beyond this page.
    pub has_more: bool,
    /// The offset to request for the next page, or `None` when this is the last page.
    pub next_offset: Option<usize>,
}

/// A per-workspace feed read provider for the REST adapter.
///
/// The feed is a tenant-local read over the workspace log, so a request must run against the
/// caller's own stream. The binary supplies an implementation that, given the resolved workspace
/// slug + the page window, returns that tenant's [`FeedPage`] (the composition path reads the
/// per-workspace event log); a test supplies an in-memory one. This is the read-only mirror of the
/// MCP feed / `/stream` per-workspace read — the composition root owns the log policy, the adapter
/// only asks for "the page for *this* workspace".
#[async_trait]
pub trait WorkspaceFeed: Send + Sync {
    /// One page of the activity feed for `workspace` (the slug from the auth context, never the
    /// path), newest-first, optionally filtered to a `channel` (event-kind namespace).
    async fn page(
        &self,
        workspace: &str,
        channel: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<FeedPage, AppError>;
}

/// Everything the feed REST handler needs, baked into the router with [`Router::with_state`] before
/// it leaves [`feed_router`] so the merged application router carries no outstanding state type
/// (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct FeedApiState {
    /// The per-workspace read provider the binary supplies — the module owns no store of its own.
    feed: Arc<dyn WorkspaceFeed>,
}

impl FeedApiState {
    /// Build the REST state over a per-workspace read provider.
    pub fn new(feed: Arc<dyn WorkspaceFeed>) -> Self {
        Self { feed }
    }
}

/// Build the read-only feed REST router with `state` baked in (`hq-web-extras.14`).
///
/// The path is **relative**: the builder nests it under `/api/v1/feed` and applies the scope
/// guard. `register_routes` on the HTTP-enabled feed module returns exactly this router.
///
/// | Method + path | Surfaces                              |
/// |---------------|---------------------------------------|
/// | `GET /`       | a paginated page of the activity feed |
pub fn feed_router(state: FeedApiState) -> Router {
    Router::new().route("/", get(get_feed).with_state(state))
}

/// Querystring for `GET /` — the page window + optional channel filter.
#[derive(Debug, Deserialize)]
struct FeedQuery {
    /// Keep only events whose kind is in this namespace (e.g. `merge` → `merge.*`).
    channel: Option<String>,
    /// Where this page starts (0-based, from the newest end). Defaults to 0.
    #[serde(default)]
    offset: usize,
    /// Max items in this page. Defaults to [`default_limit`].
    #[serde(default = "default_limit")]
    limit: usize,
}

/// `GET /?channel=&offset=&limit=` — a page of the caller's workspace activity feed, newest-first.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    params(
        ("channel" = Option<String>, Query, description = "Event-kind namespace filter (e.g. merge)"),
        ("offset" = Option<usize>, Query, description = "0-based page offset from the newest end (default 0)"),
        ("limit" = Option<usize>, Query, description = "Max items per page (default 50)"),
    ),
    responses((status = 200, description = "A page of the activity feed for the caller's workspace, newest-first")),
))]
async fn get_feed(
    State(st): State<FeedApiState>,
    ctx: WorkspaceContext,
    Query(q): Query<FeedQuery>,
) -> Result<Json<Value>, ApiError> {
    let page = st
        .feed
        .page(ctx.workspace().as_str(), q.channel.as_deref(), q.offset, q.limit)
        .await?;
    Ok(Json(json!(page)))
}

/// The combined OpenAPI document for the read-only feed REST surface (`hq-web-extras.14`). The
/// builder mounts it under the module prefix and rewrites its relative path to `/api/v1/feed`, so
/// the `#[utoipa::path]` annotation stays prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(get_feed))]
pub struct ApiDoc;

/// HTTP wrapper over the feed errors so the handler can `?`-propagate a read failure. The only
/// failure shape is a log read error, which is a server `500`.
#[derive(Debug)]
struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // The only failure shape is a log read fault — a server error.
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_the_relative_route_prefix_free() {
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        assert!(paths.contains(&"/"), "{paths:?}");
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn read_error_maps_to_500() {
        assert_eq!(
            ApiError(AppError::Other("boom".into())).into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn feed_page_serializes_with_cursor_fields() {
        let page = FeedPage {
            items: vec![FeedItem {
                event_id: "e1".into(),
                kind: "merge.merged.v1".into(),
                correlation_id: "c1".into(),
                causation_id: None,
                ts: "2026-06-01T10:00:00Z".into(),
            }],
            offset: 0,
            limit: 50,
            has_more: true,
            next_offset: Some(50),
        };
        let v = serde_json::to_value(&page).unwrap();
        assert_eq!(v["items"][0]["kind"], "merge.merged.v1");
        assert_eq!(v["has_more"], true);
        assert_eq!(v["next_offset"], 50);
    }
}
