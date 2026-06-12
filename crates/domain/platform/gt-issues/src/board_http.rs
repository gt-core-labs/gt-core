//! The `axum` REST adapter for the board projection (hq-62130a).
//!
//! Same shape as [`crate::http`]: relative routes the builder nests under
//! `/api/v1/board` behind the `board.read`/`board.write` scope guard, each
//! dispatching to the SAME transport-free handlers the `board.*` MCP tools use
//! ([`crate::board`]) — MCP-first, one logic path, two wire shapes (ADR D6).
//!
//! | Method + path   | Maps to MCP tool        |
//! |-----------------|-------------------------|
//! | `GET /`         | `board.list.execute`    |
//! | `GET /scopes`   | `board.scopes.execute`  |
//! | `POST /move`    | `board.move.execute`    |
//! | `POST /reorder` | `board.reorder.execute` |

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use gt_workspace::WorkspaceContext;

use crate::board::{
    run_board_list, run_board_move, run_board_reorder, run_board_scopes, BoardList, BoardMove,
    BoardReorder, BoardScope,
};
use crate::events::{emit_issue_event, IssueVerb};
use crate::http::{ApiError, IssuesApiState};

/// Build the board REST router with the shared issues API `state` baked in.
/// Paths are relative; [`BoardModule`](crate::BoardModule) returns exactly this
/// router when built with HTTP state.
pub fn board_router(state: IssuesApiState) -> Router {
    Router::new()
        .route("/", get(board_list))
        .route("/scopes", get(board_scopes))
        .route("/move", post(board_move))
        .route("/reorder", post(board_reorder))
        .with_state(state)
}

/// `GET /` — the board snapshot (`board.list.execute`): status columns ordered
/// by lexorank for one `(rig, workspace)`, optional epic filter + group_by lanes.
#[utoipa::path(
    get, path = "/",
    params(BoardList),
    responses(
        (status = 200, description = "Board snapshot (columns -> cards, optional lanes)"),
        (status = 422, description = "Missing scope key or unknown group_by"),
    ),
)]
async fn board_list(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Query(args): Query<BoardList>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let snapshot = run_board_list(&store, &args, false).await?;
    Ok(Json(serde_json::to_value(snapshot).map_err(|e| {
        ApiError(gt_store_dolt::AppError::Other(format!("encode board: {e}")))
    })?))
}

/// `GET /scopes` — the distinct `(rig, workspace)` pairs present in the
/// tracker (`board.scopes.execute`): the REAL boards, for contrasting UI
/// selectors against actual data instead of the catalog cross-product
/// (hq-26d679).
#[utoipa::path(
    get, path = "/scopes",
    responses(
        (status = 200, description = "Sorted distinct (rig, workspace) pairs", body = [BoardScope]),
    ),
)]
async fn board_scopes(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let scopes = run_board_scopes(&store).await?;
    Ok(Json(json!({ "scopes": scopes })))
}

/// `POST /move` — column change + placement in ONE atomic Dolt commit
/// (`board.move.execute`). Emits the same `issues.transitioned.v1` feed event
/// as `issues.transition`, so SSE consumers see the card move live.
#[utoipa::path(
    post, path = "/move",
    responses(
        (status = 200, description = "Card moved; body carries the landing rank"),
        (status = 404, description = "Unknown card"),
        (status = 422, description = "Illegal transition or out-of-scope card"),
    ),
)]
async fn board_move(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Json(args): Json<BoardMove>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let rank = run_board_move(&store, &args, false).await?;
    emit_issue_event(
        st.sink(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Transitioned,
        &args.id,
        st.actor_str(),
    )
    .await;
    Ok(Json(json!({ "ok": true, "id": args.id, "column": args.column, "rank": rank })))
}

/// `POST /reorder` — rank-only placement within the card's current column
/// (`board.reorder.execute`). Emits `issues.updated.v1` (no status change).
#[utoipa::path(
    post, path = "/reorder",
    responses(
        (status = 200, description = "Card reordered; body carries the landing rank"),
        (status = 404, description = "Unknown card"),
        (status = 422, description = "Out-of-scope card or unknown neighbor"),
    ),
)]
async fn board_reorder(
    State(st): State<IssuesApiState>,
    ctx: WorkspaceContext,
    Json(args): Json<BoardReorder>,
) -> Result<Json<Value>, ApiError> {
    let store = st.resolve(&ctx).await?;
    let rank = run_board_reorder(&store, &args, false).await?;
    emit_issue_event(
        st.sink(),
        &store,
        Some(ctx.workspace().as_str()),
        IssueVerb::Updated,
        &args.id,
        st.actor_str(),
    )
    .await;
    Ok(Json(json!({ "ok": true, "id": args.id, "rank": rank })))
}

/// The OpenAPI spec for the board REST routes. Paths are relative; the builder
/// rewrites them under `/api/v1/board` when mounting.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(board_list, board_scopes, board_move, board_reorder),
    components(schemas(BoardList, BoardMove, BoardReorder, BoardScope))
)]
pub struct BoardApiDoc;

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        let doc = BoardApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/scopes", "/move", "/reorder"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        assert!(
            paths.iter().all(|p| !p.starts_with("/api")),
            "board paths must stay relative: {paths:?}"
        );
    }
}
