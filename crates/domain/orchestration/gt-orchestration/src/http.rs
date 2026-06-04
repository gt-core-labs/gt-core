//! The `axum` REST adapter for the `convoy.*` surface (`hq-fe-api-orch.3`).
//!
//! The orchestration sibling of the quota (`hq-fe-api-orch.4`) and merge (`hq-fe-api-orch.2`)
//! REST surfaces: it exposes the convoy board as REST routes that dispatch to the **same**
//! [`Command`] decide/apply layer ([`LaunchConvoy`]/[`CompleteMember`]/[`FailMember`]) the MCP
//! `convoy.*` tools use — no domain logic is duplicated, only the wire shape differs. It folds
//! into `gt-orchestration` behind the off-by-default `axum` feature (docs/03 Rule 4), exactly as
//! gt-quota/gt-merge gate theirs, rather than living in a sibling crate.
//!
//! ## Event-sourced, per-workspace, never from the path
//!
//! A convoy keeps **no projection table**: its state is a log of [`OrchEvent`] replayed through
//! the [`OrchState`] reducer into a [`ConvoyBoard`] (`hq-mod-refactor.6`). Every call therefore
//! rehydrates the board from the workspace's event log, executes the command against it, and
//! appends the produced events back — the exact read-modify-append the MCP `ConvoyHandler` runs,
//! with the [`WorkspaceConvoy`] provider standing in for the handler's `EventLog`. A single
//! convoy command is **variadic** (launch emits the convoy plus the first member-dispatched), so
//! the append takes the whole batch.
//!
//! The log is **per-tenant** (path-partitioned per workspace, docs/04 §15). The workspace comes
//! from the [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim /
//! sanctioned header), **never** from the URL or body (docs/03 Rule 6). The convoy id is a plain
//! resource identifier carried in the path on a mutation; the tenant a request belongs to is
//! resolved upstream from the auth context.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/convoy` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `convoys.read` / `convoys.write` for the request's verb-class.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_events::{AppError, Command, EventKind};
use gt_workspace::WorkspaceContext;

use crate::commands::{CompleteMember, FailMember, LaunchConvoy};
use crate::events::OrchEvent;
use crate::state::{Convoy, ConvoyBoard};

/// A per-workspace convoy event-log provider for the REST adapter.
///
/// Convoys are event-sourced, so a request must replay the caller's own log into the board,
/// execute the command, and append the produced events back. The binary supplies an
/// implementation that, given the resolved workspace slug, rehydrates the board and appends to
/// that tenant's log (the composition root wraps its `EventLog`); a test supplies an in-memory
/// one. This is the REST mirror of the MCP `ConvoyHandler`'s `EventLog` — the composition root
/// owns the storage policy, the adapter only asks for "the board for *this* workspace" and
/// "append these events to it".
#[async_trait]
pub trait WorkspaceConvoy: Send + Sync {
    /// Rebuild the convoy board by replaying `workspace`'s convoy event log (the slug from the
    /// auth context, never the path) — the same projection the MCP handler's `board()` bridge
    /// produces via [`ConvoyBoard::from_state`](crate::ConvoyBoard::from_state).
    async fn board(&self, workspace: &str) -> Result<ConvoyBoard, AppError>;

    /// Append a batch of decided convoy events to `workspace`'s log, closing the
    /// read-modify-append. The batch is whole because one convoy command can emit several events
    /// (a launch is convoy-created + launched + the first member-dispatched).
    async fn append(&self, workspace: &str, events: Vec<OrchEvent>) -> Result<(), AppError>;
}

/// Everything the convoy REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`convoy_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct ConvoyApiState {
    /// The per-workspace event-log provider the binary supplies — the module owns no store of
    /// its own, exactly as the MCP `ConvoyHandler` owns only its `EventLog`.
    convoy: Arc<dyn WorkspaceConvoy>,
}

impl ConvoyApiState {
    /// Build the REST state over a per-workspace event-log provider.
    pub fn new(convoy: Arc<dyn WorkspaceConvoy>) -> Self {
        Self { convoy }
    }
}

/// Build the convoy REST router with `state` baked in (`hq-fe-api-orch.3`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/convoy` and applies the
/// scope guard. `register_routes` on the HTTP-enabled convoy module returns exactly this router
/// when it carries HTTP state.
///
/// | Method + path                  | Maps to MCP tool         |
/// |--------------------------------|--------------------------|
/// | `GET /`                        | `convoy.list`            |
/// | `GET /:convoy`                 | `convoy.info`            |
/// | `POST /`                       | `convoy.launch`          |
/// | `POST /:convoy/complete-member`| `convoy.complete-member` |
/// | `POST /:convoy/fail-member`    | `convoy.fail-member`     |
pub fn convoy_router(state: ConvoyApiState) -> Router {
    Router::new()
        .route("/", get(list_convoys).post(launch_convoy))
        .route("/:convoy/complete-member", post(complete_member))
        .route("/:convoy/fail-member", post(fail_member))
        .route("/:convoy", get(get_convoy))
        .with_state(state)
}

/// `GET /` — every convoy in the workspace with its state + members (`convoy.list`). Reuses the
/// replayed board so the projection matches the MCP read exactly.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every convoy with its state + members")),
))]
async fn list_convoys(
    State(st): State<ConvoyApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let board = st.convoy.board(ctx.workspace().as_str()).await?;
    Ok(Json(json!({
        "convoys": board.snapshot().iter().map(convoy_json).collect::<Vec<_>>(),
    })))
}

/// `GET /:convoy` — one convoy's state + members (`convoy.info`); `404` when no convoy matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{convoy}",
    params(("convoy" = String, Path, description = "Convoy id")),
    responses(
        (status = 200, description = "The convoy's state + members"),
        (status = 404, description = "No convoy with that id"),
    ),
))]
async fn get_convoy(
    State(st): State<ConvoyApiState>,
    ctx: WorkspaceContext,
    Path(convoy): Path<String>,
) -> Result<Json<Value>, ApiError> {
    match st.convoy.board(ctx.workspace().as_str()).await?.get(&convoy) {
        Some(c) => Ok(Json(convoy_json(c))),
        None => Err(ApiError(AppError::NotFound(format!("convoy {convoy}")))),
    }
}

/// `POST /` — launch a convoy over an ordered member list (`convoy.launch`). The convoy id lives
/// in the body (it names the *new* resource, not an existing path), and dispatches the first
/// member. Emits convoy.created + convoy.launched + the first convoy.member-dispatched.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 200, description = "Convoy launched; echoes the emitted event kinds"),
        (status = 422, description = "Validation failed (empty members / duplicate convoy)"),
    ),
))]
async fn launch_convoy(
    State(st): State<ConvoyApiState>,
    ctx: WorkspaceContext,
    Json(cmd): Json<LaunchConvoy>,
) -> Result<Json<Value>, ApiError> {
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `POST /:convoy/complete-member` — mark the active member done and hand off to the next, closing
/// the convoy when it was the last (`convoy.complete-member`). The convoy always comes from the
/// path and overwrites any `convoy` in the body (docs/03 Rule 6); the member is the body.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{convoy}/complete-member",
    params(("convoy" = String, Path, description = "Convoy id")),
    responses(
        (status = 200, description = "Member completed; echoes the emitted event kinds"),
        (status = 422, description = "Validation failed (not the active member / not launched)"),
    ),
))]
async fn complete_member(
    State(st): State<ConvoyApiState>,
    ctx: WorkspaceContext,
    Path(convoy): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: CompleteMember = with_path_field(body, "convoy", convoy)?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `POST /:convoy/fail-member` — fail the active member and halt the convoy (`convoy.fail-member`).
/// The convoy comes from the path; `member` + `reason` are the body. Emits convoy.member-failed +
/// convoy.failed.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{convoy}/fail-member",
    params(("convoy" = String, Path, description = "Convoy id")),
    responses(
        (status = 200, description = "Member failed; echoes the emitted event kinds"),
        (status = 422, description = "Validation failed (not the active member / not launched)"),
    ),
))]
async fn fail_member(
    State(st): State<ConvoyApiState>,
    ctx: WorkspaceContext,
    Path(convoy): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: FailMember = with_path_field(body, "convoy", convoy)?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// Replay → execute → append: the REST mirror of the MCP `ConvoyHandler::run`.
///
/// Rehydrates the board from the workspace log, runs the command's decide/apply against it (the
/// same `Command::execute` the actor and MCP tools take), and appends the produced batch. The
/// `200` body echoes the emitted event kinds, exactly like the MCP path.
async fn run<C>(st: &ConvoyApiState, workspace: &str, cmd: C) -> Result<Json<Value>, ApiError>
where
    C: Command<State = ConvoyBoard, Output = Vec<OrchEvent>>,
{
    let mut board = st.convoy.board(workspace).await?;
    let events = cmd.execute(&mut board)?;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    st.convoy.append(workspace, events).await?;
    Ok(Json(json!({ "ok": true, "events": kinds })))
}

/// The combined OpenAPI document for the convoy REST surface (`hq-fe-api-orch.3`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/convoy/...`, so
/// the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_convoys,
    get_convoy,
    launch_convoy,
    complete_member,
    fail_member,
))]
pub struct ApiDoc;

/// Shape one convoy (id, state, members) as the REST payload — the same shape the MCP
/// `ConvoyHandler` emits, so a client sees an identical body across transports.
fn convoy_json(convoy: &Convoy) -> Value {
    json!({
        "id": convoy.id,
        "state": convoy.state.as_str(),
        "members": convoy
            .members
            .iter()
            .map(|m| json!({ "bead": m.bead, "state": m.state.as_str() }))
            .collect::<Vec<_>>(),
    })
}

/// Deserialize a path-addressed command body, forcing `field` to the path segment so the target
/// convoy is never trusted from the payload (docs/03 Rule 6). The path value overwrites any
/// same-named key in the body and supplies it when absent. A malformed payload is a `422`,
/// matching the MCP `parse` path.
fn with_path_field<T: DeserializeOwned>(
    mut body: Value,
    field: &str,
    value: String,
) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert(field.to_string(), Value::String(value));
        }
        // A non-object body (e.g. `null` from an empty request) becomes just the path field.
        other => *other = json!({ field: value }),
    }
    serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))
}

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and have it
/// rendered with the right status. The body is the bare error message — the same text the MCP
/// path surfaces — so a client sees an identical reason across transports.
#[derive(Debug)]
struct ApiError(AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::InvalidTransition(_) => StatusCode::CONFLICT,
            AppError::Handler(_) | AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn openapi_lists_every_relative_route_prefix_free() {
        // The spec the builder mounts: paths are relative (no `/api/v1/convoy`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{convoy}", "/{convoy}/complete-member", "/{convoy}/fail-member"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/convoy`.
        assert!(paths.iter().all(|p| !p.contains("/api/v1")), "{paths:?}");
    }

    #[test]
    fn error_status_mapping_matches_app_error_kinds() {
        let cases = [
            (AppError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (AppError::Validation("x".into()), StatusCode::UNPROCESSABLE_ENTITY),
            (AppError::InvalidTransition("x".into()), StatusCode::CONFLICT),
            (AppError::Handler("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
            (AppError::Other("x".into()), StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (err, want) in cases {
            assert_eq!(ApiError(err).into_response().status(), want);
        }
    }

    #[test]
    fn with_path_field_forces_convoy_over_body() {
        // The body names a DECOY convoy; the path segment must win (docs/03 Rule 6).
        let cmd: CompleteMember = with_path_field(
            json!({ "convoy": "decoy", "member": "b1" }),
            "convoy",
            "keep".to_string(),
        )
        .expect("valid complete-member");
        assert_eq!(cmd.convoy, "keep", "path convoy overrides the body");
        assert_eq!(cmd.member, "b1");
    }

    #[test]
    fn with_path_field_rejects_malformed() {
        // A fail-member missing the required `reason` is a validation fault, matching MCP parse.
        let err = with_path_field::<FailMember>(
            json!({ "member": "b1" }),
            "convoy",
            "c1".to_string(),
        )
        .unwrap_err();
        assert!(matches!(err.0, AppError::Validation(_)));
    }
}
