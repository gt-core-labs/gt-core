//! The `axum` REST adapter for the `merge.*` surface (`hq-fe-api-orch.2`).
//!
//! The orchestration sibling of the issues/rig REST surfaces: it exposes the merge board as
//! REST routes that dispatch to the **same** [`MergeCommand`](crate::MergeCommand) validate/
//! execute layer the MCP `merge.*` tools use — no domain logic is duplicated, only the wire
//! shape differs. It folds into `gt-merge` behind the off-by-default `axum` feature
//! (docs/03 Rule 4), exactly as gt-issues/gt-rig/gt-workspace gate theirs, rather than living in
//! a sibling crate.
//!
//! ## Per-workspace, never from the path
//!
//! A merge board is **per-tenant**: each workspace drives its own slots (the MCP `MergeHandler`
//! keys its event log by `ctx.workspace`). A merge request must therefore resolve the caller's
//! tenant *first* and run against that tenant's board. The tenant comes from the
//! [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim / sanctioned
//! header), **never** from the URL or body (docs/03 Rule 6, docs/04 §15). The bead id is a plain
//! resource identifier; it is never the tenant. The adapter holds a [`WorkspaceMerges`] provider
//! that hands back a workspace-scoped [`MergeRepository`](crate::MergeRepository) per request —
//! the REST mirror of the MCP handler's per-workspace event log.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/merge` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `merge.read` / `merge.write` for the request's verb-class.
//! - **It runs no real git merge.** As with the MCP path, the physical `git merge` / PR open is a
//!   composition-root edge side-effect (the board only models the slot state machine); the edge
//!   reports back through `POST /:bead/complete` and `POST /:bead/fail`.
//! - **It marks no owning rig's graph stale.** That cross-domain side-effect (`hq-graphrig.7`)
//!   lives in the composition-root `MergeHandler`, which holds the rig catalog pools; the
//!   transport-free crate adapter stays a pure merge adapter.
//!
//! The state machine itself (`Ready → Merging → Merged | Failed`), duplicate-submit rejection,
//! and not-found handling all run via the shared [`MergeCommand`](crate::MergeCommand) layer, so
//! a client sees identical semantics across MCP and REST.

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

use crate::commands::{CompleteMerge, FailMerge, StartMerge, SubmitMerge};
use crate::repo::MergeRepository;
use crate::state::{MergeBoard, MergeSlot};

/// A per-workspace merge repository provider for the REST adapter.
///
/// A merge board is tenant-local, so a request must run against the caller's own slots. The
/// binary supplies an implementation that, given the resolved workspace slug, hands back a
/// repository scoped to that tenant (production wraps the Dolt-backed store; a test supplies an
/// in-memory one). This is the REST mirror of the MCP `MergeHandler`'s per-workspace event log —
/// the composition root owns the persistence policy, the adapter only asks for "the repo for
/// *this* workspace".
#[async_trait]
pub trait WorkspaceMerges: Send + Sync {
    /// The merge repository scoped to `workspace` (the slug from the auth context, never the path).
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynMergeRepository>, AppError>;
}

/// Object-safe mirror of the [`MergeRepository`](crate::MergeRepository) port for the REST adapter.
///
/// [`MergeRepository`](crate::MergeRepository) is an RPITIT trait (returns `impl Future`), so it
/// cannot be made into a `dyn` object — but [`WorkspaceMerges`] must return one (the concrete repo
/// type is the binary's choice, hidden behind the provider). This trait restates the same three
/// operations with boxed futures (`async_trait`) so a `Box<dyn DynMergeRepository>` is returnable,
/// and the blanket impl below adapts every [`MergeRepository`](crate::MergeRepository) to it for
/// free.
#[async_trait]
pub trait DynMergeRepository: Send + Sync {
    /// Insert or replace one slot (mirrors the live slot after a transition).
    async fn upsert_slot(&self, slot: &MergeSlot) -> Result<(), AppError>;
    /// Read one slot by bead id.
    async fn get_slot(&self, bead: &str) -> Result<Option<MergeSlot>, AppError>;
    /// Snapshot of every slot in the tenant's board.
    async fn list_slots(&self) -> Result<Vec<MergeSlot>, AppError>;
}

/// Every [`MergeRepository`](crate::MergeRepository) is a [`DynMergeRepository`] — the
/// boxed-future adapter, so a provider can return `Box::new(InMemoryMergeRepo::new())` (or the
/// Dolt adapter) without restating the methods.
#[async_trait]
impl<R: MergeRepository> DynMergeRepository for R {
    async fn upsert_slot(&self, slot: &MergeSlot) -> Result<(), AppError> {
        MergeRepository::upsert_slot(self, slot).await
    }
    async fn get_slot(&self, bead: &str) -> Result<Option<MergeSlot>, AppError> {
        MergeRepository::get_slot(self, bead).await
    }
    async fn list_slots(&self) -> Result<Vec<MergeSlot>, AppError> {
        MergeRepository::list_slots(self).await
    }
}

/// Everything the merge REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`merge_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct MergeApiState {
    /// The per-workspace repository provider the binary supplies — the module owns no store of
    /// its own, exactly as the MCP `MergeHandler` owns only its per-workspace log.
    merges: Arc<dyn WorkspaceMerges>,
}

impl MergeApiState {
    /// Build the REST state over a per-workspace repository provider.
    pub fn new(merges: Arc<dyn WorkspaceMerges>) -> Self {
        Self { merges }
    }
}

/// Build the merge REST router with `state` baked in (`hq-fe-api-orch.2`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/merge` and applies the scope
/// guard. `register_routes` on the HTTP-enabled merge module returns exactly this router.
///
/// | Method + path            | Maps to MCP tool |
/// |--------------------------|------------------|
/// | `GET /`                  | `merge.list`     |
/// | `GET /:bead`             | `merge.info`     |
/// | `POST /`                 | `merge.submit`   |
/// | `POST /:bead/start`      | `merge.start`    |
/// | `POST /:bead/complete`   | `merge.complete` |
/// | `POST /:bead/fail`       | `merge.fail`     |
pub fn merge_router(state: MergeApiState) -> Router {
    Router::new()
        .route("/", get(list_merges).post(submit_merge))
        .route("/:bead", get(get_merge))
        .route("/:bead/start", post(start_merge))
        .route("/:bead/complete", post(complete_merge))
        .route("/:bead/fail", post(fail_merge))
        .with_state(state)
}

/// `GET /` — every slot on the caller's merge board (`merge.list`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every slot on the workspace merge board")),
))]
async fn list_merges(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    let slots = repo.list_slots().await?;
    Ok(Json(json!({ "slots": slots.iter().map(slot_json).collect::<Vec<_>>() })))
}

/// `GET /:bead` — one bead's merge slot (`merge.info`); `404` when no slot matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{bead}",
    params(("bead" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "The bead's merge slot"),
        (status = 404, description = "No merge slot for that bead"),
    ),
))]
async fn get_merge(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
    Path(bead): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    match repo.get_slot(&bead).await? {
        Some(slot) => Ok(Json(slot_json(&slot))),
        None => Err(ApiError(AppError::NotFound(format!("merge slot {bead}")))),
    }
}

/// `POST /` — submit a bead's branch into the queue (`merge.submit`). The bead lives in the body
/// (it names the new slot, like the issues `create` / rig `add`); a duplicate submit is rejected
/// against the rehydrated board. `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Merge slot registered in Ready"),
        (status = 422, description = "Validation failed (empty fields or already submitted)"),
    ),
))]
async fn submit_merge(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    let cmd: SubmitMerge = parse_cmd(body)?;
    let bead = cmd.bead.clone();
    // Decide + persist; the `201` envelope below replaces the helper's `200` body.
    let _ = run_cmd(&*repo, &bead, &cmd).await?;
    Ok((StatusCode::CREATED, Json(json!({ "ok": true, "bead": bead }))).into_response())
}

/// `POST /:bead/start` — take a queued slot to the physical merge, `Ready → Merging`
/// (`merge.start`). The bead always comes from the path (docs/03 Rule 6).
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{bead}/start",
    params(("bead" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Slot moved to Merging"),
        (status = 404, description = "No merge slot for that bead"),
        (status = 409, description = "Illegal transition (not in Ready)"),
    ),
))]
async fn start_merge(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
    Path(bead): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    let cmd: StartMerge = with_path_bead(body, bead.clone())?;
    run_cmd(&*repo, &bead, &cmd).await
}

/// `POST /:bead/complete` — the edge reports a landed merge, `Merging → Merged` (`merge.complete`).
/// The bead comes from the path; the landed `sha` lives in the body.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{bead}/complete",
    params(("bead" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Slot moved to Merged"),
        (status = 404, description = "No merge slot for that bead"),
        (status = 409, description = "Illegal transition (not in Merging)"),
        (status = 422, description = "Validation failed (missing sha)"),
    ),
))]
async fn complete_merge(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
    Path(bead): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    let cmd: CompleteMerge = with_path_bead(body, bead.clone())?;
    run_cmd(&*repo, &bead, &cmd).await
}

/// `POST /:bead/fail` — the edge reports a failed merge, `Merging → Failed` (`merge.fail`). The
/// bead comes from the path; the `reason` lives in the body.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{bead}/fail",
    params(("bead" = String, Path, description = "Bead id")),
    responses(
        (status = 200, description = "Slot moved to Failed"),
        (status = 404, description = "No merge slot for that bead"),
        (status = 409, description = "Illegal transition (not in Merging)"),
        (status = 422, description = "Validation failed (missing reason)"),
    ),
))]
async fn fail_merge(
    State(st): State<MergeApiState>,
    ctx: WorkspaceContext,
    Path(bead): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let repo = st.merges.repo(ctx.workspace().as_str()).await?;
    let cmd: FailMerge = with_path_bead(body, bead.clone())?;
    run_cmd(&*repo, &bead, &cmd).await
}

/// The combined OpenAPI document for the merge REST surface (`hq-fe-api-orch.2`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/merge/...`, so
/// the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_merges,
    get_merge,
    submit_merge,
    start_merge,
    complete_merge,
    fail_merge,
))]
pub struct ApiDoc;

/// Rehydrate the tenant's [`MergeBoard`] from its repository projection (the same
/// decide-against-live-state path the MCP `MergeHandler` takes when it replays the event log).
async fn hydrate(repo: &dyn DynMergeRepository) -> Result<MergeBoard, ApiError> {
    Ok(MergeBoard::from_slots(repo.list_slots().await?))
}

/// Dispatch one board-mutating command, then persist the touched slot.
///
/// `execute` validates against the rehydrated board and applies the state-machine transition (the
/// same path the actor + MCP handler take), returning the produced [`MergeEvent`](crate::MergeEvent);
/// the slot the command touched is then upserted back so the change is durable. The response
/// echoes the emitted event kind so a client can confirm the transition that ran.
async fn run_cmd<C>(
    repo: &dyn DynMergeRepository,
    bead: &str,
    cmd: &C,
) -> Result<Json<Value>, ApiError>
where
    C: Command<State = MergeBoard, Output = crate::MergeEvent>,
{
    let mut board = hydrate(repo).await?;
    let event = cmd.execute(&mut board)?;
    let kind = EventKind::kind(&event);
    let slot = board
        .get(bead)
        .cloned()
        .ok_or_else(|| ApiError(AppError::Other(format!("merge slot {bead} missing after execute"))))?;
    repo.upsert_slot(&slot).await?;
    Ok(Json(json!({ "ok": true, "bead": bead, "event": kind })))
}

/// Deserialize a command struct from a request body. A malformed payload is a `422`, matching the
/// MCP `parse` path.
fn parse_cmd<T: DeserializeOwned>(body: Value) -> Result<T, ApiError> {
    serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))
}

/// Deserialize a path-addressed command body, forcing `bead` to the path segment so it is never
/// trusted from the payload (docs/03 Rule 6). The path bead overwrites any `bead` in the body and
/// supplies it when absent, so a REST client need not repeat the id it already named in the URL.
fn with_path_bead<T: DeserializeOwned>(mut body: Value, bead: String) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert("bead".to_string(), Value::String(bead));
        }
        // A non-object body (e.g. `null` from an empty `start` request) becomes just the bead.
        other => *other = json!({ "bead": bead }),
    }
    parse_cmd(body)
}

/// Shape one merge slot as the REST payload — the same field set the MCP `MergeHandler` emits.
fn slot_json(slot: &MergeSlot) -> Value {
    json!({
        "bead": slot.bead,
        "branch": slot.branch,
        "state": slot.state.as_str(),
    })
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
        // The spec the builder mounts: paths are relative (no `/api/v1/merge`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{bead}", "/{bead}/start", "/{bead}/complete", "/{bead}/fail"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/merge`.
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
    fn with_path_bead_forces_bead_over_body() {
        // The body names a DECOY bead; the path segment must win (docs/03 Rule 6).
        let cmd: CompleteMerge =
            with_path_bead(json!({ "bead": "decoy", "sha": "abc1234" }), "keep".to_string())
                .expect("valid complete");
        assert_eq!(cmd.bead, "keep", "path bead overrides the body");
        assert_eq!(cmd.sha, "abc1234");
    }

    #[test]
    fn with_path_bead_fills_empty_body() {
        // A `start` carries no body fields beyond the path bead.
        let cmd: StartMerge = with_path_bead(Value::Null, "b1".to_string()).expect("valid start");
        assert_eq!(cmd.bead, "b1");
    }

    #[test]
    fn parse_cmd_rejects_malformed() {
        let err = parse_cmd::<SubmitMerge>(json!({ "bead": "b1" })).unwrap_err();
        assert!(matches!(err.0, AppError::Validation(_)));
    }
}
