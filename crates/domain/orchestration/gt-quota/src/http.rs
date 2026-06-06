//! The `axum` REST adapter for the `quota.*` surface (`hq-fe-api-orch.4`).
//!
//! The orchestration sibling of the issues (`hq-auth-routes.2`) and rig
//! (`hq-fe-api-platform.2`) REST surfaces: it exposes the quota registry as REST routes that
//! dispatch to the **same** [`QuotaCommand`](crate::QuotaCommand) decide/apply layer the MCP
//! `quota.*` tools use — no domain logic is duplicated, only the wire shape differs. It folds
//! into `gt-quota` behind the off-by-default `axum` feature (docs/03 Rule 4), exactly as
//! gt-issues/gt-rig/gt-workspace gate theirs, rather than living in a sibling crate.
//!
//! ## Event-sourced, per-workspace, never from the path
//!
//! Quota keeps **no projection table**: its state is a log of [`QuotaEvent`](crate::QuotaEvent)
//! replayed through the [`QuotaState`](crate::QuotaState) reducer (docs/06 Step-3 gate). Every
//! call therefore rehydrates the [`AccountRegistry`] from the workspace's event log, executes
//! the command against it, and appends the produced event back — the exact read-modify-append
//! the MCP `QuotaHandler` runs, with the [`WorkspaceQuota`] provider standing in for the
//! handler's `EventLog`.
//!
//! The log is **per-tenant** (path-partitioned per workspace, docs/04 §15). The workspace comes
//! from the [`WorkspaceContext`](gt_workspace::WorkspaceContext) extractor (the JWT claim /
//! sanctioned header), **never** from the URL or body (docs/03 Rule 6). The account id is a
//! plain resource identifier carried in the path; the tenant a request belongs to is resolved
//! upstream from the auth context.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/quota` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `quota.read` / `quota.write` for the request's verb-class.
//! - **It records no per-workspace `consumed` metric.** The `gt_workspace_quota_consumed`
//!   counter the MCP `quota.sample` path bumps is a composition-edge telemetry concern (it lives
//!   in `gt-composition`, not the domain tier); the REST adapter, like the rig one, carries no
//!   telemetry — the same degraded mode a no-`gt_telemetry::init` server runs in.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

use crate::commands::{ProbeWindow, RegisterAccount, RetireAccount, RotateAccount, SampleTokens};
use crate::events::QuotaEvent;
use crate::state::{Account, AccountRegistry};

/// A per-workspace quota event-log provider for the REST adapter.
///
/// Quota is event-sourced, so a request must replay the caller's own log into the registry,
/// execute the command, and append the produced event back. The binary supplies an
/// implementation that, given the resolved workspace slug, rehydrates the registry and appends
/// to that tenant's log (the composition root wraps its `EventLog`); a test supplies an
/// in-memory one. This is the REST mirror of the MCP `QuotaHandler`'s `EventLog` — the
/// composition root owns the storage policy, the adapter only asks for "the registry for *this*
/// workspace" and "append this event to it".
#[async_trait]
pub trait WorkspaceQuota: Send + Sync {
    /// Rebuild the account registry by replaying `workspace`'s quota event log (the slug from
    /// the auth context, never the path) — the same projection the MCP handler's `registry()`
    /// bridge produces via [`AccountRegistry::from_state`](crate::AccountRegistry::from_state).
    async fn registry(&self, workspace: &str) -> Result<AccountRegistry, AppError>;

    /// Append one decided quota event to `workspace`'s log, closing the read-modify-append.
    async fn append(&self, workspace: &str, event: QuotaEvent) -> Result<(), AppError>;
}

/// Everything the quota REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`quota_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
#[derive(Clone)]
pub struct QuotaApiState {
    /// The per-workspace event-log provider the binary supplies — the module owns no store of
    /// its own, exactly as the MCP `QuotaHandler` owns only its `EventLog`.
    quota: Arc<dyn WorkspaceQuota>,
}

impl QuotaApiState {
    /// Build the REST state over a per-workspace event-log provider.
    pub fn new(quota: Arc<dyn WorkspaceQuota>) -> Self {
        Self { quota }
    }
}

/// Build the quota REST router with `state` baked in (`hq-fe-api-orch.4`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/quota` and applies the
/// scope guard. `register_routes` on the HTTP-enabled quota module returns exactly this router
/// when it carries HTTP state.
///
/// | Method + path             | Maps to MCP tool |
/// |---------------------------|------------------|
/// | `GET /`                   | `quota.list`     |
/// | `GET /:account`           | `quota.info`     |
/// | `POST /:account/sample`   | `quota.sample`   |
/// | `POST /:account/probe`    | `quota.probe`    |
/// | `POST /:account/rotate`   | `quota.rotate`   |
/// | `POST /account`           | `quota.register` |
/// | `DELETE /:account`        | `quota.retire`   |
pub fn quota_router(state: QuotaApiState) -> Router {
    Router::new()
        .route("/", get(list_accounts))
        // `/account` (singular) is the onboarding collection POST — distinct from `/:account` so the
        // register body carries the id, not the path (the account does not exist yet).
        .route("/account", post(register_account))
        .route("/:account", get(get_account).delete(retire_account))
        .route("/:account/sample", post(sample_account))
        .route("/:account/probe", post(probe_account))
        .route("/:account/rotate", post(rotate_account))
        .with_state(state)
}

/// `GET /` — every tracked account with its usage (`quota.list`). Reuses the replayed registry
/// so the projection matches the MCP read exactly.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    responses((status = 200, description = "Every tracked account with its usage + window")),
))]
async fn list_accounts(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
) -> Result<Json<Value>, ApiError> {
    let registry = st.quota.registry(ctx.workspace().as_str()).await?;
    Ok(Json(json!({
        "accounts": registry.accounts().map(account_json).collect::<Vec<_>>(),
    })))
}

/// `GET /:account` — one account's usage + window (`quota.info`); `404` when no account matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{account}",
    params(("account" = String, Path, description = "Account id")),
    responses(
        (status = 200, description = "The account's usage + window"),
        (status = 404, description = "No account with that id"),
    ),
))]
async fn get_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Path(account): Path<String>,
) -> Result<Json<Value>, ApiError> {
    match st.quota.registry(ctx.workspace().as_str()).await?.get(&account) {
        Some(acc) => Ok(Json(account_json(acc))),
        None => Err(ApiError(AppError::NotFound(format!("account {account}")))),
    }
}

/// `POST /:account/sample` — record a token-usage sample (`quota.sample`). The account always
/// comes from the path and overwrites any `account` in the body (docs/03 Rule 6); the rest of
/// the body (`session`/`model`/usage counters) is the sample. Emits `quota.tokens_sampled.v1`.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{account}/sample",
    params(("account" = String, Path, description = "Account id")),
    responses(
        (status = 200, description = "Sample recorded; echoes the emitted event kind"),
        (status = 422, description = "Validation failed (empty session/model)"),
    ),
))]
async fn sample_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Path(account): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: SampleTokens = with_path_field(body, "account", account)?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `POST /:account/probe` — reconcile the live window against provider remaining/reset
/// (`quota.probe`). The account comes from the path. Emits `quota.usage_probed.v1`.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{account}/probe",
    params(("account" = String, Path, description = "Account id")),
    responses(
        (status = 200, description = "Window reconciled; echoes the emitted event kind"),
        (status = 422, description = "Validation failed"),
    ),
))]
async fn probe_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Path(account): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: ProbeWindow = with_path_field(body, "account", account)?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `POST /:account/rotate` — rotate active usage off this account onto another (`quota.rotate`),
/// parking the source in cooldown. The path account is the `from_account` (the one rotated away
/// from, docs/03 Rule 6); `to_account` is the healthy target in the body. A rotation onto itself
/// is a `422`. Emits `quota.rotated.v1`.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{account}/rotate",
    params(("account" = String, Path, description = "Account rotated away from (from_account)")),
    responses(
        (status = 200, description = "Rotated; echoes the emitted event kind"),
        (status = 422, description = "Validation failed (empty or self-rotation)"),
    ),
))]
async fn rotate_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Path(account): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let cmd: RotateAccount = with_path_field(body, "from_account", account)?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `POST /account` — onboard a claude account (`quota.register`, `hq-quota-accounts.4`). Body
/// carries `{account, config_dir}`; the id is in the body (not the path) because the account does
/// not exist yet. Emits `AccountRegistered`. The edge sanitizes `config_dir` (composition
/// `account_dirs`) before this; the domain only checks it is non-empty.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/account",
    responses(
        (status = 200, description = "Account onboarded; emits quota.account_registered"),
        (status = 422, description = "Empty account or config_dir"),
    ),
))]
async fn register_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Json(mut body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    // No path id to force; just stamp the server clock when the caller omits it.
    if let Value::Object(map) = &mut body {
        map.entry("now_secs").or_insert_with(|| json!(now_secs()));
    }
    let cmd: RegisterAccount = serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))?;
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// `DELETE /:account` — retire an account from the rotation pool (`quota.retire`,
/// `hq-quota-accounts.4`). Emits `AccountDeregistered`; idempotent.
#[cfg_attr(feature = "axum", utoipa::path(
    delete, path = "/{account}",
    params(("account" = String, Path, description = "Account id")),
    responses((status = 200, description = "Account retired; emits quota.account_deregistered")),
))]
async fn retire_account(
    State(st): State<QuotaApiState>,
    ctx: WorkspaceContext,
    Path(account): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let cmd = RetireAccount {
        account,
        now_secs: now_secs(),
    };
    run(&st, ctx.workspace().as_str(), cmd).await
}

/// Replay → execute → append: the REST mirror of the MCP `QuotaHandler::run`.
///
/// Rehydrates the registry from the workspace log, runs the command's decide/apply against it
/// (the same `Command::execute` the actor and MCP tools take), and appends the produced event.
/// The `200` body echoes the emitted event kind, exactly like the MCP path.
async fn run<C>(st: &QuotaApiState, workspace: &str, cmd: C) -> Result<Json<Value>, ApiError>
where
    C: Command<State = AccountRegistry, Output = QuotaEvent>,
{
    let mut registry = st.quota.registry(workspace).await?;
    let event = cmd.execute(&mut registry)?;
    let kind = event.kind().to_string();
    st.quota.append(workspace, event).await?;
    Ok(Json(json!({ "ok": true, "event": kind })))
}

/// The combined OpenAPI document for the quota REST surface (`hq-fe-api-orch.4`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/quota/...`, so
/// the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_accounts,
    get_account,
    sample_account,
    probe_account,
    rotate_account,
    register_account,
    retire_account,
))]
pub struct ApiDoc;

/// Shape one account (id, status, window) as the REST payload. `Account` derives `Serialize`, so
/// the projection is the domain type verbatim — the same shape the MCP `QuotaHandler` emits.
fn account_json(account: &Account) -> Value {
    serde_json::to_value(account).unwrap_or_else(|_| json!({}))
}

/// Deserialize a path-addressed command body, forcing `field` to the path segment so the target
/// account is never trusted from the payload (docs/03 Rule 6). The path value overwrites any
/// same-named key in the body and supplies it when absent; `now_secs` is then stamped with the
/// server clock when the caller omits it (the clock is the edge's to supply, not the model's),
/// mirroring the MCP `parse_cmd`. A malformed payload is a `422`, matching the MCP path.
fn with_path_field<T: DeserializeOwned>(
    mut body: Value,
    field: &str,
    value: String,
) -> Result<T, ApiError> {
    match &mut body {
        Value::Object(map) => {
            map.insert(field.to_string(), Value::String(value));
            map.entry("now_secs").or_insert_with(|| json!(now_secs()));
        }
        // A non-object body (e.g. `null` from an empty request) becomes just the path field.
        other => *other = json!({ field: value, "now_secs": now_secs() }),
    }
    serde_json::from_value(body)
        .map_err(|e| ApiError(AppError::Validation(format!("invalid arguments: {e}"))))
}

/// Server-side epoch-seconds clock for command timestamps.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        // The spec the builder mounts: paths are relative (no `/api/v1/quota`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{account}", "/{account}/sample", "/{account}/probe", "/{account}/rotate"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/quota`.
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
    fn with_path_field_forces_account_over_body_and_stamps_now() {
        // The body names a DECOY account; the path segment must win (docs/03 Rule 6), and the
        // server stamps `now_secs` when the caller omits it.
        let cmd: ProbeWindow = with_path_field(
            json!({ "account": "decoy", "remaining": 250, "resets_at_secs": 20_000 }),
            "account",
            "keep".to_string(),
        )
        .expect("valid probe");
        assert_eq!(cmd.account, "keep", "path account overrides the body");
        assert!(cmd.now_secs > 0, "server stamped a clock");
    }

    #[test]
    fn with_path_field_maps_rotate_from_account_to_path() {
        // `rotate` keys the path segment onto `from_account` (the one rotated away from).
        let cmd: RotateAccount = with_path_field(
            json!({ "to_account": "acc-2" }),
            "from_account",
            "acc-1".to_string(),
        )
        .expect("valid rotate");
        assert_eq!(cmd.from_account, "acc-1");
        assert_eq!(cmd.to_account, "acc-2");
    }

    #[test]
    fn with_path_field_rejects_malformed() {
        // A `sample` missing required usage counters is a validation fault, matching MCP parse.
        let err = with_path_field::<SampleTokens>(
            json!({ "session": "s1" }),
            "account",
            "acc-1".to_string(),
        )
        .unwrap_err();
        assert!(matches!(err.0, AppError::Validation(_)));
    }
}
