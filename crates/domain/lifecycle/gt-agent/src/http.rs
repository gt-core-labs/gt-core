//! The `axum` REST adapter for the `agent.*` session-lifecycle surface (`hq-fe-api-orch.1`).
//!
//! Copies the gt-issues template (`hq-auth-routes.2`): it exposes the agent/session substrate as
//! REST routes that dispatch to the **same** event-log replay+append the MCP `agent.*` tools use
//! — the pure [`SessionRegistry::apply`](crate::SessionRegistry) reducer folded over the workspace
//! [`AgentEvent`](crate::AgentEvent) stream — so no domain logic is duplicated, only the wire shape
//! differs. The dispatch body is ported verbatim from the MCP `AgentHandler`
//! (`gt-composition/src/mcp/agent.rs`): replay the workspace log into a registry, run the light
//! existence check, then append the lifecycle event. It folds into `gt-agent` behind the
//! off-by-default `axum` feature (docs/03 Rule 4), exactly as gt-issues/gt-documents gate theirs,
//! rather than living in a sibling crate.
//!
//! ## Why the event facet (no projection table)
//!
//! gt-agent's sessions are event-sourced: the registry keeps no table and is rebuilt by folding
//! the [`AgentEvent`] stream. So like merge/convoy, the durable backing is the per-workspace
//! event log ([`gt_eventlog`]), not Postgres. A request is therefore symmetric to the
//! store-backed adapters' hydrate → execute → persist, with the log standing in for the table:
//! [`replay`] the workspace log into a [`SessionRegistry`], decide, then append the event.
//!
//! ## What it does *not* do
//!
//! - **It does not authenticate or authorize.** The builder mounts this router under
//!   `/api/v1/agent` and wraps it with the capability-derived scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)); the composition root layers the
//!   auth middleware in front. A handler here only runs once the caller already holds
//!   `agent.read` / `agent.write` for the request's verb-class.
//! - **It never reads the workspace from the path or body** (docs/04 §15, docs/03 Rule 6). The
//!   tenant is the server-injected `X-Workspace` header (resolved upstream from the auth context),
//!   exactly as the MCP dispatch keys on it. The session id is a plain resource identifier on the
//!   path; the workspace is never a route parameter or a body field.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use gt_eventlog::{replay, EventRecord, EventStore, JsonlWriter};
use gt_events::{AppError, Envelope, EventKind};

use crate::state::{Session, SessionRegistry, SessionRole, SessionState};
use crate::AgentEvent;

/// The event-log kind prefix for every agent event (`agent.*`) — the filter that keeps the
/// heterogeneous log's foreign kinds out of the typed [`AgentEvent`] reducer.
const NS: &str = "agent.";

/// The header carrying the request's tenant — server-injected by the auth/workspace middleware,
/// never read from a URL or body field (the same `X-Workspace` convention the MCP dispatch keys
/// on).
const WORKSPACE_HEADER: &str = "x-workspace";

/// The workspace a request resolves to when it carries no `X-Workspace` header — the
/// single-tenant default (matches the MCP dispatch / event-log default).
const DEFAULT_WORKSPACE: &str = "default";

/// Everything the agent REST handlers need, baked into the router with [`Router::with_state`]
/// before it leaves [`agent_router`] so the merged application router carries no outstanding
/// state type (the kernel's state-erased `Router<()>` contract).
///
/// The only handle is the event-log root volume: the per-workspace partition is resolved per
/// request (so a single state serves every tenant), exactly as the MCP `EventLog` does. There is
/// no store — the registry is replayed from the log on every call.
#[derive(Clone, Debug)]
pub struct AgentApiState {
    /// The event-log root the workspace partitions live under (`<root>/<ws>/`). The binary
    /// supplies the production volume; the contract test points it at a tempdir.
    root: Arc<PathBuf>,
    /// Optional orchd dispatch channel dir. When `Some`, a polecat spawn with a crew bead drops a
    /// `{bead,priority}` message here so the orchd scheduler actually slings the agent. `None` ⇒
    /// the event is still recorded but orchd is not notified (GT_CHANNEL_ROOT unset, tests, or
    /// the MCP server surface where the channel dir is not wired).
    dispatch_channel: Option<Arc<PathBuf>>,
}

impl AgentApiState {
    /// Build the REST state over the event-log root volume (the same root the MCP `EventLog`
    /// uses). The binary calls this and hands the module the result via
    /// [`AgentModule::with_http`](crate::AgentModule::with_http).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: Arc::new(root.into()), dispatch_channel: None }
    }

    /// Wire the orchd dispatch channel dir so a polecat spawn auto-drops a dispatch request
    /// (hq-agent-auto-dispatch.1). Mirrors the `ConvoyHandler::with_dispatch_channel` pattern.
    pub fn with_dispatch_channel(mut self, channel_dir: PathBuf) -> Self {
        self.dispatch_channel = Some(Arc::new(channel_dir));
        self
    }

    /// The workspace's append-only log (segments under `<root>/<ws>/`).
    fn store(&self, workspace: Option<&str>) -> Result<JsonlWriter, AppError> {
        JsonlWriter::for_workspace_in(self.root.as_path(), workspace.unwrap_or(DEFAULT_WORKSPACE))
    }

    /// Rebuild the session registry by folding the workspace's `agent.*` events — the same
    /// replay the MCP handler runs (`replay_domain` with the `agent.` prefix + the pure reducer).
    fn registry(&self, workspace: Option<&str>) -> Result<SessionRegistry, AppError> {
        let records: Vec<EventRecord> = self
            .store(workspace)?
            .read_all()?
            .into_iter()
            .filter(|r| r.kind.starts_with(NS))
            .collect();
        replay(&records, SessionRegistry::default(), SessionRegistry::apply)
    }

    /// Append one decided lifecycle event to the workspace log + echo the dispatch payload (the
    /// `{ ok, session, event }` shape the MCP `record` returns).
    fn record(&self, workspace: Option<&str>, event: AgentEvent, session: &str) -> Result<Value, AppError> {
        let kind = event.kind().to_string();
        let record = EventRecord::from_envelope(&Envelope::root(event))?;
        self.store(workspace)?.append(&record)?;
        Ok(json!({ "ok": true, "session": session, "event": kind }))
    }

    /// Resolve a session that must already exist in the rehydrated registry — a heartbeat/end/kill
    /// on an unknown session is a not-found, exactly as the MCP `require_session` check.
    fn require_session(&self, workspace: Option<&str>, id: &str) -> Result<(), AppError> {
        if self.registry(workspace)?.get(id).is_none() {
            return Err(AppError::NotFound(format!("session {id}")));
        }
        Ok(())
    }
}

/// Build the agent REST router with `state` baked in (`hq-fe-api-orch.1`).
///
/// The paths are **relative**: the builder nests them under `/api/v1/agent` and applies the scope
/// guard. `register_routes` on [`AgentModule`](crate::AgentModule) returns exactly this router
/// when the module carries HTTP state.
///
/// | Method + path        | Maps to MCP tool   |
/// |----------------------|--------------------|
/// | `GET /`              | `agent.list`       |
/// | `GET /:id`           | `agent.info`       |
/// | `POST /`             | `agent.spawn`      |
/// | `POST /:id/heartbeat`| `agent.heartbeat`  |
/// | `POST /:id/end`      | `agent.end`        |
/// | `POST /:id/kill`     | `agent.kill`       |
pub fn agent_router(state: AgentApiState) -> Router {
    Router::new()
        .route("/", get(list_sessions).post(spawn_session))
        .route("/:id", get(get_session))
        .route("/:id/heartbeat", post(heartbeat_session))
        .route("/:id/end", post(end_session))
        .route("/:id/kill", post(kill_session))
        .with_state(state)
}

/// `agent.spawn` body: a session id + rig, with an optional role/crew (the `Spawned` event's own
/// defaults apply when omitted). The session id names the new resource, so — like an issues
/// create — it rides in the body, not the path.
#[derive(Debug, Deserialize)]
struct SpawnArgs {
    /// The new session id (the resource being created).
    session: String,
    /// The rig the session runs on.
    rig: String,
    /// Agent kind (mayor/dog/polecat); defaults to polecat for an omitted role.
    #[serde(default)]
    role: SessionRole,
    /// The claude crew running inside a polecat; only meaningful for polecats.
    #[serde(default)]
    crew: Option<String>,
}

/// `agent.kill` body: the reason the session was forcibly killed.
#[derive(Debug, Deserialize)]
struct KillArgs {
    /// Why the session was killed (recorded on the `Killed` event).
    reason: String,
}

/// `?rig=` filter for `GET /` (hq-rig-isolation.2). Absent ⇒ workspace-wide (back-compat).
#[derive(Debug, Default, Deserialize)]
struct ListSessionsQuery {
    rig: Option<String>,
}

/// `GET /` — every agent session in the workspace (`agent.list`).
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/",
    params(("rig" = Option<String>, Query, description = "Narrow to sessions with this rig")),
    responses((status = 200, description = "Agent sessions, optionally filtered by rig")),
))]
async fn list_sessions(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Query(q): Query<ListSessionsQuery>,
) -> Result<Json<Value>, ApiError> {
    let reg = st.registry(workspace_of(&headers).as_deref())?;
    let sessions: Vec<_> = reg
        .snapshot()
        .iter()
        .filter(|s| q.rig.as_deref().map_or(true, |r| s.rig == r))
        .map(session_json)
        .collect();
    Ok(Json(json!({ "sessions": sessions })))
}

/// `GET /:id` — one session's state (`agent.info`); `404` when no session matches.
#[cfg_attr(feature = "axum", utoipa::path(
    get, path = "/{id}",
    params(("id" = String, Path, description = "Session id")),
    responses(
        (status = 200, description = "The session's state"),
        (status = 404, description = "No session with that id"),
    ),
))]
async fn get_session(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    match st.registry(workspace_of(&headers).as_deref())?.get(&id) {
        Some(s) => Ok(Json(session_json(s))),
        None => Err(ApiError(AppError::NotFound(format!("session {id}")))),
    }
}

/// `POST /` — record a new agent session (`agent.spawn`). Emits `agent.spawned.v1`. A re-spawn of
/// an id already in the registry is a `422` (mirrors the MCP duplicate check). `201` on success.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/",
    responses(
        (status = 201, description = "Session spawned"),
        (status = 422, description = "A session with that id already exists"),
    ),
))]
async fn spawn_session(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Json(a): Json<SpawnArgs>,
) -> Result<Response, ApiError> {
    let ws = workspace_of(&headers);
    let ws = ws.as_deref();
    if st.registry(ws)?.get(&a.session).is_some() {
        return Err(ApiError(AppError::Validation(format!("session {} already exists", a.session))));
    }
    let session = a.session.clone();
    let role = a.role;
    let crew = a.crew.clone();
    let body = st.record(
        ws,
        AgentEvent::Spawned {
            session: a.session,
            rig: a.rig,
            role,
            crew: a.crew,
            // A manual UI spawn has no worktree manifest (hq-orch-sessions.2).
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: role.maintains_heartbeat(),
            tmux_socket: role.tmux_socket(ws.unwrap_or("default")),
            spawned_by: Some("ui".into()),
        },
        &session,
    )?;
    // Auto-dispatch: a polecat spawn with a crew bead drops a dispatch request on the orchd
    // channel so the scheduler actually slings the agent (hq-agent-auto-dispatch.1). Role
    // defaults to Polecat; mayors and dogs are interactive and don't go through the queue.
    if role == SessionRole::Polecat {
        if let (Some(bead), Some(channel)) = (&crew, st.dispatch_channel.as_ref().map(|p| p.as_path())) {
            drop_dispatch_event(channel, bead, 1);
        }
    }
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

/// `POST /:id/heartbeat` — record a liveness heartbeat (`agent.heartbeat`). Emits
/// `agent.heartbeat.v1`; a no-op on the registry but accepted + logged. `404` on an unknown id.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/heartbeat",
    params(("id" = String, Path, description = "Session id")),
    responses(
        (status = 200, description = "Heartbeat recorded"),
        (status = 404, description = "No session with that id"),
    ),
))]
async fn heartbeat_session(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let ws = ws.as_deref();
    st.require_session(ws, &id)?;
    let timestamp_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .ok();
    Ok(Json(st.record(ws, AgentEvent::Heartbeat { session: id.clone(), timestamp_secs }, &id)?))
}

/// `POST /:id/end` — record a session's normal end (`agent.end`). Emits `agent.session-end.v1`,
/// folding the session to `done`. `404` on an unknown id.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/end",
    params(("id" = String, Path, description = "Session id")),
    responses(
        (status = 200, description = "Session ended"),
        (status = 404, description = "No session with that id"),
    ),
))]
async fn end_session(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let ws = ws.as_deref();
    st.require_session(ws, &id)?;
    Ok(Json(st.record(ws, AgentEvent::SessionEnd { session: id.clone() }, &id)?))
}

/// `POST /:id/kill` — record a session forcibly killed with a reason (`agent.kill`). Emits
/// `agent.killed.v1`, folding the session to `killed`. `404` on an unknown id; a missing `reason`
/// is a `422`.
#[cfg_attr(feature = "axum", utoipa::path(
    post, path = "/{id}/kill",
    params(("id" = String, Path, description = "Session id")),
    responses(
        (status = 200, description = "Session killed"),
        (status = 404, description = "No session with that id"),
        (status = 422, description = "Missing kill reason"),
    ),
))]
async fn kill_session(
    State(st): State<AgentApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(a): Json<KillArgs>,
) -> Result<Json<Value>, ApiError> {
    let ws = workspace_of(&headers);
    let ws = ws.as_deref();
    st.require_session(ws, &id)?;
    let body = st.record(ws, AgentEvent::Killed { session: id.clone(), reason: a.reason }, &id)?;
    // Terminate the tmux session so the Claude process is cleaned up immediately.
    // Only fires when the operator explicitly calls the kill endpoint — WS disconnect does not kill.
    let server = format!("gt-{}", ws.unwrap_or("default"));
    let _ = std::process::Command::new("tmux")
        .args(["-L", &server, "kill-session", "-t", id.as_str()])
        .status();
    Ok(Json(body))
}

/// The combined OpenAPI document for the agent REST surface (`hq-fe-api-orch.1`). The builder
/// mounts it under the module prefix and rewrites its relative paths to `/api/v1/agent/...`, so
/// the `#[utoipa::path]` annotations stay prefix-free.
#[derive(utoipa::OpenApi)]
#[openapi(paths(
    list_sessions,
    get_session,
    spawn_session,
    heartbeat_session,
    end_session,
    kill_session,
))]
pub struct ApiDoc;

/// Drop a `{bead,priority}` dispatch request into the orchd channel dir (atomic write: tmp →
/// rename so the watcher never sees a partial file). Best-effort — a failure is logged but
/// never propagates to the caller (the spawn event is already durable at this point).
fn drop_dispatch_event(channel: &std::path::Path, bead: &str, priority: u8) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let id = format!("{ts:016x}{:08x}{seq:08x}", std::process::id());
    let tmp = channel.join(format!(".{id}.tmp"));
    let final_ = channel.join(format!("{id}.event"));
    let payload = serde_json::json!({"bead": bead, "priority": priority}).to_string();
    if let Err(e) = std::fs::create_dir_all(channel)
        .and_then(|_| std::fs::write(&tmp, payload.as_bytes()))
        .and_then(|_| std::fs::rename(&tmp, &final_))
    {
        eprintln!("[agent] dispatch bridge: write failed for {bead} — {e}");
    }
}

/// Stable spelling of a session state (matches the MCP dispatch payload).
fn state_str(state: SessionState) -> &'static str {
    match state {
        SessionState::Spawned => "spawned",
        SessionState::Working => "working",
        SessionState::Done => "done",
        SessionState::Killed => "killed",
    }
}

/// Shape one session as the dispatch payload (identical to the MCP `session_json`).
fn session_json(session: &Session) -> Value {
    json!({
        "id": session.id,
        "rig": session.rig,
        "state": state_str(session.state),
        "role": session.role.as_str(),
        "crew": session.crew,
    })
}

/// The request's tenant from the server-injected `X-Workspace` header, or `None` (the default
/// workspace) when absent/blank. Never read from the path or body (docs/04 §15).
fn workspace_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(WORKSPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// HTTP wrapper over the domain [`AppError`] so a handler can `?`-propagate it and have it
/// rendered with the right status. The body is the bare error message — the same text the MCP
/// path surfaces — so a client sees an identical reason across transports.
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
        // The spec the builder mounts: paths are relative (no `/api/v1/agent`), so nesting can
        // rewrite them. Every declared route must be present so the combined document is complete.
        let doc = ApiDoc::openapi();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        for expected in ["/", "/{id}", "/{id}/heartbeat", "/{id}/end", "/{id}/kill"] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // Prefix-free: the module builder, not the annotation, owns `/api/v1/agent`.
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
    fn session_json_uses_flat_state_and_role_spellings() {
        let s = Session::with_role("p1", "granite", SessionRole::Polecat, Some("ada".into()));
        let v = session_json(&s);
        assert_eq!(v["id"], "p1");
        assert_eq!(v["rig"], "granite");
        assert_eq!(v["state"], "spawned");
        assert_eq!(v["role"], "polecat");
        assert_eq!(v["crew"], "ada");
    }

    #[test]
    fn workspace_header_blank_is_default() {
        let mut h = HeaderMap::new();
        assert_eq!(workspace_of(&h), None);
        h.insert(WORKSPACE_HEADER, "".parse().unwrap());
        assert_eq!(workspace_of(&h), None, "blank header ⇒ default workspace");
        h.insert(WORKSPACE_HEADER, "acme".parse().unwrap());
        assert_eq!(workspace_of(&h).as_deref(), Some("acme"));
    }
}
