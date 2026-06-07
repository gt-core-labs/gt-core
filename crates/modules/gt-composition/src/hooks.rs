//! Global Claude Code hook registry REST surface (`hq-hooks`).
//!
//! Exposes `/api/v1/hooks`: list / register / retire the [`gt_claude_hooks`] registry the terminal
//! materialises into a launching session's `.claude/settings.json`. Unlike the per-tenant
//! `/api/v1/skills` surface (mounted as a `GtModule` behind the `WorkspaceContext` extractor), the
//! hook registry is **global** — one set of hooks, each self-scoping via its `HookTarget`. So this
//! router is mounted **directly** (like [`terminal`](crate::terminal)) and authenticates itself,
//! rather than riding the per-workspace module/scope-guard path.
//!
//! ## Security
//!
//! Same verifier as the terminal + SSE feed (RS256 cookie *or* bearer). Reads require the
//! [`READ_SCOPE`] (`hooks.read`), writes the [`WRITE_SCOPE`] (`hooks.write`); the seeded admin's
//! `*` wildcard satisfies both. `hooks.write` is admin-only by convention — never in a non-admin
//! preset (editing a hook changes what every agent session may run). Every rejected request audits
//! a denial into the shared sink, exactly as the terminal upgrade does.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use time::OffsetDateTime;

use gt_auth::JwtClaims;
use gt_claude_hooks::{HookTarget, HooksStore, RegisterHook, RetireHook};
use gt_events::Command;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};

/// The browser-nav JWT cookie (same as the terminal + SSE feed).
const TOKEN_COOKIE: &str = "gt_web_token";
/// Scope a caller must hold (besides `*`) to read the registry.
const READ_SCOPE: &str = "hooks.read";
/// Scope a caller must hold (besides `*`) to mutate the registry — admin-only by convention.
const WRITE_SCOPE: &str = "hooks.write";
/// The superuser grant authorizing every scope (the seeded admin carries it).
const SCOPE_WILDCARD: &str = "*";
/// The route prefix — also the audited path on a denial.
const HOOKS_PATH: &str = "/api/v1/hooks";

/// Everything the hook handlers need: the shared verifier, the audit sink, and the global registry
/// store. Built by the composition root when auth is on.
#[derive(Clone)]
pub struct HooksApiState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    store: Arc<dyn HooksStore>,
}

impl HooksApiState {
    /// Bundle the verifier + audit sink + global store for the hooks router.
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        store: Arc<dyn HooksStore>,
    ) -> Self {
        Self {
            authenticator,
            audit,
            store,
        }
    }
}

/// Build the global hooks REST router (`/api/v1/hooks`) with `state` baked in. The composition root
/// merges this into the top-level app exactly like [`terminal_router`](crate::terminal).
pub fn hooks_router(state: HooksApiState) -> Router {
    Router::new()
        .route(HOOKS_PATH, get(list_hooks).post(register_hook))
        .route(
            &format!("{HOOKS_PATH}/:id"),
            axum::routing::delete(retire_hook),
        )
        .with_state(state)
}

/// Server-stamped wall-clock seconds for a hook event's `now_secs` — never from the request.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `GET /api/v1/hooks` — the whole global registry (`hooks.read`).
async fn list_hooks(State(st): State<HooksApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize(&st, &headers, READ_SCOPE, &Method::GET) {
        return resp;
    }
    match st.store.registry() {
        Ok(reg) => {
            let hooks = reg.hooks();
            (
                StatusCode::OK,
                Json(json!({ "count": hooks.len(), "hooks": hooks })),
            )
                .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `POST /api/v1/hooks` body — the hook to register/replace. `now_secs` is server-stamped. `id` is
/// optional: omitted/empty ⇒ the server auto-generates one (the common "register a new hook" path);
/// supplying an existing id upserts it (the edit path).
#[derive(Debug, Deserialize)]
struct RegisterHookBody {
    #[serde(default)]
    id: String,
    event: String,
    #[serde(default)]
    matcher: String,
    command: String,
    #[serde(default)]
    target: HookTarget,
}

/// Auto-generate a readable, unique-enough hook id from the event + a wall-clock nanosecond nonce
/// (`hq-hooks`: ids are server-generated, not user-typed). Shape `hook-<event-lc>-<8 hex>`, all
/// lowercase ASCII alnum + `-`, so it passes `validate_hook_id`.
fn generate_hook_id(event: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let secs = now_secs();
    let event_slug: String = event
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    format!(
        "hook-{event_slug}-{:08x}",
        secs.wrapping_mul(1_000_000_000).wrapping_add(nanos as u64) as u32
    )
}

/// `POST /api/v1/hooks` — register (or replace) a hook (`hooks.write`). Validates the shape (id /
/// event / non-empty command) via the pure [`RegisterHook`] command, then persists
/// `hooks.registered.v1`. Shape errors are `422`.
async fn register_hook(
    State(st): State<HooksApiState>,
    headers: HeaderMap,
    Json(body): Json<RegisterHookBody>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        return resp;
    }
    let mut reg = match st.store.registry() {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    // Auto-generate the id when the client didn't supply one (the default "new hook" path).
    let id = if body.id.trim().is_empty() {
        generate_hook_id(&body.event)
    } else {
        body.id
    };
    let cmd = RegisterHook {
        id,
        event: body.event,
        matcher: body.matcher,
        command: body.command,
        target: body.target,
        now_secs: now_secs(),
    };
    let event = match cmd.execute(&mut reg) {
        Ok(e) => e,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };
    if let Err(e) = st.store.append(event) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    (StatusCode::CREATED, Json(json!({ "id": cmd.id }))).into_response()
}

/// `DELETE /api/v1/hooks/:id` — retire a hook (`hooks.write`). `404` when no such hook.
async fn retire_hook(
    State(st): State<HooksApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::DELETE) {
        return resp;
    }
    let mut reg = match st.store.registry() {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    if reg.get(&id).is_none() {
        return (
            StatusCode::NOT_FOUND,
            format!("no hook registered under id `{id}`"),
        )
            .into_response();
    }
    let cmd = RetireHook {
        id,
        now_secs: now_secs(),
    };
    let event = match cmd.execute(&mut reg) {
        Ok(e) => e,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };
    if let Err(e) = st.store.append(event) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    (StatusCode::OK, Json(json!({ "retired": cmd.id }))).into_response()
}

/// Verify the token (cookie or bearer), gate its clock/workspace invariants, and require `scope`
/// (or `*`). Every failure audits a denial and returns the response to send. `Ok` on success.
fn authorize(
    st: &HooksApiState,
    headers: &HeaderMap,
    scope: &'static str,
    method: &Method,
) -> Result<JwtClaims, Response> {
    let reject = |status: StatusCode, msg: &'static str| -> Response {
        record_denial(
            st.audit.as_ref(),
            ANONYMOUS,
            None,
            method,
            &HOOKS_PATH.parse().expect("static uri"),
            Some(scope),
            status,
        );
        (status, msg).into_response()
    };

    let token = bearer(headers)
        .or_else(|| cookie(headers, TOKEN_COOKIE))
        .ok_or_else(|| {
            reject(
                StatusCode::UNAUTHORIZED,
                "missing gt_web_token cookie or bearer",
            )
        })?;

    let claims = st
        .authenticator
        .authenticate(&token)
        .map_err(|_| reject(StatusCode::UNAUTHORIZED, "invalid token"))?;

    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims
        .validate(now, JwtClaims::workspace_optional_from_env())
        .is_err()
    {
        return Err(reject(
            StatusCode::UNAUTHORIZED,
            "expired or incomplete token",
        ));
    }
    if !has_scope(&claims.scopes, scope) {
        return Err(reject(StatusCode::FORBIDDEN, "missing hooks scope"));
    }
    Ok(claims)
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
}

/// The bearer token from `Authorization: Bearer <jwt>`, trimmed + non-empty.
fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?;
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// Value of a named cookie from the `Cookie` header.
fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())?
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| k.trim() == name)
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_scope_accepts_exact_and_wildcard() {
        assert!(has_scope(&["hooks.write".into()], WRITE_SCOPE));
        assert!(has_scope(&["*".into()], WRITE_SCOPE));
        assert!(has_scope(&["hooks.read".into()], READ_SCOPE));
        assert!(!has_scope(&["hooks.read".into()], WRITE_SCOPE));
        assert!(!has_scope(&[], READ_SCOPE));
    }

    #[test]
    fn generated_id_is_valid_and_event_tagged() {
        let id = generate_hook_id("PreToolUse");
        assert!(id.starts_with("hook-pretooluse-"));
        // The generated id must satisfy the domain's validator (lowercase alnum + '-', bounded).
        assert!(
            gt_claude_hooks::validate_hook_id(&id).is_ok(),
            "{id} should be a valid hook id"
        );
    }

    #[test]
    fn bearer_strips_prefix() {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer abc.def".parse().unwrap(),
        );
        assert_eq!(bearer(&h).as_deref(), Some("abc.def"));
    }

    #[test]
    fn cookie_extracts_named_value() {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::COOKIE,
            "a=1; gt_web_token=xyz".parse().unwrap(),
        );
        assert_eq!(cookie(&h, TOKEN_COOKIE).as_deref(), Some("xyz"));
    }
}
