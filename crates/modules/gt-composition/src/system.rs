//! System-level configuration REST surface (`hq-system-config`).
//!
//! Exposes `/api/v1/system/config` (GET/PUT) and `/api/v1/system/archive/run` (POST).
//! Config is persisted to `GT_SYSTEM_CONFIG_PATH` (default `<GT_EVENTLOG_ROOT>/../system_config.json`)
//! so it survives process restarts. If the file is absent the built-in defaults apply.
//!
//! The background archive ticker ([`ArchiveDaemon::run`]) reads the shared config on each
//! interval tick so a PUT takes effect on the next cycle without a restart.
//!
//! ## Scopes
//!
//! - `system.read`  — read the current config.
//! - `system.write` — update config or trigger a manual run. Admin (`*`) satisfies both.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::RwLock;

use gt_auth::JwtClaims;
use gt_store_dolt::DoltIssues;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};

const TOKEN_COOKIE: &str = "gt_web_token";
const READ_SCOPE: &str = "system.read";
const WRITE_SCOPE: &str = "system.write";
const SCOPE_WILDCARD: &str = "*";
const SYSTEM_PATH: &str = "/api/v1/system";

/// Archive daemon configuration. All fields use sensible defaults so an empty config is valid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    /// Whether the background sweep is active.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Closed issues older than this many days are eligible for archival.
    #[serde(default = "default_archive_days")]
    pub archive_after_days: u32,
    /// How often the daemon runs, in minutes.
    #[serde(default = "default_interval_minutes")]
    pub interval_minutes: u64,
}

fn default_true() -> bool { true }
fn default_archive_days() -> u32 { 30 }
fn default_interval_minutes() -> u64 { 60 }

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            archive_after_days: default_archive_days(),
            interval_minutes: default_interval_minutes(),
        }
    }
}

/// Partial update body — every field is optional so a PUT can change individual knobs.
#[derive(Debug, Deserialize)]
struct ArchiveConfigPatch {
    enabled: Option<bool>,
    archive_after_days: Option<u32>,
    interval_minutes: Option<u64>,
}

/// Shared handle to the live config: written by PUT and read by the daemon on each tick.
pub type SharedArchiveConfig = Arc<RwLock<ArchiveConfig>>;

/// Load config from disk at `path`; returns `Default` when the file is absent or malformed.
pub fn load_config(path: &PathBuf) -> ArchiveConfig {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => ArchiveConfig::default(),
    }
}

fn persist_config(path: &PathBuf, cfg: &ArchiveConfig) {
    if let Ok(json) = serde_json::to_string_pretty(cfg) {
        let _ = std::fs::write(path, json);
    }
}

/// Everything the system handlers need.
#[derive(Clone)]
pub struct SystemApiState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    store: Arc<DoltIssues>,
    config: SharedArchiveConfig,
    config_path: Option<PathBuf>,
}

impl SystemApiState {
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        store: Arc<DoltIssues>,
        config: SharedArchiveConfig,
        config_path: Option<PathBuf>,
    ) -> Self {
        Self { authenticator, audit, store, config, config_path }
    }
}

pub fn system_router(state: SystemApiState) -> Router {
    Router::new()
        .route("/api/v1/system/config", get(get_config).put(put_config))
        .route("/api/v1/system/archive/run", post(run_archive_now))
        .with_state(state)
}

/// Verify the token (cookie or bearer) and require `scope` (or `*`). Returns `JwtClaims` on
/// success; audits + returns the error `Response` on any failure — mirrors the hooks.rs pattern.
fn authorize(
    st: &SystemApiState,
    headers: &axum::http::HeaderMap,
    scope: &'static str,
    method: &Method,
) -> Result<JwtClaims, Response> {
    let uri: axum::http::Uri = SYSTEM_PATH.parse().expect("static uri");
    let reject = |status: StatusCode, msg: &'static str, actor: &str| -> Response {
        record_denial(
            st.audit.as_ref(),
            actor,
            None,
            method,
            &uri,
            Some(scope),
            status,
        );
        (status, msg).into_response()
    };

    let token = bearer(headers)
        .or_else(|| cookie(headers, TOKEN_COOKIE))
        .ok_or_else(|| reject(StatusCode::UNAUTHORIZED, "missing token", ANONYMOUS))?;

    let claims = st
        .authenticator
        .authenticate(&token)
        .map_err(|_| reject(StatusCode::UNAUTHORIZED, "invalid token", ANONYMOUS))?;

    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims
        .validate(now, JwtClaims::workspace_optional_from_env())
        .is_err()
    {
        return Err(reject(StatusCode::UNAUTHORIZED, "expired token", ANONYMOUS));
    }
    if !has_scope(&claims.scopes, scope) {
        return Err(reject(StatusCode::FORBIDDEN, "missing system scope", &claims.sub));
    }
    Ok(claims)
}

fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
}

fn bearer(headers: &axum::http::HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?
        .trim();
    (!token.is_empty()).then(|| token.to_string())
}

fn cookie(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())?
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| k.trim() == name)
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

async fn get_config(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, READ_SCOPE, &Method::GET) {
        return resp;
    }
    let cfg = st.config.read().await.clone();
    (StatusCode::OK, Json(cfg)).into_response()
}

async fn put_config(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
    Json(patch): Json<ArchiveConfigPatch>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::PUT) {
        return resp;
    }
    let mut cfg = st.config.write().await;
    if let Some(v) = patch.enabled {
        cfg.enabled = v;
    }
    if let Some(v) = patch.archive_after_days {
        cfg.archive_after_days = v;
    }
    if let Some(v) = patch.interval_minutes {
        cfg.interval_minutes = v.max(1);
    }
    let snapshot = cfg.clone();
    drop(cfg);
    if let Some(path) = &st.config_path {
        persist_config(path, &snapshot);
    }
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn run_archive_now(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        return resp;
    }
    let days = st.config.read().await.archive_after_days;
    match st.store.archive_old_closed(days).await {
        Ok(n) => (StatusCode::OK, Json(serde_json::json!({ "archived": n }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Background archive daemon. Spawn via `tokio::spawn(ArchiveDaemon::new(...).run())`.
/// Reads config on each tick so a PUT takes effect without a restart.
pub struct ArchiveDaemon {
    store: Arc<DoltIssues>,
    config: SharedArchiveConfig,
}

impl ArchiveDaemon {
    pub fn new(store: Arc<DoltIssues>, config: SharedArchiveConfig) -> Self {
        Self { store, config }
    }

    pub async fn run(self) {
        loop {
            let (enabled, days, interval_mins) = {
                let cfg = self.config.read().await;
                (cfg.enabled, cfg.archive_after_days, cfg.interval_minutes)
            };
            if enabled {
                match self.store.archive_old_closed(days).await {
                    Ok(0) => {}
                    Ok(n) => eprintln!("[archive-daemon] archived {n} closed issues (>{days}d)"),
                    Err(e) => eprintln!("[archive-daemon] sweep error: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_secs(interval_mins * 60)).await;
        }
    }
}
