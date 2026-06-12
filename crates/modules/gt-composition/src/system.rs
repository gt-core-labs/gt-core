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
use gt_store_dolt::{ArchivedIssue, DoltIssues};
use gt_store_pg::{DocumentsRepository, PgDocuments, ReportSubscriptionsRepository};

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::mcp::WsPools;

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
    /// The scheduled report digests (hq-7d50e4): a LIST of schedules, each
    /// with its own mode (daily / every-n-days / once), board scope and
    /// optional recipients.
    #[serde(default)]
    pub report_schedules: Vec<crate::report_scheduler::ReportSchedule>,
    /// The pre-multi-schedule scalar config (hq-84f93b), still read from the
    /// persisted file's `report` key so a deployed config migrates into one
    /// Daily schedule on load ([`load_config`]). Never written back.
    #[serde(default, rename = "report", skip_serializing)]
    pub report_legacy: Option<crate::report_scheduler::LegacyReportConfig>,
}

fn default_true() -> bool {
    true
}
fn default_archive_days() -> u32 {
    30
}
fn default_interval_minutes() -> u64 {
    60
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            archive_after_days: default_archive_days(),
            interval_minutes: default_interval_minutes(),
            report_schedules: Vec::new(),
            report_legacy: None,
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
/// A pre-multi-schedule `report` scalar (hq-84f93b) migrates into one Daily
/// schedule when no `report_schedules` exist yet.
pub fn load_config(path: &PathBuf) -> ArchiveConfig {
    let mut cfg: ArchiveConfig = match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => ArchiveConfig::default(),
    };
    if let Some(legacy) = cfg.report_legacy.take() {
        if cfg.report_schedules.is_empty() {
            cfg.report_schedules.push(legacy.into_schedule());
        }
    }
    cfg
}

pub(crate) fn persist_config(path: &PathBuf, cfg: &ArchiveConfig) {
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
    /// Per-workspace Postgres pool cache for the archive interceptor (hq-docs-archive-sync):
    /// soft-deleting an archived epic's `documents`. `None` when `GT_PG_URL` is unset — no
    /// documents store, so nothing to clean up.
    pools: Option<Arc<WsPools>>,
    /// The report-digest service (hq-84f93b): schedule + subscribers + manual
    /// send. `None` when `GT_PG_URL` is unset — the report routes answer 503.
    report: Option<Arc<crate::report_scheduler::ReportService>>,
}

impl SystemApiState {
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        store: Arc<DoltIssues>,
        config: SharedArchiveConfig,
        config_path: Option<PathBuf>,
        pools: Option<Arc<WsPools>>,
        report: Option<Arc<crate::report_scheduler::ReportService>>,
    ) -> Self {
        Self {
            authenticator,
            audit,
            store,
            config,
            config_path,
            pools,
            report,
        }
    }
}

pub fn system_router(state: SystemApiState) -> Router {
    Router::new()
        .route("/api/v1/system/config", get(get_config).put(put_config))
        .route("/api/v1/system/archive/run", post(run_archive_now))
        .route(
            "/api/v1/system/report/schedule",
            get(get_report_schedule).put(put_report_schedule),
        )
        .route(
            "/api/v1/system/report/subscribers",
            get(list_report_subscribers).post(add_report_subscriber),
        )
        .route(
            "/api/v1/system/report/subscribers/:email",
            axum::routing::delete(remove_report_subscriber).patch(toggle_report_subscriber),
        )
        .route("/api/v1/system/report/run", post(run_report_now))
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
        return Err(reject(
            StatusCode::FORBIDDEN,
            "missing system scope",
            &claims.sub,
        ));
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

async fn get_config(State(st): State<SystemApiState>, headers: axum::http::HeaderMap) -> Response {
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
        Ok(archived) => {
            if let Some(pools) = &st.pools {
                purge_archived_epic_documents(pools, &archived).await;
            }
            let n = archived.len();
            (StatusCode::OK, Json(serde_json::json!({ "archived": n }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// The archive interceptor (hq-docs-archive-sync, Phase B): for every archived **epic**, soft-delete
/// its `documents` rows so the epic stops surfacing in `documents.search` once it leaves the tracker.
///
/// Best-effort by design — a docs-store hiccup is logged (`eprintln!`) and swallowed so it can never
/// fail the archive sweep itself, mirroring `embed_doc`'s best-effort contract. Scoped to the default
/// workspace (the current single-tenant deployment); multi-workspace rigs are a follow-up. A no-op
/// when no epic was archived.
pub async fn purge_archived_epic_documents(pools: &WsPools, archived: &[ArchivedIssue]) {
    let epics: Vec<&ArchivedIssue> = archived
        .iter()
        .filter(|a| a.issue_type == "epic")
        .collect();
    if epics.is_empty() {
        return;
    }
    let pool = match pools.get(None).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("[archive] epic-docs cleanup skipped — workspace pool: {e}");
            return;
        }
    };
    let repo = PgDocuments::new(pool);
    for epic in epics {
        let docs = match repo.list_by_owner_id(&epic.id).await {
            Ok(docs) => docs,
            Err(e) => {
                eprintln!("[archive] list documents for epic {}: {e}", epic.id);
                continue;
            }
        };
        for doc in docs {
            if let Err(e) = repo.soft_delete(&doc.id, doc.version).await {
                eprintln!(
                    "[archive] soft-delete doc {} of archived epic {}: {e}",
                    doc.id, epic.id
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Report digest surface (hq-84f93b): schedule + subscribers + manual run.
// Same auth ladder as the archive knobs; every handler 503s when the service
// is absent (GT_PG_URL unset).
// ---------------------------------------------------------------------------

fn report_service(
    st: &SystemApiState,
) -> Result<Arc<crate::report_scheduler::ReportService>, Response> {
    st.report.clone().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, "report service off (GT_PG_URL unset)").into_response()
    })
}

fn subscriber_json(s: &gt_store_pg::ReportSubscriber) -> serde_json::Value {
    serde_json::json!({
        "id": s.id, "email": s.email, "enabled": s.enabled, "created_at": s.created_at,
    })
}

/// The workspace whose GLOBAL subscriber list the legacy endpoints manage:
/// the first schedule's, else `default`.
async fn report_workspace(st: &SystemApiState) -> String {
    st.config
        .read()
        .await
        .report_schedules
        .first()
        .map(|s| s.workspace.clone())
        .unwrap_or_else(|| "default".to_string())
}

async fn get_report_schedule(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, READ_SCOPE, &Method::GET) {
        return resp;
    }
    // Transitional shim (full CRUD: hq-c4f920): expose the FIRST schedule in
    // the legacy scalar shape, or the defaults when none exist yet.
    let report = st
        .config
        .read()
        .await
        .report_schedules
        .first()
        .cloned()
        .unwrap_or(crate::report_scheduler::ReportSchedule {
            enabled: false,
            ..Default::default()
        });
    (StatusCode::OK, Json(report)).into_response()
}

/// Partial schedule update; `last_sent_date` is daemon bookkeeping and not
/// patchable.
#[derive(Debug, Deserialize)]
struct ReportSchedulePatch {
    enabled: Option<bool>,
    hour: Option<u8>,
    minute: Option<u8>,
    tz_offset_minutes: Option<i64>,
    rig: Option<String>,
    workspace: Option<String>,
}

async fn put_report_schedule(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
    Json(patch): Json<ReportSchedulePatch>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::PUT) {
        return resp;
    }
    if patch.hour.is_some_and(|h| h > 23) || patch.minute.is_some_and(|m| m > 59) {
        return (StatusCode::BAD_REQUEST, "hour 0-23, minute 0-59").into_response();
    }
    // Transitional shim (full CRUD: hq-c4f920): patch the FIRST schedule,
    // creating a disabled Daily one when the list is empty.
    let mut cfg = st.config.write().await;
    if cfg.report_schedules.is_empty() {
        cfg.report_schedules.push(crate::report_scheduler::ReportSchedule {
            enabled: false,
            ..Default::default()
        });
    }
    let r = cfg.report_schedules.first_mut().expect("just ensured");
    if let Some(v) = patch.enabled {
        r.enabled = v;
    }
    if let Some(v) = patch.hour {
        r.hour = v;
    }
    if let Some(v) = patch.minute {
        r.minute = v;
    }
    if let Some(v) = patch.tz_offset_minutes {
        r.tz_offset_minutes = v.clamp(-14 * 60, 14 * 60);
    }
    if let Some(v) = patch.rig {
        r.rig = v;
    }
    if let Some(v) = patch.workspace {
        r.workspace = v;
    }
    let first = r.clone();
    let snapshot = cfg.clone();
    drop(cfg);
    if let Some(path) = &st.config_path {
        persist_config(path, &snapshot);
    }
    (StatusCode::OK, Json(first)).into_response()
}

async fn list_report_subscribers(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, READ_SCOPE, &Method::GET) {
        return resp;
    }
    let svc = match report_service(&st) {
        Ok(svc) => svc,
        Err(resp) => return resp,
    };
    let workspace = report_workspace(&st).await;
    match svc.subscribers().list(&workspace).await {
        Ok(subs) => {
            let subs: Vec<_> = subs.iter().map(subscriber_json).collect();
            (StatusCode::OK, Json(serde_json::json!({ "subscribers": subs }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct AddSubscriberBody {
    email: String,
}

async fn add_report_subscriber(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<AddSubscriberBody>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        return resp;
    }
    let svc = match report_service(&st) {
        Ok(svc) => svc,
        Err(resp) => return resp,
    };
    let email = body.email.trim().to_string();
    if !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "email must be an address").into_response();
    }
    let workspace = report_workspace(&st).await;
    match svc.subscribers().add(&workspace, &email).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn remove_report_subscriber(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(email): axum::extract::Path<String>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::DELETE) {
        return resp;
    }
    let svc = match report_service(&st) {
        Ok(svc) => svc,
        Err(resp) => return resp,
    };
    let workspace = report_workspace(&st).await;
    match svc.subscribers().remove(&workspace, &email).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(gt_store_pg::ReportSubscriptionError::NotFound(e)) => {
            (StatusCode::NOT_FOUND, format!("unknown subscriber {e}")).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// The send-selection switch: PATCH `{"enabled": bool}`.
#[derive(Debug, Deserialize)]
struct ToggleSubscriberBody {
    enabled: bool,
}

async fn toggle_report_subscriber(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(email): axum::extract::Path<String>,
    Json(body): Json<ToggleSubscriberBody>,
) -> Response {
    if let Err(resp) = authorize(&st, &headers, WRITE_SCOPE, &Method::PATCH) {
        return resp;
    }
    let svc = match report_service(&st) {
        Ok(svc) => svc,
        Err(resp) => return resp,
    };
    let workspace = report_workspace(&st).await;
    match svc.subscribers().set_enabled(&workspace, &email, body.enabled).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(gt_store_pg::ReportSubscriptionError::NotFound(e)) => {
            (StatusCode::NOT_FOUND, format!("unknown subscriber {e}")).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn run_report_now(
    State(st): State<SystemApiState>,
    headers: axum::http::HeaderMap,
) -> Response {
    let claims = match authorize(&st, &headers, WRITE_SCOPE, &Method::POST) {
        Ok(claims) => claims,
        Err(resp) => return resp,
    };
    let svc = match report_service(&st) {
        Ok(svc) => svc,
        Err(resp) => return resp,
    };
    // Transitional: no id ⇒ the single schedule (full per-id run: hq-c4f920).
    let schedule = match svc.resolve_schedule(None).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match svc.send_schedule(&schedule, &claims.sub).await {
        Ok(queued) => {
            (StatusCode::OK, Json(serde_json::json!({ "queued": queued }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config_migrates_legacy_scalar_report_into_one_daily_schedule() {
        let dir = std::env::temp_dir().join(format!("gt-syscfg-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("system_config.json");
        std::fs::write(
            &path,
            r#"{"enabled":true,"archive_after_days":30,"interval_minutes":60,
                "report":{"enabled":true,"hour":9,"minute":15,"rig":"hq",
                          "workspace":"default","last_sent_date":"2026-06-11"}}"#,
        )
        .unwrap();
        let cfg = load_config(&path);
        assert_eq!(cfg.report_schedules.len(), 1);
        let s = &cfg.report_schedules[0];
        assert!(s.enabled);
        assert_eq!((s.hour, s.minute), (9, 15));
        assert_eq!(s.last_sent_date.as_deref(), Some("2026-06-11"));
        // Round-trip: persist writes the NEW shape only (no `report` key).
        persist_config(&path, &cfg);
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("report_schedules"));
        assert!(!raw.contains("\"report\":"), "legacy key never written back");
        // Re-load keeps the schedule (no double migration).
        let again = load_config(&path);
        assert_eq!(again.report_schedules.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Background archive daemon. Spawn via `tokio::spawn(ArchiveDaemon::new(...).run())`.
/// Reads config on each tick so a PUT takes effect without a restart.
pub struct ArchiveDaemon {
    store: Arc<DoltIssues>,
    config: SharedArchiveConfig,
    /// Same per-workspace pool cache as [`SystemApiState`] — drives the epic-docs cleanup
    /// (hq-docs-archive-sync) after each sweep. `None` when `GT_PG_URL` is unset.
    pools: Option<Arc<WsPools>>,
}

impl ArchiveDaemon {
    pub fn new(
        store: Arc<DoltIssues>,
        config: SharedArchiveConfig,
        pools: Option<Arc<WsPools>>,
    ) -> Self {
        Self {
            store,
            config,
            pools,
        }
    }

    pub async fn run(self) {
        loop {
            let (enabled, days, interval_mins) = {
                let cfg = self.config.read().await;
                (cfg.enabled, cfg.archive_after_days, cfg.interval_minutes)
            };
            if enabled {
                match self.store.archive_old_closed(days).await {
                    Ok(archived) if archived.is_empty() => {}
                    Ok(archived) => {
                        let n = archived.len();
                        if let Some(pools) = &self.pools {
                            purge_archived_epic_documents(pools, &archived).await;
                        }
                        eprintln!("[archive-daemon] archived {n} closed issues (>{days}d)");
                    }
                    Err(e) => eprintln!("[archive-daemon] sweep error: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_secs(interval_mins * 60)).await;
        }
    }
}
