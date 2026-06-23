//! Per-account relogin of a keychain credential dir (gtcore-1fe9b4).
//!
//! The wedge incident (2026-06-18, memory `orchd-sling-provisioning-wedge-recovery`): an operator
//! relogged a keychain account by hand and left its dir with a fresh `.credentials.json` but NO
//! `oauthAccount` nor onboarding flags in `.claude.json`. A polecat slung into that dir then ran the
//! first-run onboarding TUI → OAuth prompt → wedge. Duplicate/dead dirs for the same email also
//! accumulated, and the sling selection sometimes picked one.
//!
//! This module is the backend of the relogin flow the operator drives from the FE — the
//! credential-validity sibling of [`crate::onboard`] (which onboards a NEW account). It reuses the
//! same proven process-driving machinery (`claude /login` over piped stdio, URL capture, OOB-code
//! writeback, RS256 auth) but differs on three points the incident demanded:
//!
//! 1. **Relogin, not onboard** — `start` takes an existing `account` (its email) and relogs into the
//!    account's EXISTING `CLAUDE_CONFIG_DIR` (resolved from the accounts root), never allocating a
//!    fresh dir. The fresh `.credentials.json` (with refreshToken) lands in place.
//! 2. **Onboarding-complete seeding** — on `complete`, after the login succeeds, it seeds the
//!    `oauthAccount.emailAddress` + `hasCompletedOnboarding` + theme + bypass markers into the dir
//!    (`worktree::seed_account_onboarding_complete`) so the dir is IMMEDIATELY consumable by the
//!    sling, pre-authenticated — identical to the working `01KTWX*` accounts.
//! 3. **Dedup** — `complete` then retires every OTHER dir carrying the same email (moves them to
//!    `<root>/../accounts.disabled`, reversible), guaranteeing one active dir per email so the sling
//!    can never pick a stale duplicate.
//!
//! It also serves the cred-health read surface (`GET /api/v1/quota/cred-health`): the per-account
//! `{refresh, expiry, onboarding, needs_relogin}` report (`gt_quota::assess_cred_health`) the FE
//! renders to decide which accounts need a relogin — a signal `quota.list` cannot give (it measures
//! quota usage, not credential validity nor onboarding state).
//!
//! ## Security
//!
//! Same trust boundary as [`crate::onboard`]: mounted only with an RS256 verifier, every call
//! requires a `gt_web_token` cookie or bearer carrying `quota.write` (writes) / `quota.read`
//! (cred-health read) or `*`; rejections audit a denial. `claude /login` runs with the server
//! process's privileges.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command as TokioCommand};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use ulid::Ulid;

use gt_auth::JwtClaims;
use gt_quota::{assess_cred_health, CredHealthReport, REFRESH_SKEW_MS};

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::worktree::seed_account_onboarding_complete;

/// The browser-nav JWT cookie the calls authenticate by (same as onboarding / the SSE feed).
const TOKEN_COOKIE: &str = "gt_web_token";
/// The scope a caller must hold (besides `*`) to drive a relogin — the quota mutation scope.
const WRITE_SCOPE: &str = "quota.write";
/// The scope a caller must hold (besides `*`) to read cred-health.
const READ_SCOPE: &str = "quota.read";
/// The superuser grant that authorizes every scope (the seeded admin carries it).
const SCOPE_WILDCARD: &str = "*";
/// The route prefix, also the audited path on a denial.
const RELOGIN_PATH: &str = "/api/v1/quota/relogin";

/// How long `start` waits for the login URL before killing the spawned process.
const URL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long `complete` waits for the login process to exit after the code is written.
const EXIT_TIMEOUT: Duration = Duration::from_secs(120);

/// The `claude` binary; `GT_CLAUDE_BIN` overrides (the image may install it off the default PATH).
fn claude_bin() -> String {
    std::env::var("GT_CLAUDE_BIN")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "claude".to_string())
}

/// Edge-stamped unix milliseconds (cred-health classification reads expiry in millis).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A relogin in flight: the live `claude /login` process holding the OAuth handshake open, its
/// stdin (where the OOB code is written in `complete`), the account email it relogs, and the creds
/// dir it logs into. Reader tasks keep stdout/stderr drained; `kill_on_drop` reaps an un-completed
/// child.
struct LiveRelogin {
    child: Child,
    stdin: ChildStdin,
    account: String,
    dir: PathBuf,
    _readers: [JoinHandle<()>; 2],
}

/// Resolves the account ids the quota registry tracks (`quota.list`), so cred-health can enumerate
/// accounts with no on-disk dir (gtcore-e09320). A thunk rather than a stored snapshot because the
/// registry changes as accounts are registered/probed; the binary wires it over the event log.
pub type KnownAccountsFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// Everything the relogin handlers need: auth (RS256 verifier + denial audit), the live-session
/// map, the accounts root the per-account creds dirs live under, and (optionally) the quota account
/// roster so cred-health enumerates every tracked account, not just the ones with a dir.
#[derive(Clone)]
pub struct ReloginState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    sessions: Arc<Mutex<HashMap<String, LiveRelogin>>>,
    accounts_root: Arc<Path>,
    /// The quota-tracked account ids (`quota.list`). `None` ⇒ cred-health reports only the on-disk
    /// dirs (the prior behavior, kept for tests / degraded boots without an event log).
    known_accounts: Option<KnownAccountsFn>,
}

impl ReloginState {
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        accounts_root: PathBuf,
    ) -> Self {
        Self {
            authenticator,
            audit,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            accounts_root: accounts_root.into(),
            known_accounts: None,
        }
    }

    /// Wire the quota-account roster so `GET /api/v1/quota/cred-health` enumerates every account
    /// `quota.list` tracks — a dead/absent credential then surfaces as `needs_relogin` instead of
    /// vanishing from the report (gtcore-e09320).
    pub fn with_known_accounts(mut self, known: KnownAccountsFn) -> Self {
        self.known_accounts = Some(known);
        self
    }
}

/// `{account}` — the email of the keychain account to relog.
#[derive(Deserialize)]
pub struct StartRequest {
    pub account: String,
}

/// `{session_id, url}` — the client shows `url` for the human to authenticate, then posts the OOB
/// code back with `session_id`.
#[derive(Serialize)]
pub struct StartResponse {
    pub session_id: String,
    pub url: String,
}

#[derive(Deserialize)]
pub struct CompleteRequest {
    pub session_id: String,
    pub code: String,
}

/// `{account, config_dir, deduped}` — the relogged account, its (now consumable) creds dir, and the
/// list of duplicate dirs retired by the dedup pass (basenames moved to `accounts.disabled`).
#[derive(Serialize)]
pub struct CompleteResponse {
    pub account: String,
    pub config_dir: String,
    pub deduped: Vec<String>,
}

/// `{accounts: [CredHealthReport]}` — the per-account cred-health report for the FE.
#[derive(Serialize)]
pub struct HealthResponse {
    pub accounts: Vec<CredHealthReport>,
}

/// Relogin failures, mapped to HTTP responses.
#[derive(Debug)]
pub enum ReloginError {
    UnknownAccount(String),
    Spawn(String),
    NoUrl,
    Timeout(&'static str),
    UnknownSession,
    Stdin(String),
    LoginFailed(String),
}

impl IntoResponse for ReloginError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ReloginError::UnknownAccount(a) => (
                StatusCode::NOT_FOUND,
                format!("no onboarded credentials dir for account {a}"),
            ),
            ReloginError::Spawn(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn claude: {e}"))
            }
            ReloginError::NoUrl => (
                StatusCode::BAD_GATEWAY,
                "claude /login produced no login URL".to_string(),
            ),
            ReloginError::Timeout(what) => {
                (StatusCode::GATEWAY_TIMEOUT, format!("timed out: {what}"))
            }
            ReloginError::UnknownSession => (
                StatusCode::NOT_FOUND,
                "unknown or completed relogin session".to_string(),
            ),
            ReloginError::Stdin(e) => {
                (StatusCode::BAD_GATEWAY, format!("write code to login: {e}"))
            }
            ReloginError::LoginFailed(e) => (StatusCode::BAD_GATEWAY, format!("login failed: {e}")),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

/// Read the `oauthAccount.emailAddress` an account dir carries, or `None` when absent/unreadable.
/// The same projection [`crate::mcp::rest_backings::FsAccountCatalog`] uses, kept local so relogin
/// has no dependency on the catalog's internals.
fn email_of(dir: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(dir.join(".claude.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    v.get("oauthAccount")?
        .get("emailAddress")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Whether an account dir carries the onboarding-complete markers (`oauthAccount.emailAddress`
/// present AND `hasCompletedOnboarding == true`). The IDENTITY axis of cred-health.
fn onboarding_complete(dir: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(dir.join(".claude.json")) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return false;
    };
    let has_email = v
        .get("oauthAccount")
        .and_then(|o| o.get("emailAddress"))
        .and_then(|e| e.as_str())
        .is_some_and(|s| !s.is_empty());
    let onboarded = v
        .get("hasCompletedOnboarding")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    has_email && onboarded
}

/// All direct child dirs under `root` whose `.claude.json` resolves to `email`. Used by `start` (to
/// pick the active dir to relog) and `complete` (to dedup the rest). Returned sorted by basename so
/// "keep the lexicographically-first" is deterministic.
fn dirs_for_email(root: &Path, email: &str) -> Vec<PathBuf> {
    let Ok(read) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut matches: Vec<PathBuf> = read
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| email_of(p).as_deref() == Some(email))
        .collect();
    matches.sort();
    matches
}

/// Decide which dirs to RETIRE so exactly one remains active for an email (gtcore-1fe9b4 dedup).
///
/// Pure planner over the candidate dirs and the `keep` dir: every candidate that is not `keep` is
/// retired. Kept pure so the dedup decision is unit-tested without touching the filesystem; the edge
/// (`complete`) performs the moves the plan names.
fn dedup_plan(candidates: &[PathBuf], keep: &Path) -> Vec<PathBuf> {
    candidates
        .iter()
        .filter(|p| p.as_path() != keep)
        .cloned()
        .collect()
}

/// The dir to retire duplicates INTO: a sibling `accounts.disabled` next to the accounts root, so a
/// retire is reversible (`mv` back), matching the manual recovery the incident used.
fn disabled_root(accounts_root: &Path) -> PathBuf {
    match accounts_root.parent() {
        Some(parent) => parent.join("accounts.disabled"),
        None => PathBuf::from("accounts.disabled"),
    }
}

/// Move `dir` into `<disabled_root>/<basename>` (suffixing a ULID if the target name is taken, so a
/// re-dedup never clobbers a prior retirement). Best-effort: returns the moved basename on success.
fn retire_dir(dir: &Path, disabled_root: &Path) -> Option<String> {
    let _ = std::fs::create_dir_all(disabled_root);
    let base = dir.file_name()?.to_string_lossy().into_owned();
    let mut target = disabled_root.join(&base);
    if target.exists() {
        target = disabled_root.join(format!("{base}.{}", Ulid::new()));
    }
    std::fs::rename(dir, &target).ok().map(|_| base)
}

impl ReloginState {
    /// Begin a relogin: resolve the account's existing creds dir, spawn `claude /login` into it,
    /// capture the URL, keep the process alive in the session map.
    async fn start(&self, account: &str) -> Result<StartResponse, ReloginError> {
        let account = account.trim();
        if account.is_empty() {
            return Err(ReloginError::UnknownAccount("(empty)".into()));
        }
        // Relog into the EXISTING dir for this email (never allocate a fresh one). If several dirs
        // carry the email, relog the first (dedup in `complete` retires the rest onto this one).
        let dir = dirs_for_email(&self.accounts_root, account)
            .into_iter()
            .next()
            .ok_or_else(|| ReloginError::UnknownAccount(account.to_string()))?;

        let mut cmd = TokioCommand::new(claude_bin());
        cmd.args(["/login"])
            .env("CLAUDE_CONFIG_DIR", &dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| ReloginError::Spawn(e.to_string()))?;

        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take().expect("stderr piped");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let readers = [
            spawn_line_reader(stdout, tx.clone()),
            spawn_line_reader(stderr, tx),
        ];

        let url = match tokio::time::timeout(URL_TIMEOUT, async {
            while let Some(line) = rx.recv().await {
                if let Some(url) = extract_url(&line) {
                    return Some(url);
                }
            }
            None
        })
        .await
        {
            Ok(Some(url)) => url,
            Ok(None) => {
                let _ = child.kill().await;
                return Err(ReloginError::NoUrl);
            }
            Err(_) => {
                let _ = child.kill().await;
                return Err(ReloginError::Timeout("waiting for login URL"));
            }
        };

        let session_id = Ulid::new().to_string();
        self.sessions.lock().expect("sessions mutex").insert(
            session_id.clone(),
            LiveRelogin {
                child,
                stdin,
                account: account.to_string(),
                dir,
                _readers: readers,
            },
        );
        Ok(StartResponse { session_id, url })
    }

    /// Finish a relogin: feed the OOB code to the live process, wait for exit, seed the dir
    /// onboarding-complete (so it is consumable as-is), and dedup the email's other dirs.
    async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ReloginError> {
        let live = self
            .sessions
            .lock()
            .expect("sessions mutex")
            .remove(&req.session_id)
            .ok_or(ReloginError::UnknownSession)?;
        let LiveRelogin {
            mut child,
            mut stdin,
            account,
            dir,
            _readers,
        } = live;

        let line = format!("{}\n", req.code.trim());
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| ReloginError::Stdin(e.to_string()))?;
        stdin
            .flush()
            .await
            .map_err(|e| ReloginError::Stdin(e.to_string()))?;
        drop(stdin);

        let status = match tokio::time::timeout(EXIT_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(ReloginError::LoginFailed(e.to_string())),
            Err(_) => {
                let _ = child.kill().await;
                return Err(ReloginError::Timeout("waiting for login to complete"));
            }
        };
        if !status.success() {
            return Err(ReloginError::LoginFailed(format!(
                "claude /login exited {status}"
            )));
        }

        // The fresh `.credentials.json` is now in `dir`. Seed the IDENTITY/onboarding markers so the
        // dir is immediately consumable by the sling (no first-run TUI / OAuth wedge).
        seed_account_onboarding_complete(&dir, &account);

        // Dedup: retire every OTHER dir carrying this email onto the one we just relogged, so the
        // sling can never pick a stale duplicate. Best-effort; a failed move is logged, not fatal.
        let candidates = dirs_for_email(&self.accounts_root, &account);
        let to_retire = dedup_plan(&candidates, &dir);
        let disabled = disabled_root(&self.accounts_root);
        let mut deduped = Vec::new();
        for dup in to_retire {
            match retire_dir(&dup, &disabled) {
                Some(base) => deduped.push(base),
                None => eprintln!("[relogin] dedup: failed to retire {}", dup.display()),
            }
        }

        Ok(CompleteResponse {
            account,
            config_dir: dir.display().to_string(),
            deduped,
        })
    }

    /// Build the per-account cred-health report over every dir under the accounts root, stamping the
    /// edge clock. Delegates to [`cred_health_in`] so the read logic is unit-tested without an auth
    /// double.
    fn cred_health(&self) -> Vec<CredHealthReport> {
        let known = self
            .known_accounts
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        cred_health_in(&self.accounts_root, &known, now_ms())
    }
}

/// The per-account cred-health report over every dir under `root`, de-duped by email (the first dir
/// wins, matching [`crate::mcp::rest_backings::FsAccountCatalog`]). Pure classification via
/// [`assess_cred_health`]; this only reads the files (`now_ms` is the edge clock the caller stamps).
///
/// `known_accounts` is the set of account ids the quota registry tracks (`quota.list`). It closes
/// the visibility gap that hid dead credentials (gtcore-e09320): an account tracked in quota but
/// with NO on-disk dir — e.g. `brayanrayo`/`fsrbwowr`, whose creds dir was never provisioned or was
/// retired — would otherwise be ABSENT from this report, so the FE's `health[id]?.needs_relogin`
/// resolved `undefined → false` and rendered it `Healthy`. Every known account missing from the
/// on-disk scan is synthesized as `Unreadable` / `needs_relogin = true` (the same verdict
/// [`assess_cred_health`] gives a missing `.credentials.json`), so the report ENUMERATES every
/// quota-tracked account, never just the ones with a dir.
///
/// `pub(crate)` so the MCP `quota.cred_health` tool ([`crate::mcp::quota`]) serves the SAME report
/// the REST `GET /api/v1/quota/cred-health` does — one read path, two transports.
pub(crate) fn cred_health_in(
    root: &Path,
    known_accounts: &[String],
    now_ms: u64,
) -> Vec<CredHealthReport> {
    let mut by_email: std::collections::BTreeMap<String, CredHealthReport> =
        std::collections::BTreeMap::new();
    if let Ok(read) = std::fs::read_dir(root) {
        for entry in read.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(email) = email_of(&path) else {
                continue; // no oauth email ⇒ not yet a catalog entry
            };
            if by_email.contains_key(&email) {
                continue; // first dir per email wins (matches FsAccountCatalog)
            }
            let creds_raw = std::fs::read_to_string(path.join(".credentials.json")).ok();
            let report = assess_cred_health(
                email.clone(),
                creds_raw.as_deref(),
                onboarding_complete(&path),
                now_ms,
                REFRESH_SKEW_MS,
            );
            by_email.insert(email, report);
        }
    }
    // Enumerate every quota-tracked account: one with no on-disk dir (no entry above) is dead from
    // the FE's standpoint — a sling can't be born on it — so synthesize the `Unreadable` /
    // `needs_relogin` verdict instead of leaving it silently absent (gtcore-e09320).
    for account in known_accounts {
        by_email.entry(account.clone()).or_insert_with(|| {
            assess_cred_health(account.clone(), None, false, now_ms, REFRESH_SKEW_MS)
        });
    }
    by_email.into_values().collect()
}

/// Spawn a task that reads `reader` line by line into `tx`, ending silently at EOF.
fn spawn_line_reader<R>(reader: R, tx: mpsc::UnboundedSender<String>) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tx.send(line).is_err() {
                break;
            }
        }
    })
}

/// Pull the first `https://…`/`http://…` token out of a line, if any.
fn extract_url(line: &str) -> Option<String> {
    let start = line.find("https://").or_else(|| line.find("http://"))?;
    let url: String = line[start..]
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    (!url.is_empty()).then_some(url)
}

/// Authenticate (cookie or bearer), gate the token's clock/workspace invariants, and require
/// `required` (or `*`). Returns the verified claims or audits a denial + returns the HTTP status.
fn authorize(
    state: &ReloginState,
    headers: &HeaderMap,
    required: &'static str,
) -> Result<JwtClaims, (StatusCode, &'static str)> {
    let reject = |status: StatusCode, msg: &'static str| -> (StatusCode, &'static str) {
        record_denial(
            state.audit.as_ref(),
            ANONYMOUS,
            None,
            &Method::POST,
            &RELOGIN_PATH.parse().expect("static uri"),
            Some(required),
            status,
        );
        (status, msg)
    };

    let token = bearer(headers)
        .or_else(|| cookie(headers, TOKEN_COOKIE))
        .ok_or_else(|| {
            reject(
                StatusCode::UNAUTHORIZED,
                "missing gt_web_token cookie or bearer",
            )
        })?;

    let claims = state
        .authenticator
        .authenticate(&token)
        .map_err(|_| reject(StatusCode::UNAUTHORIZED, "invalid token"))?;

    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims
        .validate(now, JwtClaims::workspace_optional_from_env())
        .is_err()
    {
        return Err(reject(StatusCode::UNAUTHORIZED, "expired or incomplete token"));
    }
    if !has_scope(&claims.scopes, required) {
        return Err(reject(StatusCode::FORBIDDEN, "missing required quota scope"));
    }
    Ok(claims)
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
}

/// The relogin + cred-health router: `POST /api/v1/quota/relogin/{start,complete}` and
/// `GET /api/v1/quota/cred-health`, auth + scope enforced per handler (merged into the top-level
/// app like the onboarding router).
pub fn relogin_router(state: ReloginState) -> Router {
    Router::new()
        .route("/api/v1/quota/relogin/start", post(start_handler))
        .route("/api/v1/quota/relogin/complete", post(complete_handler))
        .route("/api/v1/quota/cred-health", get(health_handler))
        .with_state(state)
}

async fn start_handler(
    State(st): State<ReloginState>,
    headers: HeaderMap,
    Json(req): Json<StartRequest>,
) -> Response {
    if let Err((status, msg)) = authorize(&st, &headers, WRITE_SCOPE) {
        return (status, msg).into_response();
    }
    match st.start(&req.account).await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn complete_handler(
    State(st): State<ReloginState>,
    headers: HeaderMap,
    Json(req): Json<CompleteRequest>,
) -> Response {
    if let Err((status, msg)) = authorize(&st, &headers, WRITE_SCOPE) {
        return (status, msg).into_response();
    }
    match st.complete(req).await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn health_handler(State(st): State<ReloginState>, headers: HeaderMap) -> Response {
    if let Err((status, msg)) = authorize(&st, &headers, READ_SCOPE) {
        return (status, msg).into_response();
    }
    let accounts = st.cred_health();
    (StatusCode::OK, Json(HealthResponse { accounts })).into_response()
}

/// The bearer token from an `Authorization: Bearer <jwt>` header, trimmed and non-empty.
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

    /// Write a dir with `oauthAccount.emailAddress`, optional onboarding flag, and optional creds.
    fn write_dir(root: &Path, name: &str, email: &str, onboarded: bool, creds: Option<&str>) {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let mut claude = serde_json::json!({ "oauthAccount": { "emailAddress": email } });
        if onboarded {
            claude["hasCompletedOnboarding"] = serde_json::Value::Bool(true);
        }
        std::fs::write(dir.join(".claude.json"), claude.to_string()).unwrap();
        if let Some(c) = creds {
            std::fs::write(dir.join(".credentials.json"), c).unwrap();
        }
    }

    fn valid_creds(now_ms: u64) -> String {
        format!(
            r#"{{"claudeAiOauth":{{"accessToken":"at","refreshToken":"rt","expiresAt":{}}}}}"#,
            now_ms + 3_600_000
        )
    }

    #[test]
    fn email_of_reads_oauth_address() {
        let tmp = tempfile::tempdir().unwrap();
        write_dir(tmp.path(), "d1", "a@x.com", true, None);
        assert_eq!(
            email_of(&tmp.path().join("d1")).as_deref(),
            Some("a@x.com")
        );
        // A dir with no .claude.json ⇒ None.
        std::fs::create_dir(tmp.path().join("empty")).unwrap();
        assert_eq!(email_of(&tmp.path().join("empty")), None);
    }

    #[test]
    fn onboarding_complete_requires_both_email_and_flag() {
        let tmp = tempfile::tempdir().unwrap();
        write_dir(tmp.path(), "full", "a@x.com", true, None);
        write_dir(tmp.path(), "noflag", "a@x.com", false, None);
        assert!(onboarding_complete(&tmp.path().join("full")));
        assert!(!onboarding_complete(&tmp.path().join("noflag")));
    }

    #[test]
    fn dirs_for_email_collects_and_sorts_matches() {
        let tmp = tempfile::tempdir().unwrap();
        write_dir(tmp.path(), "01B", "dup@x.com", true, None);
        write_dir(tmp.path(), "01A", "dup@x.com", true, None);
        write_dir(tmp.path(), "01C", "other@x.com", true, None);
        let matches = dirs_for_email(tmp.path(), "dup@x.com");
        let names: Vec<String> = matches
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["01A".to_string(), "01B".to_string()]);
    }

    #[test]
    fn dedup_plan_retires_every_dir_but_keep() {
        let a = PathBuf::from("/root/01A");
        let b = PathBuf::from("/root/01B");
        let c = PathBuf::from("/root/01C");
        let plan = dedup_plan(&[a.clone(), b.clone(), c.clone()], &b);
        assert_eq!(plan, vec![a, c]);
        // Single candidate (the keep) ⇒ nothing to retire.
        assert!(dedup_plan(&[b.clone()], &b).is_empty());
    }

    #[test]
    fn disabled_root_is_sibling_of_accounts_root() {
        assert_eq!(
            disabled_root(Path::new("/var/lib/gt-core/accounts")),
            PathBuf::from("/var/lib/gt-core/accounts.disabled")
        );
    }

    #[test]
    fn retire_dir_moves_into_disabled_and_avoids_clobber() {
        let tmp = tempfile::tempdir().unwrap();
        let accounts = tmp.path().join("accounts");
        std::fs::create_dir_all(&accounts).unwrap();
        let disabled = disabled_root(&accounts);
        write_dir(&accounts, "01DUP", "a@x.com", true, None);
        let moved = retire_dir(&accounts.join("01DUP"), &disabled).unwrap();
        assert_eq!(moved, "01DUP");
        assert!(!accounts.join("01DUP").exists());
        assert!(disabled.join("01DUP").exists());

        // A second dir with the same basename is suffixed, not clobbered.
        write_dir(&accounts, "01DUP", "a@x.com", true, None);
        let moved2 = retire_dir(&accounts.join("01DUP"), &disabled).unwrap();
        assert_eq!(moved2, "01DUP");
        assert!(disabled.join("01DUP").exists()); // original intact
        // exactly one suffixed sibling was added.
        let extra = std::fs::read_dir(&disabled)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("01DUP."))
            .count();
        assert_eq!(extra, 1);
    }

    #[test]
    fn cred_health_reports_per_email_deduped() {
        let tmp = tempfile::tempdir().unwrap();
        let accounts = tmp.path().join("accounts");
        std::fs::create_dir_all(&accounts).unwrap();
        let now = now_ms();
        // Healthy + onboarded ⇒ no relogin.
        write_dir(&accounts, "01HEALTHY", "ok@x.com", true, Some(&valid_creds(now)));
        // Valid creds but NO onboarding flag ⇒ needs relogin (the wedge case).
        write_dir(&accounts, "01WEDGE", "wedge@x.com", false, Some(&valid_creds(now)));
        // No creds file at all ⇒ needs relogin.
        write_dir(&accounts, "01NOCRED", "nocred@x.com", true, None);

        let reports = cred_health_in(&accounts, &[], now);
        let by: HashMap<&str, &CredHealthReport> =
            reports.iter().map(|r| (r.account.as_str(), r)).collect();

        assert!(!by["ok@x.com"].needs_relogin);
        assert!(by["ok@x.com"].onboarding_complete);
        assert!(by["wedge@x.com"].needs_relogin);
        assert!(!by["wedge@x.com"].onboarding_complete);
        assert!(by["nocred@x.com"].needs_relogin);
    }

    #[test]
    fn cred_health_enumerates_quota_accounts_without_a_dir() {
        // gtcore-e09320: an account tracked in quota but with NO on-disk dir (brayanrayo/fsrbwowr)
        // must still appear — as Unreadable / needs_relogin — so the FE never renders it Healthy.
        let tmp = tempfile::tempdir().unwrap();
        let accounts = tmp.path().join("accounts");
        std::fs::create_dir_all(&accounts).unwrap();
        let now = now_ms();
        // One account HAS a healthy dir; two are quota-tracked but dirless.
        write_dir(&accounts, "01HEALTHY", "ok@x.com", true, Some(&valid_creds(now)));
        let known = vec![
            "ok@x.com".to_string(),
            "brayanrayo@bi-quare.com".to_string(),
            "fsrbwowr@gmail.com".to_string(),
        ];

        let reports = cred_health_in(&accounts, &known, now);
        let by: HashMap<&str, &CredHealthReport> =
            reports.iter().map(|r| (r.account.as_str(), r)).collect();

        // The on-disk healthy account keeps its real (slingable) verdict — not overwritten.
        assert!(!by["ok@x.com"].needs_relogin);
        assert!(by["ok@x.com"].onboarding_complete);
        // The dirless quota accounts are enumerated as dead.
        for dead in ["brayanrayo@bi-quare.com", "fsrbwowr@gmail.com"] {
            assert!(by[dead].needs_relogin, "{dead} must need relogin");
            assert!(!by[dead].onboarding_complete);
            assert_eq!(by[dead].credential, gt_quota::CredentialHealth::Unreadable);
            assert!(!by[dead].refresh);
            assert_eq!(by[dead].expires_at_secs, None);
        }
    }
}
