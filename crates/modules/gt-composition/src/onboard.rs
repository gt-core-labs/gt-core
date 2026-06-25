//! Web-driven claude-account onboarding, served from the backend (`hq-quota-onboard-web.4`).
//!
//! The operator should not hand-pick an account id or type a credentials path (the old qa.5 form).
//! The web "Add account" flow drives the real `claude auth login` lifecycle, split across two
//! authenticated HTTP calls because the OAuth handshake has a human in the middle:
//!
//! 1. **`POST /api/v1/quota/onboard/start`** — allocate a generic `CLAUDE_CONFIG_DIR` under the
//!    accounts root (`account_dirs`, qa.3), spawn `claude auth login` into it with piped stdio,
//!    read the login URL it prints, and keep the process **alive** (stdin open) in a session map.
//!    Returns `{session_id, url}`; the client shows the URL for the human to authenticate.
//! 2. **`POST /api/v1/quota/onboard/complete {session_id, code}`** — write the OOB code back to the
//!    live process's stdin, wait for it to exit, read the account identity from
//!    `claude auth status --json`, and register it event-sourced via the workspace quota log (the
//!    same `quota.account_registered.v1` `quota.register` emits, which the daemon hydrates from).
//!    The id is the login's `email` — captured from the handshake, never typed.
//!
//! ## Why this lives in the backend (not the orchestration daemon)
//!
//! `.1`/`.2` ran this on the host daemon because `claude` was not in the mcp-server container.
//! `.4` puts `claude` IN the image (Dockerfile.embeddings) and serves onboarding here, so it rides
//! the existing `/api/v1/*` Traefik route + RS256 auth chain — no host process, no docker→host
//! firewall hole, no file-provider route. The captured creds dir lands on the shared event-log
//! volume; the daemon (host) reads the same log to hydrate its rotation keychain.
//!
//! ## Security
//!
//! Mounted by the composition root only when an RS256 verifier is configured. Every call requires a
//! valid `gt_web_token` cookie or bearer carrying the [`REQUIRED_SCOPE`] (`quota.write`) or `*`;
//! rejections audit a denial. `claude auth login` runs with the server process's privileges — same
//! trust boundary as the interactive terminal.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command as TokioCommand};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use ulid::Ulid;

use gt_auth::JwtClaims;
use gt_events::Command;
use gt_quota::{AccountRegistry, QuotaState, RegisterAccount};

use crate::account_dirs::account_config_dir;
use crate::worktree::seed_account_onboarding_complete;
use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::mcp::eventlog::EventLog;

/// The browser-nav JWT cookie the calls authenticate by (same as the SSE feed / terminal). A bearer
/// header is accepted too, for non-browser clients.
const TOKEN_COOKIE: &str = "gt_web_token";
/// The scope a caller must hold (besides `*`) to onboard an account — the same write scope the
/// `quota.register` tool / `POST /api/v1/quota/account` route require.
const REQUIRED_SCOPE: &str = "quota.write";
/// The superuser grant that authorizes every scope (the seeded admin carries it).
const SCOPE_WILDCARD: &str = "*";
/// The route prefix, also the audited path on a denial.
const ONBOARD_PATH: &str = "/api/v1/quota/onboard";
/// The event-log kind prefix for the quota domain (mirrors `mcp::quota`).
const QUOTA_NS: &str = "quota.";

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

/// Edge-stamped unix seconds for the registry event.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A login in flight: the live `claude auth login` process holding the OAuth handshake open, its
/// stdin (where the OOB code is written in `complete`), and the credentials dir it logs into. Reader
/// tasks keep stdout/stderr drained so the process never blocks on a full pipe; `kill_on_drop` reaps
/// the child if a session is dropped un-completed.
struct LiveOnboard {
    child: Child,
    stdin: ChildStdin,
    dir: PathBuf,
    _readers: [JoinHandle<()>; 2],
}

/// Everything the onboarding handlers need: auth (RS256 verifier + denial audit), the live-session
/// map, the per-workspace event log (to register the captured account), and the accounts root the
/// generic per-onboarding dir is allocated under.
#[derive(Clone)]
pub struct OnboardState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
    sessions: Arc<Mutex<HashMap<String, LiveOnboard>>>,
    log: Arc<EventLog>,
    accounts_root: Arc<Path>,
}

impl OnboardState {
    pub fn new(
        authenticator: SharedAuthenticator,
        audit: SharedAudit,
        log: Arc<EventLog>,
        accounts_root: PathBuf,
    ) -> Self {
        Self {
            authenticator,
            audit,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            log,
            accounts_root: accounts_root.into(),
        }
    }
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

/// `{account, config_dir}` — the account id is the login's email, captured from the handshake.
#[derive(Serialize)]
pub struct CompleteResponse {
    pub account: String,
    pub config_dir: String,
}

/// Onboarding failures, mapped to HTTP responses.
#[derive(Debug)]
pub enum OnboardError {
    Dir(String),
    Spawn(String),
    NoUrl,
    Timeout(&'static str),
    UnknownSession,
    Stdin(String),
    LoginFailed(String),
    NotLoggedIn(String),
    Register(String),
}

impl IntoResponse for OnboardError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            OnboardError::Dir(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("credentials dir: {e}"),
            ),
            OnboardError::Spawn(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("spawn claude: {e}"),
            ),
            OnboardError::NoUrl => (
                StatusCode::BAD_GATEWAY,
                "claude auth login produced no login URL".to_string(),
            ),
            OnboardError::Timeout(what) => {
                (StatusCode::GATEWAY_TIMEOUT, format!("timed out: {what}"))
            }
            OnboardError::UnknownSession => (
                StatusCode::NOT_FOUND,
                "unknown or completed onboarding session".to_string(),
            ),
            OnboardError::Stdin(e) => {
                (StatusCode::BAD_GATEWAY, format!("write code to login: {e}"))
            }
            OnboardError::LoginFailed(e) => (StatusCode::BAD_GATEWAY, format!("login failed: {e}")),
            OnboardError::NotLoggedIn(e) => {
                (StatusCode::BAD_GATEWAY, format!("login incomplete: {e}"))
            }
            OnboardError::Register(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("register account: {e}"),
            ),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

/// Authenticate (cookie or bearer), gate the token's clock/workspace invariants, and require the
/// `quota.write` scope (or `*`). Returns the verified claims (the workspace the account registers
/// into) or audits a denial and returns the HTTP status/body.
fn authorize(
    state: &OnboardState,
    headers: &HeaderMap,
) -> Result<JwtClaims, (StatusCode, &'static str)> {
    let reject = |status: StatusCode, msg: &'static str| -> (StatusCode, &'static str) {
        record_denial(
            state.audit.as_ref(),
            ANONYMOUS,
            None,
            &Method::POST,
            &ONBOARD_PATH.parse().expect("static uri"),
            Some(REQUIRED_SCOPE),
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
        return Err(reject(
            StatusCode::UNAUTHORIZED,
            "expired or incomplete token",
        ));
    }
    if !has_scope(&claims.scopes, REQUIRED_SCOPE) {
        return Err(reject(StatusCode::FORBIDDEN, "missing quota.write scope"));
    }
    Ok(claims)
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
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

/// Append `prompt=select_account` to an authorize URL so the IdP shows the account chooser instead
/// of silently reusing the browser's active session (`hq-quota-onboard-web.6`). Idempotent: skips if
/// the param is already present. Uses `&` when the URL already has a query (it always does), else `?`.
fn with_select_account(url: &str) -> String {
    if url.contains("prompt=") {
        return url.to_string();
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}prompt=select_account")
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

impl OnboardState {
    /// Begin a login: allocate a dir, spawn `claude auth login`, capture the URL, keep the process
    /// alive in the session map.
    async fn start(&self) -> Result<StartResponse, OnboardError> {
        let session_id = Ulid::new().to_string();
        // The session id is a ULID (no separators) so the sanitizing join always succeeds; the dir
        // is generic storage — the real account id comes from the handshake in `complete`.
        let dir = account_config_dir(&self.accounts_root, &session_id)
            .ok_or_else(|| OnboardError::Dir("session id rejected by sanitizer".into()))?;
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| OnboardError::Dir(e.to_string()))?;

        let mut cmd = TokioCommand::new(claude_bin());
        cmd.args(["auth", "login"])
            .env("CLAUDE_CONFIG_DIR", &dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .map_err(|e| OnboardError::Spawn(e.to_string()))?;

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
                return Err(OnboardError::NoUrl);
            }
            Err(_) => {
                let _ = child.kill().await;
                return Err(OnboardError::Timeout("waiting for login URL"));
            }
        };

        self.sessions.lock().expect("sessions mutex").insert(
            session_id.clone(),
            LiveOnboard {
                child,
                stdin,
                dir,
                _readers: readers,
            },
        );
        // Force the account chooser (hq-quota-onboard-web.6): without this, claude.ai reuses the
        // browser's active session and authorizes THAT account — so onboarding a second, different
        // account silently re-registers the first one. `prompt=select_account` is the standard OIDC
        // hint to show the chooser even with a live session; if the IdP ignores it the param is
        // harmless (the user falls back to an incognito window). Appended, not in the CLI-built URL.
        Ok(StartResponse {
            session_id,
            url: with_select_account(&url),
        })
    }

    /// Finish a login: feed the OOB code to the live process, wait for exit, read the account
    /// identity, and register it into `ws`.
    async fn complete(
        &self,
        ws: &str,
        req: CompleteRequest,
    ) -> Result<CompleteResponse, OnboardError> {
        let live = self
            .sessions
            .lock()
            .expect("sessions mutex")
            .remove(&req.session_id)
            .ok_or(OnboardError::UnknownSession)?;
        let LiveOnboard {
            mut child,
            mut stdin,
            dir,
            _readers,
        } = live;

        let line = format!("{}\n", req.code.trim());
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| OnboardError::Stdin(e.to_string()))?;
        stdin
            .flush()
            .await
            .map_err(|e| OnboardError::Stdin(e.to_string()))?;
        drop(stdin);

        let status = match tokio::time::timeout(EXIT_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(OnboardError::LoginFailed(e.to_string())),
            Err(_) => {
                let _ = child.kill().await;
                return Err(OnboardError::Timeout("waiting for login to complete"));
            }
        };
        if !status.success() {
            return Err(OnboardError::LoginFailed(format!(
                "claude auth login exited {status}"
            )));
        }

        let account = claude_auth_status_email(&dir).await?;
        // Seed the onboarding-complete markers (hasCompletedOnboarding + bypass + theme +
        // oauthAccount.emailAddress) so the freshly onboarded dir is immediately consumable by the
        // sling and cred-health does not flag it `needs_relogin` — the SAME seed relogin.rs applies
        // on its `complete`. `claude auth login` writes the credential + a partial oauthAccount but
        // not the onboarding flags, so without this the dir wedges the first-run TUI at sling time.
        seed_account_onboarding_complete(&dir, &account);
        let config_dir = dir.display().to_string();
        self.register(ws, &account, &config_dir)?;
        Ok(CompleteResponse {
            account,
            config_dir,
        })
    }

    /// Append the event-sourced `quota.account_registered.v1` to `ws`'s quota log — the same
    /// read-modify-append the `quota.register` tool runs — so the account joins the rotation pool
    /// the daemon hydrates from.
    fn register(&self, ws: &str, account: &str, config_dir: &str) -> Result<(), OnboardError> {
        let state = self
            .log
            .replay_domain(Some(ws), QUOTA_NS, QuotaState::default(), QuotaState::apply)
            .map_err(|e| OnboardError::Register(e.to_string()))?;
        let mut registry = AccountRegistry::from_state(&state);
        let event = RegisterAccount {
            account: account.to_string(),
            config_dir: config_dir.to_string(),
            now_secs: now_secs(),
        }
        .execute(&mut registry)
        .map_err(|e| OnboardError::Register(format!("{e}")))?;
        self.log
            .append(Some(ws), event)
            .map_err(|e| OnboardError::Register(e.to_string()))?;
        Ok(())
    }
}

/// Run `claude auth status --json` in `dir` and extract the logged-in account's email.
async fn claude_auth_status_email(dir: &Path) -> Result<String, OnboardError> {
    let out = TokioCommand::new(claude_bin())
        .args(["auth", "status", "--json"])
        .env("CLAUDE_CONFIG_DIR", dir)
        .output()
        .await
        .map_err(|e| OnboardError::Spawn(e.to_string()))?;
    if !out.status.success() {
        return Err(OnboardError::NotLoggedIn(
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ));
    }
    let raw = String::from_utf8_lossy(&out.stdout);
    let status: serde_json::Value = serde_json::from_str(raw.trim())
        .map_err(|e| OnboardError::NotLoggedIn(format!("parse auth status: {e}")))?;
    if status.get("loggedIn").and_then(|v| v.as_bool()) != Some(true) {
        return Err(OnboardError::NotLoggedIn("loggedIn != true".into()));
    }
    status
        .get("email")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| OnboardError::NotLoggedIn("no email in auth status".into()))
}

/// The onboarding router: `POST /api/v1/quota/onboard/{start,complete}`, auth + scope enforced in
/// each handler (the composition root merges this into the top-level app like the terminal router).
pub fn onboard_router(state: OnboardState) -> Router {
    Router::new()
        .route("/api/v1/quota/onboard/start", post(start_handler))
        .route("/api/v1/quota/onboard/complete", post(complete_handler))
        .with_state(state)
}

async fn start_handler(State(st): State<OnboardState>, headers: HeaderMap) -> Response {
    if let Err((status, msg)) = authorize(&st, &headers) {
        return (status, msg).into_response();
    }
    match st.start().await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn complete_handler(
    State(st): State<OnboardState>,
    headers: HeaderMap,
    Json(req): Json<CompleteRequest>,
) -> Response {
    let claims = match authorize(&st, &headers) {
        Ok(claims) => claims,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    match st.complete(&claims.workspace, req).await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
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

    #[test]
    fn with_select_account_appends_once() {
        assert_eq!(
            with_select_account("https://x/authorize?code=true&client_id=a"),
            "https://x/authorize?code=true&client_id=a&prompt=select_account"
        );
        // Idempotent / respects an existing prompt.
        assert_eq!(
            with_select_account("https://x/authorize?prompt=login"),
            "https://x/authorize?prompt=login"
        );
    }

    #[test]
    fn extract_url_pulls_https_token() {
        assert_eq!(
            extract_url("Visit https://claude.ai/oauth?code=1 to log in"),
            Some("https://claude.ai/oauth?code=1".to_string())
        );
        assert_eq!(extract_url("no url here"), None);
    }

    #[test]
    fn has_scope_accepts_exact_and_wildcard() {
        assert!(has_scope(&["quota.write".into()], REQUIRED_SCOPE));
        assert!(has_scope(&["*".into()], REQUIRED_SCOPE));
        assert!(!has_scope(&["quota.read".into()], REQUIRED_SCOPE));
        assert!(!has_scope(&[], REQUIRED_SCOPE));
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
            "a=1; gt_web_token=xyz; b=2".parse().unwrap(),
        );
        assert_eq!(cookie(&h, TOKEN_COOKIE).as_deref(), Some("xyz"));
    }
}
