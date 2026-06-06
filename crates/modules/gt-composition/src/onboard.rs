//! Web-driven claude-account onboarding on the idle daemon (`hq-quota-onboard-web.1`).
//!
//! The operator should not hand-pick an account id or type a credentials path (the old qa.5 form).
//! Instead the web "Add account" flow drives the real `claude auth login` lifecycle on the HOST,
//! where `claude` actually lives (it is not in the mcp-server container), split across two HTTP
//! calls because the OAuth handshake has a human in the middle:
//!
//! 1. **`POST /onboard/start`** — allocate a generic `CLAUDE_CONFIG_DIR` under the accounts root
//!    (`account_dirs`, qa.3), spawn `claude auth login` into it with piped stdio, read the login
//!    URL it prints, and keep the process **alive** (stdin open) in a session map. Returns
//!    `{session_id, url}`; the web client shows the URL for the human to visit + authenticate.
//! 2. **`POST /onboard/complete {session_id, code}`** — write the OOB code the human pasted back to
//!    the live process's stdin, wait for it to exit, read the account identity from
//!    `claude auth status --json`, and register it in-process via the daemon's [`QuotaHandle`]
//!    (the same event-sourced `quota.account_registered.v1` the keychain hydrates from). The id is
//!    the login's `email` — captured from the handshake, never typed.
//!
//! This is the `gt quota onboard` (qa.7) capture logic ported SPLIT into start/complete; the hard
//! part is keeping the claude process alive between the two HTTP calls with stdin open. The daemon
//! serves this even with no dispatch (idle) — onboarding costs nothing, it spawns no polecat.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use ulid::Ulid;

use gt_quota::QuotaHandle;

use crate::account_dirs::account_config_dir;

/// How long `start` waits for the login URL to appear on the spawned process's output before giving
/// up and killing it.
const URL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long `complete` waits for the login process to exit after the code is written.
const EXIT_TIMEOUT: Duration = Duration::from_secs(120);

/// The `claude` binary; `GT_CLAUDE_BIN` overrides (it may live at `~/.local/bin/claude`, off the
/// daemon's PATH). Mirrors `gt quota onboard`.
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
/// stdin (where the OOB code is written in `complete`), and the credentials dir it logs into. The
/// reader tasks keep draining stdout/stderr so the process never blocks on a full pipe; they end at
/// EOF when the process exits. `kill_on_drop` reaps the child if a session is dropped un-completed.
struct LiveOnboard {
    child: Child,
    stdin: ChildStdin,
    dir: PathBuf,
    _readers: [JoinHandle<()>; 2],
}

/// Everything the onboarding handlers need: the live-session map, the daemon's quota handle (to
/// register the captured account in-process — no MCP hop), and the accounts root the generic
/// per-onboarding dir is allocated under.
#[derive(Clone)]
pub struct OnboardState {
    sessions: Arc<Mutex<HashMap<String, LiveOnboard>>>,
    quota: QuotaHandle,
    accounts_root: Arc<Path>,
}

impl OnboardState {
    pub fn new(quota: QuotaHandle, accounts_root: PathBuf) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            quota,
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
    /// Could not allocate/create the credentials dir.
    Dir(String),
    /// Spawning `claude` failed (binary missing, etc).
    Spawn(String),
    /// The login URL never appeared (process printed no URL / closed its output).
    NoUrl,
    /// Timed out waiting for the URL, or for the process to exit after the code.
    Timeout(&'static str),
    /// No such (or already-completed) session.
    UnknownSession,
    /// Writing the code to the live process failed.
    Stdin(String),
    /// The login process exited non-zero (bad/expired code).
    LoginFailed(String),
    /// `claude auth status --json` did not report a logged-in account.
    NotLoggedIn(String),
}

impl IntoResponse for OnboardError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            OnboardError::Dir(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("credentials dir: {e}")),
            OnboardError::Spawn(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn claude: {e}")),
            OnboardError::NoUrl => (
                StatusCode::BAD_GATEWAY,
                "claude auth login produced no login URL".to_string(),
            ),
            OnboardError::Timeout(what) => (StatusCode::GATEWAY_TIMEOUT, format!("timed out: {what}")),
            OnboardError::UnknownSession => {
                (StatusCode::NOT_FOUND, "unknown or completed onboarding session".to_string())
            }
            OnboardError::Stdin(e) => (StatusCode::BAD_GATEWAY, format!("write code to login: {e}")),
            OnboardError::LoginFailed(e) => (StatusCode::BAD_GATEWAY, format!("login failed: {e}")),
            OnboardError::NotLoggedIn(e) => (StatusCode::BAD_GATEWAY, format!("login incomplete: {e}")),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

/// Spawn a task that reads `reader` line by line into `tx`, ending silently at EOF. Keeps the
/// process's pipe drained so it never blocks on full output.
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

/// Pull the first `https://…` token out of a line, if any.
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
    pub async fn start(&self) -> Result<StartResponse, OnboardError> {
        let session_id = Ulid::new().to_string();
        // The session id is a ULID (no separators) so the sanitizing join always succeeds; the dir
        // is generic storage, the real account id comes from the handshake in `complete`.
        let dir = account_config_dir(&self.accounts_root, &session_id)
            .ok_or_else(|| OnboardError::Dir("session id rejected by sanitizer".into()))?;
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| OnboardError::Dir(e.to_string()))?;

        let mut cmd = Command::new(claude_bin());
        cmd.args(["auth", "login"])
            .env("CLAUDE_CONFIG_DIR", &dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| OnboardError::Spawn(e.to_string()))?;

        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take().expect("stderr piped");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let readers = [
            spawn_line_reader(stdout, tx.clone()),
            spawn_line_reader(stderr, tx),
        ];

        // Read output until a URL appears (claude prints it to stdout or stderr) or we time out.
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
        Ok(StartResponse { session_id, url })
    }

    /// Finish a login: feed the OOB code to the live process, wait for exit, read the account
    /// identity, and register it.
    pub async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, OnboardError> {
        // Take the session out of the map (drops the std Mutex guard before any await).
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

        // Write the code and close stdin so claude proceeds past the prompt.
        let line = format!("{}\n", req.code.trim());
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| OnboardError::Stdin(e.to_string()))?;
        stdin.flush().await.map_err(|e| OnboardError::Stdin(e.to_string()))?;
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
            return Err(OnboardError::LoginFailed(format!("claude auth login exited {status}")));
        }

        let account = claude_auth_status_email(&dir).await?;
        self.quota
            .register_account(account.clone(), dir.display().to_string(), now_secs())
            .await;
        Ok(CompleteResponse {
            account,
            config_dir: dir.display().to_string(),
        })
    }
}

/// Run `claude auth status --json` in `dir` and extract the logged-in account's email.
async fn claude_auth_status_email(dir: &Path) -> Result<String, OnboardError> {
    let out = Command::new(claude_bin())
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

/// The onboarding router: `POST /onboard/start` + `POST /onboard/complete`.
pub fn onboard_router(state: OnboardState) -> Router {
    Router::new()
        .route("/onboard/start", post(start_handler))
        .route("/onboard/complete", post(complete_handler))
        .with_state(state)
}

async fn start_handler(State(st): State<OnboardState>) -> Response {
    match st.start().await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn complete_handler(
    State(st): State<OnboardState>,
    Json(req): Json<CompleteRequest>,
) -> Response {
    match st.complete(req).await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_url_pulls_https_token() {
        assert_eq!(
            extract_url("Visit https://claude.ai/oauth?code=1 to log in"),
            Some("https://claude.ai/oauth?code=1".to_string())
        );
        assert_eq!(extract_url("no url here"), None);
        assert_eq!(
            extract_url("  http://localhost:1/x"),
            Some("http://localhost:1/x".to_string())
        );
    }
}
