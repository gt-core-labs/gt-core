//! Interactive PTY terminal over a WebSocket (`hq-terminal`).
//!
//! Exposes `GET /api/v1/terminal/ws`: a browser (xterm.js) opens a WebSocket, the handler
//! spawns `/bin/sh` on a pseudo-terminal **inside this server process**, and bridges the two
//! — keystrokes (`Binary`/`Text` frames) into the pty, pty output back out as `Binary` frames.
//! A `Text` frame shaped `{"resize":{"cols":N,"rows":M}}` resizes the pty instead of being
//! forwarded as input.
//!
//! ## Security — this is remote shell execution
//!
//! The spawned shell runs with the privileges of the server process: the event-log root, the
//! mounted RS256/ACME secrets, and the Dolt/PG/MinIO credentials in the environment are all
//! reachable. The surface is therefore gated three ways, and the composition root
//! ([`gt-mcp-server`](crate)) only mounts this router when **all** hold:
//!
//! - **Authn** — the same RS256 verifier the `/mcp` transport and SSE feed use, accepting a
//!   `gt_web_token` cookie *or* an `Authorization: Bearer` token. Anonymous never reaches a pty.
//! - **Authz** — the verified claim must carry the [`REQUIRED_SCOPE`] (`terminal.exec`) or the
//!   `*` superuser grant. `terminal.exec` is intentionally **not** in any non-admin preset.
//! - **Env gate** — the binary builds this router only when `GT_TERMINAL_ENABLE` is truthy, so a
//!   default deploy serves no terminal at all (the route 404s).
//!
//! Every rejected upgrade is recorded into the shared audit sink (`terminal.exec` denial), the
//! same way the REST chain and SSE feed audit theirs.

use std::io::{Read, Write};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use gt_auth::JwtClaims;
use gt_polecat::tmux_server_name;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};

/// The browser-nav JWT cookie the upgrade authenticates by (same as the SSE feed). A bearer
/// header is accepted too, for non-browser clients.
const TOKEN_COOKIE: &str = "gt_web_token";

/// The scope a caller must hold (besides the `*` wildcard) to open a terminal. Closed-vocabulary
/// verb `exec` (gt-rbac), admin-only by convention — never granted to a non-admin preset.
const REQUIRED_SCOPE: &str = "terminal.exec";

/// The superuser grant that authorizes every scope (the seeded admin carries it).
const SCOPE_WILDCARD: &str = "*";

/// The route the terminal upgrade lives at — also the audited path on a denial.
const TERMINAL_PATH: &str = "/api/v1/terminal/ws";

/// Query params on the terminal upgrade (`hq-agent-observability.5`). `session` attaches the pty to
/// a running agent's tmux instead of spawning a fresh shell, so an operator can watch what the
/// agent is doing live; `write` opts into an interactive (non-read-only) attach.
#[derive(Debug, Default, Deserialize)]
struct TerminalParams {
    /// The tmux session to attach to (a polecat's `<prefix>-<bead>`). Absent ⇒ a fresh `/bin/sh`,
    /// the original behaviour.
    session: Option<String>,
    /// `true` ⇒ attach read-write (can type into the agent's session); default `false` ⇒ a
    /// read-only attach, so watching never disturbs the agent.
    #[serde(default)]
    write: bool,
}

/// What the upgraded socket bridges its pty to.
enum TerminalTarget {
    /// A fresh login shell (no `session` param).
    Shell,
    /// Attach to a running tmux `session` on the workspace's tmux server, read-only unless `write`.
    Attach { workspace: String, session: String, write: bool },
}

/// Validate a tmux session name from an untrusted query param. The name is passed to `tmux` as a
/// **separate exec arg** (no shell), so the only injection risk is a value that `tmux` would read as
/// a flag; we therefore allow only `[A-Za-z0-9._-]`, forbid a leading `-`, and cap the length.
/// `None` ⇒ reject the upgrade rather than attach to an attacker-chosen target.
fn sanitize_session(name: &str) -> Option<&str> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('-')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    ok.then_some(name)
}

/// Everything the upgrade handler needs: the JWT verifier and the audit sink denials are
/// recorded into. Built by the composition root only when auth + the env gate are on.
#[derive(Clone)]
pub struct TerminalState {
    authenticator: SharedAuthenticator,
    audit: SharedAudit,
}

impl TerminalState {
    /// Bundle the shared verifier + audit sink for the terminal router.
    pub fn new(authenticator: SharedAuthenticator, audit: SharedAudit) -> Self {
        Self { authenticator, audit }
    }
}

/// Build the terminal router (`GET /api/v1/terminal/ws`) with `state` baked in. The composition
/// root merges this into the top-level app exactly like the SSE `feed_router`.
pub fn terminal_router(state: TerminalState) -> Router {
    Router::new()
        .route(TERMINAL_PATH, get(ws_upgrade))
        .with_state(state)
}

/// Authenticate + authorize **before** completing the upgrade: a rejected caller gets a
/// `401`/`403` HTTP response, not an opened socket.
async fn ws_upgrade(
    State(state): State<TerminalState>,
    headers: HeaderMap,
    Query(params): Query<TerminalParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let claims = match authorize(&state, &headers) {
        Ok(claims) => claims,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    // Resolve the pty target: a `session` param attaches to that agent's tmux (read-only unless
    // `write`), keyed to the caller's own workspace so one tenant can never attach to another's
    // session; no param keeps the original fresh-shell behaviour. A present-but-malformed session
    // is rejected (400) rather than silently downgraded to a shell.
    let target = match params.session.as_deref() {
        None => TerminalTarget::Shell,
        Some(raw) => match sanitize_session(raw) {
            Some(session) => TerminalTarget::Attach {
                workspace: claims.workspace.clone(),
                session: session.to_string(),
                write: params.write,
            },
            None => {
                return (StatusCode::BAD_REQUEST, "invalid session name").into_response();
            }
        },
    };
    ws.on_upgrade(move |socket| run_pty(socket, target))
}

/// Verify the token (cookie or bearer), gate its clock/workspace invariants, and require the
/// `terminal.exec` scope (or `*`). Every failure audits an unauthenticated/under-scoped denial
/// and returns the status + body the upgrade responds with.
fn authorize(
    state: &TerminalState,
    headers: &HeaderMap,
) -> Result<JwtClaims, (StatusCode, &'static str)> {
    let reject = |status: StatusCode, msg: &'static str| -> (StatusCode, &'static str) {
        record_denial(
            state.audit.as_ref(),
            ANONYMOUS,
            None,
            &Method::GET,
            &TERMINAL_PATH.parse().expect("static uri"),
            Some(REQUIRED_SCOPE),
            status,
        );
        (status, msg)
    };

    let token = bearer(headers)
        .or_else(|| cookie(headers, TOKEN_COOKIE))
        .ok_or_else(|| reject(StatusCode::UNAUTHORIZED, "missing gt_web_token cookie or bearer"))?;

    let claims = state
        .authenticator
        .authenticate(&token)
        .map_err(|_| reject(StatusCode::UNAUTHORIZED, "invalid token"))?;

    // Signature verified; gate the clock + workspace-presence invariants the verifier defers.
    let now = OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    if claims
        .validate(now, JwtClaims::workspace_optional_from_env())
        .is_err()
    {
        return Err(reject(StatusCode::UNAUTHORIZED, "expired or incomplete token"));
    }

    if !has_scope(&claims.scopes, REQUIRED_SCOPE) {
        return Err(reject(StatusCode::FORBIDDEN, "missing terminal.exec scope"));
    }
    Ok(claims)
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes
        .iter()
        .any(|s| s == SCOPE_WILDCARD || s == required)
}

/// Build the pty command for a [`TerminalTarget`]: a plain `/bin/sh`, or a `tmux attach` to the
/// workspace's tmux server for the named session (read-only unless `write`). Both run with a
/// `xterm-256color` `TERM`. The session name is pre-sanitized; the workspace comes from the
/// verified claim — neither is shell-interpolated (exec args), so there is no injection surface.
fn build_command(target: &TerminalTarget) -> CommandBuilder {
    match target {
        TerminalTarget::Shell => {
            let mut cmd = CommandBuilder::new("/bin/sh");
            cmd.env("TERM", "xterm-256color");
            cmd
        }
        TerminalTarget::Attach { workspace, session, write } => {
            let mut cmd = CommandBuilder::new("tmux");
            cmd.arg("-L");
            cmd.arg(tmux_server_name(workspace));
            if *write {
                // Interactive: attach-OR-CREATE (`hq-session-terminal.1`). A session merely
                // *recorded* (a manual `agent.spawn`) has no tmux yet; `new-session -A` creates it
                // (a fresh shell) on first open, or attaches if a live agent already holds it — so
                // the operator can actually communicate with the session.
                cmd.arg("new-session");
                cmd.arg("-A");
                cmd.arg("-s");
                cmd.arg(session);
            } else {
                // Read-only: attach to an EXISTING session to watch a live agent without
                // disturbing it (closes cleanly if the session does not exist).
                cmd.arg("attach-session");
                cmd.arg("-t");
                cmd.arg(session);
                cmd.arg("-r");
            }
            cmd.env("TERM", "xterm-256color");
            cmd
        }
    }
}

/// Bridge an upgraded socket to a pty until either side closes. The pty runs a fresh `/bin/sh`
/// ([`TerminalTarget::Shell`]) or a `tmux attach` to a running agent's session
/// ([`TerminalTarget::Attach`], `hq-agent-observability.5`).
///
/// A blocking std thread drains the pty master into a channel (the master reader is a blocking
/// `Read`); the async loop multiplexes that channel onto the socket while forwarding inbound
/// frames into the pty writer. On socket close, pty EOF, or any I/O error the child is killed
/// and every handle dropped.
async fn run_pty(mut socket: WebSocket, target: TerminalTarget) {
    let pair = match native_pty_system().openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(pair) => pair,
        Err(_) => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    let cmd = build_command(&target);
    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(child) => child,
        Err(_) => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    // The slave is held open by the child; drop our handle so the master sees EOF on exit.
    drop(pair.slave);

    let mut reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(_) => {
            let _ = child.kill();
            return;
        }
    };
    let mut writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(_) => {
            let _ = child.kill();
            return;
        }
    };
    let master = pair.master; // kept for resize control

    // Blocking pty-reader → bounded channel. Bounded so a slow client backpressures the shell
    // rather than letting output buffer unboundedly.
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(64);
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if out_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            chunk = out_rx.recv() => match chunk {
                Some(bytes) => {
                    if socket.send(Message::Binary(bytes)).await.is_err() {
                        break;
                    }
                }
                None => break, // pty closed (shell exited)
            },
            frame = socket.recv() => match frame {
                Some(Ok(Message::Binary(b))) => {
                    if writer.write_all(&b).is_err() {
                        break;
                    }
                    let _ = writer.flush();
                }
                Some(Ok(Message::Text(t))) => {
                    if let Some(size) = parse_resize(&t) {
                        let _ = master.resize(size);
                    } else {
                        if writer.write_all(t.as_bytes()).is_err() {
                            break;
                        }
                        let _ = writer.flush();
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {} // Ping/Pong handled by axum
            },
        }
    }

    let _ = child.kill();
    drop(writer);
    drop(master);
}

/// Parse a `{"resize":{"cols":N,"rows":M}}` control frame into a [`PtySize`]; `None` for any
/// other text (which is then forwarded to the shell as input).
fn parse_resize(text: &str) -> Option<PtySize> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let r = v.get("resize")?;
    let cols = r.get("cols")?.as_u64()? as u16;
    let rows = r.get("rows")?.as_u64()? as u16;
    Some(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    })
}

/// The bearer token from an `Authorization: Bearer <jwt>` header, trimmed and non-empty.
fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    let token = raw.strip_prefix("Bearer ").or_else(|| raw.strip_prefix("bearer "))?;
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// Value of a named cookie from the `Cookie` header (the token is the only cookie read here).
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
        assert!(has_scope(&["terminal.exec".into()], REQUIRED_SCOPE));
        assert!(has_scope(&["*".into()], REQUIRED_SCOPE));
        assert!(!has_scope(&["agent.write".into()], REQUIRED_SCOPE));
        assert!(!has_scope(&[], REQUIRED_SCOPE));
    }

    #[test]
    fn sanitize_session_allows_polecat_names_and_rejects_injection() {
        // A real polecat session name passes.
        assert_eq!(sanitize_session("hq-gg-1"), Some("hq-gg-1"));
        assert_eq!(sanitize_session("hq-agent-observability.5"), Some("hq-agent-observability.5"));
        // Empty, flag-like, or shell-meta names are rejected (never reach tmux as a flag/arg).
        assert!(sanitize_session("").is_none());
        assert!(sanitize_session("-rkill").is_none(), "leading dash could be read as a flag");
        assert!(sanitize_session("a b").is_none());
        assert!(sanitize_session("a;rm -rf /").is_none());
        assert!(sanitize_session("a$(whoami)").is_none());
        assert!(sanitize_session(&"x".repeat(129)).is_none(), "over the length cap");
    }

    #[test]
    fn build_command_attaches_read_only_by_default_and_writable_on_opt_in() {
        // Read-only attach targets the workspace's tmux server with `-r`.
        let ro = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: false,
        });
        let argv: Vec<String> = ro.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(
            argv,
            vec!["tmux", "-L", "gt-acme", "attach-session", "-t", "hq-gg-1", "-r"]
        );
        // Write mode attaches-OR-CREATES (hq-session-terminal.1): new-session -A, no -r.
        let rw = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
        });
        let argv: Vec<String> = rw.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(argv, vec!["tmux", "-L", "gt-acme", "new-session", "-A", "-s", "hq-gg-1"]);
        // No session ⇒ the original fresh shell.
        let sh = build_command(&TerminalTarget::Shell);
        let argv: Vec<String> = sh.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(argv, vec!["/bin/sh"]);
    }

    #[test]
    fn parse_resize_reads_cols_rows() {
        let s = parse_resize(r#"{"resize":{"cols":120,"rows":40}}"#).unwrap();
        assert_eq!(s.cols, 120);
        assert_eq!(s.rows, 40);
    }

    #[test]
    fn parse_resize_rejects_plain_input() {
        assert!(parse_resize("ls -la\n").is_none());
        assert!(parse_resize(r#"{"other":1}"#).is_none());
    }

    #[test]
    fn bearer_strips_prefix() {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::AUTHORIZATION, "Bearer abc.def".parse().unwrap());
        assert_eq!(bearer(&h).as_deref(), Some("abc.def"));
    }

    #[test]
    fn cookie_extracts_named_value() {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::COOKIE, "a=1; gt_web_token=xyz; b=2".parse().unwrap());
        assert_eq!(cookie(&h, TOKEN_COOKIE).as_deref(), Some("xyz"));
    }
}
