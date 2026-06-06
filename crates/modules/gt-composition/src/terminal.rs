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
        State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use time::OffsetDateTime;
use tokio::sync::mpsc;

use gt_auth::JwtClaims;

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
    ws: WebSocketUpgrade,
) -> Response {
    match authorize(&state, &headers) {
        Ok(()) => ws.on_upgrade(run_pty),
        Err((status, msg)) => (status, msg).into_response(),
    }
}

/// Verify the token (cookie or bearer), gate its clock/workspace invariants, and require the
/// `terminal.exec` scope (or `*`). Every failure audits an unauthenticated/under-scoped denial
/// and returns the status + body the upgrade responds with.
fn authorize(
    state: &TerminalState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, &'static str)> {
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
    Ok(())
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes
        .iter()
        .any(|s| s == SCOPE_WILDCARD || s == required)
}

/// Bridge an upgraded socket to a freshly-spawned `/bin/sh` pty until either side closes.
///
/// A blocking std thread drains the pty master into a channel (the master reader is a blocking
/// `Read`); the async loop multiplexes that channel onto the socket while forwarding inbound
/// frames into the pty writer. On socket close, pty EOF, or any I/O error the child is killed
/// and every handle dropped.
async fn run_pty(mut socket: WebSocket) {
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

    let mut cmd = CommandBuilder::new("/bin/sh");
    cmd.env("TERM", "xterm-256color");
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
