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
use std::path::{Path, PathBuf};

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

use std::sync::Arc;

use gt_agent::SessionRegistry;
use gt_auth::JwtClaims;
use gt_polecat::tmux_server_name;
use gt_quota::QuotaState;
use gt_skills::SkillState;

use crate::auth::SharedAuthenticator;
use crate::denial_audit::{record_denial, SharedAudit, ANONYMOUS};
use crate::mcp::eventlog::EventLog;

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
    /// Truthy ⇒ attach read-write (can type into the session, creating it if absent); default ⇒ a
    /// read-only attach. Carried as a raw string (not `bool`) because `serde_urlencoded` only
    /// parses `true`/`false` for a `bool`, so a browser `?write=1` would 400 the upgrade
    /// (`hq-term-dock.1`); [`is_truthy`] interprets `1`/`true`/`yes`/`on` here instead.
    #[serde(default)]
    write: Option<String>,
}

/// Interpret a query flag as truthy: `1`/`true`/`yes`/`on` (case-insensitive). Anything else —
/// including absent — is false. Lets `?write=1` and `?write=true` both opt into an interactive
/// attach (`hq-term-dock.1`).
fn is_truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Resolve the active claude account's `CLAUDE_CONFIG_DIR` for `workspace` by replaying the quota
/// domain from the event log (`hq-term-dock.4`). Active = the most recent rotation target, else the
/// first registered account (`BTreeMap` ⇒ deterministic). `None` when nothing is registered or the
/// log can't be read — the caller then falls back to a bare shell. This is the container-reachable
/// source of the active account (the linux keychain's live pointer lives in the host keyring).
fn active_claude_config_dir(log: &EventLog, workspace: &str) -> Option<String> {
    let state = log
        .replay_domain(Some(workspace), "quota.", QuotaState::default(), QuotaState::apply)
        .ok()?;
    if state.registered.is_empty() {
        return None;
    }
    let active = state
        .rotations
        .last()
        .map(|(_, to)| to.clone())
        .filter(|a| state.registered.contains_key(a))
        .or_else(|| state.registered.keys().next().cloned())?;
    state.registered.get(&active).cloned().filter(|d| !d.is_empty())
}

/// Materialise the session's ROLE skills into a per-session workdir and return it, so the launched
/// claude loads them from `<workdir>/.claude/skills/` (`hq-role-skills-term.3`). "A role = its
/// skills": resolve the session's role from the agent log → the role's enabled skills + their
/// `SKILL.md` bodies from the skills catalog → write each `<workdir>/.claude/skills/<id>/SKILL.md` →
/// seed claude trust for the workdir (via the account `config_dir`) so it opens without the
/// trust-folder prompt. `None` when the session has no role, the role enables no skills (with a
/// body), or every write fails — the caller then launches claude in the default dir.
fn prepare_role_skills(
    log: &EventLog,
    term_root: &Path,
    workspace: &str,
    session: &str,
    config_dir: &str,
) -> Option<PathBuf> {
    let role = log
        .replay_domain(
            Some(workspace),
            "agent.",
            SessionRegistry::default(),
            SessionRegistry::apply,
        )
        .ok()?
        .get(session)?
        .role
        .as_str()
        .to_string();
    let catalog = log
        .replay_domain(Some(workspace), "skills.", SkillState::default(), SkillState::apply)
        .ok()?
        .catalog;
    let skill_ids = catalog.skills_for_role(&role);
    if skill_ids.is_empty() {
        return None;
    }
    let workdir = term_root.join(session);
    let skills_dir = workdir.join(".claude").join("skills");
    let mut wrote = 0usize;
    for id in &skill_ids {
        let Some(skill) = catalog.get(id) else { continue };
        if skill.body.trim().is_empty() {
            continue; // a binding with no SKILL.md body has nothing to materialise
        }
        let dir = skills_dir.join(id);
        if std::fs::create_dir_all(&dir).is_ok()
            && std::fs::write(dir.join("SKILL.md"), &skill.body).is_ok()
        {
            wrote += 1;
        }
    }
    if wrote == 0 {
        return None;
    }
    // Trust the workdir so claude opens without the interactive trust-folder prompt.
    crate::worktree::seed_claude_onboarding(Path::new(config_dir), &workdir);
    Some(workdir)
}

/// What the upgraded socket bridges its pty to.
enum TerminalTarget {
    /// A fresh login shell (no `session` param).
    Shell,
    /// Attach to a running tmux `session` on the workspace's tmux server, read-only unless `write`.
    /// `claude_config_dir` (`hq-term-dock.4`): when `write` and `Some`, the interactive session
    /// launches `claude` with that account profile's `CLAUDE_CONFIG_DIR` instead of a bare shell.
    /// `workdir` (`hq-role-skills-term.3`): when `Some`, claude is launched there (`tmux -c`) so it
    /// loads the role's materialised `.claude/skills/`.
    Attach {
        workspace: String,
        session: String,
        write: bool,
        claude_config_dir: Option<String>,
        workdir: Option<String>,
    },
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
    /// The per-workspace event log, replayed to resolve the active claude account's
    /// `CLAUDE_CONFIG_DIR` when launching an interactive session (`hq-term-dock.4`). `None` ⇒ a
    /// session attach opens a bare shell instead of claude.
    accounts: Option<Arc<EventLog>>,
}

impl TerminalState {
    /// Bundle the shared verifier + audit sink for the terminal router.
    pub fn new(authenticator: SharedAuthenticator, audit: SharedAudit) -> Self {
        Self { authenticator, audit, accounts: None }
    }

    /// Wire the event log used to resolve the active claude account (`hq-term-dock.4`): an
    /// interactive session attach then launches `claude` with that account's `CLAUDE_CONFIG_DIR`.
    pub fn with_active_accounts(mut self, log: Arc<EventLog>) -> Self {
        self.accounts = Some(log);
        self
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
    let write = is_truthy(params.write.as_deref());
    let target = match params.session.as_deref() {
        None => TerminalTarget::Shell,
        Some(raw) => match sanitize_session(raw) {
            Some(session) => {
                // For an interactive session, resolve the active claude account's config dir so the
                // session launches an authenticated `claude` (hq-term-dock.4); absent ⇒ bare shell.
                let claude_config_dir = if write {
                    state
                        .accounts
                        .as_ref()
                        .and_then(|log| active_claude_config_dir(log, &claims.workspace))
                } else {
                    None
                };
                // With an account resolved, materialise the session's ROLE skills into a workdir
                // and launch claude there (hq-role-skills-term.3), so it loads the role's skills.
                let workdir = match (write, &claude_config_dir, &state.accounts) {
                    (true, Some(dir), Some(log)) => {
                        let root = std::env::var("GT_EVENTLOG_ROOT")
                            .unwrap_or_else(|_| "/var/lib/gt-core".to_string());
                        let term_root = PathBuf::from(root).join("term");
                        prepare_role_skills(log, &term_root, &claims.workspace, session, dir)
                            .map(|p| p.display().to_string())
                    }
                    _ => None,
                };
                TerminalTarget::Attach {
                    workspace: claims.workspace.clone(),
                    session: session.to_string(),
                    write,
                    claude_config_dir,
                    workdir,
                }
            }
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
        TerminalTarget::Attach { workspace, session, write, claude_config_dir, workdir } => {
            let mut cmd = CommandBuilder::new("tmux");
            cmd.arg("-L");
            cmd.arg(tmux_server_name(workspace));
            if *write {
                // Interactive: attach-OR-CREATE (`hq-session-terminal.1`). A session merely
                // *recorded* (a manual `agent.spawn`) has no tmux yet; `new-session -A` creates it
                // on first open, or attaches if a live agent already holds it.
                cmd.arg("new-session");
                cmd.arg("-A");
                cmd.arg("-s");
                cmd.arg(session);
                // Start in the role's materialised workdir (hq-role-skills-term.3) so claude loads
                // its `.claude/skills/`. Only set when role skills were materialised.
                if let Some(wd) = workdir {
                    cmd.arg("-c");
                    cmd.arg(wd);
                }
                // With an active claude account resolved (hq-term-dock.4), launch `claude` under
                // that account's CLAUDE_CONFIG_DIR (set on the new session via tmux `-e`) instead of
                // a bare shell — so opening a session drops into an authenticated claude. The
                // `--dangerously-skip-permissions` bypass is configured globally in the account
                // profile, so the bare `claude` is enough. The config dir comes from the trusted
                // quota log, not user input — no shell, exec args only.
                if let Some(dir) = claude_config_dir {
                    cmd.arg("-e");
                    cmd.arg(format!("CLAUDE_CONFIG_DIR={dir}"));
                    // claude 2.x refuses its global bypass-permissions mode under root ("cannot be
                    // used with root/sudo privileges") and exits immediately; the mcp-server
                    // container runs as root with no non-root user, so signal a sandboxed env
                    // (`IS_SANDBOX=1`), which claude honours to allow it (`hq-term-dock.5`).
                    cmd.arg("-e");
                    cmd.arg("IS_SANDBOX=1");
                    cmd.arg("claude");
                }
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
    fn is_truthy_accepts_one_and_true_rejects_others() {
        // hq-term-dock.1: a browser sends ?write=1, which must opt into interactive — the original
        // bool field 400'd it. Also accept true/yes/on; absent/0/false stay read-only.
        for t in ["1", "true", "TRUE", "yes", "on", " 1 "] {
            assert!(is_truthy(Some(t)), "{t:?} should be truthy");
        }
        for f in ["0", "false", "no", "off", "", "x"] {
            assert!(!is_truthy(Some(f)), "{f:?} should be falsey");
        }
        assert!(!is_truthy(None), "absent ⇒ read-only");
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
            claude_config_dir: None,
            workdir: None,
        });
        let argv: Vec<String> = ro.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(
            argv,
            vec!["tmux", "-L", "gt-acme", "attach-session", "-t", "hq-gg-1", "-r"]
        );
        // Write mode without an account attaches-OR-CREATES a bare shell (hq-session-terminal.1).
        let rw = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: None,
            workdir: None,
        });
        let argv: Vec<String> = rw.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(argv, vec!["tmux", "-L", "gt-acme", "new-session", "-A", "-s", "hq-gg-1"]);
        // Write mode WITH an active account + a role workdir: claude under that profile, started in
        // the workdir so it loads the role's skills (hq-term-dock.4 + hq-role-skills-term.3).
        let cl = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: Some("/var/lib/gt-core/accounts/abc".into()),
            workdir: Some("/var/lib/gt-core/term/hq-gg-1".into()),
        });
        let argv: Vec<String> = cl.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(
            argv,
            vec![
                "tmux", "-L", "gt-acme", "new-session", "-A", "-s", "hq-gg-1",
                "-c", "/var/lib/gt-core/term/hq-gg-1",
                "-e", "CLAUDE_CONFIG_DIR=/var/lib/gt-core/accounts/abc",
                "-e", "IS_SANDBOX=1", "claude"
            ]
        );
        // No session ⇒ the original fresh shell.
        let sh = build_command(&TerminalTarget::Shell);
        let argv: Vec<String> = sh.get_argv().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(argv, vec!["/bin/sh"]);
    }

    #[test]
    fn prepare_role_skills_materialises_the_roles_skill_md() {
        // hq-role-skills-term.3: the session's role → its enabled skills (with bodies) → written as
        // <workdir>/.claude/skills/<id>/SKILL.md so claude loads them.
        use gt_agent::AgentEvent;
        use gt_skills::SkillEvent;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        // A witness session.
        log.append(
            Some("acme"),
            AgentEvent::Spawned {
                session: "w1".into(),
                rig: "hq".into(),
                role: gt_agent::SessionRole::Dog(gt_agent::DogKind::Witness),
                crew: None,
                skills: vec![],
                hooks: vec![],
            },
        )
        .unwrap();
        // A skill WITH a body, enabled for witness.
        log.append(
            Some("acme"),
            SkillEvent::Registered {
                skill: "pr-list".into(),
                label: "PR list".into(),
                description: "list PRs".into(),
                default_scopes: vec![],
                body: "# PR list\nlist open PRs".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        log.append(
            Some("acme"),
            SkillEvent::EnabledForRole { role: "witness".into(), skill: "pr-list".into(), now_secs: 2 },
        )
        .unwrap();

        let term_root = dir.path().join("term");
        let cfg = dir.path().join("cfg"); // a throwaway CLAUDE_CONFIG_DIR for trust seeding
        std::fs::create_dir_all(&cfg).unwrap();
        let wd = prepare_role_skills(&log, &term_root, "acme", "w1", cfg.to_str().unwrap())
            .expect("witness has an enabled skill with a body");
        let skill_md = wd.join(".claude").join("skills").join("pr-list").join("SKILL.md");
        assert_eq!(std::fs::read_to_string(&skill_md).unwrap(), "# PR list\nlist open PRs");

        // A role with no enabled skills ⇒ None (claude launches in the default dir).
        log.append(
            Some("acme"),
            AgentEvent::Spawned {
                session: "m1".into(),
                rig: "hq".into(),
                role: gt_agent::SessionRole::Mayor,
                crew: None,
                skills: vec![],
                hooks: vec![],
            },
        )
        .unwrap();
        assert!(prepare_role_skills(&log, &term_root, "acme", "m1", cfg.to_str().unwrap()).is_none());
    }

    #[tokio::test]
    async fn active_claude_config_dir_resolves_from_the_quota_log() {
        // hq-term-dock.4: replay the quota domain → the active account's CLAUDE_CONFIG_DIR. Active
        // is the last rotation target, else the first registered account.
        use gt_quota::QuotaEvent;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            QuotaEvent::AccountRegistered { account: "acct-a".into(), config_dir: "/dirs/a".into(), now_secs: 0 },
        )
        .unwrap();
        log.append(
            Some("acme"),
            QuotaEvent::AccountRegistered { account: "acct-b".into(), config_dir: "/dirs/b".into(), now_secs: 0 },
        )
        .unwrap();
        // No rotation yet → first registered (BTreeMap order: acct-a).
        assert_eq!(active_claude_config_dir(&log, "acme").as_deref(), Some("/dirs/a"));
        // After rotating to b → b is active.
        log.append(
            Some("acme"),
            QuotaEvent::Rotated { from_account: "acct-a".into(), to_account: "acct-b".into(), now_secs: 1 },
        )
        .unwrap();
        assert_eq!(active_claude_config_dir(&log, "acme").as_deref(), Some("/dirs/b"));
        // A workspace with no accounts resolves to None (caller falls back to a shell).
        assert!(active_claude_config_dir(&log, "empty").is_none());
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
