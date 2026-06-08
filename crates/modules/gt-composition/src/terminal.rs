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
use std::time::{SystemTime, UNIX_EPOCH};

use gt_agent::SessionRegistry;
use gt_auth::{JwtClaims, JwtMinter};
use gt_claude_hooks::HooksState;
use gt_polecat::tmux_server_name;
use gt_quota::{AccountQuotaStatus, QuotaState};
use gt_skills::{ModelConfig, SkillState};

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
/// Resolve the active claude account as `(account_id, config_dir)` for `workspace` by replaying the
/// quota domain (`hq-term-dock.4`). Active = the most recent rotation target (when still registered +
/// HEALTHY), else the first registered account that is HEALTHY, else any registered account
/// (`BTreeMap` ⇒ deterministic). `None` when nothing is registered or the log can't be read — the
/// caller then falls back to a bare shell. The account id lets the session's quota-feed hook label
/// its token sample (`GT_HOOK_ACCOUNT`, `hq-quota-feed`): the daemon's predictor keys consumption by
/// this id, the same id the rotation log records, so an interactive session's burn folds into the
/// very account it is spending. This is the container-reachable source of the active account (the
/// linux keychain's live pointer lives in the host keyring).
///
/// Preferring HEALTHY over alphabetical (`hq-quota-healthy`) closes the gap where a fresh log with no
/// rotation handed every new session the alphabetically-first account even after the predictor cooled
/// it: a `Rotated` parks the source in `Cooldown`, `AccountLimited`/`Blocked` mark Limited/Blocked,
/// and those statuses replay from the log here — so a cooled/limited account is skipped for a healthy
/// one. The last-resort any-account branch keeps a session launchable even if every account is marked
/// unhealthy (better a maybe-limited claude than a bare shell with no account).
fn active_claude_account(log: &EventLog, workspace: &str) -> Option<(String, String)> {
    let state = log
        .replay_domain(
            Some(workspace),
            "quota.",
            QuotaState::default(),
            QuotaState::apply,
        )
        .ok()?;
    if state.registered.is_empty() {
        return None;
    }
    // A registered account is healthy unless the log marked it Cooldown/Limited/Blocked. Every
    // `AccountRegistered` also seeds an `accounts` entry (Healthy), so a missing entry ⇒ treat as
    // healthy (defensive; should not happen).
    let is_healthy = |account: &str| {
        state
            .accounts
            .get(account)
            .map(|a| a.status == AccountQuotaStatus::Healthy)
            .unwrap_or(true)
    };
    let active = state
        .rotations
        .last()
        .map(|(_, to)| to.clone())
        .filter(|a| state.registered.contains_key(a) && is_healthy(a))
        .or_else(|| state.registered.keys().find(|a| is_healthy(a)).cloned())
        .or_else(|| state.registered.keys().next().cloned())?;
    state
        .registered
        .get(&active)
        .cloned()
        .filter(|d| !d.is_empty())
        .map(|dir| (active, dir))
}

/// Materialise the session's ROLE skills into a per-session workdir and return it, so the launched
/// claude loads them from `<workdir>/.claude/skills/` (`hq-role-skills-term.3`). "A role = its
/// skills": resolve the session's role from the agent log → the role's enabled skills + their
/// `SKILL.md` bodies from the skills catalog → write each `<workdir>/.claude/skills/<id>/SKILL.md` →
/// seed claude trust for the workdir (via the account `config_dir`) so it opens without the
/// trust-folder prompt. `None` when the session has no role, the role enables no skills (with a
/// body), or every write fails — the caller then launches claude in the default dir.
/// Substitute role-prompt placeholders with the session's context (`hq-role-prompt-render.1`).
/// Each `(token, value)` replaces every `<token>`, `{{ .token }}` (any inner spacing) and
/// `{{token}}` occurrence — covering both the seeded `<X>` form and a raw gastown `{{ .X }}` /
/// `{{ cmd }}` template. Unknown placeholders are left intact (claude reads them as literal text).
fn render_prompt(prompt: &str, vars: &[(&str, String)]) -> String {
    let mut out = prompt.to_string();
    for (token, value) in vars {
        for pat in [
            format!("<{token}>"),
            format!("{{{{ .{token} }}}}"),
            format!("{{{{.{token}}}}}"),
            format!("{{{{ {token} }}}}"),
            format!("{{{{{token}}}}}"),
        ] {
            if out.contains(&pat) {
                out = out.replace(&pat, value);
            }
        }
    }
    out
}

/// What a role contributes to its session's `claude` launch: a materialised `workdir` (when the role
/// has skills/prompt to write, `hq-role-skills-term.3`), a `model` config (`hq-role-model.1`), and a
/// `kickoff` directive (`hq-role-kickoff`) seeded as claude's first prompt so opening the session
/// *fires the role's work* instead of dropping into an idle TUI. They are independent.
#[derive(Default)]
struct RoleLaunch {
    workdir: Option<PathBuf>,
    model: Option<ModelConfig>,
    kickoff: Option<String>,
}

/// The directive seeded as a role session's first claude prompt (`hq-role-kickoff`). Mirrors
/// `gt_polecat::polecat_prompt`: a bare `claude` opens an interactive TUI and idles, so passing this
/// as the positional `[prompt]` makes the agent begin its duties autonomously. The role-specific
/// instructions live in the materialised `CLAUDE.md` (the role's system prompt); this just activates
/// them. Only seeded on session *creation* (tmux `new-session -A` ignores the command on attach), so
/// reconnecting an operator never re-fires it.
fn role_kickoff_prompt(role: &str, workspace: &str) -> String {
    format!(
        "You are the gt `{role}` in workspace `{workspace}`. Your tools come from the `gt` MCP server \
         and are named `mcp__gt__*` (e.g. `mcp__gt__issues_create`, `mcp__gt__issues_transition`, \
         `mcp__gt__agent_spawn`, `mcp__gt__convoy_launch`, `mcp__gt__merge_list`) — the dotted forms \
         `issues.*`/`agent.*` are the underlying namespaces, not the tool names. The server connects \
         at startup and may take a few seconds: if no `mcp__gt__*` tool is in your tool list on this \
         first turn, WAIT briefly and check again — do not conclude the tools are missing or search \
         for alternatives. Once they are present, begin your duties per CLAUDE.md, coordinating \
         through shared state (the tracker, channels, the event log), never directly. Work \
         autonomously and do not wait for confirmation."
    )
}

fn prepare_role_skills(
    log: &EventLog,
    term_root: &Path,
    workspace: &str,
    session: &str,
    config_dir: &str,
    minter: Option<&JwtMinter>,
    server_url: Option<&str>,
    costs_report: bool,
) -> RoleLaunch {
    let Ok(registry) = log.replay_domain(
        Some(workspace),
        "agent.",
        SessionRegistry::default(),
        SessionRegistry::apply,
    ) else {
        return RoleLaunch::default();
    };
    let Some(sess) = registry.get(session) else {
        return RoleLaunch::default();
    };
    let role = sess.role.as_str().to_string();
    let rig = sess.rig.clone();
    // The kickoff fires the role's work on session open (hq-role-kickoff); known once the role is
    // resolved, it rides every return below so claude never opens idle.
    let kickoff = Some(role_kickoff_prompt(&role, workspace));
    let Ok(state) = log.replay_domain(
        Some(workspace),
        "skills.",
        SkillState::default(),
        SkillState::apply,
    ) else {
        return RoleLaunch {
            workdir: None,
            model: None,
            kickoff,
        };
    };
    let catalog = state.catalog;
    let model = catalog.role_model(&role); // hq-role-model.1 — applies regardless of skills/prompt
    let skill_ids = catalog.skills_for_role(&role);
    let prompt = catalog.role_prompt(&role); // hq-role-skills-term.4
                                             // The GLOBAL hook registry (hq-hooks): replay the `hooks.*` stream at the `None` scope and keep
                                             // only the hooks whose target matches this session's (workspace, rig, role). `None` ⇒ nothing
                                             // matches (or the log can't be read) — no settings.json is written.
    let hooks_settings = log
        .replay_domain(None, "hooks.", HooksState::default(), HooksState::apply)
        .ok()
        .and_then(|s| s.registry.settings_json_for(workspace, &rig, &role));
    // Per-role MCP auth (hq-role-mcp) is materialisable only when both a minter and a server URL were
    // wired; when so, every role write-session gets a workdir (for `.gt-config` + `.mcp.json`).
    let role_mcp = minter.zip(server_url);
    // No skills/prompt/hooks/MCP/costs-feed to materialise → no workdir, but the model config (if
    // any) still rides. `costs_report` alone is enough to earn a workdir: a role with nothing else
    // still gets a settings.json carrying the quota-feed Stop hook (hq-quota-feed).
    if skill_ids.is_empty()
        && prompt.is_none()
        && hooks_settings.is_none()
        && role_mcp.is_none()
        && !costs_report
    {
        return RoleLaunch {
            workdir: None,
            model,
            kickoff,
        };
    }
    let workdir = term_root.join(session);
    let skills_dir = workdir.join(".claude").join("skills");
    let mut wrote = 0usize;
    for id in &skill_ids {
        let Some(skill) = catalog.get(id) else {
            continue;
        };
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
    // The role's system prompt → CLAUDE.md (claude auto-loads it as project instructions, so it
    // rides a file instead of an injectable `--append-system-prompt` arg). hq-role-skills-term.4.
    // Render the template placeholders with this session's real context (hq-role-prompt-render.1).
    if let Some(p) = &prompt {
        let town_root = term_root
            .parent()
            .unwrap_or(term_root)
            .display()
            .to_string();
        let rendered = render_prompt(
            p,
            &[
                ("WorkDir", workdir.display().to_string()),
                ("RigName", rig.clone()),
                ("TownRoot", town_root),
                ("DefaultBranch", "main".to_string()),
                ("Polecat", session.to_string()),
                ("cmd", "gt".to_string()),
            ],
        );
        if std::fs::create_dir_all(&workdir).is_ok()
            && std::fs::write(workdir.join("CLAUDE.md"), rendered).is_ok()
        {
            wrote += 1;
        }
    }
    // Per-role MCP (hq-role-mcp): mint a least-privilege per-session token (scopes = the role's
    // skills) and write the `.gt-config/` that this session's `gt mcp` proxy reads, plus the
    // `.mcp.json` registering the `gt` MCP server. The role's claude then reaches the orchestrator's
    // MCP tools authenticated AS THE ROLE — not the operator. Best-effort: a mint/write failure just
    // drops MCP (the session still launches with its skills/prompt).
    let mcp_enabled = match role_mcp {
        Some((minter, url)) => {
            let scopes = catalog.scopes_for_roles(&[role.clone()]);
            match mint_role_token(minter, session, workspace, &scopes) {
                Some(token) if write_gt_config(&workdir, url, workspace, &rig, &role, &token) => {
                    // .mcp.json talks to /mcp over HTTP with this token (hq-mcp-http) — the stdio
                    // proxy surfaced resources but not tools. .gt-config still rides for the `gt mcp
                    // call|list` shell surface the agent may use.
                    if write_mcp_json(&workdir, url, workspace, &token) {
                        wrote += 1;
                    }
                    true
                }
                _ => false,
            }
        }
        None => false,
    };
    // The matching global hooks + the MCP enable flag → `<workdir>/.claude/settings.json`, which
    // claude auto-loads as project settings (hq-hooks). `enabledMcpjsonServers` pre-approves the
    // `gt` server so the role's claude loads it without the interactive project-MCP trust prompt
    // (hq-role-mcp). A session whose role has no skills/prompt still gets a workdir for these files.
    if let Some(settings) = build_settings(hooks_settings, mcp_enabled, costs_report) {
        let claude_dir = workdir.join(".claude");
        if std::fs::create_dir_all(&claude_dir).is_ok() {
            if let Ok(body) = serde_json::to_string_pretty(&settings) {
                if std::fs::write(claude_dir.join("settings.json"), body).is_ok() {
                    wrote += 1;
                }
            }
        }
    }
    if wrote == 0 {
        return RoleLaunch {
            workdir: None,
            model,
            kickoff,
        };
    }
    // Trust the workdir so claude opens without the interactive trust-folder prompt.
    crate::worktree::seed_claude_onboarding(Path::new(config_dir), &workdir);
    RoleLaunch {
        workdir: Some(workdir),
        model,
        kickoff,
    }
}

/// Mint a least-privilege per-session access token (`hq-role-mcp`): `sub` = the session id,
/// `scopes` = the role's skill scopes (never `*`), `exp` = `now + GT_ROLE_TOKEN_TTL_SECS` (default
/// 12h — long enough to outlast an interactive session; there is no refresh token, so a session that
/// outlives it must be re-launched). `None` on a signing error. Mirrors `polecat::AgentTokenMinter`.
fn mint_role_token(
    minter: &JwtMinter,
    session: &str,
    workspace: &str,
    scopes: &[String],
) -> Option<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ttl = std::env::var("GT_ROLE_TOKEN_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(43_200);
    let claims = JwtClaims {
        sub: session.to_string(),
        workspace: workspace.to_string(),
        scopes: scopes.to_vec(),
        exp: now + ttl,
        nbf: None,
        iat: now,
    };
    minter.mint(&claims).ok()
}

/// Write the session's `.gt-config/` — the per-project config `gt mcp` discovers and reads to
/// authenticate (`hq-role-mcp`). `config.toml` points at the named config `role.toml`, which carries
/// the server URL, the tenant + rig, the role, and the minted access token (no refresh token — the
/// token is short-lived and the session is re-launched rather than refreshed). Values are ids and a
/// compact JWT, all safe inside a TOML basic string (no quotes/backslashes/newlines). `false` on any
/// write error.
fn write_gt_config(
    workdir: &Path,
    server_url: &str,
    workspace: &str,
    rig: &str,
    role: &str,
    token: &str,
) -> bool {
    let dir = workdir.join(".gt-config");
    if std::fs::create_dir_all(&dir).is_err() {
        return false;
    }
    let named = format!(
        "server_url = \"{server_url}\"\n\
         workspace = \"{workspace}\"\n\
         rig = \"{rig}\"\n\
         role = \"{role}\"\n\
         access_token = \"{token}\"\n\
         refresh_token = \"\"\n"
    );
    std::fs::write(dir.join("config.toml"), "active = \"role\"\n").is_ok()
        && std::fs::write(dir.join("role.toml"), named).is_ok()
}

/// Write `<workdir>/.mcp.json` registering the single `gt` MCP server over **HTTP** — the same
/// streamable-HTTP `/mcp` transport the polecat config uses (`hq-role-mcp` / hq-mcp-http), NOT the
/// stdio `gt mcp` proxy. The stdio proxy re-presents the upstream `ServerInfo` on initialize, and
/// claude surfaced only its *resources* and never its *tools* (`mcp__gt__*`), so a role session could
/// read `gt://…` but not call `issues.*`/`agent.*`/`merge.*`. Talking to `/mcp` directly — exactly
/// what the proven polecat path does — makes the tools surface. The minted per-session token rides in
/// the `Authorization` header and the tenant in `X-Workspace`. `false` on any write error.
fn write_mcp_json(workdir: &Path, server_url: &str, workspace: &str, token: &str) -> bool {
    if std::fs::create_dir_all(workdir).is_err() {
        return false;
    }
    let url = format!("{}/mcp", server_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "mcpServers": {
            "gt": {
                "type": "http",
                "url": url,
                "headers": {
                    "Authorization": format!("Bearer {token}"),
                    "X-Workspace": workspace,
                }
            }
        }
    });
    serde_json::to_string_pretty(&body)
        .ok()
        .and_then(|s| std::fs::write(workdir.join(".mcp.json"), s).ok())
        .is_some()
}

/// Combine the matching global hooks settings with the MCP enable flag into the session's
/// `settings.json` value (`hq-role-mcp`). `enabledMcpjsonServers: ["gt"]` pre-approves the project
/// `.mcp.json` server so claude loads it without the interactive trust prompt. When `costs_report`,
/// a `Stop` quota-feed hook (identical to the polecat's, `hq-quota-feed`) is appended so the
/// interactive session's token usage feeds predictive account rotation. `None` when there is nothing
/// to write (no hooks, no MCP, no costs report).
fn build_settings(
    hooks: Option<serde_json::Value>,
    mcp_enabled: bool,
    costs_report: bool,
) -> Option<serde_json::Value> {
    if hooks.is_none() && !mcp_enabled && !costs_report {
        return None;
    }
    let mut v = hooks.unwrap_or_else(|| serde_json::json!({}));
    if mcp_enabled {
        if let Some(obj) = v.as_object_mut() {
            obj.insert("enabledMcpjsonServers".into(), serde_json::json!(["gt"]));
        }
    }
    // Append the quota-feed Stop hook beside any global hooks (hq-quota-feed). The command reads
    // GT_HOOK_ACCOUNT + GT_CHANNEL_ROOT from the session env (stamped by build_command), so it is
    // the polecat's exact reporter — no merge-ready half, an interactive session never merges.
    if costs_report {
        if let Some(obj) = v.as_object_mut() {
            let hooks_obj = obj.entry("hooks").or_insert_with(|| serde_json::json!({}));
            if let Some(hobj) = hooks_obj.as_object_mut() {
                let stop = hobj.entry("Stop").or_insert_with(|| serde_json::json!([]));
                if let Some(arr) = stop.as_array_mut() {
                    arr.push(gt_polecat::costs_report_hook_entry());
                }
            }
        }
    }
    Some(v)
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
        /// The role's model config (`hq-role-model.1`): when `write` launches `claude`, these levers
        /// are stamped onto it (`--model`, `--permission-mode`, `--effort`). `None` ⇒ the bare
        /// `claude` with the account default.
        model: Option<ModelConfig>,
        /// The role kickoff directive (`hq-role-kickoff`): when `write` launches `claude`, this is
        /// passed as the positional `[prompt]` so the session begins the role's work on open instead
        /// of idling. `None` ⇒ a bare interactive claude. Only applied on session creation (tmux
        /// `new-session -A` ignores the command when attaching to a live session).
        kickoff: Option<String>,
        /// The active claude account id (`hq-quota-feed`): stamped as `GT_HOOK_ACCOUNT` on the tmux
        /// session so the quota-feed Stop hook labels its token sample with the account being burnt.
        /// `None` ⇒ no account resolved (the feed hook then no-ops on the missing env var).
        hook_account: Option<String>,
        /// The quota-feed channel root (`hq-quota-feed`): stamped as `GT_CHANNEL_ROOT` so the Stop
        /// hook drops its `*.event` sample where the daemon's feed loop reads it. `None` ⇒ no account.
        channel_root: Option<String>,
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
    /// The RS256 minter used to issue a per-session, least-privilege `GT_TOKEN`-equivalent so the
    /// role's `gt mcp` proxy authenticates *as the role* (`hq-role-mcp`). The minted access token is
    /// written into the session workdir's `.gt-config/` (the file `gt mcp` reads), scoped to the
    /// role's skills. `None` ⇒ no `.gt-config` is materialised and the role session gets no MCP auth.
    token_minter: Option<Arc<JwtMinter>>,
    /// Base URL the role's `gt mcp` proxy targets (this server). Written into the materialised
    /// `.gt-config`. `None` (with a minter) ⇒ still no `.gt-config` (both are required).
    server_url: Option<String>,
}

impl TerminalState {
    /// Bundle the shared verifier + audit sink for the terminal router.
    pub fn new(authenticator: SharedAuthenticator, audit: SharedAudit) -> Self {
        Self {
            authenticator,
            audit,
            accounts: None,
            token_minter: None,
            server_url: None,
        }
    }

    /// Wire the event log used to resolve the active claude account (`hq-term-dock.4`): an
    /// interactive session attach then launches `claude` with that account's `CLAUDE_CONFIG_DIR`.
    pub fn with_active_accounts(mut self, log: Arc<EventLog>) -> Self {
        self.accounts = Some(log);
        self
    }

    /// Wire the per-role MCP auth (`hq-role-mcp`): the RS256 `minter` issues a least-privilege token
    /// (scopes = the role's skills) and `server_url` is the gt-mcp-server this session's `gt mcp`
    /// proxy talks to. Both are materialised into `<workdir>/.gt-config/` so the role's claude reaches
    /// the orchestrator's MCP tools authenticated as the role. Absent either ⇒ no `.gt-config`.
    pub fn with_role_auth(
        mut self,
        minter: Option<Arc<JwtMinter>>,
        server_url: Option<String>,
    ) -> Self {
        self.token_minter = minter;
        self.server_url = server_url;
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
                // For an interactive session, resolve the active claude account (id + config dir) so
                // the session launches an authenticated `claude` (hq-term-dock.4) AND its quota-feed
                // hook can label the account it burns (hq-quota-feed); absent ⇒ bare shell.
                let active = if write {
                    state
                        .accounts
                        .as_ref()
                        .and_then(|log| active_claude_account(log, &claims.workspace))
                } else {
                    None
                };
                let hook_account = active.as_ref().map(|(acct, _)| acct.clone());
                let claude_config_dir = active.as_ref().map(|(_, dir)| dir.clone());
                // The quota-feed channel the Stop hook drops its sample into. Matches the daemon's
                // GT_CHANNEL_ROOT (else `<GT_EVENTLOG_ROOT>/.channels`) — the shared eventlog volume,
                // so the backend's hook and the daemon's feed loop meet on the same directory.
                let channel_root = claude_config_dir.as_ref().map(|_| {
                    std::env::var("GT_CHANNEL_ROOT").unwrap_or_else(|_| {
                        let root = std::env::var("GT_EVENTLOG_ROOT")
                            .unwrap_or_else(|_| "/var/lib/gt-core".to_string());
                        PathBuf::from(root).join(".channels").display().to_string()
                    })
                });
                // With an account resolved, materialise the session's ROLE skills into a workdir and
                // launch claude there (hq-role-skills-term.3) loading the role's skills, plus resolve
                // the role's model config (hq-role-model.1) stamped onto the claude launch. An account
                // also earns the quota-feed Stop hook in the session settings (costs_report=true).
                let (workdir, model, kickoff) = match (write, &claude_config_dir, &state.accounts) {
                    (true, Some(dir), Some(log)) => {
                        let root = std::env::var("GT_EVENTLOG_ROOT")
                            .unwrap_or_else(|_| "/var/lib/gt-core".to_string());
                        let term_root = PathBuf::from(root).join("term");
                        let launch = prepare_role_skills(
                            log,
                            &term_root,
                            &claims.workspace,
                            session,
                            dir,
                            state.token_minter.as_deref(),
                            state.server_url.as_deref(),
                            true,
                        );
                        (
                            launch.workdir.map(|p| p.display().to_string()),
                            launch.model,
                            launch.kickoff,
                        )
                    }
                    _ => (None, None, None),
                };
                TerminalTarget::Attach {
                    workspace: claims.workspace.clone(),
                    session: session.to_string(),
                    write,
                    claude_config_dir,
                    workdir,
                    model,
                    kickoff,
                    hook_account,
                    channel_root,
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

    // Signature verified; gate the clock + workspace-presence invariants the verifier defers.
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
        return Err(reject(StatusCode::FORBIDDEN, "missing terminal.exec scope"));
    }
    Ok(claims)
}

/// True when `scopes` grants `required` outright or via the `*` superuser wildcard.
fn has_scope(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| s == SCOPE_WILDCARD || s == required)
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
        TerminalTarget::Attach {
            workspace,
            session,
            write,
            claude_config_dir,
            workdir,
            model,
            kickoff,
            hook_account,
            channel_root,
        } => {
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
                    // The quota-feed env the Stop costs-report hook reads (hq-quota-feed): the
                    // account being burnt + the channel the sample lands in. Both come from the
                    // trusted quota log / server env (no user input) and feed predictive rotation so
                    // an interactive session, like a polecat, rotates off an account before it blocks.
                    if let Some(account) = hook_account {
                        cmd.arg("-e");
                        cmd.arg(format!("GT_HOOK_ACCOUNT={account}"));
                    }
                    if let Some(root) = channel_root {
                        cmd.arg("-e");
                        cmd.arg(format!("GT_CHANNEL_ROOT={root}"));
                    }
                    cmd.arg("claude");
                    // Load the session's hooks via claude's `--settings <file>` flag (hq-quota-feed):
                    // claude does NOT apply a project `.claude/settings.json`'s hooks on its own in
                    // this container setup (the hq-orchd-deploy.16 gotcha the polecat launch already
                    // works around) — so the global hooks AND the quota-feed Stop hook prepare_role_skills
                    // wrote there never fire without this. The workdir is server-derived (no user input).
                    if let Some(wd) = workdir {
                        let settings = Path::new(wd).join(".claude").join("settings.json");
                        cmd.arg("--settings");
                        cmd.arg(settings.display().to_string());
                    }
                    // The role's model + permission mode + effort (hq-role-model.1) as claude flags.
                    // The values come from the trusted skills log; the command validator forbids
                    // whitespace in the model id and constrains the mode/effort to the closed CLI
                    // sets, so each stays a single exec arg (no shell, tmux passes them verbatim).
                    if let Some(m) = model {
                        if !m.model.trim().is_empty() {
                            cmd.arg("--model");
                            cmd.arg(&m.model);
                        }
                        if !m.permission_mode.trim().is_empty() {
                            cmd.arg("--permission-mode");
                            cmd.arg(&m.permission_mode);
                        }
                        if !m.effort.trim().is_empty() {
                            cmd.arg("--effort");
                            cmd.arg(&m.effort);
                        }
                    }
                    // The role kickoff (hq-role-kickoff) as claude's positional `[prompt]` — LAST,
                    // after every flag — so opening the session fires the role's work instead of an
                    // idle TUI. A single exec arg (no shell); only consumed when `new-session`
                    // actually creates the session, so an operator reconnecting never re-fires it.
                    if let Some(k) = kickoff {
                        if !k.trim().is_empty() {
                            cmd.arg(k);
                        }
                    }
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
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?;
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
        assert_eq!(
            sanitize_session("hq-agent-observability.5"),
            Some("hq-agent-observability.5")
        );
        // Empty, flag-like, or shell-meta names are rejected (never reach tmux as a flag/arg).
        assert!(sanitize_session("").is_none());
        assert!(
            sanitize_session("-rkill").is_none(),
            "leading dash could be read as a flag"
        );
        assert!(sanitize_session("a b").is_none());
        assert!(sanitize_session("a;rm -rf /").is_none());
        assert!(sanitize_session("a$(whoami)").is_none());
        assert!(
            sanitize_session(&"x".repeat(129)).is_none(),
            "over the length cap"
        );
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
            model: None,
            kickoff: None,
            hook_account: None,
            channel_root: None,
        });
        let argv: Vec<String> = ro
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            argv,
            vec![
                "tmux",
                "-L",
                "gt-acme",
                "attach-session",
                "-t",
                "hq-gg-1",
                "-r"
            ]
        );
        // Write mode without an account attaches-OR-CREATES a bare shell (hq-session-terminal.1).
        let rw = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: None,
            workdir: None,
            model: None,
            kickoff: None,
            hook_account: None,
            channel_root: None,
        });
        let argv: Vec<String> = rw
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            argv,
            vec![
                "tmux",
                "-L",
                "gt-acme",
                "new-session",
                "-A",
                "-s",
                "hq-gg-1"
            ]
        );
        // Write mode WITH an active account + a role workdir: claude under that profile, started in
        // the workdir so it loads the role's skills (hq-term-dock.4 + hq-role-skills-term.3).
        let cl = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: Some("/var/lib/gt-core/accounts/abc".into()),
            workdir: Some("/var/lib/gt-core/term/hq-gg-1".into()),
            model: None,
            kickoff: None,
            hook_account: None,
            channel_root: None,
        });
        let argv: Vec<String> = cl
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            argv,
            vec![
                "tmux",
                "-L",
                "gt-acme",
                "new-session",
                "-A",
                "-s",
                "hq-gg-1",
                "-c",
                "/var/lib/gt-core/term/hq-gg-1",
                "-e",
                "CLAUDE_CONFIG_DIR=/var/lib/gt-core/accounts/abc",
                "-e",
                "IS_SANDBOX=1",
                "claude",
                "--settings",
                "/var/lib/gt-core/term/hq-gg-1/.claude/settings.json"
            ]
        );
        // Write mode WITH a role model config (hq-role-model.1): --model/--permission-mode/--effort
        // are claude flags after the command word.
        let cm = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: Some("/var/lib/gt-core/accounts/abc".into()),
            workdir: None,
            model: Some(gt_skills::ModelConfig {
                model: "opus".into(),
                permission_mode: "acceptEdits".into(),
                effort: "xhigh".into(),
            }),
            kickoff: None,
            hook_account: None,
            channel_root: None,
        });
        let argv: Vec<String> = cm
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            argv,
            vec![
                "tmux",
                "-L",
                "gt-acme",
                "new-session",
                "-A",
                "-s",
                "hq-gg-1",
                "-e",
                "CLAUDE_CONFIG_DIR=/var/lib/gt-core/accounts/abc",
                "-e",
                "IS_SANDBOX=1",
                "claude",
                "--model",
                "opus",
                "--permission-mode",
                "acceptEdits",
                "--effort",
                "xhigh"
            ]
        );
        // No session ⇒ the original fresh shell.
        let sh = build_command(&TerminalTarget::Shell);
        let argv: Vec<String> = sh
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(argv, vec!["/bin/sh"]);
    }

    #[test]
    fn build_command_appends_the_role_kickoff_as_claudes_last_arg() {
        // hq-role-kickoff: the kickoff rides as claude's positional `[prompt]`, AFTER every flag, so
        // opening the session fires the role's work instead of an idle TUI.
        let cmd = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: Some("/var/lib/gt-core/accounts/abc".into()),
            workdir: Some("/var/lib/gt-core/term/hq-gg-1".into()),
            model: None,
            kickoff: Some("Begin your duties now.".into()),
            hook_account: None,
            channel_root: None,
        });
        let argv: Vec<String> = cmd
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        // claude is launched and the kickoff is the final argument.
        assert_eq!(argv.last().unwrap(), "Begin your duties now.");
        let claude_at = argv.iter().position(|a| a == "claude").unwrap();
        assert!(claude_at < argv.len() - 1, "kickoff must follow `claude`");
        // A read-only attach never carries a kickoff (no claude launched).
        let ro = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: false,
            claude_config_dir: None,
            workdir: None,
            model: None,
            kickoff: Some("Begin your duties now.".into()),
            hook_account: None,
            channel_root: None,
        });
        let ro_argv: Vec<String> = ro
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert!(!ro_argv.iter().any(|a| a == "Begin your duties now."));
    }

    #[test]
    fn build_command_stamps_the_quota_feed_env_when_an_account_is_resolved() {
        // hq-quota-feed: an interactive claude launch carries GT_HOOK_ACCOUNT + GT_CHANNEL_ROOT (as
        // tmux `-e` pairs, before the `claude` word) so its Stop hook labels + lands its token sample.
        let cmd = build_command(&TerminalTarget::Attach {
            workspace: "acme".into(),
            session: "hq-gg-1".into(),
            write: true,
            claude_config_dir: Some("/var/lib/gt-core/accounts/abc".into()),
            workdir: None,
            model: None,
            kickoff: None,
            hook_account: Some("brayanrayo@bi-quare.com".into()),
            channel_root: Some("/var/lib/gt-core/.channels".into()),
        });
        let argv: Vec<String> = cmd
            .get_argv()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        let claude_at = argv.iter().position(|a| a == "claude").unwrap();
        assert!(argv[..claude_at]
            .iter()
            .any(|a| a == "GT_HOOK_ACCOUNT=brayanrayo@bi-quare.com"));
        assert!(argv[..claude_at]
            .iter()
            .any(|a| a == "GT_CHANNEL_ROOT=/var/lib/gt-core/.channels"));
    }

    #[test]
    fn render_prompt_substitutes_known_placeholders_both_forms() {
        // hq-role-prompt-render.1: <X>, {{ .X }} and {{ cmd }} all render; unknown stays intact.
        let p = "Eres <RigName>. cwd <WorkDir>. raw {{ .RigName }} cmd {{ cmd }}. keep <DogName>.";
        let out = render_prompt(
            p,
            &[
                ("RigName", "gt_core".into()),
                ("WorkDir", "/var/lib/gt-core/term/w1".into()),
                ("cmd", "gt".into()),
            ],
        );
        assert_eq!(
            out,
            "Eres gt_core. cwd /var/lib/gt-core/term/w1. raw gt_core cmd gt. keep <DogName>."
        );
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
            SkillEvent::EnabledForRole {
                role: "witness".into(),
                skill: "pr-list".into(),
                now_secs: 2,
            },
        )
        .unwrap();

        let term_root = dir.path().join("term");
        let cfg = dir.path().join("cfg"); // a throwaway CLAUDE_CONFIG_DIR for trust seeding
        std::fs::create_dir_all(&cfg).unwrap();
        // A role prompt → CLAUDE.md (hq-role-skills-term.4).
        log.append(
            Some("acme"),
            SkillEvent::RolePromptSet {
                role: "witness".into(),
                prompt: "You are the witness.".into(),
                now_secs: 3,
            },
        )
        .unwrap();

        // A role model config (hq-role-model.1) so the launch resolves it alongside the workdir.
        log.append(
            Some("acme"),
            SkillEvent::RoleModelSet {
                role: "witness".into(),
                model: "sonnet".into(),
                permission_mode: "plan".into(),
                effort: "high".into(),
                now_secs: 4,
            },
        )
        .unwrap();

        let launch = prepare_role_skills(
            &log,
            &term_root,
            "acme",
            "w1",
            cfg.to_str().unwrap(),
            None,
            None,
            false,
        );
        let wd = launch
            .workdir
            .expect("witness has an enabled skill with a body");
        let skill_md = wd
            .join(".claude")
            .join("skills")
            .join("pr-list")
            .join("SKILL.md");
        assert_eq!(
            std::fs::read_to_string(&skill_md).unwrap(),
            "# PR list\nlist open PRs"
        );
        assert_eq!(
            std::fs::read_to_string(wd.join("CLAUDE.md")).unwrap(),
            "You are the witness."
        );
        // The role's model config rides on the same launch resolution.
        let m = launch.model.expect("witness has a model config");
        assert_eq!(m.model, "sonnet");
        assert_eq!(m.permission_mode, "plan");
        assert_eq!(m.effort, "high");

        // A role with no enabled skills ⇒ no workdir (claude launches in the default dir).
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
        assert!(prepare_role_skills(
            &log,
            &term_root,
            "acme",
            "m1",
            cfg.to_str().unwrap(),
            None,
            None,
            false
        )
        .workdir
        .is_none());
    }

    #[test]
    fn prepare_role_skills_writes_matching_global_hooks_settings() {
        // hq-hooks: a global hook whose target matches the session's (workspace, rig, role) is
        // materialised into <workdir>/.claude/settings.json, even when the role has no skills/prompt.
        use gt_agent::AgentEvent;
        use gt_claude_hooks::{HookEvent, HookTarget};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            AgentEvent::Spawned {
                session: "p1".into(),
                rig: "hq".into(),
                role: gt_agent::SessionRole::Dog(gt_agent::DogKind::Witness),
                crew: None,
                skills: vec![],
                hooks: vec![],
            },
        )
        .unwrap();
        // A GLOBAL hook (None scope), all-empty target ⇒ applies to every session.
        log.append(
            None,
            HookEvent::Registered {
                id: "guard-rm".into(),
                event: "PreToolUse".into(),
                matcher: "Bash(rm -rf /*)".into(),
                command: "echo BLOCKED && exit 2".into(),
                target: HookTarget::default(),
                now_secs: 1,
            },
        )
        .unwrap();

        let term_root = dir.path().join("term");
        let cfg = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg).unwrap();
        let launch = prepare_role_skills(
            &log,
            &term_root,
            "acme",
            "p1",
            cfg.to_str().unwrap(),
            None,
            None,
            false,
        );
        let wd = launch
            .workdir
            .expect("a matching global hook forces a workdir");
        let settings = std::fs::read_to_string(wd.join(".claude").join("settings.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&settings).unwrap();
        assert_eq!(v["hooks"]["PreToolUse"][0]["matcher"], "Bash(rm -rf /*)");
        assert_eq!(
            v["hooks"]["PreToolUse"][0]["hooks"][0]["command"],
            "echo BLOCKED && exit 2"
        );
    }

    #[tokio::test]
    async fn active_claude_account_resolves_from_the_quota_log() {
        // hq-term-dock.4 + hq-quota-feed: replay the quota domain → the active account's id +
        // CLAUDE_CONFIG_DIR. Active is the last rotation target, else the first registered account.
        let dir_of = |log: &EventLog, ws: &str| active_claude_account(log, ws).map(|(_, d)| d);
        use gt_quota::QuotaEvent;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        log.append(
            Some("acme"),
            QuotaEvent::AccountRegistered {
                account: "acct-a".into(),
                config_dir: "/dirs/a".into(),
                now_secs: 0,
            },
        )
        .unwrap();
        log.append(
            Some("acme"),
            QuotaEvent::AccountRegistered {
                account: "acct-b".into(),
                config_dir: "/dirs/b".into(),
                now_secs: 0,
            },
        )
        .unwrap();
        // No rotation yet → first registered (BTreeMap order: acct-a). The account id rides too.
        assert_eq!(dir_of(&log, "acme").as_deref(), Some("/dirs/a"));
        assert_eq!(
            active_claude_account(&log, "acme")
                .map(|(a, _)| a)
                .as_deref(),
            Some("acct-a")
        );
        // After rotating to b → b is active.
        log.append(
            Some("acme"),
            QuotaEvent::Rotated {
                from_account: "acct-a".into(),
                to_account: "acct-b".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        assert_eq!(dir_of(&log, "acme").as_deref(), Some("/dirs/b"));
        assert_eq!(
            active_claude_account(&log, "acme")
                .map(|(a, _)| a)
                .as_deref(),
            Some("acct-b")
        );
        // A workspace with no accounts resolves to None (caller falls back to a shell).
        assert!(active_claude_account(&log, "empty").is_none());
    }

    #[tokio::test]
    async fn active_claude_account_prefers_healthy_over_alphabetical() {
        // hq-quota-healthy: with no rotation, skip a cooled/limited account for a healthy one rather
        // than always handing out the alphabetically-first (which may be the exhausted one).
        use gt_quota::QuotaEvent;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        for (a, d) in [("acct-a", "/dirs/a"), ("acct-b", "/dirs/b")] {
            log.append(
                Some("acme"),
                QuotaEvent::AccountRegistered {
                    account: a.into(),
                    config_dir: d.into(),
                    now_secs: 0,
                },
            )
            .unwrap();
        }
        // acct-a (alphabetical first) hits its limit → Limited. Selection must skip it for acct-b.
        log.append(
            Some("acme"),
            QuotaEvent::AccountLimited {
                account: "acct-a".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        assert_eq!(
            active_claude_account(&log, "acme")
                .map(|(a, _)| a)
                .as_deref(),
            Some("acct-b"),
            "the limited alphabetical-first account is skipped for the healthy one"
        );

        // A stale rotation TARGET that since went unhealthy is not trusted: rotate b→a (so the last
        // target is acct-a), then limit acct-a too. With every account unhealthy, the last-resort
        // branch still yields one (acct-a, BTreeMap-first) so the session launches rather than None.
        log.append(
            Some("acme"),
            QuotaEvent::Rotated {
                from_account: "acct-b".into(),
                to_account: "acct-a".into(),
                now_secs: 2,
            },
        )
        .unwrap();
        // acct-a is Limited (and is the rotation target); acct-b is Cooldown (rotated away from).
        let picked = active_claude_account(&log, "acme").map(|(a, _)| a);
        assert!(
            picked.is_some(),
            "a session still launches even when no account is healthy"
        );
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

    #[test]
    fn write_mcp_json_registers_the_gt_http_server() {
        // hq-mcp-http: <workdir>/.mcp.json registers exactly the `gt` server over HTTP to /mcp, with
        // the minted token + tenant in headers (the stdio proxy surfaced resources but not tools).
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        assert!(write_mcp_json(
            dir.path(),
            "http://127.0.0.1:8765",
            "default",
            "header.payload.sig"
        ));
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join(".mcp.json")).unwrap())
                .unwrap();
        assert_eq!(v["mcpServers"]["gt"]["type"], "http");
        assert_eq!(v["mcpServers"]["gt"]["url"], "http://127.0.0.1:8765/mcp");
        assert_eq!(
            v["mcpServers"]["gt"]["headers"]["Authorization"],
            "Bearer header.payload.sig"
        );
        assert_eq!(v["mcpServers"]["gt"]["headers"]["X-Workspace"], "default");
        // Exactly one server registered.
        assert_eq!(v["mcpServers"].as_object().unwrap().len(), 1);
    }

    #[test]
    fn write_gt_config_writes_active_pointer_and_named_config() {
        // hq-role-mcp: the `.gt-config/` `gt mcp` discovers — active pointer + the named config with
        // the server URL, tenant, rig, role and the minted access token (no refresh token).
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        assert!(write_gt_config(
            dir.path(),
            "http://127.0.0.1:8765",
            "acme",
            "hq",
            "witness",
            "header.payload.sig",
        ));
        let active = std::fs::read_to_string(dir.path().join(".gt-config/config.toml")).unwrap();
        assert_eq!(active.trim(), r#"active = "role""#);
        let named = std::fs::read_to_string(dir.path().join(".gt-config/role.toml")).unwrap();
        assert!(named.contains(r#"server_url = "http://127.0.0.1:8765""#));
        assert!(named.contains(r#"workspace = "acme""#));
        assert!(named.contains(r#"rig = "hq""#));
        assert!(named.contains(r#"role = "witness""#));
        assert!(named.contains(r#"access_token = "header.payload.sig""#));
        assert!(named.contains(r#"refresh_token = """#));
    }

    #[test]
    fn build_settings_merges_mcp_enable_flag_with_hooks() {
        // hq-role-mcp: enable flag rides alongside any hooks; nothing to write ⇒ None.
        assert!(build_settings(None, false, false).is_none());

        let only_mcp = build_settings(None, true, false).unwrap();
        assert_eq!(only_mcp["enabledMcpjsonServers"], serde_json::json!(["gt"]));

        let hooks = serde_json::json!({ "hooks": { "PreToolUse": [] } });
        let merged = build_settings(Some(hooks), true, false).unwrap();
        assert_eq!(merged["enabledMcpjsonServers"], serde_json::json!(["gt"]));
        assert!(merged["hooks"]["PreToolUse"].is_array());

        // Hooks present but MCP off ⇒ the flag is absent.
        let hooks = serde_json::json!({ "hooks": {} });
        let no_mcp = build_settings(Some(hooks), false, false).unwrap();
        assert!(no_mcp.get("enabledMcpjsonServers").is_none());
    }

    #[test]
    fn build_settings_appends_the_quota_feed_stop_hook_when_costs_report() {
        // hq-quota-feed: costs_report alone earns a settings.json carrying the quota-feed Stop hook,
        // so an interactive session reports token usage to predictive rotation just like a polecat.
        let only_costs = build_settings(None, false, true).expect("costs report ⇒ settings");
        let stop = only_costs["hooks"]["Stop"]
            .as_array()
            .expect("Stop hook array");
        assert_eq!(stop.len(), 1);
        let cmd = stop[0]["hooks"][0]["command"].as_str().unwrap();
        assert!(cmd.contains("$GT_CHANNEL_ROOT/quota-feed"));
        assert!(cmd.contains("$GT_HOOK_ACCOUNT"));

        // It rides ALONGSIDE existing global hooks (a Stop hook is appended, not clobbered).
        let hooks = serde_json::json!({ "hooks": { "Stop": [ { "matcher": "", "hooks": [] } ] } });
        let merged = build_settings(Some(hooks), false, true).unwrap();
        assert_eq!(merged["hooks"]["Stop"].as_array().unwrap().len(), 2);

        // No costs report ⇒ no Stop hook injected.
        let none = build_settings(None, true, false).unwrap();
        assert!(none.get("hooks").is_none());
    }
}
