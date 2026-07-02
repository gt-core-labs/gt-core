//! Trigger-driven role agents (`gtcore-999795`): sheriff / witness / deacon run as *agents with
//! criterion*, not perpetual in-process loops.
//!
//! ## The decision this module records
//!
//! gt-core's supervisor roles existed only as in-process actors (state machines fed by the event
//! bus) or as a one-shot safety-net ([`crate::witness_sweep`]). A loop can recover a *mechanical*
//! failure, but it cannot exercise judgment — decide whether a failed merge slot is safe to
//! recover vs. needs a human, read a closed bead's acceptance criteria and tell whether the work
//! actually met them, or spot a contradiction in the flow. Those need a *reasoning agent*.
//!
//! The contrast case is the merge [`refinery`](gt_merge::refinery): for every MERGE_READY message
//! there is exactly one correct, mechanical action (decode → `Submit`), so a reasoning agent adds
//! nothing there — it stays an in-process I/O bridge. The criterion is therefore: *an agent earns
//! its place only where a non-trivial judgment lives* (recover-or-escalate a failed slot, verify a
//! closed bead met its AC); pure deterministic plumbing stays in-process. The sheriff sits one
//! layer above the refinery and keeps it as the mechanical fallback.
//!
//! But a reasoning agent that idles in a loop burns tokens for nothing between events. So the
//! design here is **event-triggered, single-shot** agents: a role is slung with a kickoff prompt
//! ONLY when its trigger fires (sheriff ← `merge.failed.v1`/`merge.ready.v1`; witness ←
//! `issues.closed.v1`; deacon ← a periodic health tick), does its one task, and exits. Between
//! triggers there is no live session, so idle cost is ≈0 tokens — the AC's "idle sin tokens entre
//! disparos".
//!
//! ## Single-flight per role
//!
//! A burst of triggers (three merges fail at once) must NOT sling three sheriffs that race on the
//! same board. [`RoleAgentDispatcher`] keeps at most one live session per role: while a role's
//! agent is alive, further triggers for that role are skipped (the live agent will see the new
//! board state itself). The slot frees on `agent.session-end.v1`/`agent.killed.v1`, so the next
//! trigger re-slings. This bounds token spend and keeps the roles within pool/host-cap budget.
//!
//! ## Standing behavior vs. kickoff
//!
//! Each role's *standing* behavior (its mission, tone, the tools it may use) already lives in the
//! Knowledge catalog as the role prompt (`crates/domain/platform/gt-skills/seeds/knowledge.json`),
//! materialized as `CLAUDE.md`. The *kickoff* built here is the per-trigger task directive — the
//! positional first prompt that tells the agent which concrete situation to act on right now. It is
//! deliberately self-contained (mission + tools + trigger context + "do this, then stop") so a
//! triggered agent works even when its CLAUDE.md is the repo's generic one.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;

use gt_agent::{AgentEvent, DogKind, SessionRole};
use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope};
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
use gt_polecat::{spawn_tmux, SpawnSpec, SpawnTemplate, Tmux};
use gt_quota::Keychain;

use crate::polecat::AgentTokenMinter;

/// What woke a role agent. Each variant maps to exactly one role via [`role_for`]; the carried
/// fields become the kickoff's trigger context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleTrigger {
    /// A merge slot failed (`merge.failed.v1`) → the **sheriff** drives recovery.
    MergeFailed { bead: String, reason: String },
    /// A branch entered the merge pipeline (`merge.ready.v1`) → the **sheriff** keeps the board
    /// flowing (ordering, BEHIND rebases, stuck slots).
    MergeReady { bead: String, branch: String },
    /// A bead was closed (`issues.closed.v1`) → the **witness** verifies it met its AC.
    BeadClosed { bead: String },
    /// A periodic flow-health tick → the **deacon** scans for stuck/contradictory work.
    HealthTick,
    /// An operator/coordinator explicitly requested this role via `agent.spawn`
    /// (gtcore-b69087). The one trigger a human controls directly: the orchd auto-raises only
    /// the MAYOR; every infra role must also be launchable on demand, or a dead one (a stuck
    /// merge queue with no refinery) leaves the operator with no lever but a full daemon
    /// restart.
    OnDemand { role: DogKind, reason: String },
}

impl RoleTrigger {
    /// A short stable token identifying the trigger subject — used to name the slung session
    /// (`<role>-<subject>-<n>`). Health ticks have no subject, so they use `"tick"`.
    fn subject(&self) -> &str {
        match self {
            RoleTrigger::MergeFailed { bead, .. } | RoleTrigger::MergeReady { bead, .. } => bead,
            RoleTrigger::BeadClosed { bead } => bead,
            RoleTrigger::HealthTick => "tick",
            RoleTrigger::OnDemand { .. } => "demand",
        }
    }
}

/// The flat role token for a [`DogKind`] — the single-flight key and session-name prefix for an
/// on-demand sling. Static so it can key the dispatcher's `live` map like [`role_for`]'s returns.
pub fn dog_role_str(kind: DogKind) -> &'static str {
    match kind {
        DogKind::Witness => "witness",
        DogKind::Refinery => "refinery",
        DogKind::Deacon => "deacon",
        DogKind::Overseer => "overseer",
        DogKind::Sheriff => "sheriff",
        DogKind::Dog => "dog",
    }
}

/// The role that owns a trigger. The single source of truth tying a trigger to a role, so the
/// dispatcher's single-flight key and the kickoff's framing never disagree.
pub fn role_for(trigger: &RoleTrigger) -> &'static str {
    match trigger {
        RoleTrigger::MergeFailed { .. } | RoleTrigger::MergeReady { .. } => "sheriff",
        RoleTrigger::BeadClosed { .. } => "witness",
        RoleTrigger::HealthTick => "deacon",
        RoleTrigger::OnDemand { role, .. } => dog_role_str(*role),
    }
}

/// The memory + stop discipline appended to every role kickoff. Mirrors the polecat prompt's
/// preamble (recall durable team memory; obey `feedback` memories) and closes with the single-shot
/// contract that keeps idle cost at zero: do the one task, then stop — do NOT poll in a loop.
fn discipline(workspace: &str) -> String {
    format!(
        " Before you act, recall durable team memory with the MCP tool `mcp__gt__memory_recall` \
         (workspace `{workspace}`); treat every memory of kind `feedback` as a hard rule. If you \
         learn something durable, persist it with `mcp__gt__memory_save` — never to local files. \
         This is a single-shot task: do it once, report your finding, then STOP. Do not idle or \
         poll in a loop — you will be re-slung when the next trigger fires."
    )
}

/// Build the per-trigger kickoff prompt for the role that owns `trigger`. Self-contained: it states
/// the role's mission, the exact `mcp__gt__*` tools to use, the concrete trigger context, and the
/// single-shot stop discipline. This is the "kickoff/prompt claro de SU trabajo" the AC asks for.
pub fn kickoff_for(workspace: &str, trigger: &RoleTrigger) -> String {
    let body = match trigger {
        RoleTrigger::MergeFailed { bead, reason } => format!(
            "You are the gt **sheriff** for workspace `{workspace}` — the watchdog with judgment over \
             the merge board, not a mechanical git-merge edge. A merge slot just FAILED: bead \
             `{bead}` — reason: {reason}. Drive the board back to health: inspect it with \
             `mcp__gt__merge_list` and `mcp__gt__merge_info` (id `{bead}`), then decide with \
             criterion — recover this slot by re-submitting the fixed branch via \
             `mcp__gt__merge_submit` once the cause is addressed, reorder/unblock a stuck slot, or, \
             if it genuinely needs a human (irreconcilable conflict, repeated failure), escalate via \
             `mcp__gt__notify_send`. Recover the slot if you can do so safely."
        ),
        RoleTrigger::MergeReady { bead, branch } => format!(
            "You are the gt **sheriff** for workspace `{workspace}` — the watchdog over the merge \
             board. Branch `{branch}` (bead `{bead}`) just entered the pipeline. Review the board \
             with `mcp__gt__merge_list`: confirm nothing is stuck Merging, the ordering is sane, and \
             no earlier Failed slot was left behind. Act only where it helps (recover a stale Failed \
             slot via `mcp__gt__merge_submit`, or flag a wedged board via `mcp__gt__notify_send`); \
             otherwise confirm the board is healthy."
        ),
        RoleTrigger::BeadClosed { bead } => format!(
            "You are the gt **witness** for workspace `{workspace}` — QA and observability over \
             closed work. Bead `{bead}` was just CLOSED. Verify it actually met its acceptance \
             criteria: read it with `mcp__gt__issues_read` (id `{bead}`) and compare the AC against \
             what was delivered. If the closure does NOT hold up (acceptance criteria unmet, work \
             left half-done, no tests where the AC demanded them), FLAG it: leave a specific comment \
             via `mcp__gt__comments_create` and alert the operator via `mcp__gt__notify_send`. If \
             the AC is genuinely met, record that it passed and do nothing else."
        ),
        RoleTrigger::HealthTick => format!(
            "You are the gt **deacon** for workspace `{workspace}` — a read-only supervisor of flow \
             health. A health tick fired. Scan for trouble with criterion: beads stuck in `working` \
             with no recent progress, contradictions between tracker and merge state, and stalled \
             merge slots. Use `mcp__gt__issues_list`, `mcp__gt__board_list`, and `mcp__gt__merge_list` \
             (read-only — you do not transition or merge anything). If you find a genuinely stuck or \
             contradictory item, escalate it via `mcp__gt__notify_send`. If the flow is healthy, say \
             so and stop."
        ),
        RoleTrigger::OnDemand { role, reason } => {
            let mission = match role {
                DogKind::Refinery => format!(
                    "You are the gt **refinery** for workspace `{workspace}`, slung ON DEMAND — the \
                     in-process merge edge may be dead or the queue stuck. Inspect the merge board \
                     with `mcp__gt__merge_list`: drive every slot stuck in `ready`/`merging` — \
                     `mcp__gt__merge_info` for detail, `mcp__gt__merge_complete` (with the landed sha) \
                     for a slot whose bead already delivered to main, `mcp__gt__merge_submit` to \
                     re-enter a branch that never landed — and escalate via `mcp__gt__notify_send` \
                     anything that needs a human (conflicts, missing branches)."
                ),
                DogKind::Sheriff => format!(
                    "You are the gt **sheriff** for workspace `{workspace}`, slung ON DEMAND. Review \
                     the merge board with `mcp__gt__merge_list`/`mcp__gt__merge_info` and drive it back \
                     to health: recover what is safe, escalate what needs a human via \
                     `mcp__gt__notify_send`."
                ),
                DogKind::Witness => format!(
                    "You are the gt **witness** for workspace `{workspace}`, slung ON DEMAND. Audit the \
                     recently closed beads (`mcp__gt__issues_list` status=closed, newest first) against \
                     their acceptance criteria; flag any false completion with `mcp__gt__comments_create` \
                     + `mcp__gt__notify_send`."
                ),
                DogKind::Deacon => format!(
                    "You are the gt **deacon** for workspace `{workspace}`, slung ON DEMAND for a flow \
                     health scan. Read-only: `mcp__gt__issues_list`, `mcp__gt__board_list`, \
                     `mcp__gt__merge_list`; escalate genuinely stuck or contradictory items via \
                     `mcp__gt__notify_send`."
                ),
                DogKind::Overseer | DogKind::Dog => format!(
                    "You are a gt **{}** supervisory agent for workspace `{workspace}`, slung ON \
                     DEMAND. Assess the situation named in the request, act within your role's \
                     standing mission (your CLAUDE.md), and escalate via `mcp__gt__notify_send` what \
                     needs a human.",
                    dog_role_str(*role)
                ),
            };
            format!("{mission} The request's stated reason: {reason}.")
        }
    };
    format!("{body}{}", discipline(workspace))
}

/// The edge that actually launches a role agent (a `claude` session). Faked in tests; the daemon
/// wires [`SpecRoleLauncher`]. Returning `Err` is surfaced by the dispatcher as a launch failure
/// and the role's single-flight slot is NOT held (so the next trigger retries).
pub trait RoleLauncher: Send + Sync {
    /// Launch `role` as session `session` with positional `kickoff` as its first prompt.
    fn launch(&self, role: &str, session: &str, kickoff: &str) -> Result<(), String>;
}

/// What the dispatcher decided for one trigger — returned so callers (and tests) can assert the
/// behavior without inspecting the launcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleDispatch {
    /// A fresh role agent was slung as `session`.
    Slung { role: &'static str, session: String },
    /// A live agent for this role already exists; the trigger was absorbed (single-flight).
    SkippedAlreadyLive { role: &'static str, session: String },
    /// The launcher failed; the slot is not held, so the next trigger will retry.
    LaunchFailed { role: &'static str, reason: String },
}

/// Turns role triggers into single-shot, single-flight role-agent slings (`gtcore-999795`).
///
/// At most one live session per role (the single-flight invariant): the `live` map keys role →
/// session id. A trigger for an already-live role is absorbed; the slot frees on
/// [`on_session_end`](Self::on_session_end). Session ids are `<role>-<subject>-<n>` with a
/// monotonic `seq` so re-slings after a session ends never collide.
pub struct RoleAgentDispatcher {
    workspace: String,
    launcher: std::sync::Arc<dyn RoleLauncher>,
    /// role → the session id of its currently-live agent.
    live: Mutex<HashMap<&'static str, String>>,
    seq: AtomicU64,
}

impl RoleAgentDispatcher {
    /// Wire the dispatcher for `workspace` with the launcher edge.
    pub fn new(workspace: impl Into<String>, launcher: std::sync::Arc<dyn RoleLauncher>) -> Self {
        Self {
            workspace: workspace.into(),
            launcher,
            live: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }
    }

    /// True if `role` currently has a live agent (test/observability helper).
    pub fn is_live(&self, role: &str) -> bool {
        self.live.lock().expect("live mutex").contains_key(role)
    }

    /// Handle one trigger: sling the owning role's agent unless one is already live for that role.
    pub fn on_trigger(&self, trigger: &RoleTrigger) -> RoleDispatch {
        let role = role_for(trigger);

        // Single-flight: an already-live role absorbs the trigger. Hold the lock across the launch
        // so two concurrent triggers for the same role can't both pass the check and double-sling.
        let mut live = self.live.lock().expect("live mutex");
        if let Some(session) = live.get(role) {
            return RoleDispatch::SkippedAlreadyLive {
                role,
                session: session.clone(),
            };
        }

        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let session = format!("{role}-{}-{n}", sanitize(trigger.subject()));
        let kickoff = kickoff_for(&self.workspace, trigger);

        match self.launcher.launch(role, &session, &kickoff) {
            Ok(()) => {
                live.insert(role, session.clone());
                RoleDispatch::Slung { role, session }
            }
            // Slot not held on failure: the next trigger retries rather than wedging the role.
            Err(reason) => RoleDispatch::LaunchFailed { role, reason },
        }
    }

    /// Free the single-flight slot held by `session`, if any. Called on `agent.session-end.v1` /
    /// `agent.killed.v1`. Returns the role whose slot was freed (for logging), or `None` when the
    /// session was not a tracked role agent. Idempotent.
    pub fn on_session_end(&self, session: &str) -> Option<&'static str> {
        let mut live = self.live.lock().expect("live mutex");
        let role = live
            .iter()
            .find(|(_, s)| s.as_str() == session)
            .map(|(r, _)| *r)?;
        live.remove(role);
        Some(role)
    }
}

/// Sanitize a trigger subject into a tmux/session-safe token (`[A-Za-z0-9._-]`), mirroring the
/// polecat session-name discipline so a bead id with odd characters can't break the session name.
fn sanitize(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '-' })
        .collect();
    let trimmed = cleaned.trim_matches('-');
    if trimmed.is_empty() {
        "x".to_string()
    } else {
        trimmed.to_string()
    }
}

/// The [`SessionRole`] for a role name, so a slung role agent's `agent.spawned.v1` carries its real
/// role (sheriff/witness/deacon are `Dog` kinds in the session model) for FE attribution + the
/// session-minutes projection.
fn session_role_for(role: &str) -> SessionRole {
    match role {
        "sheriff" => SessionRole::Dog(DogKind::Sheriff),
        "witness" => SessionRole::Dog(DogKind::Witness),
        "deacon" => SessionRole::Dog(DogKind::Deacon),
        "refinery" => SessionRole::Dog(DogKind::Refinery),
        _ => SessionRole::Dog(DogKind::Dog),
    }
}

/// Build the [`SpawnSpec`] for a triggered role agent (pure, so the env/arg shaping is unit-tested
/// without tmux). Unlike a polecat sling this is NOT a bead worker: it carries no `GT_HOOK_BEAD`,
/// no per-bead branch/worktree, and no merge-ready hook expectation — it runs in the shared rig
/// checkout, does its one task via the `gt` MCP tools, and exits. The role's `GT_ROLE` overrides
/// the template default, the minted least-privilege `token` (if any) is its identity, and the
/// `kickoff` is the positional first prompt.
fn role_spec(
    template: &SpawnTemplate,
    role: &str,
    session: &str,
    kickoff: &str,
    token: Option<String>,
) -> SpawnSpec {
    let mut env = template.base_env.clone();
    match env.iter_mut().find(|(k, _)| k == "GT_ROLE") {
        Some((_, v)) => *v = role.to_string(),
        None => env.push(("GT_ROLE".to_string(), role.to_string())),
    }
    if let Some(tok) = token {
        env.push(("GT_TOKEN".to_string(), tok));
    }
    let mut args = template.args.clone();
    args.push(kickoff.to_string());
    let heartbeat = template.heartbeat_dir.join(format!("{session}.heartbeat"));
    SpawnSpec {
        session: session.to_string(),
        rig: template.rig.clone(),
        polecat: session.to_string(),
        crew: None,
        workdir: template.workdir.clone(),
        command: template.command.clone(),
        args,
        env,
        // Not a bead worker: no pinned bead, no branch, no merge-ready hook.
        hook_bead: None,
        issue: None,
        heartbeat,
    }
}

/// Production [`RoleLauncher`]: builds a [`role_spec`], mints the role's least-privilege token,
/// resolves the active claude account's credentials (best-effort, via the keychain), spawns the
/// session in tmux, and watches it to free the dispatcher's single-flight slot when it exits.
///
/// The exit watch is what closes the single-flight loop without a perpetual session: a detached
/// task polls tmux for the session and emits `agent.session-end.v1` onto the hub when it's gone —
/// the [`RoleAgentPlugin`] reacts and frees the role's slot so the next trigger can re-sling.
pub struct SpecRoleLauncher {
    template: SpawnTemplate,
    tmux: Arc<dyn Tmux>,
    token: Option<AgentTokenMinter>,
    keychain: Option<Arc<dyn Keychain>>,
    /// Hub sender for the role session's `agent.spawned.v1` / `agent.session-end.v1` lifecycle.
    /// `None` ⇒ no emission and no exit watch (the slot then only frees if something else emits a
    /// session-end for it) — used by tests that exercise the spec/spawn without a hub.
    events: Option<broadcast::Sender<EventRecord>>,
    /// How often the exit watch polls tmux for the session (production ~30s).
    poll: Duration,
    /// Tenant slug stamped into the per-session `.mcp.json` `X-Workspace` header.
    workspace: String,
    /// Base URL of the gt MCP server (`GT_SELF_URL`). When set (with a minted token) each sling
    /// writes a fresh per-session `.mcp.json` so the role agent's `mcp__gt__*` calls authenticate as
    /// itself. `None` ⇒ no `.mcp.json` is written (the agent falls back to whatever the workdir has).
    server_url: Option<String>,
    /// Root under which each role agent gets its OWN working directory (`<root>/<session>`), so two
    /// concurrent role agents never race on a single shared `.mcp.json`/token. `None` ⇒ all role
    /// agents share the rig checkout (single-agent-safe only — the same caveat as polecats without
    /// `GT_POLECAT_WORKTREE_ROOT`).
    session_root: Option<std::path::PathBuf>,
}

impl SpecRoleLauncher {
    /// Wire the launcher with the rig spawn template and tmux edge. Tokens, keychain, and the hub
    /// sender are layered via the `with_*` builders.
    pub fn new(template: SpawnTemplate, tmux: Arc<dyn Tmux>) -> Self {
        Self {
            template,
            tmux,
            token: None,
            keychain: None,
            events: None,
            poll: Duration::from_secs(30),
            workspace: "default".to_string(),
            server_url: None,
            session_root: None,
        }
    }

    /// Set the tenant slug stamped into each per-session `.mcp.json` (default `"default"`).
    pub fn with_workspace(mut self, workspace: impl Into<String>) -> Self {
        self.workspace = workspace.into();
        self
    }

    /// Wire the gt MCP server base URL so each sling writes a per-session `.mcp.json` carrying the
    /// minted token — without it a role agent has no authenticated `gt` MCP access in its workdir.
    pub fn with_server_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.server_url = if url.is_empty() { None } else { Some(url) };
        self
    }

    /// Give each role agent its own working directory under `root` (`<root>/<session>`) so concurrent
    /// agents never overwrite each other's `.mcp.json`/token. `None` ⇒ they share the rig checkout.
    pub fn with_session_root(mut self, root: std::path::PathBuf) -> Self {
        self.session_root = Some(root);
        self
    }

    /// Mint a least-privilege `GT_TOKEN` per role sling (mirrors the polecat path). Without it a
    /// role agent carries no identity and its `gt` MCP calls are unauthenticated.
    pub fn with_agent_token(mut self, token: AgentTokenMinter) -> Self {
        self.token = Some(token);
        self
    }

    /// Resolve the active claude account's `CLAUDE_CONFIG_DIR` from the keychain at sling time, so a
    /// role agent burns the same rotated account a polecat would. `None` ⇒ host default `~/.claude`.
    pub fn with_keychain(mut self, keychain: Arc<dyn Keychain>) -> Self {
        self.keychain = Some(keychain);
        self
    }

    /// Emit the role session's lifecycle onto the hub and enable the exit watch that frees the
    /// single-flight slot. Required for the single-flight loop to re-arm in production.
    pub fn with_session_events(mut self, events: broadcast::Sender<EventRecord>) -> Self {
        self.events = Some(events);
        self
    }

    /// Override the exit-watch poll interval (tests use a short one).
    pub fn with_poll(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    fn emit(&self, ev: AgentEvent) {
        if let Some(tx) = &self.events {
            if let Ok(rec) = EventRecord::from_envelope(&Envelope::root(ev)) {
                let _ = tx.send(rec);
            }
        }
    }
}

impl RoleLauncher for SpecRoleLauncher {
    fn launch(&self, role: &str, session: &str, kickoff: &str) -> Result<(), String> {
        let token = self
            .token
            .as_ref()
            .and_then(|m| m.token_for(session, role).ok());
        // The same minted token rides the env (role_spec) AND the per-session `.mcp.json` below.
        let token_for_mcp = token.clone();
        let mut spec = role_spec(&self.template, role, session, kickoff, token);

        // Each role agent runs in its OWN working dir (`<session_root>/<session>`) when a session
        // root is configured, so two concurrent agents never overwrite each other's `.mcp.json`
        // token. Without a root they share the rig checkout (single-agent-safe only). Best-effort
        // create — a failure leaves the agent on the rig checkout, logged at spawn.
        let workdir = session_workdir(
            self.session_root.as_deref(),
            &self.template.workdir,
            session,
        );
        if workdir != self.template.workdir {
            if let Err(e) = std::fs::create_dir_all(&workdir) {
                eprintln!(
                    "[role-agent] workdir {} create skipped: {e} — using rig checkout",
                    workdir.display()
                );
            }
        }
        spec.workdir = workdir.clone();

        // Authenticated `gt` MCP for the role agent: write a per-session `.mcp.json` (+ `.gt-config`)
        // carrying the minted token, so its `mcp__gt__*` calls act as the role (least-privilege),
        // not the operator. Only when the daemon knows its own URL and a token was minted; otherwise
        // the agent has no MCP in its workdir (logged) and its kickoff's tool calls would fail.
        match (self.server_url.as_deref(), token_for_mcp.as_deref()) {
            (Some(url), Some(tok)) => {
                if !crate::worktree::write_mcp_json(
                    &workdir,
                    url,
                    &self.workspace,
                    &self.template.rig,
                    tok,
                ) {
                    eprintln!("[role-agent] .mcp.json write skipped for {session}");
                }
                let _ = crate::worktree::write_gt_config(
                    &workdir,
                    url,
                    &self.workspace,
                    &self.template.rig,
                    tok,
                );
            }
            _ => eprintln!(
                "[role-agent] no .mcp.json for {session} (server_url/token unset) — gt MCP tools unavailable in its workdir"
            ),
        }

        // Resolve the active account's credentials (best-effort, permissive on quota — the role
        // agent is short-lived; the periodic probe corrects status). A dead/absent account leaves
        // the agent on the host default `~/.claude`, which the daemon already onboarded.
        let mut effective_config_dir: Option<std::path::PathBuf> = None;
        if let Some(kc) = &self.keychain {
            if let crate::credential_guard::CredOutcome::Resolved { resolved, .. } =
                crate::credential_guard::resolve_for_sling(kc, now_ms(), |_| None, |_| 100.0)
            {
                effective_config_dir = Some(std::path::PathBuf::from(&resolved.config_dir));
                spec.env
                    .push(("CLAUDE_CONFIG_DIR".to_string(), resolved.config_dir));
            }
        }
        let effective_config_dir = effective_config_dir.or_else(|| {
            std::env::var_os("HOME").map(|h| std::path::Path::new(&h).join(".claude"))
        });
        if let Some(cd) = &effective_config_dir {
            // Pre-trust the project `.mcp.json` `gt` server + seed onboarding into the agent's
            // workdir, so a headless claude never stalls on the trust/onboarding dialog.
            crate::worktree::seed_claude_onboarding(cd, &spec.workdir);
        }

        spawn_tmux(self.tmux.as_ref(), &spec).map_err(|e| e.to_string())?;

        // Observability: the role agent is a session too. maintains_heartbeat=true because the
        // exit watch below TOUCHES the session's heartbeat file + emits agent.heartbeat.v1 every
        // poll while the tmux session lives (gtcore-efb7e6) — before this, role agents were
        // registered with neither heartbeat nor tmux_socket, which the session reconciler's safe
        // fallback could never judge: every registration whose exit watch died with the daemon
        // (orchd restart) sat in `spawned` forever (~9 zombie sheriffs observed). crew=None (not
        // a bead worker); the claude process itself installs no heartbeat hook, the WATCH is the
        // heartbeat source, matching how the polecat supervisor heartbeats its watched sessions.
        self.emit(AgentEvent::Spawned {
            session: session.to_string(),
            rig: self.template.rig.clone(),
            role: session_role_for(role),
            crew: None,
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: true,
            tmux_socket: None,
            spawned_by: Some("role-agent".into()),
        });

        // Exit watch: while the tmux session lives, maintain its liveness signals (heartbeat file
        // for the reconciler's staleness probe + agent.heartbeat.v1 for the sessions view); when
        // it is gone, emit session-end so the dispatcher frees the single-flight slot and remove
        // the heartbeat file so a reused session id starts clean. Only when a hub is wired (so
        // tests without one don't spawn a task).
        if let Some(tx) = &self.events {
            let tmux = self.tmux.clone();
            let tx = tx.clone();
            let session = session.to_string();
            let poll = self.poll;
            let heartbeat = spec.heartbeat.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(poll).await;
                    if !tmux.has_session(&session) {
                        let _ = std::fs::remove_file(&heartbeat);
                        if let Ok(rec) = EventRecord::from_envelope(&Envelope::root(
                            AgentEvent::session_end(session.clone()),
                        )) {
                            let _ = tx.send(rec);
                        }
                        break;
                    }
                    let _ = std::fs::write(&heartbeat, b"");
                    if let Ok(rec) = EventRecord::from_envelope(&Envelope::root(
                        AgentEvent::Heartbeat {
                            session: session.clone(),
                            timestamp_secs: Some(now_ms() / 1000),
                        },
                    )) {
                        let _ = tx.send(rec);
                    }
                }
            });
        }
        Ok(())
    }
}

/// The working directory a role agent's session runs in: its OWN dir under `session_root`
/// (`<root>/<session>`) when a root is configured, else the shared `fallback` (the rig checkout).
/// Pure, so the isolation rule is unit-tested without touching the filesystem.
fn session_workdir(
    session_root: Option<&std::path::Path>,
    fallback: &std::path::Path,
    session: &str,
) -> std::path::PathBuf {
    match session_root {
        Some(root) => root.join(session),
        None => fallback.to_path_buf(),
    }
}

/// Milliseconds since the unix epoch (the keychain guard's clock unit).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Hub observer that turns trigger events into role-agent slings (`gtcore-999795`). Registered on
/// the daemon's relay alongside the other edge plugins; gated behind `GT_ROLE_AGENTS` at the bin.
///
/// - `merge.failed.v1` / `merge.ready.v1` → **sheriff** trigger.
/// - `issues.closed.v1` → **witness** trigger.
/// - `agent.session-end.v1` / `agent.killed.v1` → free the role's single-flight slot.
///
/// The **deacon** health tick is time-driven, not event-driven, so the bin calls
/// [`RoleAgentDispatcher::on_trigger`] with [`RoleTrigger::HealthTick`] on a timer using the same
/// shared dispatcher (`Arc`) this plugin holds.
pub struct RoleAgentPlugin {
    dispatcher: Arc<RoleAgentDispatcher>,
}

impl RoleAgentPlugin {
    pub fn new(dispatcher: Arc<RoleAgentDispatcher>) -> Self {
        Self { dispatcher }
    }

    /// Log the dispatcher's decision uniformly (keeps `on_event` terse).
    fn report(&self, decision: RoleDispatch) {
        match decision {
            RoleDispatch::Slung { role, session } => {
                eprintln!("[role-agent] slung {role} as {session}")
            }
            RoleDispatch::SkippedAlreadyLive { role, session } => {
                eprintln!("[role-agent] {role} already live ({session}) — trigger absorbed")
            }
            RoleDispatch::LaunchFailed { role, reason } => {
                eprintln!("[role-agent] {role} sling failed: {reason}")
            }
        }
    }
}

#[async_trait]
impl Plugin for RoleAgentPlugin {
    fn name(&self) -> &'static str {
        "role-agent"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "merge.failed.v1" => {
                if let MergeEvent::Failed { bead, reason } = record.decode::<MergeEvent>()? {
                    self.report(
                        self.dispatcher
                            .on_trigger(&RoleTrigger::MergeFailed { bead, reason }),
                    );
                }
                Ok(())
            }
            "merge.ready.v1" => {
                if let MergeEvent::Ready { bead, branch, .. } = record.decode::<MergeEvent>()? {
                    self.report(
                        self.dispatcher
                            .on_trigger(&RoleTrigger::MergeReady { bead, branch }),
                    );
                }
                Ok(())
            }
            "issues.closed.v1" => {
                if let Some(id) = record.payload.get("id").and_then(|v| v.as_str()) {
                    self.report(self.dispatcher.on_trigger(&RoleTrigger::BeadClosed {
                        bead: id.to_string(),
                    }));
                }
                Ok(())
            }
            "agent.session-end.v1" | "agent.killed.v1" => {
                let session = match record.decode::<AgentEvent>()? {
                    AgentEvent::SessionEnd { session, .. } => session,
                    AgentEvent::Killed { session, .. } => session,
                    _ => return Ok(()),
                };
                if let Some(role) = self.dispatcher.on_session_end(&session) {
                    eprintln!("[role-agent] {role} session {session} ended — slot freed");
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Wire payload of an on-demand role-spawn request (gtcore-b69087): what `agent.spawn` with an
/// infra role emits onto the `role-spawn` channel and the orchd consumer decodes. `role` is the
/// flat token (`refinery`/`sheriff`/`witness`/`deacon`/`overseer`/`dog`); `mayor` and `polecat`
/// are rejected at decode — the mayor is orchestrator-owned and polecats ride the dispatch
/// channel.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoleSpawnPayload {
    pub role: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub requested_by: Option<String>,
}

impl RoleSpawnPayload {
    /// Map the wire payload onto the dispatcher trigger, rejecting non-infra roles.
    pub fn into_trigger(self) -> Result<RoleTrigger, String> {
        let role = match SessionRole::parse(&self.role) {
            Some(SessionRole::Dog(kind)) => kind,
            Some(SessionRole::Mayor) => {
                return Err("mayor is orchestrator-owned — the orchd dispatch loop raises it".into())
            }
            Some(SessionRole::Polecat) => {
                return Err("polecats ride the dispatch channel (agent.spawn with a `bead`), not role-spawn".into())
            }
            None => return Err(format!("unknown role `{}`", self.role)),
        };
        let mut reason = self
            .reason
            .filter(|r| !r.trim().is_empty())
            .unwrap_or_else(|| "operator on-demand request".to_string());
        if let Some(who) = self.requested_by.filter(|w| !w.trim().is_empty()) {
            reason = format!("{reason} (requested by {who})");
        }
        Ok(RoleTrigger::OnDemand { role, reason })
    }
}

/// Consume the `role-spawn` channel and materialize each request through the dispatcher
/// (gtcore-b69087). The single-flight invariant still holds — a request for an already-live role
/// is absorbed and logged. The launch shells tmux (blocking), so it runs on a blocking thread.
pub async fn run_on_demand<C: gt_channel::DispatchConsumer>(
    consumer: C,
    dispatcher: Arc<RoleAgentDispatcher>,
) -> Result<(), gt_channel::ChannelError> {
    let mut rx = consumer.subscribe(16)?;
    while let Some(msg) = rx.recv().await {
        match serde_json::from_slice::<RoleSpawnPayload>(&msg.payload) {
            Ok(payload) => match payload.into_trigger() {
                Ok(trigger) => {
                    let d = dispatcher.clone();
                    let outcome =
                        tokio::task::spawn_blocking(move || d.on_trigger(&trigger)).await;
                    match outcome {
                        Ok(RoleDispatch::Slung { role, session }) => eprintln!(
                            "[role-spawn] on-demand {role} slung as {session}"
                        ),
                        Ok(RoleDispatch::SkippedAlreadyLive { role, session }) => eprintln!(
                            "[role-spawn] on-demand {role} absorbed — {session} already live (single-flight)"
                        ),
                        Ok(RoleDispatch::LaunchFailed { role, reason }) => eprintln!(
                            "[role-spawn] on-demand {role} launch FAILED: {reason}"
                        ),
                        Err(e) => eprintln!("[role-spawn] launch task panicked: {e}"),
                    }
                }
                Err(e) => eprintln!("[role-spawn] rejected request: {e}"),
            },
            Err(e) => eprintln!("[role-spawn] undecodable payload ignored: {e}"),
        }
        if let Err(e) = consumer.ack(&msg) {
            eprintln!("[role-spawn] ack failed: {e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn ws() -> &'static str {
        "default"
    }

    // ---- kickoff builders (one assertion-set per role) -------------------------------------

    #[test]
    fn sheriff_kickoff_targets_the_failed_slot_with_merge_tools() {
        let t = RoleTrigger::MergeFailed {
            bead: "gtcore-abc123".into(),
            reason: "CI red: 2 tests".into(),
        };
        assert_eq!(role_for(&t), "sheriff");
        let k = kickoff_for(ws(), &t);
        assert!(k.contains("sheriff"), "names the role");
        assert!(k.contains("gtcore-abc123"), "carries the failed bead");
        assert!(k.contains("CI red: 2 tests"), "carries the failure reason");
        assert!(k.contains("mcp__gt__merge_submit"), "uses the recovery tool");
        assert!(k.contains("mcp__gt__merge_list"), "inspects the board");
        // Single-shot discipline + memory recall are appended to every kickoff.
        assert!(k.contains("mcp__gt__memory_recall"));
        assert!(k.contains("STOP"));
    }

    #[test]
    fn sheriff_merge_ready_kickoff_reviews_the_board() {
        let t = RoleTrigger::MergeReady {
            bead: "gtcore-9".into(),
            branch: "gtcore-9".into(),
        };
        assert_eq!(role_for(&t), "sheriff");
        let k = kickoff_for(ws(), &t);
        assert!(k.contains("sheriff"));
        assert!(k.contains("gtcore-9"));
        assert!(k.contains("mcp__gt__merge_list"));
        assert!(k.contains("STOP"));
    }

    #[test]
    fn witness_kickoff_verifies_the_closed_bead_against_its_ac() {
        let t = RoleTrigger::BeadClosed {
            bead: "gtcore-def456".into(),
        };
        assert_eq!(role_for(&t), "witness");
        let k = kickoff_for(ws(), &t);
        assert!(k.contains("witness"), "names the role");
        assert!(k.contains("gtcore-def456"), "carries the closed bead");
        assert!(k.contains("acceptance criteria"), "frames the QA job");
        assert!(k.contains("mcp__gt__issues_read"), "reads the bead");
        assert!(
            k.contains("mcp__gt__comments_create") && k.contains("mcp__gt__notify_send"),
            "flags a bad closure"
        );
        assert!(k.contains("mcp__gt__memory_recall"));
        assert!(k.contains("STOP"));
    }

    #[test]
    fn deacon_kickoff_scans_flow_health_read_only() {
        let t = RoleTrigger::HealthTick;
        assert_eq!(role_for(&t), "deacon");
        let k = kickoff_for(ws(), &t);
        assert!(k.contains("deacon"), "names the role");
        assert!(k.contains("read-only"), "deacon never mutates");
        assert!(k.contains("mcp__gt__issues_list"), "scans the tracker");
        assert!(k.contains("mcp__gt__notify_send"), "escalates findings");
        assert!(k.contains("STOP"));
    }

    // ---- dispatcher single-flight ----------------------------------------------------------

    /// Records every launch; can be told to fail to exercise the no-slot-held path.
    struct FakeLauncher {
        launches: Mutex<Vec<(String, String)>>, // (role, session)
        fail: AtomicBool,
    }
    impl FakeLauncher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                launches: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
            })
        }
        fn count(&self) -> usize {
            self.launches.lock().unwrap().len()
        }
    }
    impl RoleLauncher for FakeLauncher {
        fn launch(&self, role: &str, session: &str, _kickoff: &str) -> Result<(), String> {
            if self.fail.load(Ordering::Relaxed) {
                return Err("boom".into());
            }
            self.launches
                .lock()
                .unwrap()
                .push((role.to_string(), session.to_string()));
            Ok(())
        }
    }

    // ---- on-demand role spawn (gtcore-b69087) ----------------------------------------------

    #[test]
    fn on_demand_kickoff_names_role_and_reason() {
        let k = kickoff_for(
            ws(),
            &RoleTrigger::OnDemand {
                role: DogKind::Refinery,
                reason: "merge queue stuck, 2 ready slots (requested by operator)".into(),
            },
        );
        assert!(k.contains("**refinery**"), "kickoff frames the role: {k}");
        assert!(k.contains("mcp__gt__merge_list"), "names the board tool");
        assert!(
            k.contains("merge queue stuck, 2 ready slots"),
            "carries the request's reason"
        );
        assert!(k.contains("single-shot"), "keeps the stop discipline");
    }

    #[test]
    fn on_demand_trigger_slings_with_single_flight() {
        let launcher = FakeLauncher::new();
        let d = RoleAgentDispatcher::new("default", launcher.clone());
        let demand = || RoleTrigger::OnDemand {
            role: DogKind::Refinery,
            reason: "stuck queue".into(),
        };
        match d.on_trigger(&demand()) {
            RoleDispatch::Slung { role, session } => {
                assert_eq!(role, "refinery");
                assert!(session.starts_with("refinery-demand-"), "session {session}");
            }
            other => panic!("expected Slung, got {other:?}"),
        }
        // A second request while the first agent lives is absorbed — no racing herd.
        assert!(matches!(
            d.on_trigger(&demand()),
            RoleDispatch::SkippedAlreadyLive { role: "refinery", .. }
        ));
        assert_eq!(launcher.count(), 1);
    }

    #[test]
    fn role_spawn_payload_maps_infra_roles_and_rejects_owned_ones() {
        let t = RoleSpawnPayload {
            role: "refinery".into(),
            reason: Some("queue stuck".into()),
            requested_by: Some("operator@gt".into()),
        }
        .into_trigger()
        .expect("refinery is on-demand spawnable");
        match t {
            RoleTrigger::OnDemand { role, reason } => {
                assert_eq!(role, DogKind::Refinery);
                assert!(reason.contains("queue stuck") && reason.contains("operator@gt"));
            }
            other => panic!("expected OnDemand, got {other:?}"),
        }

        // The mayor is orchestrator-owned; polecats ride the dispatch channel; garbage is named.
        for (role, want) in [
            ("mayor", "orchestrator-owned"),
            ("polecat", "dispatch channel"),
            ("banana", "unknown role"),
        ] {
            let err = RoleSpawnPayload {
                role: role.into(),
                reason: None,
                requested_by: None,
            }
            .into_trigger()
            .unwrap_err();
            assert!(err.contains(want), "{role}: {err}");
        }
    }

    #[test]
    fn first_trigger_slings_then_repeats_are_absorbed_until_session_ends() {
        let launcher = FakeLauncher::new();
        let d = RoleAgentDispatcher::new("default", launcher.clone());

        let fail = || RoleTrigger::MergeFailed {
            bead: "gtcore-1".into(),
            reason: "x".into(),
        };

        // First trigger slings exactly one sheriff.
        let first = d.on_trigger(&fail());
        let session = match first {
            RoleDispatch::Slung { role, session } => {
                assert_eq!(role, "sheriff");
                session
            }
            other => panic!("expected Slung, got {other:?}"),
        };
        assert!(d.is_live("sheriff"));
        assert_eq!(launcher.count(), 1);

        // A burst of further triggers while the sheriff is live is absorbed (single-flight):
        // idle cost stays bounded, no second sling.
        for _ in 0..3 {
            match d.on_trigger(&fail()) {
                RoleDispatch::SkippedAlreadyLive { role, session: s } => {
                    assert_eq!(role, "sheriff");
                    assert_eq!(s, session);
                }
                other => panic!("expected SkippedAlreadyLive, got {other:?}"),
            }
        }
        assert_eq!(launcher.count(), 1, "no double-sling while live");

        // Session ends → slot frees → next trigger re-slings with a fresh session id.
        assert_eq!(d.on_session_end(&session), Some("sheriff"));
        assert!(!d.is_live("sheriff"));
        match d.on_trigger(&fail()) {
            RoleDispatch::Slung { role, session: s2 } => {
                assert_eq!(role, "sheriff");
                assert_ne!(s2, session, "fresh session id after re-sling");
            }
            other => panic!("expected re-sling, got {other:?}"),
        }
        assert_eq!(launcher.count(), 2);
    }

    #[test]
    fn distinct_roles_run_concurrently() {
        let launcher = FakeLauncher::new();
        let d = RoleAgentDispatcher::new("default", launcher.clone());

        let s = d.on_trigger(&RoleTrigger::MergeFailed {
            bead: "gtcore-1".into(),
            reason: "x".into(),
        });
        let w = d.on_trigger(&RoleTrigger::BeadClosed {
            bead: "gtcore-2".into(),
        });
        let dn = d.on_trigger(&RoleTrigger::HealthTick);

        assert!(matches!(s, RoleDispatch::Slung { role: "sheriff", .. }));
        assert!(matches!(w, RoleDispatch::Slung { role: "witness", .. }));
        assert!(matches!(dn, RoleDispatch::Slung { role: "deacon", .. }));
        assert!(d.is_live("sheriff") && d.is_live("witness") && d.is_live("deacon"));
        assert_eq!(launcher.count(), 3, "one role does not block another");
    }

    #[test]
    fn launch_failure_does_not_hold_the_slot() {
        let launcher = FakeLauncher::new();
        launcher.fail.store(true, Ordering::Relaxed);
        let d = RoleAgentDispatcher::new("default", launcher.clone());

        match d.on_trigger(&RoleTrigger::HealthTick) {
            RoleDispatch::LaunchFailed { role, .. } => assert_eq!(role, "deacon"),
            other => panic!("expected LaunchFailed, got {other:?}"),
        }
        // No slot held → a subsequent (successful) trigger can still sling.
        assert!(!d.is_live("deacon"));
        launcher.fail.store(false, Ordering::Relaxed);
        assert!(matches!(
            d.on_trigger(&RoleTrigger::HealthTick),
            RoleDispatch::Slung { role: "deacon", .. }
        ));
    }

    #[test]
    fn unknown_session_end_is_a_noop() {
        let launcher = FakeLauncher::new();
        let d = RoleAgentDispatcher::new("default", launcher);
        assert_eq!(d.on_session_end("not-a-role-session"), None);
    }

    #[test]
    fn session_ids_are_sanitized() {
        assert_eq!(sanitize("gtcore-abc"), "gtcore-abc");
        assert_eq!(sanitize("weird/id space"), "weird-id-space");
        assert_eq!(sanitize("--"), "x");
    }

    // ---- production spec shaping ------------------------------------------------------------

    fn template() -> SpawnTemplate {
        SpawnTemplate {
            rig: "gtcore".into(),
            prefix: "gt".into(),
            workdir: std::path::PathBuf::from("/rig-wt/gtcore"),
            command: "claude".into(),
            args: vec!["--dangerously-skip-permissions".into()],
            base_env: vec![
                ("GT_ROLE".into(), "polecat".into()),
                ("GT_RIG".into(), "gtcore".into()),
            ],
            heartbeat_dir: std::path::PathBuf::from("/tmp/hb"),
        }
    }

    #[test]
    fn role_spec_overrides_role_appends_kickoff_and_pins_no_bead() {
        let spec = role_spec(&template(), "sheriff", "sheriff-gtcore-1-0", "DO THE THING", Some("tok".into()));

        // GT_ROLE is overridden from the template's polecat default to the role.
        let role = spec.env.iter().find(|(k, _)| k == "GT_ROLE").map(|(_, v)| v.as_str());
        assert_eq!(role, Some("sheriff"));
        // The minted token rides the env.
        assert!(spec.env.iter().any(|(k, v)| k == "GT_TOKEN" && v == "tok"));
        // The kickoff is the final positional arg, after the template's fixed args.
        assert_eq!(spec.args.last().map(String::as_str), Some("DO THE THING"));
        assert!(spec.args.contains(&"--dangerously-skip-permissions".to_string()));
        // Not a bead worker: no pinned bead/issue.
        assert_eq!(spec.hook_bead, None);
        assert_eq!(spec.issue, None);
        assert_eq!(spec.session, "sheriff-gtcore-1-0");
        assert_eq!(spec.heartbeat, std::path::PathBuf::from("/tmp/hb/sheriff-gtcore-1-0.heartbeat"));
    }

    #[test]
    fn role_spec_without_token_omits_gt_token() {
        let spec = role_spec(&template(), "deacon", "deacon-tick-0", "SCAN", None);
        assert!(!spec.env.iter().any(|(k, _)| k == "GT_TOKEN"));
    }

    #[test]
    fn session_workdir_isolates_per_session_under_a_root_else_shares_the_checkout() {
        let rig = std::path::Path::new("/rig-wt/gtcore");
        let root = std::path::Path::new("/rig-wt");
        // With a session root each agent gets its OWN dir — two roles never collide on one .mcp.json.
        assert_eq!(
            session_workdir(Some(root), rig, "sheriff-gtcore-1-0"),
            std::path::PathBuf::from("/rig-wt/sheriff-gtcore-1-0")
        );
        assert_ne!(
            session_workdir(Some(root), rig, "sheriff-gtcore-1-0"),
            session_workdir(Some(root), rig, "witness-gtcore-2-1"),
        );
        // Without a root they fall back to the shared rig checkout.
        assert_eq!(session_workdir(None, rig, "deacon-tick-0"), rig.to_path_buf());
    }

    #[test]
    fn session_role_maps_each_role_to_its_dog_kind() {
        assert_eq!(session_role_for("sheriff").as_str(), "sheriff");
        assert_eq!(session_role_for("witness").as_str(), "witness");
        assert_eq!(session_role_for("deacon").as_str(), "deacon");
    }
}
