//! Autonomous polecat supervision for the orchestration daemon (`hq-orchd.3`).
//!
//! This is the production caller-loop `gt-polecat` was missing: the library has the spawner
//! ([`spawn_tmux`] / [`SpawnTemplate`]), the death-detector ([`PolecatSupervisor`]), and the pure
//! admission core ([`PoolAllocator`]), but nothing wired them onto a running event hub. The daemon
//! does:
//!
//! - **Sling on dispatch** — [`PolecatSupervisorPlugin`] observes the workspace hub: a
//!   `scheduling.dispatched.v1` claims a pool slot and, if admitted, spawns a tmux-backed polecat
//!   for the dispatched bead and registers it with the supervisor. A `merge.merged.v1` means that
//!   bead's work landed, so the polecat is unwatched (never re-slung) and its slot released.
//! - **Re-sling dead ones** — the bin drives [`PolecatSupervisor::tick`] on a timer (the live half
//!   of supervision); a polecat whose tmux session died is re-slung with backoff until its work
//!   completes.
//! - **Track host capacity** — [`host_cap_from_metrics`] recomputes the host-wide admission cap
//!   from live CPU + RAM, which the bin feeds to [`PoolAllocator::set_host_cap`] on a timer so the
//!   ceiling tracks real headroom (`hq-mt-deploy.5`, folded here).
//!
//! Eviction on a shrunk cap is *not* done — admission is side-effect-free, running polecats finish
//! naturally (NN#2, mirrored from [`gt_polecat::pool`]).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionRole};
use gt_auth::{JwtClaims, JwtMinter};
use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope};
use gt_issues::{resolve_dispatch, should_sling};
use gt_mcp_server::bead_prefix;
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
use gt_polecat::{
    hooks_from_settings, skills_from_worktree, spawn_tmux, PolecatSupervisor, PoolAllocator,
    SpawnTemplate, Tmux,
};
use gt_quota::{Keychain, QuotaHandle};
use gt_scheduling::actor::SchedHandle;
use gt_scheduling::SchedEvent;
use gt_skills::{ModelConfig, SkillState};
use gt_store_dolt::{DoltIssues, IssueStatus};

use crate::mcp::EventLog;
use crate::operator_event::IssueOperatorEvent;
use crate::polecat_event::PolecatEvent;

/// Re-read `bead`'s CURRENT tracker state and decide whether a polecat should be (re-)slung for it
/// (gtcore-db99e0) — the unified slingability gate shared by the dispatch→sling path (which, at
/// boot, replays `replay_orphaned_inflight`'s crash-orphaned beads) and the supervisor's
/// dead-polecat re-sling probe.
///
/// Reads the bead detail plus the unscoped `child_of` + dispatch maps so the dispatch policy is
/// resolved through inheritance EXACTLY as [`gt_issues::ready_for_auto`] does, then applies the pure
/// [`gt_issues::should_sling`] predicate (status ∈ {open,working} ∧ ¬epic ∧ dispatch=auto). An
/// unknown bead or any read error is treated as SLINGABLE (permissive) so a transient Dolt hiccup
/// never silently abandons live work — the same conservative degradation as the closed-bead guard
/// this generalizes.
pub async fn bead_should_sling(issues: &DoltIssues, bead: &str) -> bool {
    let detail = match issues.get_detail(bead).await {
        Ok(Some(d)) => d,
        Ok(None) => return true, // unknown bead → permissive (sling)
        Err(e) => {
            eprintln!(
                "[polecat] slingability probe: get_detail({bead}) failed: {e} — treating as slingable"
            );
            return true;
        }
    };
    // Unscoped maps (empty rig + ws) — an ancestor epic may live outside any one rig/workspace,
    // exactly the inputs `resolve_dispatch` expects. A map read error degrades to an empty map,
    // which resolves to the bead's own dispatch value (or Manual) — never a panic.
    let parents = issues.parent_map("", "").await.unwrap_or_default();
    let raw = issues.dispatch_index().await.unwrap_or_default();
    let dispatch = resolve_dispatch(bead, detail.dispatch.as_deref(), &parents, &raw);
    should_sling(&detail.status, &detail.issue_type, dispatch)
}

/// Resolves the least-privilege scope set for an agent role (`hq-agent-provisioning.3`). The
/// production resolver delegates to `gt_skills::SkillCatalog::scopes_for_roles`; tests pass a
/// closure. Returns the role's scopes, never `*`.
pub type ScopeResolver = Arc<dyn Fn(&str) -> Vec<String> + Send + Sync>;

/// Mints a per-agent least-privilege JWT for a slung polecat (`hq-agent-provisioning.3`).
///
/// On every sling the supervisor stamps `GT_TOKEN` into the polecat's env so its `gt`/hooks/MCP
/// calls carry the agent's OWN identity — scoped to its role via [`ScopeResolver`] — instead of
/// inheriting the operator's admin (`*`) config. The token is RS256-signed by [`JwtMinter`] (the
/// daemon holds the private key); the gateway verifies it with the matching public key. A short
/// `ttl_secs` bounds exposure — a polecat that outlives it is re-slung with a fresh token.
pub struct AgentTokenMinter {
    minter: JwtMinter,
    scopes_for_role: ScopeResolver,
    workspace: String,
    ttl_secs: u64,
}

impl AgentTokenMinter {
    /// Wire the RS256 `minter`, the role→scopes `resolver`, the tenant `workspace`, and the token
    /// `ttl_secs`.
    pub fn new(
        minter: JwtMinter,
        scopes_for_role: ScopeResolver,
        workspace: impl Into<String>,
        ttl_secs: u64,
    ) -> Self {
        Self {
            minter,
            scopes_for_role,
            workspace: workspace.into(),
            ttl_secs,
        }
    }

    /// Mint a token for `session` running as `role`. `sub` is the session id, scopes come from the
    /// resolver (least-privilege; never `*`), and `exp` is `now + ttl_secs`.
    fn token_for(&self, session: &str, role: &str) -> Result<String, gt_auth::AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let claims = JwtClaims {
            sub: session.to_string(),
            workspace: self.workspace.clone(),
            scopes: (self.scopes_for_role)(role),
            exp: now + self.ttl_secs,
            nbf: None,
            iat: now,
        };
        self.minter.mint(&claims)
    }
}

/// Everything the supervisor needs to sling a polecat into ONE catalog rig (`hq-0ecfec`, epic
/// hq-554308): the rig's [`SpawnTemplate`] (built via [`SpawnTemplate::for_rig`] from the live
/// rig catalog at boot) plus its per-polecat worktree root. Keyed by bead prefix in
/// [`PolecatSupervisorPlugin::with_rig_configs`] so a `gtweb-*` bead lands in the gtweb checkout
/// instead of the boot rig's.
#[derive(Debug, Clone)]
pub struct RigConfig {
    pub template: SpawnTemplate,
    /// Per-polecat worktree root for this rig. `None` ⇒ the rig's polecats share
    /// `template.workdir` directly (same semantics as the legacy `worktree_root`).
    pub worktree_root: Option<std::path::PathBuf>,
}

/// Observer that turns the scheduler's dispatch decisions into live, supervised tmux polecats for
/// one workspace, bounded by a shared [`PoolAllocator`]. Registered on the daemon's event hub
/// alongside the reactor arms (`hq-orchd.3`).
pub struct PolecatSupervisorPlugin {
    workspace: String,
    tmux: Arc<dyn Tmux>,
    template: SpawnTemplate,
    supervisor: Arc<PolecatSupervisor>,
    allocator: Arc<Mutex<PoolAllocator>>,
    /// Hub sender for the agent session lifecycle events (`hq-orchd.6`). `None` ⇒ no emission
    /// (the `.3` tests construct the plugin without a hub).
    events: Option<broadcast::Sender<EventRecord>>,
    /// Per-agent token minter (`hq-agent-provisioning.3`). `None` ⇒ the polecat is slung without a
    /// `GT_TOKEN` (it falls back to whatever creds its checkout carries).
    token: Option<AgentTokenMinter>,
    /// Claude-account keychain (`hq-agent-provisioning.7`). `None` ⇒ the polecat uses the host's
    /// default `~/.claude` (a single account). With it, each sling reads the keychain's live
    /// pointer and stamps that account's `CLAUDE_CONFIG_DIR` so the polecat's claude burns the
    /// account predictive rotation has selected.
    keychain: Option<Arc<dyn Keychain>>,
    /// Per-polecat git worktree root (`hq-orchd-deploy.9`). `None` ⇒ every polecat shares the
    /// template's `GT_RIG_PATH` checkout (legacy). `Some(root)` ⇒ each sling is provisioned its own
    /// worktree `<root>/<session>` (branch = bead) off that checkout, so concurrent polecats never
    /// race on a shared HEAD (CLAUDE.md). The base checkout is `template.workdir`.
    worktree_root: Option<std::path::PathBuf>,
    /// Non-root user the polecat re-execs as (`hq-quota-accounts.6`, `GT_POLECAT_RUN_AS`). When set,
    /// a freshly-provisioned worktree is `chown`ed to it so the dropped-privilege polecat can use
    /// the (otherwise root-owned) tree. `None` ⇒ the polecat runs as the daemon's uid (legacy).
    run_as: Option<String>,
    /// Base URL of the gt MCP server (`hq-polecat-rig-config.1`, `GT_SELF_URL`). When set, each
    /// sling writes a fresh `.mcp.json` into the worktree using the per-session token — so the
    /// agent's MCP auth survives rig changes and token rotation without the operator re-placing a
    /// static file. `None` ⇒ falls back to copying the operator-placed `.mcp.json` from the base
    /// checkout (legacy behaviour, the token in that file may be expired or rig-specific).
    server_url: Option<String>,
    /// Base URL of the Anthropic passthrough proxy (`hq-284842`, `GT_ANTHROPIC_PROXY_URL`).
    /// When set, each sling stamps `ANTHROPIC_BASE_URL` so the polecat's claude routes through
    /// the proxy, plus `ANTHROPIC_CUSTOM_HEADERS` with `x-gt-account`/`x-gt-session` so the
    /// proxy can attribute every call. `None` ⇒ claude talks straight to the real API
    /// (no per-call quota truth).
    anthropic_proxy_url: Option<String>,
    /// Event log to read the Knowledge role prompt from (`hq-polecat-knowledge.1`). When set,
    /// each sling replays the `skills.*` stream and writes the polecat role's prompt as
    /// `CLAUDE.md` in the worktree — the same pattern `terminal.rs` uses for interactive
    /// sessions. Placeholders `<workspace>`, `<bead>`, `<branch>` are rendered with the sling's
    /// real values. `None` ⇒ no CLAUDE.md is written (the repo's project CLAUDE.md is all the
    /// polecat sees).
    event_log: Option<Arc<EventLog>>,
    /// Per-rig spawn configs keyed by bead prefix (`hq-0ecfec`, epic hq-554308). A dispatched
    /// bead whose prefix matches routes to that rig's template + worktree root; an unknown
    /// prefix falls back to the legacy single `template`/`worktree_root` — so a deployment that
    /// never calls [`Self::with_rig_configs`] behaves exactly as before.
    rig_configs: HashMap<String, RigConfig>,
    /// Dolt issues store for transitioning beads to `working` at sling time (`gtcore-orchd-working`).
    /// When set, a successful spawn immediately flips the bead `open → working` in Dolt so the
    /// frontend and frontier see the state change without waiting for the polecat to self-transition.
    /// `None` ⇒ the bead stays `open` until the polecat calls `issues.transition` itself (legacy).
    issues: Option<Arc<DoltIssues>>,
    /// Quota actor handle for the sling-time quota-status gate (gtcore-2836bb). When set, the
    /// credential guard also rejects an active account that is quota-`Limited`/`Blocked` — even with
    /// valid credentials — and rotates to a `Healthy` one, so a polecat is never born into the
    /// rate-limit dialog. `None` ⇒ the guard checks credential validity only (legacy).
    quota: Option<QuotaHandle>,
    /// Session ids whose pool slot this plugin currently holds (gtcore-b05dbc). A slot is added
    /// here when a sling succeeds (the claim landed AND the spawn landed) and removed when it is
    /// released — at merge, OR when the session dies by any path (operator kill, heartbeat-stale,
    /// reconciler reap, clean self-exit) WITHOUT a merge. Membership is what makes
    /// [`release_slot_for`](Self::release_slot_for) idempotent and correctly scoped: a slot is
    /// released exactly once (a duplicate or post-merge death event is a no-op), and only for a
    /// session THIS workspace's plugin actually claimed — so a death event for some other
    /// workspace's session never decrements the wrong pool. Without this set, the slot leaked
    /// until the patrol lease expired (~300s), wedging all new slings on a phantom `pool cap`.
    claimed: Mutex<HashSet<String>>,
    /// Scheduler handle for releasing dispatch capacity when a bead's CI-retry budget is exhausted
    /// (gtcore-3a1bd4). The CI-failure re-sling keeps the slot held across retries (the bead is
    /// still in flight); only on cap-exhaustion is the bead abandoned, so the scheduler governor
    /// must be told the slot freed (mirroring `merge.merged.v1`). `None` ⇒ only the pool allocator
    /// is freed (tests without a scheduler).
    sched: Option<SchedHandle>,
    /// Per-bead count of CI-failure re-slings (gtcore-3a1bd4). A bead whose PR fails CI is
    /// re-dispatched with a fix-and-re-push prompt; this counts the attempts so the loop stops at
    /// [`Self::ci_max_retries`] instead of burning quota forever. In-memory (a daemon restart resets
    /// it — acceptable: a restart is itself a fresh start, and the merge slot's `failed` state
    /// survives in the log for the operator). Cleared for a bead when it merges or is escalated.
    ci_retries: Arc<Mutex<HashMap<String, u32>>>,
    /// Hard cap on CI-failure re-slings before escalating to the operator (gtcore-3a1bd4,
    /// `GT_CI_MAX_RETRIES`). After this many failed attempts the bead is abandoned with an alert
    /// rather than re-slung again — the "no infinite loop" half of the AC.
    ci_max_retries: u32,
}

/// Default CI-failure retry cap when `GT_CI_MAX_RETRIES` is unset (gtcore-3a1bd4): three automated
/// fix-and-re-push attempts before escalating to a human.
pub const DEFAULT_CI_MAX_RETRIES: u32 = 3;

impl PolecatSupervisorPlugin {
    /// Wire the dispatch→sling observer for `workspace`. `tmux` is the edge adapter (real
    /// [`TmuxCli`](gt_polecat::TmuxCli) in the daemon, a fake in tests); `template` is the rig's
    /// env-sourced [`SpawnTemplate`]; `supervisor` + `allocator` are shared with the bin's
    /// re-sling + capacity timers.
    pub fn new(
        workspace: impl Into<String>,
        tmux: Arc<dyn Tmux>,
        template: SpawnTemplate,
        supervisor: Arc<PolecatSupervisor>,
        allocator: Arc<Mutex<PoolAllocator>>,
    ) -> Self {
        Self {
            workspace: workspace.into(),
            tmux,
            template,
            supervisor,
            allocator,
            events: None,
            token: None,
            keychain: None,
            worktree_root: None,
            run_as: None,
            server_url: None,
            anthropic_proxy_url: None,
            event_log: None,
            rig_configs: HashMap::new(),
            issues: None,
            quota: None,
            claimed: Mutex::new(HashSet::new()),
            sched: None,
            ci_retries: Arc::new(Mutex::new(HashMap::new())),
            ci_max_retries: DEFAULT_CI_MAX_RETRIES,
        }
    }

    /// Wire the scheduler handle so a CI-retry-exhausted bead frees its dispatch capacity
    /// (gtcore-3a1bd4). Without it, an abandoned bead's slot is only returned to the pool allocator,
    /// not the scheduler governor — fine for tests, but the daemon wires both.
    pub fn with_scheduler(mut self, sched: SchedHandle) -> Self {
        self.sched = Some(sched);
        self
    }

    /// Set the CI-failure retry cap (gtcore-3a1bd4, `GT_CI_MAX_RETRIES`). A bead whose PR keeps
    /// failing CI is re-slung up to this many times before being escalated to the operator. `0`
    /// disables auto-retry (the first CI failure escalates immediately).
    pub fn with_ci_max_retries(mut self, max: u32) -> Self {
        self.ci_max_retries = max;
        self
    }

    /// Wire the quota actor so sling-time selection also rejects quota-`Limited`/`Blocked` accounts
    /// (gtcore-2836bb): the credential guard rotates off a non-`Healthy` active account onto a
    /// `Healthy` one. Without it, an account that is credential-valid but rate-limited still receives
    /// slings and the polecat is born into the usage-limit dialog.
    pub fn with_quota(mut self, quota: QuotaHandle) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Route dispatched beads to their rig by bead prefix (`hq-0ecfec`, epic hq-554308): a
    /// `gtweb-*` bead slings in the gtweb rig's checkout/template instead of the boot rig's.
    /// Beads with a prefix not in the map keep the legacy single-template path.
    pub fn with_rig_configs(mut self, configs: HashMap<String, RigConfig>) -> Self {
        self.rig_configs = configs;
        self
    }

    /// Transition beads to `working` in Dolt at sling time (`gtcore-orchd-working`): a successful
    /// spawn immediately flips `open → working` so the frontend sees the state change and the
    /// auto-dispatch frontier excludes the bead. Without it, the bead stays `open` until the
    /// polecat self-transitions (if it does at all — the gap this closes).
    pub fn with_issues(mut self, issues: Arc<DoltIssues>) -> Self {
        self.issues = Some(issues);
        self
    }

    /// The (template, worktree_root) pair for `bead`, by its rig prefix. Falls back to the
    /// legacy single `template` + `worktree_root` for an unknown prefix, so single-rig
    /// deployments are untouched.
    fn route(&self, bead: &str) -> (&SpawnTemplate, Option<&std::path::PathBuf>) {
        match self.rig_configs.get(bead_prefix(bead)) {
            Some(cfg) => (&cfg.template, cfg.worktree_root.as_ref()),
            None => (&self.template, self.worktree_root.as_ref()),
        }
    }

    /// Route each polecat's claude through the Anthropic passthrough proxy (`hq-284842`):
    /// stamps `ANTHROPIC_BASE_URL` + the `x-gt-account`/`x-gt-session` attribution headers
    /// (via `ANTHROPIC_CUSTOM_HEADERS`) at sling time. Without it, calls go straight to the
    /// real API and quota truth arrives only via the periodic /usage sweep.
    pub fn with_anthropic_proxy(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.anthropic_proxy_url = if url.is_empty() { None } else { Some(url) };
        self
    }

    /// Load the polecat role's prompt from the Knowledge event log at sling time and materialise
    /// it as `CLAUDE.md` in the worktree (`hq-polecat-knowledge.1`). Mirrors `terminal.rs`'s
    /// `prepare_role_skills`: the behavioral instructions live in a configurable Knowledge prompt,
    /// the positional kickoff carries only the task context (bead + completion command). Without
    /// this, the polecat sees only the repo's project CLAUDE.md.
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.event_log = Some(log);
        self
    }

    /// Wire the daemon's base URL so each sling gets a fresh per-session `.mcp.json`
    /// (`hq-polecat-rig-config.1`). Without it the plugin falls back to copying the
    /// operator-placed static file from the base rig checkout.
    pub fn with_server_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.server_url = if url.is_empty() { None } else { Some(url) };
        self
    }

    /// Mint a least-privilege `GT_TOKEN` into each slung polecat's env (`hq-agent-provisioning.3`).
    /// Without it, a polecat carries no agent identity of its own.
    pub fn with_agent_token(mut self, token: AgentTokenMinter) -> Self {
        self.token = Some(token);
        self
    }

    /// Stamp the active claude account's `CLAUDE_CONFIG_DIR` into each slung polecat's env
    /// (`hq-agent-provisioning.7`). The keychain's live pointer is flipped by the predictive
    /// rotation observer ([`crate::quota_rotation::QuotaRotationPlugin`]); reading it at sling time
    /// is what makes the *next* polecat pick up the rotated account. Without this, every polecat
    /// shares the host default `~/.claude` — one account, one limit.
    pub fn with_keychain(mut self, keychain: Arc<dyn Keychain>) -> Self {
        self.keychain = Some(keychain);
        self
    }

    /// Provision each polecat its own git worktree under `root` (`hq-orchd-deploy.9`), isolating
    /// the per-bead branch from the shared rig checkout so concurrent polecats don't race on HEAD.
    /// Without it, every polecat works in `template.workdir` directly (legacy single-checkout).
    pub fn with_worktree_root(mut self, root: std::path::PathBuf) -> Self {
        self.worktree_root = Some(root);
        self
    }

    /// Re-exec each polecat as a dedicated non-root user (`hq-quota-accounts.6`): the provisioned
    /// worktree is `chown`ed to it so the dropped-privilege polecat can use the tree. The command
    /// re-exec itself is wired by `SpawnTemplate::from_env` (`GT_POLECAT_RUN_AS` → `runuser`); this
    /// is the matching filesystem half.
    pub fn with_run_as(mut self, user: impl Into<String>) -> Self {
        let user = user.into();
        self.run_as = if user.trim().is_empty() {
            None
        } else {
            Some(user)
        };
        self
    }

    /// Emit the agent session lifecycle events (`agent.spawned.v1` on sling, `agent.session-end.v1`
    /// when the bead merges) onto `events` (`hq-orchd.6`). These flow through the hub to the
    /// session-minutes projector (and the durable log), so a slung polecat's runtime feeds the
    /// `gt_workspace_session_minutes` cost counter. Without this, the plugin only supervises.
    pub fn with_session_events(mut self, events: broadcast::Sender<EventRecord>) -> Self {
        self.events = Some(events);
        self
    }

    /// Publish an [`AgentEvent`] onto the hub if a sender is wired (best-effort: a closed hub or an
    /// encode failure is swallowed — session metrics are observational, never load-bearing).
    fn emit(&self, event: AgentEvent) {
        if let Some(tx) = &self.events {
            if let Ok(record) = EventRecord::from_envelope(&Envelope::root(event)) {
                let _ = tx.send(record);
            }
        }
    }

    /// Publish an [`IssueOperatorEvent`] onto the hub so the FE sees which agent operates a bead
    /// (`hq-agent-observability.2`). Same best-effort path as [`emit`](Self::emit): the daemon root
    /// persists the hub record to the per-workspace log the `?channel=issues` SSE feed reads, so a
    /// closed hub or an encode failure only costs the marker, never the sling itself.
    fn emit_operator(&self, event: IssueOperatorEvent) {
        if let Some(tx) = &self.events {
            if let Ok(record) = EventRecord::from_envelope(&Envelope::root(event)) {
                let _ = tx.send(record);
            }
        }
    }

    /// Publish a [`PolecatEvent`] (sling skipped / failed) onto the hub so the workflow-notify
    /// observer turns it into an operator notification (`gtcore-7e19fe`). Same best-effort path as
    /// [`emit`](Self::emit): a closed hub or an encode failure only costs the notification — the
    /// sling decision (skip / release) has already been made by the caller.
    fn emit_polecat(&self, event: PolecatEvent) {
        if let Some(tx) = &self.events {
            if let Ok(record) = EventRecord::from_envelope(&Envelope::root(event)) {
                let _ = tx.send(record);
            }
        }
    }

    /// Record that `session`'s pool slot is held by this plugin (gtcore-b05dbc). Called right after
    /// a sling succeeds (claim + spawn both landed), so a later death/merge of that session can
    /// release exactly the one slot it holds.
    fn mark_claimed(&self, session: &str) {
        self.claimed
            .lock()
            .expect("claimed mutex")
            .insert(session.to_string());
    }

    /// Release the pool slot held for `session`, idempotently (gtcore-b05dbc).
    ///
    /// Returns `true` if a slot was actually released. The release is gated on the session being
    /// in [`claimed`](Self::claimed): only a session this workspace's plugin claimed is released,
    /// and a second call for the same session (a duplicate death event, or a death that arrives
    /// after the merge already released it) is a harmless no-op. This is what ends the pool leak —
    /// any path that ends a session (kill / heartbeat-stale / reconciler reap / clean exit) frees
    /// the slot at once instead of waiting out the ~300s patrol lease — while keeping the count
    /// exact (no double-release, no releasing another workspace's slot).
    fn release_slot_for(&self, session: &str) -> bool {
        if !self.claimed.lock().expect("claimed mutex").remove(session) {
            return false;
        }
        self.allocator
            .lock()
            .expect("pool mutex")
            .release(&self.workspace);
        true
    }

    /// Re-sling a polecat that died of context exhaustion, handing the next agent a continuation
    /// prompt instead of the original kickoff (gtcore-3b2a68).
    ///
    /// The dead polecat checkpointed its progress into the bead notes (gtcore-2467b4) and left
    /// its work committed on the branch. The bead is NOT failed or bounced to `open` — it stays
    /// `working` (this method performs NO transition) and is re-slung directly: the same session
    /// id, the same per-bead worktree, but a prompt assembled from the checkpoint notes, the
    /// branch diff, and the acceptance criteria ([`crate::continuation`]). The continuator thus
    /// boots with a clean context window but full knowledge of the prior work.
    ///
    /// Every step is best-effort: a missing spec (the polecat is no longer supervised), an
    /// unreadable bead, or an empty diff degrades the prompt rather than aborting — liveness over
    /// completeness, matching the rest of the sling path. The pool slot is left as-is: it was
    /// claimed at the original sling and is only released when the bead merges, so re-slinging
    /// into the same slot needs no new claim.
    async fn resling_on_context_exhaustion(&self, session: &str, reason: &str) {
        let Some(mut spec) = self.supervisor.spec_for_session(session) else {
            eprintln!(
                "[polecat] context-exhaustion re-sling skipped for {session}: not supervised here"
            );
            return;
        };
        let bead = spec
            .hook_bead
            .clone()
            .unwrap_or_else(|| session.to_string());

        // Read the previous agent's checkpoint notes + the acceptance criteria from Dolt. Without
        // an issues handle (or on a read error) the continuation prompt falls back to the diff +
        // whatever the agent can read via its own `gt` MCP tools.
        let (notes, acceptance_criteria) = match &self.issues {
            Some(issues) => match issues.get_detail(&bead).await {
                Ok(Some(detail)) => (detail.notes, detail.acceptance_criteria),
                Ok(None) => (String::new(), String::new()),
                Err(e) => {
                    eprintln!(
                        "[polecat] context-exhaustion re-sling: get_detail({bead}) failed: {e} — continuing with diff only"
                    );
                    (String::new(), String::new())
                }
            },
            None => (String::new(), String::new()),
        };

        // Read what the dead polecat already committed on its branch (vs main) from its worktree.
        let diff = crate::continuation::read_branch_diff(&spec.workdir, "main");
        let prompt = crate::continuation::build_continuation_prompt(
            &bead,
            &notes,
            &diff,
            &acceptance_criteria,
        );

        // The bead prompt is always the final positional arg (spec_for pushes it last; the
        // dispatch path inserts `--settings` BEFORE it), so swapping the last element retargets
        // claude's kickoff at the continuation prompt without disturbing any flags.
        if spec.args.is_empty() {
            spec.args.push(prompt);
        } else {
            let last = spec.args.len() - 1;
            spec.args[last] = prompt;
        }

        // Re-sling directly: spawn a fresh polecat for the SAME session/bead. No bead transition —
        // it stays `working`. The deterministic session id + per-bead worktree are reused.
        if let Err(e) = spawn_tmux(self.tmux.as_ref(), &spec) {
            eprintln!(
                "[polecat] context-exhaustion re-sling spawn failed for {bead}: {e} — supervisor tick will retry"
            );
            return;
        }
        self.supervisor.watch(spec);
        eprintln!("[polecat] re-slung {bead} with a continuation prompt ({reason})");
    }

    /// Re-sling a bead whose PR failed CI, handing the next polecat a fix-and-re-push prompt
    /// (gtcore-3a1bd4). This closes the CI loop: instead of leaving the merge slot terminally
    /// `failed` for a human, the bead — its work already committed on the branch and its PR open
    /// with auto-merge armed — is re-dispatched to fix what CI flagged and push to the SAME branch,
    /// re-running CI. `attempt` is this re-sling's 1-based index in the retry budget; the caller has
    /// already gated it under [`Self::ci_max_retries`].
    ///
    /// Like [`resling_on_context_exhaustion`](Self::resling_on_context_exhaustion) it reuses the
    /// bead's deterministic session + per-bead worktree (so the pool slot, claimed at the original
    /// sling and held until merge, is reused — no new claim), swapping only the kickoff prompt. The
    /// bead is NOT transitioned (it stays `working`). Best-effort throughout: a session that is no
    /// longer supervised, an unreadable bead, or an empty diff/CI snapshot degrades the prompt
    /// rather than aborting — the retry counter still advances toward escalation.
    async fn resling_on_ci_failure(&self, bead: &str, reason: &str, attempt: u32) {
        // The session id is deterministic per bead (route by prefix → `spec_for`), the same
        // derivation the `merge.merged.v1` teardown uses — so we find the supervised spec without
        // the failed event carrying it.
        let (template, _root) = self.route(bead);
        let session = template.spec_for(&self.workspace, bead).session;
        let Some(mut spec) = self.supervisor.spec_for_session(&session) else {
            eprintln!(
                "[polecat] CI-failure re-sling skipped for {bead}: session {session} not supervised here"
            );
            return;
        };

        // Acceptance criteria from Dolt orient the fix; absent issues handle / read error degrades
        // to the diff + the agent's own `gt` MCP tools.
        let acceptance_criteria = match &self.issues {
            Some(issues) => match issues.get_detail(bead).await {
                Ok(Some(detail)) => detail.acceptance_criteria,
                Ok(None) => String::new(),
                Err(e) => {
                    eprintln!(
                        "[polecat] CI-failure re-sling: get_detail({bead}) failed: {e} — continuing without AC"
                    );
                    String::new()
                }
            },
            None => String::new(),
        };

        // The work already on the branch + a best-effort snapshot of the failing checks.
        let diff = crate::continuation::read_branch_diff(&spec.workdir, "main");
        let ci_checks = crate::continuation::read_ci_checks(&spec.workdir, bead);
        let prompt = crate::continuation::build_ci_fix_prompt(
            bead,
            reason,
            &ci_checks,
            &diff,
            &acceptance_criteria,
            attempt,
            self.ci_max_retries,
        );

        // The bead prompt is always the last positional arg — swap it for the CI-fix prompt without
        // disturbing any preceding flags (same mechanism as the context-exhaustion re-sling).
        if spec.args.is_empty() {
            spec.args.push(prompt);
        } else {
            let last = spec.args.len() - 1;
            spec.args[last] = prompt;
        }

        // Unlike the context-exhaustion re-sling (driven BY a tmux death, so the session is already
        // gone), a CI failure arrives from the CI-gate webhook independently of session liveness:
        // the original polecat commonly lingers idle after signalling merge-ready instead of
        // exiting, so its tmux session is still alive — heartbeat recent — when `merge.failed.v1`
        // lands. `tmux new-session` then rejects the duplicate name and the supervisor retries
        // forever, since the lingering session never dies on its own (gtcore-8701c4). Tear it down
        // first, guarded by `has_session` so the common clean case (polecat already exited) skips a
        // pointless `kill-session` that the real adapter would error-and-retry on. The re-sling then
        // always lands a fresh session and converges.
        if self.tmux.has_session(&session) {
            if let Err(e) = self.tmux.kill_session(&session) {
                eprintln!(
                    "[polecat] CI-failure re-sling: kill-session({session}) failed for {bead}: {e} — supervisor tick will retry"
                );
                return;
            }
        }

        if let Err(e) = spawn_tmux(self.tmux.as_ref(), &spec) {
            eprintln!(
                "[polecat] CI-failure re-sling spawn failed for {bead}: {e} — supervisor tick will retry"
            );
            return;
        }
        self.supervisor.watch(spec);
        eprintln!(
            "[polecat] re-slung {bead} to fix CI (attempt {attempt}/{}): {reason}",
            self.ci_max_retries
        );
    }
}

/// Does this `AgentEvent::Killed` reason mark a death by context exhaustion (gtcore-91fdde)?
/// The polecat supervisor records such a death as a `Killed` whose reason begins
/// `context exhausted: …` (no dedicated event kind), distinguishing it from a heartbeat-stale
/// kill or an operator kill — only the exhaustion case warrants a continuation re-sling.
fn is_context_exhaustion(reason: &str) -> bool {
    reason
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("context exhaust")
}

#[async_trait]
impl Plugin for PolecatSupervisorPlugin {
    fn name(&self) -> &'static str {
        "polecat-supervisor"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            // A bead was dispatched → admit it against the pool, then sling a supervised polecat.
            "scheduling.dispatched.v1" => {
                let SchedEvent::Dispatched { bead, .. } = record.decode::<SchedEvent>()? else {
                    return Ok(());
                };
                // Slingability gate (gtcore-db99e0): re-validate the bead's CURRENT state before
                // claiming a pool slot or spawning. A `dispatched` event can name a bead that is no
                // longer slingable — most acutely at BOOT, where `replay_orphaned_inflight`
                // re-enqueues every dispatched-but-unmerged bead WITHOUT re-checking it, so a
                // since-closed bead, an epic container, or a dispatch=manual bead would otherwise
                // be re-slung (gtcore-e7a851). When `should_sling` says no, free the scheduler
                // governor slot the dispatch just `acquire`d (so it does not leak — the same
                // capacity teardown the CI-retry-exhaustion abandon does) and return WITHOUT
                // claiming the pool. No issues handle ⇒ the gate is permissive (legacy: every
                // dispatch slings), matching the rest of the sling path's degradation.
                if let Some(issues) = &self.issues {
                    if !bead_should_sling(issues, &bead).await {
                        eprintln!(
                            "[polecat] sling skipped for {bead}: not slingable (closed/epic/manual) — boot re-hydration / stale dispatch"
                        );
                        if let Some(sched) = &self.sched {
                            sched.capacity_freed().await;
                        }
                        return Ok(());
                    }
                }
                // Admission first: a refused claim is backpressure, not an error — the bead stays
                // queued/dispatched in the log; capacity will free up as live polecats finish.
                if self
                    .allocator
                    .lock()
                    .expect("pool mutex")
                    .claim(&self.workspace)
                    .is_err()
                {
                    eprintln!(
                        "[polecat] sling skipped for {bead}: pool/host cap reached (workspace {})",
                        self.workspace
                    );
                    // Surface the backpressure to the operator (gtcore-7e19fe): the bead is in limbo
                    // until a slot frees, which is invisible from the log alone.
                    self.emit_polecat(PolecatEvent::SlingSkipped {
                        bead: bead.clone(),
                        workspace: self.workspace.clone(),
                    });
                    return Ok(());
                }
                // Route to the bead's rig by prefix (hq-0ecfec): matched ⇒ that rig's template +
                // worktree root; unknown prefix ⇒ the legacy boot template, unchanged behaviour.
                let (template, worktree_root) = self.route(&bead);
                let mut spec = template.spec_for(&self.workspace, &bead);
                // Stamp a least-privilege per-agent token (hq-agent-provisioning.3) so the polecat
                // acts as itself, scoped to its role — not as the operator. Best-effort: a mint
                // failure logs and the polecat still slings (it just lacks GT_TOKEN).
                if let Some(tm) = &self.token {
                    let role = spec
                        .env
                        .iter()
                        .find(|(k, _)| k == "GT_ROLE")
                        .map(|(_, v)| v.as_str())
                        .unwrap_or("polecat");
                    match tm.token_for(&spec.session, role) {
                        Ok(tok) => spec.env.push(("GT_TOKEN".to_string(), tok)),
                        Err(e) => eprintln!(
                            "[polecat] token mint failed for {bead} (role {role}): {e} — slinging without GT_TOKEN"
                        ),
                    }
                }
                // Point the polecat's claude at the ACTIVE account's credentials dir
                // (hq-agent-provisioning.7): the keychain's live pointer is what predictive
                // rotation flips, so reading it here is what hands the next sling the rotated
                // account. The stored secret IS the account's CLAUDE_CONFIG_DIR.
                //
                // gtcore-bf4acd: the active account's `.credentials.json` is VALIDATED here (expiry,
                // not just quota status) before stamping it. A credential-dead active account (token
                // expired ~12h with no refresh — quota-`Healthy` but unauthable) is rotated to a
                // credential-valid account; if NO account can authenticate, the sling is BLOCKED and
                // the operator alerted rather than birthing the polecat into `401`. A missing
                // credential file stays permissive (fresh/seeded dirs), and any keychain miss leaves
                // the polecat on the host default ~/.claude.
                let mut active_config_dir: Option<String> = None;
                if let Some(kc) = &self.keychain {
                    // Snapshot quota status per account for the sling-time quota gate (gtcore-2836bb).
                    // A pre-fetched map keeps the guard's `status_of` closure synchronous (the guard
                    // is pure-ish and must not await). No quota handle ⇒ empty map ⇒ the guard treats
                    // every account's status as unknown (permissive), i.e. the legacy credential-only
                    // behaviour.
                    let quota_status: HashMap<String, gt_quota::AccountQuotaStatus> =
                        match &self.quota {
                            Some(q) => q
                                .accounts()
                                .await
                                .into_iter()
                                .map(|a| (a.id, a.status))
                                .collect(),
                            None => HashMap::new(),
                        };
                    match crate::credential_guard::resolve_for_sling(kc, now_ms(), |acc| {
                        quota_status.get(acc).copied()
                    }) {
                        crate::credential_guard::CredOutcome::Resolved {
                            resolved,
                            dead,
                            rotated_from,
                        } => {
                            active_config_dir = Some(resolved.config_dir.clone());
                            spec.env
                                .push(("CLAUDE_CONFIG_DIR".to_string(), resolved.config_dir));
                            // Stamp the account id so the polecat's Stop costs-report hook can label
                            // its quota-feed sample (hq-agent-provisioning.8): the feed message needs
                            // `{account}`, and only the daemon knows which keychain account this sling
                            // resolved to.
                            spec.env.push((
                                gt_polecat::GT_HOOK_ACCOUNT.to_string(),
                                resolved.account.clone(),
                            ));
                            // Rotated off a credential-dead account → alert the operator (bell +
                            // email) so they re-onboard/rotate it. The sling itself proceeds on the
                            // healthy account picked above.
                            if let Some(from) = &rotated_from {
                                eprintln!(
                                    "[polecat] sling for {bead}: active account {from} credential-dead — using {} instead",
                                    resolved.account
                                );
                                for d in &dead {
                                    self.emit_polecat(PolecatEvent::CredentialDead {
                                        account: d.account.clone(),
                                        reason: d.reason().to_string(),
                                    });
                                }
                            }
                        }
                        crate::credential_guard::CredOutcome::NoValidAccount { dead } => {
                            // No account can authenticate: blocking the sling (and freeing the slot)
                            // is strictly better than a polecat born in 401 that latches a false
                            // heartbeat and produces nothing (gtcore-bf4acd).
                            self.allocator
                                .lock()
                                .expect("pool mutex")
                                .release(&self.workspace);
                            for d in &dead {
                                self.emit_polecat(PolecatEvent::CredentialDead {
                                    account: d.account.clone(),
                                    reason: d.reason().to_string(),
                                });
                            }
                            self.emit_polecat(PolecatEvent::SlingAuthBlocked {
                                bead: bead.clone(),
                                workspace: self.workspace.clone(),
                            });
                            eprintln!(
                                "[polecat] sling BLOCKED for {bead}: no keychain account has valid credentials — alerting operator, not slinging into 401"
                            );
                            return Ok(());
                        }
                        // No keychain rotation configured (no active pointer / no cred record) →
                        // host default ~/.claude, unchanged from before the guard.
                        crate::credential_guard::CredOutcome::HostDefault => {}
                    }
                }
                // Route claude through the Anthropic passthrough proxy (hq-284842) so EVERY call
                // feeds per-call quota truth (unified-status headers + API-reported usage).
                // Attribution rides ANTHROPIC_CUSTOM_HEADERS (claude parses one `Name: value` per
                // line; tmux passes env values as exec args, so the embedded newline survives).
                // The account is the keychain account resolved above (GT_HOOK_ACCOUNT); without
                // one the proxy still forwards but records nothing for this polecat.
                if let Some(proxy_url) = &self.anthropic_proxy_url {
                    let account = spec
                        .env
                        .iter()
                        .find(|(k, _)| k == gt_polecat::GT_HOOK_ACCOUNT)
                        .map(|(_, v)| v.clone());
                    spec.env
                        .push(("ANTHROPIC_BASE_URL".to_string(), proxy_url.clone()));
                    if let Some(account) = account {
                        spec.env.push((
                            "ANTHROPIC_CUSTOM_HEADERS".to_string(),
                            format!("x-gt-account: {account}\nx-gt-session: {}", spec.session),
                        ));
                    }
                }
                // Isolate the polecat in its own git worktree (hq-orchd-deploy.9): concurrent
                // polecats must not share the rig checkout's HEAD. Branch = bead (CLAUDE.md
                // `-b <bead-id>`), base = the template's rig checkout. Best-effort — a git failure
                // logs and the polecat falls back to the shared checkout (spec.workdir unchanged),
                // keeping liveness over isolation. Idempotent: a re-sling reuses the same tree.
                if let Some(root) = worktree_root {
                    let wt = root.join(&spec.session);
                    match crate::worktree::provision(&template.workdir, &wt, &bead) {
                        Ok(()) => {
                            // The worktree is a FRESH tree: the boot-time hook install + the
                            // machine-local .mcp.json both live in the base rig checkout, not here
                            // (hq-orchd-deploy.10). Re-provision them INTO the worktree, else the
                            // polecat runs with no heartbeat/merge-ready hooks and no `gt` MCP.
                            // Both best-effort — failures log and the polecat still slings.
                            if let Err(e) = gt_polecat::install_polecat_hooks(&wt) {
                                eprintln!("[polecat] hook install into worktree {} skipped: {e}", wt.display());
                            }
                            // Prefer a dynamic .mcp.json with the per-sling token when the daemon
                            // has a server URL (hq-polecat-rig-config.1): the generated file is
                            // always valid for THIS sling, surviving rig changes and token rotation
                            // without the operator re-placing a static file. Fall back to copying
                            // the operator-placed base-checkout file when no URL is available.
                            let url_and_tok = self.server_url.as_deref().zip(
                                spec.env.iter().find(|(k, _)| k == "GT_TOKEN").map(|(_, v)| v.as_str())
                            );
                            let mcp_written = url_and_tok.map(|(url, tok)| {
                                crate::worktree::write_mcp_json(&wt, url, &self.workspace, &spec.rig, tok)
                            }).unwrap_or(false);
                            if !mcp_written {
                                crate::worktree::seed_mcp_config(&template.workdir, &wt);
                            }
                            // hq-rig-isolation.5: write .gt-config so `gt` CLI knows the rig.
                            // Best-effort alongside write_mcp_json — same server_url + token.
                            if let Some((url, tok)) = url_and_tok {
                                if !crate::worktree::write_gt_config(
                                    &wt, url, &self.workspace, &spec.rig, tok,
                                ) {
                                    eprintln!("[polecat] .gt-config write skipped for {bead}");
                                }
                            }
                            // Hand the tree to the non-root polecat user (hq-quota-accounts.6) so the
                            // dropped-privilege re-exec can read/write it. Best-effort.
                            if let Some(user) = &self.run_as {
                                if let Err(e) = crate::worktree::chown_to(&wt, user) {
                                    eprintln!("[polecat] chown worktree {} to {user} skipped: {e}", wt.display());
                                }
                            }
                            spec.workdir = wt;
                        }
                        Err(e) => eprintln!(
                            "[polecat] worktree provision failed for {bead} at {}: {e} — using shared checkout {}",
                            wt.display(),
                            template.workdir.display()
                        ),
                    }
                }
                // Seed the account's claude config so the INTERACTIVE polecat skips the onboarding
                // TUI (hq-orchd-deploy.14): a fresh CLAUDE_CONFIG_DIR otherwise stops at the theme /
                // trust-folder / bypass-accept prompts and never works the bead. We need interactive
                // (not --print) so the heartbeat + Stop→merge-ready hooks fire. Marks onboarding +
                // bypass-mode accepted globally and trusts THIS worktree path. Best-effort.
                // Seed the EFFECTIVE config dir, not only a keychain-resolved one
                // (hq-polecat-provisioning-20260608.2): the seed used to run only when the keychain
                // handed back an account's CLAUDE_CONFIG_DIR, so a polecat slung WITHOUT a resolved
                // account (no keychain, or the active pointer not rehydrated after a daemon restart)
                // fell back to claude's default `$HOME/.claude` — which never got the onboarding /
                // bypass-accept flags, and the interactive claude stalled on the first-run dialogs
                // again. Resolve the dir claude will actually read (the stamped CLAUDE_CONFIG_DIR, or
                // `$HOME/.claude` when none) and seed THAT. Best-effort.
                let effective_config_dir: Option<std::path::PathBuf> = active_config_dir
                    .as_ref()
                    .map(std::path::PathBuf::from)
                    .or_else(|| {
                        std::env::var_os("HOME").map(|h| std::path::Path::new(&h).join(".claude"))
                    });
                if let Some(cd) = &effective_config_dir {
                    crate::worktree::seed_claude_onboarding(cd, &spec.workdir);
                    crate::worktree::seed_user_hooks(cd);
                }
                // Materialise the polecat role's Knowledge prompt as CLAUDE.md in the worktree
                // (hq-polecat-knowledge.1): mirrors terminal.rs::prepare_role_skills — behavioral
                // instructions live in a configurable Knowledge prompt, the positional kickoff
                // carries only the task context. Role comes from GT_ROLE in the spec env (default
                // "polecat"). Placeholders <workspace>, <bead>, <branch> are rendered. Best-effort.
                if let Some(log) = &self.event_log {
                    let role = spec
                        .env
                        .iter()
                        .find(|(k, _)| k == "GT_ROLE")
                        .map(|(_, v)| v.as_str())
                        .unwrap_or("polecat");
                    match log.replay_domain(
                        Some(&self.workspace),
                        "skills.",
                        SkillState::default(),
                        SkillState::apply,
                    ) {
                        Ok(state) => {
                            // SKILL.md bodies → <worktree>/.claude/skills/<id>/SKILL.md
                            // (hq-polecat-skills.1): mirrors terminal.rs::prepare_role_skills so
                            // the polecat loads the same skill bodies as an interactive role
                            // session. Best-effort per skill — one write failure never skips the
                            // rest.
                            let skills_dir = spec.workdir.join(".claude").join("skills");
                            for id in state.catalog.skills_for_role(role) {
                                let Some(skill) = state.catalog.get(&id) else {
                                    continue;
                                };
                                if skill.body.trim().is_empty() {
                                    continue;
                                }
                                let dir = skills_dir.join(&id);
                                if let Err(e) = std::fs::create_dir_all(&dir)
                                    .and_then(|_| std::fs::write(dir.join("SKILL.md"), &skill.body))
                                {
                                    eprintln!(
                                        "[polecat] skill {id} write for {bead} skipped: {e}"
                                    );
                                }
                            }
                            // Role prompt → CLAUDE.md (hq-polecat-knowledge.1)
                            if let Some(prompt) = state.catalog.role_prompt(role) {
                                let rendered = crate::terminal::render_prompt(
                                    &prompt,
                                    &[
                                        ("workspace", self.workspace.clone()),
                                        ("bead", bead.clone()),
                                        ("branch", bead.clone()),
                                    ],
                                );
                                if let Err(e) =
                                    std::fs::write(spec.workdir.join("CLAUDE.md"), &rendered)
                                {
                                    eprintln!(
                                        "[polecat] CLAUDE.md write for role {role} in {} skipped: {e}",
                                        spec.workdir.display()
                                    );
                                }
                            }
                            // Role model config (hq-b185a4): the same navbar-configured
                            // RoleModelSet the terminal applies to interactive sessions —
                            // stamp --model/--effort onto the polecat launch so Agents →
                            // Model governs autonomous agents too. permission_mode is
                            // deliberately NOT applied here (hq-e90522): see apply_role_model.
                            if let Some(model) = state.catalog.role_model(role) {
                                apply_role_model(&mut spec.args, &model);
                            }
                        }
                        Err(e) => eprintln!(
                            "[polecat] skills replay failed — no skills/CLAUDE.md written for {bead}: {e}"
                        ),
                    }
                }
                // Load the polecat hooks via claude's `--settings <file>` flag (hq-orchd-deploy.16):
                // claude does NOT apply the project/user settings.json hooks on its own in this
                // headless/container setup (verified: heartbeat + Stop→merge-ready never fired), but
                // an explicit `--settings` path is loaded deterministically — this is exactly how the
                // upstream gastown launcher wires polecat hooks. The worktree already carries the
                // gt-managed settings (install_polecat_hooks above). Insert before the trailing
                // positional bead prompt so the prompt stays last.
                let settings_file = spec.workdir.join(".claude").join("settings.json");
                if settings_file.exists() {
                    let prompt = spec.args.pop();
                    spec.args.push("--settings".to_string());
                    spec.args.push(settings_file.display().to_string());
                    if let Some(p) = prompt {
                        spec.args.push(p);
                    }
                }
                if let Err(e) = spawn_tmux(self.tmux.as_ref(), &spec) {
                    // Spawn failed → undo the claim so the slot is not leaked.
                    self.allocator
                        .lock()
                        .expect("pool mutex")
                        .release(&self.workspace);
                    eprintln!("[polecat] sling failed for {bead}: {e}");
                    // Surface the fault to the operator (gtcore-7e19fe): the slot is freed but the
                    // bead made no progress, so it needs a human to look (dead tmux / corrupt tree).
                    self.emit_polecat(PolecatEvent::SlingFailed {
                        bead: bead.clone(),
                        reason: e.to_string(),
                    });
                    return Ok(());
                }
                // The sling landed (claim + spawn both succeeded): record the slot against the
                // session so any later death of this session releases it immediately, not only a
                // merge (gtcore-b05dbc). A re-sling of the SAME session (continuation) keeps the
                // same id, so this is idempotent — it never double-claims.
                self.mark_claimed(&spec.session);
                // Transition the bead open→working in Dolt (gtcore-orchd-working): the polecat is
                // slung, so the bead IS being worked. Without this the bead stays `open` in the
                // tracker until the polecat self-transitions — which may never happen, leaving the
                // auto-dispatch frontier stale and the frontend showing no movement. Best-effort:
                // a Dolt failure logs but does not kill the sling (the polecat still works the bead).
                if let Some(issues) = &self.issues {
                    let bead_id = bead.clone();
                    let issues = issues.clone();
                    tokio::spawn(async move {
                        if let Err(e) = issues.transition(&bead_id, IssueStatus::Working).await {
                            eprintln!(
                                "[polecat] open→working transition for {bead_id} failed: {e} — bead stays open until agent self-transitions"
                            );
                        }
                    });
                }
                // Compute the agent manifest once (hq-orch-sessions.2): skills from the FINAL
                // workdir (the per-bead worktree when provisioned, else the shared checkout) + the
                // static hook kinds. Feeds BOTH the session lifecycle (so /api/v1/agent shows the
                // polecat's hooks/skills) and the issues operator chip. Role is the env's GT_ROLE.
                let role_str = spec
                    .env
                    .iter()
                    .find(|(k, _)| k == "GT_ROLE")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_else(|| "polecat".to_string());
                let skills = skills_from_worktree(&spec.workdir);
                let hooks = hooks_from_settings();
                // Open the agent session (hq-orchd.6): the slung polecat IS the session; its start
                // timestamp anchors the session-minutes projection. Built before `watch` consumes
                // the spec.
                self.emit(AgentEvent::Spawned {
                    session: spec.session.clone(),
                    rig: spec.rig.clone(),
                    role: SessionRole::default(),
                    crew: spec.crew.clone(),
                    skills: skills.clone(),
                    hooks: hooks.clone(),
                    maintains_heartbeat: true,
                    tmux_socket: None,
                    spawned_by: Some("scheduler".into()),
                });
                // Mark the bead's operating agent on the issues channel (hq-agent-observability.2)
                // so the FE shows who works it + what they loaded.
                self.emit_operator(IssueOperatorEvent::Operated {
                    bead: bead.clone(),
                    session: spec.session.clone(),
                    role: role_str,
                    skills,
                    hooks,
                });
                self.supervisor.watch(spec);
                Ok(())
            }
            // The bead's merge landed → its work is done: stop supervising it and free its slot.
            "merge.merged.v1" => {
                let MergeEvent::Merged { bead, .. } = record.decode::<MergeEvent>()? else {
                    return Ok(());
                };
                self.supervisor.unwatch_member(&bead);
                // The session id is the deterministic `spec_for` session, so it matches the
                // `Spawned` emitted at sling and the slot recorded in `claimed`. Routed by bead
                // prefix (hq-0ecfec) so a gtweb bead's session/worktree derive from the SAME
                // template the sling used — else teardown would miss the tree.
                let (template, worktree_root) = self.route(&bead);
                let session = template.spec_for(&self.workspace, &bead).session;
                // Free the slot keyed by session (gtcore-b05dbc): idempotent, so a merge that
                // follows an already-counted death (or vice versa) never double-releases.
                self.release_slot_for(&session);
                // Tear down the per-bead worktree now its work has landed (hq-orchd-deploy.9):
                // best-effort, mirrors the deterministic `<root>/<session>` path used at sling. The
                // branch itself is reaped by the branch-GC reactor on this same event.
                if let Some(root) = worktree_root {
                    let wt = root.join(&session);
                    if let Err(e) = crate::worktree::remove(&template.workdir, &wt) {
                        eprintln!(
                            "[polecat] worktree teardown for {bead} at {} skipped: {e}",
                            wt.display()
                        );
                    }
                }
                self.emit(AgentEvent::SessionEnd { session });
                // Clear the bead's operator marker (hq-agent-observability.2): its work landed, so
                // the FE drops the agent chip. One agent per bead → the id alone identifies it.
                self.emit_operator(IssueOperatorEvent::Cleared { bead: bead.clone() });
                // Forget any CI-retry tally for this bead (gtcore-3a1bd4): it merged, clean slate.
                self.ci_retries
                    .lock()
                    .expect("ci_retries mutex")
                    .remove(&bead);
                Ok(())
            }
            // The bead's PR failed CI → close the loop instead of leaving the slot terminally
            // `failed` for a human (gtcore-3a1bd4). Under the retry cap, re-sling the SAME bead with
            // a fix-and-re-push prompt carrying the CI failure context; at the cap, escalate to the
            // operator and abandon the slot (free capacity, stop supervising) so the loop is finite.
            // A `merge.failed.v1` arrives from the CI-gate webhook (CI red / PR closed unmerged) or
            // the git-merge edge (local rebase conflict); both warrant the same fix-and-re-push.
            "merge.failed.v1" => {
                let MergeEvent::Failed { bead, reason } = record.decode::<MergeEvent>()? else {
                    return Ok(());
                };
                // Count this failure as one attempt against the bead's budget.
                let attempt = {
                    let mut tally = self.ci_retries.lock().expect("ci_retries mutex");
                    let c = tally.entry(bead.clone()).or_insert(0);
                    *c += 1;
                    *c
                };
                if attempt <= self.ci_max_retries {
                    self.resling_on_ci_failure(&bead, &reason, attempt).await;
                } else {
                    // Budget exhausted: escalate and abandon rather than loop forever burning quota.
                    self.ci_retries
                        .lock()
                        .expect("ci_retries mutex")
                        .remove(&bead);
                    self.emit_polecat(PolecatEvent::CiRetriesExhausted {
                        bead: bead.clone(),
                        reason,
                        attempts: self.ci_max_retries,
                    });
                    // Free the slot in both capacity systems (mirroring the merged teardown) and stop
                    // supervising so the dead session is not re-slung by the supervisor tick.
                    self.allocator
                        .lock()
                        .expect("pool mutex")
                        .release(&self.workspace);
                    let (template, _root) = self.route(&bead);
                    let session = template.spec_for(&self.workspace, &bead).session;
                    self.supervisor.unwatch(&session);
                    if let Some(sched) = &self.sched {
                        sched.capacity_freed().await;
                    }
                    eprintln!(
                        "[polecat] {bead}: CI retries exhausted ({}) — escalated, slot abandoned",
                        self.ci_max_retries
                    );
                }
                Ok(())
            }
            // A polecat died (operator kill, heartbeat-stale, reconciler reap, or context
            // exhaustion). The supervisor/reaper records every death as an `AgentEvent::Killed`;
            // the `reason` tells the two apart (gtcore-91fdde).
            //
            // - Context exhaustion (reason begins `context exhausted`) → re-sling the SAME session
            //   with a continuation prompt (gtcore-3b2a68). The slot is REUSED, not freed: the
            //   session id is unchanged and stays in `claimed`, so a later merge/death releases it
            //   exactly once.
            // - Any other death → the work stopped without a merge. Free the slot IMMEDIATELY
            //   (gtcore-b05dbc) instead of waiting out the ~300s patrol lease — that wait is what
            //   wedged all new slings behind a phantom `pool cap reached` with zero live sessions.
            //   `release_slot_for` is idempotent, so a duplicate kill (or a kill that races the
            //   merge) is a harmless no-op.
            "agent.killed.v1" => {
                let AgentEvent::Killed { session, reason } = record.decode::<AgentEvent>()? else {
                    return Ok(());
                };
                if is_context_exhaustion(&reason) {
                    self.resling_on_context_exhaustion(&session, &reason).await;
                } else {
                    self.release_slot_for(&session);
                }
                Ok(())
            }
            // A polecat exited cleanly on its own without a merge (gtcore-b05dbc). The supervisor's
            // direct-child path and the session reconciler both emit `agent.session-end.v1` for a
            // self-exit; the merge path emits its own `SessionEnd` AFTER it has already released the
            // slot, so this handler's idempotent release is a no-op there (the session is no longer
            // in `claimed`). A self-exit that was NOT preceded by a merge frees the slot here.
            "agent.session-end.v1" => {
                let AgentEvent::SessionEnd { session } = record.decode::<AgentEvent>()? else {
                    return Ok(());
                };
                self.release_slot_for(&session);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Stamp a role's navbar-configured [`ModelConfig`] onto a polecat's launch args (`hq-b185a4`):
/// `--model` / `--effort` for each non-empty field, inserted BEFORE the trailing positional bead
/// prompt so the prompt stays last (claude reads it as the kickoff). This is how Agents → Model
/// governs autonomous polecats, not just the interactive sessions `terminal.rs` stamps.
///
/// `permission_mode` is DELIBERATELY ignored (`hq-e90522`): an interactive mode (`acceptEdits` /
/// `plan` / `default`) makes claude stop and ask before a bash command — and nobody answers in an
/// autonomous tmux session, so the polecat hangs until its restarts burn out. Autonomous agents
/// keep the template's `--dangerously-skip-permissions` (or whatever `GT_POLECAT_ARGS` says);
/// the navbar's permission mode keeps governing interactive sessions via `terminal.rs`, where a
/// human can actually answer the prompt.
pub fn apply_role_model(args: &mut Vec<String>, model: &ModelConfig) {
    let prompt = args.pop();
    if !model.model.trim().is_empty() {
        args.push("--model".to_string());
        args.push(model.model.clone());
    }
    if !model.effort.trim().is_empty() {
        args.push("--effort".to_string());
        args.push(model.effort.clone());
    }
    if let Some(p) = prompt {
        args.push(p);
    }
}

/// Build the per-rig routing tables the daemon wires at boot (`hq-d15050`, epic hq-554308) from
/// the workspace's live rig catalog: bead prefix → [`RigConfig`] for the polecat supervisor,
/// and bead prefix → rig checkout path for the git-merge edge.
///
/// Each rig's workdir is its [`RigEntry::resolved_worktree_root`] (an explicit catalog override,
/// else the `<home>/gastown-wt/<ws>/<name>` convention). The per-rig template inherits the boot
/// template's shared fields (command/args/base_env/heartbeat_dir) via [`SpawnTemplate::for_rig`];
/// `polecat_worktree_root` (the global `GT_POLECAT_WORKTREE_ROOT`) carries over so every rig's
/// polecats get per-bead worktrees — session names embed the rig prefix, so one shared root never
/// collides.
///
/// A rig whose resolved workdir does not exist on this host is skipped (logged): provisioning
/// missing rig checkouts is explicitly out of scope for hq-554308, and skipping keeps its beads
/// on the boot-template fallback instead of slinging into a dead directory.
pub fn rig_routing_from_catalog(
    rigs: &[gt_rig::RigEntry],
    base: &SpawnTemplate,
    polecat_worktree_root: Option<&std::path::Path>,
    ws: &str,
    home: &std::path::Path,
) -> (
    HashMap<String, RigConfig>,
    HashMap<String, std::path::PathBuf>,
) {
    let mut configs = HashMap::new();
    let mut paths = HashMap::new();
    for rig in rigs {
        let workdir = rig.resolved_worktree_root(ws, home);
        if !workdir.is_dir() {
            eprintln!(
                "[gt-orch-server] rig '{}' (prefix '{}') skipped — worktree root {} does not exist; its beads fall back to the boot template",
                rig.name,
                rig.prefix,
                workdir.display()
            );
            continue;
        }
        eprintln!(
            "[gt-orch-server] rig '{}' (prefix '{}') → {}",
            rig.name,
            rig.prefix,
            workdir.display()
        );
        let template = SpawnTemplate::for_rig(
            &rig.name,
            &rig.prefix,
            workdir.clone(),
            base.command.clone(),
            base.args.clone(),
            base.base_env.clone(),
            base.heartbeat_dir.clone(),
        );
        configs.insert(
            rig.prefix.clone(),
            RigConfig {
                template,
                worktree_root: polecat_worktree_root.map(Into::into),
            },
        );
        paths.insert(rig.prefix.clone(), workdir);
    }
    (configs, paths)
}

/// Wall-clock epoch milliseconds, for the sling-time credential guard (gtcore-bf4acd). Mirrors the
/// `now_ms` discipline in `usage_probe.rs`; `0` on a pre-epoch clock (the guard then treats every
/// expiry as in the future, i.e. fails open — liveness over a spurious credential block).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Compute the host-wide polecat admission cap from live metrics (`hq-orchd.3`): the lesser of the
/// CPU-core count and `MemAvailable / per-polecat budget`, floored at 1 so the daemon can always
/// make progress. Linux-native (`/proc/meminfo` + std), no external dependency — keeping the
/// dependency-light discipline of the kernel/domain tiers.
///
/// The per-polecat RAM budget is `GT_POLECAT_MEM_MB` (default 1024). When `/proc/meminfo` is
/// unreadable (non-Linux / sandbox), the cap falls back to the core count alone.
pub fn host_cap_from_metrics() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let per_polecat_mb = std::env::var("GT_POLECAT_MEM_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&m| m > 0)
        .unwrap_or(1024);
    let cap_by_mem = match mem_available_mb() {
        Some(avail) => (avail / per_polecat_mb).max(1),
        None => cores,
    };
    cores.min(cap_by_mem).max(1)
}

/// `MemAvailable` from `/proc/meminfo`, in MiB. `None` when the file is absent or malformed.
fn mem_available_mb() -> Option<usize> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            // Format: `MemAvailable:   12345678 kB`.
            let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_auth::{Authenticator, JwtAuthenticator, JwtMinter};
    use gt_eventlog::EventRecord;
    use gt_events::{Envelope, EventKind};
    use gt_polecat::{FakeTmux, RestartConfig};

    const TEST_PRIV: &[u8] = include_bytes!("../tests/fixtures/rs256_priv.pem");
    const TEST_PUB: &[u8] = include_bytes!("../tests/fixtures/rs256_pub.pem");

    fn record<E: EventKind + serde::Serialize>(event: E) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(event)).expect("encode")
    }

    fn plugin(
        alloc: Arc<Mutex<PoolAllocator>>,
    ) -> (PolecatSupervisorPlugin, Arc<PolecatSupervisor>) {
        let tmux: Arc<dyn Tmux> = Arc::new(FakeTmux::new());
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor.clone(), alloc);
        (p, supervisor)
    }

    #[tokio::test]
    async fn dispatch_slings_and_watches_a_polecat_within_capacity() {
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        assert_eq!(sup.watched_count(), 1, "dispatched bead is now supervised");
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            1,
            "a pool slot was claimed"
        );

        // The merge of that bead frees the slot + stops supervision.
        p.on_event(&record(MergeEvent::Merged {
            bead: "gg-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 0, "merged bead is unwatched");
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 0, "slot released");
    }

    #[tokio::test]
    async fn killing_a_working_polecat_frees_its_slot_immediately() {
        // gtcore-b05dbc: a non-exhaustion death (operator kill / heartbeat-stale / reconciler reap,
        // all recorded as AgentEvent::Killed) must release the pool slot AT ONCE — not wait for a
        // merge that will never come, nor the ~300s patrol lease. With the slot freed, a brand-new
        // dispatch slings without hitting the phantom `pool cap reached`.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(1, 1)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1, "slot claimed at sling");
        // The host cap is 1 and it is full — a second dispatch would be refused right now.
        assert!(!alloc.lock().unwrap().can_claim("acme"), "pool is full while the polecat lives");

        // Operator kills the working session (no merge). The slot is freed immediately.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "operator killed".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            0,
            "killed session releases its slot without waiting for merge/lease"
        );

        // A fresh dispatch now proceeds — no `pool cap reached`.
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-2".into(),
            worker: "w2".into(),
        }))
        .await
        .unwrap();
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1, "new sling admitted into the freed slot");
        assert!(sup.spec_for_session("hq-gg-2").is_some(), "the new polecat is supervised");
    }

    #[tokio::test]
    async fn clean_self_exit_without_merge_frees_its_slot() {
        // gtcore-b05dbc: a polecat that exits on its own without a merge emits
        // agent.session-end.v1 (the reconciler / direct-child path). That, too, must free the slot.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(1, 1)));
        let (p, _sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1, "slot claimed at sling");

        p.on_event(&record(AgentEvent::SessionEnd {
            session: "hq-gg-1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            0,
            "a self-exit without merge frees the slot"
        );
    }

    #[tokio::test]
    async fn slot_release_is_idempotent_no_double_count() {
        // gtcore-b05dbc: the slot must be released EXACTLY once per session, however many
        // death/merge events arrive. A kill followed by a (late) merge — or two kills, or a
        // merge then a self-exit — must never drive the count negative or steal another
        // workspace's slot. PoolAllocator saturates at 0 per workspace, so a stray double-release
        // would silently under-count a co-tenant; the `claimed` gate prevents that entirely.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(2, 2)));
        let (p, _sup) = plugin(alloc.clone());

        // Two live polecats in the same workspace.
        for bead in ["gg-1", "gg-2"] {
            p.on_event(&record(SchedEvent::Dispatched {
                bead: bead.into(),
                worker: "w".into(),
            }))
            .await
            .unwrap();
        }
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 2, "both slots claimed");

        // Kill gg-1, then a late merge for gg-1 arrives, then a duplicate kill: only the FIRST
        // event releases; the rest are no-ops.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "heartbeat stale".into(),
        }))
        .await
        .unwrap();
        p.on_event(&record(MergeEvent::Merged {
            bead: "gg-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "operator killed".into(),
        }))
        .await
        .unwrap();

        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            1,
            "gg-1 released exactly once; gg-2's slot is untouched (no double-count)"
        );
    }

    #[tokio::test]
    async fn context_exhaustion_resling_keeps_the_slot_claimed() {
        // gtcore-b05dbc + gtcore-3b2a68: a context-exhaustion death re-slings the SAME session and
        // must REUSE the slot — never release it (the work continues) and never re-claim it (no
        // double-count). The slot is freed only by the eventual merge or a real death.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(1, 1)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1, "slot claimed at sling");

        // Context-exhaustion kill → continuation re-sling on the same session.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "context exhausted: 92% context used".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 1, "still supervised after re-sling");
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            1,
            "the slot is reused by the continuation — neither freed nor doubled"
        );

        // The continuation later merges → now the slot frees, exactly once.
        p.on_event(&record(MergeEvent::Merged {
            bead: "gg-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            0,
            "merge of the continuation frees the (single) slot"
        );
    }

    #[tokio::test]
    async fn death_event_for_an_unclaimed_session_is_a_noop() {
        // gtcore-b05dbc: a death event for a session this plugin never slung (e.g. another
        // workspace's polecat on a shared hub, or a stale id) must NOT touch this workspace's
        // count. The `claimed` gate scopes the release to sessions we actually hold.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(2, 2)));
        let (p, _sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1);

        // A kill for a session we never claimed.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-someone-elses-session".into(),
            reason: "operator killed".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            1,
            "a foreign session's death does not steal our slot"
        );
    }

    #[tokio::test]
    async fn context_exhaustion_reslings_with_a_continuation_prompt_without_transition() {
        // gtcore-3b2a68: a polecat death by context exhaustion (an AgentEvent::Killed whose
        // reason begins `context exhausted`, gtcore-91fdde) re-slings the SAME bead — still
        // `working`, no transition — with a continuation prompt in place of the original kickoff.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, sup) = plugin(alloc.clone());

        // Sling the bead so the supervisor holds its spec (carrying the original kickoff prompt).
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        let original = sup.spec_for_session("hq-gg-1").expect("watched after sling");
        assert!(
            original
                .args
                .last()
                .unwrap()
                .contains("You are a gt polecat in workspace"),
            "the original kickoff prompt is stored"
        );

        // A heartbeat-stale kill is NOT context exhaustion → the prompt is left untouched.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "heartbeat stale".into(),
        }))
        .await
        .unwrap();
        assert!(
            sup.spec_for_session("hq-gg-1")
                .unwrap()
                .args
                .last()
                .unwrap()
                .contains("You are a gt polecat in workspace"),
            "a non-exhaustion kill does not rewrite the prompt"
        );

        // A context-exhaustion kill re-slings with the continuation prompt. The bead is never
        // transitioned to `open` on this path — it stays `working` (no transition call exists in
        // resling_on_context_exhaustion), and the polecat stays supervised under the same session.
        p.on_event(&record(AgentEvent::Killed {
            session: "hq-gg-1".into(),
            reason: "context exhausted: 92% context used".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 1, "still supervised after re-sling");
        let cont = sup.spec_for_session("hq-gg-1").expect("re-watched after re-sling");
        let prompt = cont.args.last().expect("continuation prompt arg");
        assert!(
            prompt.contains("CONTINUING work on bead `gg-1`"),
            "continuation prompt injected: {prompt}"
        );
        // The continuator still learns how to signal completion.
        assert!(prompt.contains("mcp__gt__merge_submit"));
    }

    #[test]
    fn is_context_exhaustion_only_matches_the_exhaustion_reason() {
        assert!(is_context_exhaustion("context exhausted: 90% context used"));
        assert!(is_context_exhaustion("  Context Exhausted: 88% context used"));
        assert!(!is_context_exhaustion("heartbeat stale"));
        assert!(!is_context_exhaustion("operator killed"));
        assert!(!is_context_exhaustion(""));
    }

    #[tokio::test]
    async fn ci_failure_under_cap_reslings_with_a_fix_prompt_without_transition() {
        // gtcore-3a1bd4: a `merge.failed.v1` (PR CI red) re-slings the SAME bead — still `working`,
        // no transition, same held slot — with a fix-and-re-push prompt in place of the kickoff.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, sup) = plugin(alloc.clone());
        let p = p.with_ci_max_retries(2);

        // Sling the bead so the supervisor holds its spec (the original kickoff prompt).
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert!(sup
            .spec_for_session("hq-gg-1")
            .unwrap()
            .args
            .last()
            .unwrap()
            .contains("You are a gt polecat in workspace"));

        // First CI failure → re-sling with the CI-fix prompt.
        p.on_event(&record(MergeEvent::Failed {
            bead: "gg-1".into(),
            reason: "CI failed: failure".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 1, "still supervised after CI re-sling");
        let prompt = sup
            .spec_for_session("hq-gg-1")
            .expect("re-watched after CI re-sling")
            .args
            .last()
            .expect("ci-fix prompt arg")
            .clone();
        assert!(
            prompt.contains("RESUMING work on bead `gg-1`"),
            "ci-fix prompt injected: {prompt}"
        );
        assert!(prompt.contains("CI failed: failure"), "carries the CI reason");
        assert!(prompt.contains("retry 1 of 2"), "carries the retry budget");
        assert!(prompt.contains("mcp__gt__merge_submit"));
    }

    #[tokio::test]
    async fn ci_failures_past_the_cap_escalate_and_abandon_the_slot() {
        // gtcore-3a1bd4: after the retry cap is exhausted the bead is NOT re-slung again — it is
        // escalated (the loop is finite) and its slot freed + unwatched so the supervisor tick does
        // not resurrect it. With cap=1: failure #1 re-slings, failure #2 escalates.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, sup) = plugin(alloc.clone());
        let p = p.with_ci_max_retries(1);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-2".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 1);

        // Failure #1 (attempt 1 ≤ cap 1) → re-sling.
        p.on_event(&record(MergeEvent::Failed {
            bead: "gg-2".into(),
            reason: "CI failed: failure".into(),
        }))
        .await
        .unwrap();
        assert_eq!(sup.watched_count(), 1, "re-slung within budget");

        // Failure #2 (attempt 2 > cap 1) → escalate + abandon: no longer supervised.
        p.on_event(&record(MergeEvent::Failed {
            bead: "gg-2".into(),
            reason: "CI failed: failure".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            sup.watched_count(),
            0,
            "the bead is unwatched once its CI-retry budget is exhausted"
        );
    }

    #[tokio::test]
    async fn ci_failure_reslings_over_a_still_live_session_by_killing_it_first() {
        // gtcore-8701c4: a `merge.failed.v1` arrives from the CI-gate webhook independently of
        // session liveness, and the original polecat commonly lingers idle (tmux session alive,
        // heartbeat recent) after signalling merge-ready rather than exiting. A naive re-sling then
        // hits `tmux new-session: duplicate session` and the supervisor retries forever, since the
        // lingering session never dies on its own. The CI re-sling must tear the live session down
        // first (idempotent) so the respawn lands a fresh session and the loop converges.
        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor.clone(), alloc)
            .with_ci_max_retries(2);

        // Sling the bead → its tmux session is created and (unlike a context-exhaustion death) stays
        // alive: the polecat has not exited when CI fails.
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        assert!(
            fake.has_session("hq-gg-1"),
            "polecat session is live before CI fails"
        );
        assert!(fake.kills().is_empty(), "no teardown on the initial sling");

        // CI fails while that session is still alive.
        p.on_event(&record(MergeEvent::Failed {
            bead: "gg-1".into(),
            reason: "CI failed: failure".into(),
        }))
        .await
        .unwrap();

        // The live session was torn down exactly once before the respawn — so the real adapter's
        // `new-session` never collides with a duplicate and the loop converges.
        assert_eq!(
            fake.kills(),
            vec!["hq-gg-1".to_string()],
            "the still-live session is killed once before the re-sling respawn"
        );
        // The re-sling still landed: a fresh session, supervised, carrying the CI-fix prompt.
        assert!(fake.has_session("hq-gg-1"), "respawned after the teardown");
        assert_eq!(
            supervisor.watched_count(),
            1,
            "still supervised after the CI re-sling"
        );
        let prompt = supervisor
            .spec_for_session("hq-gg-1")
            .expect("re-watched after CI re-sling")
            .args
            .last()
            .expect("ci-fix prompt arg")
            .clone();
        assert!(
            prompt.contains("RESUMING work on bead `gg-1`"),
            "ci-fix prompt injected: {prompt}"
        );
    }

    #[tokio::test]
    async fn dispatch_mints_a_role_scoped_token_into_the_polecat_env() {
        // hq-agent-provisioning.3: a slung polecat carries GT_TOKEN whose scopes are exactly its
        // role's least-privilege set (never the operator's `*`).
        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![("GT_ROLE".to_string(), "polecat".to_string())],
            heartbeat_dir: std::env::temp_dir(),
        };
        let resolver: ScopeResolver = Arc::new(|role| {
            if role == "polecat" {
                vec![
                    "issues.read".to_string(),
                    "issues.write".to_string(),
                    "issues.claim".to_string(),
                    "issues.transition".to_string(),
                ]
            } else {
                vec![]
            }
        });
        let minter = JwtMinter::from_rsa_pem(TEST_PRIV).unwrap();
        let token = AgentTokenMinter::new(minter, resolver, "acme", 3600);
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_agent_token(token);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        // The token rides the polecat's session env.
        let tok = fake
            .show_environment("hq-gg-1", "GT_TOKEN")
            .unwrap()
            .expect("GT_TOKEN injected into the polecat env");
        // Verifiable with the matching public key, and scoped to the role — not `*`.
        let claims = JwtAuthenticator::from_rsa_pem(TEST_PUB)
            .unwrap()
            .authenticate(&tok)
            .unwrap();
        assert_eq!(claims.sub, "hq-gg-1"); // sub = the polecat session
        assert_eq!(claims.workspace, "acme");
        assert_eq!(
            claims.scopes,
            vec![
                "issues.read",
                "issues.write",
                "issues.claim",
                "issues.transition"
            ]
        );
        assert!(
            !claims.scopes.iter().any(|s| s == "*"),
            "never the wildcard"
        );
    }

    #[tokio::test]
    async fn dispatch_injects_the_active_accounts_claude_config_dir() {
        // hq-agent-provisioning.7: a slung polecat's env carries CLAUDE_CONFIG_DIR pointing at the
        // keychain's ACTIVE account — the account predictive rotation has selected.
        use gt_quota::InMemoryKeychain;
        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let keychain = Arc::new(InMemoryKeychain::seeded([
            ("acct-a", "/home/nixos/.claude-acct-a"),
            ("acct-b", "/home/nixos/.claude-acct-b"),
        ]));
        keychain.set_active("acct-b").unwrap(); // rotation already moved the pointer to b
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_keychain(keychain);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        let cfg = fake
            .show_environment("hq-gg-1", "CLAUDE_CONFIG_DIR")
            .unwrap()
            .expect("CLAUDE_CONFIG_DIR injected from the active account");
        assert_eq!(cfg, "/home/nixos/.claude-acct-b");
        // The account id is also pinned so the Stop costs-report hook can label its quota-feed
        // sample with the right account (hq-agent-provisioning.8).
        let acct = fake
            .show_environment("hq-gg-1", gt_polecat::GT_HOOK_ACCOUNT)
            .unwrap()
            .expect("GT_HOOK_ACCOUNT injected from the active account");
        assert_eq!(acct, "acct-b");
    }

    /// gtcore-bf4acd: write a `.credentials.json` into `config_dir`, with a controllable expiry.
    fn write_creds(config_dir: &std::path::Path, expires_at_ms: Option<u64>, refresh: bool) {
        std::fs::create_dir_all(config_dir).unwrap();
        let rt = if refresh { r#","refreshToken":"rt-1""# } else { "" };
        let exp = expires_at_ms
            .map(|e| format!(r#","expiresAt":{e}"#))
            .unwrap_or_default();
        let body = format!(r#"{{"claudeAiOauth":{{"accessToken":"at-1"{rt}{exp}}}}}"#);
        std::fs::write(config_dir.join(".credentials.json"), body).unwrap();
    }

    #[tokio::test]
    async fn dispatch_skips_a_credential_dead_active_account_no_polecat_born_in_401() {
        // gtcore-bf4acd / AC1+T1: the active account's token expired (no refresh) — quota status is
        // irrelevant. The sling must NOT stamp the dead account; it rotates to the credential-valid
        // account, stamps THAT, and flips the live pointer so future slings follow.
        use gt_quota::InMemoryKeychain;
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        );
        let dead_dir = std::env::temp_dir().join(format!("gt-cred-dead-{uniq}"));
        let fresh_dir = std::env::temp_dir().join(format!("gt-cred-fresh-{uniq}"));
        let now = now_ms();
        write_creds(&dead_dir, Some(now.saturating_sub(1)), false); // expired, no refresh
        write_creds(&fresh_dir, Some(now + 3_600_000), true); // valid

        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), 8));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let keychain = Arc::new(InMemoryKeychain::seeded([
            ("dead", dead_dir.to_string_lossy().to_string()),
            ("fresh", fresh_dir.to_string_lossy().to_string()),
        ]));
        keychain.set_active("dead").unwrap();
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_keychain(keychain.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-2".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        // The polecat was slung onto the FRESH account, not the credential-dead one.
        let cfg = fake
            .show_environment("hq-gg-2", "CLAUDE_CONFIG_DIR")
            .unwrap()
            .expect("CLAUDE_CONFIG_DIR injected from the rotated-to healthy account");
        assert_eq!(cfg, fresh_dir.to_string_lossy());
        let acct = fake
            .show_environment("hq-gg-2", gt_polecat::GT_HOOK_ACCOUNT)
            .unwrap()
            .unwrap();
        assert_eq!(acct, "fresh");
        // The live pointer was persisted so the NEXT sling also lands on the healthy account.
        assert_eq!(keychain.active().unwrap().as_deref(), Some("fresh"));

        let _ = std::fs::remove_dir_all(&dead_dir);
        let _ = std::fs::remove_dir_all(&fresh_dir);
    }

    #[tokio::test]
    async fn dispatch_blocks_the_sling_when_no_account_has_valid_credentials() {
        // gtcore-bf4acd / AC2: every account's creds are dead → block the sling and free the slot
        // instead of birthing a polecat into 401.
        use gt_quota::InMemoryKeychain;
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        );
        let dead_dir = std::env::temp_dir().join(format!("gt-cred-alldead-{uniq}"));
        let now = now_ms();
        write_creds(&dead_dir, Some(now.saturating_sub(1)), false); // expired, no refresh

        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), 8));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let keychain = Arc::new(InMemoryKeychain::seeded([(
            "dead",
            dead_dir.to_string_lossy().to_string(),
        )]));
        keychain.set_active("dead").unwrap();
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc.clone())
            .with_keychain(keychain);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-3".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        // No tmux session was spawned, and the admission slot was released (not leaked).
        assert!(!fake.has_session("hq-gg-3"), "no polecat should be slung into 401");
        assert_eq!(
            alloc.lock().unwrap().in_flight("acme"),
            0,
            "the claimed slot must be released when the sling is blocked"
        );

        let _ = std::fs::remove_dir_all(&dead_dir);
    }

    #[tokio::test]
    async fn dispatch_provisions_a_per_bead_worktree_off_the_rig_checkout() {
        // hq-orchd-deploy.9: with a worktree root, a sling lands in its OWN git worktree (branch =
        // bead) off the rig checkout — not the shared one — so concurrent polecats don't race on
        // a single HEAD.
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base = std::env::temp_dir().join(format!("gt-wtbase-{uniq}"));
        let wt_root = std::env::temp_dir().join(format!("gt-wtroot-{uniq}"));
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&wt_root).unwrap();
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(&base)
                .args(args)
                .status()
                .unwrap()
                .success()
        };
        assert!(git(&["init", "-q", "-b", "main"]));
        assert!(git(&["config", "user.email", "t@t"]));
        assert!(git(&["config", "user.name", "t"]));
        std::fs::write(base.join("f"), "x").unwrap();
        assert!(git(&["add", "."]));
        assert!(git(&["commit", "-qm", "init"]));
        // A machine-local .mcp.json in the base (untracked) — provisioning must seed it into the
        // worktree (hq-orchd-deploy.10), since an untracked file does not ride the checkout.
        std::fs::write(base.join(".mcp.json"), r#"{"mcpServers":{"gt":{}}}"#).unwrap();

        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: base.clone(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_worktree_root(wt_root.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        let wt = wt_root.join("hq-gg-1"); // <prefix>-<sanitized bead>
        assert!(wt.exists(), "per-bead worktree created at {}", wt.display());
        let head = std::process::Command::new("git")
            .arg("-C")
            .arg(&wt)
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&head.stdout).trim(),
            "gg-1",
            "the worktree is on the bead's own branch"
        );
        // hq-orchd-deploy.10: the fresh worktree carries the report hooks + the seeded .mcp.json,
        // so the polecat reports back (heartbeat/merge-ready) and reaches the gt MCP.
        assert!(
            wt.join(".claude/settings.json").exists(),
            "polecat hooks installed into the worktree"
        );
        assert!(
            wt.join(".mcp.json").exists(),
            ".mcp.json seeded into the worktree from the base checkout"
        );

        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&base)
            .args(["worktree", "remove", "--force", wt.to_str().unwrap()])
            .status();
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&wt_root);
    }

    #[tokio::test]
    async fn dispatch_routes_bead_to_its_rig_config_by_prefix() {
        // hq-0ecfec (epic hq-554308): a `gtweb-*` bead slings with the gtweb rig's template —
        // session prefix + GT_RIG/GT_RIG_PATH from the catalog entry, not the boot rig's. A bead
        // with an unknown prefix keeps the legacy fallback template.
        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "gtcore".into(),
            prefix: "gtcore".into(),
            workdir: "/rig-wt/gtcore".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![
                ("GT_RIG".to_string(), "gtcore".to_string()),
                ("GT_RIG_PATH".to_string(), "/rig-wt/gtcore".to_string()),
            ],
            heartbeat_dir: std::env::temp_dir(),
        };
        let gtweb = RigConfig {
            template: SpawnTemplate::for_rig(
                "gtweb",
                "gtweb",
                "/rig-wt/gtweb".into(),
                template.command.clone(),
                template.args.clone(),
                template.base_env.clone(),
                template.heartbeat_dir.clone(),
            ),
            worktree_root: None,
        };
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_rig_configs(HashMap::from([("gtweb".to_string(), gtweb)]));

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gtweb-968172".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        // The session is named with the gtweb rig's prefix and carries ITS rig env.
        assert_eq!(
            fake.show_environment("gtweb-gtweb-968172", "GT_RIG").unwrap().as_deref(),
            Some("gtweb")
        );
        assert_eq!(
            fake.show_environment("gtweb-gtweb-968172", "GT_RIG_PATH").unwrap().as_deref(),
            Some("/rig-wt/gtweb")
        );

        // Unknown prefix → the legacy fallback template (boot rig), unchanged.
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "zz-1".into(),
            worker: "w2".into(),
        }))
        .await
        .unwrap();
        assert_eq!(
            fake.show_environment("gtcore-zz-1", "GT_RIG").unwrap().as_deref(),
            Some("gtcore")
        );
        assert_eq!(
            fake.show_environment("gtcore-zz-1", "GT_RIG_PATH").unwrap().as_deref(),
            Some("/rig-wt/gtcore")
        );
    }

    #[tokio::test]
    async fn merged_routed_bead_releases_slot_and_tears_down_its_rig_worktree() {
        // hq-0ecfec: merge.merged.v1 for a routed bead derives the session + worktree from the
        // SAME rig config the sling used — slot released, worktree gone, session-end emitted
        // under the routed session id.
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base = std::env::temp_dir().join(format!("gt-rigweb-{uniq}"));
        let wt_root = std::env::temp_dir().join(format!("gt-rigweb-wt-{uniq}"));
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&wt_root).unwrap();
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(&base)
                .args(args)
                .status()
                .unwrap()
                .success()
        };
        assert!(git(&["init", "-q", "-b", "main"]));
        assert!(git(&["config", "user.email", "t@t"]));
        assert!(git(&["config", "user.name", "t"]));
        std::fs::write(base.join("f"), "x").unwrap();
        assert!(git(&["add", "."]));
        assert!(git(&["commit", "-qm", "init"]));

        let fake = Arc::new(FakeTmux::new());
        let tmux: Arc<dyn Tmux> = fake.clone();
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        // Boot template points at a DIFFERENT (nonexistent) checkout: only the routed gtweb
        // config can have provisioned/torn down the worktree.
        let template = SpawnTemplate {
            rig: "gtcore".into(),
            prefix: "gtcore".into(),
            workdir: "/nonexistent-gtcore".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let gtweb = RigConfig {
            template: SpawnTemplate::for_rig(
                "gtweb",
                "gtweb",
                base.clone(),
                "true",
                vec![],
                vec![],
                std::env::temp_dir(),
            ),
            worktree_root: Some(wt_root.clone()),
        };
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (tx, mut rx) = broadcast::channel::<EventRecord>(16);
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc.clone())
            .with_rig_configs(HashMap::from([("gtweb".to_string(), gtweb)]))
            .with_session_events(tx);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gtweb-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        let wt = wt_root.join("gtweb-gtweb-1");
        assert!(wt.exists(), "routed worktree provisioned at {}", wt.display());
        // Drain the sling-side events (agent.spawned + issues.operated).
        assert_eq!(rx.try_recv().unwrap().kind, "agent.spawned.v1");
        assert_eq!(rx.try_recv().unwrap().kind, "issues.operated.v1");

        p.on_event(&record(MergeEvent::Merged {
            bead: "gtweb-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        assert!(!wt.exists(), "routed worktree torn down after merge");
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 0, "slot released");
        let closed = rx.try_recv().expect("session-end emitted");
        assert_eq!(closed.kind, "agent.session-end.v1");
        let AgentEvent::SessionEnd { session } = closed.decode::<AgentEvent>().unwrap() else {
            panic!("expected SessionEnd");
        };
        assert_eq!(session, "gtweb-gtweb-1", "session id derives from the ROUTED template");

        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&wt_root);
    }

    #[tokio::test]
    async fn dispatch_beyond_capacity_is_backpressure_not_a_sling() {
        // host cap 1, default pool 1: the second dispatch must be refused, not spawned.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(1, 1)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-2".into(),
            worker: "w2".into(),
        }))
        .await
        .unwrap();

        assert_eq!(
            sup.watched_count(),
            1,
            "only the admitted polecat is supervised"
        );
        assert_eq!(alloc.lock().unwrap().total_in_flight(), 1, "host cap held");
    }

    #[test]
    fn host_cap_is_at_least_one() {
        assert!(host_cap_from_metrics() >= 1);
    }

    #[test]
    fn apply_role_model_stamps_model_effort_and_keeps_bypass() {
        // hq-b185a4 + hq-e90522: the navbar RoleModelSet governs the polecat launch — non-empty
        // model/effort become flags, the prompt stays the LAST positional, and the template's
        // --dangerously-skip-permissions ALWAYS survives: an interactive permission mode would
        // hang an autonomous tmux session on the first bash prompt, so it is never applied here.
        let mut args = vec![
            "--dangerously-skip-permissions".to_string(),
            "work the bead".to_string(),
        ];
        apply_role_model(
            &mut args,
            &ModelConfig {
                model: "claude-opus-4-6".into(),
                permission_mode: "acceptEdits".into(), // navbar value — ignored for agents
                effort: "high".into(),
            },
        );
        assert_eq!(
            args,
            vec![
                "--dangerously-skip-permissions",
                "--model",
                "claude-opus-4-6",
                "--effort",
                "high",
                "work the bead",
            ]
        );
        assert!(
            !args.iter().any(|a| a == "--permission-mode"),
            "permission_mode never reaches an autonomous launch"
        );

        // Empty fields are skipped entirely.
        let mut args = vec![
            "--dangerously-skip-permissions".to_string(),
            "prompt".to_string(),
        ];
        apply_role_model(
            &mut args,
            &ModelConfig {
                model: "haiku".into(),
                permission_mode: String::new(),
                effort: String::new(),
            },
        );
        assert_eq!(
            args,
            vec!["--dangerously-skip-permissions", "--model", "haiku", "prompt"]
        );
    }

    #[tokio::test]
    async fn dispatch_applies_role_model_from_skills_log() {
        // hq-b185a4 end-to-end through the sling path: a RoleModelSet for the polecat role in the
        // skills.* log lands as launch flags on the slung spec (observed via the supervisor's
        // re-sling respec hook is overkill — assert via the spawned tmux env? args aren't recorded
        // by FakeTmux, so replay the SAME catalog read the sling does and assert the helper's
        // contract against it).
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        log.append(
            Some("acme"),
            gt_skills::SkillEvent::RoleModelSet {
                role: "polecat".into(),
                model: "claude-opus-4-6".into(),
                permission_mode: "acceptEdits".into(), // set in the navbar — ignored for agents
                effort: "high".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        let state = log
            .replay_domain(Some("acme"), "skills.", SkillState::default(), SkillState::apply)
            .unwrap();
        let model = state
            .catalog
            .role_model("polecat")
            .expect("RoleModelSet resolves for the polecat role");
        let mut args = vec![
            "--dangerously-skip-permissions".to_string(),
            "prompt".to_string(),
        ];
        apply_role_model(&mut args, &model);
        assert_eq!(
            args,
            vec![
                "--dangerously-skip-permissions",
                "--model",
                "claude-opus-4-6",
                "--effort",
                "high",
                "prompt"
            ],
            "model+effort land, the bypass flag survives, permission_mode never applies"
        );
    }

    #[test]
    fn rig_routing_from_catalog_maps_prefixes_and_skips_missing_roots() {
        // hq-d15050: a catalog rig with an existing resolved worktree root becomes a routed
        // RigConfig + merge path keyed by its prefix; one whose root is absent on this host is
        // skipped so its beads keep the boot-template fallback (provisioning is out of scope).
        use gt_rig::RigEntry;
        let dir = tempfile::tempdir().unwrap();
        let gtweb_root = dir.path().join("gtweb");
        std::fs::create_dir_all(&gtweb_root).unwrap();

        let mut gtweb = RigEntry::new("gtweb", "gtweb", "https://x/gtweb.git", "main", 0);
        gtweb.worktree_root = Some(gtweb_root.clone());
        // No override + nonexistent convention default ⇒ skipped.
        let ghost = RigEntry::new("ghost", "gh", "https://x/ghost.git", "main", 0);

        let base = SpawnTemplate {
            rig: "gtcore".into(),
            prefix: "gtcore".into(),
            workdir: "/rig".into(),
            command: "claude".into(),
            args: vec!["--flag".into()],
            base_env: vec![("GT_ROLE".to_string(), "polecat".to_string())],
            heartbeat_dir: std::env::temp_dir(),
        };
        let wt_root = std::path::Path::new("/rig-wt");
        let (configs, paths) = rig_routing_from_catalog(
            &[gtweb, ghost],
            &base,
            Some(wt_root),
            "default",
            dir.path(),
        );

        assert_eq!(configs.len(), 1, "only the rig with an existing root routes");
        let cfg = configs.get("gtweb").expect("keyed by bead prefix");
        assert_eq!(cfg.template.rig, "gtweb");
        assert_eq!(cfg.template.workdir, gtweb_root);
        assert_eq!(cfg.template.command, "claude", "shared fields inherited");
        assert_eq!(cfg.worktree_root.as_deref(), Some(wt_root));
        assert_eq!(paths.get("gtweb"), Some(&gtweb_root));
        assert!(!paths.contains_key("gh"), "missing-root rig not routed");
    }

    #[tokio::test]
    async fn sling_and_merge_emit_agent_session_lifecycle_events() {
        // hq-orchd.6: with a hub wired, a dispatch opens the session (agent.spawned.v1) and the
        // bead's merge closes it (agent.session-end.v1) — the two records the projector pairs.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, _sup) = plugin(alloc);
        let (tx, mut rx) = broadcast::channel::<EventRecord>(16);
        let p = p.with_session_events(tx);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        let opened = rx.try_recv().expect("a session-open record was emitted");
        assert_eq!(opened.kind, "agent.spawned.v1");
        // A sling also stamps the operator marker on the issues channel (hq-agent-observability.2);
        // drain it so the merge assertions below read the session-close, not this.
        assert_eq!(rx.try_recv().unwrap().kind, "issues.operated.v1");

        p.on_event(&record(MergeEvent::Merged {
            bead: "gg-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        let closed = rx.try_recv().expect("a session-close record was emitted");
        assert_eq!(closed.kind, "agent.session-end.v1");
        assert_eq!(rx.try_recv().unwrap().kind, "issues.operator-cleared.v1");
    }

    #[tokio::test]
    async fn sling_and_merge_emit_issue_operator_events_with_manifest() {
        // hq-agent-observability.2: a dispatch marks the bead's operating agent on the issues
        // channel (issues.operated.v1, carrying the skills+hooks manifest), and the bead's merge
        // clears it (issues.operator-cleared.v1) — what the FE renders/clears as the agent chip.
        use crate::operator_event::IssueOperatorEvent;

        // A workdir carrying one project skill so the manifest's skills list is non-empty.
        let work = tempfile::tempdir().unwrap();
        let skill = work.path().join(".claude").join("skills").join("graphify");
        std::fs::create_dir_all(&skill).unwrap();
        std::fs::write(skill.join("SKILL.md"), "x").unwrap();

        let tmux: Arc<dyn Tmux> = Arc::new(FakeTmux::new());
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: work.path().to_path_buf(),
            command: "true".into(),
            args: vec![],
            base_env: vec![("GT_ROLE".to_string(), "polecat".to_string())],
            heartbeat_dir: std::env::temp_dir(),
        };
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (tx, mut rx) = broadcast::channel::<EventRecord>(16);
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor, alloc)
            .with_session_events(tx);

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();
        // agent.spawned.v1 first, carrying the SAME manifest (hq-orch-sessions.2) so /api/v1/agent
        // shows the polecat's skills/hooks on the session, then the operator marker.
        let spawned = rx.try_recv().unwrap();
        assert_eq!(spawned.kind, "agent.spawned.v1");
        let AgentEvent::Spawned {
            skills: sp_skills,
            hooks: sp_hooks,
            ..
        } = spawned.decode::<AgentEvent>().unwrap()
        else {
            panic!("expected Spawned");
        };
        assert_eq!(
            sp_skills,
            vec!["graphify".to_string()],
            "session carries the worktree skill"
        );
        assert!(
            !sp_hooks.is_empty(),
            "session carries the loaded hook kinds"
        );
        let op = rx.try_recv().expect("an operator record was emitted");
        assert_eq!(op.kind, "issues.operated.v1");
        let IssueOperatorEvent::Operated {
            bead,
            session,
            role,
            skills,
            hooks,
        } = op.decode::<IssueOperatorEvent>().unwrap()
        else {
            panic!("expected Operated");
        };
        assert_eq!(bead, "gg-1");
        assert_eq!(session, "hq-gg-1");
        assert_eq!(role, "polecat");
        assert_eq!(
            skills,
            vec!["graphify".to_string()],
            "manifest carries the worktree skill"
        );
        assert!(!hooks.is_empty(), "manifest carries the loaded hook kinds");

        p.on_event(&record(MergeEvent::Merged {
            bead: "gg-1".into(),
            sha: "abc".into(),
        }))
        .await
        .unwrap();
        assert_eq!(rx.try_recv().unwrap().kind, "agent.session-end.v1");
        let cl = rx
            .try_recv()
            .expect("an operator-cleared record was emitted");
        assert_eq!(cl.kind, "issues.operator-cleared.v1");
        let IssueOperatorEvent::Cleared { bead } = cl.decode::<IssueOperatorEvent>().unwrap()
        else {
            panic!("expected Cleared");
        };
        assert_eq!(bead, "gg-1");
    }
}
