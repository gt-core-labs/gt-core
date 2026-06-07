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

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionRole};
use gt_auth::{JwtClaims, JwtMinter};
use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope};
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
use gt_polecat::{
    hooks_from_settings, skills_from_worktree, spawn_tmux, PolecatSupervisor, PoolAllocator,
    SpawnTemplate, Tmux,
};
use gt_quota::Keychain;
use gt_scheduling::SchedEvent;

use crate::operator_event::IssueOperatorEvent;

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
}

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
        }
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
                    return Ok(());
                }
                let mut spec = self.template.spec_for(&self.workspace, &bead);
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
                // account. The stored secret IS the account's CLAUDE_CONFIG_DIR. Best-effort: any
                // miss leaves the polecat on the host default ~/.claude (logged).
                let mut active_config_dir: Option<String> = None;
                if let Some(kc) = &self.keychain {
                    match kc.active() {
                        Ok(Some(account)) => match kc.get(&account) {
                            Ok(Some(cred)) => {
                                active_config_dir = Some(cred.secret.clone());
                                spec.env
                                    .push(("CLAUDE_CONFIG_DIR".to_string(), cred.secret));
                                // Stamp the account id so the polecat's Stop costs-report hook can
                                // label its quota-feed sample (hq-agent-provisioning.8): the feed
                                // message needs `{account}`, and only the daemon knows which
                                // keychain account this sling resolved to.
                                spec.env.push((
                                    gt_polecat::GT_HOOK_ACCOUNT.to_string(),
                                    account.clone(),
                                ));
                            }
                            Ok(None) => eprintln!(
                                "[polecat] active claude account {account} has no stored credential — host default ~/.claude"
                            ),
                            Err(e) => eprintln!(
                                "[polecat] keychain get({account}) failed: {e} — host default ~/.claude"
                            ),
                        },
                        Ok(None) => {} // no active pointer yet → host default, no log noise
                        Err(e) => eprintln!(
                            "[polecat] keychain active() failed: {e} — host default ~/.claude"
                        ),
                    }
                }
                // Isolate the polecat in its own git worktree (hq-orchd-deploy.9): concurrent
                // polecats must not share the rig checkout's HEAD. Branch = bead (CLAUDE.md
                // `-b <bead-id>`), base = the template's rig checkout. Best-effort — a git failure
                // logs and the polecat falls back to the shared checkout (spec.workdir unchanged),
                // keeping liveness over isolation. Idempotent: a re-sling reuses the same tree.
                if let Some(root) = &self.worktree_root {
                    let wt = root.join(&spec.session);
                    match crate::worktree::provision(&self.template.workdir, &wt, &bead) {
                        Ok(()) => {
                            // The worktree is a FRESH tree: the boot-time hook install + the
                            // machine-local .mcp.json both live in the base rig checkout, not here
                            // (hq-orchd-deploy.10). Re-provision them INTO the worktree, else the
                            // polecat runs with no heartbeat/merge-ready hooks and no `gt` MCP.
                            // Both best-effort — failures log and the polecat still slings.
                            if let Err(e) = gt_polecat::install_polecat_hooks(&wt) {
                                eprintln!("[polecat] hook install into worktree {} skipped: {e}", wt.display());
                            }
                            crate::worktree::seed_mcp_config(&self.template.workdir, &wt);
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
                            self.template.workdir.display()
                        ),
                    }
                }
                // Seed the account's claude config so the INTERACTIVE polecat skips the onboarding
                // TUI (hq-orchd-deploy.14): a fresh CLAUDE_CONFIG_DIR otherwise stops at the theme /
                // trust-folder / bypass-accept prompts and never works the bead. We need interactive
                // (not --print) so the heartbeat + Stop→merge-ready hooks fire. Marks onboarding +
                // bypass-mode accepted globally and trusts THIS worktree path. Best-effort.
                if let Some(cd) = &active_config_dir {
                    let cd = std::path::Path::new(cd);
                    crate::worktree::seed_claude_onboarding(cd, &spec.workdir);
                    crate::worktree::seed_user_hooks(cd);
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
                    return Ok(());
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
                self.allocator
                    .lock()
                    .expect("pool mutex")
                    .release(&self.workspace);
                // Close the agent session for that bead (hq-orchd.6): the session id is the
                // deterministic `spec_for` session, so it matches the `Spawned` emitted at sling.
                let session = self.template.spec_for(&self.workspace, &bead).session;
                // Tear down the per-bead worktree now its work has landed (hq-orchd-deploy.9):
                // best-effort, mirrors the deterministic `<root>/<session>` path used at sling. The
                // branch itself is reaped by the branch-GC reactor on this same event.
                if let Some(root) = &self.worktree_root {
                    let wt = root.join(&session);
                    if let Err(e) = crate::worktree::remove(&self.template.workdir, &wt) {
                        eprintln!(
                            "[polecat] worktree teardown for {bead} at {} skipped: {e}",
                            wt.display()
                        );
                    }
                }
                self.emit(AgentEvent::SessionEnd { session });
                // Clear the bead's operator marker (hq-agent-observability.2): its work landed, so
                // the FE drops the agent chip. One agent per bead → the id alone identifies it.
                self.emit_operator(IssueOperatorEvent::Cleared { bead });
                Ok(())
            }
            _ => Ok(()),
        }
    }
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
