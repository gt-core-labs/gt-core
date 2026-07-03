//! Resident role sessions (gtcore-3246e8, epic gtcore-4c40b5): sheriff / witness / deacon /
//! refinery as LONG-LIVED tmux sessions on the mayor pattern, instead of single-shot spawns.
//!
//! ## Why residents
//!
//! The single-shot model ([`crate::role_agent`]) materializes a role only while its trigger is
//! being handled. Operationally that made the roles invisible (session monitoring shows only
//! mayor + polecats), accumulated `spawned` registrations whose exit watch died with the daemon
//! (the gtcore-efb7e6 zombies), and left the operator staring at an empty tmux when asking
//! "is the sheriff alive?". The operator directive (2026-07-02) is the mayor model for every
//! infra role: one stable session per role, spawned at boot, **idle-blocked on a wake file**
//! (idle ≈ 0 tokens — the same economy single-shot bought), woken by its triggers, re-raised by
//! the orchd when it dies.
//!
//! [`ResidentRoleHost`] is the generalization of [`crate::mayor_dispatch::TmuxMayorWaker`]: the
//! same credential resolution (gtcore-559c50 — never a 401-born session), the same onboarding
//! seed, role-scoped token minting (gtcore-3f4d94), role skills/CLAUDE.md materialisation
//! (gtcore-ec24d2), and the same waker-owned session-lifecycle announcements (gtcore-a44568).
//! What differs from the mayor: sessions are per ROLE (`<role>-resident`), the wake channel is
//! `<channel_root>/role-wake/<role>.event`, and the kickoff prompt is a standing loop (handle
//! the wake, then block again — never exit, never poll).
//!
//! Trigger delivery (merge/issue/health events writing these wake files instead of single-shot
//! spawning) is the follow-up leg, gtcore-865fb8. This module only boots, supervises and wakes
//! residents; with `GT_ROLE_SESSIONS` unset nothing here is constructed and the single-shot
//! path is byte-for-byte unchanged.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use gt_agent::{AgentEvent, DogKind, SessionRole};
use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope};
use gt_merge::MergeEvent;
use gt_plugin::Plugin;
use gt_polecat::tmux::Tmux;
use gt_quota::actor::QuotaHandle;
use gt_quota::{AccountQuotaStatus, Keychain};
use gt_skills::SkillState;

use crate::credential_guard::{resolve_for_sling, CredOutcome, ResolvedCredentials};
use crate::mcp::EventLog;
use crate::role_agent::{dog_role_str, RoleSpawnPayload, RoleTrigger};

/// The infra roles a resident host keeps alive. The mayor is deliberately NOT here — it stays
/// orchestrator-owned through its own waker; `overseer`/`dog` stay on-demand only.
pub const RESIDENT_ROLES: &[DogKind] =
    &[DogKind::Sheriff, DogKind::Witness, DogKind::Deacon, DogKind::Refinery];

/// A resident role's stable tmux session name. One session per role per workspace server —
/// single-flight is structural, not bookkept.
pub fn resident_session(role: &str) -> String {
    format!("{role}-resident")
}

/// The wake file a resident blocks on: `<channel_root>/role-wake/<role>.event`. The mirror of
/// [`crate::mayor_dispatch::mayor_wake_file`] for the role plane.
pub fn role_wake_file(channel_root: &Path, role: &str) -> PathBuf {
    channel_root.join("role-wake").join(format!("{role}.event"))
}

/// Atomically write a wake payload (tmp sibling + rename, dir created on demand) so a resident
/// never reads a half-written trigger mid-signal. Same discipline as the mayor's wake file.
fn write_wake_file(path: &Path, payload: &str) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "wake file has no name"))?;
    let tmp = match path.parent() {
        Some(dir) => dir.join(format!(".{file_name}.tmp")),
        None => PathBuf::from(format!(".{file_name}.tmp")),
    };
    fs::write(&tmp, payload)?;
    fs::rename(&tmp, path)
}

/// The standing directive seeded as a resident's first prompt. Unlike the single-shot
/// [`crate::role_agent::kickoff_for`] (one task, then stop) this is a LOOP contract: block on
/// the wake file, handle one wake, block again — never exit, never busy-poll. The role's
/// mission/tools live in its Knowledge `CLAUDE.md` (materialised at spawn); this prompt only
/// pins the transport.
pub fn resident_prompt(workspace: &str, role: &str, wake_file: &Path) -> String {
    format!(
        "You are the RESIDENT gt **{role}** for workspace `{workspace}` — a long-lived session \
         the orchd keeps alive, woken by events instead of being re-spawned per trigger. Your \
         mission, judgment criteria and tools are in your CLAUDE.md (role Knowledge). \
         TRANSPORT CONTRACT: your wake channel is the file `{wake}` (also in \
         $GT_ROLE_WAKE_FILE). Each wake is a JSON object describing one trigger (e.g. \
         {{\"trigger\":\"merge.failed\",\"bead\":\"…\",\"reason\":\"…\"}}). Loop forever: \
         BLOCK until the wake file's content changes (e.g. `inotifywait -e moved_to,close_write \
         \"$(dirname \"$GT_ROLE_WAKE_FILE\")\"` or a sleep-based mtime check with a generous \
         interval), read it, act on that ONE trigger with your `mcp__gt__*` tools, report \
         anything durable via `mcp__gt__notify_send` / `mcp__gt__memory_save`, then return to \
         blocking. Recall durable team memory with `mcp__gt__memory_recall` before your first \
         action and treat every `feedback` memory as a hard rule. Do NOT exit when idle. Do NOT \
         busy-poll the tracker. Do NOT spawn other agents. Between wakes you burn no tokens — \
         blocking on the file IS your idle state. AFTER finishing a trigger, RE-READ the wake \
         file before blocking again: a wake that landed while you worked replaced its content \
         (rapid triggers coalesce, latest wins), so handle the changed content first — and \
         always re-derive current state with your tools rather than trusting a payload\'s \
         snapshot. A wake payload of {{\"trigger\":\"boot\"}} \
         means: survey your domain once (fresh daemon boot), settle anything actionable, then \
         block."
    , wake = wake_file.display())
}

/// Production host: keeps one supervised resident tmux session alive per role and delivers
/// triggers by writing the role's wake file. The role-plane sibling of `TmuxMayorWaker`.
pub struct ResidentRoleHost {
    tmux: Arc<dyn Tmux>,
    workspace: String,
    rig: String,
    workdir: PathBuf,
    command: String,
    args: Vec<String>,
    base_env: Vec<(String, String)>,
    channel_root: PathBuf,
    /// gtcore-559c50 credential guard inputs — see `TmuxMayorWaker` for the rationale; a
    /// resident must never be born into 401 either.
    keychain: Option<Arc<dyn Keychain>>,
    quota: Option<QuotaHandle>,
    anthropic_proxy_url: Option<String>,
    /// `skills.*` catalog for role CLAUDE.md/model materialisation (gtcore-ec24d2).
    event_log: Option<Arc<EventLog>>,
    /// Role-scoped MCP token minting (gtcore-3f4d94) + the server URL its `.mcp.json` points at.
    agent_token: Option<crate::polecat::AgentTokenMinter>,
    server_url: Option<String>,
    /// Session-lifecycle announcements (gtcore-a44568): residents are visible in agent.list,
    /// heartbeated on every supervision pass, supersede-killed before a re-spawn.
    session_events: Option<broadcast::Sender<EventRecord>>,
    /// Sessions this host spawned, for the supersede-kill on re-raise. In-memory by design —
    /// after an orchd restart the fresh `Spawned` refreshes the same session id.
    spawned: Mutex<HashSet<String>>,
}

impl ResidentRoleHost {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tmux: Arc<dyn Tmux>,
        workspace: impl Into<String>,
        rig: impl Into<String>,
        workdir: PathBuf,
        command: impl Into<String>,
        args: Vec<String>,
        base_env: Vec<(String, String)>,
        channel_root: PathBuf,
    ) -> Self {
        Self {
            tmux,
            workspace: workspace.into(),
            rig: rig.into(),
            workdir,
            command: command.into(),
            args,
            base_env,
            channel_root,
            keychain: None,
            quota: None,
            anthropic_proxy_url: None,
            event_log: None,
            agent_token: None,
            server_url: None,
            session_events: None,
            spawned: Mutex::new(HashSet::new()),
        }
    }

    pub fn with_keychain(mut self, keychain: Arc<dyn Keychain>) -> Self {
        self.keychain = Some(keychain);
        self
    }

    pub fn with_quota(mut self, quota: QuotaHandle) -> Self {
        self.quota = Some(quota);
        self
    }

    pub fn with_anthropic_proxy(mut self, url: impl Into<String>) -> Self {
        self.anthropic_proxy_url = Some(url.into());
        self
    }

    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.event_log = Some(log);
        self
    }

    pub fn with_agent_token(mut self, minter: crate::polecat::AgentTokenMinter) -> Self {
        self.agent_token = Some(minter);
        self
    }

    pub fn with_server_url(mut self, url: impl Into<String>) -> Self {
        self.server_url = Some(url.into());
        self
    }

    pub fn with_session_events(mut self, events: broadcast::Sender<EventRecord>) -> Self {
        self.session_events = Some(events);
        self
    }

    fn emit_session(&self, ev: AgentEvent) {
        if let Some(tx) = &self.session_events {
            if let Ok(rec) = EventRecord::from_envelope(&Envelope::root(ev)) {
                let _ = tx.send(rec);
            }
        }
    }

    /// Same account resolution as the mayor spawn (gtcore-559c50): quota snapshot + shared
    /// credential guard. `Err` ⇒ do not spawn this role (retried next supervision pass).
    async fn resolve_credentials(&self) -> Result<Option<ResolvedCredentials>, String> {
        let Some(kc) = &self.keychain else {
            return Ok(None);
        };
        let quota_status: HashMap<String, AccountQuotaStatus> = match &self.quota {
            Some(q) => q.accounts().await.into_iter().map(|a| (a.id, a.status)).collect(),
            None => HashMap::new(),
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match resolve_for_sling(kc, now_ms, |acc| quota_status.get(acc).copied(), |_| 100.0) {
            CredOutcome::Resolved { resolved, rotated_from, .. } => {
                if let Some(from) = &rotated_from {
                    eprintln!(
                        "[role-resident] active account {from} credential-dead — resident using {} instead",
                        resolved.account
                    );
                }
                Ok(Some(resolved))
            }
            CredOutcome::NoValidAccount { .. } => Err(
                "no keychain account has valid credentials — not spawning resident into 401".into(),
            ),
            CredOutcome::HostDefault => Ok(None),
        }
    }

    /// Session env for a resident: shared `base_env` with role/rig/channel/wake pointers layered
    /// on and the credential/proxy keys re-resolved per spawn (never shadowed by a stale
    /// boot-template value — same strip discipline as the mayor, gtcore-559c50).
    fn session_env(
        &self,
        role: &str,
        wake_file: &Path,
        session: &str,
        creds: Option<&ResolvedCredentials>,
    ) -> Vec<(String, String)> {
        const STRIPPED: [&str; 6] = [
            "GT_ROLE",
            "GT_RIG",
            "CLAUDE_CONFIG_DIR",
            "GT_HOOK_ACCOUNT",
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_CUSTOM_HEADERS",
        ];
        let mut env: Vec<(String, String)> = self
            .base_env
            .iter()
            .filter(|(k, _)| !STRIPPED.contains(&k.as_str()))
            .cloned()
            .collect();
        env.push(("GT_ROLE".to_string(), role.to_string()));
        env.push(("GT_RIG".to_string(), self.rig.clone()));
        env.push(("GT_CHANNEL_ROOT".to_string(), self.channel_root.display().to_string()));
        env.push(("GT_ROLE_WAKE_FILE".to_string(), wake_file.display().to_string()));
        if let Some(c) = creds {
            env.push(("CLAUDE_CONFIG_DIR".to_string(), c.config_dir.clone()));
            env.push((gt_polecat::GT_HOOK_ACCOUNT.to_string(), c.account.clone()));
            if let Some(proxy) = &self.anthropic_proxy_url {
                env.push(("ANTHROPIC_BASE_URL".to_string(), proxy.clone()));
                env.push((
                    "ANTHROPIC_CUSTOM_HEADERS".to_string(),
                    format!("x-gt-account: {}\nx-gt-session: {session}", c.account),
                ));
            }
        }
        env
    }

    /// Ensure `role`'s resident session is alive. Alive ⇒ refresh its registry heartbeat and
    /// return `Ok(false)`. Dead/absent ⇒ supersede-kill the stale registry row, resolve
    /// credentials (abort on NoValidAccount), seed onboarding, mint the role token, materialise
    /// role skills, spawn the tmux session with the standing prompt, announce it — `Ok(true)`.
    pub async fn ensure(&self, kind: DogKind) -> Result<bool, String> {
        let role = dog_role_str(kind);
        let session = resident_session(role);
        let wake_file = role_wake_file(&self.channel_root, role);
        let now_secs = || {
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
        };

        if self.tmux.has_session(&session) {
            // Observability heartbeat (gtcore-a44568/efb7e6): the registry row shows fresh
            // last_heartbeat_at while the session lives. Also folds spawned→working.
            self.emit_session(AgentEvent::Heartbeat {
                session: session.clone(),
                timestamp_secs: Some(now_secs()),
            });
            return Ok(false);
        }

        if self.spawned.lock().expect("spawned mutex").contains(&session) {
            // The previous incarnation died: close its row before the fresh spawn so agent.list
            // never shows a live resident that is not there.
            self.emit_session(AgentEvent::Killed {
                session: session.clone(),
                reason: "resident tmux session gone — superseded by a fresh spawn".to_string(),
                at_secs: Some(now_secs()),
            });
        }

        let creds = self.resolve_credentials().await.map_err(|e| {
            format!("resident {role}: {e}")
        })?;

        // Per-session workdir (gtcore-aa639a): every resident materialises its identity files
        // (.mcp.json/.gt-config/CLAUDE.md/skills) into its OWN dir — sharing the rig checkout let
        // each launch clobber the previous role's .mcp.json (the mayor authenticated as
        // refinery-resident). Rooted next to the channel dir (the daemon's durable scratch).
        let session_wd = crate::role_session::session_workdir(
            &self.channel_root,
            &session,
            &self.workdir,
        );

        // Seed first-run onboarding/trust in the dir claude will actually read, exactly like the
        // mayor/polecat paths — a resident wedged on the trust dialog would be deaf forever.
        let effective_config_dir: Option<PathBuf> = creds
            .as_ref()
            .map(|c| PathBuf::from(&c.config_dir))
            .or_else(|| {
                self.base_env
                    .iter()
                    .find(|(k, _)| k == "CLAUDE_CONFIG_DIR")
                    .map(|(_, v)| PathBuf::from(v))
            })
            .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".claude")));
        if let Some(cd) = &effective_config_dir {
            crate::worktree::seed_claude_onboarding(cd, &session_wd);
            crate::worktree::seed_user_hooks(cd);
        }

        // Make sure the wake file exists before the session boots, so the resident's very first
        // block has something to watch (a boot survey trigger).
        if !wake_file.exists() {
            let _ = write_wake_file(&wake_file, "{\"trigger\":\"boot\"}");
        }

        let mut env = self.session_env(role, &wake_file, &session, creds.as_ref());
        if let (Some(at), Some(url)) = (&self.agent_token, &self.server_url) {
            match at.token_for(&session, role) {
                Ok(tok) => {
                    crate::worktree::write_mcp_json(&session_wd, url, &self.workspace, &self.rig, &tok);
                    crate::worktree::write_gt_config(&session_wd, url, &self.workspace, &self.rig, &tok);
                    env.push(("GT_TOKEN".to_string(), tok));
                }
                Err(e) => {
                    eprintln!("[role-resident] role-scoped token mint for {role} skipped: {e}")
                }
            }
        }

        let mut args = self.args.clone();
        args.push(resident_prompt(&self.workspace, role, &wake_file));
        if let Some(log) = &self.event_log {
            match log.replay_domain(
                Some(&self.workspace),
                "skills.",
                SkillState::default(),
                SkillState::apply,
            ) {
                Ok(state) => {
                    if let Some(model) = crate::role_session::materialize_role_session(
                        &state.catalog,
                        role,
                        &session_wd,
                        &[
                            ("workspace", self.workspace.clone()),
                            ("rig", self.rig.clone()),
                        ],
                    ) {
                        crate::polecat::apply_role_model(&mut args, &model);
                    }
                }
                Err(e) => eprintln!(
                    "[role-resident] skills replay failed — no role skills/CLAUDE.md for {role}: {e}"
                ),
            }
        }

        self.tmux
            .new_session(&session, &session_wd, &self.command, &args, &env)
            .map_err(|e| format!("spawn resident session {session}: {e}"))?;
        self.spawned.lock().expect("spawned mutex").insert(session.clone());
        self.emit_session(AgentEvent::Spawned {
            session: session.clone(),
            rig: self.rig.clone(),
            role: SessionRole::Dog(kind),
            crew: None,
            spawned_by: Some("orchd-role-resident".to_string()),
            skills: Vec::new(),
            hooks: Vec::new(),
            // The host owns the record's lifecycle (heartbeat per supervision pass,
            // supersede-kill on re-spawn) — same posture as the mayor waker. With
            // tmux_socket: None + Dog role, the orchd session reconciler (gtcore-efb7e6)
            // also reaps the row if BOTH the tmux and this host are gone.
            maintains_heartbeat: false,
            tmux_socket: None,
        });
        self.emit_session(AgentEvent::Heartbeat {
            session,
            timestamp_secs: Some(now_secs()),
        });
        Ok(true)
    }

    /// Deliver a trigger to `role`'s resident: write the wake payload atomically, then make sure
    /// the session is alive to read it (re-raising it if it died). The single entry point the
    /// trigger rewiring (gtcore-865fb8) calls.
    pub async fn wake(&self, kind: DogKind, payload: &str) -> Result<(), String> {
        let role = dog_role_str(kind);
        let wake_file = role_wake_file(&self.channel_root, role);
        write_wake_file(&wake_file, payload)
            .map_err(|e| format!("write wake file {}: {e}", wake_file.display()))?;
        self.ensure(kind).await.map(|_| ())
    }

    /// Boot + supervise: ensure every resident once now, then keep re-ensuring on `tick` so a
    /// dead resident is re-raised within one pass (the AC's "re-raised by the orchd on death").
    pub fn spawn_supervisor(self: Arc<Self>, tick: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick);
            loop {
                interval.tick().await;
                for &kind in RESIDENT_ROLES {
                    match self.ensure(kind).await {
                        Ok(true) => eprintln!(
                            "[role-resident] {} session raised",
                            resident_session(dog_role_str(kind))
                        ),
                        Ok(false) => {}
                        Err(e) => eprintln!("[role-resident] ensure failed: {e}"),
                    }
                }
            }
        })
    }
}

/// Map a role trigger onto its resident + wake payload (gtcore-865fb8): the SAME
/// trigger→role ownership as the single-shot [`crate::role_agent::role_for`], with the trigger
/// context serialized as the JSON object the resident reads off its wake file.
pub fn trigger_wake(trigger: &RoleTrigger) -> (DogKind, String) {
    match trigger {
        RoleTrigger::MergeFailed { bead, reason } => (
            DogKind::Sheriff,
            serde_json::json!({"trigger":"merge.failed","bead":bead,"reason":reason}).to_string(),
        ),
        RoleTrigger::MergeReady { bead, branch } => (
            DogKind::Sheriff,
            serde_json::json!({"trigger":"merge.ready","bead":bead,"branch":branch}).to_string(),
        ),
        RoleTrigger::BeadClosed { bead } => (
            DogKind::Witness,
            serde_json::json!({"trigger":"issues.closed","bead":bead}).to_string(),
        ),
        RoleTrigger::HealthTick => {
            (DogKind::Deacon, serde_json::json!({"trigger":"health-tick"}).to_string())
        }
        RoleTrigger::OnDemand { role, reason } => (
            *role,
            serde_json::json!({"trigger":"on-demand","reason":reason}).to_string(),
        ),
    }
}

/// Hub observer that delivers role triggers as resident WAKES (gtcore-865fb8) — the resident
/// sibling of [`crate::role_agent::RoleAgentPlugin`]. Same events, same role ownership; instead
/// of a single-shot sling each trigger becomes a wake-file write + ensure. No session-end
/// handling: residents have no single-flight slot to free — one stable session per role is
/// structural.
pub struct ResidentTriggerPlugin {
    host: Arc<ResidentRoleHost>,
}

impl ResidentTriggerPlugin {
    pub fn new(host: Arc<ResidentRoleHost>) -> Self {
        Self { host }
    }

    async fn deliver(&self, trigger: &RoleTrigger) {
        let (kind, payload) = trigger_wake(trigger);
        match self.host.wake(kind, &payload).await {
            Ok(()) => eprintln!("[role-resident] {} woken — {payload}", dog_role_str(kind)),
            Err(e) => eprintln!("[role-resident] wake {} failed: {e}", dog_role_str(kind)),
        }
    }
}

#[async_trait]
impl Plugin for ResidentTriggerPlugin {
    fn name(&self) -> &'static str {
        "role-resident"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "merge.failed.v1" => {
                if let MergeEvent::Failed { bead, reason } = record.decode::<MergeEvent>()? {
                    self.deliver(&RoleTrigger::MergeFailed { bead, reason }).await;
                }
                Ok(())
            }
            "merge.ready.v1" => {
                if let MergeEvent::Ready { bead, branch, .. } = record.decode::<MergeEvent>()? {
                    self.deliver(&RoleTrigger::MergeReady { bead, branch }).await;
                }
                Ok(())
            }
            "issues.closed.v1" => {
                if let Some(id) = record.payload.get("id").and_then(|v| v.as_str()) {
                    self.deliver(&RoleTrigger::BeadClosed { bead: id.to_string() }).await;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// On-demand role-spawn consumer in resident mode (gtcore-865fb8): the same channel + payload
/// as [`crate::role_agent::run_on_demand`] (agent.spawn with an infra role, gtcore-b69087), but
/// each accepted request becomes a WAKE of the resident instead of a single-shot sling. The
/// mayor/polecat rejections live in [`RoleSpawnPayload::into_trigger`], unchanged — and failure
/// stays noisy: an undeliverable wake logs the reason, never a phantom registration.
pub async fn run_on_demand_resident<C: gt_channel::DispatchConsumer>(
    consumer: C,
    host: Arc<ResidentRoleHost>,
) -> Result<(), gt_channel::ChannelError> {
    let mut rx = consumer.subscribe(16)?;
    while let Some(msg) = rx.recv().await {
        match serde_json::from_slice::<RoleSpawnPayload>(&msg.payload) {
            Ok(payload) => match payload.into_trigger() {
                Ok(trigger) => {
                    let (kind, wake) = trigger_wake(&trigger);
                    match host.wake(kind, &wake).await {
                        Ok(()) => eprintln!(
                            "[role-spawn] on-demand {} woken (resident mode)",
                            dog_role_str(kind)
                        ),
                        Err(e) => eprintln!(
                            "[role-spawn] on-demand {} wake FAILED: {e}",
                            dog_role_str(kind)
                        ),
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
    use gt_polecat::tmux::FakeTmux;

    fn host(tmux: Arc<FakeTmux>, root: &Path) -> ResidentRoleHost {
        ResidentRoleHost::new(
            tmux,
            "acme",
            "gtcore",
            std::env::temp_dir(),
            "claude",
            vec!["--dangerously-skip-permissions".to_string()],
            vec![("GT_WORKSPACE".to_string(), "acme".to_string())],
            root.to_path_buf(),
        )
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "role-resident-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn session_and_wake_paths_are_per_role() {
        assert_eq!(resident_session("sheriff"), "sheriff-resident");
        assert_eq!(
            role_wake_file(Path::new("/ch"), "deacon"),
            PathBuf::from("/ch/role-wake/deacon.event")
        );
    }

    #[test]
    fn prompt_pins_the_transport_and_the_loop_contract() {
        let p = resident_prompt("acme", "sheriff", Path::new("/ch/role-wake/sheriff.event"));
        assert!(p.contains("RESIDENT"), "names the residency");
        assert!(p.contains("/ch/role-wake/sheriff.event"), "names the wake file");
        assert!(p.contains("Do NOT exit when idle"), "loop contract");
        assert!(p.contains("Do NOT busy-poll"), "idle economy");
        assert!(p.contains("boot"), "explains the boot survey wake");
    }

    #[tokio::test]
    async fn ensure_spawns_once_and_heartbeats_while_alive() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let root = tmp_root("ensure");
        let h = host(tmux.clone(), &root);

        assert!(h.ensure(DogKind::Sheriff).await.expect("first ensure"), "spawns");
        assert!(tmux.has_session("sheriff-resident"));
        // The wake file was pre-created with the boot survey trigger.
        let boot = std::fs::read_to_string(role_wake_file(&root, "sheriff")).unwrap();
        assert!(boot.contains("boot"));
        // Env pins the transport + role.
        assert_eq!(
            tmux.show_environment("sheriff-resident", "GT_ROLE").unwrap().as_deref(),
            Some("sheriff")
        );
        let wake_env = tmux
            .show_environment("sheriff-resident", "GT_ROLE_WAKE_FILE")
            .unwrap()
            .expect("wake file env");
        assert!(wake_env.ends_with("role-wake/sheriff.event"));

        // Alive → no re-spawn, just the heartbeat.
        assert!(!h.ensure(DogKind::Sheriff).await.expect("second ensure"), "no dup spawn");
    }

    #[tokio::test]
    async fn dead_resident_is_reraised_by_ensure() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let root = tmp_root("reraise");
        let h = host(tmux.clone(), &root);
        assert!(h.ensure(DogKind::Refinery).await.unwrap());
        tmux.kill_session("refinery-resident").unwrap();
        assert!(h.ensure(DogKind::Refinery).await.unwrap(), "re-raised after death");
        assert!(tmux.has_session("refinery-resident"));
    }

    #[test]
    fn trigger_wake_keeps_the_single_shot_role_ownership() {
        // gtcore-865fb8: same trigger→role mapping as role_agent::role_for, context serialized.
        let (k, p) = trigger_wake(&RoleTrigger::MergeFailed {
            bead: "gtcore-1".into(),
            reason: "ci red".into(),
        });
        assert_eq!(k, DogKind::Sheriff);
        assert!(p.contains("merge.failed") && p.contains("gtcore-1") && p.contains("ci red"));
        let (k, _) = trigger_wake(&RoleTrigger::MergeReady {
            bead: "gtcore-2".into(),
            branch: "gtcore-2".into(),
        });
        assert_eq!(k, DogKind::Sheriff);
        let (k, p) = trigger_wake(&RoleTrigger::BeadClosed { bead: "gtcore-3".into() });
        assert_eq!(k, DogKind::Witness);
        assert!(p.contains("issues.closed"));
        let (k, p) = trigger_wake(&RoleTrigger::HealthTick);
        assert_eq!(k, DogKind::Deacon);
        assert!(p.contains("health-tick"));
        let (k, p) = trigger_wake(&RoleTrigger::OnDemand {
            role: DogKind::Refinery,
            reason: "unstick the queue".into(),
        });
        assert_eq!(k, DogKind::Refinery);
        assert!(p.contains("on-demand") && p.contains("unstick the queue"));
    }

    #[tokio::test]
    async fn wake_writes_the_payload_and_keeps_the_session_alive() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let root = tmp_root("wake");
        let h = host(tmux.clone(), &root);

        h.wake(DogKind::Witness, r#"{"trigger":"issues.closed","bead":"gtcore-1"}"#)
            .await
            .expect("wake");
        assert!(tmux.has_session("witness-resident"), "wake raises a dead resident");
        let payload = std::fs::read_to_string(role_wake_file(&root, "witness")).unwrap();
        assert!(payload.contains("issues.closed"));

        // A second wake refreshes the payload without duplicating the session.
        h.wake(DogKind::Witness, r#"{"trigger":"issues.closed","bead":"gtcore-2"}"#)
            .await
            .expect("re-wake");
        let payload = std::fs::read_to_string(role_wake_file(&root, "witness")).unwrap();
        assert!(payload.contains("gtcore-2"));
    }
}
