//! Mayor-mode auto-dispatch (gtcore-d72302, child of gtcore-47522f).
//!
//! The legacy auto-dispatch path ([`crate::auto_dispatch`]) is bead-by-bead: the
//! orchd polls the `ready_for_auto` frontier and feeds *each* ready bead to the
//! scheduler, which slings a polecat per bead. The orchd owns the dispatch
//! decision.
//!
//! Mayor mode flips that. Instead of slinging polecats directly, the orchd loop
//! ensures a single **MAYOR** session per rig is alive and hands it the ready
//! frontier; the mayor — not the orchd — decides the bead-by-bead dispatch. The
//! orchd keeps the pool / host_cap arbitration (the mayor asks, the orchd bounds),
//! but it stops deciding *which* bead runs.
//!
//! This module is the policy core + ports, mirroring [`gt_runtime::dispatch`]:
//!
//! - [`ReadySource`] (reused from `gt_runtime`) — *where ready beads come from*.
//! - [`MayorWaker`] — *what waking a mayor means*: ensure the rig's mayor session
//!   is alive and deliver it the frontier. The production adapter
//!   ([`FnMayorWaker`]) watches a supervised mayor `tmux` session and writes the
//!   frontier to a per-rig wake file; a test records the call.
//! - [`plan_by_rig`] — the pure policy: group the ready frontier by rig.
//! - [`MayorDispatcher`] — the async driver tying the two together on a cadence.
//!
//! ## Wake-on-task (A4)
//!
//! The mayor must not busy-poll an LLM. The loop only wakes a rig's mayor when
//! that rig has ready work: an empty frontier (or a rig with no ready beads) is a
//! no-op, so an idle rig burns ~0 tokens. A live mayor blocks on its wake file
//! between signals; a dead mayor is re-slung by the supervisor (so "el mayor
//! caído se re-despierta solo" holds independently of this loop).

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gt_mcp_server::bead_prefix;
use gt_polecat::Tmux;
use gt_quota::{AccountQuotaStatus, Keychain, QuotaHandle};
use gt_runtime::{BeadId, ReadySource};
use gt_skills::SkillState;
use tokio::task::JoinHandle;

use crate::credential_guard::{resolve_for_sling, CredOutcome, ResolvedCredentials};
use crate::mcp::eventlog::EventLog;

/// Why waking a mayor failed. Opaque to the policy core, which only needs to know
/// a wake succeeded or failed (a failed rig is simply retried next tick).
#[derive(Debug)]
pub struct MayorWakeError(pub String);

impl std::fmt::Display for MayorWakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mayor wake failed: {}", self.0)
    }
}

impl std::error::Error for MayorWakeError {}

/// Waking a rig's mayor: ensure its session is alive and hand it `frontier`.
///
/// Idempotent by contract — called every tick a rig has ready work, so the
/// adapter must not re-sling an already-live mayor; it should only (re-)deliver
/// the frontier. Returns once the wake is *issued*, not once the mayor acts on
/// it; the mayor's own dispatch happens out-of-band.
pub trait MayorWaker {
    /// Ensure the mayor for `rig` is alive and deliver it `frontier`.
    fn wake(
        &self,
        rig: &str,
        frontier: &[BeadId],
    ) -> impl std::future::Future<Output = Result<(), MayorWakeError>> + Send;
}

/// The rig a bead belongs to: its id prefix (`gtcore-d72302` → `gtcore`), the
/// same convention the polecat router uses ([`bead_prefix`]).
pub fn rig_of(bead: &BeadId) -> &str {
    bead_prefix(bead.as_str())
}

/// Pure policy: group the ready frontier by rig, preserving the source's order
/// within each rig.
///
/// An empty frontier yields an empty map — the wake-on-task invariant: a tick
/// with no ready beads wakes nobody. A rig appears in the map only if it has at
/// least one ready bead, so an idle rig is never woken.
pub fn plan_by_rig(ready: &[BeadId]) -> BTreeMap<String, Vec<BeadId>> {
    let mut groups: BTreeMap<String, Vec<BeadId>> = BTreeMap::new();
    for bead in ready {
        groups
            .entry(rig_of(bead).to_string())
            .or_default()
            .push(bead.clone());
    }
    groups
}

/// The autonomous mayor dispatcher: drives [`ReadySource`] → [`plan_by_rig`] →
/// [`MayorWaker`] on a cadence. Unlike [`gt_runtime::Dispatcher`] it tracks no
/// in-flight set — the mayor (and the pool allocator behind it) owns capacity;
/// this loop only ensures the mayor is awake with a current frontier.
pub struct MayorDispatcher<S, M> {
    source: S,
    waker: M,
    /// Optional rig scope: when set, only that rig's frontier is acted on (the
    /// boot-template single-rig deployment). `None` wakes every rig present in
    /// the frontier (multi-rig routing).
    rig: Option<String>,
}

impl<S: ReadySource, M: MayorWaker> MayorDispatcher<S, M> {
    /// Build a mayor dispatcher over `source`, waking via `waker`.
    pub fn new(source: S, waker: M) -> Self {
        MayorDispatcher {
            source,
            waker,
            rig: None,
        }
    }

    /// Scope the dispatcher to a single rig (the boot-template deployment).
    pub fn with_rig(mut self, rig: impl Into<String>) -> Self {
        self.rig = Some(rig.into());
        self
    }

    /// One cycle: poll the frontier, group by rig, and wake each rig that has
    /// ready work. Returns the rigs actually woken this tick (sorted, from the
    /// `BTreeMap` order). A wake failure for one rig logs and is retried next
    /// tick; it does not abort the others.
    pub async fn tick(&self) -> Vec<String> {
        let ready = self.source.ready().await;
        let groups = plan_by_rig(&ready);
        let mut woken = Vec::new();
        for (rig, frontier) in groups {
            if let Some(scope) = &self.rig {
                if scope != &rig {
                    continue;
                }
            }
            match self.waker.wake(&rig, &frontier).await {
                Ok(()) => woken.push(rig),
                Err(e) => eprintln!("[mayor-dispatch] wake rig={rig} failed: {e}"),
            }
        }
        woken
    }

    /// Drive [`tick`](Self::tick) on a fixed cadence until aborted. The first
    /// tick fires immediately, then every `tick` interval thereafter — the same
    /// cadence shape as [`gt_runtime::Dispatcher::run`].
    pub async fn run(&self, tick: Duration) {
        let mut interval = tokio::time::interval(tick);
        loop {
            interval.tick().await;
            self.tick().await;
        }
    }
}

impl<S, M> MayorDispatcher<S, M>
where
    S: ReadySource + Send + Sync + 'static,
    M: MayorWaker + Send + Sync + 'static,
{
    /// Spawn the dispatch loop as a detached task, returning its [`JoinHandle`]
    /// so the host can abort it on shutdown.
    pub fn spawn(self: Arc<Self>, tick: Duration) -> JoinHandle<()> {
        tokio::spawn(async move { self.run(tick).await })
    }
}

/// [`MayorWaker`] adapter wrapping a synchronous closure. The production wake is
/// pure host I/O — register a supervised mayor session and write a wake file — so
/// it needs no `.await`; the closure runs to completion before the (immediately
/// ready) future resolves. Keeping it sync also keeps the composition root's
/// wiring free of `async` plumbing.
pub struct FnMayorWaker<F>(pub F);

impl<F> MayorWaker for FnMayorWaker<F>
where
    F: Fn(&str, &[BeadId]) -> Result<(), MayorWakeError> + Send + Sync,
{
    fn wake(
        &self,
        rig: &str,
        frontier: &[BeadId],
    ) -> impl std::future::Future<Output = Result<(), MayorWakeError>> + Send {
        let outcome = (self.0)(rig, frontier);
        async move { outcome }
    }
}

/// The task directive seeded as a mayor session's first prompt. Mirrors
/// [`gt_polecat::polecat_prompt`] but for the orchestration role: the mayor reads
/// the ready frontier the orchd delivered, claims/delegates beads through the MCP
/// tools (the orchd's pool/host_cap bounds how many actually sling), and otherwise
/// idles — blocking on its wake file rather than busy-polling.
pub fn mayor_prompt(workspace: &str, rig: &str) -> String {
    format!(
        "You are the gt MAYOR for rig `{rig}` in workspace `{workspace}`. You are the \
         orchestration loop: the orchd keeps you alive and signals you the ready frontier \
         instead of slinging polecats itself. Do NOT busy-poll. When signalled, read the \
         ready beads the orchd delivered to `$GT_CHANNEL_ROOT/mayor-wake/{rig}.event` (a JSON \
         array of bead ids), then for each bead decide whether to delegate it — the orchd's \
         pool / host_cap arbitration bounds how many actually sling, so request freely and let \
         it backpressure. Use the `mcp__gt__*` tools for all tracker access. Recall durable team \
         memory with `mcp__gt__memory_recall` before acting and treat every `feedback` memory as \
         a hard rule. When there is no ready work, stay idle (block on the wake file); burn no \
         tokens until the orchd signals again."
    )
}

/// Default tmux session-name prefix for mayor sessions (`<prefix>-<rig>`), the
/// orchestration counterpart of the polecat `<prefix>-<bead>` convention.
pub const DEFAULT_MAYOR_PREFIX: &str = "mayor";

/// Per-rig mayor session name: `<prefix>-<rig>`.
pub fn mayor_session(prefix: &str, rig: &str) -> String {
    format!("{prefix}-{rig}")
}

/// The wake file the orchd writes a rig's ready frontier to, and a live mayor
/// blocks on between signals: `<channel_root>/mayor-wake/<rig>.event`.
pub fn mayor_wake_file(channel_root: &Path, rig: &str) -> PathBuf {
    channel_root.join("mayor-wake").join(format!("{rig}.event"))
}

/// Serialize a frontier as the JSON array of bead-id strings the mayor reads back
/// from its wake file.
fn frontier_json(frontier: &[BeadId]) -> String {
    let ids: Vec<&str> = frontier.iter().map(BeadId::as_str).collect();
    // Bead ids are slugs; serialization cannot fail, but stay defensive.
    serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_string())
}

/// Atomically write the frontier to `path` (tmp sibling + rename), creating the
/// `mayor-wake/` directory if absent. Atomic so a mayor never reads a half-written
/// frontier mid-signal.
fn write_wake_file(path: &Path, frontier: &[BeadId]) -> io::Result<()> {
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
    fs::write(&tmp, frontier_json(frontier))?;
    fs::rename(&tmp, path)
}

/// Production [`MayorWaker`]: keeps one supervised `tmux` mayor session alive per
/// rig and delivers the frontier by writing the rig's wake file.
///
/// `wake(rig, frontier)` is idempotent (the [`MayorWaker`] contract) and does two
/// things each tick a rig has ready work:
///
/// 1. **Deliver** — atomically writes `frontier` as a JSON array of bead ids to the
///    rig's wake file ([`mayor_wake_file`]); a live mayor blocks on this file and
///    re-reads it on each signal.
/// 2. **Supervise** — if the rig's mayor `tmux` session is gone, (re-)creates it
///    seeded with [`mayor_prompt`]. This is the "el mayor caído se re-despierta
///    solo" path: the next tick a rig has ready work re-slings a dead mayor. A live
///    session is left untouched (no re-sling), so an already-awake mayor only sees
///    the refreshed wake file — it is never duplicated.
///
/// Both steps are short host I/O (a file write + at most one `tmux new-session`),
/// so [`MayorWaker::wake`] runs them synchronously and resolves an already-ready
/// future, mirroring [`FnMayorWaker`].
pub struct TmuxMayorWaker {
    tmux: Arc<dyn Tmux>,
    workspace: String,
    prefix: String,
    workdir: PathBuf,
    command: String,
    args: Vec<String>,
    base_env: Vec<(String, String)>,
    channel_root: PathBuf,
    /// Claude-account keychain (gtcore-559c50). When set, a mayor spawn resolves the active
    /// account's `CLAUDE_CONFIG_DIR` through the SAME credential guard as the polecat sling —
    /// validating its `.credentials.json`, rotating off a credential-dead/quota-blocked account,
    /// and BLOCKING the spawn (no 401-born mayor) when none can authenticate. `None` ⇒ the legacy
    /// path: the mayor inherits whatever `base_env` carries (host default `~/.claude`).
    keychain: Option<Arc<dyn Keychain>>,
    /// Quota actor handle (gtcore-559c50): snapshots per-account status so the credential guard's
    /// sling-time gate skips a Limited/Blocked active account, exactly like the polecat path. `None`
    /// ⇒ every account's status is unknown (permissive), i.e. credential-validity only.
    quota: Option<QuotaHandle>,
    /// Anthropic passthrough proxy URL (gtcore-559c50 / hq-284842): when set AND an account is
    /// resolved, the mayor's `claude` is pointed at it via `ANTHROPIC_BASE_URL` with the
    /// `x-gt-account`/`x-gt-session` attribution headers, so the mayor's spend feeds per-call quota
    /// truth like a polecat's. `None` ⇒ the mayor talks to the API directly.
    anthropic_proxy_url: Option<String>,
    /// Event log for the `skills.*` catalog (gtcore-ec24d2). When set, a mayor spawn materialises its
    /// role skills + Knowledge (`CLAUDE.md`) + model config into its workdir through the SAME shared
    /// path every role uses ([`crate::role_session::materialize_role_session`]), so the mayor loads
    /// its mayor-role config like a polecat/terminal session. `None` ⇒ no role materialisation
    /// (legacy: the mayor opens with the repo's generic `CLAUDE.md` and no role skills).
    event_log: Option<Arc<EventLog>>,
}

impl TmuxMayorWaker {
    /// Build a mayor waker from explicit fields. `base_env` is layered onto every
    /// mayor session; the role/rig/channel/wake-file vars are derived per `wake`,
    /// so any `GT_ROLE`/`GT_RIG` the caller passes is overridden.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tmux: Arc<dyn Tmux>,
        workspace: impl Into<String>,
        prefix: impl Into<String>,
        workdir: PathBuf,
        command: impl Into<String>,
        args: Vec<String>,
        base_env: Vec<(String, String)>,
        channel_root: PathBuf,
    ) -> Self {
        TmuxMayorWaker {
            tmux,
            workspace: workspace.into(),
            prefix: prefix.into(),
            workdir,
            command: command.into(),
            args,
            base_env,
            channel_root,
            keychain: None,
            quota: None,
            anthropic_proxy_url: None,
            event_log: None,
        }
    }

    /// Wire the `skills.*` catalog event log so a mayor spawn materialises its role skills +
    /// Knowledge + model config like a polecat sling (gtcore-ec24d2). Without it the mayor opens with
    /// no role config.
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.event_log = Some(log);
        self
    }

    /// Wire the claude-account keychain so a mayor spawn resolves + validates credentials like a
    /// polecat sling (gtcore-559c50). Without it the mayor uses the static `base_env` account.
    pub fn with_keychain(mut self, keychain: Arc<dyn Keychain>) -> Self {
        self.keychain = Some(keychain);
        self
    }

    /// Wire the quota actor handle so the credential guard's sling-time status gate rotates off a
    /// Limited/Blocked active account when spawning a mayor (gtcore-559c50).
    pub fn with_quota(mut self, quota: QuotaHandle) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Point a resolved-account mayor's `claude` at the Anthropic passthrough proxy (gtcore-559c50).
    /// Empty disables (treated as `None`).
    pub fn with_anthropic_proxy(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.anthropic_proxy_url = if url.is_empty() { None } else { Some(url) };
        self
    }

    /// Resolve the account a mayor spawn should authenticate as, mirroring the polecat sling
    /// (gtcore-559c50): snapshot per-account quota status, then run the shared credential guard.
    ///
    /// - `Ok(Some(creds))` — stamp this account's `CLAUDE_CONFIG_DIR`/`GT_HOOK_ACCOUNT` (+ proxy).
    /// - `Ok(None)` — no keychain configured, or the guard fell back to the host default; spawn
    ///   the mayor without stamping (legacy liveness).
    /// - `Err(_)` — no account can authenticate: the caller must NOT spawn (a mayor born in 401
    ///   produces nothing), and the rig is retried next tick when the frontier persists.
    async fn resolve_credentials(&self) -> Result<Option<ResolvedCredentials>, MayorWakeError> {
        let Some(kc) = &self.keychain else {
            return Ok(None);
        };
        let quota_status: HashMap<String, AccountQuotaStatus> = match &self.quota {
            Some(q) => q
                .accounts()
                .await
                .into_iter()
                .map(|a| (a.id, a.status))
                .collect(),
            None => HashMap::new(),
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match resolve_for_sling(kc, now_ms, |acc| quota_status.get(acc).copied(), |_| 100.0) {
            CredOutcome::Resolved {
                resolved,
                rotated_from,
                ..
            } => {
                if let Some(from) = &rotated_from {
                    eprintln!(
                        "[mayor-dispatch] active account {from} credential-dead — mayor using {} instead",
                        resolved.account
                    );
                }
                Ok(Some(resolved))
            }
            CredOutcome::NoValidAccount { .. } => Err(MayorWakeError(
                "no keychain account has valid credentials — not spawning mayor into 401".to_string(),
            )),
            CredOutcome::HostDefault => Ok(None),
        }
    }

    /// The session-level env for a rig's mayor: the shared `base_env` (workspace,
    /// sandbox flag, …) with the orchestration role + rig + channel pointers layered
    /// on. `GT_ROLE` is forced to `mayor` (the base template is a polecat template).
    ///
    /// When `creds` is `Some` (a keychain account was resolved, gtcore-559c50) the resolved
    /// `CLAUDE_CONFIG_DIR` + `GT_HOOK_ACCOUNT` (and, with a proxy, `ANTHROPIC_BASE_URL` +
    /// `ANTHROPIC_CUSTOM_HEADERS`) are stamped so the mayor's `claude` authenticates as that
    /// account — exactly like a polecat sling. Those keys are FIRST stripped from `base_env` so a
    /// stale static value baked into the boot template can never shadow the freshly resolved one.
    fn session_env(
        &self,
        rig: &str,
        wake_file: &Path,
        session: &str,
        creds: Option<&ResolvedCredentials>,
    ) -> Vec<(String, String)> {
        // Overridden below; also drop the credential/proxy keys so a boot-template value never wins
        // over the per-spawn resolution (gtcore-559c50).
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
        env.push(("GT_ROLE".to_string(), "mayor".to_string()));
        env.push(("GT_RIG".to_string(), rig.to_string()));
        env.push((
            "GT_CHANNEL_ROOT".to_string(),
            self.channel_root.display().to_string(),
        ));
        env.push((
            "GT_MAYOR_WAKE_FILE".to_string(),
            wake_file.display().to_string(),
        ));
        if let Some(c) = creds {
            env.push(("CLAUDE_CONFIG_DIR".to_string(), c.config_dir.clone()));
            env.push((gt_polecat::GT_HOOK_ACCOUNT.to_string(), c.account.clone()));
            // Route the mayor's claude through the Anthropic passthrough proxy so its spend feeds
            // per-call quota truth, with the account/session attribution headers (hq-284842).
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
}

impl MayorWaker for TmuxMayorWaker {
    fn wake(
        &self,
        rig: &str,
        frontier: &[BeadId],
    ) -> impl std::future::Future<Output = Result<(), MayorWakeError>> + Send {
        async move {
            // 1. Deliver the frontier — a live mayor blocks on this file and re-reads it per signal.
            let wake_file = mayor_wake_file(&self.channel_root, rig);
            write_wake_file(&wake_file, frontier).map_err(|e| {
                MayorWakeError(format!("write wake file {}: {e}", wake_file.display()))
            })?;

            // 2. Supervise: a live session is left untouched (only the wake file is refreshed); a
            // dead/absent one is (re-)spawned. Credential resolution happens ONLY on spawn — a live
            // mayor's env cannot be changed, and re-resolving every tick would waste a quota probe.
            let session = mayor_session(&self.prefix, rig);
            if self.tmux.has_session(&session) {
                return Ok(());
            }
            // Resolve + validate the account to launch under, like the polecat sling (gtcore-559c50).
            // An `Err` (no account can authenticate) aborts the spawn so the mayor is never born in
            // 401; the rig is retried next tick while its frontier persists.
            let creds = self.resolve_credentials().await?;
            // Pre-accept claude's first-run onboarding/trust dialogs in the dir the mayor will read,
            // exactly like the polecat sling (gtcore-a01791). Without this a freshly-resolved
            // CLAUDE_CONFIG_DIR stops the interactive `claude` at the "Is this a project you trust?"
            // folder-trust gate over the mayor's workdir, and the autonomous session hangs forever
            // (it never reads the frontier nor delegates — observed live 2026-06-28). Seed the
            // EFFECTIVE dir claude reads (the resolved account's CLAUDE_CONFIG_DIR, else a static one
            // baked into base_env, else `$HOME/.claude`) and trust `self.workdir`. Best-effort.
            let effective_config_dir: Option<PathBuf> = creds
                .as_ref()
                .map(|c| PathBuf::from(&c.config_dir))
                .or_else(|| {
                    self.base_env
                        .iter()
                        .find(|(k, _)| k == "CLAUDE_CONFIG_DIR")
                        .map(|(_, v)| PathBuf::from(v))
                })
                .or_else(|| {
                    std::env::var_os("HOME").map(|h| Path::new(&h).join(".claude"))
                });
            if let Some(cd) = &effective_config_dir {
                crate::worktree::seed_claude_onboarding(cd, &self.workdir);
                crate::worktree::seed_user_hooks(cd);
            }
            let env = self.session_env(rig, &wake_file, &session, creds.as_ref());
            let mut args = self.args.clone();
            args.push(mayor_prompt(&self.workspace, rig));
            // Materialise the mayor's role skills + Knowledge (CLAUDE.md) + model config into its
            // workdir via the SHARED role-session path every role uses (gtcore-ec24d2), so the mayor
            // loads its mayor-role config like a polecat/terminal session instead of opening with the
            // repo's generic CLAUDE.md and no skills. Best-effort: a replay/IO failure logs and the
            // mayor still launches; a configured RoleModel is stamped onto its launch args.
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
                            "mayor",
                            &self.workdir,
                            &[
                                ("workspace", self.workspace.clone()),
                                ("rig", rig.to_string()),
                                ("RigName", rig.to_string()),
                                ("WorkDir", self.workdir.display().to_string()),
                            ],
                        ) {
                            crate::polecat::apply_role_model(&mut args, &model);
                        }
                    }
                    Err(e) => eprintln!(
                        "[mayor-dispatch] skills replay failed — no role skills/CLAUDE.md for mayor {rig}: {e}"
                    ),
                }
            }
            self.tmux
                .new_session(&session, &self.workdir, &self.command, &args, &env)
                .map_err(|e| MayorWakeError(format!("spawn mayor session {session}: {e}")))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    fn ids(slugs: &[&str]) -> Vec<BeadId> {
        slugs.iter().map(|s| BeadId::from(*s)).collect()
    }

    // ---- plan_by_rig(): the pure policy core --------------------------------

    #[test]
    fn rig_of_takes_the_id_prefix() {
        assert_eq!(rig_of(&BeadId::from("gtcore-d72302")), "gtcore");
        assert_eq!(rig_of(&BeadId::from("hq-core-host.2")), "hq");
        assert_eq!(rig_of(&BeadId::from("solo")), "solo");
    }

    #[test]
    fn plan_by_rig_groups_and_preserves_order() {
        let ready = ids(&["gtcore-a", "hq-x", "gtcore-b", "hq-y"]);
        let groups = plan_by_rig(&ready);
        assert_eq!(groups.len(), 2, "two rigs present");
        assert_eq!(groups["gtcore"], ids(&["gtcore-a", "gtcore-b"]), "source order within rig");
        assert_eq!(groups["hq"], ids(&["hq-x", "hq-y"]));
    }

    #[test]
    fn plan_by_rig_empty_frontier_wakes_nobody() {
        // Wake-on-task: no ready beads => no rigs => nothing to wake.
        assert!(plan_by_rig(&[]).is_empty());
    }

    // ---- in-memory ports ----------------------------------------------------

    /// Returns a fixed ready frontier each tick.
    struct FixedSource(Vec<BeadId>);
    impl ReadySource for FixedSource {
        async fn ready(&self) -> Vec<BeadId> {
            self.0.clone()
        }
    }

    /// Records every (rig, frontier) wake; optionally fails for a named rig.
    struct RecordingWaker {
        calls: StdMutex<Vec<(String, Vec<BeadId>)>>,
        fail_rig: Option<String>,
    }
    impl RecordingWaker {
        fn new() -> Self {
            RecordingWaker { calls: StdMutex::new(Vec::new()), fail_rig: None }
        }
        fn failing(rig: &str) -> Self {
            RecordingWaker { calls: StdMutex::new(Vec::new()), fail_rig: Some(rig.to_string()) }
        }
        fn calls(&self) -> Vec<(String, Vec<BeadId>)> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl MayorWaker for RecordingWaker {
        async fn wake(&self, rig: &str, frontier: &[BeadId]) -> Result<(), MayorWakeError> {
            self.calls.lock().unwrap().push((rig.to_string(), frontier.to_vec()));
            if self.fail_rig.as_deref() == Some(rig) {
                return Err(MayorWakeError("scripted failure".into()));
            }
            Ok(())
        }
    }

    // ---- tick(): mayor mode -------------------------------------------------

    #[tokio::test]
    async fn tick_wakes_one_mayor_per_rig_with_the_frontier() {
        let waker = RecordingWaker::new();
        let d = MayorDispatcher::new(FixedSource(ids(&["gtcore-a", "gtcore-b", "hq-x"])), waker);
        let woken = d.tick().await;
        assert_eq!(woken, vec!["gtcore".to_string(), "hq".to_string()], "one wake per rig");
        let calls = d.waker.calls();
        assert_eq!(calls.len(), 2, "no polecat-per-bead; one wake per rig");
        assert_eq!(calls[0], ("gtcore".to_string(), ids(&["gtcore-a", "gtcore-b"])));
        assert_eq!(calls[1], ("hq".to_string(), ids(&["hq-x"])));
    }

    #[tokio::test]
    async fn tick_idle_frontier_wakes_nobody() {
        // Wake-on-task: an empty frontier burns ~0 tokens — no wake is issued.
        let d = MayorDispatcher::new(FixedSource(vec![]), RecordingWaker::new());
        assert!(d.tick().await.is_empty(), "idle => no wake");
        assert!(d.waker.calls().is_empty(), "the waker is never called when idle");
    }

    #[tokio::test]
    async fn tick_scoped_to_rig_ignores_other_rigs() {
        let d = MayorDispatcher::new(
            FixedSource(ids(&["gtcore-a", "hq-x"])),
            RecordingWaker::new(),
        )
        .with_rig("gtcore");
        let woken = d.tick().await;
        assert_eq!(woken, vec!["gtcore".to_string()], "only the scoped rig is woken");
        assert_eq!(d.waker.calls().len(), 1, "the other rig's frontier is ignored");
    }

    #[tokio::test]
    async fn tick_one_rig_failing_does_not_block_the_others() {
        let waker = RecordingWaker::failing("gtcore");
        let d = MayorDispatcher::new(FixedSource(ids(&["gtcore-a", "hq-x"])), waker);
        let woken = d.tick().await;
        assert_eq!(woken, vec!["hq".to_string()], "the failed rig is not reported woken");
        assert_eq!(d.waker.calls().len(), 2, "both rigs were attempted");
    }

    #[tokio::test]
    async fn fn_mayor_waker_forwards_to_the_closure() {
        let seen: Arc<StdMutex<Vec<(String, Vec<BeadId>)>>> = Arc::new(StdMutex::new(Vec::new()));
        let sink = seen.clone();
        let waker = FnMayorWaker(move |rig: &str, frontier: &[BeadId]| {
            sink.lock().unwrap().push((rig.to_string(), frontier.to_vec()));
            Ok(())
        });
        let d = MayorDispatcher::new(FixedSource(ids(&["gtcore-a"])), waker);
        assert_eq!(d.tick().await, vec!["gtcore".to_string()]);
        assert_eq!(seen.lock().unwrap().clone(), vec![("gtcore".to_string(), ids(&["gtcore-a"]))]);
    }

    #[test]
    fn mayor_prompt_names_rig_workspace_and_wake_file() {
        let p = mayor_prompt("default", "gtcore");
        assert!(p.contains("MAYOR for rig `gtcore`"));
        assert!(p.contains("workspace `default`"));
        assert!(p.contains("mayor-wake/gtcore.event"), "points the mayor at its wake file");
        assert!(p.contains("Do NOT busy-poll"), "wake-on-task directive present");
    }

    // ---- TmuxMayorWaker: the production wake adapter ------------------------

    use gt_polecat::FakeTmux;

    fn waker(tmux: Arc<FakeTmux>, root: &Path) -> TmuxMayorWaker {
        TmuxMayorWaker::new(
            tmux,
            "default",
            DEFAULT_MAYOR_PREFIX,
            PathBuf::from("/rig"),
            "claude",
            vec!["--dangerously-skip-permissions".to_string()],
            vec![("GT_ROLE".to_string(), "polecat".to_string())],
            root.to_path_buf(),
        )
    }

    #[tokio::test]
    async fn tmux_waker_delivers_frontier_to_the_wake_file() {
        let tmp = tempfile::tempdir().unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let w = waker(tmux, tmp.path());

        w.wake("gtcore", &ids(&["gtcore-a", "gtcore-b"])).await.unwrap();

        let file = mayor_wake_file(tmp.path(), "gtcore");
        let body = std::fs::read_to_string(&file).unwrap();
        assert_eq!(body, r#"["gtcore-a","gtcore-b"]"#, "frontier written as a JSON id array");
    }

    #[tokio::test]
    async fn tmux_waker_spawns_the_mayor_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let w = waker(tmux.clone(), tmp.path());

        assert!(!tmux.has_session("mayor-gtcore"), "no session before the first wake");
        w.wake("gtcore", &ids(&["gtcore-a"])).await.unwrap();
        assert!(tmux.has_session("mayor-gtcore"), "the mayor session is created on wake");
        // The orchestration role + rig are stamped on the session env.
        assert_eq!(tmux.show_environment("mayor-gtcore", "GT_ROLE").unwrap().as_deref(), Some("mayor"));
        assert_eq!(tmux.show_environment("mayor-gtcore", "GT_RIG").unwrap().as_deref(), Some("gtcore"));
        // gtcore-559c50: with NO keychain wired, the legacy path is unchanged — the mayor inherits
        // base_env and no per-spawn CLAUDE_CONFIG_DIR / GT_HOOK_ACCOUNT is stamped.
        assert!(
            tmux.show_environment("mayor-gtcore", "CLAUDE_CONFIG_DIR").unwrap().is_none(),
            "no keychain ⇒ no credential stamping (legacy)",
        );
    }

    #[tokio::test]
    async fn tmux_waker_stamps_resolved_account_on_spawn() {
        // gtcore-559c50: a mayor spawn resolves the keychain's ACTIVE account through the same
        // credential guard as the polecat sling and stamps its CLAUDE_CONFIG_DIR / GT_HOOK_ACCOUNT
        // (+ the Anthropic proxy attribution) — so the mayor's claude authenticates instead of
        // inheriting a stale boot-template account and dying in 401.
        use gt_quota::InMemoryKeychain;
        let tmp = tempfile::tempdir().unwrap();
        let acct_dir = tmp.path().join("acct-a");
        std::fs::create_dir_all(&acct_dir).unwrap();
        // A missing .credentials.json is credential-permissive (a fresh/seeded dir) — the guard
        // resolves the active account without a quota handle marking it unhealthy.
        let keychain: Arc<dyn Keychain> =
            Arc::new(InMemoryKeychain::seeded([("acct-a", acct_dir.to_str().unwrap())]));
        keychain.set_active("acct-a").unwrap();
        let fake = Arc::new(FakeTmux::new());
        let w = waker(fake.clone(), tmp.path())
            .with_keychain(keychain)
            .with_anthropic_proxy("http://127.0.0.1:8089");

        w.wake("gtcore", &ids(&["gtcore-a"])).await.unwrap();

        assert!(fake.has_session("mayor-gtcore"));
        assert_eq!(
            fake.show_environment("mayor-gtcore", "CLAUDE_CONFIG_DIR").unwrap().as_deref(),
            Some(acct_dir.to_str().unwrap()),
            "the resolved account's config dir is stamped",
        );
        assert_eq!(
            fake.show_environment("mayor-gtcore", gt_polecat::GT_HOOK_ACCOUNT).unwrap().as_deref(),
            Some("acct-a"),
        );
        assert_eq!(
            fake.show_environment("mayor-gtcore", "ANTHROPIC_BASE_URL").unwrap().as_deref(),
            Some("http://127.0.0.1:8089"),
            "a resolved account routes through the passthrough proxy",
        );
        let headers = fake
            .show_environment("mayor-gtcore", "ANTHROPIC_CUSTOM_HEADERS")
            .unwrap()
            .expect("attribution headers stamped");
        assert!(headers.contains("x-gt-account: acct-a"));
        assert!(headers.contains("x-gt-session: mayor-gtcore"));
    }

    #[tokio::test]
    async fn tmux_waker_seeds_onboarding_and_trusts_the_workdir_on_spawn() {
        // gtcore-a01791: a mayor spawn must pre-accept claude's first-run onboarding + trust the
        // workdir in the dir claude reads, or the interactive session hangs on the "Is this a project
        // you trust?" dialog and never delegates. Mirrors the polecat sling's seed_claude_onboarding.
        use gt_quota::InMemoryKeychain;
        let tmp = tempfile::tempdir().unwrap();
        let acct_dir = tmp.path().join("acct-a");
        std::fs::create_dir_all(&acct_dir).unwrap();
        let keychain: Arc<dyn Keychain> =
            Arc::new(InMemoryKeychain::seeded([("acct-a", acct_dir.to_str().unwrap())]));
        keychain.set_active("acct-a").unwrap();
        let fake = Arc::new(FakeTmux::new());
        // waker() uses workdir /rig.
        let w = waker(fake.clone(), tmp.path()).with_keychain(keychain);

        w.wake("gtcore", &ids(&["gtcore-a"])).await.unwrap();

        // The resolved account's config dir got the onboarding + folder-trust seed for /rig.
        let claude_json = acct_dir.join(".claude.json");
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&claude_json).unwrap()).unwrap();
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["bypassPermissionsModeAccepted"], serde_json::json!(true));
        assert_eq!(
            v["projects"]["/rig"]["hasTrustDialogAccepted"],
            serde_json::json!(true),
            "the mayor's workdir is pre-trusted so the session never stalls on the trust dialog",
        );
    }

    #[tokio::test]
    async fn tmux_waker_does_not_spawn_a_mayor_into_401() {
        // gtcore-559c50: when no keychain account can authenticate (active creds unreadable, no
        // healthy alternative), the spawn is ABORTED rather than birthing a mayor in 401 — but the
        // frontier is still delivered so the next tick retries.
        use gt_quota::InMemoryKeychain;
        let tmp = tempfile::tempdir().unwrap();
        let dead_dir = tmp.path().join("dead");
        std::fs::create_dir_all(&dead_dir).unwrap();
        // Garbage credentials classify as unreadable ⇒ not slingable; the single account leaves no
        // healthy alternative ⇒ NoValidAccount.
        std::fs::write(dead_dir.join(".credentials.json"), "not-json-garbage").unwrap();
        let keychain: Arc<dyn Keychain> =
            Arc::new(InMemoryKeychain::seeded([("dead", dead_dir.to_str().unwrap())]));
        keychain.set_active("dead").unwrap();
        let fake = Arc::new(FakeTmux::new());
        let w = waker(fake.clone(), tmp.path()).with_keychain(keychain);

        let res = w.wake("gtcore", &ids(&["gtcore-a"])).await;
        assert!(res.is_err(), "no authable account ⇒ wake fails");
        assert!(!fake.has_session("mayor-gtcore"), "no mayor born into 401");
        // The frontier was delivered before the credential check, so the next tick re-attempts.
        let body = std::fs::read_to_string(mayor_wake_file(tmp.path(), "gtcore")).unwrap();
        assert_eq!(body, r#"["gtcore-a"]"#);
    }

    #[tokio::test]
    async fn tmux_waker_re_slings_a_dead_mayor() {
        // "el mayor caído se re-despierta solo": a wake after the session died recreates it.
        let tmp = tempfile::tempdir().unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let w = waker(tmux.clone(), tmp.path());

        w.wake("gtcore", &ids(&["gtcore-a"])).await.unwrap();
        tmux.kill_session("mayor-gtcore").unwrap();
        assert!(!tmux.has_session("mayor-gtcore"), "mayor is down");

        w.wake("gtcore", &ids(&["gtcore-a"])).await.unwrap();
        assert!(tmux.has_session("mayor-gtcore"), "the down mayor is re-slung on the next wake");
    }

    #[tokio::test]
    async fn tmux_waker_leaves_a_live_mayor_untouched() {
        // A live session is never re-slung: only the wake file is refreshed. The waker
        // stamps GT_RIG only when it spawns, so a pre-existing bare session keeps none.
        let tmp = tempfile::tempdir().unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let w = waker(tmux.clone(), tmp.path());

        // A live session the waker did not create (no GT_RIG stamped).
        tmux.new_session(
            "mayor-gtcore",
            Path::new("/rig"),
            "claude",
            &[],
            &[("SENTINEL".to_string(), "live".to_string())],
        )
        .unwrap();

        w.wake("gtcore", &ids(&["gtcore-a", "gtcore-b"])).await.unwrap();

        assert_eq!(
            tmux.show_environment("mayor-gtcore", "SENTINEL").unwrap().as_deref(),
            Some("live"),
            "the live session is preserved",
        );
        assert!(
            tmux.show_environment("mayor-gtcore", "GT_RIG").unwrap().is_none(),
            "no re-sling — the waker never stamped its env onto the live session",
        );
        // But the frontier was still delivered.
        let body = std::fs::read_to_string(mayor_wake_file(tmp.path(), "gtcore")).unwrap();
        assert_eq!(body, r#"["gtcore-a","gtcore-b"]"#);
    }

    #[tokio::test]
    async fn tmux_waker_drives_through_the_dispatcher() {
        // End-to-end: MayorDispatcher::tick over the production waker wakes one
        // mayor per rig and delivers each rig's frontier.
        let tmp = tempfile::tempdir().unwrap();
        let tmux = Arc::new(FakeTmux::new());
        let w = waker(tmux.clone(), tmp.path());
        let d = MayorDispatcher::new(FixedSource(ids(&["gtcore-a", "gtcore-b", "hq-x"])), w);

        let woken = d.tick().await;
        assert_eq!(woken, vec!["gtcore".to_string(), "hq".to_string()]);
        assert!(tmux.has_session("mayor-gtcore"));
        assert!(tmux.has_session("mayor-hq"));
        assert_eq!(
            std::fs::read_to_string(mayor_wake_file(tmp.path(), "gtcore")).unwrap(),
            r#"["gtcore-a","gtcore-b"]"#,
        );
        assert_eq!(
            std::fs::read_to_string(mayor_wake_file(tmp.path(), "hq")).unwrap(),
            r#"["hq-x"]"#,
        );
    }
}
