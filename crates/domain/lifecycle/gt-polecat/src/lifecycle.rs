//! Polecat spawn — the real I/O that replaces the `sleep` stub in `gt_agent::supervisor`.
//!
//! Two spawn shapes, one env-building chokepoint:
//! - [`spawn_process`] launches the command as a direct child via `tokio::process::Command`.
//!   This is the daemon-supervised path the restart loop drives (and the gate test exercises):
//!   a real process with a heartbeat file, killable with `kill -9`.
//! - [`spawn_tmux`] creates a detached tmux session via the [`Tmux`](crate::tmux::Tmux) port.
//!   This is the production polecat (a coding agent in a pane); it carries [`GT_HOOK_BEAD`] in
//!   the session env so `tmux show-environment GT_HOOK_BEAD` resolves the hooked bead.
//!
//! Both paths inject [`GT_HOOK_BEAD`] through [`crate::hooks::hook_env`], the single point that
//! must never drop a supplied bead (gg-0nb).

use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

use gt_agent::supervisor::touch_heartbeat;
use gt_agent::{AgentEvent, SessionRole};
use gt_events::Envelope;

use crate::hooks::{hook_env, GT_HOOK_BEAD};
use crate::tmux::Tmux;

/// The workspace a polecat belongs to, pinned into its tmux session env so
/// `tmux show-environment GT_WORKSPACE` resolves the tenant for dashboard attribution
/// (hq-mt-runtime.5). The value is supplied by the caller through [`SpawnSpec::env`]
/// (the composition root layers it onto `base_env`); a spec without it simply skips the pin.
pub const GT_WORKSPACE: &str = "GT_WORKSPACE";

/// Session-level env keys re-applied via an explicit `set-environment` after `new-session`
/// (belt-and-suspenders): the attribution vars a reader resolves with `show-environment`
/// regardless of which path it trusts. `GT_HOOK_BEAD` identifies the hooked bead;
/// `GT_WORKSPACE` identifies the tenant.
const SESSION_ATTRIBUTION_KEYS: [&str; 2] = [GT_HOOK_BEAD, GT_WORKSPACE];

/// Everything needed to (re)spawn one polecat. Cloneable so the restart loop can re-spawn
/// from the same spec after a crash.
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// Session id (the key in the session registry / `AgentEvent`).
    pub session: String,
    pub rig: String,
    /// Polecat name (used for the tmux session / restart-tracker agent id).
    pub polecat: String,
    /// Claude crew member running inside the polecat right now → `Session::crew`.
    pub crew: Option<String>,
    pub workdir: PathBuf,
    pub command: String,
    pub args: Vec<String>,
    /// Extra env layered onto the child/session (GT_ROLE, GT_RIG, …). `GT_HOOK_BEAD` is
    /// added automatically from `hook_bead`/`issue`; do not duplicate it here.
    pub env: Vec<(String, String)>,
    /// Bead explicitly attached by sling (preferred pin).
    pub hook_bead: Option<String>,
    /// Direct issue hooked at spawn (fallback pin).
    pub issue: Option<String>,
    /// Heartbeat file the live polecat touches; staleness here means "dead".
    pub heartbeat: PathBuf,
}

impl SpawnSpec {
    /// Full env including the `GT_HOOK_BEAD` pin (if any) and `GT_HEARTBEAT_FILE` — the heartbeat
    /// path the polecat's hooks `touch` so the supervisor sees it alive (`hq-agent-provisioning.1`).
    /// Keys already present in `env` are kept; these entries are appended.
    pub fn env_with_hook(&self) -> Vec<(String, String)> {
        let mut env = self.env.clone();
        if let Some(entry) = hook_env(self.hook_bead.as_deref(), self.issue.as_deref()) {
            env.push(entry);
        }
        // The polecat can't write its own heartbeat without knowing where: export the exact path
        // the supervisor computes (SpawnSpec::heartbeat) so a `touch $GT_HEARTBEAT_FILE` hook keeps
        // the session "alive" against the staleness check.
        env.push((
            "GT_HEARTBEAT_FILE".to_string(),
            self.heartbeat.display().to_string(),
        ));
        env
    }

    /// The `AgentEvent::Spawned` fact for this polecat (role always polecat; crew carried).
    pub fn spawned_event(&self) -> AgentEvent {
        AgentEvent::Spawned {
            session: self.session.clone(),
            rig: self.rig.clone(),
            role: SessionRole::Polecat,
            crew: self.crew.clone(),
            // This lower-level spawn path carries no worktree manifest; the supervisor's sling
            // path (gt-composition) stamps skills/hooks (hq-orch-sessions.2).
            skills: Vec::new(),
            hooks: Vec::new(),
            maintains_heartbeat: true,
            tmux_socket: None,
        }
    }

    /// Envelope wrapping [`SpawnSpec::spawned_event`] for the supervisor relay.
    pub fn spawned_envelope(&self) -> Envelope<AgentEvent> {
        Envelope::root(self.spawned_event())
    }
}

/// A running polecat child process plus the metadata the supervisor needs to track and
/// re-spawn it.
pub struct SpawnedPolecat {
    pub session: String,
    pub rig: String,
    pub crew: Option<String>,
    pub heartbeat: PathBuf,
    pub hook_bead: Option<String>,
    child: tokio::process::Child,
}

impl SpawnedPolecat {
    /// OS pid of the live child (`None` once it has been reaped).
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Send SIGKILL (the `kill -9` path) — best effort, does not wait.
    pub async fn kill(&mut self) {
        let _ = self.child.start_kill();
    }

    /// Wait for the child to exit. Used by the supervisor's watch loop.
    pub async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    /// Borrow the underlying child (the watch loop selects on `child.wait()`).
    pub(crate) fn child_mut(&mut self) -> &mut tokio::process::Child {
        &mut self.child
    }
}

/// Launch the polecat as a direct child process. Writes the initial heartbeat *before*
/// returning so the supervisor never sees a spurious "stale" on a just-spawned polecat.
pub async fn spawn_process(spec: &SpawnSpec) -> io::Result<SpawnedPolecat> {
    touch_heartbeat(&spec.heartbeat).await?;

    let mut cmd = Command::new(&spec.command);
    cmd.args(&spec.args)
        .current_dir(&spec.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in spec.env_with_hook() {
        cmd.env(k, v);
    }
    let child = cmd.spawn()?;

    Ok(SpawnedPolecat {
        session: spec.session.clone(),
        rig: spec.rig.clone(),
        crew: spec.crew.clone(),
        heartbeat: spec.heartbeat.clone(),
        hook_bead: hook_env(spec.hook_bead.as_deref(), spec.issue.as_deref()).map(|(_, v)| v),
        child,
    })
}

/// Create the production tmux-backed polecat session, pinning the
/// [attribution vars](SESSION_ATTRIBUTION_KEYS) (`GT_HOOK_BEAD`, `GT_WORKSPACE`) both at
/// creation (via `-e`) and again with an explicit `set-environment` — mirrors the Go
/// session_manager belt-and-suspenders so `show-environment` resolves each regardless of which
/// path a reader trusts. Only keys actually present in the spec env are re-applied.
pub fn spawn_tmux(tmux: &dyn Tmux, spec: &SpawnSpec) -> io::Result<()> {
    let env = spec.env_with_hook();
    tmux.new_session(&spec.session, &spec.workdir, &spec.command, &spec.args, &env)?;
    for key in SESSION_ATTRIBUTION_KEYS {
        if let Some((_, value)) = env.iter().find(|(k, _)| k == key) {
            tmux.set_environment(&spec.session, key, value)?;
        }
    }
    Ok(())
}

/// True if the heartbeat at `path` is older than `max_age` (or missing). Re-exported from the
/// agent supervisor so the lifecycle and its watch loop agree on staleness.
pub fn heartbeat_is_stale(path: &Path, max_age: std::time::Duration) -> bool {
    gt_agent::supervisor::heartbeat_is_stale(path, max_age)
}

/// Everything fixed for one rig's polecats, independent of which member is dispatched. The
/// composition root builds this once (from rig config / env) and reuses it for every sling —
/// the per-member bits (session name, hooked bead) are derived in [`SpawnTemplate::spec_for`].
#[derive(Debug, Clone)]
pub struct SpawnTemplate {
    /// Rig name → `Session.rig` and `GT_RIG`.
    pub rig: String,
    /// tmux session-name prefix for this rig's polecats (`<prefix>-<name>`, e.g. `gt-furiosa`).
    pub prefix: String,
    /// Working directory the agent runs in (the rig checkout).
    pub workdir: PathBuf,
    /// The coding-agent command launched in the pane (e.g. `claude`).
    pub command: String,
    /// Fixed args for the command.
    pub args: Vec<String>,
    /// Base env layered onto every polecat (`GT_ROLE`, `GT_RIG`, `GT_RIG_PATH`, …). The
    /// per-member `GT_HOOK_BEAD` and `GT_CONVOY` are added by [`SpawnTemplate::spec_for`].
    pub base_env: Vec<(String, String)>,
    /// Directory the heartbeat files live in (`<dir>/<session>.heartbeat`).
    pub heartbeat_dir: PathBuf,
}

impl SpawnTemplate {
    /// Build a production [`SpawnTemplate`] from the daemon's environment (`hq-orchd.3`), the
    /// gt-core counterpart of the env the upstream `gt` bin read (`GT_RIG`/`GT_RIG_PATH`/
    /// `GT_POLECAT_CMD`/…). `workspace` is layered into `base_env` as [`GT_WORKSPACE`] so every
    /// slung polecat carries its tenant for dashboard attribution (`hq-mt-runtime.5`).
    ///
    /// - `GT_RIG` → rig name (default `hq`); `GT_POLECAT_PREFIX` → session-name prefix
    ///   (default = the rig name).
    /// - `GT_RIG_PATH` → working directory (default `.`).
    /// - `GT_POLECAT_CMD` → agent command (default `claude`).
    /// - `GT_HEARTBEAT_DIR` → heartbeat directory (default the system temp dir).
    /// - `GT_CHANNEL_ROOT` → gt-channel mailbox root, layered into `base_env` when set so the
    ///   polecat's merge-ready hook knows where to drop its `{bead,branch}` message
    ///   (`hq-agent-provisioning.1`).
    /// - `GT_POLECAT_ARGS` → fixed command args (space-split). Unset ⇒ for a `claude` command,
    ///   defaults to `--dangerously-skip-permissions` so an autonomous polecat doesn't hang on the
    ///   interactive trust/permission prompt (`hq-agent-provisioning.6`); for any other command,
    ///   no args.
    pub fn from_env(workspace: &str) -> Self {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let rig = env("GT_RIG").unwrap_or_else(|| "hq".to_string());
        let prefix = env("GT_POLECAT_PREFIX").unwrap_or_else(|| rig.clone());
        let workdir = PathBuf::from(env("GT_RIG_PATH").unwrap_or_else(|| ".".to_string()));
        let command = env("GT_POLECAT_CMD").unwrap_or_else(|| "claude".to_string());
        let args = match env("GT_POLECAT_ARGS") {
            Some(s) => s.split_whitespace().map(String::from).collect(),
            None if command.ends_with("claude") => {
                vec!["--dangerously-skip-permissions".to_string()]
            }
            None => Vec::new(),
        };
        // Drop privileges (hq-quota-accounts.6): the daemon runs as root (it needs the root-owned
        // eventlog volume), but a polecat must NOT — it carries `--dangerously-skip-permissions`
        // and its account's claude creds, so a root session could read OTHER accounts' creds, the
        // RS256 signing key, and the whole volume. `GT_POLECAT_RUN_AS=<user>` re-execs the polecat
        // as a dedicated non-root uid via `runuser`. Unset ⇒ runs as the daemon's uid (legacy).
        let (command, args) = wrap_run_as(command, args, env("GT_POLECAT_RUN_AS").as_deref());
        let heartbeat_dir = env("GT_HEARTBEAT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let mut base_env = vec![
            ("GT_ROLE".to_string(), "polecat".to_string()),
            ("GT_RIG".to_string(), rig.clone()),
            ("GT_RIG_PATH".to_string(), workdir.display().to_string()),
            (GT_WORKSPACE.to_string(), workspace.to_string()),
        ];
        // Rig-wide channel root: the merge-ready hook resolves `$GT_CHANNEL_ROOT/merge-ready/`.
        // Only layered when set, so a template built without it stays unchanged.
        if let Some(channel_root) = env("GT_CHANNEL_ROOT") {
            base_env.push(("GT_CHANNEL_ROOT".to_string(), channel_root));
        }
        // Sandbox flag for `claude --dangerously-skip-permissions` (hq-orchd-deploy.12): claude
        // refuses that flag when running as root unless IS_SANDBOX is set. The containerized daemon
        // runs polecats as root (the container IS the isolation, GT_POLECAT_RUN_AS unset), so the
        // compose service declares IS_SANDBOX=1 and we propagate it to the polecat. Only layered
        // when set, so a host deploy that drops privileges via GT_POLECAT_RUN_AS stays unchanged.
        if let Some(sandbox) = env("IS_SANDBOX") {
            base_env.push(("IS_SANDBOX".to_string(), sandbox));
        }
        SpawnTemplate {
            rig,
            prefix,
            workdir,
            command,
            args,
            base_env,
            heartbeat_dir,
        }
    }

    /// Build the per-member [`SpawnSpec`]. `member` is the dispatched convoy member / slung
    /// bead: it becomes both the polecat-name suffix and the pinned `GT_HOOK_BEAD`. `convoy`
    /// is carried in `GT_CONVOY` for context (mirrors the Go `gt sling <convoy> <member>`
    /// positional args).
    pub fn spec_for(&self, convoy: &str, member: &str) -> SpawnSpec {
        let name = sanitize_name(member);
        let session = format!("{}-{}", self.prefix, name);
        let heartbeat = self.heartbeat_dir.join(format!("{session}.heartbeat"));
        let mut env = self.base_env.clone();
        env.push(("GT_CONVOY".to_string(), convoy.to_string()));
        // The branch the polecat works the bead on (CLAUDE.md convention: `-b <bead-id>`); its
        // merge-ready hook reports `{bead, branch}` so the refinery submits the right branch.
        env.push(("GT_BRANCH".to_string(), member.to_string()));
        // Seed the agent's task directive as the final positional arg (hq-agent-provisioning.6):
        // without it a `claude` polecat opens an idle TUI and never works the bead. The workspace
        // comes from base_env (GT_WORKSPACE); the branch is the bead id.
        let workspace = self
            .base_env
            .iter()
            .find(|(k, _)| k == GT_WORKSPACE)
            .map(|(_, v)| v.as_str())
            .unwrap_or("default");
        let mut args = self.args.clone();
        args.push(polecat_prompt(workspace, member, member));
        SpawnSpec {
            session,
            rig: self.rig.clone(),
            polecat: name,
            crew: None,
            workdir: self.workdir.clone(),
            command: self.command.clone(),
            args,
            env,
            hook_bead: Some(member.to_string()),
            issue: None,
            heartbeat,
        }
    }
}

/// The task directive seeded as the polecat agent's first prompt (`hq-agent-provisioning.6`).
///
/// A `claude` polecat launched bare opens an interactive TUI and idles — it never works its bead.
/// Passing this string as the command's positional `[prompt]` makes it work the bead autonomously:
/// its hooks then fire (PostToolUse → heartbeat; Stop → merge-ready). The agent reads the bead via
/// the `gt` MCP tools, authenticated by the `GT_TOKEN` the daemon minted into its env
/// (`hq-agent-provisioning.3`).
/// Wrap a polecat launch so it runs as a dedicated non-root user (`hq-quota-accounts.6`). When
/// `run_as` is `Some(user)`, the returned command is `runuser -u <user> -- <command> <args…>`, so
/// `tmux` (spawned by the root daemon) re-execs the polecat — and the claude it carries, with its
/// account creds — under that uid, away from the signing key and other accounts' dirs. `None`
/// leaves the command untouched (runs as the daemon's uid). Pure: it shells nothing, just rewrites
/// the argv, so the privilege boundary is testable without a host user.
pub fn wrap_run_as(
    command: String,
    args: Vec<String>,
    run_as: Option<&str>,
) -> (String, Vec<String>) {
    match run_as.map(str::trim).filter(|u| !u.is_empty()) {
        Some(user) => {
            let mut wrapped = vec!["-u".to_string(), user.to_string(), "--".to_string(), command];
            wrapped.extend(args);
            ("runuser".to_string(), wrapped)
        }
        None => (command, args),
    }
}

pub fn polecat_prompt(workspace: &str, bead: &str, branch: &str) -> String {
    format!(
        "You are a gt polecat in workspace `{workspace}`. Your assigned bead is `{bead}`. \
         Begin your duties per CLAUDE.md. Work autonomously and do not ask for confirmation. \
         When your work is committed on branch `{branch}`, signal completion by running EXACTLY \
         this Bash command once: \
         `d=\"$GT_CHANNEL_ROOT/merge-ready\"; mkdir -p \"$d\"; i=$(cat /proc/sys/kernel/random/uuid \
         2>/dev/null || date +%s%N); printf '{{\"bead\":\"%s\",\"branch\":\"%s\"}}' \"$GT_HOOK_BEAD\" \
         \"$GT_BRANCH\" > \"$d/.$i.tmp\" && mv \"$d/.$i.tmp\" \"$d/$i.event\"` \
         Then stop."
    )
}

/// Production polecat spawner — the Rust replacement for the Go `gt sling` subprocess
/// (hq-mc72.12 D1). Holds the tmux edge adapter plus the rig's [`SpawnTemplate`]; [`sling`]
/// turns a dispatched convoy member into a live tmux-backed polecat with the slung bead pinned
/// as `GT_HOOK_BEAD`. This is the single production caller `gt-polecat` was missing: the
/// composition root's `Effects::sling` delegates here instead of shelling out to the Go binary.
///
/// [`sling`]: PolecatLifecycle::sling
pub struct PolecatLifecycle {
    tmux: Box<dyn Tmux>,
    template: SpawnTemplate,
}

impl PolecatLifecycle {
    pub fn new(tmux: Box<dyn Tmux>, template: SpawnTemplate) -> Self {
        Self { tmux, template }
    }

    /// Spawn the production polecat for a dispatched convoy `member`. Returns the [`SpawnSpec`]
    /// that was launched so the caller can emit the matching `AgentEvent::Spawned`.
    pub fn sling(&self, convoy: &str, member: &str) -> io::Result<SpawnSpec> {
        let spec = self.template.spec_for(convoy, member);
        spawn_tmux(self.tmux.as_ref(), &spec)?;
        Ok(spec)
    }

    /// The spec that *would* be spawned for `(convoy, member)`, without launching it.
    pub fn spec_for(&self, convoy: &str, member: &str) -> SpawnSpec {
        self.template.spec_for(convoy, member)
    }
}

/// Sanitize a bead id / member into a tmux-safe name component: tmux session names may not
/// contain `.` or `:` and dislike whitespace. Maps any non-`[A-Za-z0-9_-]` byte to `-` so
/// `hq-abc.1` → `hq-abc-1`.
fn sanitize_name(member: &str) -> String {
    member
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect()
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::tmux::{FakeTmux, Tmux};
    use crate::GT_HOOK_BEAD;

    fn template() -> SpawnTemplate {
        SpawnTemplate {
            rig: "granite".to_string(),
            prefix: "gt".to_string(),
            workdir: std::env::temp_dir(),
            command: "claude".to_string(),
            args: vec!["--dangerously-skip-permissions".to_string()],
            base_env: vec![("GT_ROLE".to_string(), "polecat".to_string())],
            heartbeat_dir: std::env::temp_dir(),
        }
    }

    #[test]
    fn wrap_run_as_none_leaves_command_untouched() {
        let (cmd, args) = wrap_run_as(
            "claude".to_string(),
            vec!["--dangerously-skip-permissions".to_string()],
            None,
        );
        assert_eq!(cmd, "claude");
        assert_eq!(args, vec!["--dangerously-skip-permissions"]);
    }

    #[test]
    fn wrap_run_as_reexecs_under_the_user_via_runuser() {
        // hq-quota-accounts.6: runuser -u gtpolecat -- claude --flag … so the prompt spec_for
        // appends afterward stays the last positional handed to claude.
        let (cmd, args) = wrap_run_as(
            "claude".to_string(),
            vec!["--dangerously-skip-permissions".to_string()],
            Some("gtpolecat"),
        );
        assert_eq!(cmd, "runuser");
        assert_eq!(
            args,
            vec!["-u", "gtpolecat", "--", "claude", "--dangerously-skip-permissions"]
        );
    }

    #[test]
    fn wrap_run_as_empty_user_is_treated_as_unset() {
        let (cmd, _) = wrap_run_as("claude".to_string(), vec![], Some("  "));
        assert_eq!(cmd, "claude", "blank GT_POLECAT_RUN_AS must not wrap");
    }

    #[test]
    fn wrapped_template_keeps_the_bead_prompt_last() {
        // End-to-end through spec_for: a run-as wrapped template still feeds claude the bead prompt
        // as the final positional (runuser passes everything after `--` through).
        let (command, args) = wrap_run_as(
            "claude".to_string(),
            vec!["--dangerously-skip-permissions".to_string()],
            Some("gtpolecat"),
        );
        let t = SpawnTemplate {
            command,
            args,
            ..template()
        };
        let spec = t.spec_for("acme", "gg-1");
        assert_eq!(spec.command, "runuser");
        assert_eq!(spec.args.first().map(String::as_str), Some("-u"));
        assert!(
            spec.args.last().unwrap().contains("gg-1"),
            "the bead prompt is the final arg: {:?}",
            spec.args
        );
        // The user + the real command sit between the runuser flags and the prompt.
        assert!(spec.args.iter().any(|a| a == "claude"));
        assert!(spec.args.iter().any(|a| a == "gtpolecat"));
    }

    #[test]
    fn spec_for_derives_session_pins_hook_and_carries_convoy() {
        let spec = template().spec_for("cv-1", "hq-abc.1");
        // `.` is not tmux-safe → sanitized to `-`.
        assert_eq!(spec.session, "gt-hq-abc-1");
        assert_eq!(spec.polecat, "hq-abc-1");
        assert_eq!(spec.rig, "granite");
        assert_eq!(spec.hook_bead.as_deref(), Some("hq-abc.1"));
        // GT_HOOK_BEAD is appended via env_with_hook, not base env.
        assert!(spec.env.iter().any(|(k, v)| k == "GT_ROLE" && v == "polecat"));
        assert!(spec.env.iter().any(|(k, v)| k == "GT_CONVOY" && v == "cv-1"));
        let full = spec.env_with_hook();
        assert!(full.iter().any(|(k, v)| k == GT_HOOK_BEAD && v == "hq-abc.1"));
    }

    #[test]
    fn spec_seeds_the_bead_task_prompt_after_the_fixed_flags() {
        // hq-agent-provisioning.6: the per-bead task directive is the LAST positional arg, after the
        // template's fixed flags — so a `claude` polecat works the bead instead of idling.
        let spec = template().spec_for("cv-1", "hq-abc.1");
        assert!(
            spec.args
                .iter()
                .any(|a| a == "--dangerously-skip-permissions"),
            "fixed launch flag preserved"
        );
        let prompt = spec.args.last().expect("a task prompt is appended");
        assert!(prompt.contains("hq-abc.1"), "prompt names the bead");
        assert!(prompt.contains("autonomous"), "prompt directs autonomy");
    }

    #[test]
    fn polecat_prompt_names_workspace_bead_and_branch() {
        let p = polecat_prompt("acme", "hq-9.2", "feat/x");
        assert!(p.contains("acme") && p.contains("hq-9.2") && p.contains("feat/x"));
    }

    #[test]
    fn spawn_tmux_exports_heartbeat_file_branch_and_channel_root() {
        // The reporting env a polecat's hooks need (hq-agent-provisioning.1): GT_HEARTBEAT_FILE
        // (the touch target), GT_BRANCH (the merge-ready branch), GT_CHANNEL_ROOT (mailbox root).
        // All ride the new-session `-e` env, so `tmux show-environment` resolves each.
        let mut tpl = template();
        tpl.base_env
            .push(("GT_CHANNEL_ROOT".to_string(), "/gt/.channels".to_string()));
        let spec = tpl.spec_for("cv-1", "hq-abc.1");
        let probe = FakeTmux::new();
        spawn_tmux(&probe, &spec).unwrap();
        // GT_BRANCH is the raw bead id (the branch), not the sanitized session name.
        assert_eq!(
            probe.show_environment(&spec.session, "GT_BRANCH").unwrap().as_deref(),
            Some("hq-abc.1")
        );
        assert_eq!(
            probe
                .show_environment(&spec.session, "GT_CHANNEL_ROOT")
                .unwrap()
                .as_deref(),
            Some("/gt/.channels")
        );
        assert_eq!(
            probe.show_environment(&spec.session, "GT_HEARTBEAT_FILE").unwrap(),
            Some(spec.heartbeat.display().to_string())
        );
    }

    #[test]
    fn sling_creates_session_with_hook_pinned() {
        let tmux = FakeTmux::new();
        let lifecycle = PolecatLifecycle::new(Box::new(tmux), template());
        let spec = lifecycle.sling("cv-9", "hq-9").unwrap();
        assert_eq!(spec.session, "gt-hq-9");
        // The FakeTmux is moved into the lifecycle; spawn via a fresh adapter to assert env,
        // so re-build the same spec against a standalone fake and check the pin path.
        let probe = FakeTmux::new();
        spawn_tmux(&probe, &spec).unwrap();
        assert_eq!(
            probe.show_environment("gt-hq-9", GT_HOOK_BEAD).unwrap().as_deref(),
            Some("hq-9")
        );
        assert_eq!(
            probe.show_environment("gt-hq-9", "GT_CONVOY").unwrap().as_deref(),
            Some("cv-9")
        );
    }

    #[test]
    fn spawn_tmux_pins_workspace_alongside_hook() {
        // A workspace-tagged base env: the composition root layers GT_WORKSPACE onto base_env.
        let mut tmpl = template();
        tmpl.base_env.push((GT_WORKSPACE.to_string(), "acme".to_string()));
        let spec = tmpl.spec_for("cv-7", "hq-7");
        let probe = FakeTmux::new();
        spawn_tmux(&probe, &spec).unwrap();
        // Both attribution vars resolve via show-environment for dashboard attribution.
        assert_eq!(
            probe.show_environment("gt-hq-7", GT_WORKSPACE).unwrap().as_deref(),
            Some("acme")
        );
        assert_eq!(
            probe.show_environment("gt-hq-7", GT_HOOK_BEAD).unwrap().as_deref(),
            Some("hq-7")
        );
    }

    #[test]
    fn spawn_tmux_skips_workspace_pin_when_absent() {
        // No GT_WORKSPACE in base env → the pin is simply skipped (no spurious empty var).
        let spec = template().spec_for("cv-1", "hq-1");
        let probe = FakeTmux::new();
        spawn_tmux(&probe, &spec).unwrap();
        assert!(probe.show_environment("gt-hq-1", GT_WORKSPACE).unwrap().is_none());
    }
}
