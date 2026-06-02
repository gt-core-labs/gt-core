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

use crate::hooks::hook_env;
use crate::tmux::Tmux;

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
    /// Full env including the `GT_HOOK_BEAD` pin (if any). Keys already present in `env` are
    /// kept; the hook entry is appended.
    pub fn env_with_hook(&self) -> Vec<(String, String)> {
        let mut env = self.env.clone();
        if let Some(entry) = hook_env(self.hook_bead.as_deref(), self.issue.as_deref()) {
            env.push(entry);
        }
        env
    }

    /// The `AgentEvent::Spawned` fact for this polecat (role always polecat; crew carried).
    pub fn spawned_event(&self) -> AgentEvent {
        AgentEvent::Spawned {
            session: self.session.clone(),
            rig: self.rig.clone(),
            role: SessionRole::Polecat,
            crew: self.crew.clone(),
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

/// Create the production tmux-backed polecat session, pinning `GT_HOOK_BEAD` both at creation
/// (via `-e`) and again with an explicit `set-environment` — mirrors the Go session_manager
/// belt-and-suspenders so `show-environment` resolves the bead regardless of which path a
/// reader trusts.
pub fn spawn_tmux(tmux: &dyn Tmux, spec: &SpawnSpec) -> io::Result<()> {
    let env = spec.env_with_hook();
    tmux.new_session(&spec.session, &spec.workdir, &spec.command, &spec.args, &env)?;
    if let Some((key, value)) = hook_env(spec.hook_bead.as_deref(), spec.issue.as_deref()) {
        tmux.set_environment(&spec.session, &key, &value)?;
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
        SpawnSpec {
            session,
            rig: self.rig.clone(),
            polecat: name,
            crew: None,
            workdir: self.workdir.clone(),
            command: self.command.clone(),
            args: self.args.clone(),
            env,
            hook_bead: Some(member.to_string()),
            issue: None,
            heartbeat,
        }
    }
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
}
