//! Tmux edge adapter behind a [`Tmux`] port.
//!
//! Production polecats are a coding agent running inside a detached tmux session; the
//! supervisor needs to create that session with the right environment (notably
//! [`crate::GT_HOOK_BEAD`]) and to read it back. The domain depends only on the [`Tmux`]
//! trait; [`TmuxCli`] shells out to the real `tmux` binary (port of `internal/tmux`), and
//! [`FakeTmux`] is an in-memory double so the spawn/hook logic is testable without a tmux
//! server.
//!
//! Methods are synchronous: each is a single short-lived `tmux` invocation. Callers on the
//! async edge keep them off the hot path (one call per spawn, not per tick).

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default per-invocation timeout. A `tmux` call should return in milliseconds; anything
/// slower means a wedged server (lock contention, split-brain) and we'd rather fail than hang
/// the supervisor.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default retry count for idempotent reads/teardown (total attempts = retries + 1).
const DEFAULT_RETRIES: u32 = 2;
/// Default delay between retries.
const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// The session-management surface the lifecycle needs. Implemented by [`TmuxCli`] (real) and
/// [`FakeTmux`] (tests).
pub trait Tmux: Send + Sync {
    /// Create a detached session named `session` running `command args…` in `workdir`, with
    /// `env` injected before the command starts (so the agent and its `bd` subprocesses
    /// inherit it from the start — the `-e`-flags path in the Go adapter).
    fn new_session(
        &self,
        session: &str,
        workdir: &Path,
        command: &str,
        args: &[String],
        env: &[(String, String)],
    ) -> io::Result<()>;

    /// Set a single session-level environment variable after creation.
    fn set_environment(&self, session: &str, key: &str, value: &str) -> io::Result<()>;

    /// Read a session-level environment variable back. `None` when unset.
    fn show_environment(&self, session: &str, key: &str) -> io::Result<Option<String>>;

    fn has_session(&self, session: &str) -> bool;

    fn kill_session(&self, session: &str) -> io::Result<()>;

    /// Send a tmux key chord to the session's active pane (`tmux send-keys -t <session>
    /// <keys...>`). Used by the operator e-stop chain: an `Escape` interrupt cancels the
    /// coding agent's in-flight turn without killing the polecat. `keys` is the verbatim
    /// argument list tmux expects (e.g. `&["Escape"]` or `&["C-c"]`); the adapter does
    /// not encode literals — callers stay in tmux's key-name vocabulary.
    fn send_keys(&self, session: &str, keys: &[&str]) -> io::Result<()>;

    /// Capture the last lines of a session's active pane (`tmux capture-pane -p -t <session>`).
    /// The supervisor reads this when a polecat dies to look for claude's `N% context used`
    /// marker and tell a context-exhaustion death apart from a clean exit (gtcore-91fdde).
    /// Returns `None` when the session is gone or the read fails — a missing capture is
    /// non-fatal (the caller falls back to a plain exit).
    fn capture_pane(&self, session: &str) -> Option<String>;

    /// Suspend the session **in place** with `SIGSTOP` to every pane's process group — the
    /// pause-in-place primitive (gtcore-5731e9) the quota subsystem uses to freeze in-flight
    /// polecats when every account is exhausted instead of letting them die against the rate
    /// limit (gtcore-6f449f). The coding agent stops with its context intact; [`Self::resume`]
    /// (`SIGCONT`) thaws it. Idempotent at the OS level: `SIGSTOP` on an already-stopped process
    /// is a no-op.
    fn pause(&self, session: &str) -> io::Result<()>;

    /// Resume a session suspended by [`Self::pause`] with `SIGCONT` to every pane's process
    /// group (gtcore-6f449f). The coding agent picks up exactly where it was frozen.
    fn resume(&self, session: &str) -> io::Result<()>;
}

/// Conservative bound on the assembled inline `respawn-pane … env … command args…` command, in
/// bytes (gtcore-408279). tmux rejects an over-long command with `command too long` (its imsg
/// frame is 16 KiB, shared with framing overhead); a CI-fix kickoff prompt embedding a diff + CI
/// log routinely blows past it. Above this bound the real command is written to a launch script
/// and the pane runs `sh <script>` instead — bounded, regardless of prompt size.
const MAX_INLINE_COMMAND_BYTES: usize = 8192;

/// How the pane's real command is handed to `respawn-pane` (gtcore-408279): inline argv when it
/// fits tmux's command-size budget, else the content of a launch script the adapter writes and
/// runs via `sh`. Decided by [`spawn_invocation`] — pure, so the size policy and the script
/// quoting are unit-tested without tmux.
#[derive(Debug, PartialEq, Eq)]
enum SpawnInvocation {
    /// `env K=V … command args…`, appended verbatim to the `respawn-pane` argv.
    Inline(Vec<String>),
    /// The `#!/bin/sh` launch script replacing the inline command: `exec env 'K=V' … 'command'
    /// 'args'…` with every word single-quoted, so env-in-process semantics (`GT_HOOK_BEAD` in
    /// the agent's environ) are identical to the inline form.
    Script { content: String },
}

/// POSIX-shell single-quote `s` (embedded `'` becomes `'\''`), so arbitrary prompt bytes —
/// newlines, quotes, `$`, backticks — pass through the launch script verbatim.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Build the pane invocation for `respawn-pane`: inline when the assembled words fit
/// `max_inline` bytes, else a launch script carrying the same env prefix + command + args.
fn spawn_invocation(
    pairs: &[(String, String)],
    command: &str,
    args: &[String],
    max_inline: usize,
) -> SpawnInvocation {
    let mut inline: Vec<String> = vec!["env".into()];
    for (k, v) in pairs {
        inline.push(format!("{k}={v}"));
    }
    inline.push(command.to_string());
    inline.extend(args.iter().cloned());
    // +1 per word approximates the argv separators in tmux's assembled command.
    let total: usize = inline.iter().map(|w| w.len() + 1).sum();
    if total <= max_inline {
        return SpawnInvocation::Inline(inline);
    }
    let mut content = String::from(
        "#!/bin/sh\n# generated launch script (gtcore-408279): the inline respawn command exceeded tmux's size limit\nexec env",
    );
    for (k, v) in pairs {
        content.push(' ');
        content.push_str(&shell_quote(&format!("{k}={v}")));
    }
    content.push(' ');
    content.push_str(&shell_quote(command));
    for a in args {
        content.push(' ');
        content.push_str(&shell_quote(a));
    }
    content.push('\n');
    SpawnInvocation::Script { content }
}

/// Where a session's launch script lives: an adapter-owned scratch dir, NEVER the workdir — the
/// workdir is the polecat's git worktree, and a stray script there would dirty the tree the
/// agent commits from.
fn launch_script_path(session: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("gt-spawn").join(format!("{session}.sh"))
}

/// Whether a spawn error is PERMANENT — retrying the identical spawn cannot succeed, so a retry
/// loop must abandon instead of hot-looping forever (gtcore-408279; observed live: a supervisor
/// retrying `command too long` every tick for days). Matched on the rendered message because the
/// error crosses the [`Tmux`] trait as a plain [`io::Error`]:
///
/// - `command too long` — the assembled command exceeds tmux's size limit (same input ⇒ same
///   result; also defense-in-depth should the launch-script path be bypassed).
/// - `No such file or directory` — the agent binary or the workdir does not exist.
/// - `not a directory` — the workdir path resolves to a file.
///
/// Everything else (timeouts, a busy server, a duplicate session) stays transient — today's
/// retry behaviour.
pub fn spawn_error_is_permanent(e: &io::Error) -> bool {
    let msg = e.to_string();
    msg.contains("command too long")
        || msg.contains("No such file or directory")
        || msg.contains("not a directory")
}

/// Real adapter: shells out to the `tmux` binary. Mirrors the flag shape of
/// `internal/tmux/tmux.go` (`new-session -d -s … -c … -e KEY=VAL …` then `respawn-pane`).
pub struct TmuxCli {
    bin: String,
    /// Optional `-L <socket>` server socket. Lets a caller (notably tests) run against a
    /// private tmux server instead of the shared default — never disturbing live sessions.
    socket: Option<String>,
    /// Per-invocation wall-clock budget; a slower call is killed and reported as `TimedOut`.
    timeout: Duration,
    /// Extra attempts for *idempotent* operations (reads + teardown). Spawn-time creation is
    /// never retried — re-running it could orphan or duplicate sessions.
    retries: u32,
    retry_delay: Duration,
}

/// The tmux server socket name for a workspace: `gt-<workspace>` (`hq-mt-runtime.1`).
///
/// One home for the convention so the lifecycle, runtime, and any e-stop / capacity code
/// (`hq-mt-runtime.2/.9`) derive the same socket from a workspace slug.
pub fn tmux_server_name(workspace: &str) -> String {
    format!("gt-{workspace}")
}

impl TmuxCli {
    pub fn new() -> Self {
        Self {
            bin: "tmux".to_string(),
            socket: None,
            timeout: DEFAULT_TIMEOUT,
            retries: DEFAULT_RETRIES,
            retry_delay: DEFAULT_RETRY_DELAY,
        }
    }

    /// Use a non-default tmux binary/path (kept for parity with deployments that pin it).
    pub fn with_bin(bin: impl Into<String>) -> Self {
        Self {
            bin: bin.into(),
            ..Self::new()
        }
    }

    /// A `TmuxCli` pinned to a workspace's own tmux server (`tmux -L gt-<workspace>`), so every
    /// polecat slung for that workspace lives on a per-tenant server instead of the single
    /// shared default (`hq-mt-runtime.1`). `workspace` is the workspace slug — a plain string at
    /// this boundary, so the lifecycle tier never has to depend on the platform `gt-workspace`
    /// type (the composition root, which holds the `WorkspaceId`, formats it). The server is
    /// created lazily by tmux on the first session; capping the per-host server count is
    /// `hq-mt-runtime.2`.
    pub fn for_workspace(workspace: &str) -> Self {
        Self::new().with_socket(tmux_server_name(workspace))
    }

    /// Pin a private server socket (`tmux -L <socket>`). Isolation for tests and for
    /// deployments that segregate tmux servers per role.
    pub fn with_socket(mut self, socket: impl Into<String>) -> Self {
        self.socket = Some(socket.into());
        self
    }

    /// The `-L` server socket this adapter targets, or `None` for the shared default server.
    pub fn socket(&self) -> Option<&str> {
        self.socket.as_deref()
    }

    /// Override the per-invocation timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Override retry count (idempotent ops only) and the delay between attempts.
    pub fn with_retries(mut self, retries: u32, delay: Duration) -> Self {
        self.retries = retries;
        self.retry_delay = delay;
        self
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        if let Some(socket) = &self.socket {
            cmd.arg("-L").arg(socket);
        }
        cmd
    }

    /// Spawn `tmux <args>`, capture stdout/stderr, and enforce [`Self::timeout`]. On timeout
    /// the child is killed and reaped, and a `TimedOut` error returned. std-only (no extra
    /// dep): poll `try_wait` since our tmux outputs are tiny (env lines), so reading after
    /// exit cannot deadlock on a full pipe.
    fn capture_once(&self, args: &[&str]) -> io::Result<Output> {
        let mut child = self
            .command()
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let deadline = Instant::now() + self.timeout;
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "tmux {} timed out after {:?}",
                        args.first().copied().unwrap_or(""),
                        self.timeout
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.wait_with_output()
    }

    /// Run an idempotent command, retrying on transient failure (spawn error, timeout, or a
    /// non-zero exit). Returns stdout on the first success.
    fn run_retry(&self, args: &[&str]) -> io::Result<String> {
        let mut last: Option<io::Error> = None;
        for attempt in 0..=self.retries {
            match self.run_checked(args) {
                Ok(s) => return Ok(s),
                Err(e) => {
                    last = Some(e);
                    if attempt < self.retries {
                        std::thread::sleep(self.retry_delay);
                    }
                }
            }
        }
        Err(last.unwrap_or_else(|| io::Error::other("tmux: no attempt made")))
    }

    /// One attempt: capture, fail on non-zero exit, return stdout. Used directly for the
    /// non-idempotent creation path (no retry) and as the per-attempt body of [`run_retry`].
    fn run_checked(&self, args: &[&str]) -> io::Result<String> {
        let out = self.capture_once(args)?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "tmux {} failed: {}",
                args.first().copied().unwrap_or(""),
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

impl Default for TmuxCli {
    fn default() -> Self {
        Self::new()
    }
}

impl Tmux for TmuxCli {
    fn new_session(
        &self,
        session: &str,
        workdir: &Path,
        command: &str,
        args: &[String],
        env: &[(String, String)],
    ) -> io::Result<()> {
        let workdir = workdir.to_string_lossy().into_owned();
        let mut argv: Vec<String> =
            vec!["new-session".into(), "-d".into(), "-s".into(), session.into()];
        argv.push("-c".into());
        argv.push(workdir.clone());
        // Sort env keys for deterministic invocations (matches the Go adapter).
        let mut pairs = env.to_vec();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        for (k, v) in &pairs {
            argv.push("-e".into());
            argv.push(format!("{k}={v}"));
        }
        let argv_ref: Vec<&str> = argv.iter().map(String::as_str).collect();
        // Creation is NOT retried — a re-run could leave a half-built or duplicate session.
        self.run_checked(&argv_ref)?;

        // Replace the placeholder shell with the real command in the same workdir. tmux's per-session
        // `-e` (and `respawn-pane -e`) only populate tmux's *session environment* (what
        // `show-environment` reads) — they do NOT end up in the spawned process's own environ, so the
        // polecat's claude never saw GT_HOOK_BEAD / GT_HEARTBEAT_FILE / GT_ROLE / GT_BRANCH and its
        // hooks no-op'd on the empty `[ -n "$VAR" ]` guards. Prefix the real command with `env
        // KEY=VAL …` so the process is launched with the variables in its actual environment
        // (hq-orchd-deploy.18). The `-e` on new-session is kept above so `show-environment` still
        // resolves the attribution keys the supervisor reads.
        let mut respawn: Vec<String> = vec![
            "respawn-pane".into(),
            "-k".into(),
            "-t".into(),
            session.into(),
            "-c".into(),
            workdir,
        ];
        // gtcore-408279: an over-long inline command (huge kickoff prompt) makes tmux fail with
        // `command too long` — permanently, every retry. Above the size budget the same env +
        // command + args go into a launch script and the pane runs `sh <script>`: identical
        // process semantics (exec env … command), bounded tmux command.
        match spawn_invocation(&pairs, command, args, MAX_INLINE_COMMAND_BYTES) {
            SpawnInvocation::Inline(words) => respawn.extend(words),
            SpawnInvocation::Script { content } => {
                let path = launch_script_path(session);
                let write = path
                    .parent()
                    .map(std::fs::create_dir_all)
                    .unwrap_or(Ok(()))
                    .and_then(|()| std::fs::write(&path, content));
                if let Err(e) = write {
                    let _ = self.kill_session(session);
                    return Err(io::Error::new(
                        e.kind(),
                        format!("write launch script {}: {e}", path.display()),
                    ));
                }
                respawn.push("sh".into());
                respawn.push(path.to_string_lossy().into_owned());
            }
        }
        let respawn_ref: Vec<&str> = respawn.iter().map(String::as_str).collect();
        if let Err(e) = self.run_checked(&respawn_ref) {
            let _ = self.kill_session(session);
            return Err(e);
        }
        Ok(())
    }

    fn set_environment(&self, session: &str, key: &str, value: &str) -> io::Result<()> {
        self.run_retry(&["set-environment", "-t", session, key, value])?;
        Ok(())
    }

    fn show_environment(&self, session: &str, key: &str) -> io::Result<Option<String>> {
        let out = self.run_retry(&["show-environment", "-t", session, key])?;
        Ok(parse_show_environment(&out, key))
    }

    fn has_session(&self, session: &str) -> bool {
        // `has-session` exits non-zero when the session is simply absent — that is a valid
        // answer, NOT a transient failure. So retry only on a real error (spawn/timeout) and
        // map a clean exit to its boolean. Give up as `false` if every attempt errored.
        for attempt in 0..=self.retries {
            match self.capture_once(&["has-session", "-t", session]) {
                Ok(out) => return out.status.success(),
                Err(_) if attempt < self.retries => std::thread::sleep(self.retry_delay),
                Err(_) => return false,
            }
        }
        false
    }

    fn kill_session(&self, session: &str) -> io::Result<()> {
        self.run_retry(&["kill-session", "-t", session])?;
        Ok(())
    }

    fn send_keys(&self, session: &str, keys: &[&str]) -> io::Result<()> {
        // `send-keys` is not retried: a retry could double-deliver an `Escape` /  C-c
        // and the agent has no way to dedupe. A wedged tmux server surfaces the spawn
        // error to the caller (HTTP 500), same posture as `new-session`.
        let mut argv: Vec<&str> = vec!["send-keys", "-t", session];
        argv.extend_from_slice(keys);
        self.run_checked(&argv)?;
        Ok(())
    }

    fn capture_pane(&self, session: &str) -> Option<String> {
        // Idempotent read → retry on transient failure (same posture as show-environment).
        // `-p` prints to stdout; `-S -<N>` reaches back N lines of scrollback so the context
        // marker is captured even if a shell prompt scrolled it off the visible pane. A gone
        // session / wedged server surfaces as `Err` → `None` (a missing capture is non-fatal).
        let start = format!("-{PANE_CAPTURE_LINES}");
        self.run_retry(&["capture-pane", "-p", "-t", session, "-S", &start])
            .ok()
    }

    fn pause(&self, session: &str) -> io::Result<()> {
        self.signal_panes(session, libc::SIGSTOP)
    }

    fn resume(&self, session: &str) -> io::Result<()> {
        self.signal_panes(session, libc::SIGCONT)
    }
}

impl TmuxCli {
    /// Send `signal` (`SIGSTOP`/`SIGCONT`) to the process group of every pane in `session` — the
    /// pause-in-place primitive (gtcore-5731e9), ported from the agent HTTP surface so the polecat
    /// supervisor can freeze/thaw the tmux polecats it owns (gtcore-6f449f).
    ///
    /// tmux runs each pane's command as its own session/process-group leader, so `#{pane_pid}` is
    /// the group id; signalling the **negative** pid reaches the shell *and* the coding agent (and
    /// any `bd`/tool subprocesses) it spawned, so `SIGSTOP` freezes the whole turn and `SIGCONT`
    /// thaws it. A gone session (`list-panes` exits non-zero) is `Ok(())` — there is nothing to
    /// signal, and the caller treats a vanished polecat as normal supervision territory.
    fn signal_panes(&self, session: &str, signal: libc::c_int) -> io::Result<()> {
        // `list-panes` exiting non-zero means the session is simply absent — not a transient
        // failure to retry. `run_retry` would burn its budget on a gone session, so use a single
        // capture and map a non-zero exit to "nothing to signal".
        let out = self.capture_once(&["list-panes", "-t", session, "-F", "#{pane_pid}"])?;
        if !out.status.success() {
            return Ok(());
        }
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Ok(pid) = line.trim().parse::<i32>() {
                if pid > 0 {
                    // Negative pid → the pane's whole process group (SAFETY: `kill(2)` is a plain
                    // syscall with no memory effects; the return is ignored — a reaped pane is fine).
                    unsafe {
                        libc::kill(-pid, signal);
                    }
                }
            }
        }
        Ok(())
    }
}

/// How many lines of pane scrollback [`Tmux::capture_pane`] reaches back for. Enough to keep
/// claude's `N% context used` status line even when a post-exit shell prompt has scrolled it up.
const PANE_CAPTURE_LINES: u32 = 200;

/// Parse `tmux show-environment -t <s> <key>` output. tmux prints `KEY=value` when set and
/// `-KEY` (leading dash) when explicitly unset; anything else → not present.
///
/// Hardened against: a trailing `\r` (CRLF), an empty value (`KEY=`), and a value that itself
/// contains `=` (`KEY=a=b`). The key boundary is exact — the `=` immediately after the full
/// key — so a prefix key never matches a longer variable.
fn parse_show_environment(out: &str, key: &str) -> Option<String> {
    for raw in out.lines() {
        let line = raw.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix(key) {
            if let Some(value) = rest.strip_prefix('=') {
                return Some(value.to_string());
            }
        }
        if line == format!("-{key}") {
            return None;
        }
    }
    None
}

/// In-memory [`Tmux`] for tests: records sessions and their env without a tmux server.
#[derive(Default)]
pub struct FakeTmux {
    sessions: Mutex<HashMap<String, HashMap<String, String>>>,
    /// Canned pane contents returned by [`Tmux::capture_pane`], seeded via [`FakeTmux::set_pane`].
    panes: Mutex<HashMap<String, String>>,
    /// Sessions currently SIGSTOP'd via [`Tmux::pause`] (gtcore-6f449f). In-memory stand-in for
    /// the real `SIGSTOP`/`SIGCONT` so gates can assert the supervisor froze/thawed the right set.
    paused: Mutex<std::collections::HashSet<String>>,
    /// Names passed to [`Tmux::kill_session`], in call order. Lets gates assert a teardown happened
    /// (e.g. the CI-failure re-sling killing a still-live session before respawning, gtcore-8701c4).
    kills: Mutex<Vec<String>>,
    /// When set, every [`Tmux::new_session`] fails with this message (gtcore-408279) — lets a
    /// gate exercise the permanent-spawn-failure abandon path without a real tmux server.
    new_session_error: Mutex<Option<String>>,
}

impl FakeTmux {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the pane text [`Tmux::capture_pane`] will return for `session` — lets a test stand
    /// in claude's `N% context used` status line without a real tmux server (gtcore-91fdde).
    pub fn set_pane(&self, session: &str, contents: &str) {
        self.panes
            .lock()
            .unwrap()
            .insert(session.to_string(), contents.to_string());
    }

    /// Whether `session` is currently paused (gtcore-6f449f) — test observability for the
    /// pause/resume primitive.
    pub fn is_paused(&self, session: &str) -> bool {
        self.paused.lock().unwrap().contains(session)
    }

    /// Make every subsequent [`Tmux::new_session`] fail with `msg` (gtcore-408279) — stands in
    /// for a real adapter failure like `tmux respawn-pane failed: command too long`.
    pub fn fail_new_session_with(&self, msg: &str) {
        *self.new_session_error.lock().unwrap() = Some(msg.to_string());
    }

    /// Session names handed to [`Tmux::kill_session`], in call order — test observability for
    /// teardown paths (gtcore-8701c4).
    pub fn kills(&self) -> Vec<String> {
        self.kills.lock().unwrap().clone()
    }
}

impl Tmux for FakeTmux {
    fn new_session(
        &self,
        session: &str,
        _workdir: &Path,
        _command: &str,
        _args: &[String],
        env: &[(String, String)],
    ) -> io::Result<()> {
        if let Some(msg) = self.new_session_error.lock().unwrap().as_ref() {
            return Err(io::Error::other(msg.clone()));
        }
        let mut map = self.sessions.lock().unwrap();
        let entry = map.entry(session.to_string()).or_default();
        for (k, v) in env {
            entry.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    fn set_environment(&self, session: &str, key: &str, value: &str) -> io::Result<()> {
        let mut map = self.sessions.lock().unwrap();
        map.entry(session.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn show_environment(&self, session: &str, key: &str) -> io::Result<Option<String>> {
        let map = self.sessions.lock().unwrap();
        Ok(map.get(session).and_then(|e| e.get(key).cloned()))
    }

    fn has_session(&self, session: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(session)
    }

    fn kill_session(&self, session: &str) -> io::Result<()> {
        self.kills.lock().unwrap().push(session.to_string());
        self.sessions.lock().unwrap().remove(session);
        Ok(())
    }

    fn send_keys(&self, session: &str, keys: &[&str]) -> io::Result<()> {
        // Record the chord under a synthetic env key so gates can assert "the route sent
        // this exact key sequence". A real `Escape` doesn't change tmux env, but the fake
        // is in-memory: we only need a deterministic place to read it back from.
        let value = keys.join(" ");
        let mut map = self.sessions.lock().unwrap();
        let entry = map.entry(session.to_string()).or_default();
        entry.insert("__SEND_KEYS__".to_string(), value);
        Ok(())
    }

    fn capture_pane(&self, session: &str) -> Option<String> {
        self.panes.lock().unwrap().get(session).cloned()
    }

    fn pause(&self, session: &str) -> io::Result<()> {
        // A gone session is a no-op (mirrors the real adapter: nothing to signal).
        if self.sessions.lock().unwrap().contains_key(session) {
            self.paused.lock().unwrap().insert(session.to_string());
        }
        Ok(())
    }

    fn resume(&self, session: &str) -> io::Result<()> {
        self.paused.lock().unwrap().remove(session);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_is_gt_dash_workspace() {
        assert_eq!(tmux_server_name("acme"), "gt-acme");
        assert_eq!(tmux_server_name("default"), "gt-default");
    }

    #[test]
    fn for_workspace_pins_the_per_ws_socket() {
        let cli = TmuxCli::for_workspace("acme");
        assert_eq!(cli.socket(), Some("gt-acme"));
    }

    #[test]
    fn new_targets_the_shared_default_server() {
        assert_eq!(TmuxCli::new().socket(), None);
    }

    #[test]
    fn parse_set_and_unset() {
        assert_eq!(
            parse_show_environment("GT_HOOK_BEAD=hq-9\n", "GT_HOOK_BEAD").as_deref(),
            Some("hq-9")
        );
        assert!(parse_show_environment("-GT_HOOK_BEAD\n", "GT_HOOK_BEAD").is_none());
        assert!(parse_show_environment("OTHER=1\n", "GT_HOOK_BEAD").is_none());
    }

    #[test]
    fn parse_handles_crlf_empty_and_embedded_equals() {
        // CRLF trailing carriage return.
        assert_eq!(
            parse_show_environment("GT_HOOK_BEAD=hq-9\r\n", "GT_HOOK_BEAD").as_deref(),
            Some("hq-9")
        );
        // Empty value is a real "set to empty", not absent.
        assert_eq!(
            parse_show_environment("GT_HOOK_BEAD=\n", "GT_HOOK_BEAD").as_deref(),
            Some("")
        );
        // Value containing '=' survives intact.
        assert_eq!(
            parse_show_environment("GT_HOOK_BEAD=a=b=c\n", "GT_HOOK_BEAD").as_deref(),
            Some("a=b=c")
        );
    }

    #[test]
    fn parse_key_boundary_is_exact() {
        // A prefix key must not match a longer variable name.
        assert!(parse_show_environment("GT_HOOK_BEAD_X=1\n", "GT_HOOK_BEAD").is_none());
        // The right key still resolves even when a similar one precedes it.
        assert_eq!(
            parse_show_environment("GT_HOOK_BEAD_X=1\nGT_HOOK_BEAD=hq-2\n", "GT_HOOK_BEAD")
                .as_deref(),
            Some("hq-2")
        );
    }

    #[test]
    fn cli_times_out_on_a_hanging_command() {
        // `sleep 30` stands in for a wedged tmux: capture_once must kill it and return
        // TimedOut well under the sleep, not block.
        let cli = TmuxCli::with_bin("sleep")
            .with_timeout(Duration::from_millis(150))
            .with_retries(0, Duration::from_millis(0));
        let start = Instant::now();
        let err = cli.capture_once(&["30"]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < Duration::from_secs(2), "killed promptly");
    }

    #[test]
    fn fake_capture_pane_roundtrips_seeded_contents() {
        let t = FakeTmux::new();
        // Unseeded session → None (mirrors a gone session / failed read on the real adapter).
        assert!(t.capture_pane("s1").is_none());
        t.set_pane("s1", "⏵ 88% context used");
        assert_eq!(t.capture_pane("s1").as_deref(), Some("⏵ 88% context used"));
    }

    #[test]
    fn small_command_stays_inline_oversized_goes_to_script() {
        // gtcore-408279: the size policy. A normal kickoff stays the inline `env … command args`
        // form; a prompt past the tmux budget moves the WHOLE invocation into a launch script.
        let pairs = vec![("GT_HOOK_BEAD".to_string(), "hq-1".to_string())];
        let small = spawn_invocation(&pairs, "claude", &["hola".to_string()], 8192);
        assert_eq!(
            small,
            SpawnInvocation::Inline(vec![
                "env".into(),
                "GT_HOOK_BEAD=hq-1".into(),
                "claude".into(),
                "hola".into(),
            ])
        );

        let huge_prompt = "x".repeat(20_000);
        let big = spawn_invocation(&pairs, "claude", &[huge_prompt.clone()], 8192);
        let SpawnInvocation::Script { content } = big else {
            panic!("oversized prompt must produce a launch script");
        };
        // Same env-in-process semantics: exec env K=V … command 'prompt'.
        assert!(content.starts_with("#!/bin/sh\n"));
        assert!(content.contains("exec env 'GT_HOOK_BEAD=hq-1' 'claude'"));
        assert!(content.contains(&huge_prompt), "the full prompt travels in the script");
    }

    #[test]
    fn script_quoting_survives_hostile_prompt_bytes() {
        // Quotes, `$`, backticks and newlines in a prompt must reach the agent verbatim: every
        // word is single-quoted, embedded single quotes escaped as '\'' .
        let pairs: Vec<(String, String)> = vec![];
        let hostile = format!("it's a $HOME `test`\nline2 {}", "y".repeat(9000));
        let SpawnInvocation::Script { content } =
            spawn_invocation(&pairs, "claude", &[hostile], 100)
        else {
            panic!("past the bound the invocation is a script");
        };
        assert!(content.contains("'it'\\''s a $HOME `test`\nline2"));
    }

    #[test]
    fn permanent_spawn_errors_are_classified() {
        // gtcore-408279: `command too long` / missing binary / bad workdir are permanent — the
        // retry loop must abandon; timeouts and duplicate sessions stay transient.
        let perm = |m: &str| io::Error::other(m.to_string());
        assert!(spawn_error_is_permanent(&perm(
            "tmux respawn-pane failed: command too long"
        )));
        assert!(spawn_error_is_permanent(&perm(
            "spawn tmux: No such file or directory (os error 2)"
        )));
        assert!(spawn_error_is_permanent(&perm("chdir: not a directory")));
        assert!(!spawn_error_is_permanent(&perm("tmux new-session timed out")));
        assert!(!spawn_error_is_permanent(&perm("duplicate session: hq-1")));
    }

    #[test]
    fn fake_roundtrips_env() {
        let t = FakeTmux::new();
        t.new_session(
            "s1",
            Path::new("/tmp"),
            "claude",
            &[],
            &[("GT_HOOK_BEAD".into(), "hq-1".into())],
        )
        .unwrap();
        assert!(t.has_session("s1"));
        assert_eq!(
            t.show_environment("s1", "GT_HOOK_BEAD").unwrap().as_deref(),
            Some("hq-1")
        );
        t.kill_session("s1").unwrap();
        assert!(!t.has_session("s1"));
    }
}
