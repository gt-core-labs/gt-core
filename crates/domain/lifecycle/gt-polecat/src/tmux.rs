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

    /// Pin a private server socket (`tmux -L <socket>`). Isolation for tests and for
    /// deployments that segregate tmux servers per role.
    pub fn with_socket(mut self, socket: impl Into<String>) -> Self {
        self.socket = Some(socket.into());
        self
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

        // Replace the placeholder shell with the real command in the same workdir.
        let mut respawn: Vec<String> = vec![
            "respawn-pane".into(),
            "-k".into(),
            "-t".into(),
            session.into(),
            "-c".into(),
            workdir,
            command.into(),
        ];
        respawn.extend(args.iter().cloned());
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
}

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
}

impl FakeTmux {
    pub fn new() -> Self {
        Self::default()
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
