//! [`Attach`] adapter that attaches to a live tmux session via `pipe-pane` + `send-keys`.
//!
//! Primary substrate picked by the spike (see crate-level docs). The pane I/O dance lives
//! behind the [`TmuxAttachOps`] port so the unit tests can drive the adapter through a
//! [`FakeTmuxAttachOps`] without a tmux server:
//!
//! - [`TmuxAttachOps::open_pipe`] starts streaming pane bytes back. Production impl
//!   ([`CliTmuxAttachOps`]) mkfifo's a unique path, spawns `tmux pipe-pane -O -t <session>
//!   "cat >> <fifo>"`, then opens the fifo for read. The fake returns an in-memory reader.
//! - [`TmuxAttachOps::close_pipe`] reverses it (`tmux pipe-pane` with no command stops the
//!   pipe; the prod impl also unlinks the fifo).
//! - [`TmuxAttachOps::send_keys_raw`] forwards user keystrokes via `tmux send-keys -l -t
//!   <session> <bytes>` (literal mode, no key-name parsing).
//!
//! Reader/writer are split: the reader owns the fifo file handle (one task does the
//! blocking read loop), the writer holds an `Arc<O>` + session id so keystrokes go through
//! independently. This is what lets the WS route forward client input while the read side
//! sits blocked waiting for tmux output.
//!
//! Unix-only: tmux + named FIFOs. The crate compiles on Linux/macOS; calling the prod impl
//! on Windows would fail at the `mkfifo` shell call.

use std::ffi::OsStr;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::port::{
    Attach, AttachError, AttachHandle, TerminalReader, TerminalTarget, TerminalWriter,
};

/// Ops the [`TmuxPipeAttach`] adapter needs from a tmux backend. Lives here (not in
/// `gt-polecat`) because `pipe-pane` + raw `send-keys` are attach-specific; the polecat
/// supervisor's [`gt_polecat::tmux::Tmux`] trait stays focused on session lifecycle.
pub trait TmuxAttachOps: Send + Sync + 'static {
    fn has_session(&self, session: &str) -> bool;

    /// Start piping the session's active pane bytes back. The returned reader yields
    /// bytes as tmux writes them; `Ok(0)` means the pane stream closed.
    fn open_pipe(&self, session: &str) -> io::Result<Box<dyn Read + Send>>;

    /// Stop the pipe started by [`Self::open_pipe`]. Idempotent — calling on a session
    /// without an active pipe is a no-op.
    fn close_pipe(&self, session: &str) -> io::Result<()>;

    /// Forward `bytes` to the session as keystrokes via `tmux send-keys -l`. Literal mode
    /// (`-l`) treats arguments as raw text, not tmux key names — what the WS route wants
    /// when forwarding xterm input.
    fn send_keys_raw(&self, session: &str, bytes: &[u8]) -> io::Result<()>;
}

/// Primary [`Attach`] adapter. Only accepts [`TerminalTarget::Tmux`].
pub struct TmuxPipeAttach<O: TmuxAttachOps> {
    ops: Arc<O>,
}

impl<O: TmuxAttachOps> TmuxPipeAttach<O> {
    pub fn new(ops: O) -> Self {
        Self { ops: Arc::new(ops) }
    }

    pub fn from_arc(ops: Arc<O>) -> Self {
        Self { ops }
    }
}

impl<O: TmuxAttachOps> Attach for TmuxPipeAttach<O> {
    fn open(&self, target: &TerminalTarget) -> Result<AttachHandle, AttachError> {
        let session = match target {
            TerminalTarget::Tmux { session, .. } => session.clone(),
            TerminalTarget::Spawn { program, .. } => {
                return Err(AttachError::Unsupported(format!(
                    "TmuxPipeAttach received spawn target {program}; use PtyAttach"
                )));
            }
        };
        if !self.ops.has_session(&session) {
            return Err(AttachError::NotFound(format!("tmux session {session}")));
        }
        let pipe = self.ops.open_pipe(&session)?;
        let reader = TmuxPipeReader { inner: pipe };
        let writer = TmuxPipeWriter {
            ops: Arc::clone(&self.ops),
            session,
            closed: AtomicBool::new(false),
        };
        Ok(AttachHandle {
            reader: Box::new(reader),
            writer: Arc::new(writer),
        })
    }
}

struct TmuxPipeReader {
    inner: Box<dyn Read + Send>,
}

impl TerminalReader for TmuxPipeReader {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

struct TmuxPipeWriter<O: TmuxAttachOps> {
    ops: Arc<O>,
    session: String,
    closed: AtomicBool,
}

impl<O: TmuxAttachOps> TerminalWriter for TmuxPipeWriter<O> {
    fn write_keys(&self, bytes: &[u8]) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tmux attach writer closed",
            ));
        }
        self.ops.send_keys_raw(&self.session, bytes)
    }

    fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Best-effort: ignore the result. The WS route is tearing down; surfacing an
            // error here just loses signal. The fifo file itself is owned by the reader
            // (see `FifoReader::Drop`) so nothing to unlink here either.
            let _ = self.ops.close_pipe(&self.session);
        }
    }
}

impl<O: TmuxAttachOps> Drop for TmuxPipeWriter<O> {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------------------
// CliTmuxAttachOps — production: shells out to `tmux` + manages FIFOs.

/// Real [`TmuxAttachOps`] implementation. Shells out to the `tmux` binary; FIFO files live
/// under [`Self::fifo_dir`] (defaults to `std::env::temp_dir()`).
pub struct CliTmuxAttachOps {
    bin: String,
    socket: Option<String>,
    fifo_dir: PathBuf,
}

impl CliTmuxAttachOps {
    pub fn new() -> Self {
        Self {
            bin: "tmux".into(),
            socket: None,
            fifo_dir: std::env::temp_dir(),
        }
    }

    pub fn with_bin(mut self, bin: impl Into<String>) -> Self {
        self.bin = bin.into();
        self
    }

    pub fn with_socket(mut self, socket: impl Into<String>) -> Self {
        self.socket = Some(socket.into());
        self
    }

    pub fn with_fifo_dir(mut self, dir: PathBuf) -> Self {
        self.fifo_dir = dir;
        self
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(&self.bin);
        if let Some(s) = &self.socket {
            c.arg("-L").arg(s);
        }
        c
    }

    fn run<I, S>(&self, args: I) -> io::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let out = self
            .cmd()
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "tmux: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    }

    fn unique_fifo_path(&self) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        self.fifo_dir.join(format!("gt-term-{pid}-{nanos}-{n}.fifo"))
    }

    fn mkfifo(&self, path: &Path) -> io::Result<()> {
        // Shell out to `mkfifo` — avoids pulling `libc`/`nix` for one syscall. Mode 0600 so
        // only the gt-web user can read the stream.
        let status = Command::new("mkfifo")
            .arg("-m")
            .arg("600")
            .arg(path)
            .status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "mkfifo {} failed",
                path.display()
            )));
        }
        Ok(())
    }
}

impl Default for CliTmuxAttachOps {
    fn default() -> Self {
        Self::new()
    }
}

impl TmuxAttachOps for CliTmuxAttachOps {
    fn has_session(&self, session: &str) -> bool {
        self.cmd()
            .args(["has-session", "-t", session])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn open_pipe(&self, session: &str) -> io::Result<Box<dyn Read + Send>> {
        let fifo = self.unique_fifo_path();
        self.mkfifo(&fifo)?;
        let pipe_cmd = format!("cat >> '{}'", fifo.display());
        if let Err(e) = self.run(["pipe-pane", "-O", "-t", session, &pipe_cmd]) {
            let _ = std::fs::remove_file(&fifo);
            return Err(e);
        }
        let file = match std::fs::File::open(&fifo) {
            Ok(f) => f,
            Err(e) => {
                let _ = self.run(["pipe-pane", "-t", session]);
                let _ = std::fs::remove_file(&fifo);
                return Err(e);
            }
        };
        Ok(Box::new(FifoReader { file, path: fifo }))
    }

    fn close_pipe(&self, session: &str) -> io::Result<()> {
        self.run(["pipe-pane", "-t", session])
    }

    fn send_keys_raw(&self, session: &str, bytes: &[u8]) -> io::Result<()> {
        let text = match std::str::from_utf8(bytes) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(bytes).into_owned(),
        };
        self.run(["send-keys", "-l", "-t", session, &text])
    }
}

/// Wraps the read end of a fifo + unlinks the file when the reader is dropped.
struct FifoReader {
    file: std::fs::File,
    path: PathBuf,
}

impl Read for FifoReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Drop for FifoReader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// ---------------------------------------------------------------------------
// FakeTmuxAttachOps — in-memory test double.

/// In-memory [`TmuxAttachOps`] for unit tests.
#[derive(Default)]
pub struct FakeTmuxAttachOps {
    inner: Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    sessions: Vec<String>,
    pipe_scripts: std::collections::HashMap<String, Vec<u8>>,
    writes: Vec<(String, Vec<u8>)>,
    open_pipes: std::collections::HashSet<String>,
    closed_pipes: Vec<String>,
}

impl FakeTmuxAttachOps {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_session(&self, session: impl Into<String>) {
        self.inner.lock().unwrap().sessions.push(session.into());
    }

    pub fn set_pipe_script(&self, session: impl Into<String>, bytes: Vec<u8>) {
        self.inner
            .lock()
            .unwrap()
            .pipe_scripts
            .insert(session.into(), bytes);
    }

    pub fn writes(&self) -> Vec<(String, Vec<u8>)> {
        self.inner.lock().unwrap().writes.clone()
    }

    pub fn closed_pipes(&self) -> Vec<String> {
        self.inner.lock().unwrap().closed_pipes.clone()
    }
}

impl TmuxAttachOps for FakeTmuxAttachOps {
    fn has_session(&self, session: &str) -> bool {
        self.inner.lock().unwrap().sessions.iter().any(|s| s == session)
    }

    fn open_pipe(&self, session: &str) -> io::Result<Box<dyn Read + Send>> {
        let mut g = self.inner.lock().unwrap();
        if !g.sessions.iter().any(|s| s == session) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("session {session}"),
            ));
        }
        g.open_pipes.insert(session.to_string());
        let bytes = g.pipe_scripts.remove(session).unwrap_or_default();
        Ok(Box::new(io::Cursor::new(bytes)))
    }

    fn close_pipe(&self, session: &str) -> io::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.open_pipes.remove(session);
        g.closed_pipes.push(session.to_string());
        Ok(())
    }

    fn send_keys_raw(&self, session: &str, bytes: &[u8]) -> io::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .writes
            .push((session.to_string(), bytes.to_vec()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_spawn_target_with_unsupported() {
        let adapter = TmuxPipeAttach::new(FakeTmuxAttachOps::new());
        match adapter.open(&TerminalTarget::spawn("/bin/sh", vec![])) {
            Err(AttachError::Unsupported(_)) => {}
            Err(e) => panic!("expected Unsupported, got {e:?}"),
            Ok(_) => panic!("expected Unsupported, got Ok"),
        }
    }

    #[test]
    fn missing_session_returns_not_found() {
        let adapter = TmuxPipeAttach::new(FakeTmuxAttachOps::new());
        match adapter.open(&TerminalTarget::tmux("ghost")) {
            Err(AttachError::NotFound(_)) => {}
            Err(e) => panic!("expected NotFound, got {e:?}"),
            Ok(_) => panic!("expected NotFound, got Ok"),
        }
    }

    #[test]
    fn open_streams_scripted_pane_bytes() {
        let ops = FakeTmuxAttachOps::new();
        ops.add_session("polecat-1");
        ops.set_pipe_script("polecat-1", b"shell output\n".to_vec());
        let adapter = TmuxPipeAttach::new(ops);
        let handle = adapter.open(&TerminalTarget::tmux("polecat-1")).unwrap();
        let AttachHandle { mut reader, .. } = handle;
        let mut buf = [0u8; 32];
        let n = reader.read_chunk(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"shell output\n");
        assert_eq!(reader.read_chunk(&mut buf).unwrap(), 0);
    }

    #[test]
    fn write_keys_forwards_to_send_keys_raw() {
        let ops = Arc::new(FakeTmuxAttachOps::new());
        ops.add_session("polecat-2");
        let adapter = TmuxPipeAttach::from_arc(ops.clone());
        let handle = adapter.open(&TerminalTarget::tmux("polecat-2")).unwrap();
        handle.writer.write_keys(b"ls\n").unwrap();
        handle.writer.write_keys(b"\x1b[A").unwrap(); // up-arrow escape sequence
        let writes = ops.writes();
        assert_eq!(
            writes,
            vec![
                ("polecat-2".into(), b"ls\n".to_vec()),
                ("polecat-2".into(), b"\x1b[A".to_vec()),
            ]
        );
    }

    #[test]
    fn writer_close_calls_close_pipe_and_blocks_subsequent_writes() {
        let ops = Arc::new(FakeTmuxAttachOps::new());
        ops.add_session("polecat-3");
        let adapter = TmuxPipeAttach::from_arc(ops.clone());
        let handle = adapter.open(&TerminalTarget::tmux("polecat-3")).unwrap();
        handle.writer.close();
        let err = handle.writer.write_keys(b"ignored").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(ops.closed_pipes(), vec!["polecat-3".to_string()]);
    }

    #[test]
    fn writer_close_is_idempotent_across_arc_clones() {
        let ops = Arc::new(FakeTmuxAttachOps::new());
        ops.add_session("polecat-4");
        let adapter = TmuxPipeAttach::from_arc(ops.clone());
        let handle = adapter.open(&TerminalTarget::tmux("polecat-4")).unwrap();
        let writer_a = handle.writer.clone();
        let writer_b = handle.writer;
        writer_a.close();
        writer_b.close();
        // close_pipe fires only once even with two Arc clones racing close().
        assert_eq!(ops.closed_pipes(), vec!["polecat-4".to_string()]);
    }
}
