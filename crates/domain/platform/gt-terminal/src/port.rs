//! `Attach` port + value types.
//!
//! Returns a split [`AttachHandle`] — separate [`TerminalReader`] and [`TerminalWriter`]
//! halves — so the WebSocket route can run a blocking read loop while keystroke writes
//! still get through. A combined `&mut`-bound stream would force the writer to wait for
//! every blocking `read_chunk`, which on an idle tmux pane never returns.

use std::io;
use std::sync::Arc;
use thiserror::Error;

/// Identifies what to attach to.
///
/// - `TerminalTarget::Tmux { session }` — an existing tmux session id (the polecat name).
/// - `TerminalTarget::Spawn { program, args }` — spawn a fresh pty around a command.
///
/// Window size is advisory; adapters that can't resize (fake, pty without `SIGWINCH`
/// support) ignore the hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalTarget {
    Tmux {
        session: String,
        cols: u16,
        rows: u16,
    },
    Spawn {
        program: String,
        args: Vec<String>,
        cols: u16,
        rows: u16,
    },
}

impl TerminalTarget {
    pub fn tmux(session: impl Into<String>) -> Self {
        Self::Tmux {
            session: session.into(),
            cols: 80,
            rows: 24,
        }
    }

    pub fn spawn(program: impl Into<String>, args: Vec<String>) -> Self {
        Self::Spawn {
            program: program.into(),
            args,
            cols: 80,
            rows: 24,
        }
    }
}

#[derive(Debug, Error)]
pub enum AttachError {
    #[error("terminal target not found: {0}")]
    NotFound(String),
    #[error("terminal target not supported by adapter: {0}")]
    Unsupported(String),
    #[error("terminal attach io error: {0}")]
    Io(#[from] io::Error),
}

/// Split halves returned by [`Attach::open`]. Reader is `Box<dyn>` (owned by one task);
/// writer is `Arc<dyn>` so the WS handler can hand a clone to both the keystroke pump and
/// the resize/close path without juggling locks at the call site.
pub struct AttachHandle {
    pub reader: Box<dyn TerminalReader>,
    pub writer: Arc<dyn TerminalWriter>,
}

/// Factory for [`AttachHandle`]s. Implementations: [`super::TmuxPipeAttach`],
/// [`super::PtyAttach`], [`super::FakeAttach`].
pub trait Attach: Send + Sync {
    fn open(&self, target: &TerminalTarget) -> Result<AttachHandle, AttachError>;
}

/// Read half. Owned by the WS reader task; runs blocking `read_chunk` inside
/// `spawn_blocking`. EOF (`Ok(0)`) means the target stream closed (tmux session killed,
/// pty child exited).
pub trait TerminalReader: Send {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize>;
}

/// Write half. `&self` methods + `Sync` so the WS handler can hold one `Arc` shared
/// between the keystroke pump and the cleanup hook. Adapters that need interior state
/// (e.g. a `closed` flag, a pty writer handle) wrap it in their own mutex/atomic.
pub trait TerminalWriter: Send + Sync {
    /// Forward `bytes` to the target as raw keystrokes. Returns `BrokenPipe` once the
    /// writer has been closed (`Self::close` called or target torn down).
    fn write_keys(&self, bytes: &[u8]) -> io::Result<()>;

    /// Update the advisory window size. Adapters without resize support return `Ok(())`.
    fn resize(&self, _cols: u16, _rows: u16) -> io::Result<()> {
        Ok(())
    }

    /// Best-effort teardown. Idempotent — adapters may receive `close` more than once and
    /// from more than one `Arc` clone.
    fn close(&self);
}
