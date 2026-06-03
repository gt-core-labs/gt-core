//! [`Attach`] adapter that spawns a fresh pseudo-terminal for the target command.
//!
//! Wraps [`gt_login::pty::Pty`] so the same substrate that drives `claude /login`
//! (`hq-fe-auth.1`) backs the dashboard's ad-hoc `Spawn` attach. Only accepts
//! [`TerminalTarget::Spawn`] — tmux targets are the [`TmuxPipeAttach`](super::TmuxPipeAttach)
//! adapter's job.
//!
//! ## Concurrency note
//!
//! The underlying [`gt_login::pty::PtyChild`] combines reader + writer behind the same
//! handle (`&mut self`). To present split [`TerminalReader`] / [`TerminalWriter`] halves
//! we wrap the child in `Arc<Mutex<>>`; consequently a blocking `read_chunk` will hold the
//! lock until pty bytes arrive and `write_keys` waits behind it. For interactive shells
//! this is usually fine — the pty emits output (prompt, echo) between each keystroke so
//! reads return quickly. For genuinely silent processes the writer can stall. Sites that
//! need a fully parallel pty should reach for [`TmuxPipeAttach`] (different concurrency
//! model: independent fifo read + independent send-keys subprocess) or wait for a future
//! gt-login extension that yields pre-split halves.
//!
//! Window size from [`TerminalTarget::Spawn`] is ignored — `gt_login::pty::Pty` fixes the
//! pty pair at 80x24. A later beat can widen the `Pty` port without touching this adapter.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use gt_login::pty::{Pty, PtyChild};

use crate::port::{
    Attach, AttachError, AttachHandle, TerminalReader, TerminalTarget, TerminalWriter,
};

/// Spawn-mode [`Attach`] backed by any [`Pty`] adapter (`gt_login::pty::PortablePty` in
/// prod, `gt_login::pty::FakePty` in unit tests).
pub struct PtyAttach<P: Pty> {
    pty: Arc<P>,
}

impl<P: Pty> PtyAttach<P> {
    pub fn new(pty: P) -> Self {
        Self { pty: Arc::new(pty) }
    }

    pub fn from_arc(pty: Arc<P>) -> Self {
        Self { pty }
    }
}

impl<P: Pty + 'static> Attach for PtyAttach<P> {
    fn open(&self, target: &TerminalTarget) -> Result<AttachHandle, AttachError> {
        let (program, args) = match target {
            TerminalTarget::Spawn { program, args, .. } => (program.as_str(), args.as_slice()),
            TerminalTarget::Tmux { session, .. } => {
                return Err(AttachError::Unsupported(format!(
                    "PtyAttach received tmux target {session}; use TmuxPipeAttach"
                )));
            }
        };
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let child = self.pty.spawn(program, &argv)?;
        let shared = Arc::new(Mutex::new(child));
        let closed = Arc::new(AtomicBool::new(false));
        Ok(AttachHandle {
            reader: Box::new(PtyReader {
                shared: Arc::clone(&shared),
                closed: Arc::clone(&closed),
            }),
            writer: Arc::new(PtyWriter { shared, closed }),
        })
    }
}

struct PtyReader {
    shared: Arc<Mutex<Box<dyn PtyChild>>>,
    closed: Arc<AtomicBool>,
}

impl TerminalReader for PtyReader {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(0);
        }
        // See module doc: this lock is held across a blocking pty read. The writer will
        // wait behind it until the read returns.
        self.shared.lock().unwrap().read_chunk(buf)
    }
}

struct PtyWriter {
    shared: Arc<Mutex<Box<dyn PtyChild>>>,
    closed: Arc<AtomicBool>,
}

impl TerminalWriter for PtyWriter {
    fn write_keys(&self, bytes: &[u8]) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pty attach writer closed",
            ));
        }
        self.shared.lock().unwrap().write_all(bytes)
    }

    fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Kill the child to unblock a parked reader. The reader sees `closed=true` on
            // its next iteration and returns EOF.
            self.shared.lock().unwrap().kill();
        }
    }
}

impl Drop for PtyWriter {
    fn drop(&mut self) {
        // Only fires if the WS handler dropped the Arc<dyn TerminalWriter> without calling
        // close (panic path). Belt-and-suspenders.
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_login::pty::FakePty;

    #[test]
    fn rejects_tmux_target_with_unsupported() {
        let adapter = PtyAttach::new(FakePty::default());
        match adapter.open(&TerminalTarget::tmux("polecat-x")) {
            Err(AttachError::Unsupported(_)) => {}
            Err(e) => panic!("expected Unsupported, got {e:?}"),
            Ok(_) => panic!("expected Unsupported, got Ok"),
        }
    }

    #[test]
    fn spawn_target_streams_scripted_bytes() {
        let pty = FakePty::scripted(vec![b"hi".to_vec(), b" there".to_vec()], 0);
        let adapter = PtyAttach::new(pty);
        let target = TerminalTarget::spawn("/bin/echo", vec!["unused".into()]);
        let handle = adapter.open(&target).unwrap();
        let AttachHandle { mut reader, .. } = handle;
        let mut buf = [0u8; 16];
        let n = reader.read_chunk(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hi");
        let n = reader.read_chunk(&mut buf).unwrap();
        assert_eq!(&buf[..n], b" there");
        assert_eq!(reader.read_chunk(&mut buf).unwrap(), 0);
    }

    #[test]
    fn close_kills_child_so_subsequent_reads_eof() {
        let pty = FakePty::scripted(vec![b"unread".to_vec()], 0);
        let adapter = PtyAttach::new(pty);
        let handle = adapter
            .open(&TerminalTarget::spawn("/bin/cat", vec![]))
            .unwrap();
        let AttachHandle { mut reader, writer } = handle;
        writer.close();
        let mut buf = [0u8; 8];
        assert_eq!(reader.read_chunk(&mut buf).unwrap(), 0);
    }
}
