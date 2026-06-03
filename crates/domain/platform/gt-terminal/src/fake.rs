//! In-memory [`Attach`] for tests.
//!
//! Scripted reads + recorded writes — same shape as `gt_login::pty::FakePty`. Tests assert
//! that the WS route forwarded the right keystrokes and consumed pane bytes in order.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::port::{
    Attach, AttachError, AttachHandle, TerminalReader, TerminalTarget, TerminalWriter,
};

/// In-memory adapter. Holds one script per `open()` call; the FIFO queue lets tests stage
/// several attach lifetimes in a single fake.
#[derive(Default)]
pub struct FakeAttach {
    inner: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    /// Scripts to hand out on subsequent `open()` calls. Each script is the chunk sequence
    /// the reader will yield from `read_chunk` (one `Vec<u8>` per read).
    pending_scripts: VecDeque<Vec<Vec<u8>>>,
    /// Writes recorded across all writers this fake has opened.
    writes: Vec<Vec<u8>>,
    /// Targets seen across all opens, in attach order.
    targets: Vec<TerminalTarget>,
    /// Resize events recorded across all writers.
    resizes: Vec<(u16, u16)>,
    /// Count of `close` calls. Each writer is `Arc`-clonable so the same logical close
    /// may fire from several places; only the first hit on a given writer increments.
    closes: usize,
}

impl FakeAttach {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a script for the next `open()` call. Each chunk maps to one `read_chunk`
    /// invocation; the read after the last chunk returns EOF (`Ok(0)`).
    pub fn enqueue(&self, chunks: Vec<Vec<u8>>) {
        self.inner.lock().unwrap().pending_scripts.push_back(chunks);
    }

    /// Drain the keystrokes the WS route forwarded across all writers.
    pub fn take_writes(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.inner.lock().unwrap().writes)
    }

    /// Targets the fake was asked to attach to, in order.
    pub fn targets(&self) -> Vec<TerminalTarget> {
        self.inner.lock().unwrap().targets.clone()
    }

    /// Resize events seen across all writers, in order.
    pub fn resizes(&self) -> Vec<(u16, u16)> {
        self.inner.lock().unwrap().resizes.clone()
    }

    /// Total `close` calls across all writers (first-close-per-writer is counted; later
    /// calls on the same writer are idempotent).
    pub fn close_count(&self) -> usize {
        self.inner.lock().unwrap().closes
    }
}

impl Attach for FakeAttach {
    fn open(&self, target: &TerminalTarget) -> Result<AttachHandle, AttachError> {
        let script = {
            let mut g = self.inner.lock().unwrap();
            g.targets.push(target.clone());
            g.pending_scripts.pop_front().unwrap_or_default()
        };
        Ok(AttachHandle {
            reader: Box::new(FakeReader {
                script: script.into(),
            }),
            writer: Arc::new(FakeWriter {
                shared: Arc::clone(&self.inner),
                closed: AtomicBool::new(false),
            }),
        })
    }
}

struct FakeReader {
    script: VecDeque<Vec<u8>>,
}

impl TerminalReader for FakeReader {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(next) = self.script.pop_front() else {
            return Ok(0);
        };
        let n = next.len().min(buf.len());
        buf[..n].copy_from_slice(&next[..n]);
        Ok(n)
    }
}

struct FakeWriter {
    shared: Arc<Mutex<FakeState>>,
    closed: AtomicBool,
}

impl TerminalWriter for FakeWriter {
    fn write_keys(&self, bytes: &[u8]) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "fake writer closed"));
        }
        self.shared.lock().unwrap().writes.push(bytes.to_vec());
        Ok(())
    }

    fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
        self.shared.lock().unwrap().resizes.push((cols, rows));
        Ok(())
    }

    fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.shared.lock().unwrap().closes += 1;
        }
    }
}

impl Drop for FakeWriter {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_records_target_and_scripts_reads() {
        let fake = FakeAttach::new();
        fake.enqueue(vec![b"hello".to_vec(), b" world".to_vec()]);

        let target = TerminalTarget::tmux("polecat-1");
        let handle = fake.open(&target).unwrap();
        let AttachHandle { mut reader, .. } = handle;

        let mut buf = [0u8; 32];
        let n = reader.read_chunk(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");
        let n = reader.read_chunk(&mut buf).unwrap();
        assert_eq!(&buf[..n], b" world");
        assert_eq!(reader.read_chunk(&mut buf).unwrap(), 0);
        assert_eq!(fake.targets(), vec![target]);
    }

    #[test]
    fn write_keys_records_in_order() {
        let fake = FakeAttach::new();
        fake.enqueue(vec![]);
        let handle = fake.open(&TerminalTarget::tmux("p")).unwrap();
        handle.writer.write_keys(b"ls\n").unwrap();
        handle.writer.write_keys(b"\x1b").unwrap();
        assert_eq!(
            fake.take_writes(),
            vec![b"ls\n".to_vec(), b"\x1b".to_vec()]
        );
    }

    #[test]
    fn writer_arc_clone_shares_close_state() {
        let fake = FakeAttach::new();
        fake.enqueue(vec![]);
        let handle = fake.open(&TerminalTarget::tmux("p")).unwrap();
        let writer_a = handle.writer.clone();
        let writer_b = handle.writer;
        writer_a.close();
        // Second close on the SAME writer is idempotent.
        writer_b.close();
        assert_eq!(fake.close_count(), 1);
        // Writes after close error.
        let err = writer_a.write_keys(b"x").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn resize_recorded() {
        let fake = FakeAttach::new();
        fake.enqueue(vec![]);
        let handle = fake.open(&TerminalTarget::tmux("p")).unwrap();
        handle.writer.resize(120, 40).unwrap();
        assert_eq!(fake.resizes(), vec![(120, 40)]);
    }
}
