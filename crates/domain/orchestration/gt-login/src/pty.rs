//! `Pty` port + adapters.
//!
//! The state machine in [`crate::state`] is platform-neutral; the only side-effecting hop
//! is the pseudo-terminal that hosts `claude /login`. We model it as a port so:
//!
//! - `cargo test` can drive the full state machine via [`FakePty`] without spawning a real
//!   process (CI and macOS/Windows hosts have no `claude` binary).
//! - Production uses [`PortablePty`] (the `portable-pty` crate) which gives us a real TTY
//!   so the CLI's URL printer behaves as it does on a user's terminal.
//!
//! The trait is intentionally narrow — spawn, then read/write chunks — because the driver
//! does the parsing. Adapter authors only need to forward bytes.
//!
//! `hq-fe-auth.5` extends the port with [`PtyKiller`]: a `Send + Sync` handle the driver
//! hands to a watchdog thread so it can kill the child after a timeout without holding
//! `&mut PtyChild` (the driver is blocked inside `read_chunk` during the URL phase).
//! [`PortableChild`] also reaps zombies on `Drop` so a panicked driver task never leaks a
//! `claude /login` process.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// External kill-side handle. The driver clones it once per `spawn` and hands the clone
/// to a watchdog thread; the watchdog calls [`PtyKiller::kill`] when its deadline fires.
/// Kept `Send + Sync` so adapters can back it with anything that survives a thread move
/// (`portable_pty::ChildKiller`, an atomic flag, ...). `hq-fe-auth.5`.
pub trait PtyKiller: Send + Sync {
    /// Best-effort terminate. Idempotent: calling twice after the child is already gone
    /// must not panic — the watchdog and the driver may race on shutdown.
    fn kill(&self);
}

/// Live PTY child. Drop must terminate the child (the real adapter reaps zombies on drop),
/// so the driver does not need an explicit teardown after `Failed`/`Complete`.
pub trait PtyChild: Send {
    /// Read the next chunk from the child's combined stdout+stderr. Returns `Ok(0)` on
    /// EOF (child exited or pty closed).
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Write `bytes` to the child's stdin. The driver appends a trailing newline when it
    /// forwards the user's token, so adapters should not add their own.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Wait for the child to exit and return its raw exit status. Adapters that cannot
    /// observe the status (test fakes) may return `0` to signal success.
    fn wait(&mut self) -> io::Result<i32>;

    /// Best-effort termination. Called when the caller cancels mid-flight.
    fn kill(&mut self);

    /// Clone an external-kill handle (`hq-fe-auth.5`). The driver hands this to a watchdog
    /// thread so it can kill the child after a timeout without holding `&mut self` (which
    /// is blocked inside [`PtyChild::read_chunk`] in the URL phase).
    fn killer(&self) -> Box<dyn PtyKiller>;
}

/// Spawn port. The driver passes the executable + args; adapters wire up the child.
pub trait Pty: Send + Sync {
    /// Spawn `program` with `args` under a fresh pty. The caller never sees the pty handle
    /// directly — only the child it owns.
    fn spawn(&self, program: &str, args: &[&str]) -> io::Result<Box<dyn PtyChild>>;
}

// ---------------------------------------------------------------------------
// FakePty — scripted test adapter.

/// In-memory adapter for tests. The driver thinks it spawned `claude /login`; in reality
/// reads come from a scripted byte queue and writes are recorded for assertions.
///
/// Scripts are passed as `Vec<Vec<u8>>` so each chunk maps to one `read_chunk` call —
/// useful for testing that the URL parser handles split-across-chunks output.
#[derive(Default)]
pub struct FakePty {
    inner: Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    next_script: VecDeque<Vec<u8>>,
    next_exit: i32,
    next_hang_on_empty: bool,
    last_writes: Vec<Vec<u8>>,
}

impl FakePty {
    /// Construct a fake that, on the next `spawn`, will hand the child a script of chunks
    /// and exit with `exit_status` after `wait`. Once the script is drained the child
    /// returns `Ok(0)` (EOF) — matching how a real CLI signals "no more output".
    pub fn scripted(chunks: Vec<Vec<u8>>, exit_status: i32) -> Self {
        Self {
            inner: Mutex::new(FakeState {
                next_script: chunks.into(),
                next_exit: exit_status,
                next_hang_on_empty: false,
                last_writes: Vec::new(),
            }),
        }
    }

    /// Like [`FakePty::scripted`] but, once the script is drained, `read_chunk` blocks
    /// forever instead of returning EOF. The only way to unblock it is via the
    /// [`PtyKiller`] handle — the driver's watchdog uses this to simulate a hung
    /// `claude /login` for the `hq-fe-auth.5` URL-phase-timeout test.
    pub fn blocking(chunks: Vec<Vec<u8>>, exit_status: i32) -> Self {
        Self {
            inner: Mutex::new(FakeState {
                next_script: chunks.into(),
                next_exit: exit_status,
                next_hang_on_empty: true,
                last_writes: Vec::new(),
            }),
        }
    }

    /// Drain the writes the driver pushed to the child's stdin since the last call.
    /// Tests assert that the token was forwarded with the expected trailing newline.
    pub fn take_writes(&self) -> Vec<Vec<u8>> {
        let mut g = self.inner.lock().expect("fake pty mutex");
        std::mem::take(&mut g.last_writes)
    }
}

impl Pty for FakePty {
    fn spawn(&self, _program: &str, _args: &[&str]) -> io::Result<Box<dyn PtyChild>> {
        let (script, exit, hang) = {
            let mut g = self.inner.lock().expect("fake pty mutex");
            (
                std::mem::take(&mut g.next_script),
                g.next_exit,
                g.next_hang_on_empty,
            )
        };
        let killed = Arc::new(AtomicBool::new(false));
        let cvar = Arc::new((Mutex::new(()), Condvar::new()));
        Ok(Box::new(FakeChild {
            script,
            exit,
            killed,
            cvar,
            hang_on_empty: hang,
            // SAFETY: we hold the Mutex pointer; the FakePty outlives the child within a
            // test, so a raw shared pointer to the inner state is the simplest way to
            // record writes back without `Arc`-ifying the public API.
            parent: self as *const FakePty,
        }))
    }
}

struct FakeChild {
    script: VecDeque<Vec<u8>>,
    exit: i32,
    killed: Arc<AtomicBool>,
    cvar: Arc<(Mutex<()>, Condvar)>,
    hang_on_empty: bool,
    parent: *const FakePty,
}

// SAFETY: the test only touches the fake from the driver thread; in real prod the trait
// object stays on a single owning task.
unsafe impl Send for FakeChild {}

impl PtyChild for FakeChild {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.killed.load(Ordering::SeqCst) {
            return Ok(0);
        }
        if let Some(next) = self.script.pop_front() {
            let n = next.len().min(buf.len());
            buf[..n].copy_from_slice(&next[..n]);
            return Ok(n);
        }
        if self.hang_on_empty {
            // Block until the killer notifies. Spurious wakeups: re-check the flag.
            let (lock, cv) = &*self.cvar;
            let mut guard = lock.lock().expect("fake pty cv");
            while !self.killed.load(Ordering::SeqCst) {
                guard = cv.wait(guard).expect("fake pty cv wait");
            }
        }
        Ok(0)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        // SAFETY: `parent` was constructed from a borrowed `&FakePty` whose lifetime
        // covers the FakeChild within tests. We never deref after parent is dropped.
        let parent = unsafe { &*self.parent };
        let mut g = parent.inner.lock().expect("fake pty mutex");
        g.last_writes.push(bytes.to_vec());
        Ok(())
    }

    fn wait(&mut self) -> io::Result<i32> {
        Ok(self.exit)
    }

    fn kill(&mut self) {
        self.killed.store(true, Ordering::SeqCst);
        let (_lock, cv) = &*self.cvar;
        cv.notify_all();
    }

    fn killer(&self) -> Box<dyn PtyKiller> {
        Box::new(FakeKiller {
            killed: self.killed.clone(),
            cvar: self.cvar.clone(),
        })
    }
}

struct FakeKiller {
    killed: Arc<AtomicBool>,
    cvar: Arc<(Mutex<()>, Condvar)>,
}

impl PtyKiller for FakeKiller {
    fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        let (_lock, cv) = &*self.cvar;
        cv.notify_all();
    }
}

// ---------------------------------------------------------------------------
// PortablePty — production adapter over the `portable-pty` crate.

/// Real adapter using `portable-pty`. Each `spawn` allocates a fresh pty pair, attaches
/// the child to the slave, and exposes the master's reader/writer to the driver.
///
/// Window size is fixed at 80x24 — `claude /login` only prints a one-line URL and reads
/// one line of token, so a real terminal size brings nothing.
pub struct PortablePty;

impl PortablePty {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PortablePty {
    fn default() -> Self {
        Self::new()
    }
}

impl Pty for PortablePty {
    fn spawn(&self, program: &str, args: &[&str]) -> io::Result<Box<dyn PtyChild>> {
        use portable_pty::{native_pty_system, CommandBuilder, PtySize};

        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut cmd = CommandBuilder::new(program);
        for a in args {
            cmd.arg(a);
        }
        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Slave handle is no longer needed once the child holds it; drop closes the FD on
        // our side so EOF propagates when the child exits.
        drop(pair.slave);

        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(Box::new(PortableChild {
            child: Some(child),
            reader,
            writer,
            reaped: false,
        }))
    }
}

struct PortableChild {
    // Optional so `Drop` can reap by `wait`-ing if the driver did not. None after
    // explicit `wait` succeeds.
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
    reader: Box<dyn std::io::Read + Send>,
    writer: Box<dyn std::io::Write + Send>,
    reaped: bool,
}

impl PtyChild for PortableChild {
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)?;
        self.writer.flush()
    }

    fn wait(&mut self) -> io::Result<i32> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "child already reaped"))?;
        let status = child
            .wait()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        self.reaped = true;
        Ok(status.exit_code() as i32)
    }

    fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
    }

    fn killer(&self) -> Box<dyn PtyKiller> {
        // `clone_killer` survives across threads (Send + Sync) and is harmless after
        // the child exits — `kill` on a reaped child is a no-op in `portable-pty`.
        match self.child.as_ref() {
            Some(child) => Box::new(PortableKiller {
                killer: Mutex::new(child.clone_killer()),
            }),
            None => Box::new(NoopKiller),
        }
    }
}

impl Drop for PortableChild {
    /// Zombie reaper — `hq-fe-auth.5`. If the driver task panicked or returned without
    /// calling [`PtyChild::wait`], the kernel would keep the child as a zombie until the
    /// gateway process exits. Force-terminate and wait so every `spawn` is paired with
    /// a `wait` for sure.
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if !self.reaped {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

struct PortableKiller {
    // `portable_pty::ChildKiller::kill` takes `&mut self`. Wrap in a Mutex so the trait
    // object stays `Sync` (multiple watchdog clones are fine; they serialise on the lock).
    killer: Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
}

impl PtyKiller for PortableKiller {
    fn kill(&self) {
        if let Ok(mut k) = self.killer.lock() {
            let _ = k.kill();
        }
    }
}

/// No-op killer returned by adapters whose child has already been reaped. Keeps the
/// watchdog code path uniform — there is no `Option<Killer>`.
struct NoopKiller;
impl PtyKiller for NoopKiller {
    fn kill(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering};
    use std::sync::Arc as TestArc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn fake_pty_blocking_unblocks_on_killer() {
        // Script empty + hang_on_empty: read_chunk blocks. Killer wakes it; read returns
        // Ok(0) so the driver sees EOF after the watchdog fires.
        let pty = FakePty::blocking(Vec::new(), 0);
        let mut child = pty.spawn("noop", &[]).unwrap();
        let killer = child.killer();

        let started = TestArc::new(TestAtomicBool::new(false));
        let started_c = started.clone();
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 16];
            started_c.store(true, TestOrdering::SeqCst);
            child.read_chunk(&mut buf).unwrap()
        });

        while !started.load(TestOrdering::SeqCst) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(20));
        killer.kill();
        let n = handle.join().expect("reader thread");
        assert_eq!(n, 0, "kill surfaces as EOF");
    }

    #[test]
    fn fake_pty_killer_is_idempotent() {
        let pty = FakePty::scripted(vec![b"x".to_vec()], 0);
        let child = pty.spawn("noop", &[]).unwrap();
        let killer = child.killer();
        killer.kill();
        killer.kill(); // must not panic — watchdog races driver.
    }
}
