//! [`Supervisor`] — the actor-stack lifecycle a live [`RootHandle`] owns.
//!
//! A hydrated [`RootHandle`](crate::RootHandle) is more than a composed module
//! registry: a tenant's modules run background actor tasks (mayor, sheriff,
//! witness, refinery, deacon — `hq-mt-runtime.8`). Those tasks share one
//! lifecycle, and the registry that hydrates and evicts handles
//! (`hq-mt-routing.4` idle teardown) needs a single, idempotent seam to start
//! them, drain them gracefully, and cancel them.
//!
//! This is that seam (`hq-mt-routing.2`). It is deliberately **actor-agnostic**:
//! the concrete actor crates (`gt-mayor`, `gt-sheriff`, …) migrate up from
//! gastown in Phase 4, so today the supervisor manages *zero* tasks and every
//! transition is a trivially-correct no-op. A future module registers its actor
//! with [`Supervisor::spawn_actor`] before [`start`](Supervisor::start); the
//! lifecycle contract below is what that actor must honour, and it is fully
//! exercised by the tests in this module using stand-in tasks.
//!
//! ## Why here, not the kernel
//!
//! Spawning and cancelling tasks is forbidden in kernel crates (no
//! `tokio::spawn`, docs/03). `gt-runtime` is `domain/orchestration` — the lowest
//! tier allowed to spawn — so the actor lifecycle a root owns lives here, beside
//! the [`RootRegistry`](crate::RootRegistry) that drives it.
//!
//! ## Lifecycle
//!
//! ```text
//!   Built ──start()──▶ Started ──drain()──▶ Draining ──shutdown()──▶ Stopped
//!     │                   │                                              ▲
//!     └───────────────────┴──────────── shutdown() ─────────────────────┘
//! ```
//!
//! Every transition is **idempotent and monotonic**: a phase never moves
//! backward, and calling a hook whose work is already done is a no-op. The
//! current [`Phase`] is broadcast on a [`watch`] channel; each actor holds a
//! [`watch::Receiver<Phase>`] and reacts:
//!
//! - **`Draining`** — stop accepting new work, let the in-flight queue finish,
//!   then exit. [`drain`](Supervisor::drain) awaits that natural completion.
//! - **`Stopped`** — cancel now. [`shutdown`](Supervisor::shutdown) broadcasts it
//!   and awaits the tasks; [`Drop`] broadcasts it best-effort and the owned
//!   [`JoinSet`] aborts anything still running.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::{watch, Mutex};
use tokio::task::JoinSet;

/// A point in the actor-stack lifecycle. Ordered: a handle only moves forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Phase {
    /// Composed but not running — no actor tasks spawned yet.
    Built = 0,
    /// [`start`](Supervisor::start) ran; actor tasks are live.
    Started = 1,
    /// [`drain`](Supervisor::drain) ran; actors finish in-flight work then exit.
    Draining = 2,
    /// Cancelled and joined; the supervisor owns no live tasks.
    Stopped = 3,
}

impl Phase {
    fn from_u8(v: u8) -> Phase {
        match v {
            0 => Phase::Built,
            1 => Phase::Started,
            2 => Phase::Draining,
            _ => Phase::Stopped,
        }
    }
}

/// A future an actor runs for the life of the stack. Owns its work; observes the
/// lifecycle through the [`watch::Receiver<Phase>`] it is handed at spawn.
type ActorFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Builds an actor's [`ActorFuture`] from the lifecycle receiver it should watch.
/// Run once, by [`start`](Supervisor::start), so it is `FnOnce`.
type ActorFactory = Box<dyn FnOnce(watch::Receiver<Phase>) -> ActorFuture + Send + 'static>;

/// Tasks waiting to spawn plus the ones already running. Behind the supervisor's
/// async [`Mutex`] so a lifecycle transition sees a consistent view.
#[derive(Default)]
struct Stack {
    /// Registered before `start`, drained into `tasks` when it runs.
    pending: Vec<ActorFactory>,
    /// Live actor tasks; dropping the set aborts them (the `Drop` safety net).
    tasks: JoinSet<()>,
}

/// The actor-stack lifecycle for one [`RootHandle`](crate::RootHandle).
///
/// Holds the registered/running actor tasks and the [`Phase`] broadcast they
/// react to. See the [module docs](self) for the state machine and contract.
pub struct Supervisor {
    /// Cheap, lock-free read of the current phase (for `/health`,
    /// `hq-mt-routing.8`); mirrors the value on `phases`.
    phase: AtomicU8,
    /// Broadcasts phase changes to every actor's [`watch::Receiver`].
    phases: watch::Sender<Phase>,
    stack: Mutex<Stack>,
}

impl Default for Supervisor {
    fn default() -> Self {
        let (phases, _) = watch::channel(Phase::Built);
        Supervisor {
            phase: AtomicU8::new(Phase::Built as u8),
            phases,
            stack: Mutex::new(Stack::default()),
        }
    }
}

impl Supervisor {
    /// A fresh supervisor in [`Phase::Built`] with no actors.
    pub fn new() -> Self {
        Supervisor::default()
    }

    /// The current lifecycle [`Phase`]. Lock-free; safe to poll from anywhere.
    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// A receiver an actor watches to observe [`Draining`](Phase::Draining) and
    /// [`Stopped`](Phase::Stopped). Usually obtained for you at spawn; exposed so
    /// out-of-band observers (a health probe) can subscribe too.
    pub fn subscribe(&self) -> watch::Receiver<Phase> {
        self.phases.subscribe()
    }

    /// Register an actor to spawn when [`start`](Self::start) runs.
    ///
    /// `factory` is handed a [`watch::Receiver<Phase>`] and returns the future to
    /// run. Only valid while [`Built`](Phase::Built): once started, the stack is
    /// fixed, so a late registration is rejected with the current phase. Future
    /// modules call this during hydration, before the handle starts.
    pub async fn spawn_actor<F, Fut>(&self, factory: F) -> Result<(), Phase>
    where
        F: FnOnce(watch::Receiver<Phase>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let phase = self.phase();
        if phase != Phase::Built {
            return Err(phase);
        }
        let mut stack = self.stack.lock().await;
        // Re-check under the lock: a concurrent `start` may have just moved on.
        if self.phase() != Phase::Built {
            return Err(self.phase());
        }
        stack
            .pending
            .push(Box::new(move |rx| Box::pin(factory(rx))));
        Ok(())
    }

    /// Spawn the registered actors. `Built -> Started`, idempotent: a second call
    /// (or one on an already-draining/stopped handle) is a no-op returning
    /// `false`. Returns `true` only on the transition that actually started them.
    ///
    /// Must run inside a Tokio runtime — it spawns the actor tasks.
    pub async fn start(&self) -> bool {
        let mut stack = self.stack.lock().await;
        if self.phase() != Phase::Built {
            return false;
        }
        let pending = std::mem::take(&mut stack.pending);
        for factory in pending {
            let rx = self.phases.subscribe();
            stack.tasks.spawn(factory(rx));
        }
        self.set_phase(Phase::Started);
        true
    }

    /// Graceful stop: broadcast [`Draining`](Phase::Draining), then await every
    /// actor finishing its in-flight work. Idempotent — returns immediately once
    /// the stack is already draining or stopped (and never moves the phase back
    /// below `Draining`).
    pub async fn drain(&self) {
        if self.phase() >= Phase::Draining {
            return;
        }
        let mut stack = self.stack.lock().await;
        if self.phase() >= Phase::Draining {
            return;
        }
        // Announce before awaiting so watching actors begin winding down.
        self.set_phase(Phase::Draining);
        Supervisor::join_all(&mut stack.tasks).await;
    }

    /// Hard stop: broadcast [`Stopped`](Phase::Stopped) so actors cancel now, then
    /// await them. Idempotent — a no-op once already stopped.
    pub async fn shutdown(&self) {
        if self.phase() == Phase::Stopped {
            return;
        }
        let mut stack = self.stack.lock().await;
        if self.phase() == Phase::Stopped {
            return;
        }
        self.set_phase(Phase::Stopped);
        Supervisor::join_all(&mut stack.tasks).await;
    }

    /// Drive `tasks` to completion, discarding results (a panicked actor must not
    /// abort teardown of its siblings).
    async fn join_all(tasks: &mut JoinSet<()>) {
        while tasks.join_next().await.is_some() {}
    }

    /// Move to `phase` and broadcast it. Monotonic by construction — every caller
    /// guards the transition first.
    fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
        // A send error only means no actor is listening; the phase still moved.
        let _ = self.phases.send(phase);
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // Best-effort cancel for any detached observer; the owned `JoinSet` is
        // dropped right after, which aborts every still-running task. We cannot
        // await here, so this is the un-graceful path — prefer `shutdown().await`.
        if self.phase() != Phase::Stopped {
            self.phase.store(Phase::Stopped as u8, Ordering::Release);
            let _ = self.phases.send(Phase::Stopped);
        }
    }
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor").field("phase", &self.phase()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Notify;

    use super::*;

    /// An actor that loops until it observes `>= until`, then exits. Pings
    /// `started` once it is live so the test can sequence without sleeps.
    fn looping_actor(
        until: Phase,
        started: Arc<Notify>,
    ) -> impl FnOnce(watch::Receiver<Phase>) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
    {
        move |mut rx| {
            Box::pin(async move {
                started.notify_one();
                loop {
                    if *rx.borrow_and_update() >= until {
                        return;
                    }
                    if rx.changed().await.is_err() {
                        return; // sender gone — supervisor dropped.
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn starts_idempotently() {
        let sup = Supervisor::new();
        assert_eq!(sup.phase(), Phase::Built);
        assert!(sup.start().await, "first start transitions");
        assert_eq!(sup.phase(), Phase::Started);
        assert!(!sup.start().await, "second start is a no-op");
    }

    #[tokio::test]
    async fn drain_awaits_actor_that_stops_on_draining() {
        let sup = Supervisor::new();
        let started = Arc::new(Notify::new());
        sup.spawn_actor(looping_actor(Phase::Draining, started.clone()))
            .await
            .unwrap();
        sup.start().await;
        started.notified().await; // actor is live and looping.

        // Would hang forever if drain did not broadcast Draining.
        sup.drain().await;
        assert_eq!(sup.phase(), Phase::Draining);
    }

    #[tokio::test]
    async fn shutdown_cancels_actor_that_ignores_draining() {
        let sup = Supervisor::new();
        let started = Arc::new(Notify::new());
        // This actor only exits on Stopped — drain alone never frees it.
        sup.spawn_actor(looping_actor(Phase::Stopped, started.clone()))
            .await
            .unwrap();
        sup.start().await;
        started.notified().await;

        sup.shutdown().await;
        assert_eq!(sup.phase(), Phase::Stopped);
        // Idempotent: a second shutdown returns at once.
        sup.shutdown().await;
        assert_eq!(sup.phase(), Phase::Stopped);
    }

    #[tokio::test]
    async fn register_after_start_is_rejected() {
        let sup = Supervisor::new();
        sup.start().await;
        let err = sup
            .spawn_actor(|_rx| async {})
            .await
            .expect_err("no registration once started");
        assert_eq!(err, Phase::Started);
    }

    #[tokio::test]
    async fn empty_stack_drains_and_shuts_down_immediately() {
        // The Phase-4-pending reality: zero actors, every transition trivial.
        let sup = Supervisor::new();
        sup.start().await;
        sup.drain().await;
        assert_eq!(sup.phase(), Phase::Draining);
        sup.shutdown().await;
        assert_eq!(sup.phase(), Phase::Stopped);
    }

    #[tokio::test]
    async fn drop_signals_stopped_to_observers() {
        let sup = Supervisor::new();
        let rx = sup.subscribe();
        sup.start().await;
        drop(sup);
        // A detached observer sees the cancel broadcast on drop.
        assert_eq!(*rx.borrow(), Phase::Stopped);
    }

    #[tokio::test]
    async fn phase_is_monotonic_drain_then_no_backslide() {
        let sup = Supervisor::new();
        sup.start().await;
        sup.shutdown().await;
        assert_eq!(sup.phase(), Phase::Stopped);
        // drain after shutdown must not move the phase back to Draining.
        sup.drain().await;
        assert_eq!(sup.phase(), Phase::Stopped);
    }
}
