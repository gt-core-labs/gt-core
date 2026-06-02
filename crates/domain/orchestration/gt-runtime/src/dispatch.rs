//! Autonomous dispatcher (`hq-auto.1`) — the Execute tier of the MAPE-K loop.
//!
//! Today a worker is spawned per bead by hand. This is the keystone of
//! *avanza-sin-humano*: a host-side loop that polls the ready frontier
//! (`gt://issues?ready=true`), and for each ready bead claims it and spawns a
//! worker — up to a concurrency bound, never re-spawning a bead already in
//! flight. When the in-flight set is at the bound the loop applies backpressure
//! by simply dispatching nothing until a slot frees.
//!
//! ## Ports, not adapters
//!
//! The two seams to the outside world are traits, so the core is pure policy and
//! unit-testable with in-memory fakes:
//!
//! - [`ReadySource`] — *where ready beads come from*. The production adapter is
//!   the MCP `gt://issues?ready=true` poll (paginated, unbounded — see the issues
//!   pager); a test supplies a scripted list.
//! - [`Worker`] — *what taking a bead means*. The production adapter claims the
//!   bead (`issues.transition` open→working) and spawns the worker process; a
//!   test records the call. Re-hosting the retired orchestrator's full
//!   scheduling/agent tooling is `hq-core-port`; this is the minimal, non-P4
//!   dispatcher that lives on `gt-runtime` (the orchestration tier — the only one
//!   allowed to `tokio::spawn`, docs/03).
//!
//! ## In-flight accounting and the completion signal
//!
//! The dispatcher owns the in-flight set: a bead enters it the instant it is
//! dispatched and leaves it when the host reports the worker finished via
//! [`Dispatcher::complete`]. Completion is necessarily external — the dispatcher
//! launches a worker, it does not wait on the process; the worker's exit hook
//! calls back. The bound is enforced against this set, so a crashed worker that
//! never reports completion holds its slot until the host reconciles it (the
//! lease/WIP layer that `hq-core-port` brings); here the slot is freed only by an
//! explicit [`complete`](Dispatcher::complete).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Identifier of a tracked bead, e.g. `hq-auto.1`. A thin newtype so the
/// dispatcher's in-flight set and the port signatures cannot be confused with an
/// arbitrary string.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BeadId(String);

impl BeadId {
    /// Wrap a bead id. No validation: ids are minted by the tracker, not here.
    pub fn new(id: impl Into<String>) -> Self {
        BeadId(id.into())
    }

    /// The underlying id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for BeadId {
    fn from(s: &str) -> Self {
        BeadId(s.to_string())
    }
}

impl From<String> for BeadId {
    fn from(s: String) -> Self {
        BeadId(s)
    }
}

/// Why a dispatch could not be started. Opaque to the policy core, which only
/// needs to know a dispatch succeeded or failed (a failed one frees its slot so
/// a later tick retries the bead).
#[derive(Debug)]
pub struct DispatchError(pub String);

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dispatch failed: {}", self.0)
    }
}

impl std::error::Error for DispatchError {}

/// The ready frontier: every bead currently eligible to start. The production
/// adapter is the `gt://issues?ready=true` poll, walked to completion (the cap is
/// gone; it returns the full unbounded set). Order is the source's; the
/// dispatcher does not assume any.
pub trait ReadySource {
    /// Snapshot of ready bead ids. Called once per tick.
    fn ready(&self) -> impl std::future::Future<Output = Vec<BeadId>> + Send;
}

/// Taking a bead: claim it (open→working) and spawn its worker. Returns once the
/// worker is *launched*, not once it finishes — completion comes back later via
/// [`Dispatcher::complete`]. A returned [`DispatchError`] means the bead was not
/// started (claim race lost, spawn failed), so its slot is released for retry.
pub trait Worker {
    /// Claim and spawn a worker for `bead`.
    fn dispatch(
        &self,
        bead: &BeadId,
    ) -> impl std::future::Future<Output = Result<(), DispatchError>> + Send;
}

/// Pure dispatch policy: from the ready frontier, the in-flight set, and the
/// concurrency bound, decide which beads to start this tick.
///
/// Three rules, in order:
/// 1. **Dedupe** — skip any bead already in flight (no double-spawn).
/// 2. **Bound / backpressure** — start at most `free = max_in_flight - in_flight`
///    beads; when the bound is reached `free` is zero and nothing starts.
/// 3. **Order** — preserve the source's order among the survivors, so a
///    priority-ordered frontier is honored without the policy sorting.
///
/// Pure and synchronous: this is the unit the policy tests pin down, separate
/// from the async I/O of [`Dispatcher::tick`].
pub fn plan(ready: &[BeadId], in_flight: &HashSet<BeadId>, max_in_flight: usize) -> Vec<BeadId> {
    let free = max_in_flight.saturating_sub(in_flight.len());
    if free == 0 {
        return Vec::new();
    }
    ready
        .iter()
        .filter(|b| !in_flight.contains(*b))
        .take(free)
        .cloned()
        .collect()
}

/// The autonomous dispatcher: drives [`ReadySource`] → [`plan`] → [`Worker`] on a
/// cadence, tracking the in-flight set against a concurrency bound.
pub struct Dispatcher<S, W> {
    source: S,
    worker: W,
    max_in_flight: usize,
    /// Beads dispatched and not yet reported complete. Async mutex because it is
    /// held across the `await` on [`Worker::dispatch`] within a tick.
    in_flight: Mutex<HashSet<BeadId>>,
}

impl<S: ReadySource, W: Worker> Dispatcher<S, W> {
    /// Build a dispatcher bounded to `max_in_flight` concurrent workers.
    ///
    /// `max_in_flight` of 0 dispatches nothing — a valid paused state, useful as
    /// a kill switch — though a bound of at least 1 is the normal case.
    pub fn new(source: S, worker: W, max_in_flight: usize) -> Self {
        Dispatcher { source, worker, max_in_flight, in_flight: Mutex::new(HashSet::new()) }
    }

    /// Number of beads currently in flight.
    pub async fn in_flight_len(&self) -> usize {
        self.in_flight.lock().await.len()
    }

    /// Mark `bead` finished, freeing its slot. The host's worker-exit hook calls
    /// this; idempotent — completing a bead not in flight is a no-op. Returns
    /// whether a slot was actually freed.
    pub async fn complete(&self, bead: &BeadId) -> bool {
        self.in_flight.lock().await.remove(bead)
    }

    /// One dispatch cycle: poll the frontier, plan within the free slots, and
    /// spawn a worker per chosen bead. Returns the beads actually started.
    ///
    /// A bead is inserted into the in-flight set *before* its
    /// [`Worker::dispatch`] is awaited so the bound holds even if two cycles
    /// overlap; if the dispatch fails the bead is removed again so a later tick
    /// retries it. The frontier is read once and the plan computed under the
    /// in-flight lock, so a bead cannot be chosen twice within a tick.
    pub async fn tick(&self) -> Vec<BeadId> {
        let ready = self.source.ready().await;
        // Hold the lock across planning + reservation so concurrent ticks cannot
        // both reserve the same slot; release it before the (potentially slow)
        // spawns by snapshotting the chosen beads first.
        let chosen = {
            let mut in_flight = self.in_flight.lock().await;
            let chosen = plan(&ready, &in_flight, self.max_in_flight);
            for bead in &chosen {
                in_flight.insert(bead.clone());
            }
            chosen
        };

        let mut started = Vec::with_capacity(chosen.len());
        for bead in chosen {
            match self.worker.dispatch(&bead).await {
                Ok(()) => started.push(bead),
                Err(_) => {
                    // Failed launch: release the reserved slot so the bead is
                    // eligible again next tick rather than stranded in flight.
                    self.in_flight.lock().await.remove(&bead);
                }
            }
        }
        started
    }

    /// Drive [`tick`](Self::tick) on a fixed cadence until the task is aborted.
    ///
    /// The first tick fires immediately, then every `tick` interval thereafter —
    /// the same cadence shape as the idle reaper. This borrows `&self`; the host
    /// either holds the dispatcher for the process life and `select!`s this
    /// against shutdown, or uses [`spawn`](Self::spawn) for a detached loop.
    pub async fn run(&self, tick: Duration) {
        let mut interval = tokio::time::interval(tick);
        loop {
            interval.tick().await;
            self.tick().await;
        }
    }
}

impl<S, W> Dispatcher<S, W>
where
    S: ReadySource + Send + Sync + 'static,
    W: Worker + Send + Sync + 'static,
{
    /// Spawn the dispatch loop as a detached task, returning its [`JoinHandle`]
    /// so the host can abort it on shutdown. Spawning belongs to this
    /// orchestration-tier crate, not the kernel (docs/03).
    pub fn spawn(self: Arc<Self>, tick: Duration) -> JoinHandle<()> {
        tokio::spawn(async move { self.run(tick).await })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    fn ids(slugs: &[&str]) -> Vec<BeadId> {
        slugs.iter().map(|s| BeadId::from(*s)).collect()
    }

    fn set(slugs: &[&str]) -> HashSet<BeadId> {
        slugs.iter().map(|s| BeadId::from(*s)).collect()
    }

    // ---- plan(): the pure policy core --------------------------------------

    #[test]
    fn plan_respects_bound() {
        let ready = ids(&["a", "b", "c", "d"]);
        let chosen = plan(&ready, &HashSet::new(), 2);
        assert_eq!(chosen, ids(&["a", "b"]), "at most `max_in_flight` started");
    }

    #[test]
    fn plan_skips_in_flight() {
        let ready = ids(&["a", "b", "c"]);
        let chosen = plan(&ready, &set(&["a"]), 3);
        assert_eq!(chosen, ids(&["b", "c"]), "a bead already in flight is not re-spawned");
    }

    #[test]
    fn plan_backpressure_at_bound() {
        let ready = ids(&["a", "b", "c"]);
        let chosen = plan(&ready, &set(&["x", "y"]), 2);
        assert!(chosen.is_empty(), "no free slots => nothing dispatched");
    }

    #[test]
    fn plan_free_slots_account_for_in_flight() {
        let ready = ids(&["a", "b", "c", "d"]);
        // 1 free slot (bound 3, one in flight, and `a` itself in flight is skipped).
        let chosen = plan(&ready, &set(&["a", "z"]), 3);
        assert_eq!(chosen, ids(&["b"]), "one free slot, first eligible in source order");
    }

    #[test]
    fn plan_preserves_source_order() {
        let ready = ids(&["hi", "mid", "lo"]);
        let chosen = plan(&ready, &HashSet::new(), 10);
        assert_eq!(chosen, ids(&["hi", "mid", "lo"]), "frontier order is honored");
    }

    #[test]
    fn plan_zero_bound_dispatches_nothing() {
        let chosen = plan(&ids(&["a"]), &HashSet::new(), 0);
        assert!(chosen.is_empty(), "a zero bound is a paused dispatcher");
    }

    // ---- in-memory ports ---------------------------------------------------

    /// Returns a fixed ready frontier each tick. Models a static frontier.
    struct FixedSource(Vec<BeadId>);
    impl ReadySource for FixedSource {
        async fn ready(&self) -> Vec<BeadId> {
            self.0.clone()
        }
    }

    /// A mutable frontier the test edits to model the real `ready=true` source:
    /// a claimed bead becomes `working` and so drops out of the ready set.
    struct MutSource(Arc<StdMutex<Vec<BeadId>>>);
    impl MutSource {
        fn new(slugs: &[&str]) -> (Self, Arc<StdMutex<Vec<BeadId>>>) {
            let inner = Arc::new(StdMutex::new(ids(slugs)));
            (MutSource(Arc::clone(&inner)), inner)
        }
    }
    impl ReadySource for MutSource {
        async fn ready(&self) -> Vec<BeadId> {
            self.0.lock().unwrap().clone()
        }
    }

    /// Records every dispatch *attempt* (success or failure); optionally fails on
    /// a named bead for its first `fail_times` attempts, then succeeds.
    struct RecordingWorker {
        attempts: StdMutex<Vec<BeadId>>,
        succeeded: StdMutex<Vec<BeadId>>,
        fail_on: Option<BeadId>,
        fail_times: StdMutex<usize>,
    }
    impl RecordingWorker {
        fn new() -> Self {
            RecordingWorker {
                attempts: StdMutex::new(Vec::new()),
                succeeded: StdMutex::new(Vec::new()),
                fail_on: None,
                fail_times: StdMutex::new(0),
            }
        }
        /// Fails on `bead` for its first `times` attempts, then succeeds.
        fn failing(bead: &str, times: usize) -> Self {
            RecordingWorker {
                attempts: StdMutex::new(Vec::new()),
                succeeded: StdMutex::new(Vec::new()),
                fail_on: Some(BeadId::from(bead)),
                fail_times: StdMutex::new(times),
            }
        }
        fn calls(&self) -> Vec<BeadId> {
            self.succeeded.lock().unwrap().clone()
        }
        fn attempts(&self) -> Vec<BeadId> {
            self.attempts.lock().unwrap().clone()
        }
    }
    impl Worker for RecordingWorker {
        async fn dispatch(&self, bead: &BeadId) -> Result<(), DispatchError> {
            self.attempts.lock().unwrap().push(bead.clone());
            if self.fail_on.as_ref() == Some(bead) {
                let mut left = self.fail_times.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    return Err(DispatchError("scripted failure".into()));
                }
            }
            self.succeeded.lock().unwrap().push(bead.clone());
            Ok(())
        }
    }

    // ---- tick(): the async driver ------------------------------------------

    #[tokio::test]
    async fn tick_dispatches_up_to_bound_and_marks_in_flight() {
        let d = Dispatcher::new(FixedSource(ids(&["a", "b", "c"])), RecordingWorker::new(), 2);
        let started = d.tick().await;
        assert_eq!(started, ids(&["a", "b"]));
        assert_eq!(d.in_flight_len().await, 2, "started beads occupy slots");
    }

    #[tokio::test]
    async fn second_tick_skips_in_flight_then_completion_frees_a_slot() {
        // Frontier [a,b,c], bound 2. A claimed bead leaves `ready` (it is now
        // `working`), modeled by removing it from the source on dispatch.
        let (source, frontier) = MutSource::new(&["a", "b", "c"]);
        let d = Dispatcher::new(source, RecordingWorker::new(), 2);

        assert_eq!(d.tick().await, ids(&["a", "b"]));
        // a,b are now `working`: the real source would stop listing them.
        *frontier.lock().unwrap() = ids(&["c"]);
        assert!(d.tick().await.is_empty(), "bound reached: backpressure, nothing new");

        // A worker finishes -> one slot frees -> the remaining ready bead starts.
        assert!(d.complete(&BeadId::from("a")).await);
        assert_eq!(d.tick().await, ids(&["c"]), "freed slot picks the next ready bead");
        assert_eq!(d.in_flight_len().await, 2, "b (still running) + c");
    }

    #[tokio::test]
    async fn failed_dispatch_releases_the_slot_for_retry() {
        // `a` fails its first attempt, then succeeds; frontier keeps listing it
        // until it is taken (a failed claim leaves it `open`, still ready).
        let d = Dispatcher::new(FixedSource(ids(&["a", "b"])), RecordingWorker::failing("a", 1), 5);
        let started = d.tick().await;
        assert_eq!(started, ids(&["b"]), "only the successful dispatch is reported started");
        assert_eq!(d.in_flight_len().await, 1, "failed `a` did not keep its slot");

        // Next tick retries `a` (still ready, no longer in flight) — now it sticks.
        let started2 = d.tick().await;
        assert_eq!(started2, ids(&["a"]), "the failed bead is retried and starts");
        assert_eq!(d.worker.attempts(), ids(&["a", "b", "a"]), "a was attempted twice");
    }

    #[tokio::test]
    async fn complete_unknown_bead_is_noop() {
        let d = Dispatcher::new(FixedSource(vec![]), RecordingWorker::new(), 1);
        assert!(!d.complete(&BeadId::from("ghost")).await, "completing an absent bead frees nothing");
    }

    #[tokio::test]
    async fn worker_sees_exactly_the_started_beads() {
        let worker = RecordingWorker::new();
        let d = Dispatcher::new(FixedSource(ids(&["a", "b", "c", "d"])), worker, 3);
        d.tick().await;
        // Borrow the worker back through the dispatcher to read its log.
        assert_eq!(d.worker.calls(), ids(&["a", "b", "c"]));
    }
}
