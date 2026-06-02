//! Daemon supervision: watch a polecat's heartbeat and restart it with backoff.
//!
//! Port of the live half of `internal/daemon` (the parts that supervise polecats). The pure
//! decision of *whether* to restart lives in [`RestartTracker`]; this module is the async edge
//! that watches a real child, decides death (process exit or stale heartbeat), and drives the
//! (re)spawn loop — emitting [`AgentEvent`]s to the relay so the session projection/replay
//! stays authoritative (the gate: "tracked in sessions + AgentEvent log").
//!
//! Like `gt_agent::supervisor`, it never touches the (sync, `!Send`) bus directly: it pushes
//! envelopes to an `mpsc` the bus-owning task drains.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use gt_agent::AgentEvent;
use gt_events::Envelope;

use crate::lifecycle::{heartbeat_is_stale, spawn_process, spawn_tmux, SpawnSpec, SpawnedPolecat};
use crate::restart::{RestartConfig, RestartTracker};
use crate::tmux::Tmux;

/// Why a watched polecat stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchOutcome {
    /// The process exited on its own — normal completion or an external `kill -9`.
    Exited,
    /// The heartbeat went stale; the supervisor killed the (hung) process.
    StaleKilled,
}

impl WatchOutcome {
    /// The death event to record for this outcome.
    fn end_event(self, session: &str) -> AgentEvent {
        match self {
            WatchOutcome::Exited => AgentEvent::SessionEnd {
                session: session.to_string(),
            },
            WatchOutcome::StaleKilled => AgentEvent::Killed {
                session: session.to_string(),
                reason: "heartbeat stale".to_string(),
            },
        }
    }
}

/// Watch a spawned polecat until it dies. Polls the heartbeat every `poll`; if it is older
/// than `stale_after` the process is presumed hung and killed. Removes the heartbeat file
/// before returning so a re-spawn starts clean.
pub async fn watch(p: &mut SpawnedPolecat, stale_after: Duration, poll: Duration) -> WatchOutcome {
    let hb = p.heartbeat.clone();
    let child = p.child_mut();
    let mut tick = tokio::time::interval(poll);
    let outcome = loop {
        tokio::select! {
            _ = child.wait() => break WatchOutcome::Exited,
            _ = tick.tick() => {
                if heartbeat_is_stale(&hb, stale_after) {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    break WatchOutcome::StaleKilled;
                }
            }
        }
    };
    let _ = tokio::fs::remove_file(&hb).await;
    outcome
}

/// Supervision policy for [`supervise_polecat`].
#[derive(Debug, Clone, Copy)]
pub struct RespawnPolicy {
    /// Heartbeat age past which the polecat is presumed hung.
    pub stale_after: Duration,
    /// How often to check the heartbeat.
    pub poll: Duration,
    /// Hard cap on re-spawns before giving up (separate from the crash-loop budget). Use a
    /// large value for an effectively-unbounded production supervisor; small for tests.
    pub max_restarts: u32,
}

impl Default for RespawnPolicy {
    fn default() -> Self {
        Self {
            stale_after: Duration::from_secs(90),
            poll: Duration::from_secs(1),
            max_restarts: u32::MAX,
        }
    }
}

/// Run the spawn → watch → restart loop for one polecat.
///
/// `make_spec` produces a fresh [`SpawnSpec`] for each (re)spawn (so a re-spawn can pick a new
/// run id / refresh env). `tracker` gates restarts with backoff + crash-loop detection;
/// `now_fn` injects unix-seconds at the edge. Stops when the restart budget is exhausted, the
/// tracker refuses (crash loop), or `max_restarts` is hit.
pub async fn supervise_polecat<F, N>(
    agent_id: &str,
    mut make_spec: F,
    tracker: &mut RestartTracker,
    policy: RespawnPolicy,
    events: mpsc::Sender<Envelope<AgentEvent>>,
    now_fn: N,
) -> io::Result<()>
where
    F: FnMut() -> SpawnSpec,
    N: Fn() -> u64,
{
    let mut restarts = 0u32;
    loop {
        let spec = make_spec();
        let session = spec.session.clone();
        let mut p = spawn_process(&spec).await?;
        let _ = events.send(spec.spawned_envelope()).await;

        let outcome = watch(&mut p, policy.stale_after, policy.poll).await;
        let _ = events
            .send(Envelope::root(outcome.end_event(&session)))
            .await;

        if restarts >= policy.max_restarts {
            break;
        }
        let now = now_fn();
        if !tracker.can_restart(agent_id, now) {
            break;
        }
        tracker.record_restart(agent_id, now);
        let backoff = tracker.backoff_remaining(agent_id, now);
        if backoff > 0 {
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
        restarts += 1;
    }
    Ok(())
}

/// Run a long-running daemon loop under restart + backoff supervision — the generic sibling of
/// [`supervise_polecat`] for the in-process role daemons (refinery channel watcher, witness
/// patrol tick, mayor orch loop, …) the composition root boots (hq-mc72.12 C2).
///
/// `run` produces the daemon future for each (re)start; when it resolves — the loop returned
/// (channel abandoned) or crashed — the same [`RestartTracker`] that gates polecats decides
/// whether to restart and how long to back off. Stops on crash-loop, when the tracker refuses,
/// or when `max_restarts` is hit (use `u32::MAX` for an effectively-unbounded daemon). `name`
/// keys the restart bookkeeping; `now_fn` injects unix seconds at the edge.
pub async fn supervise_daemon<F, Fut, N>(
    name: &str,
    mut run: F,
    tracker: &mut RestartTracker,
    max_restarts: u32,
    now_fn: N,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
    N: Fn() -> u64,
{
    let mut restarts = 0u32;
    loop {
        run().await;
        if restarts >= max_restarts {
            break;
        }
        let now = now_fn();
        if !tracker.can_restart(name, now) {
            break;
        }
        tracker.record_restart(name, now);
        let backoff = tracker.backoff_remaining(name, now);
        if backoff > 0 {
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
        restarts += 1;
    }
}

#[cfg(test)]
mod daemon_tests {
    use super::*;
    use crate::restart::RestartConfig;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;

    /// A `Fn() -> u64` clock that advances 1000s per call so each restart's backoff window has
    /// elapsed by the next `can_restart` check (otherwise a fixed `now` parks the loop in its
    /// own backoff). `fetch_add` returns the pre-increment value: 0, 1000, 2000, …
    fn advancing_clock() -> impl Fn() -> u64 {
        let now = Arc::new(AtomicU64::new(0));
        move || now.fetch_add(1000, Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn supervise_daemon_restarts_until_max() {
        // A daemon whose loop returns immediately drives the restart loop fast; paused time
        // makes the backoff sleeps auto-advance so the test stays instant.
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let mut tracker = RestartTracker::new(RestartConfig {
            initial_backoff_secs: 1,
            crash_loop_count: 100,
            ..RestartConfig::default()
        });
        supervise_daemon(
            "d",
            move || {
                let r = r.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                }
            },
            &mut tracker,
            2,
            advancing_clock(),
        )
        .await;
        // initial run + 2 restarts.
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn supervise_daemon_stops_on_crash_loop() {
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        // crash_loop_count=2: once 2 restarts land inside the window the tracker refuses, so
        // the loop stops well before max_restarts.
        let mut tracker = RestartTracker::new(RestartConfig {
            initial_backoff_secs: 1,
            crash_loop_count: 2,
            ..RestartConfig::default()
        });
        supervise_daemon(
            "d",
            move || {
                let r = r.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                }
            },
            &mut tracker,
            u32::MAX,
            advancing_clock(),
        )
        .await;
        // initial + 2 restarts, then crash-loop blocks further restarts.
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }
}

/// Supervises the set of **production tmux polecats** the orchestrator has slung (hq-mc72.12
/// C5). Unlike [`supervise_polecat`] — which owns a single direct-child `tokio::process::Child`
/// — production polecats are detached `tmux` sessions (a coding agent in a pane) created by
/// `RealEffects::sling` via [`crate::PolecatLifecycle`]; this watcher can only observe them by
/// asking `tmux` whether the session still exists. Each [`tick`] is one supervision pass:
/// a watched session whose `tmux has-session` is gone is presumed dead and re-slung (subject
/// to the same [`RestartTracker`] backoff + crash-loop budget polecats already use); a session
/// that is still alive resets its budget; one that has exhausted its budget is dropped.
///
/// `unwatch_member` is how completed work leaves the set: when a slung bead finishes (the
/// composition root observes `MergeEvent::Merged`), the entry whose `hook_bead` matches is
/// removed so finished work is never re-slung. The type is `Send + Sync` (interior `Mutex`) so
/// the same `Arc<PolecatSupervisor>` can be shared by the sling edge (which calls [`watch`]),
/// the reactor (which calls [`unwatch_member`]), and the boot loop (which calls [`tick`]).
///
/// [`tick`]: PolecatSupervisor::tick
/// [`watch`]: PolecatSupervisor::watch
/// [`unwatch_member`]: PolecatSupervisor::unwatch_member
pub struct PolecatSupervisor {
    tmux: Arc<dyn Tmux>,
    state: Mutex<SupervisorState>,
}

struct SupervisorState {
    /// session id -> spec to re-sling it from.
    watched: HashMap<String, SpawnSpec>,
    tracker: RestartTracker,
    /// Per-session re-sling count (separate from the tracker's crash-loop window) — a hard
    /// cap so a permanently-broken polecat is eventually abandoned.
    restarts: HashMap<String, u32>,
    max_restarts: u32,
}

impl PolecatSupervisor {
    pub fn new(tmux: Arc<dyn Tmux>, config: RestartConfig, max_restarts: u32) -> Self {
        Self {
            tmux,
            state: Mutex::new(SupervisorState {
                watched: HashMap::new(),
                tracker: RestartTracker::new(config),
                restarts: HashMap::new(),
                max_restarts,
            }),
        }
    }

    /// Register a freshly-slung polecat so its death is detected and recovered. Keyed by
    /// `spec.session`; re-watching the same session replaces the spec.
    pub fn watch(&self, spec: SpawnSpec) {
        let mut st = self.state.lock().unwrap();
        st.watched.insert(spec.session.clone(), spec);
    }

    /// Stop supervising a session by id (e.g. operator-killed; do not resurrect).
    pub fn unwatch(&self, session: &str) {
        let mut st = self.state.lock().unwrap();
        st.watched.remove(session);
        st.restarts.remove(session);
    }

    /// Stop supervising whatever polecat was slung for `member` (its `hook_bead`). Called when
    /// the work completes so finished beads are never re-slung. Removes every matching entry.
    pub fn unwatch_member(&self, member: &str) {
        let mut st = self.state.lock().unwrap();
        let drop: Vec<String> = st
            .watched
            .iter()
            .filter(|(_, spec)| spec.hook_bead.as_deref() == Some(member))
            .map(|(session, _)| session.clone())
            .collect();
        for session in drop {
            st.watched.remove(&session);
            st.restarts.remove(&session);
        }
    }

    /// Number of polecats currently supervised.
    pub fn watched_count(&self) -> usize {
        self.state.lock().unwrap().watched.len()
    }

    /// One supervision pass. For each watched session: alive (tmux has-session) → reset its
    /// restart budget; dead → re-sling if budget + backoff allow, else drop. Returns how many
    /// were re-slung this pass. `now` is edge-stamped unix seconds (same discipline as the
    /// rest of the kernel). The `tmux` I/O runs while the state lock is held — `tick` is driven
    /// by a single slow timer and `watch`/`unwatch` are rare, so contention is negligible.
    pub fn tick(&self, now: u64) -> usize {
        let mut st = self.state.lock().unwrap();
        let sessions: Vec<String> = st.watched.keys().cloned().collect();
        let mut reslung = 0usize;
        let mut to_drop: Vec<String> = Vec::new();
        for session in sessions {
            if self.tmux.has_session(&session) {
                st.tracker.record_success(&session, now);
                continue;
            }
            // Dead. Check the hard cap and the tracker's backoff / crash-loop gate.
            let count = *st.restarts.get(&session).unwrap_or(&0);
            if count >= st.max_restarts || !st.tracker.can_restart(&session, now) {
                to_drop.push(session);
                continue;
            }
            st.tracker.record_restart(&session, now);
            let spec = st.watched.get(&session).cloned();
            if let Some(spec) = spec {
                match spawn_tmux(self.tmux.as_ref(), &spec) {
                    Ok(()) => {
                        *st.restarts.entry(session).or_insert(0) += 1;
                        reslung += 1;
                    }
                    Err(e) => {
                        eprintln!("[polecat-supervisor] re-sling failed session={session}: {e}");
                    }
                }
            }
        }
        for session in to_drop {
            st.watched.remove(&session);
            st.restarts.remove(&session);
            eprintln!(
                "[polecat-supervisor] giving up on session={session} (restart budget exhausted)"
            );
        }
        reslung
    }
}

#[cfg(test)]
mod polecat_supervisor_tests {
    use super::*;
    use crate::tmux::FakeTmux;

    fn spec(session: &str, member: &str) -> SpawnSpec {
        SpawnSpec {
            session: session.to_string(),
            rig: "granite".to_string(),
            polecat: session.to_string(),
            crew: None,
            workdir: std::env::temp_dir(),
            command: "sleep".to_string(),
            args: vec!["30".to_string()],
            env: vec![("GT_ROLE".to_string(), "polecat".to_string())],
            hook_bead: Some(member.to_string()),
            issue: None,
            heartbeat: std::env::temp_dir().join(format!("{session}.hb")),
        }
    }

    #[test]
    fn dead_session_is_reslung_alive_is_left_alone() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig {
                initial_backoff_secs: 1,
                crash_loop_count: 100,
                ..RestartConfig::default()
            },
            u32::MAX,
        );
        // Sling it once (the session now exists) and watch it.
        let s = spec("gt-furiosa", "hq-1");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        assert!(tmux.has_session("gt-furiosa"));

        // Alive → no re-sling.
        assert_eq!(sup.tick(0), 0);

        // Kill it → next tick re-slings (session re-created).
        tmux.kill_session("gt-furiosa").unwrap();
        assert!(!tmux.has_session("gt-furiosa"));
        assert_eq!(sup.tick(1000), 1);
        assert!(tmux.has_session("gt-furiosa"), "re-slung session exists again");
    }

    #[test]
    fn unwatch_member_stops_resurrection() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), u32::MAX);
        let s = spec("gt-nux", "hq-7");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        assert_eq!(sup.watched_count(), 1);

        // Work completed → unwatch by member; a dead session is now NOT re-slung.
        sup.unwatch_member("hq-7");
        assert_eq!(sup.watched_count(), 0);
        tmux.kill_session("gt-nux").unwrap();
        assert_eq!(sup.tick(1000), 0);
        assert!(!tmux.has_session("gt-nux"), "completed polecat stays dead");
    }

    #[test]
    fn restart_budget_exhaustion_drops_the_entry() {
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig {
                initial_backoff_secs: 1,
                crash_loop_count: 1000,
                ..RestartConfig::default()
            },
            2, // hard cap: 2 re-slings then give up
        );
        let s = spec("gt-slit", "hq-9");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);

        // Kill + tick three times with an advancing clock so backoff windows pass. The first
        // two ticks re-sling; the third hits the cap and drops the entry.
        for (i, now) in [1000u64, 2000, 3000].into_iter().enumerate() {
            tmux.kill_session("gt-slit").unwrap();
            let reslung = sup.tick(now);
            if i < 2 {
                assert_eq!(reslung, 1, "tick {i} should re-sling");
            } else {
                assert_eq!(reslung, 0, "tick {i} is past the cap");
            }
        }
        assert_eq!(sup.watched_count(), 0, "exhausted polecat dropped");
    }
}
