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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use regex::Regex;
use tokio::sync::mpsc;

use gt_agent::AgentEvent;
use gt_events::Envelope;

use crate::lifecycle::{heartbeat_is_stale, spawn_process, spawn_tmux, SpawnSpec, SpawnedPolecat};
use crate::restart::{RestartConfig, RestartTracker};
use crate::tmux::Tmux;

/// Context-usage percentage at or above which a polecat's self-exit is read as a death by
/// context exhaustion rather than a clean completion (gtcore-91fdde). Claude Code surfaces an
/// `N% context used` indicator in its TUI; once the window is nearly full the agent can no
/// longer make progress and bails, which looks identical to a normal exit unless we inspect
/// the pane.
const CONTEXT_EXHAUSTION_THRESHOLD: u8 = 85;

/// Why a watched polecat stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchOutcome {
    /// The process exited on its own — normal completion or an external `kill -9`.
    Exited,
    /// The process exited on its own, but its pane showed claude at or above
    /// [`CONTEXT_EXHAUSTION_THRESHOLD`]% context used — the death is presumed to be context
    /// exhaustion, not a clean finish (gtcore-91fdde). `context_pct` is the parsed percentage,
    /// so the layer above can distinguish "ran out of room" from a normal `Exited`.
    ContextExhausted { context_pct: u8 },
    /// The heartbeat went stale; the supervisor killed the (hung) process.
    StaleKilled,
}

impl WatchOutcome {
    /// Classify a self-exit from the last lines captured off the polecat's tmux pane: if the
    /// pane carries an `N% context used` marker with `N >=` [`CONTEXT_EXHAUSTION_THRESHOLD`],
    /// the exit is [`WatchOutcome::ContextExhausted`]; otherwise (no marker, or below the
    /// threshold) it is a plain [`WatchOutcome::Exited`].
    fn classify_exit(pane_tail: &str) -> WatchOutcome {
        match parse_context_pct(pane_tail) {
            Some(pct) if pct >= CONTEXT_EXHAUSTION_THRESHOLD => {
                WatchOutcome::ContextExhausted { context_pct: pct }
            }
            _ => WatchOutcome::Exited,
        }
    }

    /// The death event to record for this outcome. A context-exhaustion death is recorded as a
    /// [`AgentEvent::Killed`] whose reason carries the parsed percentage, so the audit log keeps
    /// the context figure (gtcore-91fdde) without a new event kind.
    fn end_event(self, session: &str) -> AgentEvent {
        match self {
            WatchOutcome::Exited => AgentEvent::SessionEnd {
                session: session.to_string(),
            },
            WatchOutcome::ContextExhausted { context_pct } => AgentEvent::Killed {
                session: session.to_string(),
                reason: format!("context exhausted: {context_pct}% context used"),
            },
            WatchOutcome::StaleKilled => AgentEvent::Killed {
                session: session.to_string(),
                reason: "heartbeat stale".to_string(),
            },
        }
    }
}

/// Parse the highest-line `N% context used` indicator out of captured pane text, returning the
/// percentage (clamped to 100). Scans every match and keeps the **last** one — captured
/// scrollback is oldest-first, so the final occurrence is claude's most recent context reading.
/// `None` when the text carries no such marker.
fn parse_context_pct(output: &str) -> Option<u8> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // Case-insensitive; tolerant of the spacing tmux/claude may render between the figure
        // and the label. The percentage is 1–3 digits.
        Regex::new(r"(?i)(\d{1,3})%\s*context used").expect("static regex compiles")
    });
    let pct = re
        .captures_iter(output)
        .filter_map(|c| c.get(1)?.as_str().parse::<u32>().ok())
        .last()?;
    Some(pct.min(100) as u8)
}

/// Watch a spawned polecat until it dies. Polls the heartbeat every `poll`; if it is older
/// than `stale_after` the process is presumed hung and killed. Removes the heartbeat file
/// before returning so a re-spawn starts clean.
///
/// When the process exits on its own, `capture_pane` is invoked once to read the last lines of
/// the polecat's tmux pane (`tmux capture-pane`); the captured text is classified by
/// [`WatchOutcome::classify_exit`] so a death by context exhaustion is told apart from a clean
/// finish (gtcore-91fdde). Callers with no pane to inspect (the direct-child path, whose stdout
/// is `/dev/null`) pass a closure returning `None`, which collapses to a plain
/// [`WatchOutcome::Exited`] — no behavior change. The closure is only called on the self-exit
/// branch, never when the heartbeat-stale kill fires.
pub async fn watch(
    p: &mut SpawnedPolecat,
    stale_after: Duration,
    poll: Duration,
    capture_pane: impl Fn() -> Option<String>,
) -> WatchOutcome {
    let hb = p.heartbeat.clone();
    let child = p.child_mut();
    let mut tick = tokio::time::interval(poll);
    let outcome = loop {
        tokio::select! {
            _ = child.wait() => {
                break match capture_pane() {
                    Some(tail) => WatchOutcome::classify_exit(&tail),
                    None => WatchOutcome::Exited,
                };
            }
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

        // The direct-child path runs the polecat with stdout/stderr → /dev/null (no tmux pane),
        // so there is nothing to inspect for a context marker: capture yields `None` and the
        // self-exit stays a plain `Exited`. The tmux-backed production path (`PolecatSupervisor`)
        // is where a real `capture_pane` would be wired.
        let outcome = watch(&mut p, policy.stale_after, policy.poll, || None).await;
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
mod context_exhaustion_tests {
    use super::*;
    use crate::lifecycle::{spawn_process, SpawnSpec};

    #[test]
    fn parses_context_percentage_from_status_line() {
        assert_eq!(parse_context_pct("23% context used"), Some(23));
        // Embedded in a larger line, with leading text.
        assert_eq!(
            parse_context_pct("  ⏵⏵ accept edits on · 92% context used"),
            Some(92)
        );
        // Case-insensitive and tolerant of the spacing around the label.
        assert_eq!(parse_context_pct("88%  Context Used"), Some(88));
        assert_eq!(parse_context_pct("88%context used"), Some(88));
    }

    #[test]
    fn keeps_the_last_marker_in_scrollback() {
        // Captured scrollback is oldest-first; the final reading is claude's most recent. A
        // session that climbed 10% → 47% → 96% must classify on the 96%.
        let pane = "10% context used\nworking…\n47% context used\nmore work\n96% context used\n";
        assert_eq!(parse_context_pct(pane), Some(96));
    }

    #[test]
    fn no_marker_is_none_and_oversized_is_clamped() {
        assert_eq!(parse_context_pct("just a shell prompt $ "), None);
        assert_eq!(parse_context_pct(""), None);
        // A nonsensical >100 reading is clamped rather than overflowing the u8.
        assert_eq!(parse_context_pct("150% context used"), Some(100));
    }

    #[test]
    fn classify_exit_thresholds_at_85() {
        // At/above the threshold → ContextExhausted carrying the percentage.
        assert_eq!(
            WatchOutcome::classify_exit("85% context used"),
            WatchOutcome::ContextExhausted { context_pct: 85 }
        );
        assert_eq!(
            WatchOutcome::classify_exit("99% context used"),
            WatchOutcome::ContextExhausted { context_pct: 99 }
        );
        // Below it → a plain Exited (lots of room left, normal completion).
        assert_eq!(
            WatchOutcome::classify_exit("84% context used"),
            WatchOutcome::Exited
        );
        // No marker at all → Exited (e.g. the direct-child path or a session that closed clean).
        assert_eq!(WatchOutcome::classify_exit("done.\n"), WatchOutcome::Exited);
    }

    #[test]
    fn context_exhausted_end_event_records_the_percentage() {
        let ev = WatchOutcome::ContextExhausted { context_pct: 91 }.end_event("gt-max");
        match ev {
            AgentEvent::Killed { session, reason } => {
                assert_eq!(session, "gt-max");
                assert!(reason.contains("91"), "reason carries the pct: {reason}");
                assert!(reason.contains("context"), "reason names the cause: {reason}");
            }
            other => panic!("expected Killed, got {other:?}"),
        }
    }

    fn quick_spec(command: &str, args: &[&str]) -> SpawnSpec {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir();
        // Unique per call so parallel tests never share a heartbeat file.
        let session = format!(
            "gt-ctx-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        );
        SpawnSpec {
            session: session.clone(),
            rig: "granite".to_string(),
            polecat: session.clone(),
            crew: None,
            workdir: dir.clone(),
            command: command.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            env: vec![],
            hook_bead: None,
            issue: None,
            heartbeat: dir.join(format!("{session}.heartbeat")),
        }
    }

    #[tokio::test]
    async fn watch_classifies_a_self_exit_from_the_captured_pane() {
        // A child that exits immediately; the capture closure stands in a near-full pane, so
        // watch() must report ContextExhausted rather than a bare Exited (gtcore-91fdde).
        let spec = quick_spec("true", &[]);
        let mut p = spawn_process(&spec).await.expect("spawn true");
        let outcome = watch(
            &mut p,
            Duration::from_secs(60),
            Duration::from_millis(10),
            || Some("⏵ 90% context used".to_string()),
        )
        .await;
        assert_eq!(outcome, WatchOutcome::ContextExhausted { context_pct: 90 });
    }

    #[tokio::test]
    async fn watch_without_a_pane_is_a_plain_exit() {
        // No pane to inspect (the direct-child path passes `|| None`) → unchanged Exited.
        let spec = quick_spec("true", &[]);
        let mut p = spawn_process(&spec).await.expect("spawn true");
        let outcome = watch(
            &mut p,
            Duration::from_secs(60),
            Duration::from_millis(10),
            || None,
        )
        .await;
        assert_eq!(outcome, WatchOutcome::Exited);
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
    /// Optional spec rewriter applied at RE-sling time (hq-49198f). A polecat that died
    /// mid-task may have been slung on an account that is now Blocked/rotated; re-slinging
    /// the stored spec verbatim burns the restart budget against dead credentials. The
    /// composition root wires a closure that re-resolves the account-dependent env
    /// (`CLAUDE_CONFIG_DIR`, `GT_HOOK_ACCOUNT`, proxy attribution headers) from the
    /// keychain's CURRENT active pointer, so the re-sling lands on a healthy account while
    /// the bead's branch/worktree survive untouched. `None` ⇒ verbatim re-sling (legacy).
    respec: Mutex<Option<RespecFn>>,
    /// Optional "is this bead closed?" probe consulted at RE-sling time (gtcore-177770). A
    /// polecat can die (tmux session gone) after its bead was already closed — by the operator
    /// or by merge auto-close — while the work is delivered. Re-slinging it then burns the
    /// restart budget on finished work and keeps the slot occupied, starving new dispatches.
    /// The composition root wires a closure that reads the bead's tracker status; when it
    /// returns `true` for a dead polecat's `hook_bead`, `tick` unwatches the session instead of
    /// re-slinging it. `None` ⇒ never closed-checks (legacy: re-sling every dead polecat).
    bead_closed: Mutex<Option<BeadClosedFn>>,
}

/// Spec rewriter applied before a re-sling (see [`PolecatSupervisor::set_respec`]).
pub type RespecFn = Box<dyn Fn(SpawnSpec) -> SpawnSpec + Send + Sync>;

/// Bead-status probe consulted before a re-sling (see [`PolecatSupervisor::set_bead_closed`]).
/// Given a `hook_bead` id, returns `true` when that bead is closed in the tracker.
pub type BeadClosedFn = Box<dyn Fn(&str) -> bool + Send + Sync>;

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
            respec: Mutex::new(None),
            bead_closed: Mutex::new(None),
        }
    }

    /// Install the re-sling spec rewriter (hq-49198f) — settable post-`Arc` because the
    /// composition root builds the supervisor before the keychain exists. See [`RespecFn`].
    pub fn set_respec(&self, f: RespecFn) {
        *self.respec.lock().unwrap() = Some(f);
    }

    /// Install the bead-closed probe (gtcore-177770) — settable post-`Arc` because the
    /// composition root builds the supervisor before the Dolt store handle exists. A dead
    /// polecat whose `hook_bead` this probe reports closed is unwatched instead of re-slung.
    /// See [`BeadClosedFn`].
    pub fn set_bead_closed(&self, f: BeadClosedFn) {
        *self.bead_closed.lock().unwrap() = Some(f);
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

    /// The stored [`SpawnSpec`] for `session`, cloned, or `None` if it is not (or no longer)
    /// supervised. The context-exhaustion re-sling (gtcore-3b2a68) reads it to derive the bead +
    /// worktree of the dead polecat, then re-watches a continuation spec built from it.
    pub fn spec_for_session(&self, session: &str) -> Option<SpawnSpec> {
        self.state.lock().unwrap().watched.get(session).cloned()
    }

    /// Session ids of every supervised polecat whose env carries `GT_HOOK_ACCOUNT == account`.
    /// All session ids currently watched by the supervisor — alive or pending re-sling.
    /// Used to emit MCP agent heartbeats after each tick without changing tick's return type.
    pub fn watched_sessions(&self) -> Vec<String> {
        self.state.lock().unwrap().watched.keys().cloned().collect()
    }

    /// Used by the quota-rotation observer to detect in-flight polecats at risk when their account
    /// is rotated (`hq-quota-refinement.3`). Returns an empty vec when none match.
    pub fn sessions_for_account(&self, account: &str) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .watched
            .values()
            .filter(|spec| {
                spec.env
                    .iter()
                    .any(|(k, v)| k == crate::GT_HOOK_ACCOUNT && v == account)
            })
            .map(|spec| spec.session.clone())
            .collect()
    }

    /// Return `(session, CLAUDE_CONFIG_DIR)` pairs for every polecat backed by `account`.
    /// Used by hot credential rotation to copy the new account's credentials in-place.
    pub fn config_dirs_for_account(&self, account: &str) -> Vec<(String, String)> {
        self.state
            .lock()
            .unwrap()
            .watched
            .values()
            .filter(|spec| {
                spec.env
                    .iter()
                    .any(|(k, v)| k == crate::GT_HOOK_ACCOUNT && v == account)
            })
            .filter_map(|spec| {
                let dir = spec
                    .env
                    .iter()
                    .find(|(k, _)| k == "CLAUDE_CONFIG_DIR")
                    .map(|(_, v)| v.clone())?;
                Some((spec.session.clone(), dir))
            })
            .collect()
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
            // Dead. If the bead this polecat was slung for has already closed (operator or
            // merge auto-close), the work is delivered — unwatch instead of burning restart
            // budget re-slinging finished work and blocking the slot (gtcore-177770). Only the
            // positively-closed case drops; an unknown bead or a probe error falls through to
            // the normal re-sling path, so open beads (and a missing probe) never regress.
            let hook_bead = st.watched.get(&session).and_then(|s| s.hook_bead.clone());
            if let Some(bead) = hook_bead {
                let closed = self
                    .bead_closed
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|f| f(&bead))
                    .unwrap_or(false);
                if closed {
                    st.watched.remove(&session);
                    st.restarts.remove(&session);
                    eprintln!(
                        "[polecat-supervisor] unwatching session={session} — bead {bead} is closed"
                    );
                    continue;
                }
            }
            // Check the hard cap and the tracker's backoff / crash-loop gate.
            let count = *st.restarts.get(&session).unwrap_or(&0);
            if count >= st.max_restarts || !st.tracker.can_restart(&session, now) {
                to_drop.push(session);
                continue;
            }
            st.tracker.record_restart(&session, now);
            let spec = st.watched.get(&session).cloned();
            if let Some(spec) = spec {
                // Re-resolve account-dependent env before the re-sling (hq-49198f): the
                // stored spec may point at an account that blocked/rotated since the
                // original sling. The rewritten spec replaces the watched one so future
                // re-slings (and `sessions_for_account`) see the current account.
                let spec = match self.respec.lock().unwrap().as_ref() {
                    Some(f) => {
                        let fresh = f(spec);
                        st.watched.insert(session.clone(), fresh.clone());
                        fresh
                    }
                    None => spec,
                };
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
    fn resling_applies_respec_and_persists_the_rewritten_spec() {
        // hq-49198f: the re-sling must re-resolve account-dependent env (the stored spec may
        // point at a now-blocked account) and keep the rewrite for subsequent passes.
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
        let mut s = spec("gt-rotor", "hq-9");
        s.env
            .push((crate::GT_HOOK_ACCOUNT.to_string(), "old-acct".to_string()));
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        sup.set_respec(Box::new(|mut spec| {
            for (k, v) in spec.env.iter_mut() {
                if k == crate::GT_HOOK_ACCOUNT {
                    *v = "new-acct".to_string();
                }
            }
            spec
        }));

        tmux.kill_session("gt-rotor").unwrap();
        assert_eq!(sup.tick(1000), 1, "dead session re-slung");
        // The rewritten spec replaced the watched one: account attribution moved.
        assert!(sup.sessions_for_account("old-acct").is_empty());
        assert_eq!(sup.sessions_for_account("new-acct"), vec!["gt-rotor".to_string()]);
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
    fn closed_bead_is_unwatched_not_reslung() {
        // gtcore-177770: a polecat whose bead closed (operator / merge auto-close) while its
        // tmux session was dead must be dropped on the first tick, never re-slung.
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
        let s = spec("gt-max", "hq-closed");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        // Probe reports this exact bead closed.
        sup.set_bead_closed(Box::new(|bead: &str| bead == "hq-closed"));
        assert_eq!(sup.watched_count(), 1);

        // Session dies, but the bead is closed → first tick unwatches, no re-sling.
        tmux.kill_session("gt-max").unwrap();
        assert_eq!(sup.tick(1000), 0, "closed bead is not re-slung");
        assert_eq!(sup.watched_count(), 0, "session dropped from the watched map");
        assert!(!tmux.has_session("gt-max"), "closed polecat stays dead");
    }

    #[test]
    fn open_bead_is_still_reslung_with_probe_installed() {
        // gtcore-177770 (no-regression): with the closed-probe installed, a dead polecat whose
        // bead is still open must re-sling exactly as before.
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
        let s = spec("gt-rictus", "hq-open");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        // Probe never reports closed (e.g. open bead, or unknown id).
        sup.set_bead_closed(Box::new(|_bead: &str| false));

        tmux.kill_session("gt-rictus").unwrap();
        assert_eq!(sup.tick(1000), 1, "open bead is re-slung");
        assert!(tmux.has_session("gt-rictus"), "re-slung session exists again");
        assert_eq!(sup.watched_count(), 1, "still supervised");
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

    fn spec_with_account(session: &str, member: &str, account: &str) -> SpawnSpec {
        let mut s = spec(session, member);
        s.env.push(("GT_HOOK_ACCOUNT".to_string(), account.to_string()));
        s
    }

    #[test]
    fn sessions_for_account_finds_matching_watched_polecats() {
        // hq-quota-refinement.3: detects in-flight polecats on the rotated account.
        let tmux = Arc::new(FakeTmux::default());
        let sup = PolecatSupervisor::new(tmux, RestartConfig::default(), 3);
        sup.watch(spec_with_account("sess-1", "hq-abc.1", "acct-a"));
        sup.watch(spec_with_account("sess-2", "hq-abc.2", "acct-b"));
        sup.watch(spec_with_account("sess-3", "hq-abc.3", "acct-a"));

        let mut found = sup.sessions_for_account("acct-a");
        found.sort();
        assert_eq!(found, vec!["sess-1", "sess-3"], "both acct-a sessions returned");
        assert!(sup.sessions_for_account("acct-b") == vec!["sess-2"]);
        assert!(sup.sessions_for_account("acct-x").is_empty(), "unknown account → empty");
    }
}
