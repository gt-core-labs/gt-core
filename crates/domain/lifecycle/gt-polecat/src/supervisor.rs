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

use std::collections::{HashMap, HashSet};
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
use crate::wedge::{classify_wedge, WedgeDialog};

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
    /// Optional "should a polecat still be slung for this bead?" probe consulted at RE-sling time
    /// (gtcore-177770, generalized by gtcore-db99e0). A polecat can die (tmux session gone) after
    /// its bead became un-slingable — closed by the operator or merge auto-close (work delivered),
    /// flipped to an epic container, or set dispatch=manual. Re-slinging it then burns the restart
    /// budget on work no autonomous agent should touch and keeps the slot occupied, starving new
    /// dispatches. The composition root wires a closure that re-reads the bead's CURRENT state and
    /// applies the unified `should_sling` predicate (status ∈ {open,working} ∧ ¬epic ∧
    /// dispatch=auto); when it returns `false` for a dead polecat's `hook_bead`, `tick` unwatches
    /// the session instead of re-slinging it. `None` ⇒ never re-validates (legacy: re-sling every
    /// dead polecat).
    slingable: Mutex<Option<BeadSlingableFn>>,
    /// Optional wedge-recovery hook consulted when `tick` finds an ALIVE session frozen on a known
    /// interactive dialog (gtcore-2836bb). A wedged polecat keeps its tmux session, so the bare
    /// `has_session` liveness check reads it as healthy while it produces nothing and holds a pool
    /// slot. When wired, `tick` captures the pane (via [`Tmux::capture_pane`]), classifies it with
    /// [`classify_wedge`], and — on a hit — invokes this hook with the wedged session's spec and the
    /// dialog. The composition root's hook applies the recovery (re-seed onboarding for a trust
    /// prompt / let the rotated pointer take for a usage-limit) and alerts the operator; the
    /// supervisor then kills the wedged session so the SAME tick's re-sling path treats it as dead
    /// and re-slings it onto the recovered state. The pool slot is NOT freed — the re-sling reuses
    /// the slot the original sling claimed, exactly like a crash re-sling. `None` ⇒ wedges are not
    /// detected (legacy: an alive session is always counted healthy).
    on_wedge: Mutex<Option<WedgeFn>>,
}

/// Spec rewriter applied before a re-sling (see [`PolecatSupervisor::set_respec`]).
pub type RespecFn = Box<dyn Fn(SpawnSpec) -> SpawnSpec + Send + Sync>;

/// Slingability probe consulted before a re-sling (see [`PolecatSupervisor::set_bead_slingable`]).
/// Given a `hook_bead` id, returns `true` when a polecat should still be (re-)slung for that bead —
/// it is open/working, not an epic, and dispatch=auto (the unified `should_sling` predicate,
/// gtcore-db99e0). `false` ⇒ `tick` drops the dead polecat instead of re-slinging it.
pub type BeadSlingableFn = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// Wedge-recovery hook (see [`PolecatSupervisor::set_on_wedge`]). Given the wedged session's spec
/// and the dialog it is frozen on, the composition root applies the recovery (re-seed onboarding /
/// rotate account) and alerts the operator. Called by `tick` BEFORE it kills the wedged session, so
/// the recovery (e.g. re-seeding the config dir) is in place for the re-sling that follows.
pub type WedgeFn = Box<dyn Fn(&SpawnSpec, WedgeDialog) + Send + Sync>;

struct SupervisorState {
    /// session id -> spec to re-sling it from.
    watched: HashMap<String, SpawnSpec>,
    tracker: RestartTracker,
    /// Per-session re-sling count (separate from the tracker's crash-loop window) — a hard
    /// cap so a permanently-broken polecat is eventually abandoned.
    restarts: HashMap<String, u32>,
    max_restarts: u32,
    /// Sessions currently suspended in place (`SIGSTOP`) by pause-on-exhaustion (gtcore-6f449f).
    /// A paused polecat keeps its tmux session, so `tick`'s `has_session` check still sees it alive
    /// (no spurious re-sling); the set is what [`PolecatSupervisor::resume_account`] thaws.
    paused: HashSet<String>,
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
                paused: HashSet::new(),
            }),
            respec: Mutex::new(None),
            slingable: Mutex::new(None),
            on_wedge: Mutex::new(None),
        }
    }

    /// Install the re-sling spec rewriter (hq-49198f) — settable post-`Arc` because the
    /// composition root builds the supervisor before the keychain exists. See [`RespecFn`].
    pub fn set_respec(&self, f: RespecFn) {
        *self.respec.lock().unwrap() = Some(f);
    }

    /// Install the slingability probe (gtcore-177770, generalized by gtcore-db99e0) — settable
    /// post-`Arc` because the composition root builds the supervisor before the Dolt store handle
    /// exists. A dead polecat whose `hook_bead` this probe reports NOT slingable (closed, an epic,
    /// or dispatch=manual) is unwatched instead of re-slung — and without spending its restart
    /// budget. See [`BeadSlingableFn`].
    pub fn set_bead_slingable(&self, f: BeadSlingableFn) {
        *self.slingable.lock().unwrap() = Some(f);
    }

    /// Install the wedge-recovery hook (gtcore-2836bb) — settable post-`Arc` because the
    /// composition root builds the supervisor before the keychain/allocator exist. When set, an
    /// alive session frozen on a known interactive dialog is recovered (re-seed/rotate + slot
    /// release + alert) and re-slung instead of being counted healthy. See [`WedgeFn`].
    pub fn set_on_wedge(&self, f: WedgeFn) {
        *self.on_wedge.lock().unwrap() = Some(f);
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
        st.paused.remove(session);
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
            st.paused.remove(&session);
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

    /// Suspend in place (`SIGSTOP`) every watched polecat backed by `account` — pause-on-exhaustion
    /// (gtcore-6f449f): when the quota rotation finds no healthy alternative, freezing the in-flight
    /// polecats preserves their context instead of letting them burn against the rate limit. Returns
    /// the session ids actually paused (sorted, deterministic). A paused session keeps its tmux
    /// session, so [`Self::tick`] does not treat it as dead; [`Self::resume_account`] thaws it.
    /// Idempotent: re-pausing an already-paused session re-sends a (harmless) `SIGSTOP`.
    pub fn pause_account(&self, account: &str) -> Vec<String> {
        let mut st = self.state.lock().unwrap();
        let mut sessions: Vec<String> = st
            .watched
            .values()
            .filter(|spec| {
                spec.env
                    .iter()
                    .any(|(k, v)| k == crate::GT_HOOK_ACCOUNT && v == account)
            })
            .map(|spec| spec.session.clone())
            .collect();
        sessions.sort();
        let mut paused = Vec::new();
        for session in sessions {
            match self.tmux.pause(&session) {
                Ok(()) => {
                    st.paused.insert(session.clone());
                    paused.push(session);
                }
                Err(e) => {
                    eprintln!("[polecat-supervisor] pause failed session={session}: {e}")
                }
            }
        }
        paused
    }

    /// Resume (`SIGCONT`) every paused polecat backed by `account` (gtcore-6f449f): called when a
    /// previously-exhausted account recovers to `Healthy`. Returns the session ids resumed (sorted).
    /// Only sessions this supervisor paused AND still watches are thawed — a polecat that died while
    /// paused is no longer watched, so it is silently skipped and recovered by normal supervision.
    pub fn resume_account(&self, account: &str) -> Vec<String> {
        let mut st = self.state.lock().unwrap();
        let mut sessions: Vec<String> = st
            .watched
            .values()
            .filter(|spec| {
                spec.env
                    .iter()
                    .any(|(k, v)| k == crate::GT_HOOK_ACCOUNT && v == account)
            })
            .map(|spec| spec.session.clone())
            .filter(|s| st.paused.contains(s))
            .collect();
        sessions.sort();
        let mut resumed = Vec::new();
        for session in sessions {
            match self.tmux.resume(&session) {
                Ok(()) => {
                    st.paused.remove(&session);
                    resumed.push(session);
                }
                Err(e) => {
                    eprintln!("[polecat-supervisor] resume failed session={session}: {e}")
                }
            }
        }
        resumed
    }

    /// Session ids currently suspended by [`Self::pause_account`] (gtcore-6f449f). Test/diagnostic
    /// view of the pause set.
    pub fn paused_sessions(&self) -> Vec<String> {
        let mut v: Vec<String> = self.state.lock().unwrap().paused.iter().cloned().collect();
        v.sort();
        v
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
                // Alive by `has_session` — but a polecat can be alive yet WEDGED on an interactive
                // dialog claude never gets past (trust-folder / usage-limit). Such a session
                // produces nothing while holding its pool slot and would otherwise be counted
                // healthy here (gtcore-2836bb). When a wedge hook is wired, inspect the pane: a
                // known dialog ⇒ run the recovery (re-seed/rotate + alert via the hook), then KILL
                // the session so the dead-path below re-slings it THIS tick onto the recovered
                // state. A paused (SIGSTOP'd) polecat is deliberately skipped — its frozen pane is
                // not a wedge, and resuming it is the quota subsystem's job.
                let wedge_armed = self.on_wedge.lock().unwrap().is_some();
                let wedged = if wedge_armed && !st.paused.contains(&session) {
                    self.tmux
                        .capture_pane(&session)
                        .and_then(|pane| classify_wedge(&pane))
                } else {
                    None
                };
                let Some(dialog) = wedged else {
                    st.tracker.record_success(&session, now);
                    continue;
                };
                // Recovery hook (re-seed / rotate + alert) before the kill, so the re-sling that
                // follows lands on the recovered state. Clone the spec out so the hook does not
                // hold a borrow of `st.watched` across the call.
                if let Some(spec) = st.watched.get(&session).cloned() {
                    if let Some(hook) = self.on_wedge.lock().unwrap().as_ref() {
                        hook(&spec, dialog);
                    }
                }
                eprintln!(
                    "[polecat-supervisor] session={session} wedged on {} — recovering + re-slinging",
                    dialog.reason()
                );
                // Kill the wedged session and fall through to the dead-handling path below: the
                // session is now gone, so it re-slings THIS tick exactly as a crashed polecat
                // would (subject to the same restart budget + backoff). The slot is reused, not
                // re-claimed.
                let _ = self.tmux.kill_session(&session);
            }
            // Dead. A paused polecat that died (killed while suspended) is gone — drop it from the
            // pause set so a later resume does not thaw a stale id and any re-sling below starts a
            // fresh, un-paused session (gtcore-6f449f).
            st.paused.remove(&session);
            // If the bead this polecat was slung for is no longer slingable — closed (operator or
            // merge auto-close, work delivered), an epic container, or dispatch=manual — unwatch
            // instead of burning restart budget re-slinging work no autonomous agent should touch
            // and blocking the slot (gtcore-177770, unified by gtcore-db99e0). Only a positive
            // ¬slingable verdict drops; an unknown bead, a probe error, or a missing probe is
            // treated as slingable and falls through to the normal re-sling path, so live work
            // (and a deployment without the probe wired) never regresses.
            let hook_bead = st.watched.get(&session).and_then(|s| s.hook_bead.clone());
            if let Some(bead) = hook_bead {
                let slingable = self
                    .slingable
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|f| f(&bead))
                    .unwrap_or(true);
                if !slingable {
                    st.watched.remove(&session);
                    st.restarts.remove(&session);
                    eprintln!(
                        "[polecat-supervisor] unwatching session={session} — bead {bead} is not slingable (closed/epic/manual)"
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
    fn unslingable_bead_is_unwatched_not_reslung() {
        // gtcore-177770 + gtcore-db99e0: a polecat whose bead became un-slingable (closed by
        // operator / merge auto-close, or flipped to an epic / dispatch=manual) while its tmux
        // session was dead must be dropped on the first tick, never re-slung — and without
        // spending its restart budget.
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
        // Probe reports this exact bead NOT slingable (the wired closure embodies should_sling).
        sup.set_bead_slingable(Box::new(|bead: &str| bead != "hq-closed"));
        assert_eq!(sup.watched_count(), 1);

        // Session dies, but the bead is not slingable → first tick unwatches, no re-sling.
        tmux.kill_session("gt-max").unwrap();
        assert_eq!(sup.tick(1000), 0, "un-slingable bead is not re-slung");
        assert_eq!(sup.watched_count(), 0, "session dropped from the watched map");
        assert!(!tmux.has_session("gt-max"), "un-slingable polecat stays dead");
    }

    #[test]
    fn slingable_bead_is_still_reslung_with_probe_installed() {
        // gtcore-177770 (no-regression): with the slingability probe installed, a dead polecat
        // whose bead is still slingable (open/working, ¬epic, dispatch=auto) must re-sling as
        // before.
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
        // Probe reports every bead slingable (e.g. open auto bead, or unknown id → permissive).
        sup.set_bead_slingable(Box::new(|_bead: &str| true));

        tmux.kill_session("gt-rictus").unwrap();
        assert_eq!(sup.tick(1000), 1, "slingable bead is re-slung");
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
    fn pause_account_suspends_only_that_accounts_polecats() {
        // gtcore-6f449f: pause_account SIGSTOPs every watched polecat backed by the account and
        // leaves the others running; resume_account thaws exactly the paused ones.
        let tmux = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), 3);
        for (s, m, a) in [
            ("sess-1", "hq-1", "acct-a"),
            ("sess-2", "hq-2", "acct-b"),
            ("sess-3", "hq-3", "acct-a"),
        ] {
            let spec = spec_with_account(s, m, a);
            spawn_tmux(tmux.as_ref(), &spec).unwrap();
            sup.watch(spec);
        }

        let paused = sup.pause_account("acct-a");
        assert_eq!(paused, vec!["sess-1", "sess-3"], "both acct-a sessions paused");
        assert!(tmux.is_paused("sess-1") && tmux.is_paused("sess-3"));
        assert!(!tmux.is_paused("sess-2"), "acct-b polecat keeps running");
        assert_eq!(sup.paused_sessions(), vec!["sess-1", "sess-3"]);

        // A paused polecat keeps its tmux session → tick must NOT treat it as dead.
        assert_eq!(sup.tick(1000), 0, "paused sessions are not re-slung");
        assert!(tmux.has_session("sess-1") && tmux.has_session("sess-3"));

        let resumed = sup.resume_account("acct-a");
        assert_eq!(resumed, vec!["sess-1", "sess-3"], "both acct-a sessions resumed");
        assert!(!tmux.is_paused("sess-1") && !tmux.is_paused("sess-3"));
        assert!(sup.paused_sessions().is_empty(), "pause set cleared");
    }

    #[test]
    fn resume_skips_a_polecat_that_died_while_paused() {
        // gtcore-6f449f: a polecat killed (tmux gone) while paused is no longer watched after
        // tick re-slings or drops it; resume_account must only thaw the sessions still paused.
        let tmux = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig {
                initial_backoff_secs: 1,
                crash_loop_count: 100,
                ..RestartConfig::default()
            },
            u32::MAX,
        );
        let s1 = spec_with_account("sess-1", "hq-1", "acct-a");
        let s2 = spec_with_account("sess-2", "hq-2", "acct-a");
        spawn_tmux(tmux.as_ref(), &s1).unwrap();
        spawn_tmux(tmux.as_ref(), &s2).unwrap();
        sup.watch(s1);
        sup.watch(s2);
        assert_eq!(sup.pause_account("acct-a"), vec!["sess-1", "sess-2"]);

        // sess-1 dies while paused → next tick re-slings it (a fresh, un-paused session).
        tmux.kill_session("sess-1").unwrap();
        assert_eq!(sup.tick(1000), 1, "dead paused polecat re-slung");

        // Resume: sess-2 is still paused; sess-1 was re-slung fresh (not in the pause set).
        let resumed = sup.resume_account("acct-a");
        assert_eq!(resumed, vec!["sess-2"], "only the still-paused session is thawed");
    }

    #[test]
    fn wedged_alive_session_is_recovered_and_reslung() {
        // gtcore-2836bb: a session that is alive (has_session) but frozen on the trust-folder
        // dialog must be detected as wedged — the recovery hook fires, the session is killed and
        // re-slung THIS tick, and the operator-facing dialog is reported to the hook.
        use std::sync::atomic::{AtomicUsize, Ordering};
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
        let s = spec("gt-wedged", "hq-w");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);

        let hook_calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(None));
        let hc = hook_calls.clone();
        let sn = seen.clone();
        let tmux_for_hook = tmux.clone();
        sup.set_on_wedge(Box::new(move |spec, dialog| {
            hc.fetch_add(1, Ordering::SeqCst);
            *sn.lock().unwrap() = Some(dialog);
            // The recovery clears the wedge for the re-slung session (in production this is the
            // re-seed; here we just drop the stale pane text so the next tick sees a clean session).
            tmux_for_hook.set_pane(&spec.session, "");
        }));

        // The pane shows the trust prompt → wedged.
        tmux.set_pane("gt-wedged", "Do you trust this folder? 1. Yes 2. No");
        assert!(tmux.has_session("gt-wedged"), "session is alive before the tick");

        let reslung = sup.tick(1000);
        assert_eq!(reslung, 1, "the wedged session is killed and re-slung this tick");
        assert_eq!(hook_calls.load(Ordering::SeqCst), 1, "recovery hook fired once");
        assert_eq!(
            *seen.lock().unwrap(),
            Some(WedgeDialog::TrustPrompt),
            "the hook saw the trust-folder dialog"
        );
        assert!(tmux.has_session("gt-wedged"), "the re-slung session exists again");
        assert_eq!(sup.watched_count(), 1, "still supervised");

        // The recovery cleared the pane → the next tick sees a healthy session (no re-sling).
        assert_eq!(sup.tick(2000), 0, "a recovered session is no longer wedged");
    }

    #[test]
    fn unwedged_alive_session_is_left_alone() {
        // gtcore-2836bb no-regression: with the wedge hook installed, an alive session whose pane
        // shows normal working output is NOT killed — it is counted healthy exactly as before.
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), u32::MAX);
        let s = spec("gt-busy", "hq-b");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        sup.set_on_wedge(Box::new(|_spec, _dialog| {
            panic!("a healthy session must not trigger the wedge hook");
        }));
        tmux.set_pane("gt-busy", "Running tests… 12 passed");
        assert_eq!(sup.tick(1000), 0, "healthy session not re-slung");
        assert!(tmux.has_session("gt-busy"), "healthy session left running");
    }

    #[test]
    fn wedge_detection_is_off_without_the_hook() {
        // Without set_on_wedge, the pane is never even captured — an alive session is always
        // healthy (legacy behaviour preserved).
        let tmux: Arc<FakeTmux> = Arc::new(FakeTmux::new());
        let sup = PolecatSupervisor::new(tmux.clone(), RestartConfig::default(), u32::MAX);
        let s = spec("gt-legacy", "hq-l");
        spawn_tmux(tmux.as_ref(), &s).unwrap();
        sup.watch(s);
        // Even a pane that WOULD classify as wedged is ignored when no hook is wired.
        tmux.set_pane("gt-legacy", "Usage limit reached\n1. Stop and wait for limit to reset");
        assert_eq!(sup.tick(1000), 0, "no hook ⇒ no wedge detection");
        assert!(tmux.has_session("gt-legacy"), "session left running");
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
