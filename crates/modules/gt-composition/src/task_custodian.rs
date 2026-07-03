//! Task custodian (gtcore-912043): the TASK-side reconciler.
//!
//! The orchd flips a bead `open → working` at sling time ("sling→working transition on") so the
//! board and the auto-dispatch frontier see the claim immediately. But nothing reconciled the task
//! side afterwards: a spawn that failed (e.g. `tmux respawn-pane failed: command too long`), or an
//! agent that died without a terminator, left the bead `working` forever — dispatched, invisible
//! to the frontier, worked by nobody. [`crate::session_reconcile`] is the AGENT-side half (it
//! closes orphaned `agent.*` sessions); this module is the missing half the operator diagnosed as
//! "el dispatch se preocupa solo por los agentes, no por las tareas".
//!
//! Each sweep the custodian:
//!
//! 1. lists the workspace's `working` beads (Dolt),
//! 2. replays the `agent.*` log into the session registry and keeps only ACTIVE sessions —
//!    the session reconciler has already reaped provably-dead ones, so registry-active is the
//!    daemon's best truth of "an agent is on it",
//! 3. skips any bead with a merge slot in flight (`Ready`/`Merging` on the merge board — the bead
//!    is past its session's useful life and the refinery owns it),
//! 4. and for a bead that stays session-less past a grace window, RE-OPENS it via the store's
//!    CAS release (`release_claim`: `working → open` + owner cleared, guarded on `working`).
//!
//! Recovery is deliberately just the status flip: an `open` bead with `dispatch=auto` re-enters
//! the very next frontier tick (re-dispatch), while a manual/epic/held bead simply becomes
//! visible-and-claimable again instead of stranded — exactly the "re-opened but NOT re-dispatched"
//! split the dispatch policy already encodes ([`gt_issues::should_sling`]).
//!
//! The grace window is tracked in-memory (first sweep that sees the bead session-less starts its
//! clock): a daemon restart resets the clocks, which is safe — boot re-hydration already
//! re-enqueues dispatched-but-unmerged beads, and a still-stuck bead just waits one more grace.
//!
//! The decision core ([`recovery_candidates`], [`should_recover`]) is pure so the policy is
//! unit-tested without Dolt, tmux or an event log.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use gt_agent::AgentEvent;
use gt_eventlog::EventRecord;
use gt_events::Envelope;
use gt_merge::actor::MergeHandle;
use gt_merge::MergeSlotState;
use gt_store_dolt::{DoltIssues, IssueFilter};

use crate::mcp::eventlog::EventLog;
use crate::polecat_event::PolecatEvent;

/// A bead's session id component, exactly as the sling names tmux sessions
/// (`SpawnTemplate::spec_for`: non-`[A-Za-z0-9_-]` bytes map to `-`). Kept in sync by the
/// matching test in this module's suite.
fn sanitize_name(member: &str) -> String {
    member
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect()
}

/// Does an active session belong to `bead`? Sessions are named `{template-prefix}-{bead}` (the
/// bead sanitized), so a session owns the bead when it *is* the sanitized bead or ends with
/// `-{sanitized-bead}`. Conservative on purpose: a false positive keeps a bead untouched (safe),
/// while recovery only fires when NO active session plausibly matches.
fn session_matches(session_id: &str, bead: &str) -> bool {
    let name = sanitize_name(bead);
    session_id == name || session_id.ends_with(&format!("-{name}"))
}

/// Pure verdict for one `working` bead this sweep.
///
/// Recover iff nobody is on it (`!has_live_session`), the refinery does not own it
/// (`!merge_in_flight`), and it has been session-less past the grace window (`grace_elapsed`).
pub fn should_recover(has_live_session: bool, merge_in_flight: bool, grace_elapsed: bool) -> bool {
    !has_live_session && !merge_in_flight && grace_elapsed
}

/// Pure candidate filter: which `working` beads are session-less and not on the merge board.
/// (Grace is stateful, layered on top by [`TaskCustodian::grace_elapsed`].)
pub fn recovery_candidates<'a>(
    working: &'a [String],
    live_sessions: &HashSet<String>,
    merge_inflight: &HashSet<String>,
) -> Vec<&'a String> {
    working
        .iter()
        .filter(|bead| {
            let live = live_sessions.iter().any(|s| session_matches(s, bead));
            let merging = merge_inflight.contains(bead.as_str());
            !live && !merging
        })
        .collect()
}

/// The periodic task-side sweep. Wired in `gt-orch-server` next to the session reconciler.
pub struct TaskCustodian {
    /// Root of the shared per-workspace event log (the `agent.*` replay source).
    event_root: PathBuf,
    workspace: String,
    issues: Arc<DoltIssues>,
    /// The live merge board; `None` ⇒ no merge-in-flight guard (test/minimal deployments).
    merge: Option<MergeHandle>,
    grace: Duration,
    /// Operator-visibility sink: recovery emits `polecat.working-recovered.v1` onto the daemon
    /// hub (the workflow-notification bridge turns it into a bell).
    events: Option<broadcast::Sender<EventRecord>>,
    /// First sweep instant each candidate bead was seen session-less — the grace clock.
    first_seen: Mutex<HashMap<String, Instant>>,
}

impl TaskCustodian {
    pub fn new(
        event_root: PathBuf,
        workspace: String,
        issues: Arc<DoltIssues>,
        merge: Option<MergeHandle>,
        grace: Duration,
        events: Option<broadcast::Sender<EventRecord>>,
    ) -> Self {
        Self {
            event_root,
            workspace,
            issues,
            merge,
            grace,
            events,
            first_seen: Mutex::new(HashMap::new()),
        }
    }

    /// Advance `bead`'s grace clock to `now` and answer whether the window elapsed. First
    /// observation arms the clock and answers `false` (a bead is never recovered on the sweep
    /// that discovers it, however long the tick).
    fn grace_elapsed(&self, bead: &str, now: Instant) -> bool {
        let mut seen = self.first_seen.lock().expect("custodian clock mutex");
        match seen.get(bead) {
            Some(first) => now.duration_since(*first) >= self.grace,
            None => {
                seen.insert(bead.to_string(), now);
                false
            }
        }
    }

    /// Drop the grace clock of a bead that stopped being a candidate (session appeared, merge
    /// started, status changed) or was recovered — the next episode starts a fresh window.
    fn clear(&self, bead: &str) {
        self.first_seen
            .lock()
            .expect("custodian clock mutex")
            .remove(bead);
    }

    /// Sweep once; returns the beads recovered (re-opened). Best-effort throughout: a store or
    /// replay failure logs and yields nothing rather than aborting the daemon.
    pub async fn sweep(&self) -> Vec<String> {
        let filter = IssueFilter {
            status: vec!["working".to_string()],
            ..Default::default()
        };
        let working: Vec<String> = match self.issues.list(&filter).await {
            Ok(rows) => rows.into_iter().map(|r| r.id).collect(),
            Err(e) => {
                eprintln!("[task-custodian] working-bead list failed: {e}");
                return Vec::new();
            }
        };

        let log = EventLog::new(Some(self.event_root.clone()));
        let live_sessions: HashSet<String> = match log
            .replay_domain::<gt_agent::SessionRegistry, AgentEvent, _>(
                Some(&self.workspace),
                "agent.",
                gt_agent::SessionRegistry::default(),
                gt_agent::SessionRegistry::apply,
            ) {
            Ok(reg) => reg.active().into_iter().map(|s| s.id).collect(),
            Err(e) => {
                // Without session truth every bead would look abandoned — recover nothing.
                eprintln!("[task-custodian] agent log replay failed: {e}");
                return Vec::new();
            }
        };

        let merge_inflight: HashSet<String> = match &self.merge {
            Some(merge) => merge
                .snapshot()
                .await
                .into_iter()
                .filter(|s| matches!(s.state, MergeSlotState::Ready | MergeSlotState::Merging))
                .map(|s| s.bead)
                .collect(),
            None => HashSet::new(),
        };

        let now = Instant::now();
        let candidates: Vec<String> = recovery_candidates(&working, &live_sessions, &merge_inflight)
            .into_iter()
            .cloned()
            .collect();
        // Beads that stopped being candidates get their clocks dropped so a NEW session-less
        // episode measures a fresh grace, not one accumulated across recoveries.
        {
            let candidate_set: HashSet<&str> = candidates.iter().map(String::as_str).collect();
            self.first_seen
                .lock()
                .expect("custodian clock mutex")
                .retain(|bead, _| candidate_set.contains(bead.as_str()));
        }

        let mut recovered = Vec::new();
        for bead in candidates {
            if !self.grace_elapsed(&bead, now) {
                continue;
            }
            // CAS re-open: only flips `working → open` (and clears owner), so a bead that
            // changed state since the list is untouched — the guard that makes the sweep
            // idempotent and race-safe.
            match self.issues.release_claim(&bead).await {
                Ok(true) => {
                    eprintln!(
                        "[task-custodian] recovered {bead}: working with no live session for ≥{}s — re-opened",
                        self.grace.as_secs()
                    );
                    self.emit(PolecatEvent::WorkingRecovered {
                        bead: bead.clone(),
                        reason: format!(
                            "working with no live agent session for ≥{}s; re-opened (auto beads re-enter the frontier, manual beads become claimable)",
                            self.grace.as_secs()
                        ),
                    });
                    self.clear(&bead);
                    recovered.push(bead);
                }
                Ok(false) => self.clear(&bead), // no longer `working` — someone else moved it
                Err(e) => eprintln!("[task-custodian] re-open of {bead} failed: {e}"),
            }
        }
        recovered
    }

    fn emit(&self, event: PolecatEvent) {
        if let Some(tx) = &self.events {
            if let Ok(record) = EventRecord::from_envelope(&Envelope::root(event)) {
                let _ = tx.send(record);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn recover_only_sessionless_unmerged_graced_beads() {
        // The AC truth table: working+no-session+grace → recover; a live session, an in-flight
        // merge slot, or an unelapsed grace each veto.
        assert!(should_recover(false, false, true));
        assert!(!should_recover(true, false, true), "live session vetoes");
        assert!(!should_recover(false, true, true), "merge in flight vetoes");
        assert!(!should_recover(false, false, false), "grace not elapsed vetoes");
    }

    #[test]
    fn candidates_exclude_live_sessions_and_merge_slots() {
        let working = vec![
            "gtcore-aaaaaa".to_string(), // live session (template-prefixed name)
            "gtcore-bbbbbb".to_string(), // merge in flight
            "gtcore-cccccc".to_string(), // abandoned → candidate
        ];
        let sessions = set(&["gt-gtcore-aaaaaa", "refinery-default"]);
        let merging = set(&["gtcore-bbbbbb"]);
        let got = recovery_candidates(&working, &sessions, &merging);
        assert_eq!(got, vec![&"gtcore-cccccc".to_string()]);
    }

    #[test]
    fn session_matching_is_sanitized_and_suffix_anchored() {
        // Sessions are `{prefix}-{sanitized-bead}`; dots sanitize to dashes exactly as the sling
        // names tmux sessions.
        assert!(session_matches("gt-hq-abc-1", "hq-abc.1"));
        assert!(session_matches("hq-abc-1", "hq-abc.1"), "bare sanitized name matches");
        // Suffix must be `-`-anchored: another bead that merely ends with the same characters
        // does not own the session.
        assert!(!session_matches("gt-xgtcore-45", "gtcore-45"));
    }

    #[test]
    fn grace_clock_arms_on_first_sight_and_elapses_after_window() {
        let custodian = TaskCustodian::new(
            std::env::temp_dir(),
            "default".into(),
            // A lazy store that is never dialed: grace bookkeeping is pure in-memory.
            Arc::new(DoltIssues::connect("mysql://user@127.0.0.1:1/hq").expect("lazy pool")),
            None,
            Duration::from_secs(600),
            None,
        );
        let t0 = Instant::now();
        assert!(!custodian.grace_elapsed("gtcore-x", t0), "first sight arms, never fires");
        assert!(
            !custodian.grace_elapsed("gtcore-x", t0 + Duration::from_secs(599)),
            "window not elapsed"
        );
        assert!(custodian.grace_elapsed("gtcore-x", t0 + Duration::from_secs(600)));
        // A cleared bead starts a fresh episode.
        custodian.clear("gtcore-x");
        assert!(!custodian.grace_elapsed("gtcore-x", t0 + Duration::from_secs(1200)));
    }
}
