//! Close the CI loop: re-dispatch a bead with its CI-failure log when its PR fails CI
//! (gtcore-3a1bd4, hueco 3 of epic gtcore-dddd4a).
//!
//! ## The gap this closes
//!
//! Today a polecat signals `merge-ready` and stops. The `GitMergePlugin` opens the PR with
//! auto-merge; GitHub runs CI; the webhook ([`crate::webhook`]) forwards the terminal verdict to
//! the orchd CI-gate receiver ([`crate::ci_gate`]). On `pull_request.closed (merged)` the slot
//! `complete`s. On a failed check-suite it `fail`s → `merge.failed.v1`, which the
//! [`SchedulerPlugin`](crate::SchedulerPlugin) re-enqueues.
//!
//! But that re-enqueue handed the next agent a *fresh* kickoff — it never saw **why** the PR
//! failed. So a polecat that wrote Rust without compiling locally (missing `derive`, a stale
//! parity-test entry — exactly what bit gtcore-1fe9b4) would be re-slung blind, with no reason to
//! do anything different, and the PR stayed wedged until a human read the log. The loop was open.
//!
//! ## The closed loop
//!
//! This module is the pure core that closes it. A shared [`CiFailureStore`] records, per bead, the
//! CI failure reason/log and how many CI-fix attempts the bead has burned. Two wirings use it:
//!
//! - On `merge.failed.v1`, the scheduler reactor (see [`crate::SchedulerPlugin`]) calls
//!   [`CiFailureStore::record_failure`]. Under the retry cap it returns
//!   [`RetryDecision::Retry`] (re-enqueue, as before, now WITH context); at the cap it returns
//!   [`RetryDecision::Exhausted`] — the bead is NOT re-enqueued and the operator is escalated
//!   ([`PolecatEvent::CiRetriesExhausted`](crate::polecat_event::PolecatEvent)), so a perpetually
//!   red PR cannot burn quota in an infinite re-sling loop.
//!
//! - At sling time, [`PolecatSupervisorPlugin`](crate::polecat::PolecatSupervisorPlugin) calls
//!   [`CiFailureStore::take`] for the dispatched bead; on a hit it swaps the kickoff prompt for the
//!   [CI-fix continuation prompt](build_ci_fix_prompt) (the CI log + the branch diff + the
//!   acceptance criteria + "fix, build locally, re-push"). A bead that passed CI first try has no
//!   entry, so its sling is unchanged.
//!
//! The store is in-memory and Arc-shared between the two plugins. It is observational recovery
//! state, not a durable fact — a daemon restart loses the in-flight CI-fix context (the bead's
//! PR is still red on GitHub, so the next webhook re-seeds it), which is the right trade: we never
//! want a *persisted* retry counter to permanently wedge a bead across restarts.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::continuation::MAX_DIFF_BYTES;

/// Default CI-fix retry cap (`GT_CI_MAX_RETRIES`). After this many failed CI attempts on a bead's
/// PR, the loop stops re-slinging and escalates to the operator instead of burning quota forever.
/// Three gives the agent a couple of genuine chances to read the log and fix the build without
/// letting a structurally-broken bead loop indefinitely.
pub const DEFAULT_MAX_CI_RETRIES: u32 = 3;

/// What to do with a bead whose PR just failed CI, decided by [`CiFailureStore::record_failure`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Under the cap: re-enqueue the bead. `attempt` is the 1-based number of this CI-fix attempt
    /// (1 on the first failure), for logging/operator context.
    Retry { attempt: u32 },
    /// At/over the cap: do NOT re-enqueue. Escalate to the operator. `attempts` is how many CI
    /// failures the bead accumulated before giving up; `reason` is the last CI log.
    Exhausted { attempts: u32, reason: String },
}

/// The CI-failure context recorded for a bead, handed to the next sling as continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiFailure {
    /// The CI failure reason/log forwarded on `merge.failed.v1` (cargo build/test errors, etc.).
    pub reason: String,
    /// 1-based attempt number this context belongs to (how many times CI has failed so far).
    pub attempt: u32,
}

/// In-memory, Arc-shared map `bead → CI-failure context`, with a per-bead attempt counter and a
/// retry cap. Writer: the scheduler reactor on `merge.failed.v1`. Reader: the sling path, which
/// drains the entry when it re-dispatches the bead.
#[derive(Debug)]
pub struct CiFailureStore {
    inner: Mutex<HashMap<String, CiFailure>>,
    max_retries: u32,
}

impl CiFailureStore {
    /// A store with the given retry cap. A cap of 0 means "never retry" — the first CI failure
    /// escalates immediately.
    pub fn new(max_retries: u32) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_retries,
        }
    }

    /// The configured retry cap.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Record a CI failure for `bead` and decide whether to retry. Bumps the bead's attempt
    /// counter; under the cap it stores the new failure context (for the next sling to pick up)
    /// and returns [`RetryDecision::Retry`]; at/over the cap it clears any stored context and
    /// returns [`RetryDecision::Exhausted`] so the caller escalates instead of re-enqueuing.
    ///
    /// `reason` is the CI log from the `merge.failed.v1` event. An empty reason is kept as-is —
    /// the prompt builder substitutes a placeholder.
    pub fn record_failure(&self, bead: &str, reason: &str) -> RetryDecision {
        let mut map = self.inner.lock().expect("ci-failure store mutex");
        // The attempt about to be made: prior attempts + 1. A bead with no entry is on attempt 1.
        let prev = map.get(bead).map(|f| f.attempt).unwrap_or(0);
        let attempt = prev + 1;
        if attempt > self.max_retries {
            // Budget spent: drop any stored context (the bead won't be re-slung) and escalate.
            map.remove(bead);
            return RetryDecision::Exhausted {
                attempts: prev,
                reason: reason.to_string(),
            };
        }
        map.insert(
            bead.to_string(),
            CiFailure {
                reason: reason.to_string(),
                attempt,
            },
        );
        RetryDecision::Retry { attempt }
    }

    /// Peek the CI-failure context for `bead` without removing it. The sling path uses this to
    /// decide whether to build a CI-fix prompt; it drains with [`take`](Self::take) once it commits
    /// to the sling.
    pub fn peek(&self, bead: &str) -> Option<CiFailure> {
        self.inner
            .lock()
            .expect("ci-failure store mutex")
            .get(bead)
            .cloned()
    }

    /// Drain the CI-failure context for `bead` (remove + return). Called at sling time: the context
    /// has been consumed into the kickoff prompt, so it must not leak into a *later*, unrelated
    /// dispatch of the same bead. The attempt counter is intentionally NOT reset here — a fresh
    /// `record_failure` after this sling correctly counts as the next attempt — but a successful
    /// merge clears it via [`clear`](Self::clear).
    ///
    /// Note the counter lives in the same entry as the context, so draining the entry would also
    /// drop the counter. To keep counting across slings we re-insert a counter-only marker (empty
    /// reason) so the next `record_failure` sees the right `prev`.
    pub fn take(&self, bead: &str) -> Option<CiFailure> {
        let mut map = self.inner.lock().expect("ci-failure store mutex");
        let taken = map.remove(bead)?;
        // Preserve the attempt counter for the in-flight retry without re-handing the (now
        // consumed) log to a future sling: a marker with an empty reason carries the count only.
        map.insert(
            bead.to_string(),
            CiFailure {
                reason: String::new(),
                attempt: taken.attempt,
            },
        );
        Some(taken)
    }

    /// Forget a bead entirely (context + attempt counter). Called when the bead finally merges
    /// (`merge.merged.v1`): the loop closed green, so a later, unrelated failure starts a fresh
    /// budget. Idempotent.
    pub fn clear(&self, bead: &str) {
        self.inner
            .lock()
            .expect("ci-failure store mutex")
            .remove(bead);
    }
}

/// Assemble the kickoff prompt for a bead whose PR FAILED CI (gtcore-3a1bd4). Unlike the
/// context-exhaustion continuation ([`crate::continuation::build_continuation_prompt`]), the agent
/// here is not picking up where a tired predecessor stopped — it is *fixing its own (or a peer's)
/// red build*. The prompt leads with the CI log so the very first thing the agent reads is the
/// concrete failure, then the branch diff (what's already committed), then the acceptance criteria,
/// then the imperative: fix it, build/test LOCALLY before pushing, re-push, signal completion.
///
/// `attempt`/`max` are surfaced so the agent knows how much budget remains (a 3rd-of-3 attempt
/// should be conservative). `branch` follows the repo convention of equalling the bead id.
pub fn build_ci_fix_prompt(
    bead: &str,
    ci_log: &str,
    diff: &str,
    acceptance_criteria: &str,
    attempt: u32,
    max: u32,
) -> String {
    let mut p = String::new();
    p.push_str(&format!(
        "You are a gt polecat RE-DISPATCHED on bead `{bead}` (branch `{bead}`) because its pull \
         request FAILED CI. This is CI-fix attempt {attempt} of {max}. Do NOT restart from \
         scratch and do NOT open a new approach — the branch already carries the work; your job is \
         to make CI GREEN by fixing the specific failure below, then re-push the SAME branch.\n\n"
    ));

    p.push_str("## CI failure log (fix THIS)\n");
    let log = ci_log.trim();
    if log.is_empty() {
        p.push_str(
            "(no CI log was captured on the failure event — inspect the PR's checks on GitHub, or \
             reproduce locally with `cargo build --locked` / `cargo test`)\n",
        );
    } else {
        p.push_str("```\n");
        p.push_str(log);
        p.push_str("\n```\n");
    }

    p.push_str("\n## Acceptance criteria (the bead is done when all are met AND CI is green)\n");
    let ac = acceptance_criteria.trim();
    p.push_str(if ac.is_empty() {
        "(none recorded on the bead — read it via the `gt` MCP tools)"
    } else {
        ac
    });
    p.push('\n');

    p.push_str("\n## Work already on the branch (`git diff main...HEAD`)\n");
    let diff = diff.trim();
    if diff.is_empty() {
        p.push_str(
            "(no diff captured — inspect the working tree; the failing commit is already on the \
             branch)\n",
        );
    } else {
        p.push_str("```diff\n");
        p.push_str(diff);
        p.push_str("\n```\n");
    }

    p.push_str(&format!(
        "\nFix the cause of the CI failure on branch `{bead}`. Before you push, BUILD AND TEST \
         LOCALLY — run `cargo build --locked` and the relevant `cargo test` (the most common cause \
         of this loop is Rust pushed without compiling locally: a missing `derive`, a type error, \
         or a stale test fixture such as the MCP parity list). Only once it compiles and the \
         affected tests pass, make a conventional commit and signal completion by calling the MCP \
         tool `mcp__gt__merge_submit` with arguments {{\"bead\":\"{bead}\",\"branch\":\"{bead}\"}}. \
         If the MCP tool is unavailable, fall back to the merge-ready channel as described in \
         CLAUDE.md. Then stop. Do not loop: if after a genuine fix you cannot get it green, leave \
         the diagnosis in the bead notes so the operator can take over."
    ));

    p
}

/// Truncate a CI log to the continuation diff budget so an enormous build log never blows the
/// kickoff prompt (the same byte ceiling the branch diff uses). Re-exported convenience over the
/// boundary helper in [`crate::continuation`]; kept here so the CI-loop call sites read clearly.
pub fn truncate_ci_log(log: &str) -> String {
    crate::continuation::truncate_for_prompt(log, MAX_DIFF_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_retries_at_attempt_one() {
        let store = CiFailureStore::new(3);
        assert_eq!(
            store.record_failure("gtcore-x", "cargo build error E0277"),
            RetryDecision::Retry { attempt: 1 }
        );
        let ctx = store.peek("gtcore-x").expect("context stored");
        assert_eq!(ctx.attempt, 1);
        assert!(ctx.reason.contains("E0277"));
    }

    #[test]
    fn retries_until_cap_then_escalates() {
        let store = CiFailureStore::new(2);
        assert_eq!(
            store.record_failure("b", "fail 1"),
            RetryDecision::Retry { attempt: 1 }
        );
        assert_eq!(
            store.record_failure("b", "fail 2"),
            RetryDecision::Retry { attempt: 2 }
        );
        // Third failure is over the cap of 2 → escalate, not retry.
        match store.record_failure("b", "fail 3") {
            RetryDecision::Exhausted { attempts, reason } => {
                assert_eq!(attempts, 2);
                assert_eq!(reason, "fail 3");
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
        // The stored context is gone — the bead won't be re-slung.
        assert!(store.peek("b").is_none());
    }

    #[test]
    fn cap_zero_escalates_on_first_failure() {
        let store = CiFailureStore::new(0);
        match store.record_failure("b", "boom") {
            RetryDecision::Exhausted { attempts, .. } => assert_eq!(attempts, 0),
            other => panic!("expected immediate Exhausted, got {other:?}"),
        }
    }

    #[test]
    fn take_drains_context_but_keeps_the_counter() {
        let store = CiFailureStore::new(3);
        store.record_failure("b", "fail 1");
        let taken = store.take("b").expect("context present");
        assert_eq!(taken.attempt, 1);
        assert_eq!(taken.reason, "fail 1");
        // The log is consumed (a later sling must not re-show it)…
        assert_eq!(store.peek("b").map(|c| c.reason), Some(String::new()));
        // …but the counter survives, so the NEXT failure is attempt 2, not 1.
        assert_eq!(
            store.record_failure("b", "fail 2"),
            RetryDecision::Retry { attempt: 2 }
        );
    }

    #[test]
    fn clear_resets_the_budget() {
        let store = CiFailureStore::new(2);
        store.record_failure("b", "fail 1");
        store.record_failure("b", "fail 2");
        store.clear("b");
        // After a clear (merge landed), a brand-new failure starts the budget over.
        assert_eq!(
            store.record_failure("b", "fresh fail"),
            RetryDecision::Retry { attempt: 1 }
        );
    }

    #[test]
    fn take_on_unknown_bead_is_none() {
        let store = CiFailureStore::new(3);
        assert!(store.take("nope").is_none());
    }

    #[test]
    fn ci_fix_prompt_leads_with_log_and_demands_local_build() {
        let prompt = build_ci_fix_prompt(
            "gtcore-3a1bd4",
            "error[E0277]: the trait bound `X: Serialize` is not satisfied",
            "diff --git a/x b/x\n+broken",
            "- cargo test green\n- CI --locked green",
            1,
            3,
        );
        assert!(prompt.contains("FAILED CI"));
        assert!(prompt.contains("CI-fix attempt 1 of 3"));
        // The concrete error is embedded.
        assert!(prompt.contains("E0277"));
        // It carries the diff and AC.
        assert!(prompt.contains("```diff"));
        assert!(prompt.contains("+broken"));
        assert!(prompt.contains("CI --locked green"));
        // It demands a local build before pushing and the merge_submit completion signal.
        assert!(prompt.contains("BUILD AND TEST"));
        assert!(prompt.contains("cargo build --locked"));
        assert!(prompt.contains("mcp__gt__merge_submit"));
        assert!(prompt.contains("\"bead\":\"gtcore-3a1bd4\""));
    }

    #[test]
    fn ci_fix_prompt_handles_empty_log_and_diff() {
        let prompt = build_ci_fix_prompt("b", "", "", "", 2, 3);
        assert!(prompt.contains("no CI log was captured"));
        assert!(prompt.contains("no diff captured"));
        assert!(!prompt.contains("```diff"));
    }
}
