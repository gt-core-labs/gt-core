//! Escalation intents derived from feed problems (hq-mysw).
//!
//! The feed already detects semantic gaps ([`crate::FeedProblem`]). This module is the pure
//! read-side decision of *which* of those gaps warrant an operator escalation — the "feed
//! detecta hueco past threshold" half of hq-mysw. It emits a type-erased [`EscalationIntent`]
//! (class + subject + summary) and stays kernel-only: it does **not** know about the Notifier
//! port, beads, or roles. The composition root maps these intents onto the live escalation
//! action (status bead + notification) and the `gt-notify` routing policy.
//!
//! Mapping (see `docs/13-operator-signals.md` for the rationale):
//!
//! - [`FeedProblem::TimeoutMissed`] → a stuck workflow (e.g. a `merge.failed` with no later
//!   `merge.merged`). Escalation-worthy.
//! - [`FeedProblem::DeadLetterDrain`] → a handler returned `Err`; the work silently dropped.
//!   Escalation-worthy.
//! - [`FeedProblem::UnhandledEvent`] → published with no subscriber. Feed-only: it is a wiring
//!   smell surfaced in the TUI, not an operator page, so it does **not** raise an intent.

use serde::{Deserialize, Serialize};

use crate::problems::FeedProblem;

/// A gap the feed considers worth escalating, in type-erased form. `class` is a stable string
/// the root maps onto a `gt-notify` signal; `subject` is the correlation/kind the gap is about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscalationIntent {
    /// Stable class tag: `"timeout_missed"` or `"handler_error"`.
    pub class: String,
    /// What the gap is about: the correlation id (timeout) or the event kind (handler error).
    pub subject: String,
    /// Human-readable one-liner for the status bead / notification body.
    pub summary: String,
}

/// Derive escalation intents from detected feed problems. Pure and order-preserving:
/// re-running over the same problem slice yields the same intents.
pub fn intents(problems: &[FeedProblem]) -> Vec<EscalationIntent> {
    problems.iter().filter_map(intent).collect()
}

fn intent(p: &FeedProblem) -> Option<EscalationIntent> {
    match p {
        FeedProblem::TimeoutMissed {
            correlation_id,
            terminal_kind,
            ..
        } => Some(EscalationIntent {
            class: "timeout_missed".to_string(),
            subject: correlation_id.clone(),
            summary: format!("{correlation_id} ended in {terminal_kind} without success"),
        }),
        FeedProblem::DeadLetterDrain { kind, error } => Some(EscalationIntent {
            class: "handler_error".to_string(),
            subject: kind.clone(),
            summary: format!("handler for {kind} failed: {error}"),
        }),
        // Unhandled = no subscriber. A wiring smell for the TUI, not an operator page.
        FeedProblem::UnhandledEvent { .. } => None,
    }
}
