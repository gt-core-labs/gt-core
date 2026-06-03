//! The signal → channel routing policy: which signals warrant a notification vs feed-only.
//!
//! The audit log + the feed (`gt-feed`) already record *everything*. A notification is the
//! extra step of pushing a signal at a human. The rule of thumb hq-mysw documents
//! (`docs/13-operator-signals.md`): notify only when a human must act and the runtime cannot
//! self-heal. Routine, self-recovering, or purely informational gaps stay feed-only so the
//! operator's inbox does not become noise they learn to ignore.

use crate::notification::Signal;

/// Where a signal goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Deliver via the [`crate::Notifier`] (the bead-backed mail adapter in production).
    Mail,
    /// Record-only: it is already in the audit log / feed; do not page anyone.
    FeedOnly,
}

/// The policy. All three hq-mysw operator signals warrant mail today; the enum keeps room for
/// feed-only classes (e.g. an unhandled-event wiring smell the TUI surfaces) without a code
/// change at the call sites.
pub fn route(signal: &Signal) -> Channel {
    match signal {
        // A stuck worker needs an operator: the runtime cannot un-stick it on its own.
        Signal::WorkerStuck { .. } => Channel::Mail,
        // A failed/stuck merge blocks the lane until someone resolves the conflict.
        Signal::MergeStuck { .. } => Channel::Mail,
        // Rotation self-heals capacity, but a human should know an account went dark.
        Signal::QuotaBlock { .. } => Channel::Mail,
    }
}
