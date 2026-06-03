//! The transport-agnostic notification payload + the signal taxonomy.

use serde::{Deserialize, Serialize};

/// Urgency of a notification. Maps onto the Go `mail.Priority` levels the bead-backed adapter
/// stamps on the mail it creates (`high`/`urgent` → interrupt delivery, lower → queue).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Urgent,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Urgent => "urgent",
        }
    }
}

/// What caused the notification. The routing policy ([`crate::route`]) is defined over this
/// enum, so adding a signal forces a routing decision at compile time. Mirrors the three
/// operator signals hq-mysw routes through the Notifier: escalation, quota-block, merge-stuck.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signal {
    /// A watched worker crossed its inactivity threshold (`witness.escalation_raised`).
    WorkerStuck { worker: String, age_secs: u64 },
    /// A merge slot failed / stalled (`merge.failed`, or a feed `TimeoutMissed` on a merge
    /// correlation).
    MergeStuck { bead: String, reason: String },
    /// An account hit / will hit its quota limit (`quota.account_limited` /
    /// `quota.block_predicted` / `quota.blocked`). The rotation chain already reacts; this is
    /// the human heads-up.
    QuotaBlock { account: String },
}

impl Signal {
    /// Stable tag for logs / the mail subject prefix.
    pub fn tag(&self) -> &'static str {
        match self {
            Signal::WorkerStuck { .. } => "worker_stuck",
            Signal::MergeStuck { .. } => "merge_stuck",
            Signal::QuotaBlock { .. } => "quota_block",
        }
    }
}

/// A ready-to-deliver notification. `signal` is the structured cause; `subject`/`body` are the
/// human-readable rendering an adapter puts on a mail / webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notification {
    pub signal: Signal,
    pub severity: Severity,
    pub subject: String,
    pub body: String,
}

impl Notification {
    pub fn new(signal: Signal, severity: Severity, subject: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            signal,
            severity,
            subject: subject.into(),
            body: body.into(),
        }
    }

    /// Build the canonical notification for a [`Signal`] with a sensible default subject/body
    /// and severity. The composition root uses this so every call site renders signals the
    /// same way.
    pub fn for_signal(signal: Signal) -> Self {
        match &signal {
            Signal::WorkerStuck { worker, age_secs } => Notification::new(
                signal.clone(),
                Severity::Urgent,
                format!("[escalation] worker {worker} stuck"),
                format!("Worker {worker} has been inactive for {age_secs}s past its threshold."),
            ),
            Signal::MergeStuck { bead, reason } => Notification::new(
                signal.clone(),
                Severity::Warning,
                format!("[merge-stuck] {bead}"),
                format!("Merge for {bead} did not complete: {reason}"),
            ),
            Signal::QuotaBlock { account } => Notification::new(
                signal.clone(),
                Severity::Warning,
                format!("[quota] account {account} blocked"),
                format!("Account {account} hit its quota limit; rotation triggered."),
            ),
        }
    }
}
