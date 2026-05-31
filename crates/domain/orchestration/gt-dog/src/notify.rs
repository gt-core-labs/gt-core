//! [`Notification`] + [`Notifier`] + [`notify_failure`] — surfacing a failed run
//! to an operator (`hq-mod-dogs.6`).
//!
//! `hq-mod-dogs.5` produced the [`Digest`] receipt of a run. This bead closes
//! the operator-signal loop carried over from `hq-mysw`: when a run **fails**, a
//! [`Notification`] is raised and pushed through the [`Notifier`] port
//! (`gt-notify`). Success is silent — [`notify_failure`] is the policy that only
//! fires on [`Outcome::Failed`], so a healthy pipeline stays quiet and an
//! operator's attention is reserved for real failures.
//!
//! ## Core vs. edge
//!
//! Deciding *whether* to notify and *what* the notification says is pure (it
//! reads a [`Digest`]); actually delivering it (mail, webhook, dashboard signal)
//! is I/O behind the [`Notifier`] port. `gt-notify` migrates up in Phase 4; per
//! the crate's hexagonal rule this module ships the port plus an
//! [`InMemoryNotifier`] adapter so the failure path is testable without I/O.

use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::state::DogId;
use crate::tracking::Digest;

/// An operator-facing alert that a Dog run failed.
///
/// Built from a failed [`Digest`] via [`from_digest`](Notification::from_digest);
/// carries the run's identity, the failure reason, and the digest's full label
/// set (with the reserved `dog`/`claim`/`outcome` keys) for routing and grouping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    /// The Dog whose run failed.
    pub dog: DogId,
    /// The claim that failed.
    pub claim: String,
    /// The failure reason carried from the report, or a default if none was
    /// recorded.
    pub reason: String,
    /// The digest's full label set (custom labels + reserved keys).
    pub labels: BTreeMap<String, String>,
}

/// Used when a failed digest carries no detail string — keeps a notification
/// from ever having an empty reason.
const UNSPECIFIED_REASON: &str = "unspecified failure";

impl Notification {
    /// Build a notification from a digest, or `None` if the run succeeded.
    ///
    /// Only [`Outcome::Failed`](crate::Outcome::Failed) digests yield a
    /// notification; a successful run returns `None` so callers can funnel every
    /// digest through without branching. The reason falls back to a non-empty
    /// default when the digest recorded no detail.
    pub fn from_digest(digest: &Digest) -> Option<Self> {
        if !digest.is_failure() {
            return None;
        }
        Some(Notification {
            dog: digest.dog.clone(),
            claim: digest.claim.clone(),
            reason: digest
                .detail
                .clone()
                .unwrap_or_else(|| UNSPECIFIED_REASON.to_string()),
            labels: digest.labels(),
        })
    }

    /// A one-line operator summary, e.g. `dog `sheriff` failed claim
    /// `premerge-42`: boom`.
    pub fn summary(&self) -> String {
        format!("dog `{}` failed claim `{}`: {}", self.dog, self.claim, self.reason)
    }
}

/// Why delivering a [`Notification`] failed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotifyError {
    /// The notifier could not deliver; carries an adapter-specific reason.
    Delivery(String),
}

impl std::fmt::Display for NotifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotifyError::Delivery(reason) => write!(f, "notification delivery failed: {reason}"),
        }
    }
}

impl std::error::Error for NotifyError {}

/// The delivery port for a [`Notification`] — the `gt-notify` seam.
///
/// `async` + `dyn`-friendly under the non-negotiable #1 plugin exception:
/// delivery (mail, webhook, dashboard signal) is I/O at the edge. The real
/// `gt-notify` adapter lands in Phase 4; [`InMemoryNotifier`] is the in-repo
/// adapter for tests.
#[async_trait]
pub trait Notifier: Send + Sync {
    /// Deliver `notification`. Returns [`NotifyError`] if it could not be sent.
    async fn notify(&self, notification: &Notification) -> Result<(), NotifyError>;
}

/// Notify an operator iff `digest` records a failure.
///
/// Returns `Ok(true)` when a notification was built and delivered, `Ok(false)`
/// when the run succeeded and nothing was sent, or the [`Notifier`]'s
/// [`NotifyError`] if delivery failed. This is the single entry point a caller
/// uses to funnel every digest through without inspecting the outcome itself.
pub async fn notify_failure<N: Notifier + ?Sized>(
    notifier: &N,
    digest: &Digest,
) -> Result<bool, NotifyError> {
    match Notification::from_digest(digest) {
        Some(n) => notifier.notify(&n).await.map(|()| true),
        None => Ok(false),
    }
}

/// In-memory [`Notifier`] retaining every delivered [`Notification`] in order.
///
/// The hexagonal in-repo adapter (Port + InMemory): drives tests and any caller
/// that just needs alerts collected, with no `gt-notify` dependency.
#[derive(Default)]
pub struct InMemoryNotifier {
    sent: std::sync::Mutex<Vec<Notification>>,
}

impl InMemoryNotifier {
    /// A fresh notifier holding no alerts.
    pub fn new() -> Self {
        InMemoryNotifier::default()
    }

    /// Snapshot the notifications delivered so far, in delivery order.
    pub fn sent(&self) -> Vec<Notification> {
        self.sent.lock().expect("notifier mutex not poisoned").clone()
    }

    /// How many notifications have been delivered.
    pub fn len(&self) -> usize {
        self.sent.lock().expect("notifier mutex not poisoned").len()
    }

    /// Whether no notifications have been delivered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Notifier for InMemoryNotifier {
    async fn notify(&self, notification: &Notification) -> Result<(), NotifyError> {
        self.sent
            .lock()
            .expect("notifier mutex not poisoned")
            .push(notification.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dog::DogReport;
    use crate::tracking::TrackingLabels;

    fn dog(id: &str) -> DogId {
        DogId::new(id).unwrap()
    }

    fn failed_digest(reason: Option<&str>) -> Digest {
        let report = match reason {
            Some(r) => DogReport::Failed(r.to_string()),
            None => DogReport::Failed(String::new()),
        };
        let mut d = Digest::from_report(dog("sheriff"), "premerge-42", &report, TrackingLabels::new());
        if reason.is_none() {
            // Model a failure that carried no detail string.
            d.detail = None;
        }
        d
    }

    fn ok_digest() -> Digest {
        Digest::from_report(dog("sheriff"), "premerge-42", &DogReport::Completed, TrackingLabels::new())
    }

    #[test]
    fn notification_built_only_from_a_failure() {
        assert!(Notification::from_digest(&ok_digest()).is_none());
        let n = Notification::from_digest(&failed_digest(Some("boom"))).unwrap();
        assert_eq!(n.dog.as_str(), "sheriff");
        assert_eq!(n.claim, "premerge-42");
        assert_eq!(n.reason, "boom");
    }

    #[test]
    fn notification_carries_digest_labels() {
        let d = Digest::from_report(
            dog("sheriff"),
            "x",
            &DogReport::Failed("boom".into()),
            TrackingLabels::new().with("epic", "hq-mod-dogs"),
        );
        let n = Notification::from_digest(&d).unwrap();
        assert_eq!(n.labels["outcome"], "failed");
        assert_eq!(n.labels["epic"], "hq-mod-dogs");
    }

    #[test]
    fn missing_detail_falls_back_to_a_non_empty_reason() {
        let n = Notification::from_digest(&failed_digest(None)).unwrap();
        assert_eq!(n.reason, UNSPECIFIED_REASON);
        assert!(!n.reason.is_empty());
    }

    #[test]
    fn summary_is_one_line() {
        let n = Notification::from_digest(&failed_digest(Some("boom"))).unwrap();
        assert_eq!(n.summary(), "dog `sheriff` failed claim `premerge-42`: boom");
    }

    #[tokio::test]
    async fn notify_failure_sends_for_a_failed_run() {
        let notifier = InMemoryNotifier::new();
        let sent = notify_failure(&notifier, &failed_digest(Some("boom"))).await.unwrap();
        assert!(sent);
        assert_eq!(notifier.len(), 1);
        assert_eq!(notifier.sent()[0].reason, "boom");
    }

    #[tokio::test]
    async fn notify_failure_is_silent_for_a_successful_run() {
        let notifier = InMemoryNotifier::new();
        let sent = notify_failure(&notifier, &ok_digest()).await.unwrap();
        assert!(!sent);
        assert!(notifier.is_empty());
    }

    #[tokio::test]
    async fn delivery_error_propagates() {
        struct FailingNotifier;
        #[async_trait]
        impl Notifier for FailingNotifier {
            async fn notify(&self, _n: &Notification) -> Result<(), NotifyError> {
                Err(NotifyError::Delivery("smtp down".into()))
            }
        }
        let err = notify_failure(&FailingNotifier, &failed_digest(Some("boom")))
            .await
            .unwrap_err();
        assert_eq!(err, NotifyError::Delivery("smtp down".into()));
    }
}
