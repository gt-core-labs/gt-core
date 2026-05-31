//! [`Digest`] + [`TrackingLabels`] + [`ReceiptSink`] — the receipt a Dog run
//! produces and where it is injected (`hq-mod-dogs.5`).
//!
//! When a [`Dog`](crate::Dog) finishes a claim it leaves a trail: a coarse
//! outcome plus provenance labels (which Dog, which claim, completed/failed).
//! `hq-mod-dogs.4` produced the [`DogReport`]; this bead turns that into a
//! [`Digest`] — the receipt — carrying [`TrackingLabels`] for grouping and
//! attribution, and defines the [`ReceiptSink`] port the digest is injected into
//! (a Wisp ephemeral-memory entry, a receipt bead).
//!
//! ## What is core and what is edge
//!
//! Building a [`Digest`] from a report is pure and deterministic — it sits in
//! the sync core (non-negotiable #2). Writing it to Wisp or a bead is I/O, so it
//! lives behind the [`ReceiptSink`] port; the real Wisp/bead adapter lands with
//! the runtime wiring (`hq-mod-dogs.8`/`.9`, `gt-wisp`/`store.beads` migrate up
//! in Phase 4). Per the crate's hexagonal rule this module ships the port plus
//! an [`InMemoryReceipts`] adapter so the digest path is testable without I/O.

use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::dog::DogReport;
use crate::state::DogId;

/// The coarse result a run is recorded under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The claim ran to completion.
    Completed,
    /// The claim failed.
    Failed,
}

impl Outcome {
    /// The canonical lowercase label for this outcome (`completed`/`failed`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::Completed => "completed",
            Outcome::Failed => "failed",
        }
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Free-form provenance labels attached to a [`Digest`].
///
/// A sorted key→value map, so the rendered label set is deterministic (a receipt
/// replays/serializes identically). Reserved keys (`dog`, `claim`, `outcome`)
/// are injected by [`Digest::labels`] and always win over a user label of the
/// same name, so attribution can never be spoofed by a colliding custom label.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrackingLabels(BTreeMap<String, String>);

/// Keys [`Digest::labels`] injects; a user-supplied label of the same name is
/// overridden so provenance is authoritative.
const RESERVED_KEYS: [&str; 3] = ["dog", "claim", "outcome"];

impl TrackingLabels {
    /// An empty label set.
    pub fn new() -> Self {
        TrackingLabels(BTreeMap::new())
    }

    /// Add or replace a label, returning `self` so calls chain.
    ///
    /// A reserved key (`dog`/`claim`/`outcome`) may be set here but is overridden
    /// by [`Digest::labels`]; it is not rejected, so callers can stage defaults.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }

    /// Look up a label value.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Iterate labels in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Number of labels held.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no labels are held.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The receipt one Dog run leaves behind.
///
/// Built from a [`DogReport`] via [`from_report`](Digest::from_report): the
/// outcome, the failure detail if any, plus the run's identity and custom
/// [`TrackingLabels`]. Pure data — cheap to clone, safe to snapshot, injected
/// into a [`ReceiptSink`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Digest {
    /// The Dog that ran the claim.
    pub dog: DogId,
    /// The claim reference that was run.
    pub claim: String,
    /// The coarse outcome.
    pub outcome: Outcome,
    /// Human-readable detail — the failure reason for [`Outcome::Failed`],
    /// `None` on success.
    pub detail: Option<String>,
    /// Custom provenance labels supplied by the caller.
    pub labels: TrackingLabels,
}

impl Digest {
    /// Build a digest from a finished run.
    ///
    /// Maps [`DogReport::Completed`] to [`Outcome::Completed`] (no detail) and
    /// [`DogReport::Failed`] to [`Outcome::Failed`], carrying the reason as the
    /// detail. `labels` are the caller's custom labels; the reserved
    /// `dog`/`claim`/`outcome` keys are added by [`labels`](Digest::labels).
    pub fn from_report(
        dog: DogId,
        claim: impl Into<String>,
        report: &DogReport,
        labels: TrackingLabels,
    ) -> Self {
        let (outcome, detail) = match report {
            DogReport::Completed => (Outcome::Completed, None),
            DogReport::Failed(reason) => (Outcome::Failed, Some(reason.clone())),
        };
        Digest {
            dog,
            claim: claim.into(),
            outcome,
            detail,
            labels,
        }
    }

    /// Whether this digest records a failure.
    pub fn is_failure(&self) -> bool {
        self.outcome == Outcome::Failed
    }

    /// The full label set injected with the run: the custom labels merged with
    /// the reserved `dog`/`claim`/`outcome` keys, which override any custom label
    /// of the same name. Returned key-sorted for a deterministic receipt.
    pub fn labels(&self) -> BTreeMap<String, String> {
        let mut merged: BTreeMap<String, String> = self
            .labels
            .iter()
            .filter(|(k, _)| !RESERVED_KEYS.contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        merged.insert("dog".into(), self.dog.as_str().to_string());
        merged.insert("claim".into(), self.claim.clone());
        merged.insert("outcome".into(), self.outcome.as_str().to_string());
        merged
    }
}

/// Why injecting a [`Digest`] into a sink failed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReceiptError {
    /// The sink could not record the digest; carries an adapter-specific reason.
    Sink(String),
}

impl std::fmt::Display for ReceiptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiptError::Sink(reason) => write!(f, "receipt sink failed: {reason}"),
        }
    }
}

impl std::error::Error for ReceiptError {}

/// Where a [`Digest`] is injected — a Wisp ephemeral-memory entry, a receipt
/// bead.
///
/// `async` + `dyn`-friendly (the non-negotiable #1 plugin exception): emitting a
/// receipt is I/O at the edge. The real Wisp/bead adapter lands with the runtime
/// wiring; [`InMemoryReceipts`] is the in-repo adapter for tests.
#[async_trait]
pub trait ReceiptSink: Send + Sync {
    /// Record `digest`. Returns [`ReceiptError`] if the sink could not persist it.
    async fn emit(&self, digest: &Digest) -> Result<(), ReceiptError>;
}

/// In-memory [`ReceiptSink`] that retains every emitted [`Digest`] in order.
///
/// The hexagonal in-repo adapter (Port + InMemory): drives tests and any caller
/// that just needs the receipts collected, with no Wisp/bead dependency.
#[derive(Default)]
pub struct InMemoryReceipts {
    emitted: std::sync::Mutex<Vec<Digest>>,
}

impl InMemoryReceipts {
    /// A fresh sink holding no receipts.
    pub fn new() -> Self {
        InMemoryReceipts::default()
    }

    /// Snapshot the receipts emitted so far, in emission order.
    pub fn recorded(&self) -> Vec<Digest> {
        self.emitted.lock().expect("receipts mutex not poisoned").clone()
    }

    /// How many receipts have been emitted.
    pub fn len(&self) -> usize {
        self.emitted.lock().expect("receipts mutex not poisoned").len()
    }

    /// Whether no receipts have been emitted.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ReceiptSink for InMemoryReceipts {
    async fn emit(&self, digest: &Digest) -> Result<(), ReceiptError> {
        self.emitted
            .lock()
            .expect("receipts mutex not poisoned")
            .push(digest.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dog(id: &str) -> DogId {
        DogId::new(id).unwrap()
    }

    #[test]
    fn digest_from_completed_report_has_no_detail() {
        let d = Digest::from_report(dog("sheriff"), "premerge-42", &DogReport::Completed, TrackingLabels::new());
        assert_eq!(d.outcome, Outcome::Completed);
        assert_eq!(d.detail, None);
        assert!(!d.is_failure());
    }

    #[test]
    fn digest_from_failed_report_carries_the_reason() {
        let d = Digest::from_report(
            dog("sheriff"),
            "premerge-42",
            &DogReport::Failed("boom".into()),
            TrackingLabels::new(),
        );
        assert_eq!(d.outcome, Outcome::Failed);
        assert_eq!(d.detail.as_deref(), Some("boom"));
        assert!(d.is_failure());
    }

    #[test]
    fn labels_inject_reserved_keys() {
        let d = Digest::from_report(
            dog("digest"),
            "claim-7",
            &DogReport::Completed,
            TrackingLabels::new().with("epic", "hq-mod-dogs"),
        );
        let labels = d.labels();
        assert_eq!(labels["dog"], "digest");
        assert_eq!(labels["claim"], "claim-7");
        assert_eq!(labels["outcome"], "completed");
        assert_eq!(labels["epic"], "hq-mod-dogs");
    }

    #[test]
    fn reserved_keys_override_custom_labels() {
        // A caller trying to spoof `dog`/`outcome` is overridden by the run's truth.
        let d = Digest::from_report(
            dog("sheriff"),
            "x",
            &DogReport::Failed("nope".into()),
            TrackingLabels::new().with("dog", "impostor").with("outcome", "completed"),
        );
        let labels = d.labels();
        assert_eq!(labels["dog"], "sheriff");
        assert_eq!(labels["outcome"], "failed");
    }

    #[test]
    fn tracking_labels_are_sorted_and_queryable() {
        let l = TrackingLabels::new().with("b", "2").with("a", "1");
        assert_eq!(l.get("a"), Some("1"));
        assert_eq!(l.len(), 2);
        let keys: Vec<_> = l.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn in_memory_sink_retains_digests_in_order() {
        let sink = InMemoryReceipts::new();
        assert!(sink.is_empty());
        let a = Digest::from_report(dog("a"), "1", &DogReport::Completed, TrackingLabels::new());
        let b = Digest::from_report(dog("b"), "2", &DogReport::Failed("x".into()), TrackingLabels::new());
        sink.emit(&a).await.unwrap();
        sink.emit(&b).await.unwrap();
        let recorded = sink.recorded();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].dog.as_str(), "a");
        assert_eq!(recorded[1].dog.as_str(), "b");
        assert!(recorded[1].is_failure());
    }
}
