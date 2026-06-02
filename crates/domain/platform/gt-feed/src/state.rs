//! Aggregated, type-erased projection over the audit log.
//!
//! `FeedState` is grouped by `correlation_id` so the TUI can show every workflow as a
//! lifeline (the events caused by one spawn / one merge / one rotation, in order). Inside a
//! correlation, the per-`kind` counts let problem detection flag opener/closer asymmetries
//! without knowing what those `kind`s mean.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use gt_eventlog::EventRecord;

/// Reference to one event inside a correlation lifeline — enough to display + sort, no
/// payload (the curator stays type-erased).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRef {
    pub event_id: String,
    pub kind: String,
    pub ts: String,
    pub causation_id: Option<String>,
}

impl EventRef {
    pub(crate) fn from_record(rec: &EventRecord) -> Self {
        Self {
            event_id: rec.event_id.clone(),
            kind: rec.kind.clone(),
            ts: rec.ts.clone(),
            causation_id: rec.causation_id.clone(),
        }
    }
}

/// All events sharing one `correlation_id` — one workflow lifeline.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Correlation {
    pub correlation_id: String,
    pub events: Vec<EventRef>,
    /// `kind` → count within this correlation. Append-only; used by problem detection.
    pub kind_counts: BTreeMap<String, u32>,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

impl Correlation {
    fn new(correlation_id: String) -> Self {
        Self {
            correlation_id,
            ..Self::default()
        }
    }

    fn ingest(&mut self, rec: &EventRecord) {
        if self.first_ts.is_none() {
            self.first_ts = Some(rec.ts.clone());
        }
        self.last_ts = Some(rec.ts.clone());
        *self.kind_counts.entry(rec.kind.clone()).or_insert(0) += 1;
        self.events.push(EventRef::from_record(rec));
    }
}

/// Whole-log projection. `BTreeMap` keeps deterministic iteration order (a prerequisite for
/// the byte-identical replay gate).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FeedState {
    pub correlations: BTreeMap<String, Correlation>,
    /// Total tally of every `kind` seen across all correlations. Sorted for determinism.
    pub kind_totals: BTreeMap<String, u32>,
    pub total_events: u64,
    pub last_ts: Option<String>,
}

impl FeedState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn ingest(&mut self, rec: &EventRecord) {
        self.total_events += 1;
        self.last_ts = Some(rec.ts.clone());
        *self.kind_totals.entry(rec.kind.clone()).or_insert(0) += 1;
        let entry = self
            .correlations
            .entry(rec.correlation_id.clone())
            .or_insert_with(|| Correlation::new(rec.correlation_id.clone()));
        entry.ingest(rec);
    }
}
