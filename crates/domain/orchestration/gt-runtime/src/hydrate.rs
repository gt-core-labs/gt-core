//! Per-workspace event-log hydration on first resolve (`hq-mt-routing.3`).
//!
//! When [`RootRegistry`](crate::RootRegistry) resolves a workspace for the first time it
//! builds the tenant's [`RootHandle`](crate::RootHandle) through a caller-supplied hydrate
//! closure (`get_or_hydrate*`). That closure needs the workspace's **persisted history** to
//! rebuild domain state: this module is the read side of it.
//!
//! [`workspace_event_records`] loads a workspace's append-only log from the `gt-eventlog`
//! volume — the per-day segments `<root>/<ws>/events-YYYY-MM-DD.jsonl` laid down by
//! `hq-mt-data.8` — into the records to replay, in chronological order across segments. The
//! hydrate closure then folds them through each domain's reducer with
//! [`gt_eventlog::replay`] to reconstruct byte-identical state (the replay gate — the core is
//! pure and synchronous, so re-running the log reproduces the exact state, see
//! `docs/06-observability.md`).
//!
//! A brand-new workspace has no log yet, so it hydrates from an **empty** slice — nothing to
//! replay, the domains start at their initial state. The lookup itself provisions the
//! workspace's log directory lazily and idempotently (the filesystem mirror of how a PG
//! schema is created on first use, `docs/09-mt-storage-layout.md`).

use std::path::Path;

use gt_eventlog::{EventRecord, EventStore, JsonlWriter};
use gt_events::AppError;
use gt_workspace::WorkspaceId;

/// Load a workspace's persisted events from the event-log volume rooted at `eventlog_root`,
/// in chronological order across the daily segments. Returns an empty vector for a workspace
/// with no log yet (a new tenant — nothing to replay).
///
/// The records are the input to the per-domain replay the hydrate closure runs on first
/// resolve; this function performs no replay itself (it stays free of any domain reducer, so
/// the composition root decides which domains fold which kinds).
pub fn workspace_event_records(
    eventlog_root: impl AsRef<Path>,
    ws: &WorkspaceId,
) -> Result<Vec<EventRecord>, AppError> {
    JsonlWriter::for_workspace_in(eventlog_root, ws.as_str())?.read_all()
}

/// [`workspace_event_records`] against the production volume root
/// ([`gt_eventlog::DEFAULT_EVENTLOG_ROOT`], `/var/lib/gt-core`).
pub fn workspace_event_records_default(ws: &WorkspaceId) -> Result<Vec<EventRecord>, AppError> {
    workspace_event_records(gt_eventlog::DEFAULT_EVENTLOG_ROOT, ws)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::new(id).unwrap()
    }

    fn rec(id: &str, ts: &str) -> EventRecord {
        EventRecord {
            event_id: id.to_string(),
            correlation_id: id.to_string(),
            causation_id: None,
            ts: ts.to_string(),
            kind: "test.event.v1".to_string(),
            payload: serde_json::json!({ "n": id }),
        }
    }

    /// First resolve of a workspace that has never persisted an event hydrates from nothing.
    #[test]
    fn new_workspace_hydrates_empty() {
        let root = tempfile::tempdir().unwrap();
        let recs = workspace_event_records(root.path(), &ws("fresh")).unwrap();
        assert!(recs.is_empty());
    }

    /// The log is read back in chronological order across day-rotated segments — the
    /// byte-replay gate at the log level: what the writer persisted is what hydration sees,
    /// in order, regardless of which segment each record landed in.
    #[test]
    fn hydration_reads_segments_in_chronological_order() {
        let root = tempfile::tempdir().unwrap();
        let writer = JsonlWriter::for_workspace_in(root.path(), "acme").unwrap();
        // Persist across two UTC days, appended out of day order on purpose.
        writer.append(&rec("c", "2026-06-03T09:00:00Z")).unwrap();
        writer.append(&rec("a", "2026-06-02T08:00:00Z")).unwrap();
        writer.append(&rec("b", "2026-06-02T20:00:00Z")).unwrap();

        let recs = workspace_event_records(root.path(), &ws("acme")).unwrap();
        let ids: Vec<_> = recs.iter().map(|r| r.event_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    /// A workspace's records hydrate independently — no cross-tenant bleed.
    #[test]
    fn hydration_is_per_workspace() {
        let root = tempfile::tempdir().unwrap();
        JsonlWriter::for_workspace_in(root.path(), "acme")
            .unwrap()
            .append(&rec("a", "2026-06-02T08:00:00Z"))
            .unwrap();
        JsonlWriter::for_workspace_in(root.path(), "globex")
            .unwrap()
            .append(&rec("g1", "2026-06-02T08:00:00Z"))
            .unwrap();

        assert_eq!(workspace_event_records(root.path(), &ws("acme")).unwrap().len(), 1);
        assert_eq!(workspace_event_records(root.path(), &ws("globex")).unwrap().len(), 1);
    }
}
