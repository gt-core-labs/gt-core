//! Per-workspace event-log rehydration for the **event-sourced** domain handlers
//! (`hq-mcp-dispatch.5/.6/.7` + quota's probe/rotate).
//!
//! merge, convoy (orchestration), and agent keep no projection table: their state
//! is a log of events replayed into an in-memory reducer (docs/06 Step-3 gate).
//! Their durable store is therefore [`gt_eventlog`], not Postgres. A dispatch
//! call on such a domain is symmetric to the table handlers' hydrate → execute →
//! persist, with the event log standing in for the table:
//!
//! 1. [`replay_domain`](EventLog::replay_domain) reads the workspace's log, keeps
//!    the records whose `kind` belongs to the domain (`<ns>.`), and folds them
//!    through the domain reducer to rebuild state;
//! 2. the handler executes the command against that state, producing a new event;
//! 3. [`append`](EventLog::append) writes the event back to the workspace log.
//!
//! The read-modify-append is not cross-process atomic (each append is file-locked,
//! but the read→append window is not). That matches the single-writer MCP server;
//! a stronger guarantee is a follow-up when multiple writers share a log.

use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;

use gt_eventlog::{replay, EventRecord, EventStore, JsonlWriter, DEFAULT_EVENTLOG_ROOT};
use gt_events::{Envelope, EventKind};
use gt_store_dolt::AppError;

use super::util::ev_err;

/// The workspace a request resolves to when it carries no `X-Workspace` header.
const DEFAULT_WORKSPACE: &str = "default";

/// Per-workspace event-log access rooted at a log volume (defaults to
/// [`DEFAULT_EVENTLOG_ROOT`], `/var/lib/gt-core`).
#[derive(Clone)]
pub struct EventLog {
    root: PathBuf,
}

impl EventLog {
    /// Root the log access at `root`, or [`DEFAULT_EVENTLOG_ROOT`] when `None`.
    pub fn new(root: Option<PathBuf>) -> Self {
        Self {
            root: root.unwrap_or_else(|| PathBuf::from(DEFAULT_EVENTLOG_ROOT)),
        }
    }

    /// The workspace's append-only log (segments under `<root>/<ws>/`).
    fn store(&self, workspace: Option<&str>) -> Result<JsonlWriter, AppError> {
        JsonlWriter::for_workspace_in(&self.root, workspace.unwrap_or(DEFAULT_WORKSPACE))
            .map_err(ev_err)
    }

    /// Rebuild a domain's state by folding the workspace log.
    ///
    /// Only records whose `kind` starts with `prefix` (the domain namespace plus a
    /// dot, e.g. `"merge."`) are decoded into `E` and applied — the log is
    /// heterogeneous, so a foreign kind must never reach the typed reducer. `apply`
    /// is the domain's pure reducer (`State::apply`).
    pub fn replay_domain<S, E, F>(
        &self,
        workspace: Option<&str>,
        prefix: &str,
        initial: S,
        apply: F,
    ) -> Result<S, AppError>
    where
        E: for<'de> DeserializeOwned,
        F: FnMut(&mut S, &E),
    {
        let store = self.store(workspace)?;
        let records: Vec<EventRecord> = store
            .read_all()
            .map_err(ev_err)?
            .into_iter()
            .filter(|r| r.kind.starts_with(prefix))
            .collect();
        replay(&records, initial, apply).map_err(ev_err)
    }

    /// Append one decided event to the workspace log.
    pub fn append<E>(&self, workspace: Option<&str>, event: E) -> Result<(), AppError>
    where
        E: EventKind + Serialize,
    {
        let store = self.store(workspace)?;
        let record = EventRecord::from_envelope(&Envelope::root(event)).map_err(ev_err)?;
        store.append(&record).map_err(ev_err)
    }
}
