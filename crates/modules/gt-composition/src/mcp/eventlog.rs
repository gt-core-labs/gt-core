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
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use gt_eventlog::{
    replay, EventRecord, EventStore, JsonlWriter, PgEventStore, DEFAULT_EVENTLOG_ROOT,
};
use gt_events::{Envelope, EventKind};
use gt_issues::{IssueEvent, IssueEventSink};
use gt_store_dolt::AppError;

use super::util::ev_err;

/// The workspace a request resolves to when it carries no `X-Workspace` header.
const DEFAULT_WORKSPACE: &str = "default";

/// The durable backend the [`EventLog`] dispatches its four sync operations to.
///
/// REVERSIBLE BY ENV (hq-talos-migration.10): the file backend ([`Backend::File`]) is the default
/// and fallback — the existing single-node shared-volume deploy keeps working untouched. The
/// Postgres backend ([`Backend::Pg`]) is selected by `GT_EVENTLOG_PG` / `GT_PG_URL` so N
/// mcp-server replicas + a separate orchd write the log concurrently over one DB with no shared
/// volume; flipping the env back to unset restores the file path.
///
/// Both arms satisfy the SAME synchronous contract — the public `EventLog` methods never become
/// `async`, so the ~30 consumers across every event-sourced domain are byte-for-byte unaffected.
#[derive(Clone)]
enum Backend {
    /// Path-partitioned `events*.jsonl` under a log root (`<root>/<ws>/`).
    File(PathBuf),
    /// A single `public.events` table keyed by a `workspace` column, over a sync pooled
    /// Postgres client (concurrent writers, no shared volume).
    Pg(PgEventStore),
}

/// Per-workspace event-log access. Backed either by the path-partitioned file log (default /
/// fallback) or by Postgres (concurrent writers), selected at construction; the four public
/// methods (`append`/`replay_domain`/`read_since`/`workspaces`) are SYNC on both and dispatch to
/// the active backend.
#[derive(Clone)]
pub struct EventLog {
    backend: Backend,
}

impl EventLog {
    /// File backend: root the log access at `root`, or [`DEFAULT_EVENTLOG_ROOT`] when `None`. This
    /// is the default/fallback the existing single-node deploy uses — unchanged behaviour.
    pub fn new(root: Option<PathBuf>) -> Self {
        Self {
            backend: Backend::File(
                root.unwrap_or_else(|| PathBuf::from(DEFAULT_EVENTLOG_ROOT)),
            ),
        }
    }

    /// Postgres backend (hq-talos-migration.10): back the event log with the sync
    /// [`PgEventStore`] over `conn_str` so concurrent writers share one DB instead of a volume.
    /// Self-heals the `public.events` schema on construction (the boot migration loop also applies
    /// it — idempotent), so a backend built outside the boot path (orchd, tests) is usable.
    pub fn new_pg(conn_str: &str) -> Result<Self, AppError> {
        let store = PgEventStore::connect(conn_str).map_err(ev_err)?;
        store.ensure_schema().map_err(ev_err)?;
        Ok(Self {
            backend: Backend::Pg(store),
        })
    }

    /// Wrap an already-built [`PgEventStore`] (tests construct one against a disposable container).
    pub fn from_pg_store(store: PgEventStore) -> Self {
        Self {
            backend: Backend::Pg(store),
        }
    }

    /// The whole workspace log in durable order, the read primitive both `replay_domain` and
    /// `read_since` filter in Rust. ONE place reads, so the prefix/channel/since/limit semantics
    /// below are byte-for-byte identical on both backends — the file path concatenates its daily
    /// segments, the PG path `SELECT … ORDER BY seq`, then the SAME Rust filters apply.
    fn read_all(&self, workspace: Option<&str>) -> Result<Vec<EventRecord>, AppError> {
        let ws = workspace.unwrap_or(DEFAULT_WORKSPACE);
        match &self.backend {
            Backend::File(root) => JsonlWriter::for_workspace_in(root, ws)
                .map_err(ev_err)?
                .read_all()
                .map_err(ev_err),
            Backend::Pg(store) => store.read_all(ws).map_err(ev_err),
        }
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
        let records: Vec<EventRecord> = self
            .read_all(workspace)?
            .into_iter()
            .filter(|r| r.kind.starts_with(prefix))
            .collect();
        replay(&records, initial, apply).map_err(ev_err)
    }

    /// All records for `workspace` whose `kind` matches `kind` exactly (after legacy upgrade).
    /// Used by the quota history endpoint to surface `quota.window_reset.v1` events without
    /// replaying full domain state.
    pub fn read_kind(&self, workspace: Option<&str>, kind: &str) -> Result<Vec<EventRecord>, AppError> {
        Ok(self.read_all(workspace)?.into_iter().filter(|r| r.kind == kind).collect())
    }

    /// Append one decided event to the workspace log.
    pub fn append<E>(&self, workspace: Option<&str>, event: E) -> Result<(), AppError>
    where
        E: EventKind + Serialize,
    {
        let ws = workspace.unwrap_or(DEFAULT_WORKSPACE);
        let record = EventRecord::from_envelope(&Envelope::root(event)).map_err(ev_err)?;
        match &self.backend {
            Backend::File(root) => JsonlWriter::for_workspace_in(root, ws)
                .map_err(ev_err)?
                .append(&record)
                .map_err(ev_err),
            Backend::Pg(store) => store.append(ws, &record).map_err(ev_err),
        }
    }

    /// Append a pre-built [`EventRecord`] to the workspace log — the raw-record counterpart of
    /// [`append`](Self::append). Used by REST adapters that construct the record themselves (the
    /// agent REST surface builds one from `AgentEvent` via `Envelope::root` before appending).
    pub fn append_raw(
        &self,
        workspace: Option<&str>,
        record: &EventRecord,
    ) -> Result<(), AppError> {
        let ws = workspace.unwrap_or(DEFAULT_WORKSPACE);
        match &self.backend {
            Backend::File(root) => JsonlWriter::for_workspace_in(root, ws)
                .map_err(ev_err)?
                .append(record)
                .map_err(ev_err),
            Backend::Pg(store) => store.append(ws, record).map_err(ev_err),
        }
    }

    /// Read a workspace's log records for the SSE feed (`hq-mcp-dispatch.10`),
    /// newest-bounded, in chronological order.
    ///
    /// - `channel` filters by event-kind prefix — `Some("merge")` yields only
    ///   `merge.*` records, the per-channel keying; `None` is the whole feed.
    /// - `since_ts` is the `Last-Event-ID` resume marker (a record `ts`, RFC3339):
    ///   only records strictly newer are returned, so a reconnecting client replays
    ///   exactly what it missed. `None` seeds from the tail.
    /// - `limit` caps the batch from the newest end (a reconnect can't replay an
    ///   unbounded backlog).
    ///
    /// On the file backend the log is **path-partitioned per workspace** (mt-data.8); on the PG
    /// backend the `workspace` column partitions it and [`read_all`](Self::read_all) constrains
    /// `WHERE workspace = $1`. Either way this only ever reads the caller's own tenant — the
    /// cross-workspace isolation is structural on both, not a filter that could be forgotten here.
    pub fn read_since(
        &self,
        workspace: Option<&str>,
        channel: Option<&str>,
        since_ts: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, AppError> {
        let mut records = self.read_all(workspace)?;
        if let Some(prefix) = channel.filter(|c| !c.is_empty()) {
            // Match the namespace exactly or as a dotted prefix (`merge` matches
            // `merge.merged.v1` but not `merger.*`).
            records.retain(|r| r.kind == prefix || r.kind.starts_with(&format!("{prefix}.")));
        }
        if let Some(since) = since_ts.filter(|s| !s.is_empty()) {
            records.retain(|r| r.ts.as_str() > since);
        }
        let start = records.len().saturating_sub(limit);
        Ok(records[start..].to_vec())
    }

    /// Every workspace partition currently on disk: the names of the immediate subdirectories of the
    /// log root, each being one tenant's append-only log (`<root>/<ws>/`). A daemon that must sweep
    /// across tenants (e.g. the graph drift-reconcile, hq-vcs-connections.8) enumerates the workspaces
    /// this way — the log is path-partitioned per workspace, with no central registry of partitions.
    ///
    /// On the file backend: non-directory entries and the sibling `accounts/` credential root (not
    /// a workspace) are skipped. On the PG backend: `SELECT DISTINCT workspace FROM public.events`
    /// (no `accounts/` sibling exists in the table, so no filter is needed). An unreadable root /
    /// unreachable DB yields an empty list (nothing to sweep), never an error — a best-effort
    /// daemon must not abort because the volume/DB is momentarily unavailable.
    pub fn workspaces(&self) -> Vec<String> {
        match &self.backend {
            Backend::File(root) => {
                let Ok(entries) = std::fs::read_dir(root) else {
                    return Vec::new();
                };
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .filter_map(|e| e.file_name().into_string().ok())
                    // The orphan-credential GC roots claude account dirs under `accounts/` (a
                    // sibling of the workspace dirs), which is not a tenant — skip it.
                    .filter(|name| name != "accounts")
                    .collect()
            }
            Backend::Pg(store) => store.workspaces(),
        }
    }
}

/// The composition-root [`IssueEventSink`] for the issues tracker (`hq-issues-sse`).
///
/// The issues tracker is Dolt-backed, not event-sourced, so its mutations never
/// reached the per-workspace event log the `GET /stream` SSE feed fans out — a
/// frontend never saw the tracker move without polling. This sink closes that gap:
/// it appends every issue mutation's [`IssueEvent`] to that same log (the exact handle
/// the feed streams from), so a `create`/`update`/`transition`/`close`/`claim` — over
/// REST **or** MCP — surfaces on `GET /stream?channel=issues`, keyed to the mutation's
/// workspace.
///
/// Best-effort by the [`IssueEventSink`] contract: a mutation has already committed by
/// the time this emits, so an append failure is swallowed (the log layer records its
/// own diagnostics) and never surfaces to the caller.
pub struct EventLogIssueSink {
    log: Arc<EventLog>,
}

impl EventLogIssueSink {
    /// Back the sink with the shared per-workspace event log — the same handle the SSE
    /// feed reads, so an emitted event is immediately visible to a live subscriber.
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

impl IssueEventSink for EventLogIssueSink {
    fn emit(&self, workspace: Option<&str>, event: &IssueEvent) {
        // Best-effort: a feed append must never undo the mutation that already
        // committed, so a failure is swallowed rather than propagated.
        let _ = self.log.append(workspace, event.clone());
    }
}

#[cfg(test)]
mod issue_sink_tests {
    use super::*;
    use gt_issues::IssueVerb;
    use tempfile::TempDir;

    /// An emitted issue event lands in the workspace log under a channel-routable
    /// `issues.*` kind — exactly what `read_since(ws, Some("issues"), …)` (the SSE feed)
    /// reads — and stays isolated to the mutation's tenant.
    #[test]
    fn emit_appends_a_channel_routable_issues_event_per_workspace() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let sink = EventLogIssueSink::new(log.clone());

        sink.emit(
            Some("acme"),
            &IssueEvent {
                verb: IssueVerb::Transitioned,
                id: "hq-x.1".into(),
                actor: "mcp-local".into(),
                rig: "hq".into(),
                issue: Some(serde_json::json!({ "id": "hq-x.1", "status": "working" })),
            },
        );

        // The feed reads it back through the `issues` channel filter.
        let on_channel = log
            .read_since(Some("acme"), Some("issues"), None, 256)
            .unwrap();
        assert_eq!(on_channel.len(), 1);
        assert_eq!(on_channel[0].kind, "issues.transitioned.v1");
        assert_eq!(on_channel[0].payload["id"], "hq-x.1");
        assert_eq!(on_channel[0].payload["issue"]["status"], "working");

        // Another tenant's feed never sees it (path-partitioned isolation).
        let other = log
            .read_since(Some("beta"), Some("issues"), None, 256)
            .unwrap();
        assert!(other.is_empty());
    }

    /// `workspaces()` enumerates the on-disk tenant partitions (the immediate subdirs created lazily
    /// on first append), skipping the `accounts/` credential root. This is the seam the graph
    /// drift-reconcile daemon sweeps across (hq-vcs-connections.8).
    #[test]
    fn workspaces_lists_tenant_partitions_and_skips_accounts() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        // No partitions yet.
        assert!(log.workspaces().is_empty());

        // Two tenants get a partition the first time something is appended for them.
        for ws in ["acme", "beta"] {
            log.append(
                Some(ws),
                IssueEvent {
                    verb: IssueVerb::Transitioned,
                    id: "hq-x.1".into(),
                    actor: "t".into(),
                    rig: "hq".into(),
                    issue: None,
                },
            )
            .unwrap();
        }
        // A sibling `accounts/` dir (the credential GC root) is NOT a tenant.
        std::fs::create_dir_all(dir.path().join("accounts")).unwrap();

        let mut found = log.workspaces();
        found.sort();
        assert_eq!(found, vec!["acme".to_string(), "beta".to_string()]);
    }
}

/// PG-backed `EventLog` contract tier (hq-talos-migration.10), gated on `GT_PG_URL` against a
/// DISPOSABLE local Postgres. Proves the four ops behave IDENTICALLY to the file path on the PG
/// backend — the same prefix/channel/since/limit semantics, ordering, and cross-workspace
/// isolation — so flipping the backend env is a transparent swap. Skipped (and the suite stays
/// green) when GT_PG_URL is unset, exactly like the other PG contract tests in this crate.
#[cfg(test)]
mod pg_backend_tests {
    use super::*;
    use gt_issues::{IssueEvent, IssueVerb};

    /// A PG-backed log against the contract DB, with the test workspaces' rows truncated for
    /// deterministic reruns. `None` ⇒ GT_PG_URL unset (skip).
    fn fresh_pg_log(workspaces: &[&str]) -> Option<EventLog> {
        let url = std::env::var("GT_PG_URL").ok()?;
        let store = gt_eventlog::PgEventStore::connect(&url).expect("connect contract PG");
        store.ensure_schema().expect("ensure events schema");
        // Truncate via a throwaway log: append nothing, just clean. We reach the rows through a
        // fresh connection from the same store by appending a sentinel then deleting — simpler: use
        // the store's own pool indirectly is private, so re-clean by replaying + matching is moot.
        // Instead, clean by connecting a one-off psql-style DELETE through a second postgres client.
        let mut client =
            postgres_client(&url);
        for ws in workspaces {
            client
                .execute("DELETE FROM public.events WHERE workspace = $1", &[ws])
                .expect("clean ws rows");
        }
        Some(EventLog::from_pg_store(store))
    }

    /// A bare synchronous `postgres::Client` for test cleanup (the pool inside `PgEventStore` is
    /// private). Same `NoTls` local container the store connects to.
    fn postgres_client(url: &str) -> postgres::Client {
        postgres::Client::connect(url, postgres::NoTls).expect("connect cleanup client")
    }

    fn issue_ev(id: &str, status: &str) -> IssueEvent {
        IssueEvent {
            verb: IssueVerb::Transitioned,
            id: id.into(),
            actor: "t".into(),
            rig: "hq".into(),
            issue: Some(serde_json::json!({ "id": id, "status": status })),
        }
    }

    /// append → replay_domain round-trips through Postgres, folding only the domain's prefix.
    #[test]
    fn append_then_replay_domain_round_trips_over_pg() {
        let Some(log) = fresh_pg_log(&["pg-wrap-rt"]) else {
            eprintln!("GT_PG_URL unset; skipping PG replay_domain round-trip");
            return;
        };
        log.append(Some("pg-wrap-rt"), issue_ev("hq-a.1", "working")).unwrap();
        log.append(Some("pg-wrap-rt"), issue_ev("hq-a.2", "closed")).unwrap();

        // replay_domain folds the `issues.` prefix into a count of records seen.
        let n = log
            .replay_domain::<usize, serde_json::Value, _>(
                Some("pg-wrap-rt"),
                "issues.",
                0usize,
                |acc, _ev| *acc += 1,
            )
            .unwrap();
        assert_eq!(n, 2, "both issues.* events fold");
    }

    /// The prefix/channel filter is byte-for-byte the file path's: `merge` matches `merge.*` but
    /// NOT `merger.*` — proven on the PG backend through `read_since`.
    #[test]
    fn read_since_channel_filter_merge_vs_merger_over_pg() {
        let Some(log) = fresh_pg_log(&["pg-wrap-chan"]) else {
            eprintln!("GT_PG_URL unset; skipping PG channel-filter test");
            return;
        };
        // Three records: a bare `merge`, a dotted `merge.merged.v1`, and a sibling `merger.x.v1`.
        // The IssueEvent kind is fixed, so append raw records via a cleanup client to control kind.
        let url = std::env::var("GT_PG_URL").unwrap();
        let mut client = postgres_client(&url);
        for (id, kind, ts) in [
            ("m0", "merge", "2026-06-01T00:00:00Z"),
            ("m1", "merge.merged.v1", "2026-06-01T00:00:01Z"),
            ("mr", "merger.created.v1", "2026-06-01T00:00:02Z"),
        ] {
            client
                .execute(
                    "INSERT INTO public.events (workspace, event_id, correlation_id, causation_id, kind, ts, payload) \
                     VALUES ($1,$2,$3,NULL,$4,$5,'{}'::jsonb)",
                    &[&"pg-wrap-chan", &id, &id, &kind, &ts],
                )
                .unwrap();
        }
        let on_channel = log.read_since(Some("pg-wrap-chan"), Some("merge"), None, 256).unwrap();
        let kinds: Vec<_> = on_channel.iter().map(|r| r.kind.as_str()).collect();
        assert_eq!(kinds, vec!["merge", "merge.merged.v1"], "merge matches merge.* not merger.*");
    }

    /// `read_since` honours `since_ts` (strictly newer) + `limit` (tail), and `workspaces()` lists
    /// the PG partitions DISTINCT — same as the file path.
    #[test]
    fn read_since_since_and_limit_plus_workspaces_over_pg() {
        let Some(log) = fresh_pg_log(&["pg-wrap-since", "pg-wrap-other"]) else {
            eprintln!("GT_PG_URL unset; skipping PG since/limit/workspaces test");
            return;
        };
        let url = std::env::var("GT_PG_URL").unwrap();
        let mut client = postgres_client(&url);
        for (id, ts) in [
            ("e1", "2026-06-01T00:00:00Z"),
            ("e2", "2026-06-02T00:00:00Z"),
            ("e3", "2026-06-03T00:00:00Z"),
        ] {
            client
                .execute(
                    "INSERT INTO public.events (workspace, event_id, correlation_id, causation_id, kind, ts, payload) \
                     VALUES ($1,$2,$3,NULL,'feed.v1',$4,'{}'::jsonb)",
                    &[&"pg-wrap-since", &id, &id, &ts],
                )
                .unwrap();
        }
        client
            .execute(
                "INSERT INTO public.events (workspace, event_id, correlation_id, causation_id, kind, ts, payload) \
                 VALUES ('pg-wrap-other','o1','o1',NULL,'feed.v1','2026-06-01T00:00:00Z','{}'::jsonb)",
                &[],
            )
            .unwrap();

        // since: strictly newer than e1 → e2, e3.
        let since = log
            .read_since(Some("pg-wrap-since"), None, Some("2026-06-01T00:00:00Z"), 256)
            .unwrap();
        let ids: Vec<_> = since.iter().map(|r| r.event_id.as_str()).collect();
        assert_eq!(ids, vec!["e2", "e3"], "strictly-newer-than-since");

        // limit: tail 1 of the three → e3.
        let tail = log.read_since(Some("pg-wrap-since"), None, None, 1).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].event_id, "e3", "limit caps from the newest end");

        // workspaces: both partitions present, distinct.
        let ws = log.workspaces();
        assert!(ws.contains(&"pg-wrap-since".to_string()));
        assert!(ws.contains(&"pg-wrap-other".to_string()));
        // Cross-workspace isolation: the other tenant's row never appears in this tenant's read.
        assert!(!since.iter().any(|r| r.event_id == "o1"));
    }
}
