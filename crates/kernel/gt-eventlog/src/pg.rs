//! Synchronous Postgres-backed event store (`hq-talos-migration.10`).
//!
//! The file backend ([`crate::JsonlWriter`]) persists the per-workspace log on a shared volume:
//! gt-mcp-server (API + daemons) and gt-orch-server both append to / read from the same
//! `GT_EVENTLOG_ROOT`. On Kubernetes a `ReadWriteOnce` PVC binds to one node, so that coupling
//! forces the two pods onto the same node and the API to a single replica — no horizontal scale.
//! An `ReadWriteMany` network volume for a concurrent append-log is fragile.
//!
//! This adapter backs the same event store with a single `public.events` table keyed by a
//! `workspace` column (the partition the file path expresses as a `<root>/<ws>/` directory), so N
//! API replicas + a separate orchd write/read concurrently over one Postgres — no shared volume,
//! no co-scheduling. The `seq BIGINT GENERATED ALWAYS AS IDENTITY` column is the durable total
//! order the file path got from append order + daily-segment lexicographic concatenation.
//!
//! **Synchronous on purpose.** The [`EventLog`](crate) public API (`append`/`replay_domain`/
//! `read_since`/`workspaces`) is sync and called by ~30 consumers from many contexts; making it
//! async would ripple `.await` across every event-sourced domain. So this is backed by the
//! blocking [`postgres`] client behind an [`r2d2`] pool, NOT async sqlx — the same sync contract
//! the file path satisfies, now with concurrent writers.
//!
//! **Runtime-safe under tokio (`hq-talos-migration.12`).** The blocking [`postgres`] client spins
//! its OWN tokio `Runtime` internally and `block_on`s every call — invoking it on a tokio worker
//! thread panics "Cannot start a runtime from within a runtime". These four operations are reached
//! from `DomainHandler::dispatch` (async, on the server's tokio runtime), so each public method is
//! a thin wrapper that runs its blocking `*_blocking` body on a SCOPED std thread (off the async
//! runtime) and joins for the result — exactly the pattern `gt-channel::pg::PgQueue` (the sibling
//! .11 dispatch queue) uses for the identical hazard. The join blocks the caller briefly, which is
//! the same synchronous cost the file backend's locked file I/O already imposes; the public sync
//! signatures are byte-for-byte unchanged so the ~30 consumers and the [`EventLog`](crate) wrapper
//! see no difference.

use postgres::types::Json;
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use gt_events::AppError;

use crate::record::EventRecord;

/// The public, cross-workspace event table. The `workspace` column partitions the log (mirroring
/// the file path's per-workspace directory), so this is NOT a per-tenant template table — it lives
/// in `public`, like `notifications` / `mcp_audit`.
pub const EVENTS_TABLE: &str = "public.events";

type Pool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

/// Map any pool / DB error to the project's `AppError::Other`, matching the file path's error
/// shape (every file-backend failure is also `AppError::Other`).
fn pg_err<E: std::fmt::Display>(ctx: &str, e: E) -> AppError {
    AppError::Other(format!("{ctx}: {e}"))
}

/// Run a blocking `postgres` closure OFF the caller's thread, on a scoped std thread.
///
/// The blocking [`postgres`] client owns its own tokio `Runtime` and `block_on`s internally;
/// calling it on a tokio worker thread panics "Cannot start a runtime from within a runtime"
/// (`hq-talos-migration.12`). Every `PgEventStore` operation is reached from the async
/// `DomainHandler::dispatch`, so each routes its blocking body through here: [`std::thread::scope`]
/// spawns a fresh std thread (never a tokio worker), runs the closure there, and joins for the
/// result — so the `postgres` client's internal runtime is never nested inside the server's.
/// Mirrors `gt-channel::pg::PgQueue`'s identical offload for the .11 dispatch queue.
///
/// `R: Send` because the closure's return value crosses the thread boundary back to the caller.
/// A panic inside the closure surfaces as `AppError::Other` rather than tearing down the caller.
fn off_runtime<R, F>(ctx: &str, f: F) -> Result<R, AppError>
where
    F: FnOnce() -> Result<R, AppError> + Send,
    R: Send,
{
    std::thread::scope(|s| {
        s.spawn(f)
            .join()
            .map_err(|_| pg_err(ctx, "blocking thread panicked"))?
    })
}

/// A synchronous, pooled Postgres event store. Clone is cheap (the `r2d2` pool is `Arc`-internal),
/// so the [`EventLog`](crate) wrapper holds one and every per-call dispatch checks out a connection.
#[derive(Clone)]
pub struct PgEventStore {
    pool: Pool,
}

impl PgEventStore {
    /// Connect a blocking connection pool to `conn_str` (a `postgres://…` URL). The pool is lazy —
    /// connections open on first checkout — so construction never blocks boot on DB reachability
    /// (the first append/replay surfaces a connect error as `AppError::Other`, like the file path
    /// surfaces an unwritable volume).
    pub fn connect(conn_str: &str) -> Result<Self, AppError> {
        let manager = PostgresConnectionManager::new(
            conn_str
                .parse()
                .map_err(|e| pg_err("parse GT_EVENTLOG_PG/GT_PG_URL", e))?,
            NoTls,
        );
        let pool = r2d2::Pool::builder()
            .build_unchecked(manager); // lazy: do not block boot on a DB round-trip
        Ok(Self { pool })
    }

    /// Wrap an already-built pool (tests construct one against a disposable container).
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Idempotently create the `public.events` table + its indexes. Mirrors the file path's lazy
    /// `mkdir -p <root>/<ws>/`: a fresh DB is provisioned on first use. The boot migration loop
    /// also applies the same DDL (see `events_migration_sql`), so this is a belt-and-suspenders
    /// self-heal for a backend constructed outside the boot path (e.g. orchd / tests).
    pub fn ensure_schema(&self) -> Result<(), AppError> {
        // Called from the async boot path (`build_domain_router`); run the blocking DDL off the
        // tokio runtime on a scoped std thread (see [`off_runtime`] for why the blocking client
        // panics on a tokio worker).
        off_runtime("ensure_schema thread", || self.ensure_schema_blocking())
    }

    /// The actual blocking DDL. MUST run off any tokio runtime thread (see [`Self::ensure_schema`]).
    fn ensure_schema_blocking(&self) -> Result<(), AppError> {
        let mut conn = self.pool.get().map_err(|e| pg_err("events pool checkout", e))?;
        // Serialize the DDL under a transaction-scoped advisory lock: this self-heal runs on EVERY
        // backend construction (N mcp-server replicas + orchd at startup, plus parallel tests), and
        // concurrent `CREATE INDEX IF NOT EXISTS` against the same table races in Postgres ("tuple
        // concurrently updated"). The lock makes the provisioning a single-writer critical section
        // — exactly the concurrent-writers guarantee this backend exists to provide. The constant
        // key is arbitrary-but-fixed so every process contends on the same lock.
        const EVENTS_DDL_LOCK: i64 = 0x6774_6576_0001; // "gtev" + 1
        conn.batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({EVENTS_DDL_LOCK}); {EVENTS_DDL} COMMIT;"
        ))
        .map_err(|e| pg_err("create public.events", e))
    }

    /// Append one record to a workspace's log: an INSERT into `public.events`. The `seq` IDENTITY
    /// column assigns the durable total order — Postgres serializes concurrent inserts, so N
    /// writers never interleave or collide (the file path's per-file exclusive lock, now done by
    /// the DB across processes/nodes).
    pub fn append(&self, workspace: &str, record: &EventRecord) -> Result<(), AppError> {
        // The blocking `postgres` client panics on a tokio worker (see [`off_runtime`]); this is
        // reached from the async `DomainHandler::dispatch`, so do the INSERT on a scoped std thread.
        off_runtime("append thread", || self.append_blocking(workspace, record))
    }

    /// The actual blocking INSERT. MUST run off any tokio runtime thread (see [`Self::append`]).
    fn append_blocking(&self, workspace: &str, record: &EventRecord) -> Result<(), AppError> {
        let mut conn = self.pool.get().map_err(|e| pg_err("events pool checkout", e))?;
        conn.execute(
            "INSERT INTO public.events (workspace, event_id, correlation_id, causation_id, kind, ts, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &workspace,
                &record.event_id,
                &record.correlation_id,
                &record.causation_id,
                &record.kind,
                &record.ts,
                &Json(&record.payload),
            ],
        )
        .map(|_| ())
        .map_err(|e| pg_err("append event", e))
    }

    /// Read every record for a workspace in durable log order (`ORDER BY seq`). This is the read
    /// primitive the [`EventLog`](crate) wrapper's `replay_domain` / `read_since` filter in Rust,
    /// EXACTLY as the file path filters the concatenated segments in Rust — so the PG semantics
    /// are byte-for-byte the file path's (the prefix/channel/since/limit logic lives in one place,
    /// the wrapper, shared by both backends).
    pub fn read_all(&self, workspace: &str) -> Result<Vec<EventRecord>, AppError> {
        // The blocking `postgres` client panics on a tokio worker (see [`off_runtime`]); reached
        // from the async `DomainHandler::dispatch` (replay/SSE), so query on a scoped std thread.
        off_runtime("read_all thread", || self.read_all_blocking(workspace))
    }

    /// The actual blocking SELECT. MUST run off any tokio runtime thread (see [`Self::read_all`]).
    fn read_all_blocking(&self, workspace: &str) -> Result<Vec<EventRecord>, AppError> {
        let mut conn = self.pool.get().map_err(|e| pg_err("events pool checkout", e))?;
        let rows = conn
            .query(
                "SELECT event_id, correlation_id, causation_id, kind, ts, payload \
                 FROM public.events WHERE workspace = $1 ORDER BY seq",
                &[&workspace],
            )
            .map_err(|e| pg_err("read events", e))?;
        rows.iter()
            .map(|row| {
                let Json(payload): Json<serde_json::Value> = row.get("payload");
                Ok(EventRecord {
                    event_id: row.get("event_id"),
                    correlation_id: row.get("correlation_id"),
                    causation_id: row.get("causation_id"),
                    ts: row.get("ts"),
                    kind: row.get("kind"),
                    payload,
                })
            })
            .collect()
    }

    /// Every workspace partition with at least one event: `SELECT DISTINCT workspace`. The file
    /// path enumerates `<root>/<ws>/` subdirectories; here the `workspace` column is the partition.
    /// An unreachable DB yields an empty list (never an error) — the file path's `workspaces()` is
    /// likewise infallible so a best-effort daemon sweep never aborts on a momentary outage.
    pub fn workspaces(&self) -> Vec<String> {
        // The blocking `postgres` client panics on a tokio worker (see [`off_runtime`]); the
        // daemon sweep that calls this runs on the async runtime, so query on a scoped std thread.
        // Infallible: a thread panic / DB error collapses to an empty list, like the file path's
        // unreadable-root case — a best-effort sweep must never abort.
        off_runtime("workspaces thread", || Ok(self.workspaces_blocking())).unwrap_or_default()
    }

    /// The actual blocking `SELECT DISTINCT`. MUST run off any tokio runtime thread (see
    /// [`Self::workspaces`]). Infallible: an unreachable DB yields an empty list.
    fn workspaces_blocking(&self) -> Vec<String> {
        let Ok(mut conn) = self.pool.get() else {
            return Vec::new();
        };
        let Ok(rows) = conn.query("SELECT DISTINCT workspace FROM public.events", &[]) else {
            return Vec::new();
        };
        rows.iter().map(|row| row.get::<_, String>("workspace")).collect()
    }
}

/// The `public.events` DDL — idempotent (`CREATE … IF NOT EXISTS`). Shared by [`PgEventStore::
/// ensure_schema`] and the boot migration loop (`events_migration_sql`).
const EVENTS_DDL: &str = "\
CREATE TABLE IF NOT EXISTS public.events (
    workspace      TEXT   NOT NULL,
    seq            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT   NOT NULL,
    correlation_id TEXT   NOT NULL,
    causation_id   TEXT,
    kind           TEXT   NOT NULL,
    ts             TEXT   NOT NULL,
    payload        JSONB  NOT NULL
);
CREATE INDEX IF NOT EXISTS events_workspace_seq_idx  ON public.events (workspace, seq);
CREATE INDEX IF NOT EXISTS events_workspace_kind_idx ON public.events (workspace, kind);
";

/// The `public.events` migration SQL, for the boot migration loop. A standalone `&'static str` so
/// the `modules` tier can register it as a `Migration` without re-deriving the DDL — the same text
/// [`PgEventStore::ensure_schema`] runs, so the table shape is defined once.
pub const fn events_migration_sql() -> &'static str {
    EVENTS_DDL
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The migration DDL realizes the locked schema: a `public.events` table with the
    /// `seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY` total order, a JSONB payload, and the
    /// two declared indexes — all idempotent. Pure (no DB), so it runs in plain `cargo test`.
    #[test]
    fn migration_sql_defines_the_locked_events_schema() {
        let sql = events_migration_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS public.events"), "public events table");
        assert!(sql.contains("workspace      TEXT   NOT NULL"), "workspace partition column");
        assert!(
            sql.contains("seq            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
            "seq is the durable total order + PK",
        );
        assert!(sql.contains("payload        JSONB  NOT NULL"), "payload is JSONB");
        assert!(sql.contains("kind           TEXT   NOT NULL"), "kind column");
        assert!(sql.contains("ts             TEXT   NOT NULL"), "ts column (RFC3339 string)");
        assert!(
            sql.contains("events_workspace_seq_idx  ON public.events (workspace, seq)"),
            "(workspace, seq) index",
        );
        assert!(
            sql.contains("events_workspace_kind_idx ON public.events (workspace, kind)"),
            "(workspace, kind) index",
        );
        // Idempotent: every object is IF NOT EXISTS.
        assert_eq!(
            sql.matches("IF NOT EXISTS").count(),
            3,
            "table + two indexes are all IF NOT EXISTS",
        );
    }

    // ----- PG contract tier (gated on GT_PG_URL, against a DISPOSABLE local container) ----------
    //
    // These prove the PG store's four operations behave EXACTLY like the file path: append+replay
    // round-trip, durable ordering by `seq`, distinct workspaces, cross-workspace isolation. The
    // prefix/channel/since/limit semantics live in the `EventLog` wrapper (gt-composition), so they
    // are proven there (also GT_PG_URL-gated); here we pin the storage primitives.

    fn rec(id: &str, kind: &str, ts: &str) -> EventRecord {
        EventRecord {
            event_id: id.into(),
            correlation_id: format!("corr-{id}"),
            causation_id: None,
            ts: ts.into(),
            kind: kind.into(),
            payload: serde_json::json!({ "n": id }),
        }
    }

    /// A fresh store against the contract DB, with its `events` rows for the test's workspaces
    /// truncated so reruns are deterministic. `None` when GT_PG_URL is unset (skip the test).
    ///
    /// The cleanup `DELETE` is a blocking `postgres` call; run it on a scoped std thread (via
    /// [`off_runtime`]) so this helper is safe to call from the async regression test
    /// (`append_replay_round_trips_inside_a_tokio_runtime`) — off the tokio runtime, exactly like
    /// the `.11` `fresh_queue` helper.
    fn fresh_store(workspaces: &[&str]) -> Option<PgEventStore> {
        let url = std::env::var("GT_PG_URL").ok()?;
        let store = PgEventStore::connect(&url).expect("connect contract PG");
        store.ensure_schema().expect("ensure events schema");
        off_runtime("fresh_store cleanup", || {
            let mut conn = store.pool.get().expect("checkout");
            for ws in workspaces {
                conn.execute("DELETE FROM public.events WHERE workspace = $1", &[ws])
                    .expect("clean ws rows");
            }
            Ok(())
        })
        .expect("cleanup thread");
        Some(store)
    }

    #[test]
    fn append_replay_round_trip_preserves_seq_order() {
        let Some(store) = fresh_store(&["pg-test-rt"]) else {
            eprintln!("GT_PG_URL unset; skipping PgEventStore round-trip test");
            return;
        };
        // Append out of `ts` order on purpose — the durable order is INSERT (seq), not ts.
        store.append("pg-test-rt", &rec("c", "merge.merged.v1", "2026-06-03T00:00:00Z")).unwrap();
        store.append("pg-test-rt", &rec("a", "merge.ready.v1", "2026-06-01T00:00:00Z")).unwrap();
        store.append("pg-test-rt", &rec("b", "merge.started.v1", "2026-06-02T00:00:00Z")).unwrap();

        let ids: Vec<_> =
            store.read_all("pg-test-rt").unwrap().into_iter().map(|r| r.event_id).collect();
        assert_eq!(ids, vec!["c", "a", "b"], "read_all returns INSERT (seq) order, not ts order");
        // Payload + correlation round-trip intact through JSONB.
        let all = store.read_all("pg-test-rt").unwrap();
        assert_eq!(all[0].payload["n"], "c");
        assert_eq!(all[1].correlation_id, "corr-a");
    }

    #[test]
    fn workspaces_are_isolated_and_listed_distinct() {
        let Some(store) = fresh_store(&["pg-test-iso-a", "pg-test-iso-b"]) else {
            eprintln!("GT_PG_URL unset; skipping PgEventStore isolation test");
            return;
        };
        store.append("pg-test-iso-a", &rec("a1", "x.v1", "2026-06-01T00:00:00Z")).unwrap();
        store.append("pg-test-iso-a", &rec("a2", "x.v1", "2026-06-01T00:00:01Z")).unwrap();
        store.append("pg-test-iso-b", &rec("b1", "x.v1", "2026-06-01T00:00:00Z")).unwrap();

        assert_eq!(store.read_all("pg-test-iso-a").unwrap().len(), 2);
        assert_eq!(store.read_all("pg-test-iso-b").unwrap().len(), 1);

        let ws = store.workspaces();
        assert!(ws.contains(&"pg-test-iso-a".to_string()));
        assert!(ws.contains(&"pg-test-iso-b".to_string()));
        // DISTINCT: ws-a has two events but appears once.
        assert_eq!(ws.iter().filter(|w| *w == "pg-test-iso-a").count(), 1);
    }

    /// REGRESSION (`hq-talos-migration.12`): the same append/read_all/workspaces round-trip, but
    /// driven FROM WITHIN a tokio runtime — the real context, since `DomainHandler::dispatch` is
    /// async and runs every `EventLog` operation on a tokio worker thread.
    ///
    /// Before the fix, `append`/`read_all`/`workspaces` called the blocking `postgres` client
    /// directly on the calling thread; the client spins its own tokio `Runtime` and `block_on`s,
    /// so on a tokio worker it panicked "Cannot start a runtime from within a runtime" — making
    /// the whole `GT_EVENTLOG_PG=1` path unusable under the async server. The `.10` tests are
    /// plain `#[test]` (no tokio runtime), so they never exercised this and never caught it.
    ///
    /// Now each method offloads its blocking body to a scoped std thread ([`off_runtime`]), so the
    /// `postgres` client's runtime is never nested inside tokio's: this test PASSES. (Run on a
    /// multi-thread runtime — the panic reproduces on any tokio worker; `flavor` only documents
    /// the realistic server topology.)
    #[tokio::test(flavor = "multi_thread")]
    async fn append_replay_round_trips_inside_a_tokio_runtime() {
        let Some(store) = fresh_store(&["pg-test-tokio"]) else {
            eprintln!("GT_PG_URL unset; skipping PgEventStore tokio-runtime regression test");
            return;
        };
        // These calls land on a tokio worker thread (we are inside #[tokio::test]); pre-fix every
        // one panicked at the blocking `postgres` call.
        store.append("pg-test-tokio", &rec("c", "merge.merged.v1", "2026-06-03T00:00:00Z")).unwrap();
        store.append("pg-test-tokio", &rec("a", "merge.ready.v1", "2026-06-01T00:00:00Z")).unwrap();
        store.append("pg-test-tokio", &rec("b", "merge.started.v1", "2026-06-02T00:00:00Z")).unwrap();

        let ids: Vec<_> =
            store.read_all("pg-test-tokio").unwrap().into_iter().map(|r| r.event_id).collect();
        assert_eq!(ids, vec!["c", "a", "b"], "append+read_all round-trips on a tokio worker");
        assert!(
            store.workspaces().contains(&"pg-test-tokio".to_string()),
            "workspaces() also runs off-runtime without panicking",
        );
    }
}
