//! Synchronous Postgres-backed dispatch queue (`hq-talos-migration.11`).
//!
//! The file backend ([`crate::Channel`]) persists a channel as a directory of `*.event` files on
//! a shared volume: gt-mcp-server (the convoy→scheduler producer) emits a file, gt-orch-server
//! (the scheduler consumer) `notify`-watches the same `GT_CHANNEL_ROOT` directory. On Kubernetes a
//! `ReadWriteOnce` PVC binds to one node, so that coupling forces the two pods onto the same node —
//! the SAME shared-volume coupling [`crate::pg`](crate)'s sibling `gt-eventlog::PgEventStore`
//! removed for the event log (hq-talos-migration.10). This adapter removes it for the dispatch
//! channel: the queue lives in a single `public.dispatch_jobs` table, so N mcp-server replicas
//! produce and a separate orchd consumes over one Postgres with no shared volume, no co-scheduling.
//!
//! **Concurrency-safe claim (the horizontal-scale point).** A consumer claims a job with
//! `UPDATE … WHERE id = (SELECT id … ORDER BY priority, id FOR UPDATE SKIP LOCKED LIMIT 1)
//! RETURNING …`: `FOR UPDATE SKIP LOCKED` lets two concurrent orchd consumers (or a transient
//! double-schedule) each grab a DISTINCT row without blocking or double-delivering. The claim
//! deletes the row (at-most-once on a clean claim); the file path's at-least-once re-delivery is
//! traded for a simpler at-most-once because a dispatch is idempotent downstream (`create_bead`
//! upsert + CAS-claim enqueue — a re-dispatch of an already-`Dispatched` bead is a no-op).
//!
//! **Wake without polling.** The producer fires `pg_notify('dispatch_jobs', '')` after the INSERT;
//! the consumer `LISTEN dispatch_jobs` and drains on each notification. A bounded poll fallback
//! (default 5s) covers a missed notification (NOTIFY is best-effort across a dropped connection)
//! and the rows present before the LISTEN started — the same "drain existing then stream new"
//! shape [`Channel::subscribe`] has.
//!
//! **Synchronous on purpose.** Mirrors `gt-eventlog::PgEventStore`: the blocking [`postgres`]
//! client behind an [`r2d2`] pool, NOT async sqlx, so the kernel crate's default build pulls no
//! tokio/sqlx. The consumer loop runs on a dedicated std thread and bridges into the existing
//! `tokio::sync::mpsc` the file path already uses, so the orchd consumer is backend-agnostic.

use std::time::Duration;

use postgres::types::Json;
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use tokio::sync::mpsc;

use crate::mailbox::ChannelMessage;
use crate::ChannelError;

/// The public, cross-workspace dispatch queue table. The `channel` column keys the queue by name
/// (the file path's `<root>/<name>/` directory), so `dispatch` and `merge-ready` can share the
/// table. It lives in `public` (like `events` / `notifications`), NOT a per-tenant template table.
pub const DISPATCH_TABLE: &str = "public.dispatch_jobs";

/// The `pg_notify` channel the producer fires on INSERT and the consumer `LISTEN`s. A fixed name
/// (Postgres NOTIFY channels are not the queue's `channel` column — one wake signal drains all).
const NOTIFY_CHANNEL: &str = "gt_dispatch_jobs";

/// Poll fallback interval: how long the consumer waits for a NOTIFY before draining anyway. NOTIFY
/// is best-effort (a dropped connection loses pending notifications), and rows can predate the
/// LISTEN, so a bounded poll guarantees liveness without busy-spinning.
const POLL_FALLBACK: Duration = Duration::from_secs(5);

type Pool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

/// Map any pool / DB error to [`ChannelError::Pg`], matching the file path's error surface (every
/// file-backend failure is a `ChannelError`).
fn pg_err<E: std::fmt::Display>(ctx: &str, e: E) -> ChannelError {
    ChannelError::Pg(format!("{ctx}: {e}"))
}

/// A synchronous, pooled Postgres dispatch queue. Clone is cheap (the `r2d2` pool is `Arc`-internal),
/// so the producer holds one and every `emit` checks out a connection. Drop-in for [`Channel`](crate)
/// on the convoy→scheduler bridge, selected by the same `GT_EVENTLOG_PG` opt-in the event log uses.
#[derive(Clone)]
pub struct PgQueue {
    pool: Pool,
    /// The queue name this handle produces to / consumes from (the file path's channel directory).
    channel: String,
}

impl PgQueue {
    /// Connect a blocking connection pool to `conn_str` for queue `channel`. Lazy — connections
    /// open on first checkout — so construction never blocks boot on DB reachability (the first
    /// emit/consume surfaces a connect error, like the file path surfaces an unwritable volume).
    pub fn connect(conn_str: &str, channel: &str) -> Result<Self, ChannelError> {
        let manager = PostgresConnectionManager::new(
            conn_str
                .parse()
                .map_err(|e| pg_err("parse GT_PG_URL", e))?,
            NoTls,
        );
        let pool = r2d2::Pool::builder().build_unchecked(manager); // lazy: no boot DB round-trip
        Ok(Self {
            pool,
            channel: channel.to_string(),
        })
    }

    /// The queue name this handle is bound to.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Idempotently create the `public.dispatch_jobs` table + its claim index. Mirrors the file
    /// path's lazy `mkdir -p <root>/<name>/` and `PgEventStore::ensure_schema`: a fresh DB is
    /// provisioned on first use. The boot migration loop also applies the same DDL, so this is a
    /// belt-and-suspenders self-heal for a handle built outside the boot path (orchd / tests).
    pub fn ensure_schema(&self) -> Result<(), ChannelError> {
        // Called from the async boot path (`build_domain_router`), so run the blocking DDL off the
        // tokio runtime on a scoped std thread (see [`emit`] for why the blocking client panics on
        // a tokio worker).
        let queue = self;
        std::thread::scope(|s| {
            s.spawn(|| queue.ensure_schema_blocking())
                .join()
                .map_err(|_| ChannelError::Pg("ensure_schema thread panicked".to_string()))?
        })
    }

    /// The actual blocking DDL. MUST run off any tokio runtime thread (see [`ensure_schema`]).
    fn ensure_schema_blocking(&self) -> Result<(), ChannelError> {
        let mut conn = self.pool.get().map_err(|e| pg_err("dispatch pool checkout", e))?;
        // Serialize the DDL under a transaction-scoped advisory lock: this self-heal runs on EVERY
        // handle construction (N mcp-server replicas + orchd at startup, plus parallel tests), and
        // concurrent `CREATE INDEX IF NOT EXISTS` against the same table races in Postgres. The
        // lock makes provisioning a single-writer critical section. Distinct key from the events
        // store's so the two self-heals never contend on the same lock.
        const DISPATCH_DDL_LOCK: i64 = 0x6774_6477_0001; // "gtdw" + 1
        conn.batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({DISPATCH_DDL_LOCK}); {DISPATCH_DDL} COMMIT;"
        ))
        .map_err(|e| pg_err("create public.dispatch_jobs", e))
    }

    /// Enqueue one dispatch request: an INSERT into `public.dispatch_jobs` keyed by this handle's
    /// `channel`, then a `pg_notify` to wake any listening consumer. `payload` is the raw bytes the
    /// file path stored in the `*.event` file (a `{bead,priority}` JSON) — stored as JSONB so the
    /// payload contract is unchanged. Returns the row id (the file path's message id).
    pub fn emit(&self, payload: &[u8]) -> Result<i64, ChannelError> {
        // The blocking `postgres` client spins its own tokio `Runtime` and `block_on`s — calling
        // it on a tokio worker panics "Cannot start a runtime from within a runtime". The producer
        // (`ConvoyHandler::bridge_to_scheduler`) runs on the async MCP dispatch worker, so do the
        // DB round-trip on a SCOPED std thread (off the async runtime) and join for the result. The
        // join blocks the caller briefly — acceptable for a fire-and-forget best-effort dispatch.
        let queue = self;
        std::thread::scope(|s| {
            s.spawn(|| queue.emit_blocking(payload))
                .join()
                .map_err(|_| ChannelError::Pg("emit thread panicked".to_string()))?
        })
    }

    /// The actual blocking INSERT + NOTIFY. MUST run off any tokio runtime thread (see [`emit`]).
    fn emit_blocking(&self, payload: &[u8]) -> Result<i64, ChannelError> {
        // The payload is the producer's JSON; round-trip it through serde_json so a malformed
        // payload fails here (the producer's contract) rather than poisoning the queue.
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|e| pg_err("dispatch payload not json", e))?;
        let mut conn = self.pool.get().map_err(|e| pg_err("dispatch pool checkout", e))?;
        let row = conn
            .query_one(
                "INSERT INTO public.dispatch_jobs (channel, payload) VALUES ($1, $2) RETURNING id",
                &[&self.channel, &Json(&value)],
            )
            .map_err(|e| pg_err("enqueue dispatch", e))?;
        let id: i64 = row.get("id");
        // Wake a listening consumer. Best-effort: a NOTIFY failure is non-fatal because the poll
        // fallback drains the row anyway (liveness without the notification).
        let _ = conn.execute(&format!("NOTIFY {NOTIFY_CHANNEL}"), &[]);
        Ok(id)
    }

    /// Claim and remove the next job for this channel, concurrency-safe across consumers:
    /// `FOR UPDATE SKIP LOCKED` makes two consumers grab DISTINCT rows (or one to skip a row the
    /// other holds) without blocking. Returns the claimed payload bytes, or `None` when the queue
    /// is empty. Ordered `priority ASC, id ASC` (0 = highest priority, then FIFO) — the file path
    /// is unordered, so this is a strict improvement the queue table affords for free.
    fn claim_next(&self) -> Result<Option<(i64, Vec<u8>)>, ChannelError> {
        let mut conn = self.pool.get().map_err(|e| pg_err("dispatch pool checkout", e))?;
        let rows = conn
            .query(
                "DELETE FROM public.dispatch_jobs \
                 WHERE id = ( \
                     SELECT id FROM public.dispatch_jobs \
                     WHERE channel = $1 \
                     ORDER BY (payload->>'priority')::int NULLS LAST, id \
                     FOR UPDATE SKIP LOCKED \
                     LIMIT 1 \
                 ) \
                 RETURNING id, payload",
                &[&self.channel],
            )
            .map_err(|e| pg_err("claim dispatch", e))?;
        match rows.first() {
            None => Ok(None),
            Some(row) => {
                let id: i64 = row.get("id");
                let Json(payload): Json<serde_json::Value> = row.get("payload");
                let bytes = serde_json::to_vec(&payload)
                    .map_err(|e| pg_err("claimed payload serialize", e))?;
                Ok(Some((id, bytes)))
            }
        }
    }

    /// Drain + stream claimed jobs into an mpsc receiver, mirroring [`Channel::subscribe`] so the
    /// orchd consumer is backend-agnostic. The claim already removed the row (at-most-once), so the
    /// emitted [`ChannelMessage`] carries an empty `path`; the consumer's `ack` is a no-op on the PG
    /// backend (the claim WAS the ack — see [`PgQueue::ack`]).
    ///
    /// A dedicated std thread runs the blocking LISTEN/poll loop (the `postgres` client is blocking,
    /// not a tokio worker) and `blocking_send`s each claimed job. The thread:
    /// 1. `LISTEN`s the wake channel,
    /// 2. drains every currently-claimable job (the pre-existing rows the file path drains first),
    /// 3. blocks on a notification with a [`POLL_FALLBACK`] timeout, then loops back to drain —
    ///    so a missed/lost NOTIFY still gets serviced within the poll interval (liveness).
    ///
    /// Dropping the receiver tears the thread down on its next send.
    pub fn subscribe(&self, buffer: usize) -> Result<mpsc::Receiver<ChannelMessage>, ChannelError> {
        let (tx, rx) = mpsc::channel::<ChannelMessage>(buffer);
        let queue = self.clone();

        // ALL blocking `postgres` work runs on this dedicated std thread, NEVER on the caller's
        // thread. `dispatch::run` is an async fn driven by a tokio worker, and the blocking
        // `postgres` client spins its own runtime internally (`block_on`) — calling it on a tokio
        // worker panics with "Cannot start a runtime from within a runtime". So the LISTEN
        // connection checkout + every claim happen here, off the async runtime, and only the
        // already-decoded `ChannelMessage` crosses back via `blocking_send`.
        std::thread::spawn(move || {
            // A dedicated LISTEN connection, separate from the pooled claim connections (a LISTEN
            // connection blocks on notifications; it must not be a checked-out pool member). Held
            // for the thread's lifetime; dropping it stops listening. A checkout/LISTEN failure
            // closes the receiver (the consumer's `subscribe` future then sees the channel drop and
            // the supervisor restarts the loop) rather than panicking the thread.
            let mut listen_conn = match queue.pool.get() {
                Ok(c) => c,
                Err(_) => return,
            };
            if listen_conn
                .batch_execute(&format!("LISTEN {NOTIFY_CHANNEL}"))
                .is_err()
            {
                return;
            }
            loop {
                // Drain everything currently claimable before blocking on the next notification.
                loop {
                    match queue.claim_next() {
                        Ok(Some((id, payload))) => {
                            let msg = ChannelMessage {
                                id: id.to_string(),
                                path: std::path::PathBuf::new(), // PG path: claim was the ack
                                payload,
                            };
                            if tx.blocking_send(msg).is_err() {
                                return; // receiver dropped
                            }
                        }
                        Ok(None) => break,       // queue drained
                        Err(_) => break,         // transient DB error: retry after the next wake
                    }
                }
                // Block for a notification, bounded by the poll fallback so a lost NOTIFY (or a row
                // that predates the LISTEN) is still serviced within the interval. `timeout_iter`
                // yields a fallible iterator; `next()` returns `Ok(None)` on the timeout (→ drain
                // anyway) and `Err` if the LISTEN connection broke.
                use postgres::fallible_iterator::FallibleIterator;
                let mut notifications = listen_conn.notifications();
                // Bind the result so the `TimeoutIter` temporary (which borrows `notifications`) is
                // dropped before the match body, not held to block end.
                let woke = notifications.timeout_iter(POLL_FALLBACK).next();
                match woke {
                    Ok(Some(_notification)) => { /* woke on NOTIFY → drain */ }
                    Ok(None) => { /* poll-fallback timeout → drain anyway */ }
                    Err(_) => {
                        // The LISTEN connection broke. The pooled claim connections are
                        // independent, so keep looping: the drain above re-establishes liveness
                        // via the poll fallback even without a working LISTEN. Avoid a hot spin.
                        std::thread::sleep(POLL_FALLBACK);
                    }
                }
            }
        });

        Ok(rx)
    }

    /// Acknowledge a claimed job. No-op on the PG backend: [`claim_next`](Self::claim_next) deletes
    /// the row in the same statement it reads it (at-most-once), so there is nothing to clean up —
    /// the method exists only to mirror [`Channel::ack`] so the consumer code is backend-agnostic.
    pub fn ack(&self, _msg: &ChannelMessage) -> Result<(), ChannelError> {
        Ok(())
    }
}

/// The `public.dispatch_jobs` DDL — idempotent (`CREATE … IF NOT EXISTS`). Shared by
/// [`PgQueue::ensure_schema`] and the boot migration loop (`dispatch_migration_sql`).
const DISPATCH_DDL: &str = "\
CREATE TABLE IF NOT EXISTS public.dispatch_jobs (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel    TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dispatch_jobs_channel_id_idx ON public.dispatch_jobs (channel, id);
";

/// The `public.dispatch_jobs` migration SQL, for the boot migration loop. A standalone
/// `&'static str` so the `modules` tier registers it as a `Migration` without re-deriving the DDL —
/// the same text [`PgQueue::ensure_schema`] runs, so the table shape is defined once.
pub const fn dispatch_migration_sql() -> &'static str {
    DISPATCH_DDL
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The migration DDL realizes the queue schema: a `public.dispatch_jobs` table with the
    /// `id … IDENTITY PRIMARY KEY` order, a JSONB payload, the `channel` key, and the claim index —
    /// all idempotent. Pure (no DB), so it runs in plain `cargo test`.
    #[test]
    fn migration_sql_defines_the_dispatch_queue_schema() {
        let sql = dispatch_migration_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS public.dispatch_jobs"), "queue table");
        assert!(sql.contains("channel    TEXT        NOT NULL"), "channel key column");
        assert!(sql.contains("payload    JSONB       NOT NULL"), "payload is JSONB");
        assert!(
            sql.contains("id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
            "id is the FIFO order + PK",
        );
        assert!(
            sql.contains("dispatch_jobs_channel_id_idx ON public.dispatch_jobs (channel, id)"),
            "(channel, id) claim index",
        );
        // Idempotent: table + index are IF NOT EXISTS.
        assert_eq!(sql.matches("IF NOT EXISTS").count(), 2, "table + index are IF NOT EXISTS");
    }

    // ----- PG contract tier (gated on GT_PG_URL, against a DISPOSABLE local container) ----------
    //
    // These prove the queue's producer→consumer round-trip and the concurrency-safe SKIP LOCKED
    // claim. Skipped (suite stays green) when GT_PG_URL is unset, like the other PG contract tests.

    /// A fresh queue against the contract DB, with its rows for the test's channel truncated so
    /// reruns are deterministic. `None` when GT_PG_URL is unset (skip the test).
    fn fresh_queue(channel: &str) -> Option<PgQueue> {
        let url = std::env::var("GT_PG_URL").ok()?;
        let q = PgQueue::connect(&url, channel).expect("connect contract PG");
        q.ensure_schema().expect("ensure dispatch schema");
        // The DELETE is a blocking `postgres` call; run it on a scoped std thread so this helper is
        // safe to call from the async `subscribe` test (off the tokio runtime — see `emit`).
        let q2 = q.clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut conn = q2.pool.get().expect("checkout");
                conn.execute("DELETE FROM public.dispatch_jobs WHERE channel = $1", &[&channel])
                    .expect("clean channel rows");
            })
            .join()
            .expect("cleanup thread");
        });
        Some(q)
    }

    fn payload(bead: &str, priority: u8) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "bead": bead, "priority": priority })).unwrap()
    }

    /// emit → claim_next round-trips through Postgres, and claim removes the row (at-most-once).
    #[test]
    fn emit_then_claim_round_trips_and_removes_the_row() {
        let Some(q) = fresh_queue("test-dispatch-rt") else {
            eprintln!("GT_PG_URL unset; skipping PgQueue round-trip test");
            return;
        };
        q.emit(&payload("hq-a.1", 1)).unwrap();
        let (_, bytes) = q.claim_next().unwrap().expect("one job claimable");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["bead"], "hq-a.1");
        // The row is gone after the claim.
        assert!(q.claim_next().unwrap().is_none(), "claim deletes the row");
    }

    /// Priority ordering: a P0 job claims before a P1 even when enqueued after it.
    #[test]
    fn claim_orders_by_priority_then_fifo() {
        let Some(q) = fresh_queue("test-dispatch-prio") else {
            eprintln!("GT_PG_URL unset; skipping PgQueue priority test");
            return;
        };
        q.emit(&payload("low", 1)).unwrap();
        q.emit(&payload("high", 0)).unwrap();
        q.emit(&payload("low2", 1)).unwrap();
        let first: serde_json::Value =
            serde_json::from_slice(&q.claim_next().unwrap().unwrap().1).unwrap();
        assert_eq!(first["bead"], "high", "P0 claimed before P1");
        let second: serde_json::Value =
            serde_json::from_slice(&q.claim_next().unwrap().unwrap().1).unwrap();
        assert_eq!(second["bead"], "low", "then FIFO within priority");
    }

    /// Concurrency-safe claim (the horizontal-scale point): two consumers claiming in parallel each
    /// get DISTINCT jobs and no job is delivered twice (SKIP LOCKED). The whole batch is accounted
    /// for exactly once across both consumers.
    #[test]
    fn concurrent_consumers_each_claim_distinct_jobs_skip_locked() {
        let Some(q) = fresh_queue("test-dispatch-conc") else {
            eprintln!("GT_PG_URL unset; skipping PgQueue concurrency test");
            return;
        };
        const N: usize = 50;
        for i in 0..N {
            q.emit(&payload(&format!("b{i}"), 1)).unwrap();
        }
        let q2 = q.clone();
        let h1 = std::thread::spawn(move || drain_all(&q));
        let h2 = std::thread::spawn(move || drain_all(&q2));
        let mut a = h1.join().unwrap();
        let mut b = h2.join().unwrap();
        let total = a.len() + b.len();
        assert_eq!(total, N, "every job claimed exactly once across both consumers");
        // No overlap: the union has no duplicates.
        a.append(&mut b);
        a.sort();
        let before = a.len();
        a.dedup();
        assert_eq!(a.len(), before, "no job delivered to both consumers (SKIP LOCKED)");
    }

    /// Claim every job a queue currently holds, returning the bead ids (for the concurrency test).
    fn drain_all(q: &PgQueue) -> Vec<String> {
        let mut beads = Vec::new();
        while let Some((_, bytes)) = q.claim_next().unwrap() {
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            beads.push(v["bead"].as_str().unwrap().to_string());
        }
        beads
    }

    /// End-to-end producer→consumer over `subscribe` (the shape `gt_scheduling::dispatch::run`
    /// uses): a row enqueued BEFORE the subscribe is drained, and a row enqueued AFTER wakes the
    /// LISTEN — both land on the mpsc receiver with their `{bead,priority}` payload intact, proving
    /// the same `subscribe`→`recv`→`ack` contract the file path satisfies works on the PG backend.
    #[tokio::test(flavor = "multi_thread")]
    async fn subscribe_drains_existing_then_streams_new_over_pg() {
        let Some(q) = fresh_queue("test-dispatch-sub") else {
            eprintln!("GT_PG_URL unset; skipping PgQueue subscribe round-trip test");
            return;
        };
        // A row present BEFORE the subscribe (the file path's "drain existing" case).
        q.emit(&payload("pre", 1)).unwrap();

        let mut rx = q.subscribe(16).unwrap();

        let first = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("pre-existing row drained within timeout")
            .expect("receiver open");
        let v: serde_json::Value = serde_json::from_slice(&first.payload).unwrap();
        assert_eq!(v["bead"], "pre", "pre-existing row drained on subscribe");
        q.ack(&first).unwrap(); // no-op on PG, mirrors the consumer contract

        // A row enqueued AFTER the subscribe wakes the LISTEN (the file path's "stream new" case).
        q.emit(&payload("post", 0)).unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("post-subscribe row delivered within timeout")
            .expect("receiver open");
        let v: serde_json::Value = serde_json::from_slice(&second.payload).unwrap();
        assert_eq!(v["bead"], "post", "post-subscribe row streamed via NOTIFY");
    }
}
