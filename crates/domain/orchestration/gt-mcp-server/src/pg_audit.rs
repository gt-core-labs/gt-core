//! Durable Postgres audit sink for the gt-core MCP server (hq-core-host.6).
//!
//! The kernel `gt-audit` crate stays sync + tokio-free (no `tokio::spawn` in
//! kernel — docs/03 anti-pattern), shipping only the [`AuditSink`] port + the
//! in-memory adapter. The async Postgres write therefore lives HERE, in the
//! orchestration-tier server bin where tokio is allowed.
//!
//! Design: [`AuditSink::record`] is sync and must never block the dispatch path,
//! so it just pushes onto an unbounded channel (non-blocking `send`). A detached
//! background task owns the receiver + the `PgPool` and drains records into the
//! `mcp_audit` table. A write failure is logged and skipped — auditing must never
//! take the tool surface down.

use gt_audit::{AppError, AuditRecord, AuditSink, Outcome};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

/// A durable audit sink that forwards records to a background Postgres writer.
/// Cloneable via the `Arc` the server holds; the sender is cheap to share.
///
/// The `pool` is retained alongside the writer channel so [`AuditSink::read_all`]
/// can serve the per-tenant audit dump (`audit.tail`, hq-mt-ops.3): writes flow
/// through the non-blocking channel, reads query Postgres directly.
pub struct PgAuditSink {
    tx: UnboundedSender<AuditRecord>,
    pool: PgPool,
}

impl PgAuditSink {
    /// Connect to Postgres, ensure the `mcp_audit` table, and spawn the drain
    /// task. Returns a sink whose `record` is a non-blocking channel push.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new().max_connections(2).connect(url).await?;
        ensure_schema(&pool).await?;
        let (tx, mut rx) = unbounded_channel::<AuditRecord>();
        // The drain owns its own pool handle; the struct keeps a clone for reads.
        let writer_pool = pool.clone();
        // Detached drain: lives for the process. Each record is one INSERT; a
        // failure is logged, not propagated — the server keeps serving.
        tokio::spawn(async move {
            while let Some(rec) = rx.recv().await {
                if let Err(e) = insert(&writer_pool, &rec).await {
                    eprintln!("[gt-mcp-server] PG audit insert failed ({}): {e}", rec.tool);
                }
            }
        });
        Ok(Self { tx, pool })
    }
}

impl AuditSink for PgAuditSink {
    fn record(&self, record: AuditRecord) -> Result<(), AppError> {
        self.tx
            .send(record)
            .map_err(|_| AppError::Sink("audit drain task is gone".into()))
    }

    /// Read the whole append-ordered trail from Postgres. `read_all` is a sync
    /// trait method (the kernel `gt-audit` port stays tokio-free), but the query
    /// is async — so we bridge through `block_in_place` + `block_on` on the
    /// server's multi-threaded runtime. The caller (`audit.tail`) applies the
    /// per-tenant + field filters and the `limit` window on the result; a future
    /// optimisation can push the `WHERE workspace_id` down to SQL if the trail
    /// grows large.
    fn read_all(&self) -> Result<Vec<AuditRecord>, AppError> {
        let pool = self.pool.clone();
        let rows = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { select_all(&pool).await })
        })
        .map_err(|e| AppError::Sink(format!("audit read_all query failed: {e}")))?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }
}

/// One `mcp_audit` row in the column order [`select_all`] projects: workspace_id,
/// actor, tool, args (JSON text), outcome (`invoked`/`unauthorized`), ts (RFC3339),
/// scopes (A5, gtcore-f3a016).
type AuditRow = (String, String, String, String, String, String, Vec<String>);

/// Read every row in append order (`id`). `ts` is rendered UTC RFC3339 in SQL so
/// the record's `ts` round-trips the same string shape the in-memory sink carries.
async fn select_all(pool: &PgPool) -> Result<Vec<AuditRow>, sqlx::Error> {
    sqlx::query_as::<_, AuditRow>(
        "SELECT workspace_id, actor, tool, args, outcome, \
         to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), \
         COALESCE(scopes, '{}') \
         FROM mcp_audit ORDER BY id",
    )
    .fetch_all(pool)
    .await
}

/// Rebuild an [`AuditRecord`] from a projected row. A malformed `args` text (should
/// not happen — we wrote it) degrades to JSON null rather than failing the dump; an
/// unknown outcome string maps to `Invoked` (the only non-`unauthorized` value).
fn row_to_record((workspace_id, actor, tool, args, outcome, ts, scopes): AuditRow) -> AuditRecord {
    let args = serde_json::from_str(&args).unwrap_or(serde_json::Value::Null);
    let outcome = match outcome.as_str() {
        "unauthorized" => Outcome::Unauthorized,
        _ => Outcome::Invoked,
    };
    AuditRecord { workspace_id, actor, tool, args, outcome, ts, scopes }
}

/// Idempotent table create — append-only audit of every tool dispatch.
///
/// `mcp_audit` lives in the `public` schema, not a per-tenant `ws_<slug>` schema:
/// a SOC2 per-tenant audit dump must read every tenant's trail from one place, so
/// the table stays cross-tenant and partitions by the `workspace_id` column instead
/// (docs/04 §15 — public-schema tables keep a `workspace_id` discriminator).
async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mcp_audit (
            id           bigserial PRIMARY KEY,
            workspace_id text NOT NULL DEFAULT 'default',
            actor        text NOT NULL,
            tool         text NOT NULL,
            args         text NOT NULL,
            outcome      text NOT NULL,
            ts           timestamptz NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    // Idempotent upgrade for a table created before the tenant column existed
    // (hq-mt-auth.7). Backfills NULLs to 'default' via the column default.
    sqlx::query(
        "ALTER TABLE mcp_audit
            ADD COLUMN IF NOT EXISTS workspace_id text NOT NULL DEFAULT 'default'",
    )
    .execute(pool)
    .await?;
    // Per-tenant audit queries filter on this column; index it so a SOC2 dump for
    // one workspace does not scan the whole cross-tenant trail.
    sqlx::query("CREATE INDEX IF NOT EXISTS mcp_audit_workspace_idx ON mcp_audit (workspace_id)")
        .execute(pool)
        .await?;
    // A5 (gtcore-f3a016): per-record scope snapshot so the audit trail captures what the
    // actor held at dispatch time. Stored as text[] for efficient contains queries.
    sqlx::query(
        "ALTER TABLE mcp_audit
            ADD COLUMN IF NOT EXISTS scopes text[] NOT NULL DEFAULT '{}'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Append one record. `args` is stored as text (the JSON the caller received) so
/// the sink needs no sqlx `json` feature; `ts` defaults to the DB clock.
async fn insert(pool: &PgPool, rec: &AuditRecord) -> Result<(), sqlx::Error> {
    let outcome = match rec.outcome {
        Outcome::Invoked => "invoked",
        Outcome::Unauthorized => "unauthorized",
    };
    sqlx::query(
        "INSERT INTO mcp_audit (workspace_id, actor, tool, args, outcome, scopes) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&rec.workspace_id)
    .bind(&rec.actor)
    .bind(&rec.tool)
    .bind(rec.args.to_string())
    .bind(outcome)
    .bind(&rec.scopes)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// End-to-end against a throwaway Postgres (gated on `GT_PG_URL`): the schema
    /// gains the `workspace_id` column and a per-tenant query returns only that
    /// tenant's rows. Run with a sandbox PG (see reference_sandbox_dolt_pg_gated_tests):
    /// `GT_PG_URL=postgres://postgres@127.0.0.1:PORT/postgres cargo test -p gt-mcp-server`.
    #[tokio::test]
    async fn workspace_id_persists_and_filters_per_tenant() {
        let Ok(url) = std::env::var("GT_PG_URL") else {
            eprintln!("skipping: GT_PG_URL unset");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        // Isolate from any prior run sharing the table.
        sqlx::query("DROP TABLE IF EXISTS mcp_audit")
            .execute(&pool)
            .await
            .unwrap();
        ensure_schema(&pool).await.expect("schema");

        insert(
            &pool,
            &AuditRecord::invoked("a", "issues.read", json!({})).in_workspace("acme"),
        )
        .await
        .unwrap();
        insert(
            &pool,
            &AuditRecord::invoked("b", "issues.read", json!({})).in_workspace("globex"),
        )
        .await
        .unwrap();
        // A 3-arg record (no explicit tenant) lands under the default tenant.
        insert(&pool, &AuditRecord::invoked("c", "issues.read", json!({})))
            .await
            .unwrap();

        let acme: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_audit WHERE workspace_id = $1")
            .bind("acme")
            .fetch_one(&pool)
            .await
            .unwrap();
        let default: i64 =
            sqlx::query_scalar("SELECT count(*) FROM mcp_audit WHERE workspace_id = 'default'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(acme, 1, "acme tenant trail is isolated");
        assert_eq!(default, 1, "3-arg record falls under default tenant");

        // read_all (audit.tail backend) returns every row, append-ordered, with a
        // round-tripped RFC3339 ts (hq-mt-ops.3).
        let all = select_all(&pool).await.unwrap();
        assert_eq!(all.len(), 3, "read_all sees all tenants' rows");
        let recs: Vec<AuditRecord> = all.into_iter().map(row_to_record).collect();
        assert_eq!(recs[0].actor, "a", "append order preserved");
        assert!(
            recs.iter().all(|r| r.ts.ends_with('Z') && r.ts.contains('T')),
            "ts rendered as UTC RFC3339"
        );
        assert_eq!(recs[0].outcome, Outcome::Invoked);
    }

    /// The durability guarantee (hq-mcp-test.6): the trail survives a server
    /// **restart**. Writes land through one pool; that pool is then closed (the
    /// process exit), a brand-new pool connects to the same database, and the
    /// trail `audit.tail` reads (`select_all`) still returns the prior entries —
    /// proving the sink is Postgres-durable, not in-memory (which would lose them).
    ///
    /// Gated on `GT_PG_URL`; shares the one `mcp_audit` table with the test above,
    /// so the suite runs serially in CI (`-- --test-threads=1`).
    #[tokio::test]
    async fn audit_trail_survives_a_reconnect() {
        let Ok(url) = std::env::var("GT_PG_URL") else {
            eprintln!("skipping: GT_PG_URL unset");
            return;
        };

        // --- "first boot": connect, seed the schema, record two calls -----------
        let pool_a = PgPoolOptions::new().max_connections(2).connect(&url).await.expect("connect a");
        sqlx::query("DROP TABLE IF EXISTS mcp_audit").execute(&pool_a).await.unwrap();
        ensure_schema(&pool_a).await.expect("schema");
        insert(&pool_a, &AuditRecord::invoked("a", "issues.read", json!({})).in_workspace("acme"))
            .await
            .unwrap();
        insert(
            &pool_a,
            &AuditRecord::invoked("b", "issues.close.execute", json!({})).in_workspace("acme"),
        )
        .await
        .unwrap();

        // --- "shutdown": drop every connection, as a process exit would ---------
        pool_a.close().await;

        // --- "restart": a fresh pool to the same DB — no re-seed, no re-insert --
        let pool_b = PgPoolOptions::new().max_connections(2).connect(&url).await.expect("connect b");
        let all = select_all(&pool_b).await.expect("read trail after restart");
        let recs: Vec<AuditRecord> = all.into_iter().map(row_to_record).collect();

        assert_eq!(recs.len(), 2, "the prior trail survived the reconnect (durable, not in-memory)");
        assert_eq!(recs[0].actor, "a", "append order preserved across restart");
        assert_eq!(recs[1].tool, "issues.close.execute");
        assert!(recs.iter().all(|r| r.workspace_id == "acme"), "per-tenant attribution survives");
    }
}
