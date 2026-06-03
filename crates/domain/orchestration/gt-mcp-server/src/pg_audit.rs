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
pub struct PgAuditSink {
    tx: UnboundedSender<AuditRecord>,
}

impl PgAuditSink {
    /// Connect to Postgres, ensure the `mcp_audit` table, and spawn the drain
    /// task. Returns a sink whose `record` is a non-blocking channel push.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new().max_connections(2).connect(url).await?;
        ensure_schema(&pool).await?;
        let (tx, mut rx) = unbounded_channel::<AuditRecord>();
        // Detached drain: lives for the process. Each record is one INSERT; a
        // failure is logged, not propagated — the server keeps serving.
        tokio::spawn(async move {
            while let Some(rec) = rx.recv().await {
                if let Err(e) = insert(&pool, &rec).await {
                    eprintln!("[gt-mcp-server] PG audit insert failed ({}): {e}", rec.tool);
                }
            }
        });
        Ok(Self { tx })
    }
}

impl AuditSink for PgAuditSink {
    fn record(&self, record: AuditRecord) -> Result<(), AppError> {
        self.tx
            .send(record)
            .map_err(|_| AppError::Sink("audit drain task is gone".into()))
    }

    fn read_all(&self) -> Result<Vec<AuditRecord>, AppError> {
        // The durable trail lives in Postgres; the server never reads it back
        // through the sink (queries go straight to PG). Not supported here.
        Err(AppError::Sink("read_all is not supported by PgAuditSink".into()))
    }
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
        "INSERT INTO mcp_audit (workspace_id, actor, tool, args, outcome) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&rec.workspace_id)
    .bind(&rec.actor)
    .bind(&rec.tool)
    .bind(rec.args.to_string())
    .bind(outcome)
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
    }
}
