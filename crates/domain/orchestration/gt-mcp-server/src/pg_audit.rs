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
async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mcp_audit (
            id      bigserial PRIMARY KEY,
            actor   text NOT NULL,
            tool    text NOT NULL,
            args    text NOT NULL,
            outcome text NOT NULL,
            ts      timestamptz NOT NULL DEFAULT now()
        )",
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
    sqlx::query("INSERT INTO mcp_audit (actor, tool, args, outcome) VALUES ($1, $2, $3, $4)")
        .bind(&rec.actor)
        .bind(&rec.tool)
        .bind(rec.args.to_string())
        .bind(outcome)
        .execute(pool)
        .await?;
    Ok(())
}
