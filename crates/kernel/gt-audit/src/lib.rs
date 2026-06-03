//! `gt-audit` — audit sink for the MCP tool dispatch path (hq-core-host.4).
//!
//! Every tool dispatch records one [`AuditRecord`]: the resolved `actor`, the `tool`
//! name, its `args`, and an [`Outcome`] — `Invoked` when [`crate::Outcome::Invoked`]
//! the scope check passed and the tool ran, `Unauthorized` when `gt_rbac::Scope::check`
//! rejected it. The server (hq-core-host.3) calls [`AuditSink::record`] on both paths
//! so an unauthorized attempt leaves a trail rather than vanishing.
//!
//! MVP ships the [`AuditSink`] port + an in-memory adapter ([`InMemoryAudit`]). A
//! Postgres sink (append into a `hq.audit` table) follows once the server bin lands;
//! it implements the same trait, so the dispatch path never changes.
//!
//! Sync, no tokio: the trait takes `&self` with interior mutability so the server can
//! share one sink across concurrent dispatches behind an `Arc`.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Local error for the audit sink. In-memory recording does not fail; the `Result`
/// exists for the Postgres sink to surface I/O errors without changing the trait.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AppError {
    /// The sink could not persist the record (I/O, encode).
    #[error("audit sink error: {0}")]
    Sink(String),
}

/// Whether the dispatch ran or was rejected at the scope frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Scope check passed; the tool ran.
    Invoked,
    /// `Scope::check` rejected the call before the tool ran.
    Unauthorized,
}

/// Default tenant for a record built without an explicit workspace. Matches the
/// `mcp_audit.workspace_id` column default so a single-tenant dispatch (the server
/// before per-workspace auth lands, hq-mt-auth.5/.9) still produces a valid row.
fn default_workspace() -> String {
    "default".to_string()
}

/// One audited tool dispatch. `args` is the raw, type-erased tool input so the trail
/// is independent of any tool's typed payload (mirrors how the event log stays
/// type-erased). `ts` is an RFC3339 string supplied by the caller — the kernel crate
/// stays clock-free; the server stamps the time it already has on the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// Server-injected tenant. Like `actor`, never a wire field — the server resolves
    /// it from the auth context and stamps it here so the trail is per-tenant filterable
    /// (SOC2 per-tenant audit dump). Defaults to `"default"` so a record built before
    /// per-workspace auth is wired still carries a non-null tenant (hq-mt-auth.7).
    #[serde(default = "default_workspace")]
    pub workspace_id: String,
    /// Server-injected scope actor (never a wire field — same posture as workspace_id).
    pub actor: String,
    /// Dotted tool name, e.g. `issues.close.execute`.
    pub tool: String,
    /// Raw tool arguments as received.
    pub args: serde_json::Value,
    /// Whether the call was invoked or rejected.
    pub outcome: Outcome,
    /// RFC3339 timestamp supplied by the caller. Empty when the caller has no clock.
    #[serde(default)]
    pub ts: String,
}

impl AuditRecord {
    /// Build a record with an explicit timestamp. The tenant defaults to `"default"`;
    /// the server stamps the real one with [`AuditRecord::in_workspace`] once it has
    /// resolved the request's workspace from the auth context.
    pub fn new(
        actor: impl Into<String>,
        tool: impl Into<String>,
        args: serde_json::Value,
        outcome: Outcome,
        ts: impl Into<String>,
    ) -> Self {
        Self {
            workspace_id: default_workspace(),
            actor: actor.into(),
            tool: tool.into(),
            args,
            outcome,
            ts: ts.into(),
        }
    }

    /// Stamp the resolved tenant onto a record (builder style). The dispatch path
    /// keeps building records with the existing 3-arg constructors and tags the
    /// workspace here, so adding the column never broke a call site.
    pub fn in_workspace(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = workspace_id.into();
        self
    }

    /// Record of a successful dispatch, timestamp left empty for the caller to fill.
    pub fn invoked(
        actor: impl Into<String>,
        tool: impl Into<String>,
        args: serde_json::Value,
    ) -> Self {
        Self::new(actor, tool, args, Outcome::Invoked, "")
    }

    /// Record of a scope rejection, timestamp left empty for the caller to fill.
    pub fn unauthorized(
        actor: impl Into<String>,
        tool: impl Into<String>,
        args: serde_json::Value,
    ) -> Self {
        Self::new(actor, tool, args, Outcome::Unauthorized, "")
    }
}

/// Port for the audit log. `InMemoryAudit` implements it for tests/dev; the Postgres
/// adapter implements it in production. `&self` + interior mutability so one sink is
/// shared across dispatches.
pub trait AuditSink {
    /// Append one record. Append-only — no update or delete.
    fn record(&self, record: AuditRecord) -> Result<(), AppError>;

    /// Read every record in append order. Primarily for tests and the dev dashboard;
    /// the Postgres sink may bound this later.
    fn read_all(&self) -> Result<Vec<AuditRecord>, AppError>;
}

/// In-memory audit sink: a `Mutex`-guarded append-only vector. Zero I/O, so `record`
/// never returns `Err`. Used by tests and any single-process dev run before the
/// Postgres sink is wired.
#[derive(Debug, Default)]
pub struct InMemoryAudit {
    records: Mutex<Vec<AuditRecord>>,
}

impl InMemoryAudit {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of recorded rows. Convenience for assertions.
    pub fn len(&self) -> usize {
        self.records.lock().expect("audit mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AuditSink for InMemoryAudit {
    fn record(&self, record: AuditRecord) -> Result<(), AppError> {
        self.records
            .lock()
            .map_err(|e| AppError::Sink(format!("mutex poisoned: {e}")))?
            .push(record);
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<AuditRecord>, AppError> {
        Ok(self
            .records
            .lock()
            .map_err(|e| AppError::Sink(format!("mutex poisoned: {e}")))?
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn records_invoked_and_unauthorized_in_order() {
        let sink = InMemoryAudit::new();
        assert!(sink.is_empty());

        sink.record(AuditRecord::invoked(
            "claude-host",
            "issues.close.execute",
            json!({ "id": "hq-core-host.4" }),
        ))
        .unwrap();
        sink.record(AuditRecord::unauthorized(
            "watcher",
            "issues.close.execute",
            json!({ "id": "hq-core-host.4" }),
        ))
        .unwrap();

        let rows = sink.read_all().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].actor, "claude-host");
        assert_eq!(rows[0].outcome, Outcome::Invoked);
        assert_eq!(rows[1].actor, "watcher");
        assert_eq!(rows[1].outcome, Outcome::Unauthorized);
        assert_eq!(rows[1].tool, "issues.close.execute");
    }

    #[test]
    fn record_carries_actor_tool_args_outcome() {
        let sink = InMemoryAudit::new();
        let args = json!({ "id": "x", "commit_sha": "abc1234" });
        sink.record(AuditRecord::new(
            "max",
            "issues.update.execute",
            args.clone(),
            Outcome::Invoked,
            "2026-06-01T12:00:00Z",
        ))
        .unwrap();

        let row = &sink.read_all().unwrap()[0];
        assert_eq!(row.actor, "max");
        assert_eq!(row.tool, "issues.update.execute");
        assert_eq!(row.args, args);
        assert_eq!(row.outcome, Outcome::Invoked);
        assert_eq!(row.ts, "2026-06-01T12:00:00Z");
    }

    #[test]
    fn workspace_defaults_then_stamps_via_builder() {
        // A record built with the 3-arg constructor carries the default tenant,
        // so a single-tenant dispatch (pre per-workspace auth) is still valid.
        let rec = AuditRecord::invoked("max", "issues.update.execute", json!({}));
        assert_eq!(rec.workspace_id, "default");

        // The server stamps the resolved tenant without touching the 3-arg call site.
        let scoped = rec.in_workspace("acme");
        assert_eq!(scoped.workspace_id, "acme");
        assert_eq!(scoped.actor, "max");
    }

    #[test]
    fn record_missing_workspace_deserializes_to_default() {
        // Legacy rows / payloads written before the column existed have no
        // `workspace_id`; serde must fill the default rather than fail.
        let legacy = json!({
            "actor": "claude-host",
            "tool": "issues.close.execute",
            "args": { "id": "x" },
            "outcome": "invoked",
            "ts": ""
        });
        let rec: AuditRecord = serde_json::from_value(legacy).unwrap();
        assert_eq!(rec.workspace_id, "default");
    }

    #[test]
    fn outcome_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Outcome::Unauthorized).unwrap(),
            "\"unauthorized\""
        );
        assert_eq!(
            serde_json::to_string(&Outcome::Invoked).unwrap(),
            "\"invoked\""
        );
    }
}
