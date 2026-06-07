//! `audit.*` domain dispatch (`hq-mt-ops.3`).
//!
//! Ports gt-cli's `gt audit` reader into gt-core as a native MCP handler: tail the
//! MCP audit trail (`mcp.invoked` / `mcp.unauthorized` dispatch records) the server
//! writes through its [`AuditSink`]. The sink is the SAME `Arc` the server records
//! into ([`gt_audit::AuditSink`]), so a `tail` sees the live trail without a second
//! store.
//!
//! **Per-tenant by construction.** Every returned record is filtered to the caller's
//! resolved workspace (`DomainCtx::workspace`, defaulting to `"default"` for the
//! single-tenant build) — the `mcp_audit.workspace_id` column landed in
//! `hq-mt-auth.7` precisely so a SOC2 / GDPR per-tenant dump never leaks another
//! tenant's calls. A record stamped for workspace `B` is invisible to a `tail` run
//! in workspace `A`, mirroring the cross-workspace leak invariant (`hq-mt-ops.4`).
//!
//! Tool: `audit.tail` — a read (no state change). Optional `actor` / `tool` /
//! `outcome` exact-match filters, an RFC3339 `since` lower bound (inclusive;
//! RFC3339 sorts lexicographically so a string compare is a time compare), and a
//! `limit` (default 20) of the most-recent-first records.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use gt_audit::{AuditRecord, AuditSink};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;

use super::util::{descriptor, opt};

use super::util::parse;

/// The default tenant a record without an explicit workspace carries, matching
/// [`gt_audit`]'s `mcp_audit.workspace_id` default. A `tail` with no resolved
/// workspace reads this same partition.
const DEFAULT_WS: &str = "default";

/// Default `limit` when the caller omits it — the same cap gt-cli's `gt audit` uses.
fn default_limit() -> usize {
    20
}

/// Parsed `audit.tail` arguments. Every filter is optional; an empty query tails the
/// most recent [`default_limit`] records for the caller's workspace.
#[derive(Debug, Deserialize, Default)]
struct AuditQuery {
    /// Keep only records whose `actor` equals this (exact match).
    actor: Option<String>,
    /// Keep only records whose `tool` equals this (exact match).
    tool: Option<String>,
    /// Keep only records with this outcome (`"invoked"` | `"unauthorized"`).
    outcome: Option<String>,
    /// RFC3339 inclusive lower bound on `ts`. Records with an empty `ts` are dropped
    /// when this is set (they cannot be compared).
    since: Option<String>,
    /// Max records returned, most recent first.
    #[serde(default = "default_limit")]
    limit: usize,
}

/// Read-only handler for the `audit.*` tool namespace over a shared [`AuditSink`].
pub struct AuditHandler {
    sink: Arc<dyn AuditSink + Send + Sync>,
}

impl AuditHandler {
    /// Wrap the server's audit sink. Pass the same `Arc` handed to the server so the
    /// tail reflects the live trail.
    pub fn new(sink: Arc<dyn AuditSink + Send + Sync>) -> Self {
        Self { sink }
    }

    /// Tail the audit trail for `ws`, applying the [`AuditQuery`] filters and
    /// returning the most-recent-first window.
    fn tail(&self, ws: &str, q: &AuditQuery) -> Result<Vec<AuditRecord>, AppError> {
        // `read_all` returns append (oldest -> newest) order; filter, then take the
        // newest `limit` and reverse so the response leads with the most recent call.
        let all = self
            .sink
            .read_all()
            .map_err(|e| AppError::Other(format!("audit sink: {e}")))?;
        let mut hits: Vec<AuditRecord> = all
            .into_iter()
            .filter(|r| record_matches(r, ws, q))
            .collect();
        if hits.len() > q.limit {
            hits.drain(0..hits.len() - q.limit);
        }
        hits.reverse();
        Ok(hits)
    }
}

/// Whether one record passes the tenant gate + every supplied filter.
fn record_matches(r: &AuditRecord, ws: &str, q: &AuditQuery) -> bool {
    if r.workspace_id != ws {
        return false; // per-tenant gate — never leak another workspace's calls.
    }
    if q.actor.as_deref().is_some_and(|a| a != r.actor) {
        return false;
    }
    if q.tool.as_deref().is_some_and(|t| t != r.tool) {
        return false;
    }
    if let Some(want) = q.outcome.as_deref() {
        // Compare against the snake_case wire form (`invoked` / `unauthorized`).
        let got = serde_json::to_value(r.outcome).ok();
        if got.as_ref().and_then(Value::as_str) != Some(want) {
            return false;
        }
    }
    if let Some(since) = q.since.as_deref() {
        // RFC3339 sorts lexicographically, so a string compare is a time compare.
        // A record with no timestamp cannot satisfy a `since` bound.
        if r.ts.is_empty() || r.ts.as_str() < since {
            return false;
        }
    }
    true
}

#[async_trait]
impl DomainHandler for AuditHandler {
    fn namespace(&self) -> &'static str {
        "audit"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![descriptor(
            "audit.tail",
            "Tail the MCP audit trail for the caller's workspace, most recent first. \
             Optional actor/tool/outcome exact-match filters, an RFC3339 `since` lower \
             bound, and a `limit` (default 20).",
            &[
                opt("actor", "string"),
                opt("tool", "string"),
                opt("outcome", "string"),
                opt("since", "string"),
                opt("limit", "integer"),
            ],
        )]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace.unwrap_or(DEFAULT_WS);
        match tool {
            "audit.tail" => {
                let q: AuditQuery = parse(ctx.args)?;
                let records = self.tail(ws, &q)?;
                Ok(json!({ "count": records.len(), "records": records }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_audit::{InMemoryAudit, Outcome};
    use serde_json::json;

    /// Seed a sink with records across two workspaces, actors, tools and outcomes.
    fn seeded() -> Arc<dyn AuditSink + Send + Sync> {
        let sink = InMemoryAudit::new();
        let recs = [
            AuditRecord::new(
                "alice",
                "issues.close.execute",
                json!({}),
                Outcome::Invoked,
                "2026-06-01T10:00:00Z",
            )
            .in_workspace("acme"),
            AuditRecord::new(
                "bob",
                "merge.submit.execute",
                json!({}),
                Outcome::Unauthorized,
                "2026-06-02T10:00:00Z",
            )
            .in_workspace("acme"),
            AuditRecord::new(
                "alice",
                "rig.add.execute",
                json!({}),
                Outcome::Invoked,
                "2026-06-03T10:00:00Z",
            )
            .in_workspace("acme"),
            // A different tenant — must never surface in an `acme` tail.
            AuditRecord::new(
                "mallory",
                "issues.close.execute",
                json!({}),
                Outcome::Invoked,
                "2026-06-03T11:00:00Z",
            )
            .in_workspace("other"),
        ];
        for r in recs {
            sink.record(r).unwrap();
        }
        Arc::new(sink)
    }

    fn ctx(ws: &'static str, args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: Some(ws),
            actor: "tester",
            args,
        }
    }

    #[tokio::test]
    async fn tails_caller_workspace_most_recent_first() {
        let h = AuditHandler::new(seeded());
        let out = h
            .dispatch("audit.tail", ctx("acme", json!({})))
            .await
            .unwrap();
        assert_eq!(out["count"], 3); // the three acme records, not the `other` one.
        let recs = out["records"].as_array().unwrap();
        // Newest first.
        assert_eq!(recs[0]["tool"], "rig.add.execute");
        assert_eq!(recs[2]["tool"], "issues.close.execute");
    }

    #[tokio::test]
    async fn never_leaks_another_tenant() {
        let h = AuditHandler::new(seeded());
        let out = h
            .dispatch("audit.tail", ctx("acme", json!({})))
            .await
            .unwrap();
        let recs = out["records"].as_array().unwrap();
        assert!(recs.iter().all(|r| r["actor"] != "mallory"));
        // And a tail in the other tenant sees only its one record.
        let out2 = h
            .dispatch("audit.tail", ctx("other", json!({})))
            .await
            .unwrap();
        assert_eq!(out2["count"], 1);
        assert_eq!(out2["records"][0]["actor"], "mallory");
    }

    #[tokio::test]
    async fn filters_by_actor_tool_outcome_and_since() {
        let h = AuditHandler::new(seeded());
        // actor.
        let out = h
            .dispatch("audit.tail", ctx("acme", json!({ "actor": "alice" })))
            .await
            .unwrap();
        assert_eq!(out["count"], 2);
        // tool.
        let out = h
            .dispatch(
                "audit.tail",
                ctx("acme", json!({ "tool": "merge.submit.execute" })),
            )
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        // outcome.
        let out = h
            .dispatch(
                "audit.tail",
                ctx("acme", json!({ "outcome": "unauthorized" })),
            )
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["records"][0]["actor"], "bob");
        // since (inclusive lower bound) — drops the 06-01 record.
        let out = h
            .dispatch(
                "audit.tail",
                ctx("acme", json!({ "since": "2026-06-02T00:00:00Z" })),
            )
            .await
            .unwrap();
        assert_eq!(out["count"], 2);
    }

    #[tokio::test]
    async fn limit_caps_the_window_to_most_recent() {
        let h = AuditHandler::new(seeded());
        let out = h
            .dispatch("audit.tail", ctx("acme", json!({ "limit": 1 })))
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["records"][0]["tool"], "rig.add.execute"); // the newest.
    }

    #[tokio::test]
    async fn unknown_tool_is_validation_error() {
        let h = AuditHandler::new(seeded());
        assert!(matches!(
            h.dispatch("audit.bogus", ctx("acme", json!({}))).await,
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn namespace_is_audit() {
        assert_eq!(AuditHandler::new(seeded()).namespace(), "audit");
    }
}
