//! `analytics.*` domain dispatch — the Kanban dashboard summary
//! (hq-1cd840, epic hq-56b5ee).
//!
//! `analytics.summary` projects one (rig, workspace) board into the operator's
//! four KPIs (avance/errores/pendientes/retrasos) + chart series — a pure
//! read over the SAME tracker rows `board.list` and the report engine consume
//! ([`gt_issues::analytics`]), so the numbers reconcile by construction. The
//! reopen half of ERRORES comes from the audit trail: `issues.transition.execute`
//! calls whose target was `open` (a closed bead coming back is a reopen signal).
//! Read-only — no new store.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use gt_audit::AuditSink;
use gt_issues::analytics::summarize;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::{AppError, DoltIssues, IssueFilter, WorkspacePools};

use super::util::{descriptor, opt, req, str_arg};

const DEFAULT_AT_RISK_DAYS: i64 = 3;
const DEFAULT_SERIES_DAYS: i64 = 30;

/// Dolt + audit backed handler for the `analytics.*` tool namespace.
pub struct AnalyticsHandler {
    dolt: Arc<DoltIssues>,
    dolt_workspaces: Option<Arc<WorkspacePools>>,
    audit: Arc<dyn AuditSink + Send + Sync>,
}

impl AnalyticsHandler {
    /// Wire the tracker (default store + optional per-workspace pools) and the
    /// audit sink the reopen count derives from.
    pub fn new(
        dolt: Arc<DoltIssues>,
        dolt_workspaces: Option<Arc<WorkspacePools>>,
        audit: Arc<dyn AuditSink + Send + Sync>,
    ) -> Self {
        Self { dolt, dolt_workspaces, audit }
    }

    async fn tracker(&self, ws: Option<&str>) -> Result<Arc<DoltIssues>, AppError> {
        match (&self.dolt_workspaces, ws) {
            (Some(pools), Some(ws)) => Ok(Arc::new(DoltIssues::new(pools.ensured_pool(ws).await?))),
            _ => Ok(self.dolt.clone()),
        }
    }

    /// Reopened transitions in this tenant's audit trail: successful
    /// `issues.transition.execute` calls targeting `open` on a rig-prefixed
    /// bead. Best-effort — an empty/unreadable trail counts zero.
    fn reopens(&self, ws: &str, rig: &str) -> i64 {
        let Ok(all) = self.audit.read_all() else { return 0 };
        all.iter()
            .filter(|r| r.workspace_id == ws)
            .filter(|r| r.tool == "issues.transition.execute")
            .filter(|r| r.args.get("target").and_then(Value::as_str) == Some("open"))
            .filter(|r| {
                r.args
                    .get("id")
                    .and_then(Value::as_str)
                    .map(|id| id.starts_with(&format!("{rig}-")))
                    .unwrap_or(false)
            })
            .count() as i64
    }
}

#[async_trait]
impl DomainHandler for AnalyticsHandler {
    fn namespace(&self) -> &'static str {
        "analytics"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![descriptor(
            "analytics.summary",
            "The Kanban dashboard summary for one (rig, workspace) board: the four \
             operator KPIs — avance % (closed/total), errores (defect-type beads + \
             audit-derived reopens, by module and Nivel), pendientes (open+working, by \
             assignee/priority/module), retrasos (due_date < today and not closed, with \
             a configurable at-risk window) — plus horas (estimated-hours totals: \
             estimadas/completadas/pendientes, sin_estimar coverage gap, by module), \
             burndown / created-vs-resolved series (cards and remaining hours) and \
             the status distribution. Same projection board.list and \
             report.generate read, so the numbers reconcile. Read-only.",
            &[
                req("rig", "string"),
                req("workspace", "string"),
                opt("epic", "string"),
                opt("at_risk_days", "integer"),
                opt("series_days", "integer"),
            ],
        )]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "analytics.summary" => {
                let rig = str_arg(&ctx.args, "rig")?.trim().to_string();
                let workspace = str_arg(&ctx.args, "workspace")?.trim().to_string();
                if rig.is_empty() || workspace.is_empty() {
                    return Err(AppError::Validation(
                        "rig and workspace are required (board scope key)".into(),
                    ));
                }
                let epic = ctx.args.get("epic").and_then(Value::as_str).map(str::to_string);
                let at_risk_days = ctx
                    .args
                    .get("at_risk_days")
                    .and_then(Value::as_i64)
                    .unwrap_or(DEFAULT_AT_RISK_DAYS)
                    .clamp(0, 365);
                let series_days = ctx
                    .args
                    .get("series_days")
                    .and_then(Value::as_i64)
                    .unwrap_or(DEFAULT_SERIES_DAYS)
                    .clamp(7, 365);

                let tracker = self.tracker(ctx.workspace).await?;
                let rows = tracker
                    .list(&IssueFilter {
                        rig: Some(rig.clone()),
                        workspace: Some(workspace.clone()),
                        parent_id: epic,
                        limit: Some(gt_store_dolt::issues_max_limit()),
                        ..Default::default()
                    })
                    .await?;
                let parent_map = tracker.parent_map(&rig, &workspace).await?;
                let reopens = self.reopens(ctx.workspace.unwrap_or("default"), &rig);
                let today = sqlx::types::chrono::Utc::now().format("%Y-%m-%d").to_string();
                let summary =
                    summarize(&rig, &workspace, &rows, reopens, &today, at_risk_days, series_days, &parent_map);
                serde_json::to_value(&summary)
                    .map_err(|e| AppError::Other(format!("encode summary: {e}")))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
