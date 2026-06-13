//! `a2a.delegate` MCP tool (Fase 2 — A3): lets a running agent delegate a
//! sub-task to another agent via the A2A intake pipeline without an HTTP
//! round-trip.
//!
//! The tool stamps the **caller's MCP session id** (`ctx.actor`) as the bead's
//! `created_by` (A2 attribution, Fase 2), so the resulting bead is traceable
//! to the specific agent that requested the work — not to the generic
//! gateway identity.
//!
//! Design: the handler calls `run_create_issue` directly (the same path the
//! HTTP A2A gateway drives through `DoltIntake`) and emits a `{bead,priority}`
//! dispatch order on the existing scheduler channel, bypassing the gateway's
//! HTTP round-trip entirely. No new pipeline — A2A intake is the one door.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_channel::DispatchSink;
use gt_issues::handlers::run_create_issue;
use gt_issues::{CreateIssue, Domain, IssueType, SurfaceTree};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::{AppError, DoltIssues};

use super::util::{descriptor, opt, req};

/// Intake beads carry no surface, so existence checks are vacuous.
struct NoSurfaces;
impl SurfaceTree for NoSurfaces {
    fn contains(&self, _path: &str) -> bool {
        true
    }
}

/// MCP handler for the `a2a` namespace (currently: `a2a.delegate`).
///
/// Wired in `build_domain_router` when ALL of the required A2A envs are set:
/// `GT_A2A_DEFAULT_RIG`, `GT_A2A_INTAKE_EPIC`, `GT_DOLT_URL`, and a dispatch
/// channel (file or PG, same as convoy/agent).
pub struct A2aDelegateHandler {
    store: Arc<DoltIssues>,
    sink: Arc<DispatchSink>,
    default_rig: String,
    default_parent: String,
}

impl A2aDelegateHandler {
    pub fn new(
        store: Arc<DoltIssues>,
        sink: Arc<DispatchSink>,
        default_rig: impl Into<String>,
        default_parent: impl Into<String>,
    ) -> Self {
        Self {
            store,
            sink,
            default_rig: default_rig.into(),
            default_parent: default_parent.into(),
        }
    }
}

#[async_trait]
impl DomainHandler for A2aDelegateHandler {
    fn namespace(&self) -> &'static str {
        "a2a"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![descriptor(
            "a2a.delegate",
            "Delegate a sub-task to another agent via the A2A intake pipeline. \
             Mints a tracker bead (child of the default intake epic) and dispatches it \
             onto the scheduler. The calling agent's session id is stamped as `created_by`.",
            &[
                req("title", "string"),
                opt("description", "string"),
                opt("rig", "string"),
                opt("parent_id", "string"),
                opt("priority", "number"),
            ],
        )]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "a2a.delegate" => {
                let title = ctx
                    .args
                    .get("title")
                    .and_then(Value::as_str)
                    .ok_or_else(|| AppError::Validation("`title` is required".into()))?
                    .to_string();
                let description = ctx
                    .args
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(&title)
                    .to_string();
                let rig = ctx
                    .args
                    .get("rig")
                    .and_then(Value::as_str)
                    .unwrap_or(&self.default_rig)
                    .to_string();
                let parent_id = ctx
                    .args
                    .get("parent_id")
                    .and_then(Value::as_str)
                    .unwrap_or(&self.default_parent)
                    .to_string();
                let priority = ctx
                    .args
                    .get("priority")
                    .and_then(Value::as_u64)
                    .map(|p| p.min(2) as u8)
                    .unwrap_or(1);

                let args = CreateIssue {
                    id: None,
                    rig: rig.clone(),
                    title,
                    description,
                    design: String::new(),
                    acceptance_criteria: String::new(),
                    notes: String::new(),
                    priority,
                    issue_type: IssueType::Task,
                    // A2 attribution (Fase 2): stamp the delegating agent's MCP
                    // session id so the bead is traceable to the specific agent.
                    created_by: ctx.actor.to_string(),
                    parent_id: Some(parent_id),
                    assignee: None,
                    owner: None,
                    // Beads minted via delegation carry no own dispatch policy —
                    // they inherit the intake epic's (same as the HTTP A2A path).
                    domain: vec![Domain::MetaGap],
                    surface: Vec::new(),
                    depends_on: Vec::new(),
                    role_scope: None,
                    phase: None,
                    workspace: String::new(),
                    dispatch: None,
                };

                let bead = run_create_issue(&self.store, &args, &NoSurfaces, false)
                    .await
                    .map_err(|e| AppError::Validation(e.to_string()))?;

                let payload = json!({"bead": bead, "priority": priority}).to_string();
                self.sink
                    .emit(payload.as_bytes())
                    .map_err(|e| AppError::Validation(format!("dispatch: {e}")))?;

                Ok(json!({ "id": bead, "rig": rig, "status": "submitted" }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
