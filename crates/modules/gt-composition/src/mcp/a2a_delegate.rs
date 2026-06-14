//! `a2a.*` MCP tools — inter-agent delegation + peer discovery (Fase 2).
//!
//! ## Tools
//!
//! - **`a2a.delegate`** (A3): delegates a sub-task to another agent via the A2A
//!   intake pipeline without an HTTP round-trip. Stamps the caller's MCP session
//!   id as `created_by` (A2 attribution).
//!
//! - **`a2a.discover`** (A7, gtcore-3a3557): returns the Agent Card skills for
//!   every rig in the caller's workspace, so an agent can decide which peer to
//!   delegate to based on capabilities (tags, repo, branch). Optionally filters
//!   by a `tag` argument for skill matching.
//!
//! - **`a2a.status`** (A7, gtcore-3a3557): queries the tracker for a previously
//!   delegated bead, returning its status + title so the delegating agent can
//!   poll the outcome without leaving the MCP surface.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_a2a::AgentSkill;
use gt_channel::DispatchSink;
use gt_issues::handlers::run_create_issue;
use gt_issues::{CreateIssue, Domain, IssueType, SurfaceTree};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_rig::{PgRigs, RigRepository};
use gt_store_dolt::{AppError, DoltIssues};

use crate::delegation::{rfc3339_now, DelegationEvent, DEFAULT_TIMEOUT_SECS};

use super::eventlog::EventLog;
use super::pools::WsPools;
use super::util::{descriptor, opt, req};

/// Intake beads carry no surface, so existence checks are vacuous.
struct NoSurfaces;
impl SurfaceTree for NoSurfaces {
    fn contains(&self, _path: &str) -> bool {
        true
    }
}

/// MCP handler for the `a2a` namespace.
///
/// Wired in `build_domain_router` when ALL of the required A2A envs are set:
/// `GT_A2A_DEFAULT_RIG`, `GT_A2A_INTAKE_EPIC`, `GT_DOLT_URL`, and a dispatch
/// channel (file or PG, same as convoy/agent).
pub struct A2aDelegateHandler {
    store: Arc<DoltIssues>,
    sink: Arc<DispatchSink>,
    default_rig: String,
    default_parent: String,
    /// Per-workspace Postgres pool cache for rig discovery (A7). `None` when
    /// `GT_PG_URL` is unset — `a2a.discover` returns an empty skill list.
    pools: Option<Arc<WsPools>>,
    /// Event log a `delegation.requested.v1` is appended to so the daemon-side
    /// callback/timeout machinery (B5) can push the outcome back to the parent
    /// instead of the parent polling `a2a.status`. `None` ⇒ delegations still
    /// dispatch, just without push-callback tracking.
    delegation_log: Option<Arc<EventLog>>,
    /// Default completion timeout stamped on a delegation when the caller passes
    /// no `timeout_secs` (B5). `0` disables the timeout.
    default_timeout_secs: u64,
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
            pools: None,
            delegation_log: None,
            default_timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }

    /// Wire the per-workspace pool cache so `a2a.discover` can read the rig
    /// catalog. Without this, discovery returns an empty skill list.
    pub fn with_pools(mut self, pools: Arc<WsPools>) -> Self {
        self.pools = Some(pools);
        self
    }

    /// Wire push-callback tracking (B5): a successful `a2a.delegate` appends a
    /// `delegation.requested.v1` to `log`, which the daemon-side
    /// [`DelegationCallbackPlugin`](crate::delegation::DelegationCallbackPlugin)
    /// and [`DelegationTimeoutTicker`](crate::delegation::DelegationTimeoutTicker)
    /// consume to push the outcome back to the parent. `default_timeout_secs`
    /// (0 disables) applies when the caller passes no `timeout_secs`.
    pub fn with_delegation_log(
        mut self,
        log: Arc<EventLog>,
        default_timeout_secs: u64,
    ) -> Self {
        self.delegation_log = Some(log);
        self.default_timeout_secs = default_timeout_secs;
        self
    }
}

#[async_trait]
impl DomainHandler for A2aDelegateHandler {
    fn namespace(&self) -> &'static str {
        "a2a"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "a2a.delegate",
                "Delegate a sub-task to another agent via the A2A intake pipeline. \
                 Mints a tracker bead (child of the default intake epic) and dispatches it \
                 onto the scheduler. The calling agent's session id is stamped as `created_by`. \
                 You do NOT need to poll a2a.status: when the child reaches a terminal state the \
                 daemon pushes a callback (delegation.completed.v1 + operator bell), and a \
                 `timeout_secs` (default applies) auto-escalates a stuck delegation.",
                &[
                    req("title", "string"),
                    opt("description", "string"),
                    opt("rig", "string"),
                    opt("parent_id", "string"),
                    opt("priority", "number"),
                    opt("timeout_secs", "number"),
                ],
            ),
            descriptor(
                "a2a.discover",
                "Discover peer agent capabilities. Returns one Agent Card skill per rig in \
                 the workspace catalog, so you can decide which peer to delegate to based on \
                 name, tags, and repo. Pass `tag` to filter by skill tag.",
                &[opt("tag", "string")],
            ),
            descriptor(
                "a2a.status",
                "One-shot read of a previously delegated bead's current status, title, and \
                 timestamps. Polling is no longer required: a2a.delegate registers a push \
                 callback that reports the terminal outcome (delegation.completed.v1 / bell). \
                 Use this only for an on-demand check.",
                &[req("id", "string")],
            ),
        ]
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
                // B5: per-delegation completion timeout (0 disables). Defaults to
                // the deploy default when the caller omits it.
                let timeout_secs = ctx
                    .args
                    .get("timeout_secs")
                    .and_then(Value::as_u64)
                    .unwrap_or(self.default_timeout_secs);

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

                // B5: register the delegation so the daemon pushes the outcome
                // back to the parent instead of the parent polling a2a.status.
                // Best-effort — the bead is already minted + dispatched, so a
                // tracking-log failure must not fail the delegation.
                if let Some(log) = &self.delegation_log {
                    if let Err(e) = log.append(
                        ctx.workspace,
                        DelegationEvent::Requested {
                            child: bead.clone(),
                            parent: ctx.actor.to_string(),
                            rig: rig.clone(),
                            at: rfc3339_now(),
                            timeout_secs,
                        },
                    ) {
                        eprintln!("[a2a.delegate] delegation tracking append failed for {bead}: {e}");
                    }
                }

                Ok(json!({
                    "id": bead,
                    "rig": rig,
                    "status": "submitted",
                    "timeout_secs": timeout_secs,
                }))
            }

            // A7 (gtcore-3a3557): peer discovery — list rig capabilities as Agent
            // Card skills so an agent can choose whom to delegate to.
            "a2a.discover" => {
                let tag_filter = ctx.args.get("tag").and_then(Value::as_str);

                let skills: Vec<Value> = match &self.pools {
                    Some(pools) => {
                        let pool = pools.get(ctx.workspace).await?;
                        let repo = PgRigs::new(pool.pool().clone());
                        let rigs = repo.list().await.map_err(|e| {
                            AppError::Other(format!("rig catalog: {e}"))
                        })?;
                        rigs.iter()
                            .map(|r| AgentSkill {
                                id: r.name.clone(),
                                name: r.name.clone(),
                                description: Some(if r.git_url.is_empty() {
                                    format!("dispatch work onto rig `{}`", r.name)
                                } else {
                                    format!(
                                        "dispatch work onto rig `{}` ({}; default branch {})",
                                        r.name, r.git_url, r.default_branch
                                    )
                                }),
                                tags: vec![r.prefix.clone()],
                            })
                            .filter(|s| match tag_filter {
                                Some(tag) => s.tags.iter().any(|t| t == tag) || s.id == tag,
                                None => true,
                            })
                            .map(|s| json!({
                                "id": s.id,
                                "name": s.name,
                                "description": s.description,
                                "tags": s.tags,
                            }))
                            .collect()
                    }
                    None => Vec::new(),
                };

                Ok(json!({ "skills": skills }))
            }

            // A7 (gtcore-3a3557): delegation status — poll a delegated bead's
            // tracker state so the parent agent knows when the child finishes.
            "a2a.status" => {
                let id = ctx
                    .args
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| AppError::Validation("`id` is required".into()))?;

                match self.store.get_detail(id).await {
                    Ok(Some(detail)) => Ok(json!({
                        "id": detail.id,
                        "title": detail.title,
                        "status": detail.status,
                        "assignee": detail.assignee,
                        "created_at": detail.created_at,
                        "updated_at": detail.updated_at,
                        "closed_at": detail.closed_at,
                    })),
                    Ok(None) => Err(AppError::NotFound(format!("bead {id}"))),
                    Err(e) => Err(AppError::Other(format!("tracker: {e}"))),
                }
            }

            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
