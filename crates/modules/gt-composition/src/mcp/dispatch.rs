//! `dispatch.*` domain handler (gtcore-7bec8c — C3 of the "Control de despacho"
//! epic).
//!
//! Exposes the composite `ready_for_auto` agent-dispatch frontier as an MCP tool
//! so operators and the autonomous dispatcher can query which beads are safe to
//! hand to an agent right now.
//!
//! The frontier ANDs five clauses (see [`gt_issues::ready_for_auto`]):
//!
//! 1. **Readiness** (§S4): deps delivered, phase open, surface exists, status open.
//! 2. **Dispatch policy** (C1): resolved `dispatch = auto` (own or inherited).
//! 3. **Operator lock** (C2): not inside a human-claimed epic subtree.
//! 4. **Surface overlap** (C3): no crate path collision with a `working` bead.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_channel::DispatchSink;
use gt_issues::{
    locked_roots, occupied_surfaces, ready_for_auto, session_like_actor, AllowAllTree, SurfaceTree,
};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_rig::PgRigs;
use gt_scheduling::dispatch::DispatchPayload;
use gt_store_dolt::{AppError, DoltIssues, IssueFilter};

use super::pools::WsPools;
use super::util::{descriptor, opt, req};
use crate::auto_dispatch::{CatalogHeldRigs, HeldRigs};

/// Dolt-backed handler for the `dispatch.*` tool namespace.
pub struct DispatchHandler {
    store: Arc<DoltIssues>,
    repo_dir: Option<PathBuf>,
    /// Per-workspace rig pools, used to resolve the rigs on dispatch hold (rig-hold H2) so the
    /// probe reflects the SAME frontier the orchd would sling. Absent ⇒ no holds applied.
    held_pools: Option<Arc<WsPools>>,
    /// When wired, auto-completes the bead's merge slot after a successful close with a
    /// `delivered_sha` — prevents stale `failed` slots for work already in main (gtcore-71c575).
    merge: Option<Arc<super::MergeHandler>>,
    /// The mayor's delegation edge (gtcore-d24661): `dispatch.request` drops a `{bead,priority}`
    /// request on the SAME channel the orchd scheduler consumes, so a delegation materializes a
    /// real polecat. Unlike the `agent.spawn` bridge this edge is LOUD: an unwired sink or a
    /// failed emit returns an error to the caller instead of recording silently.
    sink: Option<Arc<DispatchSink>>,
}

impl DispatchHandler {
    pub fn new(store: Arc<DoltIssues>, repo_dir: Option<PathBuf>) -> Self {
        Self {
            store,
            repo_dir,
            held_pools: None,
            merge: None,
            sink: None,
        }
    }

    /// Wire the orchd dispatch sink so `dispatch.request` can enqueue a real sling
    /// (gtcore-d24661). Without it the tool errors loudly instead of no-op'ing.
    pub fn with_dispatch_sink(mut self, sink: Arc<DispatchSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Wire the merge handler so closed beads auto-complete their merge slot (gtcore-71c575).
    pub fn with_merge(mut self, merge: Arc<super::MergeHandler>) -> Self {
        self.merge = Some(merge);
        self
    }

    /// Wire the per-workspace rig pools so the probe excludes held rigs (rig-hold H2, gtcore-1f5e67),
    /// matching the orchd's [`crate::auto_dispatch::FrontierSource`]. Without this the probe ignores
    /// holds (back-compat).
    pub fn with_held_rigs(mut self, pools: Arc<WsPools>) -> Self {
        self.held_pools = Some(pools);
        self
    }

    /// The rigs on dispatch hold in `workspace` — fail-open to an empty set so a rig-catalog read
    /// fault never makes the probe over-report the frontier.
    async fn held_rigs(&self, workspace: Option<&str>) -> HashSet<String> {
        let Some(pools) = &self.held_pools else {
            return HashSet::new();
        };
        match pools.get(workspace).await {
            Ok(pool) => {
                CatalogHeldRigs::new(PgRigs::new(pool.pool().clone()))
                    .held()
                    .await
            }
            Err(e) => {
                eprintln!("[dispatch-probe] held-rigs pool resolve failed for ws {workspace:?} (fail-open): {e}");
                HashSet::new()
            }
        }
    }

    fn surface_tree(&self) -> Box<dyn SurfaceTree + Send + Sync> {
        match &self.repo_dir {
            Some(dir) => gt_mcp_server::git_tree::surface_tree(Some(dir)),
            None => Box::new(AllowAllTree),
        }
    }

    /// Query the full agent-dispatch frontier for the given rig/workspace scope.
    async fn probe(&self, rig: Option<&str>, workspace: Option<&str>) -> Result<Value, AppError> {
        let filter = IssueFilter {
            rig: rig.map(str::to_string),
            workspace: workspace.map(str::to_string),
            ready: false,
            ..Default::default()
        };

        // Fetch candidate rows (open beads in the scope).
        let rows = self.store.list(&filter).await?;

        // Gather the five data sets the frontier needs (all unscoped where noted).
        let deps_map = self.store.depends_on_edges(&filter).await?;
        let dep_facts = self.store.dep_index().await?;
        let open_phase = self.store.open_phase().await?;
        let tree = self.surface_tree();

        // Unscoped maps: ancestors / working epics may live outside the rig/ws.
        let parents = self.store.parent_map("", "").await?;
        let dispatch_raw = self.store.dispatch_index().await?;

        // C2: operator lock roots.
        let working_epics = self.store.working_epics().await?;
        let locked = locked_roots(
            working_epics.iter().map(|(id, o)| (id.as_str(), o.as_str())),
            &|a| session_like_actor(a),
        );

        // C3: surfaces occupied by in-flight work.
        let working_surfs = self.store.working_surfaces().await?;
        let occupied = occupied_surfaces(&working_surfs);

        // Rigs on dispatch hold (rig-hold H2): excluded from the frontier so the probe matches
        // what the orchd would actually sling.
        let held = self.held_rigs(workspace).await;

        let dep_fact_fn = |id: &str| dep_facts.get(id).cloned();
        let frontier = ready_for_auto(
            rows,
            &deps_map,
            &dep_fact_fn,
            open_phase,
            tree.as_ref(),
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &held,
        );

        Ok(json!({
            "count": frontier.len(),
            "beads": frontier.iter().map(|r| json!({
                "id": r.id,
                "priority": r.priority,
                "phase": r.phase,
                "title": r.title,
            })).collect::<Vec<_>>(),
            "locked_epics": locked.iter().collect::<Vec<_>>(),
            "occupied_surfaces": occupied.iter().collect::<Vec<_>>(),
        }))
    }
}

#[async_trait]
impl DomainHandler for DispatchHandler {
    fn namespace(&self) -> &'static str {
        "dispatch"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "dispatch.probe",
                "Return the agent-dispatch frontier: beads that are ready, dispatch=auto, not operator-locked, and not surface-overlapping with in-flight work.",
                &[
                    opt("rig", "string"),
                    opt("workspace", "string"),
                ],
            ),
            descriptor(
                "dispatch.request",
                "Request the orchd scheduler to sling a real polecat for a bead (the mayor's \
                 delegation edge). Enqueues on the orchd dispatch channel; pool/host-cap \
                 arbitration still bounds how many actually sling. Errors if the dispatch \
                 channel is not wired on this server.",
                &[req("bead", "string"), opt("priority", "number")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "dispatch.probe" => {
                let rig = ctx.args.get("rig").and_then(Value::as_str);
                let workspace = ctx.workspace.or_else(|| ctx.args.get("workspace").and_then(Value::as_str));
                self.probe(rig, workspace).await
            }
            "dispatch.request" => {
                let bead = ctx
                    .args
                    .get("bead")
                    .and_then(Value::as_str)
                    .filter(|b| !b.trim().is_empty())
                    .ok_or_else(|| AppError::Validation("`bead` is required".to_string()))?;
                let priority = ctx
                    .args
                    .get("priority")
                    .and_then(Value::as_u64)
                    .unwrap_or(1) as u8;
                let Some(sink) = &self.sink else {
                    return Err(AppError::Validation(
                        "dispatch channel not wired on this server — a dispatch.request here \
                         cannot sling a polecat (set GT_CHANNEL_ROOT or GT_EVENTLOG_PG=1)"
                            .to_string(),
                    ));
                };
                let payload = DispatchPayload {
                    bead: bead.to_string(),
                    title: None,
                    priority,
                };
                let bytes = serde_json::to_vec(&payload)
                    .map_err(|e| AppError::Validation(format!("serialize dispatch request: {e}")))?;
                sink.emit(&bytes).map_err(|e| {
                    AppError::Validation(format!(
                        "dispatch request for {bead} failed to enqueue: {e}"
                    ))
                })?;
                Ok(json!({ "ok": true, "bead": bead, "queued": true, "priority": priority }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_channel::Channel;
    use tempfile::TempDir;

    /// A handler over a lazy (never-connected) pool: `dispatch.request` touches only the sink.
    fn handler() -> DispatchHandler {
        let store = DoltIssues::connect("mysql://gt@127.0.0.1:3306/unused").unwrap();
        DispatchHandler::new(Arc::new(store), None)
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// gtcore-d24661: an unwired sink must fail LOUDLY — the silent-record failure mode of the
    /// `agent.spawn` bridge is exactly what stranded the mayor's delegations.
    #[tokio::test]
    async fn request_without_sink_errors_loudly() {
        let h = handler();
        let err = h
            .dispatch("dispatch.request", ctx(json!({ "bead": "gtcore-abc123" })))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("not wired"), "unexpected error: {msg}");
    }

    #[tokio::test]
    async fn request_requires_bead() {
        let h = handler();
        let err = h
            .dispatch("dispatch.request", ctx(json!({})))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("`bead` is required"));
    }

    /// A wired File sink lands the `{bead,priority}` request as an `*.event` file on the same
    /// channel the orchd scheduler consumes.
    #[tokio::test]
    async fn request_with_file_sink_enqueues_event() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        let channel = Channel::open(&root, "dispatch").unwrap();
        let h = handler().with_dispatch_sink(Arc::new(DispatchSink::File(channel)));

        let out = h
            .dispatch(
                "dispatch.request",
                ctx(json!({ "bead": "gtcore-abc123", "priority": 0 })),
            )
            .await
            .unwrap();
        assert_eq!(out["ok"], true);
        assert_eq!(out["queued"], true);

        let events: Vec<_> = std::fs::read_dir(dir.path().join("dispatch"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "event"))
            .collect();
        assert_eq!(events.len(), 1, "expected exactly one queued .event file");
        let payload: DispatchPayload =
            serde_json::from_slice(&std::fs::read(events[0].path()).unwrap()).unwrap();
        assert_eq!(payload.bead, "gtcore-abc123");
        assert_eq!(payload.priority, 0);
    }
}
