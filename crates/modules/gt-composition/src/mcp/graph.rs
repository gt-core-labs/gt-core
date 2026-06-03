//! `graph.*` domain dispatch — read-only knowledge-graph queries (`hq-graphrig.10`).
//!
//! This is the surface other agents use to **consult** a rig's codebase graph for
//! context; only the warden writes (freshness state + the index itself). The
//! handler is tool-neutral: it talks to a [`GraphIndexer`] trait object, so the
//! backing tool (graphify today) is swapped by constructing a different adapter at
//! the composition edge — no change here.
//!
//! Per-rig routing: the repo a query runs against is resolved by replaying the
//! warden's events from the workspace log into a [`WardenState`] and reading the
//! rig's `repo_dir`. So a `graph.query` only works for a rig the warden has under
//! custody (`graphwarden.rig-registered.v1`) — exactly the rigs whose graphs exist.
//!
//! Tools (read-only): `graph.query`, `graph.explain`, `graph.status`, `graph.list`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_graphindex::{GraphError, GraphIndexer};
use gt_graphwarden::WardenState;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::str_arg;

/// The warden event-log kind prefix the handler replays to resolve rigs.
const NS: &str = "graphwarden.";

/// Read-only handler for the `graph.*` tool namespace.
pub struct GraphHandler {
    log: Arc<EventLog>,
    indexer: Arc<dyn GraphIndexer>,
}

impl GraphHandler {
    /// Wrap the per-workspace event log + the active graph indexer.
    pub fn new(log: Arc<EventLog>, indexer: Arc<dyn GraphIndexer>) -> Self {
        Self { log, indexer }
    }

    /// Replay the warden events for `ws` into a fresh [`WardenState`].
    fn warden(&self, ws: Option<&str>) -> Result<WardenState, AppError> {
        self.log.replay_domain(ws, NS, WardenState::default(), |s, e| {
            let _ = s.apply(e);
        })
    }

    /// The on-disk checkout for `rig`, or `NotFound` if the warden has no custody of it.
    fn repo_dir(&self, ws: Option<&str>, rig: &str) -> Result<PathBuf, AppError> {
        let state = self.warden(ws)?;
        state
            .rigs
            .get(rig)
            .map(|g| PathBuf::from(&g.repo_dir))
            .ok_or_else(|| AppError::NotFound(format!("no graph custody for rig `{rig}`")))
    }
}

/// Map an indexer failure onto the MCP-server error type.
fn graph_err(e: GraphError) -> AppError {
    match e {
        GraphError::NotBuilt(r) => AppError::NotFound(format!("graph not built: {r}")),
        other => AppError::Validation(other.to_string()),
    }
}

#[async_trait]
impl DomainHandler for GraphHandler {
    fn namespace(&self) -> &'static str {
        "graph"
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "graph.query" => {
                let rig = str_arg(&ctx.args, "rig")?;
                let question = str_arg(&ctx.args, "question")?;
                let repo = self.repo_dir(ws, rig)?;
                let ans = self.indexer.query(&repo, question).await.map_err(graph_err)?;
                Ok(json!({ "text": ans.text, "nodes": ans.nodes }))
            }
            "graph.explain" => {
                let rig = str_arg(&ctx.args, "rig")?;
                let node = str_arg(&ctx.args, "node")?;
                let repo = self.repo_dir(ws, rig)?;
                let ans = self.indexer.explain(&repo, node).await.map_err(graph_err)?;
                Ok(json!({ "text": ans.text, "nodes": ans.nodes }))
            }
            "graph.status" => {
                let rig = str_arg(&ctx.args, "rig")?;
                let repo = self.repo_dir(ws, rig)?;
                let st = self.indexer.status(&repo).await.map_err(graph_err)?;
                Ok(json!({
                    "built": st.built,
                    "tool": st.tool,
                    "nodes": st.stats.map(|s| s.nodes),
                    "edges": st.stats.map(|s| s.edges),
                    "communities": st.stats.map(|s| s.communities),
                    "built_at_commit": st.built_at_commit,
                }))
            }
            "graph.list" => {
                let state = self.warden(ws)?;
                let rigs: Vec<Value> = state
                    .rigs
                    .values()
                    .map(|g| {
                        json!({
                            "rig": g.rig,
                            "repo_dir": g.repo_dir,
                            "stale": g.stale,
                            "pending_changes": g.pending_changes,
                            "last_indexed_commit": g.last_indexed_commit,
                        })
                    })
                    .collect();
                Ok(json!({ "rigs": rigs }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_graphindex::InMemoryGraphIndexer;
    use gt_graphwarden::WardenEvent;
    use tempfile::TempDir;

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx { workspace: None, actor: "tester", args }
    }

    /// Seed the workspace log with a rig under custody, then query/list/status it.
    #[tokio::test]
    async fn query_resolves_rig_and_answers() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        // The warden registered rig `alpha` pointing at a repo dir.
        log.append(
            None,
            WardenEvent::RigRegistered {
                rig: "alpha".into(),
                repo_dir: "/repo/alpha".into(),
                now_secs: 1,
            },
        )
        .unwrap();

        let indexer = Arc::new(InMemoryGraphIndexer::new());
        // InMemory needs a built graph for that repo to answer.
        indexer.build(std::path::Path::new("/repo/alpha")).await.unwrap();

        let h = GraphHandler::new(log, indexer);

        let out = h
            .dispatch("graph.query", ctx(json!({ "rig": "alpha", "question": "where is auth" })))
            .await
            .unwrap();
        assert!(out["text"].as_str().unwrap().contains("where is auth"));

        let list = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["rigs"].as_array().unwrap().len(), 1);
        assert_eq!(list["rigs"][0]["rig"], "alpha");

        let st = h.dispatch("graph.status", ctx(json!({ "rig": "alpha" }))).await.unwrap();
        assert_eq!(st["built"], true);
        assert_eq!(st["tool"], "inmemory");
    }

    /// A rig the warden never registered has no custody → NotFound.
    #[tokio::test]
    async fn query_unknown_rig_is_not_found() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));
        let err = h
            .dispatch("graph.query", ctx(json!({ "rig": "ghost", "question": "x" })))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }
}
