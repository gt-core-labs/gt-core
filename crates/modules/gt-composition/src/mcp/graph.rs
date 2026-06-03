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

use gt_graphindex::{ensure_ignored, GraphError, GraphIndexer, IndexStats};
use gt_graphwarden::{MarkRefreshed, RegisterRig, WardenCommand, WardenState};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;

use super::eventlog::EventLog;
use super::util::{descriptor, now_secs, opt, req, str_arg};

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

    /// Run a warden command against the replayed state and append its events to the log,
    /// so the next replay (read or refresh) sees the new freshness state.
    fn warden_apply(&self, ws: Option<&str>, cmd: WardenCommand) -> Result<(), AppError> {
        let state = self.warden(ws)?;
        let events = cmd.execute(&state).map_err(ev_err)?;
        for ev in events {
            self.log.append(ws, ev)?;
        }
        Ok(())
    }

    /// Ensure-ignore, build-or-update `repo`'s graph, and record the refresh on the warden
    /// log. The rig must already be under custody (caller registers first). Shared by the
    /// single-rig `graph.refresh` and the batch `graph.refresh-stale`.
    async fn refresh_one(
        &self,
        ws: Option<&str>,
        rig: &str,
        repo: &std::path::Path,
    ) -> Result<(String, IndexStats), AppError> {
        let _ = ensure_ignored(repo, self.indexer.tool());
        let built = self.indexer.status(repo).await.map_err(graph_err)?.built;
        let stats = if built {
            self.indexer.update(repo, &[]).await.map_err(graph_err)?.after
        } else {
            self.indexer.build(repo).await.map_err(graph_err)?
        };
        let commit = head_commit(repo);
        self.warden_apply(
            ws,
            WardenCommand::MarkRefreshed(MarkRefreshed {
                rig: rig.to_string(),
                commit: commit.clone(),
                now_secs: now_secs(),
            }),
        )?;
        Ok((commit, stats))
    }
}

/// `git -C <repo> rev-parse --short HEAD`, or `"unknown"` if it cannot be read — the warden
/// requires a non-empty commit and an unknown checkout still refreshes the graph.
fn head_commit(repo: &std::path::Path) -> String {
    std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Map a `gt_events::AppError` (warden command path) onto the server error type.
fn ev_err(e: gt_events::AppError) -> AppError {
    AppError::Validation(e.to_string())
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

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "graph.query",
                "Ask a natural-language question against a rig's codebase knowledge graph.",
                &[req("rig", "string"), req("question", "string")],
            ),
            descriptor(
                "graph.explain",
                "Explain one node (crate/concept) in a rig's knowledge graph.",
                &[req("rig", "string"), req("node", "string")],
            ),
            descriptor("graph.status", "Report a rig's graph freshness + index stats.", &[req("rig", "string")]),
            descriptor(
                "graph.refresh",
                "Rebuild a rig's knowledge graph; optionally over an explicit repo_dir.",
                &[req("rig", "string"), opt("repo_dir", "string")],
            ),
            descriptor("graph.refresh-stale", "Rebuild every rig whose graph the warden marked stale.", &[]),
            descriptor("graph.list", "List the rigs under warden custody with their freshness.", &[]),
        ]
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
            "graph.refresh" => {
                // The custodian's write trigger: register the rig (first time, given a
                // repo_dir), ensure its artifacts are ignored, build-or-update the graph,
                // and record the refresh. Idempotent — re-running updates in place.
                let rig = str_arg(&ctx.args, "rig")?;
                let state = self.warden(ws)?;
                // repo_dir from the arg (first registration) or the existing custody record.
                let repo: PathBuf = match ctx.args.get("repo_dir").and_then(|v| v.as_str()) {
                    Some(d) => PathBuf::from(d),
                    None => state
                        .rigs
                        .get(rig)
                        .map(|g| PathBuf::from(&g.repo_dir))
                        .ok_or_else(|| {
                            AppError::Validation(format!(
                                "rig `{rig}` not under custody — pass repo_dir to register it"
                            ))
                        })?,
                };
                if !state.rigs.contains_key(rig) {
                    self.warden_apply(
                        ws,
                        WardenCommand::Register(RegisterRig {
                            rig: rig.to_string(),
                            repo_dir: repo.to_string_lossy().into_owned(),
                            now_secs: now_secs(),
                        }),
                    )?;
                }
                let (commit, stats) = self.refresh_one(ws, rig, &repo).await?;
                Ok(json!({
                    "ok": true,
                    "rig": rig,
                    "commit": commit,
                    "nodes": stats.nodes,
                    "edges": stats.edges,
                    "communities": stats.communities,
                }))
            }
            "graph.refresh-stale" => {
                // The custodian's batch tick: refresh every rig currently marked stale. A
                // loop/cron invokes this; `graph.agent.backend` is who runs the loop.
                let state = self.warden(ws)?;
                let stale: Vec<(String, PathBuf)> = state
                    .rigs
                    .values()
                    .filter(|g| g.stale)
                    .map(|g| (g.rig.clone(), PathBuf::from(&g.repo_dir)))
                    .collect();
                let mut refreshed = Vec::new();
                for (rig, repo) in stale {
                    let (commit, stats) = self.refresh_one(ws, &rig, &repo).await?;
                    refreshed.push(json!({ "rig": rig, "commit": commit, "nodes": stats.nodes }));
                }
                Ok(json!({ "ok": true, "refreshed": refreshed }))
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

    /// `graph.refresh` registers a fresh rig, builds it, marks it fresh — then it is
    /// queryable and listed as not-stale.
    #[tokio::test]
    async fn refresh_registers_builds_and_marks_fresh() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));

        // First refresh registers the rig (needs repo_dir) and builds the graph.
        let out = h
            .dispatch(
                "graph.refresh",
                ctx(json!({ "rig": "alpha", "repo_dir": "/repo/alpha" })),
            )
            .await
            .unwrap();
        assert_eq!(out["ok"], true);
        assert_eq!(out["rig"], "alpha");

        // Now listed, not stale (just refreshed).
        let list = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["rigs"][0]["stale"], false);

        // And queryable (the graph now exists).
        let q = h
            .dispatch("graph.query", ctx(json!({ "rig": "alpha", "question": "x term" })))
            .await
            .unwrap();
        assert!(q["text"].is_string());

        // Second refresh without repo_dir resolves it from custody and updates in place.
        let again = h.dispatch("graph.refresh", ctx(json!({ "rig": "alpha" }))).await.unwrap();
        assert_eq!(again["ok"], true);
    }

    /// Refreshing an unregistered rig without a repo_dir is rejected.
    #[tokio::test]
    async fn refresh_unregistered_without_repo_dir_is_rejected() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));
        let err = h.dispatch("graph.refresh", ctx(json!({ "rig": "ghost" }))).await.unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
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
