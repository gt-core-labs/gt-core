//! [`InMemoryGraphIndexer`] — the dependency-free test double for [`GraphIndexer`].
//!
//! It keeps a per-repo graph size in a `Mutex` and fabricates deterministic
//! answers, so consumers (the warden, the reactor, the MCP surface) can be unit
//! tested without a real tool, a python venv, or any I/O. It is also the literal
//! "swap" target the epic's independence test uses: wire this in place of the
//! graphify adapter and everything downstream still compiles and runs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::port::{GraphAnswer, GraphError, GraphIndexer, IndexDiff, IndexStats, IndexStatus};

/// In-memory [`GraphIndexer`]. `build` seeds a small graph; `update` grows it by
/// the number of changed files; `query`/`explain` echo deterministically.
#[derive(Debug, Default)]
pub struct InMemoryGraphIndexer {
    graphs: Mutex<HashMap<String, IndexStats>>,
}

impl InMemoryGraphIndexer {
    /// A fresh, empty indexer holding no graphs.
    pub fn new() -> Self {
        Self::default()
    }

    fn key(repo: &Path) -> String {
        repo.to_string_lossy().into_owned()
    }
}

#[async_trait]
impl GraphIndexer for InMemoryGraphIndexer {
    fn tool(&self) -> &str {
        "inmemory"
    }

    async fn build(&self, repo: &Path) -> Result<IndexStats, GraphError> {
        let stats = IndexStats { nodes: 1, edges: 0, communities: 1 };
        self.graphs.lock().unwrap().insert(Self::key(repo), stats);
        Ok(stats)
    }

    async fn update(&self, repo: &Path, changed: &[&Path]) -> Result<IndexDiff, GraphError> {
        let mut graphs = self.graphs.lock().unwrap();
        let stats = graphs
            .get_mut(&Self::key(repo))
            .ok_or_else(|| GraphError::NotBuilt(Self::key(repo)))?;
        let added = changed.len();
        stats.nodes += added;
        stats.edges += added;
        Ok(IndexDiff { after: *stats, new_nodes: added, new_edges: added })
    }

    async fn query(&self, repo: &Path, question: &str) -> Result<GraphAnswer, GraphError> {
        let graphs = self.graphs.lock().unwrap();
        if !graphs.contains_key(&Self::key(repo)) {
            return Err(GraphError::NotBuilt(Self::key(repo)));
        }
        Ok(GraphAnswer {
            text: format!("inmemory answer for: {question}"),
            nodes: Vec::new(),
        })
    }

    async fn explain(&self, repo: &Path, node: &str) -> Result<GraphAnswer, GraphError> {
        let graphs = self.graphs.lock().unwrap();
        if !graphs.contains_key(&Self::key(repo)) {
            return Err(GraphError::NotBuilt(Self::key(repo)));
        }
        Ok(GraphAnswer {
            text: format!("inmemory explanation of: {node}"),
            nodes: vec![node.to_string()],
        })
    }

    async fn status(&self, repo: &Path) -> Result<IndexStatus, GraphError> {
        let graphs = self.graphs.lock().unwrap();
        let stats = graphs.get(&Self::key(repo)).copied();
        Ok(IndexStatus {
            built: stats.is_some(),
            stats,
            tool: self.tool().to_string(),
            built_at_commit: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn repo() -> PathBuf {
        PathBuf::from("/tmp/rig-a")
    }

    #[tokio::test]
    async fn query_before_build_is_not_built() {
        let ix = InMemoryGraphIndexer::new();
        let err = ix.query(&repo(), "anything").await.unwrap_err();
        assert!(matches!(err, GraphError::NotBuilt(_)));
        assert!(!ix.status(&repo()).await.unwrap().built);
    }

    #[tokio::test]
    async fn build_then_update_grows_graph() {
        let ix = InMemoryGraphIndexer::new();
        let built = ix.build(&repo()).await.unwrap();
        assert_eq!(built.nodes, 1);

        let a = PathBuf::from("src/a.rs");
        let b = PathBuf::from("src/b.rs");
        let diff = ix.update(&repo(), &[a.as_path(), b.as_path()]).await.unwrap();
        assert_eq!(diff.new_nodes, 2);
        assert_eq!(diff.after.nodes, 3);

        let st = ix.status(&repo()).await.unwrap();
        assert!(st.built);
        assert_eq!(st.stats.unwrap().nodes, 3);
        assert_eq!(st.tool, "inmemory");
    }

    #[tokio::test]
    async fn update_unbuilt_repo_errors() {
        let ix = InMemoryGraphIndexer::new();
        let f = PathBuf::from("src/a.rs");
        let err = ix.update(&repo(), &[f.as_path()]).await.unwrap_err();
        assert!(matches!(err, GraphError::NotBuilt(_)));
    }

    #[tokio::test]
    async fn explain_cites_the_node() {
        let ix = InMemoryGraphIndexer::new();
        ix.build(&repo()).await.unwrap();
        let ans = ix.explain(&repo(), "HookContext").await.unwrap();
        assert_eq!(ans.nodes, vec!["HookContext".to_string()]);
    }
}
