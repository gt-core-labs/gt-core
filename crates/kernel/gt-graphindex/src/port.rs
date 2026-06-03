//! The [`GraphIndexer`] port and its tool-neutral value types.

use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Size of a built graph, reported by [`GraphIndexer::build`] /
/// [`GraphIndexer::status`]. Tool-neutral counts — every adapter maps its own
/// artifact onto these three numbers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexStats {
    /// Total nodes in the graph.
    pub nodes: usize,
    /// Total edges in the graph.
    pub edges: usize,
    /// Number of detected communities/clusters (0 if the tool has no notion).
    pub communities: usize,
}

/// What an incremental [`GraphIndexer::update`] changed: the totals after the run
/// plus how much was newly added. `new_nodes == 0` is normal when edits only
/// touch the bodies of already-indexed symbols.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDiff {
    /// Graph size after the update.
    pub after: IndexStats,
    /// Nodes added by this update.
    pub new_nodes: usize,
    /// Edges added by this update.
    pub new_edges: usize,
}

/// A read answer from [`GraphIndexer::query`] / [`GraphIndexer::explain`]:
/// human-readable prose plus the node labels the answer was grounded on (for
/// citation / further traversal).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphAnswer {
    /// Prose answer, ready to hand to a querying agent as context.
    pub text: String,
    /// Node labels the answer cites, in relevance order.
    pub nodes: Vec<String>,
}

/// Freshness snapshot of a repo's graph: whether it exists, its size, the tool
/// that built it, and the commit it was built at (when the adapter records one).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexStatus {
    /// Whether a graph artifact currently exists for the repo.
    pub built: bool,
    /// Size, if built.
    pub stats: Option<IndexStats>,
    /// The tool that produced it (mirrors [`GraphIndexer::tool`]).
    pub tool: String,
    /// The repo commit the graph was last built/updated at, if recorded.
    pub built_at_commit: Option<String>,
}

/// Why a [`GraphIndexer`] operation failed. `#[non_exhaustive]` so adapters can
/// surface new failure shapes without breaking matches.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GraphError {
    /// No graph has been built for the repo yet — call [`GraphIndexer::build`]
    /// before querying or updating.
    #[error("no graph built for repo at {0}")]
    NotBuilt(String),
    /// The underlying tool ran but reported failure (non-zero exit, parse error).
    #[error("graph tool error: {0}")]
    Tool(String),
    /// An I/O failure reaching the repo or the artifact.
    #[error("graph io error: {0}")]
    Io(String),
    /// The adapter does not support this operation (e.g. a read-only tool).
    #[error("operation unsupported by {0}")]
    Unsupported(String),
}

/// Builds and queries a codebase knowledge graph for a single repository.
///
/// Implementors are I/O adapters held behind `dyn` at the composition edge (the
/// reactor in `hq-graphrig.8`, the MCP surface in `hq-graphrig.10`). Every method
/// takes the repo root explicitly so one indexer instance serves every rig — the
/// per-repo artifact location is the adapter's own convention.
#[async_trait]
pub trait GraphIndexer: Send + Sync {
    /// Stable name of the backing tool (e.g. `"graphify"`). Used in diagnostics,
    /// in [`IndexStatus::tool`], and to drive the ignore-pattern set
    /// (`hq-graphrig.3`).
    fn tool(&self) -> &str;

    /// VCS-ignore patterns for this tool's artifacts, so a repo never tracks its
    /// graph. The default reads the active tool name; an adapter rarely overrides
    /// it. This is the source the per-rig propagation consumes (`hq-graphrig.11`).
    fn ignore_patterns(&self) -> Vec<String> {
        crate::artifacts::patterns_for(self.tool())
    }

    /// Build the full graph for `repo` from scratch. Returns the resulting size.
    async fn build(&self, repo: &Path) -> Result<IndexStats, GraphError>;

    /// Incrementally re-index only `changed` files into an existing graph.
    /// Returns [`GraphError::NotBuilt`] if [`build`](Self::build) never ran.
    async fn update(&self, repo: &Path, changed: &[&Path]) -> Result<IndexDiff, GraphError>;

    /// Answer a free-text question from the graph, grounded in real nodes.
    async fn query(&self, repo: &Path, question: &str) -> Result<GraphAnswer, GraphError>;

    /// Explain a single node and its connections.
    async fn explain(&self, repo: &Path, node: &str) -> Result<GraphAnswer, GraphError>;

    /// Report whether a graph exists for `repo` and, if so, its size.
    async fn status(&self, repo: &Path) -> Result<IndexStatus, GraphError>;
}
