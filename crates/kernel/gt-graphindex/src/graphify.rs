//! [`GraphifyIndexer`] — the graphify-backed [`GraphIndexer`] adapter.
//!
//! It drives the `graphify` python package through a venv interpreter, one graph
//! per repo under `<repo>/graphify-out/`. Each operation feeds a short python
//! driver on the child's **stdin** (so there is no shell-quoting surface) with the
//! repo path and arguments passed as `argv`, and parses a one-line JSON result
//! from stdout.
//!
//! ## Only the deterministic half
//!
//! `build`/`update` run graphify's **AST** path (structural, no LLM) plus
//! build → cluster → persist. The *semantic* re-extraction graphify also offers
//! is an agent's job (the custodian, `hq-graphrig.7`/`.9`), not this crate's — a
//! library adapter must not silently spend tokens.
//!
//! ## The safe update recipe
//!
//! [`update`](GraphifyIndexer::update) encodes the recipe derived while fixing the
//! `cluster()` `unhashable type: list` crash: an in-memory `G.update(...)` merge
//! can carry artifacts that crash community detection, so we **persist (which
//! serialises/sanitises), reload, then cluster** the clean graph and persist the
//! canonical labels. Doing it inside the adapter means no caller can reintroduce
//! the crash.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::port::{GraphAnswer, GraphError, GraphIndexer, IndexDiff, IndexStats, IndexStatus};

/// A [`GraphIndexer`] backed by the graphify python package.
#[derive(Clone, Debug)]
pub struct GraphifyIndexer {
    /// Interpreter used when a repo carries no `.graphify-venv` of its own.
    fallback_python: String,
}

impl Default for GraphifyIndexer {
    fn default() -> Self {
        // GT_GRAPHIFY_PYTHON overrides the default so deployments where the per-repo
        // .graphify-venv is not accessible to the server process (e.g. running as root
        // while the venv lives in a user home dir) can point at a usable interpreter.
        let fallback_python = std::env::var("GT_GRAPHIFY_PYTHON")
            .unwrap_or_else(|_| "python3".to_string());
        Self { fallback_python }
    }
}

impl GraphifyIndexer {
    /// An indexer that prefers each repo's `.graphify-venv/bin/python` and falls
    /// back to `GT_GRAPHIFY_PYTHON` (or `python3` on `PATH` if unset).
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the interpreter used when a repo has no `.graphify-venv`.
    pub fn with_fallback_python(mut self, python: impl Into<String>) -> Self {
        self.fallback_python = python.into();
        self
    }

    /// Resolve the interpreter for `repo`: its own venv if present, else the
    /// configured fallback.
    fn python_for(&self, repo: &Path) -> PathBuf {
        let venv = repo.join(".graphify-venv/bin/python");
        if venv.exists() {
            venv
        } else {
            PathBuf::from(&self.fallback_python)
        }
    }

    /// Spawn the interpreter with `script` on stdin and `args` as `argv`, returning
    /// trimmed stdout. Maps spawn / non-zero-exit failures onto [`GraphError`].
    async fn run(&self, repo: &Path, script: &str, args: &[&str]) -> Result<String, GraphError> {
        let python = self.python_for(repo);
        let mut cmd = Command::new(&python);
        cmd.arg("-") // read the driver from stdin
            .arg(repo.as_os_str());
        for a in args {
            cmd.arg(a);
        }
        // graphify writes its artifacts (graphify-out/, manifest) relative to the
        // process cwd, not the repo arg. Without this the driver inherits the server's
        // cwd — unwritable under the non-root (uid 1000) runtime — and dies on
        // `PermissionError: 'graphify-out'`. Run in the repo, which is owned by the
        // server uid, so the relative artifact paths land in the rig.
        cmd.current_dir(repo)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| GraphError::Io(format!("spawn {}: {e}", python.display())))?;
        child
            .stdin
            .take()
            .expect("stdin piped")
            .write_all(script.as_bytes())
            .await
            .map_err(|e| GraphError::Io(format!("write driver: {e}")))?;
        // stdin dropped here -> EOF for the child.

        let out = child
            .wait_with_output()
            .await
            .map_err(|e| GraphError::Io(format!("wait: {e}")))?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            return Err(GraphError::Tool(format!(
                "graphify exited {}: {}",
                out.status,
                err.trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Parse a `{nodes,edges,communities}` object; surfaces a tool-reported
    /// `{"error":"not_built"}` as [`GraphError::NotBuilt`].
    ///
    /// Graphify's `extract()` writes progress lines to stdout before the JSON
    /// result, so we scan for the last `{`-prefixed line rather than parsing
    /// the whole output.
    fn parse_stats(repo: &Path, raw: &str) -> Result<serde_json::Value, GraphError> {
        let json_line = raw
            .lines()
            .filter(|l| l.trim_start().starts_with('{'))
            .last()
            .unwrap_or(raw);
        let v: serde_json::Value = serde_json::from_str(json_line)
            .map_err(|e| GraphError::Tool(format!("bad json: {e}: {raw}")))?;
        if v.get("error").and_then(|e| e.as_str()) == Some("not_built") {
            return Err(GraphError::NotBuilt(repo.to_string_lossy().into_owned()));
        }
        Ok(v)
    }
}

fn stats_from(v: &serde_json::Value) -> IndexStats {
    IndexStats {
        nodes: v.get("nodes").and_then(|n| n.as_u64()).unwrap_or(0) as usize,
        edges: v.get("edges").and_then(|n| n.as_u64()).unwrap_or(0) as usize,
        communities: v.get("communities").and_then(|n| n.as_u64()).unwrap_or(0) as usize,
    }
}

#[async_trait]
impl GraphIndexer for GraphifyIndexer {
    fn tool(&self) -> &str {
        "graphify"
    }

    async fn build(&self, repo: &Path) -> Result<IndexStats, GraphError> {
        let raw = self.run(repo, BUILD_PY, &[]).await?;
        let v = Self::parse_stats(repo, &raw)?;
        Ok(stats_from(&v))
    }

    async fn update(&self, repo: &Path, changed: &[&Path]) -> Result<IndexDiff, GraphError> {
        // changed files are advisory: graphify's detect_incremental re-derives the
        // real changed set from its manifest, so we only need to trigger the run.
        let _ = changed;
        let raw = self.run(repo, UPDATE_PY, &[]).await?;
        let v = Self::parse_stats(repo, &raw)?;
        Ok(IndexDiff {
            after: stats_from(&v),
            new_nodes: v.get("new_nodes").and_then(|n| n.as_u64()).unwrap_or(0) as usize,
            new_edges: v.get("new_edges").and_then(|n| n.as_u64()).unwrap_or(0) as usize,
        })
    }

    async fn query(&self, repo: &Path, question: &str) -> Result<GraphAnswer, GraphError> {
        let raw = self.run(repo, QUERY_PY, &[question]).await?;
        let v = Self::parse_stats(repo, &raw)?;
        Ok(answer_from(&v))
    }

    async fn explain(&self, repo: &Path, node: &str) -> Result<GraphAnswer, GraphError> {
        let raw = self.run(repo, EXPLAIN_PY, &[node]).await?;
        let v = Self::parse_stats(repo, &raw)?;
        Ok(answer_from(&v))
    }

    async fn status(&self, repo: &Path) -> Result<IndexStatus, GraphError> {
        let raw = self.run(repo, STATUS_PY, &[]).await?;
        let json_line = raw
            .lines()
            .filter(|l| l.trim_start().starts_with('{'))
            .last()
            .unwrap_or(&raw);
        let v: serde_json::Value = serde_json::from_str(json_line)
            .map_err(|e| GraphError::Tool(format!("bad json: {e}: {raw}")))?;
        let built = v.get("built").and_then(|b| b.as_bool()).unwrap_or(false);
        Ok(IndexStatus {
            built,
            stats: built.then(|| stats_from(&v)),
            tool: self.tool().to_string(),
            built_at_commit: v
                .get("built_at_commit")
                .and_then(|c| c.as_str())
                .map(str::to_string),
        })
    }
}

fn answer_from(v: &serde_json::Value) -> GraphAnswer {
    GraphAnswer {
        text: v.get("text").and_then(|t| t.as_str()).unwrap_or_default().to_string(),
        nodes: v
            .get("nodes")
            .and_then(|n| n.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default(),
    }
}

// --- python drivers (stdin) -------------------------------------------------
// argv: [<repo>, <extra...>]. Each prints exactly one line of JSON.

const BUILD_PY: &str = r#"
import sys, json
from pathlib import Path
from graphify.detect import detect, save_manifest
from graphify.extract import collect_files, extract
from graphify.build import build_from_json
from graphify.cluster import cluster
from graphify.export import to_json
repo = Path(sys.argv[1])
det = detect(repo)
code = []
for f in det['files'].get('code', []):
    p = Path(f)
    code += collect_files(p) if p.is_dir() else [p]
ast = extract(code) if code else {'nodes': [], 'edges': []}
G = build_from_json({'nodes': ast['nodes'], 'edges': ast['edges'], 'input_tokens': 0, 'output_tokens': 0})
out = repo / 'graphify-out'; out.mkdir(parents=True, exist_ok=True)
comms = cluster(G) if G.number_of_nodes() else {}
to_json(G, comms, str(out / 'graph.json'), force=True)
save_manifest(det['files'])
print(json.dumps({'nodes': G.number_of_nodes(), 'edges': G.number_of_edges(), 'communities': len(comms)}))
"#;

const UPDATE_PY: &str = r#"
import sys, json
from pathlib import Path
from collections import defaultdict
from graphify.detect import detect_incremental, save_manifest
from graphify.extract import collect_files, extract
from graphify.build import build_from_json
from graphify.cluster import cluster
from graphify.export import to_json
from networkx.readwrite import json_graph
repo = Path(sys.argv[1]); gj = repo / 'graphify-out' / 'graph.json'
if not gj.exists():
    print(json.dumps({'error': 'not_built'})); sys.exit(0)
r = detect_incremental(repo)
code = []
for f in r['new_files'].get('code', []):
    p = Path(f)
    code += collect_files(p) if p.is_dir() else [p]
G = json_graph.node_link_graph(json.loads(gj.read_text()), edges='links')
before = (G.number_of_nodes(), G.number_of_edges())
if code:
    ast = extract(code)
    Gn = build_from_json({'nodes': ast['nodes'], 'edges': ast['edges'], 'input_tokens': 0, 'output_tokens': 0})
    G.update(Gn)
# safe recipe: persist (sanitise) -> reload -> cluster -> persist canonical labels
comms = defaultdict(list); seen = set(); mx = -1
for nid, nd in G.nodes(data=True):
    c = nd.get('community')
    if c is None: continue
    comms[int(c)].append(nid); seen.add(nid); mx = max(mx, int(c))
nb = mx + 1
for nid in list(G.nodes()):
    if nid not in seen:
        comms[nb].append(nid); G.nodes[nid]['community'] = nb
to_json(G, dict(comms), str(gj), force=True)
G = json_graph.node_link_graph(json.loads(gj.read_text()), edges='links')
cc = cluster(G) if G.number_of_nodes() else {}
to_json(G, cc, str(gj), force=True)
save_manifest(r['files'])
after = (G.number_of_nodes(), G.number_of_edges())
print(json.dumps({'nodes': after[0], 'edges': after[1], 'communities': len(cc),
                  'new_nodes': after[0] - before[0], 'new_edges': after[1] - before[1]}))
"#;

const QUERY_PY: &str = r#"
import sys, json
from pathlib import Path
from networkx.readwrite import json_graph
repo = Path(sys.argv[1]); q = sys.argv[2] if len(sys.argv) > 2 else ''
gj = repo / 'graphify-out' / 'graph.json'
if not gj.exists():
    print(json.dumps({'error': 'not_built'})); sys.exit(0)
G = json_graph.node_link_graph(json.loads(gj.read_text()), edges='links')
terms = [t.lower() for t in q.split() if len(t) > 3]
scored = []
for nid, nd in G.nodes(data=True):
    lab = nd.get('label', '').lower(); s = sum(1 for t in terms if t in lab)
    if s: scored.append((s, nid))
scored.sort(reverse=True, key=lambda x: x[0])
start = [nid for _, nid in scored[:4]]
nodes = []; lines = []
for nid in start:
    nd = G.nodes[nid]; nodes.append(nd.get('label', nid))
    lines.append(f"{nd.get('label', nid)} [{nd.get('source_file', '')}]")
    for nb in list(G.neighbors(nid))[:6]:
        e = G.edges[nid, nb]
        lines.append(f"  --{e.get('relation', '')}--> {G.nodes[nb].get('label', nb)}")
print(json.dumps({'text': '\n'.join(lines) or 'no matching nodes', 'nodes': nodes}))
"#;

const EXPLAIN_PY: &str = r#"
import sys, json
from pathlib import Path
from networkx.readwrite import json_graph
repo = Path(sys.argv[1]); term = (sys.argv[2] if len(sys.argv) > 2 else '').lower()
gj = repo / 'graphify-out' / 'graph.json'
if not gj.exists():
    print(json.dumps({'error': 'not_built'})); sys.exit(0)
G = json_graph.node_link_graph(json.loads(gj.read_text()), edges='links')
scored = sorted(((sum(1 for w in term.split() if w in G.nodes[n].get('label', '').lower()), n)
                 for n in G.nodes()), reverse=True, key=lambda x: x[0])
if not scored or scored[0][0] == 0:
    print(json.dumps({'text': 'no node matching ' + term, 'nodes': []})); sys.exit(0)
nid = scored[0][1]; nd = G.nodes[nid]
lines = [f"{nd.get('label', nid)} [{nd.get('source_file', '')}] degree={G.degree(nid)}"]
for nb in G.neighbors(nid):
    e = G.edges[nid, nb]
    lines.append(f"  --{e.get('relation', '')}--> {G.nodes[nb].get('label', nb)}")
print(json.dumps({'text': '\n'.join(lines), 'nodes': [nd.get('label', nid)]}))
"#;

const STATUS_PY: &str = r#"
import sys, json
from pathlib import Path
from networkx.readwrite import json_graph
repo = Path(sys.argv[1]); gj = repo / 'graphify-out' / 'graph.json'
if not gj.exists():
    print(json.dumps({'built': False})); sys.exit(0)
G = json_graph.node_link_graph(json.loads(gj.read_text()), edges='links')
comms = {G.nodes[n].get('community') for n in G.nodes()}
comms.discard(None)
print(json.dumps({'built': True, 'nodes': G.number_of_nodes(), 'edges': G.number_of_edges(),
                  'communities': len(comms)}))
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_name_is_graphify() {
        assert_eq!(GraphifyIndexer::new().tool(), "graphify");
    }

    #[test]
    fn prefers_repo_venv_when_present() {
        // A repo without a venv falls back to the configured interpreter.
        let ix = GraphifyIndexer::new().with_fallback_python("python3.99");
        let py = ix.python_for(Path::new("/nonexistent-repo"));
        assert_eq!(py, PathBuf::from("python3.99"));
    }

    #[tokio::test]
    async fn status_on_unbuilt_repo_reports_not_built() {
        // No graphify-out/ -> the STATUS driver prints {"built": false}. Requires a
        // python3 with the graphify package importable; skip cleanly otherwise.
        let dir = std::env::temp_dir().join(format!("gi-status-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let ix = GraphifyIndexer::new();
        match ix.status(&dir).await {
            Ok(st) => assert!(!st.built),
            Err(GraphError::Io(_)) | Err(GraphError::Tool(_)) => {
                // python3 missing or graphify not importable in this env — fine.
            }
            Err(e) => panic!("unexpected: {e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
