//! The corpus: the loaded set of memories, with **RECALL** (relevance-filtered
//! lookup) and **APPLY** (kind-partitioned access).

use std::fs;
use std::path::Path;

use crate::memory::{Memory, MemoryKind};

/// A scored recall hit: the memory plus the relevance score it matched at
/// (count of distinct query terms found in its name/description/body).
#[derive(Debug, Clone, Copy)]
pub struct Hit<'a> {
    pub memory: &'a Memory,
    pub score: usize,
}

/// The accumulated memory corpus — the **K** of MAPE-K.
///
/// STALENESS GUARD: memories record what was true *when written*. RECALL is a
/// point-in-time lookup; a caller acting on a recalled memory that names a file,
/// function, or flag MUST verify it still exists against live code/graph before
/// relying on it. The corpus cannot self-verify — this is the consumer's
/// contract, surfaced here so it is not forgotten.
#[derive(Debug, Clone, Default)]
pub struct Corpus {
    memories: Vec<Memory>,
}

impl Corpus {
    /// Build from already-parsed memories (the unit-test / in-memory path).
    pub fn from_memories(memories: Vec<Memory>) -> Self {
        Corpus { memories }
    }

    /// Load every `*.md` in `dir` that carries memory frontmatter. Files without
    /// a frontmatter fence (notably the `MEMORY.md` index) and unreadable files
    /// are skipped — the corpus is a best-effort load, not all-or-nothing. The
    /// resulting memories are sorted by `name` for deterministic iteration.
    pub fn from_dir(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut memories = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else { continue };
            if let Ok(m) = Memory::parse(&text) {
                memories.push(m);
            }
        }
        memories.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Corpus { memories })
    }

    /// Number of loaded memories.
    pub fn len(&self) -> usize {
        self.memories.len()
    }

    /// Whether the corpus is empty.
    pub fn is_empty(&self) -> bool {
        self.memories.is_empty()
    }

    /// Iterate all memories.
    pub fn iter(&self) -> impl Iterator<Item = &Memory> {
        self.memories.iter()
    }

    /// Look up a memory by its `name` slug (the `[[name]]` cross-link key).
    pub fn get(&self, name: &str) -> Option<&Memory> {
        self.memories.iter().find(|m| m.name == name)
    }

    /// All memories of a given kind, in corpus order.
    pub fn by_kind(&self, kind: MemoryKind) -> Vec<&Memory> {
        self.memories.iter().filter(|m| m.kind == kind).collect()
    }

    /// The hard operating rules (all `Feedback` memories) — the constraints the
    /// loop must APPLY before acting. Always loaded in full, not relevance-gated:
    /// a rule you fail to recall is a rule you violate.
    pub fn operating_rules(&self) -> Vec<&Memory> {
        self.by_kind(MemoryKind::Feedback)
    }

    /// **RECALL**: the `limit` memories most relevant to `query`, best-first.
    ///
    /// Relevance = the number of distinct query terms (lowercased alphanumeric
    /// words) that appear as substrings of the memory's name + description +
    /// body. Zero-score memories are excluded, so an unrelated query recalls
    /// nothing rather than noise. Ties break by `name` for determinism.
    pub fn recall(&self, query: &str, limit: usize) -> Vec<&Memory> {
        self.recall_scored(query, limit)
            .into_iter()
            .map(|h| h.memory)
            .collect()
    }

    /// Like [`recall`](Corpus::recall) but keeps each hit's score, for callers
    /// that want to threshold or display relevance.
    pub fn recall_scored(&self, query: &str, limit: usize) -> Vec<Hit<'_>> {
        let terms = tokenize(query);
        if terms.is_empty() || limit == 0 {
            return Vec::new();
        }
        let mut hits: Vec<Hit<'_>> = self
            .memories
            .iter()
            .filter_map(|m| {
                let hay = haystack(m);
                let score = terms.iter().filter(|t| hay.contains(t.as_str())).count();
                (score > 0).then_some(Hit { memory: m, score })
            })
            .collect();
        // Best score first; stable tiebreak on name.
        hits.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.memory.name.cmp(&b.memory.name)));
        hits.truncate(limit);
        hits
    }

    /// **WRITE-BACK**: append a newly-learned memory (meta-loop, hq-auto.7).
    /// In-memory only; persistence is the caller's job via [`Memory::to_markdown`].
    /// Replaces any existing memory with the same `name` so write-back is an
    /// upsert, not a duplicate.
    pub fn upsert(&mut self, memory: Memory) {
        match self.memories.iter_mut().find(|m| m.name == memory.name) {
            Some(slot) => *slot = memory,
            None => self.memories.push(memory),
        }
    }
}

/// Lowercased name + description + body, the text RECALL searches.
fn haystack(m: &Memory) -> String {
    let mut s = String::with_capacity(m.name.len() + m.description.len() + m.body.len() + 2);
    s.push_str(&m.name);
    s.push(' ');
    s.push_str(&m.description);
    s.push(' ');
    s.push_str(&m.body);
    s.to_lowercase()
}

/// Split a query into lowercased alphanumeric terms, dropping 1-char noise.
fn tokenize(q: &str) -> Vec<String> {
    q.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 1)
        .map(|w| w.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem(name: &str, desc: &str, kind: MemoryKind, body: &str) -> Memory {
        Memory { name: name.into(), description: desc.into(), kind, body: body.into() }
    }

    fn corpus() -> Corpus {
        Corpus::from_memories(vec![
            mem(
                "verify-build-before-commit",
                "run cargo test as its own call before merge",
                MemoryKind::Feedback,
                "Gate on N passed 0 failed.",
            ),
            mem(
                "communicate-in-spanish",
                "speak to the human in Spanish; code stays English",
                MemoryKind::Feedback,
                "Identifiers and commits remain English.",
            ),
            mem(
                "hq-mt-data-partitioning",
                "PG schema-per-workspace, not shared-table workspace_id",
                MemoryKind::Project,
                "Dolt DB-per-workspace hq_<ws>.",
            ),
            mem(
                "gt-mcp-cli-repo",
                "Rust MCP client CLI location and usage",
                MemoryKind::Reference,
                "On PATH via cargo install.",
            ),
        ])
    }

    #[test]
    fn recall_ranks_by_term_overlap() {
        let c = corpus();
        let hits = c.recall_scored("cargo test build", 5);
        assert_eq!(hits[0].memory.name, "verify-build-before-commit");
        assert!(hits[0].score >= 2);
    }

    #[test]
    fn recall_excludes_zero_score() {
        let c = corpus();
        // Nothing about kubernetes in the corpus.
        assert!(c.recall("kubernetes ingress", 10).is_empty());
    }

    #[test]
    fn recall_respects_limit_and_empty_query() {
        let c = corpus();
        assert_eq!(c.recall("workspace", 1).len(), 1);
        assert!(c.recall("", 10).is_empty());
        assert!(c.recall("workspace", 0).is_empty());
    }

    #[test]
    fn recall_matches_body_and_description() {
        let c = corpus();
        // "dolt" appears only in a body.
        let hits = c.recall("dolt", 5);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "hq-mt-data-partitioning");
    }

    #[test]
    fn operating_rules_are_the_feedback_memories() {
        let c = corpus();
        let rules: Vec<_> = c.operating_rules().iter().map(|m| m.name.clone()).collect();
        assert_eq!(rules, vec!["verify-build-before-commit", "communicate-in-spanish"]);
    }

    #[test]
    fn by_kind_partitions() {
        let c = corpus();
        assert_eq!(c.by_kind(MemoryKind::Project).len(), 1);
        assert_eq!(c.by_kind(MemoryKind::Reference).len(), 1);
    }

    #[test]
    fn upsert_replaces_same_name() {
        let mut c = corpus();
        let n = c.len();
        c.upsert(mem("verify-build-before-commit", "updated", MemoryKind::Feedback, "new body"));
        assert_eq!(c.len(), n, "upsert must not duplicate");
        assert_eq!(c.get("verify-build-before-commit").unwrap().description, "updated");
        c.upsert(mem("fresh-lesson", "a new thing", MemoryKind::Project, "x"));
        assert_eq!(c.len(), n + 1);
    }

    #[test]
    fn get_finds_by_name() {
        let c = corpus();
        assert!(c.get("gt-mcp-cli-repo").is_some());
        assert!(c.get("nope").is_none());
    }
}
