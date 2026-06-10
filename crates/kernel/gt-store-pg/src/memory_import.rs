//! Corpus `.md` → `memories` table importer (hq-memory-mcp.5).
//!
//! Seeds (and re-seeds) the per-workspace semantic memory store from the file-based memory
//! corpus the repo already keeps (`…/memory/*.md`). For a human-assisted Claude those `.md`
//! files ARE the memory; for the autonomous loop the same lessons must live in the queryable
//! [`MemoryRepository`] so they recall BY MEANING. This importer is the bridge: point it at a
//! corpus dir and it upserts every memory into the table and (optionally) embeds it.
//!
//! Two properties make it safe to run on every boot and to re-run after a model swap:
//!
//! - **Idempotent** — [`MemoryRepository::upsert`] is keyed by `name`, so re-importing the
//!   same corpus replaces in place and never duplicates. Counting upserts therefore counts
//!   distinct memories, not invocations.
//! - **Re-seedable / re-embeddable** — pass an [`Embedder`] and every memory's embedding is
//!   (re)written, so changing the embedding model is just a re-import. Embedding is
//!   *best-effort*: a single embed failure is recorded and skipped, never aborting the rest
//!   of the import (a half-populated store still recalls on the full-text side).
//!
//! The lives in `gt-store-pg` rather than `gt-memory` because it needs the repository and the
//! embedder seams; `gt-memory` is a pure-`std` domain crate and must stay free of PG/embed
//! dependencies. It depends only on the `MemoryRepository` / `Embedder` *traits*, so it needs
//! no concrete Postgres or ML engine to compile or to unit-test.

#![cfg(feature = "pg")]

use gt_docs_embed::Embedder;
use gt_memory::Corpus;

use crate::memory_store::{MemoryError, MemoryRepository, NewMemory};

/// `created_by` attribution stamped on every memory this importer writes.
const IMPORTER_ACTOR: &str = "corpus-import";

/// Outcome of an [`import_corpus`] run — what the caller logs to know the seed worked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Memories upserted into the store (idempotent by name): the corpus size, minus any
    /// non-memory files `Corpus::from_dir` already skips (notably the `MEMORY.md` index).
    pub upserted: usize,
    /// Memories whose embedding was written (0 when no embedder was supplied).
    pub embedded: usize,
    /// Memories whose embedding step failed and was skipped (best-effort): `(name, error)`.
    /// A non-empty list does not fail the import — the rows are still present and full-text
    /// recallable; re-running with a working embedder fills the gaps.
    pub embed_failures: Vec<(String, String)>,
}

/// Load the corpus at `dir` and upsert every memory into `repo`, optionally embedding each.
///
/// For each [`Memory`](gt_memory::Memory) in [`Corpus::from_dir`]: `repo.upsert(NewMemory)`
/// (idempotent by `name`); then, if `embedder` is `Some`, embed `name + description + body`
/// and `repo.set_embedding(name, vec)`. The full text — not just the description — is embedded
/// so vector recall sees the body of the lesson, matching how the corpus is searched.
///
/// Upsert errors abort (a store that can't write is unrecoverable); embed errors do not — they
/// are collected into [`ImportReport::embed_failures`] and the import continues. Re-running is
/// safe and is the supported way to re-seed after an embedding-model change.
pub async fn import_corpus(
    dir: impl AsRef<std::path::Path>,
    repo: &dyn MemoryRepository,
    embedder: Option<&dyn Embedder>,
) -> Result<ImportReport, ImportError> {
    let corpus = Corpus::from_dir(dir).map_err(ImportError::Load)?;
    let mut report = ImportReport::default();

    for memory in corpus.iter() {
        repo.upsert(NewMemory::from_memory(memory, IMPORTER_ACTOR))
            .await
            .map_err(ImportError::Upsert)?;
        report.upserted += 1;

        let Some(embedder) = embedder else { continue };
        // Embed the full lesson (name + description + body) so vector recall sees its content.
        let text = format!("{} {} {}", memory.name, memory.description, memory.body);
        match embedder.embed(&text).await {
            Ok(vec) => match repo.set_embedding(&memory.name, vec).await {
                Ok(()) => report.embedded += 1,
                Err(e) => report.embed_failures.push((memory.name.clone(), e.to_string())),
            },
            Err(e) => report.embed_failures.push((memory.name.clone(), e.to_string())),
        }
    }

    Ok(report)
}

/// Why an import run aborted (as opposed to a per-memory embed failure, which is collected and
/// non-fatal in [`ImportReport::embed_failures`]).
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// The corpus directory could not be read.
    #[error("could not load corpus dir: {0}")]
    Load(#[source] std::io::Error),
    /// A memory could not be upserted — the store is unwritable, so the run cannot continue.
    #[error("could not upsert memory: {0}")]
    Upsert(#[source] MemoryError),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sqlx::types::chrono::Utc;

    use super::*;
    use crate::memory_store::MemoryRow;

    /// An in-memory [`MemoryRepository`] for the importer test — no Postgres. Models the one
    /// property the importer relies on: `upsert` is keyed by `name` (replace, never duplicate).
    #[derive(Default)]
    struct FakeRepo {
        rows: Mutex<HashMap<String, MemoryRow>>,
        embeddings: Mutex<HashMap<String, Vec<f32>>>,
    }

    impl FakeRepo {
        fn len(&self) -> usize {
            self.rows.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl MemoryRepository for FakeRepo {
        async fn upsert(&self, mem: NewMemory) -> Result<MemoryRow, MemoryError> {
            let mut rows = self.rows.lock().unwrap();
            let version = rows.get(&mem.name).map_or(0, |r| r.version + 1);
            let row = MemoryRow {
                name: mem.name.clone(),
                description: mem.description,
                kind: mem.kind,
                body: mem.body,
                version,
                created_by: mem.created_by,
                updated_at: Utc::now(),
            };
            rows.insert(mem.name, row.clone());
            Ok(row)
        }

        async fn get(&self, name: &str) -> Result<Option<MemoryRow>, MemoryError> {
            Ok(self.rows.lock().unwrap().get(name).cloned())
        }

        async fn by_kind(&self, kind: &str) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(self.rows.lock().unwrap().values().filter(|r| r.kind == kind).cloned().collect())
        }

        async fn list(&self) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(self.rows.lock().unwrap().values().cloned().collect())
        }

        async fn recall(
            &self,
            _query: &str,
            _kind: Option<&str>,
            _limit: i64,
        ) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(Vec::new())
        }

        async fn set_embedding(&self, name: &str, embedding: Vec<f32>) -> Result<(), MemoryError> {
            self.embeddings.lock().unwrap().insert(name.to_string(), embedding);
            Ok(())
        }

        async fn recall_hybrid(
            &self,
            _query: &str,
            _query_embedding: &[f32],
            _kind: Option<&str>,
            _limit: i64,
        ) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(Vec::new())
        }

        async fn forget(&self, name: &str) -> Result<(), MemoryError> {
            self.rows.lock().unwrap().remove(name);
            Ok(())
        }

        async fn clear(&self, kind: Option<&str>) -> Result<u64, MemoryError> {
            let mut rows = self.rows.lock().unwrap();
            let before = rows.len();
            match kind {
                Some(k) => rows.retain(|_, r| r.kind != k),
                None => rows.retain(|_, r| r.kind == "feedback"),
            }
            Ok((before - rows.len()) as u64)
        }
    }

    /// Write a corpus `.md` with frontmatter into `dir`.
    fn write_memory(dir: &std::path::Path, file: &str, name: &str, kind: &str, body: &str) {
        let text = format!(
            "---\nname: {name}\ndescription: a {kind} memory for {name}\nmetadata:\n  type: {kind}\n---\n\n{body}\n"
        );
        std::fs::write(dir.join(file), text).unwrap();
    }

    /// A throwaway corpus dir: two memories (one feedback, one project) + a MEMORY.md index
    /// (no frontmatter) the importer must skip. Returns the temp dir (kept alive by caller).
    fn sample_corpus() -> tempdir_lite::TempDir {
        let dir = tempdir_lite::TempDir::new();
        write_memory(dir.path(), "verify-build.md", "verify-build-before-commit", "feedback", "Gate on N passed 0 failed.");
        write_memory(dir.path(), "data-partition.md", "hq-mt-data-partitioning", "project", "PG schema-per-workspace.");
        // The index carries no frontmatter — Corpus::from_dir skips it, so it must not import.
        std::fs::write(dir.path().join("MEMORY.md"), "# Memory index\n- [verify-build](verify-build.md)\n").unwrap();
        dir
    }

    #[tokio::test]
    async fn imports_memories_skipping_index_and_is_idempotent() {
        let dir = sample_corpus();
        let repo = FakeRepo::default();

        // First import: the two frontmatter memories land; MEMORY.md does not.
        let report = import_corpus(dir.path(), &repo, None).await.unwrap();
        assert_eq!(report.upserted, 2, "two memories imported, MEMORY.md skipped");
        assert_eq!(report.embedded, 0, "no embedder supplied");
        assert!(report.embed_failures.is_empty());
        assert_eq!(repo.len(), 2, "store holds exactly the two memories");
        assert!(repo.get("verify-build-before-commit").await.unwrap().is_some());
        assert!(repo.get("hq-mt-data-partitioning").await.unwrap().is_some());

        // Re-run: upsert-by-name replaces in place — no duplicates, version bumps.
        let report2 = import_corpus(dir.path(), &repo, None).await.unwrap();
        assert_eq!(report2.upserted, 2, "re-import upserts the same two");
        assert_eq!(repo.len(), 2, "re-running does not duplicate");
        assert_eq!(
            repo.get("verify-build-before-commit").await.unwrap().unwrap().version,
            1,
            "second upsert bumped the version (proves replace, not insert)",
        );
    }

    #[tokio::test]
    async fn embeds_each_memory_when_embedder_present() {
        let dir = sample_corpus();
        let repo = FakeRepo::default();

        let embedder = ConstEmbedder;
        let report = import_corpus(dir.path(), &repo, Some(&embedder)).await.unwrap();
        assert_eq!(report.upserted, 2);
        assert_eq!(report.embedded, 2, "both memories embedded");
        assert!(report.embed_failures.is_empty());
        assert_eq!(repo.embeddings.lock().unwrap().len(), 2, "both embeddings stored");
    }

    /// A trivial deterministic [`Embedder`] — returns a fixed zero vector, no ML engine.
    struct ConstEmbedder;

    #[async_trait]
    impl Embedder for ConstEmbedder {
        async fn embed(&self, _text: &str) -> Result<Vec<f32>, gt_docs_embed::EmbedError> {
            Ok(vec![0.0; gt_docs_embed::EMBEDDING_DIM])
        }
    }

    /// Minimal stdlib-only temp dir (no `tempfile` dep): a uniquely-named dir under the OS
    /// temp root, recursively removed on drop. Sufficient for these single-test corpora.
    mod tempdir_lite {
        use std::path::{Path, PathBuf};
        use std::sync::atomic::{AtomicU32, Ordering};

        static COUNTER: AtomicU32 = AtomicU32::new(0);

        pub struct TempDir {
            path: PathBuf,
        }

        impl TempDir {
            pub fn new() -> Self {
                let n = COUNTER.fetch_add(1, Ordering::Relaxed);
                let pid = std::process::id();
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                let path = std::env::temp_dir().join(format!("gt-corpus-import-{pid}-{nanos}-{n}"));
                std::fs::create_dir_all(&path).unwrap();
                TempDir { path }
            }

            pub fn path(&self) -> &Path {
                &self.path
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
    }
}
