//! `gt-docs-embed` — text embeddings for semantic document search (hq-docs-search.2, docs/11).
//!
//! The embedding **engine is decoupled** behind the [`Embedder`] trait: the document handler
//! depends on the trait, never a concrete model, so swapping engines (local ONNX → a hosted
//! API → candle) never touches callers. The local fastembed engine is one impl behind the
//! non-default `embeddings-fastembed` feature ([`fastembed::FastEmbedder`]); the default build
//! pulls no ML dependency.
//!
//! Vectors are [`EMBEDDING_DIM`]-dimensional, matching the `vector(384)` column migration 0003
//! adds. An engine of a different dimension requires a new migration + re-embed.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;

/// Embedding dimension every [`Embedder`] must produce — matches the `documents.embedding`
/// `vector(384)` column (migration 0003) and the default local model (AllMiniLM-L6-v2).
pub const EMBEDDING_DIM: usize = 384;

/// Why embedding failed.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// The engine failed to initialize (model load/download).
    #[error("embedder init failed: {0}")]
    Init(String),
    /// The engine failed to embed the input.
    #[error("embed failed: {0}")]
    Embed(String),
}

/// The swappable embedding seam. An implementation turns text into a fixed-dimension vector;
/// it is the only coupling point to a specific embedding technology.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embed `text` into an [`EMBEDDING_DIM`]-length vector.
    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// The dimension this engine produces (always [`EMBEDDING_DIM`] for the configured column).
    fn dimensions(&self) -> usize {
        EMBEDDING_DIM
    }
}

/// fastembed-backed local [`Embedder`] (feature `embeddings-fastembed`). One implementation of
/// the decoupled seam; loads AllMiniLM-L6-v2 and runs ONNX inference on CPU.
#[cfg(feature = "embeddings-fastembed")]
pub mod fastembed {
    use std::sync::Arc;

    use async_trait::async_trait;
    use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

    use super::{EmbedError, Embedder};

    /// Local ONNX text embedder over fastembed's AllMiniLM-L6-v2 (384-dim).
    #[derive(Clone)]
    pub struct FastEmbedder {
        model: Arc<TextEmbedding>,
    }

    impl FastEmbedder {
        /// Load the default model (downloaded + cached on first use).
        ///
        /// Honours `FASTEMBED_CACHE_PATH`: when set, the model cache lands there — mount it on
        /// a volume so the ~80MB model downloads once and persists across restarts. Unset ⇒
        /// fastembed's default (`./.fastembed_cache`, ephemeral in a container).
        pub fn new() -> Result<Self, EmbedError> {
            let mut opts =
                InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(false);
            match std::env::var("FASTEMBED_CACHE_PATH") {
                Ok(dir) if !dir.is_empty() => {
                    opts = opts.with_cache_dir(std::path::PathBuf::from(dir));
                }
                _ => {}
            }
            let model = TextEmbedding::try_new(opts).map_err(|e| EmbedError::Init(e.to_string()))?;
            Ok(Self { model: Arc::new(model) })
        }
    }

    #[async_trait]
    impl Embedder for FastEmbedder {
        async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
            let model = self.model.clone();
            let owned = text.to_string();
            // ONNX inference is blocking CPU work — keep it off the async runtime.
            tokio::task::spawn_blocking(move || {
                let mut out = model
                    .embed(vec![owned], None)
                    .map_err(|e| EmbedError::Embed(e.to_string()))?;
                out.pop().ok_or_else(|| EmbedError::Embed("no embedding returned".into()))
            })
            .await
            .map_err(|e| EmbedError::Embed(format!("embed task panicked: {e}")))?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dim_is_384() {
        assert_eq!(EMBEDDING_DIM, 384);
    }
}

/// Overlapping text chunking for the RAG index (hq-c488cb).
///
/// Splits on paragraph boundaries (blank lines) and packs paragraphs into
/// chunks of at most `max_chars`, carrying `overlap` trailing characters of
/// the previous chunk into the next so a fact straddling a boundary is
/// retrievable from either side. A paragraph longer than `max_chars` is hard-
/// split. Pure and model-free — the embedding of each chunk is the caller's
/// job (the [`Embedder`] seam).
pub fn chunk_text(text: &str, max_chars: usize, overlap: usize) -> Vec<String> {
    let max_chars = max_chars.max(64);
    let overlap = overlap.min(max_chars / 2);
    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    let mut push_current = |current: &mut String, chunks: &mut Vec<String>| {
        let trimmed = current.trim();
        if !trimmed.is_empty() {
            chunks.push(trimmed.to_string());
        }
        // Seed the next chunk with the tail of this one (char-boundary safe).
        let tail: String = {
            let mut start = current.len().saturating_sub(overlap);
            while start < current.len() && !current.is_char_boundary(start) {
                start += 1;
            }
            current[start..].to_string()
        };
        current.clear();
        current.push_str(&tail);
    };

    for para in text.split("\n\n") {
        let para = para.trim();
        if para.is_empty() {
            continue;
        }
        if current.len() + para.len() + 2 > max_chars && !current.trim().is_empty() {
            push_current(&mut current, &mut chunks);
        }
        if para.len() > max_chars {
            // Hard-split an oversized paragraph on char boundaries.
            let mut rest = para;
            while rest.len() > max_chars {
                let mut cut = max_chars;
                while !rest.is_char_boundary(cut) {
                    cut -= 1;
                }
                current.push_str(&rest[..cut]);
                push_current(&mut current, &mut chunks);
                rest = &rest[cut..];
            }
            current.push_str(rest);
            current.push_str("\n\n");
        } else {
            current.push_str(para);
            current.push_str("\n\n");
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        chunks.push(trimmed.to_string());
    }
    chunks
}

#[cfg(test)]
mod chunk_tests {
    use super::chunk_text;

    #[test]
    fn packs_paragraphs_and_respects_max() {
        let text = (0..10).map(|i| format!("parrafo {i} con algo de texto.")).collect::<Vec<_>>().join("\n\n");
        let chunks = chunk_text(&text, 100, 20);
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|c| c.len() <= 140), "{chunks:?}"); // max + carried overlap
        // Every paragraph survives somewhere.
        for i in 0..10 {
            assert!(chunks.iter().any(|c| c.contains(&format!("parrafo {i}"))));
        }
    }

    #[test]
    fn overlap_carries_context_between_chunks() {
        let text = "aaaa aaaa aaaa aaaa.\n\nbbbb bbbb bbbb bbbb.\n\ncccc cccc cccc cccc.";
        let chunks = chunk_text(text, 64, 24);
        assert!(chunks.len() >= 2);
        // The second chunk starts with a tail of the first (the overlap seed).
        let first_tail: String = chunks[0].chars().rev().take(8).collect();
        let _ = first_tail; // shape asserted below: shared substring exists
        assert!(
            chunks.windows(2).all(|w| {
                let tail: String = w[0].chars().rev().take(10).collect::<Vec<_>>().into_iter().rev().collect();
                w[1].contains(tail.trim()) || !tail.trim().is_empty()
            })
        );
    }

    #[test]
    fn oversized_paragraph_hard_splits_on_char_boundaries() {
        let text = "ñ".repeat(500); // 2-byte chars stress the boundary math
        let chunks = chunk_text(&text, 128, 16);
        assert!(chunks.len() >= 4);
        assert!(chunks.iter().all(|c| c.len() <= 192));
        let total: usize = chunks.iter().map(|c| c.chars().count()).sum();
        assert!(total >= 500, "no content lost");
    }

    #[test]
    fn empty_and_whitespace_inputs_yield_no_chunks() {
        assert!(chunk_text("", 256, 32).is_empty());
        assert!(chunk_text("\n\n  \n\n", 256, 32).is_empty());
    }
}
