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
        pub fn new() -> Result<Self, EmbedError> {
            let model = TextEmbedding::try_new(
                InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(false),
            )
            .map_err(|e| EmbedError::Init(e.to_string()))?;
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
