-- gt-docs #0003 — semantic search embedding column (hq-docs-search.2, docs/11 phase 2).
--
-- Adds a pgvector column for semantic retrieval alongside the phase-1 `tsv` full-text column,
-- so `documents.search` can rank by meaning (vector cosine) fused with keyword (ts_rank) —
-- the hybrid path. The vector is produced by the decoupled `Embedder` port (gt-docs-embed):
-- the dimension here MUST match the configured model. 384 = the default local model
-- (fastembed AllMiniLM-L6-v2 / bge-small). Swapping to a different-dimension model is a NEW
-- migration (drop + re-add + re-embed), never an edit to this applied file.
--
-- pgvector is a database-global extension; `CREATE EXTENSION` here installs it once. The
-- column + its ANN index are added to the `ws_default` template, so `CREATE TABLE ... LIKE
-- INCLUDING ALL` clones both into every tenant schema (docs/11), exactly like the phase-1
-- columns. Requires a Postgres image carrying pgvector (pgvector/pgvector) — CI + the gt-app
-- compose use it.

CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE ws_default.documents
    ADD COLUMN IF NOT EXISTS embedding vector(384);

-- HNSW index for cosine-distance ANN search (`<=>`). Built on the template; cloned per tenant.
CREATE INDEX IF NOT EXISTS documents_embedding_idx
    ON ws_default.documents USING hnsw (embedding vector_cosine_ops);
