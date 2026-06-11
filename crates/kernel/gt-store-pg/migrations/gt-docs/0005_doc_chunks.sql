-- gt-docs #0005 — chunk-level embeddings for agent RAG (hq-c488cb, epic hq-56b5ee).
--
-- Phase-2 search (0003) embeds ONE vector per whole document — good for ranked
-- doc search, too coarse for prompt injection. This table holds the CHUNKED
-- index: overlapping text segments of body_md / extracted_text, each with its
-- own vector(384) (the same gt-docs-embed AllMiniLM-L6-v2 runtime — ADR D9: no
-- new model stack). `documents.retrieve` returns the top-k chunks for an agent
-- prompt.
--
-- Schema-per-ws like `documents`: workspace isolation is STRUCTURAL (the
-- tenant's search_path), so retrieval can never leak another workspace's
-- chunks. The rig half of the scope key derives from the owning bead id's
-- prefix (owner_id), filterable per query.
CREATE TABLE IF NOT EXISTS ws_default.doc_chunks (
    doc_id     TEXT        NOT NULL,
    chunk_idx  INT         NOT NULL,
    -- Denormalized owner for rig-scoped retrieval without a join.
    owner_type TEXT        NOT NULL DEFAULT '',
    owner_id   TEXT        NOT NULL DEFAULT '',
    filename   TEXT        NOT NULL DEFAULT '',
    content    TEXT        NOT NULL,
    embedding  vector(384) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (doc_id, chunk_idx)
);

-- ANN index for the cosine retrieve. HNSW like documents_embedding_idx (0003).
CREATE INDEX IF NOT EXISTS doc_chunks_embedding_idx
    ON ws_default.doc_chunks USING hnsw (embedding vector_cosine_ops);

-- Purge path: all of one document's chunks.
CREATE INDEX IF NOT EXISTS doc_chunks_doc_idx ON ws_default.doc_chunks (doc_id);
