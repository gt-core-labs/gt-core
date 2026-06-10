-- gt-memory #0001 — per-workspace semantic agent memory store (hq-memory-mcp.1).
--
-- Backs the memory MCP namespace: durable, recallable notes an agent (or operator)
-- writes once and later retrieves BY MEANING — the same hybrid keyword (tsv) + vector
-- (embedding) retrieval the `documents` store uses, but keyed by a stable `name` slug
-- instead of an owner/attachment. A memory is upsert-by-name: re-writing `name` replaces
-- the prior body (guarded by `version`), so an agent accretes a small, named, queryable
-- knowledge base rather than a growing append log. `kind` partitions the store into the
-- recall classes the MCP surfaces: 'feedback' (corrections to remember), 'project'
-- (working context), 'reference' (durable facts), 'user' (operator preferences); the
-- per-kind index backs `list` / operating_rules lookups that scan one class.
--
-- This migration mirrors gt-docs/0001 (`tsv` + version) and gt-docs/0003 (pgvector
-- column + HNSW ANN index) collapsed into a single shape — the memory store ships its
-- semantic column from row zero, it has no phase-1/phase-2 split.
--
-- Schema-per-ws (docs/04 §15, like `documents`/`rigs`): per-workspace projection data,
-- defined ONCE in the `ws_default` template schema. `gt_create_workspace_schema`
-- (hq-mt-data.2) clones the template structure into every `ws_<slug>` via
-- `CREATE TABLE ... LIKE ... INCLUDING ALL`, and a `WorkspacePool` sets `search_path`
-- to the tenant schema on checkout, so the port reads/writes an unqualified `memories`
-- that resolves to the tenant's own copy. NO `workspace_id` column and NO FK: tenant
-- isolation is structural (separate schema), not a `WHERE` predicate — a row in
-- `ws_acme.memories` IS acme's, by schema.
--
-- The `tsv` column is GENERATED STORED, NOT a trigger: `LIKE ... INCLUDING ALL` clones
-- generated columns + indexes into every tenant schema but does NOT clone triggers, so
-- the trigger form would silently leave cloned tenants un-indexed. The 2-arg
-- `to_tsvector('english', …)` is IMMUTABLE (the bare 1-arg form is only STABLE and is
-- rejected for a generated column).
--
-- `IF NOT EXISTS` everywhere keeps this idempotent; sqlx checksum-validates applied
-- migrations, so this file must never be edited once shipped — corrections land as a
-- new migration.

-- The template schema the per-workspace provisioner clones from. Idempotent so whichever
-- template table migrates first bootstraps it; `gt_create_workspace_schema` only reads
-- from `ws_default`, it does not create it.
CREATE SCHEMA IF NOT EXISTS ws_default;

-- pgvector is a database-global extension; gt-docs/0003 already installs it, but the
-- memory store must not depend on apply order — `IF NOT EXISTS` makes a second install
-- a no-op rather than a failure.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS ws_default.memories (
    -- Slug naming the memory; the natural key. Upsert-by-name semantics: writing an
    -- existing `name` replaces its body (guarded by `version`). PRIMARY KEY ⇒ unique
    -- per workspace (one memory per name within a tenant schema).
    name        TEXT        PRIMARY KEY,
    -- Short human/agent summary of what this memory holds; surfaced in `list`.
    description TEXT        NOT NULL,
    -- Recall class. Partitions the store; the per-kind index scans one class for
    -- `list` / operating_rules.
    kind        TEXT        NOT NULL
                CHECK (kind IN ('feedback', 'project', 'reference', 'user')),
    -- The memory content itself, model-readable as-is.
    body        TEXT        NOT NULL,
    -- Hybrid-search full-text vector over name + description + body. GENERATED STORED
    -- (not a trigger) so `LIKE ... INCLUDING ALL` carries it into every cloned tenant.
    tsv         tsvector    GENERATED ALWAYS AS (
                    to_tsvector(
                        'english',
                        coalesce(name, '') || ' ' || coalesce(description, '') || ' ' || coalesce(body, '')
                    )
                ) STORED,
    -- Semantic-search vector, produced by the decoupled `Embedder` port. 384 dims =
    -- the default local model (fastembed AllMiniLM-L6-v2), same as `documents`. A
    -- different-dimension model is a NEW migration (drop + re-add + re-embed).
    embedding   vector(384),
    -- Optimistic-concurrency token (mirrors documents.version / issues.version): a stale
    -- upsert fails loud instead of clobbering a concurrent write.
    version     BIGINT      NOT NULL DEFAULT 0,
    created_by  TEXT        NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full-text search over the generated vector.
CREATE INDEX IF NOT EXISTS memories_tsv_idx
    ON ws_default.memories USING gin (tsv);

-- HNSW index for cosine-distance ANN search (`<=>`). Built on the template; cloned per tenant.
CREATE INDEX IF NOT EXISTS memories_embedding_idx
    ON ws_default.memories USING hnsw (embedding vector_cosine_ops);

-- Per-kind scan: `list` / operating_rules filter one recall class.
CREATE INDEX IF NOT EXISTS memories_kind_idx
    ON ws_default.memories (kind);
