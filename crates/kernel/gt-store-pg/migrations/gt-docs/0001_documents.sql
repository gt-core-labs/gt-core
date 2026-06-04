-- gt-docs #0001 — per-workspace document store (hq-docs-store.1).
--
-- Backs the `DocumentsRepository` port (hq-docs-store.3): supporting material an
-- operator or agent attaches to a bead (epic / skill / spec) so a model resolving
-- that bead reads it as context. Two content classes share the row:
--   * kind='md'   — the .md content itself lives in `body_md` (model-readable as-is).
--   * kind='blob' — bytes live in MinIO (bucket/key pointer); `extracted_text` holds
--                   the text pulled out for the model + search.
--
-- Schema-per-ws (docs/04 §15, mirrors gt-rig `rigs`): per-workspace projection data,
-- so the table is defined ONCE in the `ws_default` template schema.
-- `gt_create_workspace_schema` (hq-mt-data.2) clones the template structure into every
-- `ws_<slug>`, and a `WorkspacePool` sets `search_path` to the tenant schema on checkout,
-- so the port reads/writes an unqualified `documents` that resolves to the tenant's own
-- copy. NO `workspace_id` column and NO FK to `workspaces.id`: tenant isolation is
-- structural (separate schema), not a `WHERE` predicate. A row in `ws_acme.documents`
-- IS acme's, by schema.
--
-- Phasing (docs/11): this migration lands the phase-1 shape — `body_md`/`extracted_text`
-- + a `tsv` full-text column. The phase-2 `embedding` vector column + its ANN index are a
-- LATER migration (hq-docs-search.2); never edit this applied file (sqlx checksum-validates),
-- add a new one. `IF NOT EXISTS` keeps it idempotent for the same transitional reason as
-- the other template migrations.

-- The template schema the per-workspace provisioner clones from. Idempotent so whichever
-- template table migrates first bootstraps it; `gt_create_workspace_schema` only reads from
-- `ws_default`, it does not create it.
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.documents (
    -- Opaque document id (uuid/ULID minted by the port). Stable across versions.
    id             TEXT        PRIMARY KEY,
    -- What this document is attached to. owner_type is the bead class; owner_id its id.
    owner_type     TEXT        NOT NULL,
    owner_id       TEXT        NOT NULL,
    -- Content class. 'md' keeps body_md; 'blob' keeps bucket/key + extracted_text.
    kind           TEXT        NOT NULL DEFAULT 'md'
                   CHECK (kind IN ('md', 'blob')),
    filename       TEXT        NOT NULL,
    content_type   TEXT        NULL,
    size           BIGINT      NULL,
    -- Content hash; the dedup key (hq-docs decision 3): one blob, many rows.
    sha256         CHAR(64)    NULL,
    -- kind='md': the markdown body, model-readable with no extraction step.
    body_md        TEXT        NULL,
    -- kind='blob': MinIO pointer (bytes never enter Postgres).
    bucket         TEXT        NULL,
    key            TEXT        NULL,
    -- kind='blob': text pulled out of the binary for the model + full-text search.
    extracted_text TEXT        NULL,
    -- Phase-1 full-text vector over body_md / extracted_text (hq-docs-search.1). A GENERATED
    -- STORED column, NOT a trigger: `CREATE TABLE ... LIKE ... INCLUDING ALL` clones generated
    -- columns + indexes into every tenant schema but does NOT clone triggers, so the trigger
    -- form would silently leave cloned tenants un-indexed. The 2-arg `to_tsvector('english', …)`
    -- is IMMUTABLE (the bare 1-arg form is only STABLE and is rejected for a generated column).
    -- Phase-2 (hq-docs-search.2) adds an `embedding vector` column alongside it.
    tsv            tsvector    GENERATED ALWAYS AS (
                       to_tsvector('english', coalesce(body_md, '') || ' ' || coalesce(extracted_text, ''))
                   ) STORED,
    -- Optimistic-concurrency token (mirrors issues.version): a stale edit fails loud
    -- instead of clobbering a concurrent write. No file-merge step.
    version        BIGINT      NOT NULL DEFAULT 0,
    -- Soft-delete (hq-docs decision 2): non-NULL hides the row; a reconciler later purges
    -- the backing blob of long-dead rows.
    deleted_at     TIMESTAMPTZ NULL,
    uploaded_by    TEXT        NOT NULL DEFAULT '',
    uploaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: list the live documents of one owner.
CREATE INDEX IF NOT EXISTS documents_owner_idx
    ON ws_default.documents (owner_type, owner_id)
    WHERE deleted_at IS NULL;

-- Dedup lookup by content hash.
CREATE INDEX IF NOT EXISTS documents_sha_idx
    ON ws_default.documents (sha256);

-- Phase-1 full-text search.
CREATE INDEX IF NOT EXISTS documents_tsv_idx
    ON ws_default.documents USING gin (tsv);
