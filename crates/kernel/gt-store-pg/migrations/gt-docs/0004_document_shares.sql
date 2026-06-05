-- gt-docs #0004 — document share links (hq-web-extras.9: capability URLs).
--
-- A share is a public, unguessable capability URL onto a LIVE document: anyone holding the
-- 128-bit url-safe `hash` reads the document's current content without authenticating, until
-- the share expires (`expires_at`) or is revoked (`revoked`). The content is never copied —
-- the public read joins back to `documents` and renders whatever the row holds NOW.
--
-- Lifecycle (the owner drives it over the authenticated `documents.write` surface): create
-- with an optional TTL, PATCH the limit (extend / shorten / lift), DELETE to revoke. Expiry is
-- LAZY: the public read computes `now() > expires_at` at read time, so no GC job is required
-- (an optional reconciler may later purge long-dead rows). The derived state is
-- active | expired | revoked.
--
-- Schema-per-ws, same rule as 0001/0002: defined in the `ws_default` template, cloned per
-- tenant by `gt_create_workspace_schema`, no `workspace_id` column / FK — tenant isolation is
-- the schema itself. `document_id` references the tenant's own `documents` and cascades on a
-- hard purge of the doc. Many shares per document are allowed (decision: several capability
-- URLs, each with its own TTL).
--
-- Never edit this applied file (sqlx checksum-validates); add a new migration. `IF NOT EXISTS`
-- keeps it idempotent like the other template migrations.

CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.document_shares (
    -- The capability token: 128 bits of CSPRNG entropy, url-safe base64 (22 chars). The PK and
    -- the only thing a public reader presents — brute-force is computationally infeasible.
    hash             TEXT        PRIMARY KEY,
    -- The document this share exposes (live content; never a snapshot).
    document_id      TEXT        NOT NULL
                     REFERENCES ws_default.documents (id) ON DELETE CASCADE,
    -- Who minted the share (attribution; mirrors documents.uploaded_by).
    created_by       TEXT        NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Expiry instant. NULL = no limit (lives until revoked). Compared lazily at read time.
    expires_at       TIMESTAMPTZ NULL,
    -- Hard revocation: a revoked share is permanently dead regardless of expiry.
    revoked          BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Last public read (best-effort touch; informational, never gates access).
    last_accessed_at TIMESTAMPTZ NULL
);

-- Hot path: list the shares of one document (owner management view).
CREATE INDEX IF NOT EXISTS document_shares_doc_idx
    ON ws_default.document_shares (document_id);
