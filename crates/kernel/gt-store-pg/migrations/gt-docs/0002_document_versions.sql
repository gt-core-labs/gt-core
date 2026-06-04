-- gt-docs #0002 — document version history (hq-docs-store.1, decision 1: versioning).
--
-- Every mutation of a `documents` row appends an immutable snapshot here so a document's
-- full edit history is recoverable (diff / restore). The live row in `documents` always
-- holds the current version; this table holds every prior `version` of `body_md` /
-- `extracted_text` / blob pointer. The port writes a row here in the same transaction it
-- bumps `documents.version`.
--
-- Schema-per-ws, same rule as 0001: defined in the `ws_default` template, cloned per tenant,
-- no `workspace_id` / FK. `document_id` references the tenant's own `documents` and cascades
-- on hard purge (the soft-delete path leaves history intact).

CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.document_versions (
    document_id    TEXT        NOT NULL
                   REFERENCES ws_default.documents (id) ON DELETE CASCADE,
    -- The version this snapshot captures; (document_id, version) is the natural key.
    version        BIGINT      NOT NULL,
    kind           TEXT        NOT NULL,
    filename       TEXT        NOT NULL,
    content_type   TEXT        NULL,
    sha256         CHAR(64)    NULL,
    body_md        TEXT        NULL,
    bucket         TEXT        NULL,
    key            TEXT        NULL,
    extracted_text TEXT        NULL,
    -- Who produced this version and when.
    edited_by      TEXT        NOT NULL DEFAULT '',
    edited_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (document_id, version)
);
