-- gt-comments #0001 — per-workspace threaded comments (hq-57042e, epic hq-56b5ee).
--
-- One small polymorphic store for the gt Kanban: a comment targets either a
-- board card (a bead, target_kind='card', target_id = the bead id in the
-- tenant's Dolt tracker) or a document (target_kind='doc', target_id = the
-- ws_<slug>.documents id). Threading is parent_id within the same table.
--
-- Schema-per-ws (docs/04 §15, mirrors gt-docs `documents`): per-workspace
-- projection data, defined ONCE in the `ws_default` template schema and cloned
-- into every tenant by `gt_create_workspace_schema`. Tenant isolation is
-- structural — no workspace column, no cross-schema FK. NO FK on target_id
-- either: a 'card' target lives in Dolt (another store entirely), so target
-- existence is validated by the handler, not the database. parent_id carries no
-- self-FK because `CREATE TABLE ... LIKE ... INCLUDING ALL` does not clone FK
-- constraints into tenant schemas — the handler validates the parent instead.
--
-- @mention (ADR low-coupling): mentions are parsed and dispatched by the
-- handler as notifications; nothing mention-related is stored here.

CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.comments (
    -- Opaque comment id (ULID/uuid minted by the handler).
    id          TEXT        PRIMARY KEY,
    -- Polymorphic target: a Kanban card (bead) or a document.
    target_kind TEXT        NOT NULL CHECK (target_kind IN ('card', 'doc')),
    target_id   TEXT        NOT NULL,
    -- Comment author (the server-injected scope actor; never body-supplied).
    author      TEXT        NOT NULL,
    body        TEXT        NOT NULL,
    -- Threading: NULL = top-level; else the parent comment's id (same target).
    parent_id   TEXT        NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Set on every body edit; NULL = never edited.
    edited_at   TIMESTAMPTZ NULL,
    -- Soft-delete (mirrors documents.deleted_at): non-NULL hides the row but
    -- keeps the thread skeleton so replies stay anchored.
    deleted_at  TIMESTAMPTZ NULL
);

-- Hot path: the live comment thread of one target, in chronological order.
CREATE INDEX IF NOT EXISTS comments_target_idx
    ON ws_default.comments (target_kind, target_id, created_at)
    WHERE deleted_at IS NULL;

-- Reply lookup while threading.
CREATE INDEX IF NOT EXISTS comments_parent_idx
    ON ws_default.comments (parent_id);
