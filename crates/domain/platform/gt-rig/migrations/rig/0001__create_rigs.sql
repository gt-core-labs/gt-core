-- Paso 10 C11c (hq-mc72.12.31): rig catalog table backing `RigRepository` (gt-rig).
-- Schema-per-ws (hq-mt-data.3, docs/04 §15): a rig is per-workspace projection data, so the
-- table is defined ONCE in the `ws_default` template schema. `gt_create_workspace_schema`
-- (hq-mt-data.2) clones the template structure into every `ws_<slug>`, and a `WorkspacePool`
-- sets `search_path` to the tenant schema on checkout, so the port reads/writes an unqualified
-- `rigs` that resolves to the tenant's own copy. NO `workspace_id` column and NO FK to
-- `workspaces.id`: tenant isolation is structural (separate schema), not a `WHERE` predicate.
--
-- Light-write (a registration is rare), read-mostly. `name` is the identity; `prefix` carries
-- a UNIQUE index so the DB mirrors the catalog's prefix-collision invariant (the port's
-- `prefix_owner` contract) — scoped WITHIN the tenant schema, so two workspaces may reuse a
-- prefix without colliding. `registered_at` is the `now_secs` epoch stored as BIGINT — the
-- reducer already keeps time as data so replay stays byte-for-byte.
--
-- `IF NOT EXISTS` for the same transition reason as the other migrations; never edit an
-- applied file (sqlx checksum-validates), add a new one.

-- The template schema the per-workspace provisioner clones from. Idempotent so the first
-- template table to be migrated bootstraps it; `gt_create_workspace_schema` (hq-mt-data.2)
-- only reads from `ws_default`, it does not create it.
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.rigs (
    name            TEXT PRIMARY KEY,
    prefix          TEXT   NOT NULL UNIQUE,
    git_url         TEXT   NOT NULL,
    push_url        TEXT   NULL,
    upstream_url    TEXT   NULL,
    default_branch  TEXT   NOT NULL,
    registered_at   BIGINT NOT NULL
);
