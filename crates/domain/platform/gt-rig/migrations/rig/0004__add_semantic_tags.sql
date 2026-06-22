-- B3 (gtcore-dd3763): semantic capability tags column for capability-based peer selection.
--
-- Backs `RigEntry.semantic_tags` (Vec<String>) so the `rig.set-semantic-tags` tool's change is
-- durable in the projection, not just the event log. These free-form labels (`rust`, `backend`,
-- `frontend`) are advertised on the Agent Card skills and filtered by `a2a.discover`, so a peer
-- can select a rig by capability in addition to its beads prefix. `'{}'` (empty array) is the
-- default — a rig advertises no extra capabilities until tags are set.
--
-- TEXT[] (Postgres array): sqlx binds/reads a `Vec<String>` to/from it natively, matching how
-- `gt-auth` stores PAT `scopes` (auth/0008). NOT NULL DEFAULT '{}' so existing rows backfill to
-- an empty set and the adapter never has to handle a NULL.
--
-- Schema-per-ws (docs/04 §15): added to the `ws_default` template, exactly like the columns in
-- 0002/0003 — `gt_create_workspace_schema` clones the template, so every existing and new tenant
-- gets the column. Never edit an applied file (sqlx checksum-validates); this is a new migration.
-- `IF NOT EXISTS` keeps it idempotent.
ALTER TABLE ws_default.rigs
    ADD COLUMN IF NOT EXISTS semantic_tags TEXT[] NOT NULL DEFAULT '{}';
