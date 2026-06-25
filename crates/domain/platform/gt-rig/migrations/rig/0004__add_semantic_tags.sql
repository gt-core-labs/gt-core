-- B3 (gtcore-1caa48): per-rig semantic capability tags column.
--
-- Backs `RigEntry.semantic_tags` so `rig.set-tags`' change is durable in the projection (the
-- catalog read `a2a.discover` filters on), not only in the event log. A TEXT[] of short
-- capability keywords (`rust`, `frontend`, `infra`); the application normalises (lowercase,
-- dedupe) and validates grammar before writing, so the column carries no constraint of its own.
-- DEFAULT '{}' (empty array, NOT NULL) makes pre-B3 rows read back as "no tags" — discoverable
-- by prefix only, exactly as before this migration. Backward compatible.
--
-- Schema-per-ws (docs/04 §15): added to the `ws_default` template like the columns in 0001-0003 —
-- `gt_create_workspace_schema` clones the template, and the per-tenant `WorkspacePool` resolves
-- an unqualified `rigs` to the caller's schema. Never edit an applied file (sqlx
-- checksum-validates); this is a new migration. `IF NOT EXISTS` keeps it idempotent.
ALTER TABLE ws_default.rigs
    ADD COLUMN IF NOT EXISTS semantic_tags TEXT[] NOT NULL DEFAULT '{}';
