-- hq-mt-rigs.5: per-rig worktree-root override column.
--
-- Backs `RigEntry.worktree_root` (hq-mt-rigs.3) so the `rig.set-worktree-root` tool's change
-- is durable in the projection, not just the event log. NULL means the rig follows the
-- convention default (`<home>/gastown-wt/<ws>/<name>`, resolved at the edge — hq-mt-rigs.4);
-- a non-NULL value is the absolute root pinned for the rig.
--
-- Schema-per-ws (docs/04 §15): added to the `ws_default` template, exactly like the columns
-- in 0001 — `gt_create_workspace_schema` clones the template, and the per-tenant `WorkspacePool`
-- resolves an unqualified `rigs` to the caller's schema. Never edit an applied file
-- (sqlx checksum-validates); this is a new migration. `IF NOT EXISTS` keeps it idempotent.
ALTER TABLE ws_default.rigs
    ADD COLUMN IF NOT EXISTS worktree_root TEXT NULL;
