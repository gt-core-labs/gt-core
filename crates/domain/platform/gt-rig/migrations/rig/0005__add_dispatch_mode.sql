-- rig-hold H1 (gtcore-fab8fb, epic gtcore-4b7d56): per-rig dispatch-mode column.
--
-- Backs `RigEntry.dispatch_mode` so the operator's hold/resume is durable in the projection (the
-- catalog read the scheduler + watchdogs will gate on in H2/H3), not only in the event log. A TEXT
-- carrying the lowercase token `auto` | `hold`; the application validates the value (the closed
-- `DispatchMode` enum) before writing, so the column needs no CHECK of its own. DEFAULT 'auto'
-- (NOT NULL) makes every pre-feature row read back as `auto` — dispatchable, exactly as before this
-- migration. Backward compatible: a never-held rig is indistinguishable from one explicitly resumed.
--
-- Schema-per-ws (docs/04 §15): added to the `ws_default` template like the columns in 0001-0004 —
-- `gt_create_workspace_schema` clones the template, and the per-tenant `WorkspacePool` resolves an
-- unqualified `rigs` to the caller's schema. Never edit an applied file (sqlx checksum-validates);
-- this is a new migration. `IF NOT EXISTS` keeps it idempotent.
ALTER TABLE ws_default.rigs
    ADD COLUMN IF NOT EXISTS dispatch_mode TEXT NOT NULL DEFAULT 'auto';
