-- hq-vcs-connections.3: link a rig to the VCS connection it clones with.
--
-- Backs `RigEntry.git_connection_ref` so a rig records WHICH `public.vcs_connections.id`
-- (the workspace's GitHub App install / sealed-PAT connection, hq-vcs-connections.1) the
-- server-side provisioner mints a clone token from. NULL means the rig has no connection
-- yet (the legacy operator-mounted / public-repo path); a non-NULL value is the connection
-- id resolved at refresh time.
--
-- SOFT reference, NO cross-schema FK: `vcs_connections` lives in `public` while `rigs` is a
-- per-tenant `ws_<slug>` table with no `workspace_id` column — the codebase resolves tenant
-- structurally (search_path), so a `public.vcs_connections(id)` FK from a tenant schema would
-- couple the two against the established pattern (0001 already declares NO FK to
-- `workspaces.id`). The connection id is validated/resolved at the application layer, not the DB.
--
-- Schema-per-ws (docs/04 §15): added to the `ws_default` template, exactly like the
-- `worktree_root` column in 0002 — `gt_create_workspace_schema` clones the template, so every
-- existing and new tenant gets the column. Never edit an applied file (sqlx checksum-validates);
-- this is a new migration. `IF NOT EXISTS` keeps it idempotent.
ALTER TABLE ws_default.rigs
    ADD COLUMN IF NOT EXISTS git_connection_ref TEXT NULL;
