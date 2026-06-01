-- gt-workspace #0001 — workspace catalog.
--
-- Mirrors `gt_workspace::WorkspaceEntry` (id, slug, name, status). Per
-- docs/04 rule 14, every projection table FKs to `workspaces.id`; this is
-- the table that anchors those references, so it must exist first.
CREATE TABLE IF NOT EXISTS workspaces (
    id          TEXT PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    -- Matches `WorkspaceStatus` serialized snake_case.
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'suspended', 'archived')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bootstrap default workspace so the platform is usable before any tenant is
-- provisioned. Idempotent: re-applying the migration must not error or
-- duplicate the row (the (module_id, sequence) ledger already guards normal
-- re-runs; ON CONFLICT additionally guards a manual re-apply).
INSERT INTO workspaces (id, slug, name, status)
VALUES ('default', 'default', 'Default Workspace', 'active')
ON CONFLICT (id) DO NOTHING;
