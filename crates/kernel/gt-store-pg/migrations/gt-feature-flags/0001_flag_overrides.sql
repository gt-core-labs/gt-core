-- gt-feature-flags #0001 — per-workspace flag overrides.
--
-- Backs the `FeatureFlags` port (gt-feature-flags/src/repo.rs): an
-- override-only store keyed by (workspace, flag_key). Absence of a row means
-- "use the flag's build default"; a row is the deviation from that default, so a
-- flag reverts by DELETE, never by writing the default value.
--
-- `flag_key` is the dotted-kebab `FlagKey` (e.g. `module.beads`, `feature.x`),
-- which subsumes the per-module toggle the original spec called `module_id`.
-- Per docs/04 rule 14 every projection table FKs to `workspaces.id`; this one
-- cascades so removing a workspace drops its overrides with it.
CREATE TABLE IF NOT EXISTS flag_overrides (
    workspace_id TEXT        NOT NULL REFERENCES workspaces (id) ON DELETE CASCADE,
    flag_key     TEXT        NOT NULL,
    enabled      BOOLEAN     NOT NULL,
    -- When the override was last set, and by whom (audit). `set_by` defaults to
    -- empty rather than NULL to match the empty-string-means-unattributed
    -- convention used elsewhere in hq.issues.
    since        TIMESTAMPTZ NOT NULL DEFAULT now(),
    set_by       TEXT        NOT NULL DEFAULT '',
    -- One override per (workspace, key): set_override is an upsert on this key.
    PRIMARY KEY (workspace_id, flag_key)
);
