-- hq-rbac.3: the role assignment column on the login store.
--
-- A user holds zero or more role names (the `roles` catalog, migration 0003) in addition to any
-- direct scopes. At login, `PgUsers` expands these role names → their scope bundles and unions
-- them with the row's direct `scopes`; the union rides the minted JWT. Assignment (writing this
-- column) is the role-management surface (hq-rbac.4).
--
-- An additive `ADD COLUMN ... IF NOT EXISTS` with a default keeps the transition idempotent and
-- back-compatible: existing rows get an empty array (no roles ⇒ expansion is a no-op, behaviour
-- unchanged), and the migration is a separate file because 0001 is already applied — never edit
-- an applied file (sqlx checksum-validates).

ALTER TABLE ws_default.users
    ADD COLUMN IF NOT EXISTS roles TEXT[] NOT NULL DEFAULT '{}';
