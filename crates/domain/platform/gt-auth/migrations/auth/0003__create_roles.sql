-- hq-rbac.3: the role catalog backing role→scopes expansion at login.
--
-- A role is a named bundle of authorization scopes (the DB counterpart of `RbacConfig`'s
-- `[roles.*]` table in gt-rbac). At login, a user's assigned roles (the `users.roles` column,
-- migration 0004) are expanded against this table and unioned with the user's direct scopes;
-- the union is what rides the minted JWT. Editing a role re-shapes every assignee's next token
-- without touching user rows — the point of indirection.
--
-- Schema-per-ws (docs/03 Rule 6, docs/04 §15): a role belongs to exactly one workspace, so this
-- is per-workspace projection data — defined ONCE in the `ws_default` template,
-- `gt_create_workspace_schema` clones it into every `ws_<slug>`, and a `WorkspacePool` scopes
-- `search_path` to the tenant schema on checkout. `PgRoles` issues UNQUALIFIED statements that
-- resolve to the caller's own copy — the same shape as `users`/`refresh_tokens`.
--
-- `name` is the role identifier (PK, referenced by `users.roles`). `scopes` is the TEXT[]
-- bundle, each entry validated against the closed scope vocabulary (gt-rbac `validate_scope`,
-- hq-rbac.1) before write by the CRUD layer (hq-rbac.4) — so a typo never reaches storage.
-- Times are the injected `now_secs` epoch as BIGINT (time-as-data, deterministic replay),
-- matching the rest of the projection.
--
-- `IF NOT EXISTS` keeps the transition idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.

-- The template schema the per-workspace provisioner clones from (idempotent — see
-- 0001__create_users.sql; whichever template table migrates first bootstraps it).
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.roles (
    name        TEXT PRIMARY KEY,
    scopes      TEXT[] NOT NULL DEFAULT '{}',
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL
);
