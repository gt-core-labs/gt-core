-- hq-identity.1: global user identity + N:N workspace membership.
--
-- The schema-per-ws login store (0001 `ws_default.users`) tied one user to exactly one
-- workspace. This epic flips that: identity becomes GLOBAL (one `public.users` row per human,
-- email unique across the whole deploy) and the user's reach into workspaces is an explicit
-- N:N membership (`public.user_workspaces`). Login (hq-identity.2) authenticates against the
-- global row, then resolves memberships to pick the JWT's active workspace + role; switching
-- (hq-identity.3) re-mints against another membership.
--
-- These tables live in `public` (NOT a per-ws schema): identity is cross-tenant by definition,
-- so it is the one piece of auth state that is shared rather than cloned per `ws_<slug>`. The
-- per-workspace `ws_default.users` / `roles` tables stay in place during the transition for
-- back-compat (hq-identity.4 migrates rows across); this migration only ADDS, it drops nothing.
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.

-- Global identity: one row per human. `email` is UNIQUE across the WHOLE deploy (unlike the
-- per-ws `users.email`, unique only within a tenant schema). `id` is the subject (`sub`) the
-- minted token carries. `password_hash` is the argon2 PHC string — never the plaintext.
-- Timestamps are the `now_secs` epoch as BIGINT, matching the rest of the projection.
CREATE TABLE IF NOT EXISTS public.users (
    id             TEXT   PRIMARY KEY,
    email          TEXT   NOT NULL UNIQUE,
    password_hash  TEXT   NOT NULL,
    created_at     BIGINT NOT NULL,
    updated_at     BIGINT NOT NULL
);

-- Membership: which workspaces a user belongs to, and the role they hold in each. The
-- `(user_id, workspace_slug)` pair is UNIQUE — a user holds at most one role per workspace.
-- `role` is the per-workspace role name (expanded to scopes against `ws_<slug>.roles` at login,
-- hq-identity.2); empty string means "member with no role-granted scopes". Deleting the
-- identity cascades its memberships away.
CREATE TABLE IF NOT EXISTS public.user_workspaces (
    user_id        TEXT   NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    workspace_slug TEXT   NOT NULL,
    role           TEXT   NOT NULL DEFAULT '',
    created_at     BIGINT NOT NULL,
    UNIQUE (user_id, workspace_slug)
);

-- Lookup memberships by user (the login/switch hot path: "which workspaces can this sub reach").
CREATE INDEX IF NOT EXISTS user_workspaces_user_id_idx ON public.user_workspaces (user_id);
