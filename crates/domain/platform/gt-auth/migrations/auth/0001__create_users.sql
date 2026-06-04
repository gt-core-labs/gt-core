-- hq-auth-mint.2: the login credential store backing `IdentityProvider` (gt-auth `PgUsers`).
--
-- This is the email+password store the login step reads BEFORE a token is minted: an email
-- selects a row, the presented password is verified against the stored argon2 PHC hash, and a
-- `VerifiedIdentity` is built from the row's id + scopes.
--
-- Schema-per-ws (docs/03 Rule 6, docs/04 §15): a user belongs to exactly one workspace, so
-- this is per-workspace projection data — the table is defined ONCE in the `ws_default`
-- template schema. `gt_create_workspace_schema` (hq-mt-data.2) clones the template into every
-- `ws_<slug>`, and a `WorkspacePool` sets `search_path` to the tenant schema on checkout, so
-- `PgUsers` issues an UNQUALIFIED `users` statement that resolves to the caller's own copy.
-- There is therefore NO `workspace` column and NO FK to `workspaces.id`: tenant isolation is
-- structural (separate schema), not a `WHERE` predicate — the same shape as gt-rig's `rigs`.
--
-- This is also why the row does not carry the workspace: the workspace a login resolves to is
-- the schema the pool is scoped to, INJECTED by the server (docs/04 §15 — the server injects
-- the workspace, the credentials payload never supplies it). `PgUsers` stamps that
-- server-side slug onto every `VerifiedIdentity` it returns; a forged workspace cannot enter
-- through the login path.
--
-- `email` carries a UNIQUE index (the login identity, scoped WITHIN the tenant schema, so two
-- workspaces may reuse an address without colliding). `id` is the subject (`sub`) the minted
-- token carries. `password_hash` is the argon2 PHC string — never the plaintext. `scopes` is a
-- TEXT[] folded straight into `VerifiedIdentity::scopes`. Timestamps are the `now_secs` epoch
-- as BIGINT, matching the rest of the projection (time as data, replay stays deterministic).
--
-- `IF NOT EXISTS` keeps the transition idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.

-- The template schema the per-workspace provisioner clones from. Idempotent so the first
-- template table to be migrated bootstraps it; `gt_create_workspace_schema` (hq-mt-data.2)
-- only reads from `ws_default`, it does not create it.
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.users (
    id             TEXT PRIMARY KEY,
    email          TEXT   NOT NULL UNIQUE,
    password_hash  TEXT   NOT NULL,
    scopes         TEXT[] NOT NULL DEFAULT '{}',
    created_at     BIGINT NOT NULL,
    updated_at     BIGINT NOT NULL
);
