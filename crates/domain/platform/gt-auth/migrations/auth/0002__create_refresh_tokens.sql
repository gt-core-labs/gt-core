-- hq-auth-session.1: the durable refresh-token store backing `RefreshStore` (gt-auth
-- `PgRefreshStore`) — the persistent counterpart of the in-memory reference adapter.
--
-- A refresh token is the long-lived OPAQUE credential a client trades in for a fresh access
-- token without re-login. The short-lived access JWT stays stateless (it just expires); this
-- table is what makes logout and reuse-revocation DURABLE — they outlive a process restart,
-- which an in-memory map cannot do.
--
-- Schema-per-ws (docs/03 Rule 6, docs/04 §15): a session belongs to exactly one workspace, so
-- this is per-workspace projection data — the table is defined ONCE in the `ws_default`
-- template, `gt_create_workspace_schema` clones it into every `ws_<slug>`, and a
-- `WorkspacePool` sets `search_path` to the tenant schema on checkout. `PgRefreshStore` issues
-- UNQUALIFIED statements that resolve to the caller's own copy — same shape as `users`/`rigs`.
--
-- The secret itself is NEVER stored. `token_hash` is the SHA-256 (hex) of the opaque token, so
-- a database leak yields no usable bearer credentials and a lookup is still a single indexed
-- equality on the (high-entropy, hence un-bruteforceable) hash. It is the PRIMARY KEY: the hash
-- IS the identity of a row for lookup, and its uniqueness guards against the (astronomically
-- unlikely) minted-token collision.
--
-- Rotation + reuse detection (RFC 6819 §5.2.2.3) is a single atomic compare-and-swap:
--   UPDATE ... SET status='rotated' WHERE token_hash=$1 AND status='active' AND expires_at>$now
--   RETURNING ...
-- Exactly one concurrent presentation of an active token wins the row lock and flips it; every
-- other presentation finds it no longer 'active' and reports reuse — so the reuse race is
-- settled by the row lock, never by a read-then-write gap. `family` carries an index because
-- reuse and logout both revoke a whole family in one `UPDATE ... WHERE family=$1`.
--
-- `status` is the lifecycle: 'active' (rotatable), 'rotated' (spent — presenting again is the
-- theft signal), 'revoked' (family killed). `scopes` is the granted-scope set, carried so a
-- rotation re-mints a faithful access token without re-querying the user store (inherited
-- unchanged down the chain). Times are the injected `now_secs` epoch as BIGINT (time-as-data,
-- deterministic replay), matching `users`/the rest of the projection.
--
-- `IF NOT EXISTS` keeps the transition idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.

-- The template schema the per-workspace provisioner clones from (idempotent — see
-- 0001__create_users.sql; whichever template table migrates first bootstraps it).
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.refresh_tokens (
    token_hash   TEXT PRIMARY KEY,
    id           TEXT   NOT NULL,
    family       TEXT   NOT NULL,
    sub          TEXT   NOT NULL,
    workspace    TEXT   NOT NULL,
    scopes       TEXT[] NOT NULL DEFAULT '{}',
    issued_at    BIGINT NOT NULL,
    expires_at   BIGINT NOT NULL,
    status       TEXT   NOT NULL
);

-- Reuse-revocation and logout sweep an entire family in one statement.
CREATE INDEX IF NOT EXISTS refresh_tokens_family_idx ON ws_default.refresh_tokens (family);
