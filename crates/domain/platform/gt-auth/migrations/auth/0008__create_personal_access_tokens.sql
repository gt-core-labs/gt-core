-- hq-security-pat.1: the durable Personal Access Token (PAT) store backing `PgPatStore`
-- (gt-auth) — the long-lived, self-service bearer credential a user mints to call the API
-- without a browser session.
--
-- A PAT is an OPAQUE token (`gtpat_` + 256 bits) the holder presents as `Authorization: Bearer`.
-- Unlike the short-lived access JWT (stateless, signature-verified) it is verified by a STORE
-- lookup, so it can be revoked the instant it leaks — the same reason the refresh token is
-- opaque-and-stored (see 0002__create_refresh_tokens.sql). It differs from a refresh token in
-- intent: a refresh token mints access JWTs at `/auth/refresh`; a PAT authenticates API requests
-- directly (its row carries the `scopes` the synthesized claims grant).
--
-- Schema-per-ws (docs/03 Rule 6, docs/04 §15): a PAT belongs to exactly one workspace, so this is
-- per-workspace projection data — defined ONCE in the `ws_default` template, cloned into every
-- `ws_<slug>` by `gt_create_workspace_schema` (it copies every template table with
-- `LIKE ... INCLUDING ALL`), and reached through a `search_path`-scoped `WorkspacePool`.
-- `PgPatStore` issues UNQUALIFIED statements that resolve to the caller's own copy — same shape
-- as `users`/`refresh_tokens`.
--
-- The secret itself is NEVER stored. `token_hash` is the SHA-256 (hex) of the opaque token, so a
-- database leak yields no usable bearer credentials and a lookup is a single indexed equality on
-- the (high-entropy, un-bruteforceable) hash. It is the PRIMARY KEY: the hash IS the row's lookup
-- identity, and its uniqueness guards the (astronomically unlikely) minted-token collision.
--
-- `id` is a separate random, NON-secret handle (the secret is never echoed back), the address the
-- self-service REST surface lists and revokes by (`DELETE /auth/tokens/{id}`). `scopes` is the
-- clamped grant the token carries — minted as the intersection of what the user requested and what
-- the user already holds, so a PAT can never escalate privilege. `expires_at` is NULLABLE: a NULL
-- means "never expires". `last_used_at` is stamped on each successful verify (NULL until first
-- use), so a user can spot a stale or compromised token. `status` is the lifecycle: 'active'
-- (usable) or 'revoked' (killed — verify rejects it). Times are the injected `now_secs` epoch as
-- BIGINT (time-as-data, deterministic replay), matching `users`/`refresh_tokens`.
--
-- `IF NOT EXISTS` keeps the transition idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.

-- The template schema the per-workspace provisioner clones from (idempotent — see
-- 0001__create_users.sql; whichever template table migrates first bootstraps it).
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.personal_access_tokens (
    token_hash    TEXT PRIMARY KEY,
    id            TEXT   NOT NULL,
    sub           TEXT   NOT NULL,
    workspace     TEXT   NOT NULL,
    name          TEXT   NOT NULL,
    scopes        TEXT[] NOT NULL DEFAULT '{}',
    created_at    BIGINT NOT NULL,
    expires_at    BIGINT,
    last_used_at  BIGINT,
    status        TEXT   NOT NULL
);

-- The self-service surface lists/revokes a caller's OWN tokens by `sub` (self-only), and addresses
-- a single token by its non-secret `id` for revocation.
CREATE INDEX IF NOT EXISTS personal_access_tokens_sub_idx ON ws_default.personal_access_tokens (sub);
CREATE INDEX IF NOT EXISTS personal_access_tokens_id_idx ON ws_default.personal_access_tokens (id);
