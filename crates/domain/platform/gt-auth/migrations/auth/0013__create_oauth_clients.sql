-- gtcore-95f950: OAuth client registration — gt-core as an authorization SERVER.
--
-- The sibling `public.oauth_providers` (0006) is the INBOUND-SSO side: the external IdPs gt-core
-- logs its users INTO (Google/GitHub/generic OIDC). THIS table is the OUTBOUND side: the OAuth
-- *clients* (relying parties such as Claude.ai) that authenticate AGAINST gt-core's own
-- `/oauth/authorize` + `/oauth/token` endpoints. Each row is one registered client the token
-- endpoint validates a code exchange against.
--
-- Scope is GLOBAL (deploy infrastructure, not tenant data), so — like `public.oauth_providers`
-- and `public.users` — it lives in `public` with no `workspace_id` and is never cloned into a
-- `ws_<slug>` schema.
--
-- The `client_secret` is NEVER stored in cleartext: `client_secret_enc` is the AES-256-GCM sealed
-- `nonce || ciphertext+tag` (a fresh random 96-bit nonce per row), sealed with the master key from
-- `GT_SECRET_KEY` (`crypto.rs`, the SAME infra the provider store reuses). A database leak yields no
-- usable secret; it is unsealed only in memory when the token endpoint verifies a client.
--
-- `redirect_uris` is a TEXT[] of the client's registered callback URLs. Validation is EXACT MATCH
-- with NO wildcards (enforced in `oauth_client.rs` at registration and on lookup): an incoming
-- `redirect_uri` must equal one of these strings byte-for-byte, closing the open-redirect hole a
-- prefix/wildcard match would leave. `scopes` is the TEXT[] of scopes the client may request.
-- `enabled` gates whether the client may complete a flow (a soft revoke that keeps the row for
-- audit; `revoke` deletes it outright). Times are TIMESTAMPTZ to match `oauth_providers`.
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.
CREATE TABLE IF NOT EXISTS public.oauth_clients (
    client_id           TEXT        PRIMARY KEY,
    client_secret_enc   BYTEA       NOT NULL,
    display_name        TEXT        NOT NULL DEFAULT '',
    redirect_uris       TEXT[]      NOT NULL,
    scopes              TEXT[]      NOT NULL DEFAULT '{}',
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
