-- hq-idp-db.1: DB-backed OAuth/OIDC login providers (admin-configurable).
--
-- The OAuth/OIDC login adapter (`oauth.rs`, hq-platform-hardening.3) read its single provider
-- from the environment (`OidcConfig::from_env`). This epic moves that config into the database so
-- an admin can register MANY providers — Google/GitHub/Microsoft presets or a generic OIDC IdP —
-- without a redeploy, and each appears as a login button. This migration lands the store.
--
-- Scope is GLOBAL (system-wide, NOT per-workspace): a login provider is deploy infrastructure,
-- not tenant projection data, so — like `public.users` (0005) — the table lives in `public` with
-- NO `workspace_id` column. It is shared, never cloned into each `ws_<slug>` schema.
--
-- The `client_secret` is the one secret here, so it is NEVER stored in cleartext: `client_secret_enc`
-- is the AES-GCM sealed `nonce || ciphertext` (a random 96-bit nonce per row prepended to the
-- ciphertext), sealed with the master key from `GT_SECRET_KEY` (`crypto.rs`). A database leak
-- therefore yields no usable client secret; it is unsealed only in memory when a row is turned into
-- an `OidcConfig` for the handshake.
--
-- `kind` is the provider variant ('google'/'github'/'microsoft'/'generic'); presets bake their
-- authorize/token/userinfo endpoints, the generic kind carries admin-supplied endpoints. `issuer`
-- and `scopes` mirror the `OidcConfig` shape. `enabled` gates whether the provider shows as a login
-- button. Times are TIMESTAMPTZ (the rest of `public` uses BIGINT epochs, but this admin-CRUD table
-- has no time-as-data replay requirement, so a native timestamp keeps the column self-describing).
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file (sqlx
-- checksum-validates) — add a new migration instead.
CREATE TABLE IF NOT EXISTS public.oauth_providers (
    id                  TEXT        PRIMARY KEY,
    kind                TEXT        NOT NULL,
    display_name        TEXT        NOT NULL,
    client_id           TEXT        NOT NULL,
    client_secret_enc   BYTEA       NOT NULL,
    issuer              TEXT        NOT NULL,
    authorize_endpoint  TEXT        NOT NULL,
    token_endpoint      TEXT        NOT NULL,
    userinfo_endpoint   TEXT        NOT NULL,
    scopes              TEXT        NOT NULL DEFAULT '',
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
