-- hq-vcs-connections.1: per-workspace VCS connections (GitHub App / PAT fallback).
--
-- The foundation of the VCS-connections epic (hq-vcs-connections): one connection per workspace
-- the rest of the epic references, so the server has a credential source to clone private repos
-- from. It MIRRORS `public.oauth_providers` (auth/0006 + the `workspace_id` column from auth/0011):
-- a single GLOBAL table in `public` carrying an optional `workspace_id` (NULL = global,
-- non-NULL = scoped to one tenant), NOT a schema-per-tenant table. A connection is deploy /
-- workspace-config data resolved by the server, not per-tenant projection data, so `public` with
-- an explicit `workspace_id` is the right home — the same call the OAuth provider store made.
--
-- `kind` is the connection variant: `github_app` (the server mints ephemeral installation tokens
-- JIT — NO long-lived credential is stored, only the `installation_id`) or `pat` (the fallback:
-- a Personal Access Token sealed at rest). `installation_id` + `account_login` are the GitHub App
-- coordinates (NULL for a PAT connection).
--
-- `secret` is the ONE secret here and is NEVER stored in cleartext: it is the AES-GCM sealed
-- `nonce || ciphertext` (a random 96-bit nonce per row prepended to the ciphertext), sealed with
-- the master key from `GT_SECRET_KEY` — exactly as `oauth_providers.client_secret_enc`, reusing
-- `gt_auth`'s `seal`/`unseal` helper (no new crypto). It is populated ONLY for `kind = 'pat'`; a
-- `github_app` connection holds no secret (NULL), since its tokens are minted on demand and never
-- persisted. A database leak therefore yields no usable token for a GitHub App connection and only
-- a sealed (useless without the key) blob for a PAT.
--
-- `status` gates whether the connection is usable (`active` / `disabled` / `revoked`); times are
-- TIMESTAMPTZ like the `oauth_providers` admin-CRUD table (no time-as-data replay requirement).
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file (the module
-- migration runner records a checksum) — add a new migration instead.
CREATE TABLE IF NOT EXISTS public.vcs_connections (
    id                  TEXT        PRIMARY KEY,
    workspace_id        TEXT,
    kind                TEXT        NOT NULL,
    installation_id     TEXT,
    account_login       TEXT,
    secret              BYTEA,
    status              TEXT        NOT NULL DEFAULT 'active',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The hot read path is "the connections visible to workspace W" (its own scoped rows plus the
-- global ones), mirroring `oauth_providers.list_for_workspace`. Index `workspace_id` so that scan
-- stays cheap as connections accumulate.
CREATE INDEX IF NOT EXISTS vcs_connections_workspace_id_idx
    ON public.vcs_connections (workspace_id);
