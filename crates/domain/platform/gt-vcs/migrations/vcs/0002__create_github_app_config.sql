-- hq-61ea43: DB-backed platform GitHub App config.
--
-- The platform GitHub App identity (App ID, URL slug, RS256 private key PEM, webhook secret) used to
-- come ONLY from mounted files / env vars (gt_vcs::GithubAppConfig::from_env). That forced a redeploy
-- to configure the App and left the `/api/v1/connection/github/*` install flow unmounted (404) until
-- the env was set. This table lets an admin set the App config from the UI, no redeploy.
--
-- ONE global row (id = 'default'): the platform App is deploy infrastructure, not tenant data —
-- exactly like public.oauth_providers is global. The two SECRETS are NEVER stored in cleartext:
-- `private_key_enc` / `webhook_secret_enc` are the AES-GCM sealed `nonce || ciphertext` blobs
-- (gt_auth::seal, GT_SECRET_KEY), the same helper the PAT and OAuth client secret use. `app_id` and
-- `app_slug` are public identifiers. A leak of this table yields no usable signing key.
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file — add a new
-- migration instead.
CREATE TABLE IF NOT EXISTS public.github_app_config (
    id                  TEXT        PRIMARY KEY DEFAULT 'default',
    app_id              TEXT        NOT NULL,
    app_slug            TEXT        NOT NULL,
    private_key_enc     BYTEA       NOT NULL,
    webhook_secret_enc  BYTEA,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
