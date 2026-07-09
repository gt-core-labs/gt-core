-- OAuth Authorization Server: ephemeral authorization codes (hq-oauth-as.2).
--
-- When gt acts as an Authorization Server (the inverse of the IdP-client flow in
-- oauth_authz_state), a successful /oauth/authorize stores a short-lived, one-shot
-- authorization code here.  The /oauth/token endpoint consumes it (DELETE-on-read)
-- to mint access + refresh tokens for the downstream OAuth client (e.g. Claude.ai).
--
-- One-shot + TTL: the code is DELETEd on read (replay rejected), and rows past
-- expires_at are swept periodically (same pattern as oauth_authz_state).
CREATE TABLE IF NOT EXISTS public.oauth_as_codes (
    code            TEXT        PRIMARY KEY,
    client_id       TEXT        NOT NULL,
    user_sub        TEXT        NOT NULL,
    user_workspace  TEXT        NOT NULL,
    user_scopes     TEXT        NOT NULL DEFAULT '',
    redirect_uri    TEXT        NOT NULL,
    code_challenge  TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);
