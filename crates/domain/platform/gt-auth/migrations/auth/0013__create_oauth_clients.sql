-- OAuth clients (Authorization Server surface, hq-oauth-as.1): applications that
-- authenticate users THROUGH gt (e.g. Claude.ai remote MCP connector).  Conceptually
-- the inverse of `oauth_providers` (upstream IdPs gt logs into).
--
-- Each row is a registered OAuth 2.0 client that may start an authorization-code flow
-- against gt's /oauth/authorize endpoint.  The client_secret is AES-256-GCM sealed at
-- rest (same crypto as oauth_providers), and redirect_uris is a strict allowlist
-- checked on /oauth/authorize (exact match, no wildcards — RFC 6749 §3.1.2.3).
CREATE TABLE IF NOT EXISTS public.oauth_clients (
    client_id           TEXT        PRIMARY KEY,
    client_secret_enc   BYTEA       NOT NULL,
    display_name        TEXT        NOT NULL,
    redirect_uris       TEXT[]      NOT NULL DEFAULT '{}',
    allowed_scopes      TEXT        NOT NULL DEFAULT '',
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
