-- hq-idp-db.3: ephemeral OAuth authorize state + PKCE verifier store.
--
-- The public `GET /auth/providers/{id}/authorize` endpoint starts the authorization-code flow: it
-- mints a random anti-CSRF `state` and a PKCE `code_verifier`, redirects the browser to the IdP with
-- `state` + the S256 `code_challenge`, and must remember the verifier so the matching
-- `GET /auth/callback?code&state` can validate the `state` and pass the verifier to the token
-- exchange. That pending handshake is what this table holds.
--
-- Scope is GLOBAL (like `public.oauth_providers`, 0006): the flow is keyed by an opaque `state`, not
-- by tenant. The row is short-lived (~10 min TTL via `expires_at`) and ONE-SHOT — the callback
-- DELETEs it on read, so a replayed `state` finds nothing and is rejected (anti-CSRF + replay
-- defence). It is durable (Postgres, not in-memory) so an in-flight login survives a gt-mcp-server
-- redeploy, consistent with the durable refresh store (hq-platform-hardening.1).
--
-- `redirect_uri` records the app's own callback URL echoed on the exchange, so the callback rebuilds
-- the exact same value the authorize step sent the IdP (the OAuth spec requires they match).
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file — add a new
-- migration instead.
CREATE TABLE IF NOT EXISTS public.oauth_authz_state (
    state           TEXT        PRIMARY KEY,
    code_verifier   TEXT        NOT NULL,
    provider_id     TEXT        NOT NULL,
    redirect_uri    TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);
