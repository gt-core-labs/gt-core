-- hq-gt-login-oauth.2: the one-shot CLI hand-off code store.
--
-- The `gt login` browser flow can't read the token from a URL fragment (a loopback HTTP server
-- never receives the `#fragment`), and putting the access+refresh pair in the loopback query string
-- would leak long-lived secrets into shell history / proxy logs. So the callback, when it finishes a
-- CLI handshake, parks the freshly minted token pair here under a short opaque `code` and 302s only
-- that `code` to the loopback. The CLI then redeems it ONCE at `POST /auth/cli/exchange`.
--
-- Scope is GLOBAL (like oauth_authz_state, 0007) and the row is short-lived (~60s TTL) and ONE-SHOT
-- — the exchange DELETEs it on read, so a replayed `code` finds nothing (a captured loopback URL is
-- useless after the first redemption). Durable (Postgres) so the redemption survives a redeploy and
-- works across replicas, consistent with the rest of the auth state.
--
-- `IF NOT EXISTS` keeps the boot-time apply idempotent; never edit an applied file — add a new
-- migration instead.
CREATE TABLE IF NOT EXISTS public.oauth_cli_code (
    code            TEXT        PRIMARY KEY,
    access_token    TEXT        NOT NULL,
    refresh_token   TEXT        NOT NULL,
    token_type      TEXT        NOT NULL,
    expires_in      BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);
