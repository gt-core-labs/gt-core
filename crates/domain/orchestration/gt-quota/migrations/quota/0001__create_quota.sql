-- Step 6.c base schema: `accounts` + `token_usage` + indexes (module-owned copy for
-- QuotaModule, hq-mod-refactor.5). Verbatim from gt-store-pg's 20260527000001_init_quota.sql
-- so the module is the single declaration source; gt-store-pg keeps the transitional applied
-- copy until hq-mod-migrate consolidates. Do NOT edit applied SQL — add a new file instead.
--
-- `IF NOT EXISTS` is intentional for the transition off the old inline `ensure_schema()`:
-- a database that already ran that function holds the tables but no `_sqlx_migrations`
-- row, so a bare `CREATE TABLE` would collide. Idempotent DDL lets sqlx record this
-- migration as applied without fighting a pre-existing schema.

-- `window_json` not `window`: WINDOW is a reserved keyword in Postgres (window functions),
-- so an unquoted `window` column is a syntax error. The name carries the encoding (JSONB
-- round-tripped through serde) and sidesteps quoting every reference.
CREATE TABLE IF NOT EXISTS accounts (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    window_json  JSONB NULL
);

-- Heavy-write, append-only: the rate and the block prediction are computed over these
-- samples + the account state, never by an `UPDATE` of a counter (avoids lost-update).
CREATE TABLE IF NOT EXISTS token_usage (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT        NOT NULL,
    session_id      TEXT        NOT NULL,
    model           TEXT        NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    input_tokens    BIGINT      NOT NULL,
    output_tokens   BIGINT      NOT NULL,
    cache_read      BIGINT      NOT NULL DEFAULT 0,
    cache_creation  BIGINT      NOT NULL DEFAULT 0
);

-- `(account_id, ts)` covers the per-account rollup; `(session_id, ts)` the per-session
-- breakdown. The spec's materialized view (`account_window_usage`) lands in a later
-- migration once there is real data.
CREATE INDEX IF NOT EXISTS token_usage_account_ts ON token_usage (account_id, ts);
CREATE INDEX IF NOT EXISTS token_usage_session_ts ON token_usage (session_id, ts);
