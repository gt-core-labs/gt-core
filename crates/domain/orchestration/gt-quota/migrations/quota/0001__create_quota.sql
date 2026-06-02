-- Step 6.c base schema: `accounts` + `token_usage` + indexes (module-owned copy for
-- QuotaModule, hq-mod-refactor.5). Verbatim from gt-store-pg's 20260527000001_init_quota.sql
-- so the module is the single declaration source; gt-store-pg keeps the transitional applied
-- copy until hq-mod-migrate consolidates. Do NOT edit applied SQL — add a new file instead.
--
-- Schema-per-ws (hq-mt-data.4, docs/04 §15): quota is per-workspace operational data — each
-- tenant runs its own crew burning its own provider accounts, so the tables are defined ONCE
-- in the `ws_default` template schema. `gt_create_workspace_schema` (hq-mt-data.2) clones the
-- template structure into every `ws_<slug>`, and the `WorkspacePool` sets `search_path` to the
-- tenant schema on checkout, so the port reads/writes unqualified `accounts` / `token_usage`
-- that resolve to the tenant's own copy. NO `workspace_id` column and NO FK to `workspaces.id`:
-- tenant isolation is structural (separate schema), not a `WHERE` predicate.
--
-- WHY per-ws and not a cross-tenant `public` catalog (the open question §15 settles): a
-- provider account is a token bucket owned by ONE tenant, not a town-global pool shared across
-- workspaces, so it carries no cross-tenant identity that would force it into `public` (which
-- holds only genuinely shared catalogs like `workspaces`). This SUPERSEDES the obsolete
-- `account_id -> (workspace_id, account)` composite-key wording in hq-mt-runtime.7, which
-- predates the §15 schema-per-ws resolution (2026-05-31): the workspace dimension is the schema
-- itself, never a column. Same fate as hq-mt-data.5's retired FK-model wording.
--
-- `IF NOT EXISTS` is intentional for the transition off the old inline `ensure_schema()`:
-- a database that already ran that function holds the tables but no `_sqlx_migrations`
-- row, so a bare `CREATE TABLE` would collide. Idempotent DDL lets sqlx record this
-- migration as applied without fighting a pre-existing schema.

-- The template schema the per-workspace provisioner clones from. Idempotent so the first
-- template table to be migrated bootstraps it; `gt_create_workspace_schema` (hq-mt-data.2)
-- only reads from `ws_default`, it does not create it.
CREATE SCHEMA IF NOT EXISTS ws_default;

-- `window_json` not `window`: WINDOW is a reserved keyword in Postgres (window functions),
-- so an unquoted `window` column is a syntax error. The name carries the encoding (JSONB
-- round-tripped through serde) and sidesteps quoting every reference.
CREATE TABLE IF NOT EXISTS ws_default.accounts (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    window_json  JSONB NULL
);

-- Heavy-write, append-only: the rate and the block prediction are computed over these
-- samples + the account state, never by an `UPDATE` of a counter (avoids lost-update).
CREATE TABLE IF NOT EXISTS ws_default.token_usage (
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
CREATE INDEX IF NOT EXISTS token_usage_account_ts ON ws_default.token_usage (account_id, ts);
CREATE INDEX IF NOT EXISTS token_usage_session_ts ON ws_default.token_usage (session_id, ts);
