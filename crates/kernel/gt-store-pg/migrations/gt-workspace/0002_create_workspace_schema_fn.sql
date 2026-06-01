-- gt-workspace #0002 — per-workspace schema provisioning function.
--
-- `gt_create_workspace_schema(ws)` materializes a tenant's Postgres schema by
-- cloning the structure of the default workspace's schema (the template). It is
-- the runtime counterpart to `gt_store_pg::schema_for` (hq-mt-data.1): both map a
-- workspace slug to `ws_<slug>` (hyphens → underscores), so a schema this
-- function creates is exactly the one a `WorkspacePool` later sets `search_path`
-- to.
--
-- Cloning is structure-only: `CREATE TABLE ... LIKE template INCLUDING ALL`
-- carries column types, defaults, NOT NULL, CHECK, indexes, and identity, but
-- NOT data and NOT foreign keys (LIKE never copies FK constraints). Per-tenant
-- tables are otherwise independent: each workspace gets its own copy of every
-- table that exists in the template schema.
--
-- Idempotent: `CREATE SCHEMA IF NOT EXISTS` + `CREATE TABLE IF NOT EXISTS`, so
-- re-provisioning an existing workspace is a no-op rather than an error. New
-- tables added to the template after a workspace was created are picked up on
-- the next call.
--
-- The template is the default workspace's schema (`ws_default`). It is empty
-- until projection tables are migrated under per-workspace schemas
-- (hq-mt-data.4/.5); cloning an empty template simply creates the bare schema,
-- which is correct.
CREATE OR REPLACE FUNCTION gt_create_workspace_schema(ws TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    -- Mirror `schema_for`: 'ws_' prefix + hyphens replaced with underscores.
    target   TEXT := 'ws_' || replace(ws, '-', '_');
    template TEXT := 'ws_default';
    tbl      TEXT;
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', target);

    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = template
        ORDER BY tablename
    LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I (LIKE %I.%I INCLUDING ALL)',
            target, tbl, template, tbl
        );
    END LOOP;
END;
$$;
