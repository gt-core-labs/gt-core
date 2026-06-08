-- hq-epic.auth-refactor.2: per-workspace OAuth/OIDC provider config.
--
-- NULL workspace_id = global (current behaviour, shown on every workspace's login page).
-- Non-NULL workspace_id = scoped to one workspace only (enterprise SSO: each org owns its IdP).
--
-- No FK to a workspaces table: provider rows are GLOBAL (public schema, no search_path scope),
-- and workspace existence is enforced by the application layer, not the DB.
ALTER TABLE public.oauth_providers
    ADD COLUMN IF NOT EXISTS workspace_id TEXT;
