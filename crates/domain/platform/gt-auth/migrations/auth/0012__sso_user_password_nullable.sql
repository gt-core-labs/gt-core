-- hq-epic.auth-refactor.4: SSO / JIT user provisioning.
--
-- OAuth/OIDC users are provisioned on first login by find_or_create_sso_user.  They have no
-- password (they authenticate via IdP, never via email+password), so password_hash must be
-- nullable.  Existing rows (email+password users) keep their non-NULL hash unchanged; the
-- `DEFAULT NULL` only affects future INSERTs that omit the column.
--
-- `IF NOT EXISTS`-style: ALTER COLUMN is idempotent when the column is already nullable, so
-- re-applying on an already-migrated schema is safe.
ALTER TABLE public.users
    ALTER COLUMN password_hash DROP NOT NULL;
