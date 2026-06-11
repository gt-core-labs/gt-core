-- invites #0001 — collaborator invites (hq-4231c1, epic hq-56b5ee).
--
-- PUBLIC schema (like notifications/email_outbox): one table keyed by a
-- workspace column. An owner/admin mints an invite (token + email + role); the
-- token travels by email (via the email_outbox) and is consumed exactly once
-- during the gt-login-authenticated accept, which binds the verified identity
-- to the workspace membership (gt-auth add_workspace_member). Identity is
-- ALWAYS gt-login — this table never stores credentials.
--
-- Status machine: pending → accepted (the one-shot consume)
--                 pending → revoked (admin recall)
--                 pending → expired (lazy, on touch after expires_at)
CREATE TABLE IF NOT EXISTS workspace_invites (
    id          TEXT        PRIMARY KEY,
    -- The unguessable capability the email carries. Never logged.
    token       TEXT        NOT NULL UNIQUE,
    workspace   TEXT        NOT NULL,
    -- The invited identity (must sign in/up through gt-login as this email).
    email       TEXT        NOT NULL,
    -- The membership role the accept binds: the Kanban collaborator ladder.
    role        TEXT        NOT NULL
                CHECK (role IN ('viewer', 'commenter', 'editor', 'admin')),
    status      TEXT        NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'accepted', 'revoked', 'expired')),
    expires_at  TIMESTAMPTZ NOT NULL,
    created_by  TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at TIMESTAMPTZ NULL,
    -- The gt-login identity that consumed the token (normally = email).
    accepted_by TEXT        NULL
);

-- Admin listing per workspace.
CREATE INDEX IF NOT EXISTS workspace_invites_workspace_idx
    ON workspace_invites (workspace, created_at DESC);

-- Accept path: one-shot token consume.
CREATE INDEX IF NOT EXISTS workspace_invites_token_idx
    ON workspace_invites (token)
    WHERE status = 'pending';
