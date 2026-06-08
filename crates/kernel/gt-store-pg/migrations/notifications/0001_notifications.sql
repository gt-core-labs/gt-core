-- notifications table in PUBLIC schema (not per-workspace, like workspaces/mcp_audit).
-- Stores operator notifications that agents (e.g. mayor) send to the human via
-- notify.send.execute; the web UI polls or SSE-streams these to render a bell icon.
CREATE TABLE IF NOT EXISTS notifications (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace  TEXT        NOT NULL DEFAULT 'default',
    from_role  TEXT        NOT NULL DEFAULT 'mayor',
    title      TEXT        NOT NULL,
    body       TEXT        NOT NULL DEFAULT '',
    kind       TEXT        NOT NULL DEFAULT 'decision' CHECK (kind IN ('decision', 'info', 'alert')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS notifications_workspace_read ON notifications(workspace, read_at);
