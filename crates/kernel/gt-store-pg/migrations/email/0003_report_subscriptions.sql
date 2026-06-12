-- email #0003 — scheduled-report subscribers (hq-562e0b, epic hq-efc379).
--
-- Recipients of the periodic HTML report digest (planning bitácora +
-- analytics). Distinct from email_subscriptions (per-target seguimiento for
-- the mailbox): this is a flat, workspace-keyed recipient list. `enabled`
-- is the operator's send-selection switch — the scheduler mails ONLY enabled
-- rows; a disabled subscriber stays listed (re-enable without re-typing).
CREATE TABLE IF NOT EXISTS report_subscriptions (
    id         TEXT        PRIMARY KEY,
    workspace  TEXT        NOT NULL DEFAULT 'default',
    email      TEXT        NOT NULL,
    enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace, email)
);

-- Scheduler fan-out path: enabled recipients of one workspace.
CREATE INDEX IF NOT EXISTS report_subscriptions_ws_enabled_idx
    ON report_subscriptions (workspace, enabled);
