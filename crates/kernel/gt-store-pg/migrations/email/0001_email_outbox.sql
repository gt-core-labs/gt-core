-- email #0001 — the transport-agnostic email outbox (hq-f24599, epic hq-56b5ee).
--
-- PUBLIC schema (like notifications/mcp_audit): one table keyed by a workspace
-- column. Producers (report delivery, subscriptions, notify) INSERT rows here;
-- the drain daemon pushes due rows through the EmailTransport seam (gt-notify).
-- Nothing in the platform talks to a mail server directly — the SMTP server is
-- PENDING and must not block anything (ADR hq-423a4b D8).
--
-- Status machine: pending → sending → sent
--                            └→ retry (backoff, send_at pushed forward) → sending …
--                            └→ failed (attempts exhausted)
--                  pending|retry → cancelled (operator cancel)
CREATE TABLE IF NOT EXISTS email_outbox (
    id           TEXT        PRIMARY KEY,
    workspace    TEXT        NOT NULL DEFAULT 'default',
    recipient    TEXT        NOT NULL,
    subject      TEXT        NOT NULL,
    body         TEXT        NOT NULL DEFAULT '',
    -- Optional pointer to a template/document the body was (or will be) rendered
    -- from; the outbox stores the rendered body, this is provenance.
    template_ref TEXT        NULL,
    send_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT        NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'sending', 'sent', 'retry', 'failed', 'cancelled')),
    attempts     INT         NOT NULL DEFAULT 0,
    last_error   TEXT        NULL,
    created_by   TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ NULL
);

-- The drain's hot path: due rows still owed a delivery.
CREATE INDEX IF NOT EXISTS email_outbox_due_idx
    ON email_outbox (send_at)
    WHERE status IN ('pending', 'retry');

-- Operator listing per workspace.
CREATE INDEX IF NOT EXISTS email_outbox_workspace_idx
    ON email_outbox (workspace, created_at DESC);
