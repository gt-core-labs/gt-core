-- email #0002 — seguimiento subscriptions (hq-8a521a, epic hq-56b5ee).
--
-- A workspace member subscribes to a card / doc / board; the mailbox daemon
-- fans matching feed events out as outbox emails, and replying to one routes
-- back as a comment (the email<->comment bridge). PUBLIC schema like the
-- outbox, keyed by a workspace column.
CREATE TABLE IF NOT EXISTS email_subscriptions (
    id           TEXT        PRIMARY KEY,
    workspace    TEXT        NOT NULL DEFAULT 'default',
    member_email TEXT        NOT NULL,
    target_kind  TEXT        NOT NULL CHECK (target_kind IN ('card', 'doc', 'board')),
    target_id    TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace, member_email, target_kind, target_id)
);

-- Fan-out path: who watches this target.
CREATE INDEX IF NOT EXISTS email_subscriptions_target_idx
    ON email_subscriptions (workspace, target_kind, target_id);
