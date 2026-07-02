-- Dedup + lifecycle for operator notifications (gtcore-7a707a).
--
-- The audit of 32 notification rows found (a) the SAME finding re-emitted on every
-- daemon tick for days with no memory of what was already alerted, and (b) no state
-- beyond the boolean `read_at`. This migration gives the row the columns a
-- dedup-aware writer needs:
--
--   fingerprint  — stable dedup key for a recurring finding (NULL = never deduped,
--                  the legacy one-shot behaviour of notify.send / workflow events).
--   state        — new → acked → resolved lifecycle, a closed set mirroring the
--                  `kind` CHECK. A daemon consults it before re-alerting.
--   count        — how many times this fingerprint was seen (repeat counter).
--   last_seen_at — the sliding-window anchor; each re-emission bumps it.
--
-- All additive + idempotent (IF NOT EXISTS / duplicate_object guard) so a replay on
-- an already-migrated database is a no-op.
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS fingerprint  TEXT;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS state        TEXT        NOT NULL DEFAULT 'new';
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS count        INTEGER     NOT NULL DEFAULT 1;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Closed lifecycle set (same shape as the kind CHECK): new = unseen by the operator,
-- acked = the operator has seen it (legacy `read_at`), resolved = the finding cleared.
DO $$ BEGIN
    ALTER TABLE notifications
        ADD CONSTRAINT notifications_state_check CHECK (state IN ('new', 'acked', 'resolved'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Backfill existing rows: anchor the window at creation time and inherit the ack
-- state from the legacy `read_at` boolean so history keeps its meaning.
UPDATE notifications SET last_seen_at = created_at;
UPDATE notifications SET state = 'acked' WHERE read_at IS NOT NULL AND state = 'new';

-- Dedup lookup: newest live row per (workspace, from_role, fingerprint) within the
-- window. Partial — a NULL fingerprint (one-shot notifications) is never an index entry.
CREATE INDEX IF NOT EXISTS notifications_fingerprint
    ON notifications (workspace, from_role, fingerprint, last_seen_at)
    WHERE fingerprint IS NOT NULL;
