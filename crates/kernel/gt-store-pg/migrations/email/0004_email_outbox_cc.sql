-- email #0004 — carbon-copy recipients on an outbox row (gtcore-ecf70d).
--
-- The scheduled report digest now sends ONE email To: the configured sender
-- (GT_SMTP_FROM) with every registered recipient in CC, instead of one row per
-- recipient. `cc` holds those addresses comma-separated (empty = no CC, the
-- prior single-recipient shape). Idempotent so a re-run is a no-op.
ALTER TABLE email_outbox ADD COLUMN IF NOT EXISTS cc TEXT NOT NULL DEFAULT '';
