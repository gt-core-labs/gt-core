-- email #0005 — scheduled-report SCHEDULES persisted in the DB (gtcore-915232,
-- epic gtcore-68bcf7 "los report schedules se pierden en cada redeploy").
--
-- Before this, the schedule LIST lived in `system_config.json` on an ephemeral
-- path: every redeploy lost it, and it was single-file (not multi-tenant). This
-- table is the durable, workspace-keyed home for the same `ReportSchedule`
-- shape the MCP `report.schedules.*` tools and the System REST surface drive —
-- one row per schedule, keyed by its ulid `id`, scoped by `workspace`.
--
-- Public schema (like `report_subscriptions`): a flat table keyed by a
-- `workspace` column, NOT a per-tenant template — the scheduler daemon iterates
-- every row across tenants on each tick. `last_sent_date` is the daemon's
-- at-most-once guard, persisted here so a redeploy never re-sends; `once`
-- auto-disable persists via `enabled = FALSE`.
CREATE TABLE IF NOT EXISTS report_schedules (
    id                TEXT        PRIMARY KEY,
    workspace         TEXT        NOT NULL DEFAULT 'default',
    kind              TEXT        NOT NULL DEFAULT 'planning-digest',
    mode              TEXT        NOT NULL DEFAULT 'daily',
    n_days            INTEGER     NOT NULL DEFAULT 1,
    date              TEXT,
    weekday           SMALLINT    NOT NULL DEFAULT 1,
    day_of_month      SMALLINT    NOT NULL DEFAULT 1,
    start_date        TEXT,
    hour              SMALLINT    NOT NULL DEFAULT 8,
    minute            SMALLINT    NOT NULL DEFAULT 0,
    tz_offset_minutes INTEGER     NOT NULL DEFAULT -300,
    rig               TEXT        NOT NULL DEFAULT 'hq',
    enabled           BOOLEAN     NOT NULL DEFAULT TRUE,
    last_sent_date    TEXT,
    -- Per-schedule recipient list, a JSON array of addresses encoded as text by
    -- the composition edge (the kernel store stays serialization-free, like its
    -- siblings). NULL ⇒ fall back to the workspace's global enabled
    -- `report_subscriptions`.
    subscribers       TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Daemon tick + tenant-scoped CRUD list both key on workspace; created_at gives
-- the stable list order.
CREATE INDEX IF NOT EXISTS report_schedules_ws_created_idx
    ON report_schedules (workspace, created_at);
