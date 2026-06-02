-- Paso 10 C11c (hq-mc72.12.31): rig catalog table backing `RigRepository` (gt-rig).
--
-- Light-write (a registration is rare), read-mostly. `name` is the identity; `prefix` carries
-- a UNIQUE index so the DB mirrors the catalog's prefix-collision invariant (the port's
-- `prefix_owner` contract). `registered_at` is the `now_secs` epoch stored as BIGINT — the
-- reducer already keeps time as data so replay stays byte-for-byte.
--
-- `IF NOT EXISTS` for the same transition reason as the other migrations; never edit an
-- applied file (sqlx checksum-validates), add a new one.
CREATE TABLE IF NOT EXISTS rigs (
    name            TEXT PRIMARY KEY,
    prefix          TEXT   NOT NULL UNIQUE,
    git_url         TEXT   NOT NULL,
    push_url        TEXT   NULL,
    upstream_url    TEXT   NULL,
    default_branch  TEXT   NOT NULL,
    registered_at   BIGINT NOT NULL
);
