# 09 — Multi-tenant storage layout (deployment)

How the three persistent stores partition per workspace **on the deployed
containers**. [docs/04 §15](04-non-negotiables.md) fixes the *isolation
invariant* (per-workspace data must be structurally isolated, not filtered by a
`workspace_id` `WHERE` clause); this doc fixes the *deployment layout* that
realizes it: which volume holds what, how a tenant's partition is named, and how
it is provisioned. It is the binding spec the compose/entrypoint host
implements.

## Shared instance, partitioned inside

Every store runs as **one shared instance** — a single Postgres, a single Dolt
sql-server, a single event-log volume — and partitions workspaces *inside* it.
Isolation is logical (schema / database / subdirectory), not a separate server
per tenant. Process and connection overhead therefore stays O(1) in the number
of workspaces, not O(workspaces); a new tenant is a cheap `CREATE`, never a new
container.

| Store | Shared resource | Per-workspace partition | Provisioned by |
|-------|-----------------|-------------------------|----------------|
| Postgres (projections) | single PG, `gt-pgdata` volume | schema `ws_<slug>` (`gt_store_pg::schema_for`) | `gt_create_workspace_schema(ws)` — `hq-mt-data.2` |
| Dolt (issues / versioned domain) | single `dolt sql-server`, `dolt-data` volume | database `hq_<ws>` | `hq-mt-data.6/.7/.12` |
| Event log (append-only) | `gt-eventlog` named volume at `/var/lib/gt-core` | subdir `/var/lib/gt-core/<ws>/` with daily segments `events-YYYY-MM-DD.jsonl` | created on first workspace resolution — `hq-mt-data.8` |

The bootstrap `default` workspace occupies `ws_default` / `hq_default` /
`/var/lib/gt-core/default/`, so a single-tenant deployment is just the
multi-tenant layout with one workspace.

## Event-log filesystem layout

```
/var/lib/gt-core/                  # gt-eventlog volume mount
├── default/
│   ├── events-2026-06-01.jsonl
│   └── events-2026-06-02.jsonl
├── <ws-a>/
│   └── events-2026-06-02.jsonl
└── <ws-b>/
    └── events-2026-06-02.jsonl
```

Each workspace gets its own append-only log under its subdirectory, **rotated
into one segment per UTC day** (`events-YYYY-MM-DD.jsonl`). The directory is
created lazily the first time a workspace is resolved
(`mkdir -p /var/lib/gt-core/<ws>/`, idempotent) — the filesystem mirror of how
`gt_create_workspace_schema` provisions a PG schema on demand. The day for a
record is taken from the **event's own `ts`**, not a wall clock, so the writer
stays pure and replay re-routes byte-identically to the same segments. The
reader concatenates segments in chronological order (the ISO segment name sorts
lexicographically = chronologically). Rotation does **not** rewrite the log
([docs/03](03-architecture-guardrails.md) rule: never rewrite the event log) —
it only opens a new file when the day rolls over; old segments stay immutable.

## Compose footprint

No new named volumes. The three volumes already exist and **stay shared**:

- `gt-pgdata` — one PG cluster, many `ws_<slug>` schemas.
- `dolt-data` — one Dolt server, many `hq_<ws>` databases.
- `gt-eventlog` — one volume, one `<ws>/events.jsonl` subdir per workspace.

The only mechanical change a workspace needs at the deployment layer is the
per-workspace event-log directory, which the container entrypoint creates on
boot / on first request rather than via a volume definition. PG schemas and
Dolt databases are created through their own provisioning calls, not compose.

> The compose file and container entrypoint live in the **deployment host**
> (the gastown compose, `COMPOSE_PROJECT_NAME=gastown`, during the cutover
> window), not in this repo. gt-core owns this layout spec; the host wires the
> `mkdir` and mounts. Do not add a `docker-compose.yml` to gt-core for this.

## Backup / disaster-recovery seam

Per-workspace backup (`hq-mt-deploy.6`) and the DR runbook (`hq-mt-deploy.7`)
iterate exactly these three partitions for a given workspace: `pg_dump` the
`ws_<slug>` schema, `dolt dump` the `hq_<ws>` database, and tar the
`/var/lib/gt-core/<ws>/` log directory. A workspace's entire durable state is
the union of those three partitions — nothing tenant-specific lives outside
them.

**Provenance:** `hq-mt-deploy.1`. Realizes the isolation invariant of
[04 §15](04-non-negotiables.md) at the deployment layer; layout follows the
`hq-mt-data` plan (PG schema-per-ws, Dolt DB-per-ws, event-log split). See also
[06-epic-roadmap.md](06-epic-roadmap.md) row 1 (`hq-mt-data`).
