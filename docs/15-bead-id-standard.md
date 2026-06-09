# 15 — Bead-ID Standard: `{rig}-{hash}` Format

**Status:** active  
**Epic:** `hq-bead-id-standard`

## Problem

The old bead-id format (`{prefix}-{slug}.{n}`, e.g. `gw-ui-redesign.1`) used a
*shorthand prefix* (`gw`) that differed from the canonical rig name (`gtweb`).
This created two disjoint namespaces for the same rig, broke `?rig=gtweb`
filter queries silently, and required agents to look up prefix→name mappings
before routing work.

## New Standard

```
{canonical-rig-name}-{6hex}     e.g.  gtweb-a3f2c1
```

Rules:

1. **First token = canonical rig name.**  
   `SUBSTRING_INDEX(id, '-', 1)` returns the rig's registered name directly —
   no secondary resolution needed.

2. **6-hex random hash** provides uniqueness without requiring the caller to
   invent a human-readable slug. The DB `PRIMARY KEY` constraint catches the
   rare collision; callers retry on that error.

3. **Server-generated.** Callers pass `rig` (canonical name) in
   `issues.create`; the id is minted server-side and returned in the response.

4. **`rig` replaces `prefix` as the routing key.** After
   `hq-bead-id-standard.4`, every registered rig has `prefix == name`, so
   `is_allowed(ws, rig_name)` routes correctly without a separate lookup.

## Wire Shape

`issues.create` (new):

```json
{
  "rig": "gtweb",
  "title": "Add dark mode toggle",
  "issue_type": "task",
  "external_ref": "gtweb-abc123",
  "domain": ["fe.web"],
  "created_by": "actor-id"
}
```

Response:

```json
{ "ok": true, "id": "gtweb-4fa2c0" }
```

### Backward compat

When `id` is supplied explicitly (old format, e.g. `hq-core-host.2`), the old
`{external_ref}.{n}` NN-16 rule still applies. The `rig` field is then derived
from the id's leading token. Both paths coexist indefinitely; new beads should
use the server-generated format.

## Rig Prefix Invariant (bead .4)

Every rig registered after `hq-bead-id-standard.4` must satisfy
`prefix == name`. The `rig.add` command now defaults `prefix` to `name` when
the field is absent. Rigs with underscore names (e.g. `gt_core`) cannot use
their name as a prefix (underscores are not allowed in prefixes per
`validate_prefix`); those must supply an explicit prefix.

## Backfill (bead .3)

`ensure_schema` maps historic shorthand prefixes to canonical rig names on
startup:

| old `issues.rig` | canonical |
|---|---|
| `gw` | `gtweb` |
| `gm` | `gtmcp` |
| `gp` | `gtproxy` |

## Special Cases

- **`hq` namespace** — `hq` is a reserved town-level namespace
  (`RESERVED_RIG_NAMES`), not a rig name. Tracker beads (`hq-*`) continue to
  use `rig = "hq"`. The rig registered as `gt_core` keeps `prefix = "hq"` since
  `gt_core` is not a valid prefix (underscore).
- **`gt` rig** — already had `prefix == name`; no change needed.
