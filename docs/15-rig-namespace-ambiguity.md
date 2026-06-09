# 15 · Rig namespace ambiguity — bead prefix vs canonical rig id

Related: epic `hq-rig-isolation`, original symptom `hq-rig-isolation.5`.

## The two namespaces

Two different strings identify the same rig:

| Prefix (bead id token) | Canonical rig id | Equal? |
|------------------------|-----------------|--------|
| `gw` | `gtweb` | **no** |
| `gm` | `gtmcp` | **no** |
| `gp` | `gtproxy` | **no** |
| `gt` | `gt` | yes |
| `hq` | `hq` | yes |

For `gt` and `hq` the two happen to be the same string, so no mismatch ever fires. For
`gw`, `gm`, and `gp` they diverge — that divergence is the root of every cross-namespace
comparison failure described below.

**Prefix** — the leading token of a bead id, extracted at create time by
`SUBSTRING_INDEX(id, '-', 1)`. Persisted into `issues.rig` in
`gt-store-dolt/src/issues_repo.rs`. Example: bead `gw-ui-redesign.1` → `issues.rig = "gw"`.

**Canonical rig id** — the `name` column in the `rigs` PG table. Exposed by
`rig.lookup-by-prefix {prefix: "gw"} → {name: "gtweb"}`. Used as:

- `Session.rig` — the string branded into every polecat session (`SpawnTemplate.rig`,
  populated from `GT_RIG` env var at sling time,
  `gt-polecat/src/lifecycle.rs`).
- `GT_RIG` environment variable inside every polecat.
- `X-Rig` HTTP header — written into `.mcp.json` at sling time (`worktree.rs:195`) and
  at role-terminal launch (`terminal.rs:467`) from `Session.rig`.

## Where the mismatch fires

### 1. `issues.list` implicit rig scoping

`dispatch.rs:213` wires implicit rig scoping (hq-rig-isolation.7):

```rust
rig: a.rig.or_else(|| request_rig.map(str::to_string)),
```

`request_rig` comes from `X-Rig` → canonical id (e.g., `"gtweb"`).  
The SQL filter (`issues_repo.rs:1574`) is `WHERE rig = :rig` — compared against
`issues.rig` which stores the prefix (`"gw"`).

A polecat running under rig `gtweb` that calls `issues.list.execute` without an explicit
`rig` argument automatically passes `X-Rig: gtweb`. The filter becomes
`WHERE rig = 'gtweb'`, but every `gw-*` bead has `rig = 'gw'` → **zero results**.

### 2. Convoy dispatch rig routing

`convoy.launch` maps each member bead to the rig that will run it. The bead's rig
column stores the prefix (`"gw"`); the scheduler and polecat supervisor operate on the
canonical id (`"gtweb"`). When the dispatch bridge compares bead rig against a live rig
record, a string equality check `"gw" == "gtweb"` fails, sending the work to the wrong
target. This was the original symptom fixed in `hq-rig-isolation.5` (commit `ac0178b` —
the fix derived the rig in the dispatch path to avoid the direct comparison, but did not
correct the stored value).

### 3. REST `?rig=` query parameter

Any REST endpoint that forwards `?rig=<value>` to `IssueFilter.rig` suffers the same
mismatch when callers pass the canonical name. A human typing `?rig=gtweb` gets 0 beads;
`?rig=gw` returns results. The two forms are not interchangeable via any current alias
lookup.

### 4. SSE streams and stats

`/stream?channel=issues` and the stats aggregation (`gt-issues/src/stats.rs`) both filter
by `rig`. The same column stores the prefix, so canonical-name filters miss all
non-identity-prefix rigs.

## Current state after hq-rig-isolation

The epic shipped rig isolation across several layers but left `issues.rig` storing the
bead prefix, not the canonical id. The collision-free rigs (`gt`, `hq`) hide the problem;
the asymmetric ones (`gw`, `gm`, `gp`) expose it whenever implicit X-Rig scoping or a
canonical-name filter is used.

## Recommended resolution

**Canonicalize `issues.rig` to the canonical rig id** — a single backfill migration
plus a one-line change to the create path:

1. At bead create time (`issues_repo.rs:630–682`): resolve prefix → canonical id via
   `prefix_owner` and persist the canonical id instead of the raw prefix.
2. Backfill existing rows: `UPDATE issues SET rig = <canonical> WHERE rig = <prefix>` for
   each asymmetric pair (`gw→gtweb`, `gm→gtmcp`, `gp→gtproxy`).
3. `?rig=` and `X-Rig` callers already pass the canonical id — no change needed there.
4. `rig.lookup-by-prefix` stays as a utility for resolving a bead id prefix in other
   contexts (CLI helpers, etc.).

This brings `issues.rig` into the same namespace as `Session.rig`, `GT_RIG`, and
`X-Rig`, making every string comparison safe without an alias lookup.

Track the backfill + create-path fix as a dedicated bead (not this one — this bead is
documentation only, no behaviour change).
