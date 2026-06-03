# 10 · Per-rig knowledge graph (tool-agnostic, agent-custodied)

Epic `hq-graphrig`. When a repo is attached to a rig, a **custodian agent** keeps a
codebase knowledge graph fresh for it; every other agent **only queries** the graph for
context. Two things stay replaceable:

1. **The graph tool** — graphify today, swap by writing a new adapter + flipping config.
2. **The custodian agent** — Haiku today, swap by a launch binding (config, not code).

Naming rule: the neutral layer is always `graph*` (`gt-graphindex`, `GraphIndexer`,
`gt-graphwarden`, `graph.*` MCP, `.graphindex/`); the word `graphify` appears **only** inside
the one concrete adapter and its real artifact paths.

## Pieces

| Concern | Where | Notes |
|---|---|---|
| Tool-neutral port | `crates/kernel/gt-graphindex` · `GraphIndexer` | `build/update/query/explain/status`; `InMemoryGraphIndexer` test double. Kernel tier so a *role* can depend on it without a domain→domain edge (docs/03 Rule 4). |
| graphify adapter | same crate, feature `graphify` · `GraphifyIndexer` | Shells the repo's `.graphify-venv` python. `build`/`update` run the deterministic **AST** path only — semantic (LLM) re-extraction is the custodian agent's job, not a library's. |
| Custodian role | `crates/domain/roles/gt-graphwarden` | Event-sourced (gt-witness shape). Tracks per-rig freshness: `RigRegistered` → `MarkedStale` → `Refreshed` → `Unregistered`, kebab `.v1` kinds, pure replay fold (gate green). |
| Read query surface | `crates/modules/gt-composition/src/mcp/graph.rs` · `GraphHandler` | MCP `graph.query / explain / status / list`. Resolves rig→repo_dir by replaying the warden's events. **Read-only by construction** — no graph write tool is exposed, so "agents only query" holds. |
| Ignore propagation | `gt-graphindex::artifacts` · `patterns_for` / `ensure_ignored` | Single source of ignore patterns. `ensure_ignored(repo, tool)` writes them into a repo's local `.git/info/exclude` (never its tracked `.gitignore`, since a rig may be a repo we don't own). |

## The two swap points

- **Swap the tool:** implement `GraphIndexer` for the new tool in `gt-graphindex` behind its
  own feature, construct it where `GraphifyIndexer::new()` is wired
  (`gt-composition/src/bin/gt-mcp-server.rs`). The warden, the reactor, the MCP surface, and
  the ignore-propagation all keep working unchanged — they name only the trait. `patterns_for`
  learns the new tool's artifacts; the umbrella `.graphindex/` already covers it.
- **Swap the agent:** the warden role only records *what* needs (re)indexing; *who* runs the
  index is a launch binding the composition edge resolves (`graph.agent.backend`, default
  `haiku`). Changing it is config, not code.

## The freshness loop (intended)

```
rig attached ──▶ RigEvent::Added ──▶ reactor: GraphIndexer.build + ensure_ignored
                                            + GraphWarden.RegisterRig
merge lands  ──▶ merge-complete event ──▶ warden MarkStale ──▶ edge runs the configured agent
                                            ──▶ GraphIndexer.update ──▶ warden MarkRefreshed
other agent  ──▶ graph.query (read-only) ──▶ context, no writes
```

## Status (2026-06-03)

Delivered + merged: the port + graphify adapter + ignore patterns (`hq-graphrig.1/.2/.3`),
the warden role with replay gate (`.6`), ignore propagation (`.11`), and the read-only MCP
query surface (`.10`). The hook dispatcher (`.4`) and the merge-complete signal (`.5`) were
already provided by `hq-mod-hooks.9` and `hq-core-port.5` respectively.

**Open** — the live trigger automation (`.7` warden bus-observer, `.8` composition reactor,
`.9` agent binding): the codebase routes cross-domain reactions via bus Plugin observers on
versioned events, but `merge.merged.v1` carries only `{bead, sha}` — no rig/workspace — so a
Plugin cannot route a merge to the right rig's graph. Resolution chosen: add `workspace_id`
to `MergeEvent` (server-injected), then wire the reactor. Tracked by
`hq-gap-merge-merged-v1-lacks-rig-workspace-...`. Until then the graph is built/queried but
not auto-refreshed on merge.
