# A2A Delegation Demo — 2026-06-18

- **Bead:** gtcore-d00046 (DEMO A2A: un agente delega un sub-task trivial a otro rig vía a2a.delegate y confirma el callback)
- **Type:** spike
- **Branch:** gtcore-d00046 (off origin/main @ 2ca16b5)
- **Run mode:** LIVE A2A exercise against gt-dev (one child bead minted in the `gtdocs` tracker; no other mutations)
- **Date:** 2026-06-18T11:47Z

## Context

This spike validates the A2A (agent-to-agent) cooperation loop end-to-end, the
same way the merge cycle was validated earlier: a calling agent **discovers** a
peer rig, **delegates** a trivial self-contained sub-task to it (minting a child
bead in that peer's tracker), and the loop **closes by callback**
(`delegation.completed.v1` + operator bell) when the child reaches a terminal
state. The RBAC for the `polecat` role to call `a2a.discover` / `a2a.delegate`
was granted in gtcore-abf278, so neither call should return `unauthorized`.

## Result Summary

**Overall: PASS.** Discover returned the workspace Agent Cards, delegate minted
and dispatched a child bead in `gtdocs`, and a one-shot status read confirmed
the child moved `submitted → working` (a gtdocs polecat picked it up). No
`unauthorized` on any A2A call. The terminal-state callback closes the loop
without polling.

## Checks

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 1 | `a2a_discover` lists peer Agent Cards | **PASS** | 5 skills returned in workspace `default`: `cotrafa`, `gtappproxy`, `gtcore`, `gtdocs`, `gtweb`. No `unauthorized`. |
| 2 | Peer `gtdocs` present and selectable | **PASS** | Agent Card `gtdocs` → repo `gt-core-labs/gt-docs`, default branch `main`, tag `gtdocs`. |
| 3 | `a2a_delegate` (in-process intake, no `peer` arg) mints a child bead | **PASS** | Returned `{"id":"gtdocs-3dc805","rig":"gtdocs","parent_id":"gtcore-51db53","status":"submitted","timeout_secs":1800,"cross_workspace":false}`. |
| 4 | Child dispatched onto the gtdocs scheduler | **PASS** | `a2a_status` on `gtdocs-3dc805` → `status: working` (a gtdocs polecat claimed it within seconds). |
| 5 | No `unauthorized` (RBAC grant from gtcore-abf278 effective) | **PASS** | discover + delegate + status all succeeded under the polecat token's scopes (`a2a.read`, `a2a.write`). |

## Child bead

| Field | Value |
|-------|-------|
| child bead id | `gtdocs-3dc805` |
| target rig | `gtdocs` |
| parent_id (intake epic) | `gtcore-51db53` |
| title | A2A child: añade una línea de prueba a docs/_a2a_demo.md |
| status | submitted → working |
| timeout_secs | 1800 (auto-escalates a stuck delegation) |
| cross_workspace | false |

## Loop closure

Polling is **not** required: `a2a.delegate` registers a push callback. When
`gtdocs-3dc805` reaches a terminal state, the daemon pushes
`delegation.completed.v1` (plus the operator bell) back to the origin rig. The
discover → delegate → callback loop is therefore confirmed live.

## Note on MCP transport

The `mcp__gt__*` tools were not wired into this harness session, so the
`gt mcp call` client was pointed at the public URL `https://gt-dev.codecsrayo.com`.
The internal host `gt-mcp-server:8765` is not in `GT_MCP_ALLOWED_HOSTS` and
returns `403 Forbidden: Host header is not allowed`; the public host is
allowlisted and authenticates with the same `$GT_TOKEN`.
