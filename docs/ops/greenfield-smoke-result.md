# Greenfield smoke — E2E "functional from zero" run result

> **hq-greenfield-seeds.7** — the proof that the greenfield bring-up
> ([`greenfield-bringup.md`](./greenfield-bringup.md) §5 / [`greenfield-seeds.md`](./greenfield-seeds.md) §6)
> actually delivers a functional platform from an EMPTY cluster, plus the reproducible
> harness that re-proves it on demand. This file records the latest run; re-run the harness
> and update the table below when the seeds or image move on.

---

## The harness

- **`scripts/greenfield-smoke.sh`** — stands up an isolated stack on FRESH volumes + alt
  ports, waits for `/health`, runs the §6 assertions, prints PASS/FAIL per check, and tears
  down (`down -v` scoped to its own project only). Idempotent and safe to re-run. Subcommand
  `teardown` removes a leftover stack; `KEEP_UP=1` leaves it running for inspection.
- **`scripts/greenfield-smoke.compose.yml`** — a trimmed, prod-disjoint compose (the four
  stores + the API only; no proxy/web/docs/watchtower), parameterized by the env the script
  exports. It pins the SAME locally-built `codecsrayo/gt-core-mcp-server:embeddings` image
  prod runs (so the `oauth` provider seed fires) with `pull_policy: never`.

### Isolation contract (prod `gt-app` is never touched)

| Guard | How |
|---|---|
| Distinct compose project | `COMPOSE_PROJECT_NAME=gtgf-smoke` → volumes/network namespaced `gtgf-smoke_*` |
| Distinct host port | `127.0.0.1:18765` (never prod's `8765`/`80`/`443`) |
| Fresh volumes | `gtgf-pgdata/gtgf-dolt/gtgf-minio/gtgf-eventlog` — never a `gt-app_*` volume |
| Pre-flight | aborts if any `gtgf-smoke` volume/container already exists |
| Teardown | `docker compose -p gtgf-smoke down -v` — scoped to this project only |
| Read-only secret reuse | prod's RS256 PEMs mounted `:ro`; never modified |

Run it:

```bash
scripts/greenfield-smoke.sh                 # up (fresh) → assert → down; exit 0 iff all pass
KEEP_UP=1 scripts/greenfield-smoke.sh       # leave the stack up to poke at :18765
scripts/greenfield-smoke.sh teardown        # remove a leftover gtgf-smoke stack
```

The harness's own HTTP assertions use `python3` urllib (no `curl` dependency on the host).

---

## Run record — 2026-06-11

- **Image:** `codecsrayo/gt-core-mcp-server:embeddings`
  - id `sha256:a4877b262d8f141bbd0fc53addb43c702f3c0dd0f304cdf5de51a89e5f2241c1`
  - revision label `ccdbb28c48c59e73a2edae8de0925d374c1e6ff6` (origin/main HEAD)
  - built `2026-06-11T00:23:14Z`, locally (oauth + embeddings features)
- **Stack:** fresh `gtgf-smoke` project, empty volumes, host port `18765`.
- **Boot:** server reached `/health` in ~3s; the boot log confirmed the auto-seeds fired —
  `global admin ensured: admin@gt.local`, `oauth provider seeded: google`,
  `skills: seeded 71 role-catalog event(s) into empty 'default' catalog`,
  `hooks: seeded 3 safety guard(s)`, `PG catalog migrations: 15 applied`, and the rig
  catalog (5 rigs). One documented post-boot runbook step was applied by the harness:
  enabling the seeded `google` IdP (runbook §4a — `PATCH /auth/providers/google {enabled:true}`).
- **Result:** **7 / 7 PASS** (harness exit 0).

| # | §6 check | Result | Evidence |
|---|---|---|---|
| 1 | Admin login (`GT_ADMIN_*`) | **PASS** | `POST /auth/login` → 200, non-empty bearer |
| 2 | Default workspace present | **PASS** | `workspace_list` → `default` (Default Workspace, active) |
| 3 | Role resolves non-empty prompt + scopes (§4.1 Knowledge seed) | **PASS** | `mayor` prompt_len=3318, scopes=`[notifications.write, workspace.member, graph.read]` |
| 4 | ≥1 IdP on the login page (§4.2 OAuth seed + §4a enable) | **PASS** | `GET /auth/providers` → `[google]` after the §4a enable step |
| 5 | Rig catalog populated (§4.3 rig seed) | **PASS** | `rig_list` → 5 rigs `gt, hq, gtmcp, gtproxy, gtweb` |
| 6 | Quota surface operational; ≥1 *healthy* account | **PASS (surface) / N/A (account)** | `quota_list` responds; `healthy_accounts=0` — onboarding an account is the human-OAuth + cost-gated-daemon step (§4c), out of scope for an unattended smoke |
| 7 | `/mcp` authenticated + `issues.*` operational | **PASS** | full MCP session → `issues_list_execute` → `{rows:[],total:0}` (tracker live, empty on a fresh Dolt) |
| — | prod `gt-app` untouched | **PASS** | all 9 `gt-app-*` containers up, 6 `gt-app_*` volumes intact; no `gtgf-smoke` residue after teardown |

### Honest scope notes

- **Check 6 (quota account):** the smoke proves the quota *surface* is live from zero, but a
  **Healthy** account requires the irreducible human `claude auth login` OAuth handshake plus
  the cost-gated rotation daemon (`--profile orchd`) — runbook §4c. That is deliberately NOT
  driven by an unattended smoke (it would burn a paid Claude account), so check 6 is reported
  as "surface operational" with `healthy_accounts=0`, not faked green.
- **`graph.refresh {rig}` clone+index** (the §6 parenthetical for check 5): not exercised — it
  needs the rig repos mounted + a clone token; the smoke asserts the catalog is *populated*
  (the seed's job), not that a live index runs. Follow-up if a deeper smoke is wanted.

---

## Runbook drift surfaced by this run (follow-ups)

The harness validated the runbook end-to-end and turned up three small **doc drifts** between
the §5/§6 copy-paste snippets and the actual binary. The harness uses the real surfaces; the
runbook prose should be reconciled:

1. **Health path is `/health`, not `/healthz`.** §1 (k8s probe text) and §3 reference `/healthz`;
   the binary serves `/health` (200) and has no `/healthz` (404). The harness probes both.
2. **`/mcp` needs the full MCP session lifecycle.** The §6 check-7 snippet curls a bare
   `tools/call`, which 422s with *"Unexpected message, expect initialize request"*. The real
   transport (rmcp streamable-HTTP) requires `initialize` → `mcp-session-id` →
   `notifications/initialized` → `tools/call`, with SSE (`text/event-stream`) replies. The
   harness implements this minimal client.
3. **MCP tool ids on the wire use underscores.** The runbook prose says e.g.
   `issues.list.execute`; the registered tool ids are `issues_list_execute`, `workspace_list`,
   `rig_list`, `quota_list`. (The dotted form is the namespace prose, not the wire name.)
4. **`GET /api/v1/roles/{role}` (the §6 check-3 snippet) is not a route.** Role prompt+scopes
   resolve from the per-role *bindings* on `GET /api/v1/skills` (`{role, prompt, scopes, ...}`).
   The harness reads that surface.

Also re-confirmed (already flagged in the runbook §6 appendix as known): the Helm `seed-job`
comment is stale (live seeds are done by the API binary on boot, not the Job) — no code change,
the harness exercises the compose path where this is moot.

### Build-freshness gotcha observed during this bead

The local `:embeddings` tag was rebuilt mid-session by a concurrent process (the image id moved
`b3e38a9e → a4877b26`, both revision `ccdbb28`). An EARLY run against the stale build came up with
an **empty rig catalog** (`seed_rigs` produced 0 rows / no log line); the rebuilt image seeded all
5 rigs correctly. Lesson for re-runs: confirm the image id the harness prints (`BRING-UP` line)
is the build you intend — a half-built local tag can yield a false rig-seed failure that is a
build artifact, not a code regression.
