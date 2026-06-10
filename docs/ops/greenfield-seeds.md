# Greenfield seeds — bringing a clean deploy to a functional platform

> **hq-greenfield-seeds.1** — canonical inventory + gap analysis of everything the platform
> seeds on a fresh deploy. On a new cluster (Talos/k8s) there is **no prior state**: everything
> the platform needs to be *functional* must come from a reproducible, idempotent seed. This doc
> is the source of truth for what's already code-driven vs. what was applied **live** and must be
> made reproducible (the gaps). Orchestrator-agnostic: the same seeds run under `docker compose`
> today and under k8s tomorrow.

Ground truth: `crates/modules/gt-composition/src/bin/gt-mcp-server.rs` boot path + the crate
migration dirs, as of 2026-06-10.

---

## 1. Infrastructure prerequisites (must exist before the server boots)

| Dependency | Used for | Bootstrap |
|---|---|---|
| **Postgres** (pgvector/pgvector:pg16) | table domains (workspace/rig/vcs/quota), audit, docs, the event-sourced dispatch | `GT_PG_URL`; the server applies migrations on connect |
| **Dolt** (dolthub/dolt-sql-server) | `hq` issues/meta tracker | `DoltIssues::ensure_database` + `ensure_schema` on connect; wire user ensured by the dolt container |
| **MinIO** (S3) | binary document attachment bytes | `GT_BLOB_*`; bucket `gt-documents` created by a one-shot init |
| **Event log volume** | event-sourced domains (graph warden, merge, agent, …) | `GT_EVENTLOG_ROOT=/var/lib/gt-core` (a PVC under k8s) |

---

## 2. Seeds already driven by code (reproduce automatically, idempotent)

### 2.1 PG schema migrations — boot for-loop, `CREATE … IF NOT EXISTS`
Applied on boot against `public` / the `ws_default` template. **Gotcha:** a migration const is inert
until it is added to the boot loop (see incident `system-providers-500`); and migrations only reach
the `ws_default` template — **existing tenant schemas are NOT re-migrated** (see [§4 gap, hq-vcs-connections.13]).

| Crate | Migrations |
|---|---|
| gt-auth | 0001–0012 (users, refresh_tokens, roles, user_roles, global_identity, oauth_providers, oauth_authz_state, cli_redirect, oauth_cli_code, personal_access_tokens, provider_workspace, sso_password_nullable) |
| gt-rig | 0001–0003 (rigs, worktree_root, git_connection_ref) |
| gt-vcs | 0001 (vcs_connections) |
| gt-quota | 0001 |
| gt-store-pg / gt-docs | 0001–0004 (documents, versions, embedding, shares) |
| gt-store-pg / gt-feature-flags | 0001 |
| gt-store-pg / gt-memory | 0001 |
| gt-store-pg / gt-workspace | 0001–0002 (workspaces, `gt_create_workspace_schema` fn) |
| gt-store-pg / notifications | 0001 |

### 2.2 Data seeds (boot, idempotent)

| Seed | Where | Gate / input |
|---|---|---|
| Global admin user (scopes `[*]`) | `seed_admin` (gt-mcp-server.rs) | `GT_ADMIN_EMAIL` + `GT_ADMIN_PASSWORD` — **both** required, else skipped |
| Default workspace + `ws_default` schema template | `gt_create_workspace_schema` | — |
| Per-workspace user → global identity migration | `migrate_users_to_global` | idempotent |
| Role/skills **catalog** | `gt_skills::presets::workspace_seed_events` | seeded into a workspace's `skills.*` log when empty. **Catalog only** — prompts/bodies/scopes are a gap (§4) |
| Hook safety guards (`rm -rf /`, `git push --force`) | boot, into empty global hook registry | — |
| Per-tenant RBAC | `seed_workspace_rbac` | runs at workspace provision |
| Dolt `hq` DB + base `issues` table + catalog epic | `ensure_database` / `ensure_schema` / `ensure_catalog_epic` | — |
| Blob bucket `gt-documents` | minio init job | — |

---

## 3. Secrets / env matrix (provide before boot)

> Under k8s these become a `Secret` (sensitive) + `ConfigMap` (non-sensitive); under compose, `./secrets` + `.env`.

**Sensitive (Secret):**
- `GT_SECRET_KEY` — AES-256-GCM master key sealing OAuth/PAT secrets at rest. Required if the `oauth` feature is built.
- `GT_JWT_RS256_PRIVATE_KEY_FILE` / `…_KEYS` / `…_SIGNING_KID` — RS256 login signing + verification.
- `GT_ADMIN_EMAIL` / `GT_ADMIN_PASSWORD` — the seed admin (argon2 upsert).
- `GT_GITHUB_APP_ID` / `GT_GITHUB_APP_PRIVATE_KEY_FILE` / `GT_GITHUB_APP_WEBHOOK_SECRET_FILE` / `GT_GITHUB_APP_SLUG` — GitHub App (private-repo clone + webhooks; unset ⇒ App surface disabled).
- DB creds: `POSTGRES_*`, `DOLT_USER`, `MINIO_ROOT_*`.
- `GT_OIDC_REDIRECT_URI` / `GT_OAUTH_FE_REDIRECT_URL` — required when the `oauth` feature is built.

**Non-sensitive (ConfigMap):**
- `GT_PG_URL`, `GT_PG_AUDIT_URL`, `GT_DOLT_URL`, `GT_DOLT_BASE_URL` (multi-tenant routing), `GT_BLOB_*`, `GT_EVENTLOG_ROOT`, `GT_SELF_URL`, `GT_MCP_ALLOWED_HOSTS`, `GT_MCP_SCOPE_PROFILE`, `GT_GRAPHIFY_PYTHON`, `GT_TERMINAL_ENABLE`.
- `GT_RUN_DAEMONS` — master switch for the singleton background daemons (default ON; set `0` on scaled API replicas, `1` on the singleton tier — hq-talos-migration.4).

---

## 4. GAPS — state applied LIVE in prod, NOT reproducible (the work of this epic)

A clean deploy will come up **missing** these until they are made code/config-driven:

| Gap | Today | Bead |
|---|---|---|
| **Role prompts + skill bodies + role.scopes** | rewritten via REST in prod (memory `knowledge-prompts-degastown`). Catalog seeds, but the *content/bindings* do not → roles boot with empty/default prompts | hq-greenfield-seeds.2 |
| **OAuth/IdP providers** (Google, …) | configured by hand in `/admin/providers` | hq-greenfield-seeds.3 |
| **GitHub App** | App creation + org installation are manual | hq-greenfield-seeds.3 |
| **Quota Claude accounts** | onboarded live (per-account `CLAUDE_CONFIG_DIR`) | hq-greenfield-seeds.4 |
| **Rig catalog** (gt, gt_core, gtweb, gtproxy, gtmcp) | `rig_add` run manually | hq-greenfield-seeds.5 |
| **Migrations don't reach existing tenants** | rig/0003 altered only `ws_default`; `hq_confiar` lacked `git_connection_ref` → drift-reconcile errored in prod | hq-vcs-connections.13 |

---

## 5. Bring-up order (greenfield)

1. Provision infra: Postgres, Dolt, MinIO + the event-log volume.
2. Provide the secret/env matrix (§3).
3. Start `gt-mcp-server` → it runs PG + Dolt migrations, seeds admin (if `GT_ADMIN_*` set), default workspace, role/skills catalog, hook guards, blob bucket.
4. Apply the **gap seeds** (§4) — knowledge content, IdP, GitHub App, quota accounts, rig catalog — via their reproducible seed mechanisms (the beads above).
5. Verify (§6).

The detailed step-by-step runbook (with the exact apply commands per orchestrator) is **hq-greenfield-seeds.6**.

---

## 6. Verification (smoke — the proof of "functional from zero")

`hq-greenfield-seeds.7` automates this against a clean stack:
- Admin login works (`GT_ADMIN_*`).
- Default workspace present; a member can be provisioned (RBAC seeded).
- Roles resolve a **non-empty prompt + scopes** (not just the catalog).
- At least one IdP provider available on the login page.
- Rig catalog populated; `graph.refresh {rig}` clones + indexes (real commit, not `unknown`).
- Quota has ≥1 healthy account.
- MCP `/mcp` reachable + authenticated; tracker (`issues.*`) operational.

No manual step outside the runbook (§5 / .6).
