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
- `GT_SECRET_KEY` — AES-256-GCM master key sealing OAuth/PAT/VCS secrets at rest. Required if the `oauth` feature is built; the OAuth provider seed (§4.2) is short-circuited without it.
- `GT_JWT_RS256_PRIVATE_KEY_FILE` / `…_KEYS` / `…_SIGNING_KID` — RS256 login signing + verification.
- `GT_ADMIN_EMAIL` / `GT_ADMIN_PASSWORD` — the seed admin (argon2 upsert).
- `GT_OAUTH_SEED_SECRET_<ID>` — the cleartext OAuth `client_secret` for a seeded provider, read by the provider seed at boot (§4.2). `<ID>` = the provider id upper-cased (e.g. `GT_OAUTH_SEED_SECRET_GOOGLE`). Unset ⇒ that provider is skipped (clean, never fatal). Never vendored — the seed JSON holds only the non-secret config + the env var name.
- `GT_GITHUB_APP_ID` *(or `GT_GITHUB_APP_ID_FILE`)* / `GT_GITHUB_APP_PRIVATE_KEY_FILE` / `GT_GITHUB_APP_WEBHOOK_SECRET_FILE` / `GT_GITHUB_APP_SLUG` — GitHub App (private-repo clone + webhooks; the App is OPTIONAL — neither ID var set ⇒ App surface disabled, the PAT fallback still works). App ID + slug are public identifiers; the private-key PEM + webhook secret are the actual secrets, read from MOUNTED FILES (mirroring `GT_JWT_RS256_PRIVATE_KEY_FILE`). Registering the App on GitHub is an irreducible human action (§4.2).
- DB creds: `POSTGRES_*`, `DOLT_USER`, `MINIO_ROOT_*`.
- `GT_OIDC_REDIRECT_URI` — required when the `oauth` feature is built (the app's own callback URL; `db_oauth_resolver` panics at boot if unset). `GT_OAUTH_FE_REDIRECT_URL` — where `/auth/callback` hands the token off (optional; absent ⇒ returns token JSON for a non-browser client). `GT_OIDC_WORKSPACE` — tenant the OAuth login resolves to (optional, default `default`).

**Non-sensitive (ConfigMap):**
- `GT_PG_URL`, `GT_PG_AUDIT_URL`, `GT_DOLT_URL`, `GT_DOLT_BASE_URL` (multi-tenant routing), `GT_BLOB_*`, `GT_EVENTLOG_ROOT`, `GT_SELF_URL`, `GT_MCP_ALLOWED_HOSTS`, `GT_MCP_SCOPE_PROFILE`, `GT_GRAPHIFY_PYTHON`, `GT_TERMINAL_ENABLE`.
- `GT_RUN_DAEMONS` — master switch for the singleton background daemons (default ON; set `0` on scaled API replicas, `1` on the singleton tier — hq-talos-migration.4).

---

## 4. GAPS — state applied LIVE in prod, NOT reproducible (the work of this epic)

A clean deploy will come up **missing** these until they are made code/config-driven:

| Gap | Today | Bead |
|---|---|---|
| ~~Role prompts + skill bodies + role.scopes~~ → **DONE (§4.1)** | was rewritten via REST in prod (memory `knowledge-prompts-degastown`); now a versioned seed | hq-greenfield-seeds.2 |
| ~~OAuth/IdP providers~~ → **DONE (§4.2)** (Google, …) | was configured by hand in `/admin/providers`; now a versioned non-secret seed (secret via env) | hq-greenfield-seeds.3 |
| **GitHub App** | App registration + org installation are an irreducible human action (§4.2); the in-repo config (env wiring) is reproducible | hq-greenfield-seeds.3 |
| ~~Quota Claude accounts~~ → **DOCUMENTED + SCRIPTED (§4.4)** | onboarded live (per-account `CLAUDE_CONFIG_DIR`); creds are per-account secrets, so the *procedure* is reproduced (doc + helper script), not the creds | hq-greenfield-seeds.4 |
| ~~Rig catalog~~ → **DONE (§4.3)** (gt, gt_core, gtweb, gtproxy, gtmcp) | was `rig.add` run manually; now a versioned declarative seed replayed into an empty `rigs` table | hq-greenfield-seeds.5 |
| **Migrations don't reach existing tenants** | rig/0003 altered only `ws_default`; `hq_confiar` lacked `git_connection_ref` → drift-reconcile errored in prod | hq-vcs-connections.13 |

### 4.1 Knowledge seed (hq-greenfield-seeds.2) — DELIVERED

The interactive-role Knowledge — each role's **system prompt**, **model config**, and the
**`SKILL.md` bodies** its bound skills carry — was curated live in prod (memory
`knowledge-prompts-degastown`) and did not reproduce on a clean cluster: the catalog seeded only
empty scope-carriers, so greenfield roles came up with blank prompts and bodyless skills.

It is now a **versioned, idempotent boot seed**:

- **`crates/domain/platform/gt-skills/seeds/knowledge.json`** — the extract, vendored in-repo
  (role-functional subset: the 14 skills bound to ≥1 role + the 8 roles). Embedded into the binary
  via `include_str!`, so the seed is orchestrator-agnostic (no external file under k8s).
- **`presets::workspace_seed_events`** parses it and emits `Registered` (with `body` +
  `default_scopes` + `group`) + `EnabledForRole` + `RolePromptSet` + `RoleModelSet`, then runs
  `role_scopes_migration` so each role's scopes **derive** from its bound skills' `default_scopes`
  (the same values prod resolved — e.g. mayor: `notify-ops`→`notifications.write`,
  `tracker-ops`→`workspace.member`+`graph.read`). Role scopes are therefore *not* stored per-role.
- **Idempotent:** the caller (`rest_modules.rs`) seeds only when the workspace catalog `is_empty()`,
  so it never clobbers a human-curated catalog (no effect on the already-populated prod `default`).

**Regenerate from a live deploy** (when prod's curated Knowledge moves on):

```bash
# Copy the source workspace's event log out of the running container (compose example):
docker cp gt-app-mcp-server:/var/lib/gt-core/default /tmp/knowledge-src
python3 scripts/extract-knowledge-seed.py /tmp/knowledge-src   # rewrites seeds/knowledge.json
cargo test -p gt-skills --lib presets                          # replay-asserts the seed
```

**Out of scope for .2:** the 12 unbound `taste-skill`/design-library skills (`brandkit`,
`design-taste-frontend`, `imagegen-*`, …, ~287 KB) are a curated UI-design catalog bound to no
role, so they don't affect "functional roles." Seed them separately if a deploy wants that library.

### 4.2 IdP/OAuth providers + GitHub App (hq-greenfield-seeds.3) — DELIVERED

The login providers (Google, …) were configured by hand in `/admin/providers` and lived ONLY in
prod's `public.oauth_providers` table — a clean cluster came up with a blank login page. The GitHub
App is reachable only after a human registers it on GitHub. This bead makes both **reproducible**
(the in-repo, code/config-driven parts) and documents the **irreducible human action** (GitHub's
App-registration UI), and lands the full secrets matrix above (§3).

**OAuth/IdP provider config — now a versioned, idempotent, secret-gated boot seed:**

The NON-SECRET provider config was extracted read-only from the live deploy (one provider: `google`,
the baked Google endpoints, `scopes=openid,email,profile`, global, currently `enabled=false`) and is
now:

- **`crates/domain/platform/gt-auth/seeds/oauth-providers.json`** — the extract, vendored in-repo
  (id / kind / client_id / issuer / endpoints / scopes / enabled / workspace_id + the **name** of the
  env var each provider's secret is read from). Embedded into the binary via `include_str!`
  (orchestrator-agnostic — no external file under k8s).
- **`gt_auth::provider_seed`** (`SeedProvider::resolve` / `seed_providers`) parses it and builds a
  `NewProvider`, reading the cleartext `client_secret` from the named env var
  (`GT_OAUTH_SEED_SECRET_<ID>`); the repo then AES-256-GCM seals it under `GT_SECRET_KEY`.
- **`gt-mcp-server.rs::seed_oauth_providers`** (wired right after `seed_admin`, `#[cfg(oauth)]`)
  replays it. **Idempotent + non-clobbering:** seeds ONLY when `oauth_providers` is EMPTY — a curated
  prod (already populated) is untouched, so this has **no effect on the live `default`**. **Secret-
  gated** like `seed_admin`: with `GT_SECRET_KEY` unset the whole step is skipped (logged, never
  fatal); a provider whose `GT_OAUTH_SEED_SECRET_<ID>` is unset is skipped individually.

> **The `client_secret` is NEVER vendored.** A DB leak yields no usable secret (sealed at rest), and
> the repo carries only the non-secret config + the env var name. The cleartext is supplied per-deploy
> as a k8s `Secret` key / compose `./secrets` value bound to `GT_OAUTH_SEED_SECRET_<ID>`.

> **Note — the seeded `google` provider is `enabled=false`** (mirroring the live row). A greenfield
> deploy that wants it as a login button supplies the secret AND flips `enabled` (in the seed JSON, or
> via `/admin/providers` after boot). This is faithful to prod, not a bug.

**GitHub App — the in-repo config is reproducible; App registration is the irreducible human action:**

The App's identity + secrets are loaded from env/mounted files (`gt_vcs::github`,
`GT_GITHUB_APP_*`, §3), so wiring an App into a greenfield deploy is fully declarative. What is NOT
automatable — and is therefore the **one manual step** — is:

1. A human **registers the GitHub App** on GitHub (App name, permissions, webhook URL) — GitHub's UI,
   no API to create an org-owned App unattended. This yields the App ID, slug, a generated private-key
   PEM, and a webhook secret → mount them as `GT_GITHUB_APP_*` (§3).
2. A human **installs the App** on each org/repo. This is per-org and creates the *installation*; the
   resulting `vcs_connections` row is written at runtime by the install/callback flow, **not** seeded.

> `public.vcs_connections` is EMPTY in prod (verified read-only) — connections are runtime install
> artifacts (one per org installation), not declarative config, so there is nothing to seed there. The
> reproducible part is the env wiring above + the migration (`gt-vcs/0001`); the irreducible part is
> the two human steps.

**Regenerate the OAuth seed from a live deploy** (when prod's providers change):

```bash
# Export the NON-SECRET provider config from the running deploy (the secret is never read):
docker exec gt-app-pg psql "postgres://gtapp:gtapp@localhost:5432/gtapp" -tAc \
  "SELECT json_agg(row_to_json(t)) FROM (
     SELECT id, kind, display_name, client_id, issuer, authorize_endpoint,
            token_endpoint, userinfo_endpoint, scopes, enabled, workspace_id
     FROM public.oauth_providers ORDER BY created_at) t;" \
  | python3 scripts/extract-oauth-seed.py            # rewrites seeds/oauth-providers.json
cargo test -p gt-auth --features oauth provider_seed # replay-asserts the seed
```

### 4.3 Rig catalog (hq-greenfield-seeds.5) — DELIVERED

**Was:** the five rigs — `gt` (prefix `gt`), `gt_core` (`hq`), `gtmcp` (`gtmcp`), `gtproxy`
(`gtproxy`), `gtweb` (`gtweb`) — were registered by hand with `rig.add` and lived ONLY in prod's
per-tenant `ws_default.rigs` table. A greenfield cluster came up with an empty catalog: no beads
prefix routing (`issues.create` rejects an unregistered prefix), no dispatch, no `graph.refresh`
target — until an operator re-ran every `rig.add`.

**Now:** the catalog is a versioned, declarative seed, mirroring §4.2:

- **Extract (read-only):** `crates/domain/platform/gt-rig/seeds/rigs.json` is the live extract of
  `ws_default.rigs` — name / prefix / git_url / push_url / upstream_url / default_branch /
  worktree_root / git_connection_ref. Regenerate from a running deploy with
  `scripts/extract-rigs-seed.py` (round-trip byte-identical to the committed seed) — **do NOT
  invent the content.**
- **Embed:** `rig_seed::SEED_JSON` (`gt-rig/src/rig_seed.rs`) `include_str!`s it, so the seed
  travels inside the binary (orchestrator-agnostic — no external file under k8s).
- **Replay:** `gt-mcp-server.rs::seed_rigs` runs on boot right after `reconcile_tenant_schemas`,
  over a `ws_default`-scoped `WorkspacePool` (the rigs table is per-tenant, NOT `public`). It seeds
  ONLY when the `rigs` table is **empty** — a populated table (curated prod, or any deploy where an
  operator registered a rig) is left exactly as-is, so it is **idempotent with zero effect on
  populated prod**. `registered_at_secs` is stamped from the boot clock (the seed never vendors
  prod's epochs). A `WorkspacePool` connect failure is a clean skip (logged, never fatal).

**No secrets, no runtime artifacts:** the seed carries only declarative identity. `git_connection_ref`
— a soft reference to a `public.vcs_connections` row, which is a runtime GitHub-App install artifact
(§4.2) — is `null` for all five prod rigs (plain SSH clone), so the seed binds no connection. **A rig
that needs a private-repo clone token still depends on its VCS connection existing first** (§4.2's
GitHub App + an install): re-bind the ref out of band (`rig.*` / a future connection-bind path) on a
deploy that uses one — the seed does not, and cannot, recreate the runtime install.

Regenerate + verify:

```bash
docker exec gt-app-pg psql "postgres://gtapp:gtapp@localhost:5432/gtapp" -tAc \
  "SELECT json_agg(row_to_json(t)) FROM (
     SELECT name, prefix, git_url, push_url, upstream_url, default_branch,
            worktree_root, git_connection_ref
     FROM ws_default.rigs ORDER BY name) t;" \
  | python3 scripts/extract-rigs-seed.py   # rewrites seeds/rigs.json
cargo test -p gt-rig rig_seed              # replay-asserts the embedded seed
```

### 4.4 Quota Claude accounts (hq-greenfield-seeds.4) — DOCUMENTED + SCRIPTED

The platform rotates across **multiple Claude accounts** so a polecat that hits a rate limit
fails over to a healthy account instead of stalling (`gt-quota`, the predictive rotation in
`gt-composition/src/quota_rotation.rs`). Each account is a logged-in `CLAUDE_CONFIG_DIR`; the
registry of `(account, config_dir)` is **event-sourced** (`quota.account_registered.v1` in the
workspace quota log), and the orchestration daemon hydrates its rotation keychain by replaying it.

These accounts were onboarded **live** in prod (memory `quota-accounts-epic`) and cannot be
fully reproduced from the repo: an account's credentials are a **per-account OAuth secret** that
is NEVER vendored. So — like the GitHub App (§4.2) — what this bead makes reproducible is the
*procedure* (a doc + a helper script), with one **irreducible human step**: completing the
`claude auth login` OAuth handshake for each account.

**The real mechanism (ground truth):** the live web "Add account" flow
(`gt-composition/src/onboard.rs`), driven by two authenticated REST calls because the OAuth
handshake has a human in the middle:

1. **`POST /api/v1/quota/onboard/start`** — the backend (claude is baked into the
   `mcp-server` image) allocates a generic `CLAUDE_CONFIG_DIR` under the **accounts root**
   (`account_dirs::accounts_root`: `GT_CLAUDE_ACCOUNTS_ROOT`, else `<GT_EVENTLOG_ROOT>/accounts`),
   spawns `claude auth login` into it (stdin held open), and returns `{session_id, url}`. The dir
   is named by an opaque session ULID; the real account id comes from the handshake.
2. *(human)* open `url`, authenticate **as the account to add** (`prompt=select_account` is
   appended so the browser shows the chooser), copy the OOB **code**.
3. **`POST /api/v1/quota/onboard/complete {session_id, code}`** — the backend writes the code to
   the live login process's stdin, waits for exit, reads the account **email** via
   `claude auth status --json`, and registers it event-sourced: `RegisterAccount { account=email,
   config_dir }` → `quota.account_registered.v1`, appended to the **workspace quota log** (the
   same read-modify-append the `quota.register` MCP tool and `POST /api/v1/quota/account` REST
   route run).

**Confirmed live (read-only):** the accounts root (`/var/lib/gt-core/accounts/<ULID>/`) holds the
per-account dirs; the `default` workspace log carries the registry events, e.g.
`quota.account_registered.v1 → {account:"<email>", config_dir:"/var/lib/gt-core/accounts/<ULID>", now_secs}`.
The deploy-global catalog (`FsAccountCatalog`) reports each account by the **email** read from its
`<dir>/.claude.json` (`oauthAccount.emailAddress`) — the email is the account's identity across the
system.

**How the daemon picks it up:** `gt-orch-server` (the rotation daemon, compose service
`gt-app-orchd`, profile `orchd`, gated by `GT_RUN_DAEMONS`) rebuilds its keychain on (re)start from
TWO merged sources (`seed_claude_accounts`):

- **The quota log** (durable source of truth) — every registered `(account → config_dir)`. Because
  onboarding writes the dir from INSIDE the backend container, the stored path is container-absolute;
  the daemon (on the HOST) resolves it to the host mount of the same shared volume by basename
  (`resolve_host_account_dir`).
- **`GT_CLAUDE_ACCOUNTS` env** (bootstrap) — a comma list of `account=CLAUDE_CONFIG_DIR` pairs; an
  env account not yet in the log is promoted to a durable `AccountRegistered` **once** (so the first
  account(s) can be seeded by env and then persist as events). The first account is the boot-active
  one; a prior rotation target in the log wins if present.

So a newly onboarded account is picked up by **replaying the log** — no env edit needed; restart the
daemon (or wait for next boot) for it to enter the live rotation pool.

**Reproducible procedure (per account, greenfield):** the helper script
**`scripts/onboard-quota-account.sh`** drives the two REST calls and pauses at the human OAuth step.
It is idempotent (skips when `--email` is already registered, via `GET /api/v1/quota/`) and embeds
**no secret** (no credential material is read or echoed):

```bash
# Token must carry quota.write (or *) — the seeded admin's PAT/JWT does.
scripts/onboard-quota-account.sh \
  --url https://gt.example.com \
  --token "$GT_TOKEN" \
  --email account-to-add@example.com      # --email optional; enables the idempotent skip
# → opens a login URL, prompts for the OOB code, registers the account, prints next steps.

# Then let the rotation daemon hydrate the new account (restart if it was already running):
docker compose --profile orchd restart gt-app-orchd
```

For a **fully scripted bootstrap of the FIRST account** (no daemon restart needed — the env path
promotes it on boot), pre-create a `CLAUDE_CONFIG_DIR` on the accounts-root volume, run
`CLAUDE_CONFIG_DIR=<dir> claude auth login` (the OOB handshake — still the one human step), and set:

```
GT_CLAUDE_ACCOUNTS=account@example.com=/var/lib/gt-core/accounts/<dir>
```

before the daemon boots. `seed_claude_accounts` then registers it as a durable event on first start.

**Secrets/env matrix (§3) additions:** all **non-sensitive (ConfigMap)** — the credentials never
enter env, they live as files on the accounts-root volume:

- `GT_CLAUDE_ACCOUNTS_ROOT` — accounts-root path (default `<GT_EVENTLOG_ROOT>/accounts`); the
  backend (writer) and the daemon (reader) must resolve it to the **same shared volume**.
- `GT_CLAUDE_ACCOUNTS` — *(optional bootstrap)* comma list of `email=CLAUDE_CONFIG_DIR`; promoted to
  durable events on first daemon boot. Live onboarding (script/web) supersedes it.
- `GT_CLAUDE_BIN` — *(optional)* path to the `claude` binary if off the default PATH (it is baked
  into the `mcp-server` image at `/usr/bin/claude`).
- `GT_RUN_DAEMONS` / compose profile `orchd` — must be ON for the rotation daemon (and thus the
  keychain hydration) to run; the credential dirs themselves are not secrets-matrix entries.

**Known caveat (documented, NOT fixed here):** an **interactive terminal session** bakes the
**alphabetically-first** account at launch, not the *healthiest* one — so an interactive mayor can
land on an exhausted account even when a healthy one exists (memory `quota-rotation-dormant-prod`).
The autonomous polecat path uses the keychain's live active pointer (which predictive rotation
moves), so it is unaffected; this caveat is specific to interactive sessions and is a follow-up
(healthiest-account selection at launch), not part of greenfield bring-up.

---

## 5. Bring-up order (greenfield)

1. Provision infra: Postgres, Dolt, MinIO + the event-log volume.
2. Provide the secret/env matrix (§3).
3. Start `gt-mcp-server` → it runs PG + Dolt migrations, seeds admin (if `GT_ADMIN_*` set), default workspace, role/skills catalog (§4.1), **OAuth/IdP providers** (§4.2 — when `oauth` is built, `GT_SECRET_KEY` + the per-provider `GT_OAUTH_SEED_SECRET_*` are set, and the table is empty), **rig catalog** (§4.3 — when the `rigs` table is empty), hook guards, blob bucket.
4. Apply the remaining **gap seeds** (§4) — onboard ≥1 **quota Claude account** (§4.4 — `scripts/onboard-quota-account.sh`, one human OAuth step per account) and (re)start the `orchd` daemon to hydrate the keychain, and complete the GitHub-App manual steps (§4.2) if the deploy uses one (also re-binds any rig's `git_connection_ref` for private-repo clones, §4.3).
5. Verify (§6).

The detailed step-by-step runbook (with the exact apply commands per orchestrator, for both
docker compose and Talos/k8s) is **[`greenfield-bringup.md`](./greenfield-bringup.md)**
(hq-greenfield-seeds.6).

---

## 6. Verification (smoke — the proof of "functional from zero")

`hq-greenfield-seeds.7` automates this against a clean stack:
- Admin login works (`GT_ADMIN_*`).
- Default workspace present; a member can be provisioned (RBAC seeded).
- Roles resolve a **non-empty prompt + scopes** (not just the catalog).
- At least one IdP provider available on the login page.
- Rig catalog populated; `graph.refresh {rig}` clones + indexes (real commit, not `unknown`).
- Quota has ≥1 healthy account (§4.4: `GET /api/v1/quota/` / `quota.list` shows it `Healthy`; the `orchd` daemon logs `claude keychain seeded with N account(s)`).
- MCP `/mcp` reachable + authenticated; tracker (`issues.*`) operational.

No manual step outside the runbook (§5 / .6).
