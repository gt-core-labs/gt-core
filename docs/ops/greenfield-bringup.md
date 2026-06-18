# Greenfield bring-up runbook — zero to functional platform

> **hq-greenfield-seeds.6** — the operator's step-by-step procedure to bring the gt-core
> platform up from **nothing** on a clean cluster, in the right order, with copy-pasteable
> commands for **both** orchestrators: **Talos/k8s** (the target, via the Helm chart in
> `gt-app-proxy/chart/gt`) and **docker compose** (concretely runnable on this host today via
> `gt-app-proxy/docker-compose.yml` + `compose.embeddings.yml`).
>
> This is the **HOW**. The **WHAT/WHY** — the full seed inventory, the secrets matrix tables,
> the gap analysis, and the per-seed regeneration recipes — lives in the companion reference
> **[`greenfield-seeds.md`](./greenfield-seeds.md)**. This runbook **links** to it by section
> (§3 matrix, §4 gaps, §6 verification) instead of restating it. Read §0–§2 of that doc once
> before your first bring-up.

---

## 0. Scope — what "functional from zero" means

The bring-up is **done** when, against a freshly provisioned cluster with no prior state:

- the admin can log in (seeded `GT_ADMIN_*`);
- the **default** workspace exists and a member can be provisioned (RBAC seeded);
- every interactive role resolves a **non-empty system prompt + non-empty scopes** (not just
  an empty catalog) — the §4.1 Knowledge seed;
- at least one **IdP** button shows on the login page — the §4.2 OAuth seed (+ its secret);
- the **rig catalog** is populated and `graph.refresh {rig}` clones + indexes a real commit —
  the §4.3 rig seed;
- **quota** has ≥1 **Healthy** Claude account and the rotation daemon hydrated its keychain —
  the §4.4 procedure;
- `/mcp` is reachable + authenticated and the `issues.*` tracker is operational.

Most of that is **automatic at server boot** (§3 below). A short list of steps need a **human or
a per-deploy secret** (§4): the OAuth client secret + enable flag, the GitHub App registration +
install, and the Claude-account OAuth handshake. Those are the irreducible manual steps.

> Section numbers `§3.x / §4.x / §6` below refer to **[`greenfield-seeds.md`](./greenfield-seeds.md)**
> unless they start with "this §". This runbook's own steps are numbered 1–6.

---

## 1. Provision infrastructure (before the server boots)

Four dependencies must be reachable before `gt-mcp-server` starts; see the
[reference §1 table](./greenfield-seeds.md#1-infrastructure-prerequisites-must-exist-before-the-server-boots)
for what each is for. The server applies its **own** PG migrations and Dolt `ensure_database` /
`ensure_schema` on connect — you do **not** pre-create schemas, only the empty stores.

| Dependency | Connect env | compose | k8s |
|---|---|---|---|
| Postgres (pgvector pg16) | `GT_PG_URL=postgres://gtapp:<pw>@<host>:5432/gtapp` | `postgres` service | `postgres` StatefulSet + headless Service + PVC |
| Dolt (sql-server) | `GT_DOLT_URL=mysql://gtapp@<host>:3307/hq` | `dolt` service | `dolt` StatefulSet + Service + PVC |
| MinIO (S3) | `GT_BLOB_ENDPOINT/_BUCKET/_REGION/_ACCESS/_SECRET` | `minio` + `minio-createbucket` Job | `minio` StatefulSet + `minio-init` Job + PVC |
| Event-log volume | `GT_EVENTLOG_ROOT=/var/lib/gt-core` | `gt-eventlog` named volume | RWX PVC (shared API ↔ daemon) |

**compose** — stand up the stores (NOT the orchd profile yet):

```bash
cd /home/nixos/gt-app-proxy
cp .env.example .env            # then edit (see this §2)
# ALWAYS pass BOTH compose files or gt-mcp-server reverts to :latest (no oauth):
docker compose -f docker-compose.yml -f compose.embeddings.yml up -d postgres dolt minio minio-createbucket
```

**k8s** — the chart's StatefulSets + the MinIO-bucket post-install Job create these; nothing to
run by hand. The bucket Job (`minio-init`) and the schema-bootstrap Job (`seed-job`) run as
`helm.sh/hook: post-install,post-upgrade` so they precede the API roll. See `chart/gt/README.md`.

> **Eventlog volume is shared, single-writer.** The API pods **and** the orchd singleton mount
> the *same* eventlog (channels, heartbeats, onboarded accounts live there). Under k8s use a
> **RWX** CSI class (`eventlog.accessMode: ReadWriteMany`); on a single node you may co-schedule
> and use RWO (see chart README "API vs daemon split").

---

## 2. Supply the secrets / env matrix

The full annotated matrix — every var, sensitive vs non-sensitive, and what breaks if it's
missing — is **[reference §3](./greenfield-seeds.md#3-secrets--env-matrix-provide-before-boot)**.
Do not duplicate it; this section gives the concrete **layout per orchestrator** and flags the
**boot-fatal** ones.

### Must-set-or-boot-fails (when the image is built with the `oauth` feature — the `:embeddings` image is)

| Var | Why fatal | Where |
|---|---|---|
| `GT_OIDC_REDIRECT_URI` | `db_oauth_resolver` **panics at boot** if unset (`gt-mcp-server.rs:2381` `.expect(...)`) | Secret |
| `GT_SECRET_KEY` | AES-256-GCM master key; without it the OAuth provider seed (this §4) is silently skipped — login providers never seed | Secret |
| `GT_JWT_RS256_PRIVATE_KEY_FILE` (+ `…_KEYS`, `…_SIGNING_KID`) | no RS256 signing ⇒ no login tokens | Secret (mounted PEM files) |

`GT_ADMIN_EMAIL` + `GT_ADMIN_PASSWORD` are **both** required for the admin seed to run (skipped,
not fatal, if either is missing — but then you have no way in).

### compose layout (`gt-app-proxy/`)

- **`.env`** — non-secret + low-sensitivity values read by `docker compose` (DB creds,
  `GT_DOMAIN`, `GT_MCP_ALLOWED_HOSTS`, `GT_ADMIN_*`, ttls). Start from `.env.example`.
- **`./secrets/`** — mounted files: `jwt_private.pem`, `jwt_public.pem` (RS256), `acme.json`
  (Traefik), and `github_app_private_key.pem` if you use the App.
- The **`oauth`-feature** vars (`GT_OIDC_REDIRECT_URI`, `GT_SECRET_KEY`,
  `GT_OAUTH_SEED_SECRET_GOOGLE`, `GT_OAUTH_FE_REDIRECT_URL`) go in `.env` / the
  `gt-mcp-server.environment` block. `GT_OIDC_REDIRECT_URI` = `https://<GT_DOMAIN>/auth/callback`.

> **compose gotcha:** every `docker compose` invocation must pass **both**
> `-f docker-compose.yml -f compose.embeddings.yml`. Drop the overlay and the service falls back
> to the `:latest` image which is built **without** `oauth` — login providers, the secret seal,
> and the OAuth seed all vanish.

### k8s layout (`chart/gt`)

- All sensitive material renders into one **Secret** (`templates/secret.yaml`) from
  `.Values.secrets.*`; non-sensitive into a **ConfigMap** (`templates/configmap.yaml`). Supply
  `.Values.secrets` from a **gitignored** `values-secret.yaml` (template:
  `values-secret.yaml.example`) or `--set`, or set `secrets.create=false` and reference an
  externally-managed Secret (sealed-secrets / external-secrets / SOPS).
- `GT_OIDC_REDIRECT_URI` → `secrets.oidcRedirectUri`; `GT_SECRET_KEY` → `secrets.secretKey`;
  admin → `secrets.adminEmail/adminPassword`; RS256 PEMs → `secrets.jwtPrivateKey/jwtPublicKey`.

> **Chart gaps to wire for this epic's seeds (the chart's `secrets.create` Secret predates
> .3/.4):** the rendered Secret does **not** yet expose `GT_OAUTH_SEED_SECRET_GOOGLE` (the §4.2
> OAuth client secret) nor `GT_CLAUDE_ACCOUNTS_ROOT` (§4.4). Add `GT_OAUTH_SEED_SECRET_GOOGLE`
> as a Secret key on the **API** pods and set `GT_CLAUDE_ACCOUNTS_ROOT=/var/lib/gt-core/accounts`
> on both tiers (it already defaults under `GT_EVENTLOG_ROOT`, but pin it so writer and reader
> agree). Until then those two seeds are post-boot operator steps (this §4) regardless of chart.

---

## 3. Boot the server — what auto-seeds, in order

Start the API. Everything in this section is **idempotent** and runs **automatically** on every
boot; a restart against an already-seeded store is a no-op (each seed self-skips when its target
is non-empty, so **curated prod is never clobbered**).

**compose:**

```bash
cd /home/nixos/gt-app-proxy
docker compose -f docker-compose.yml -f compose.embeddings.yml up -d gt-mcp-server gt-web gt-docs proxy
docker compose -f docker-compose.yml -f compose.embeddings.yml logs -f gt-mcp-server   # watch the seed log lines
```

**k8s:** `helm install gt chart/gt -f values-secret.yaml --set storageClass=<csi-class>`. The
API Deployment rolls only once its pods pass `/health` (probes gate the rollout —
`maxUnavailable: 0`), so a boot panic never receives traffic. (The binary serves
`/health` only — there is no `/healthz`.)

### Boot order (ground truth: `crates/modules/gt-composition/src/bin/gt-mcp-server.rs`)

The binary executes these in sequence on a fresh store:

1. **PG migrations** — the boot for-loop applies the `CREATE … IF NOT EXISTS` array against
   `public` + the `ws_default` template (auth 0001–0012, rig 0001–0003, vcs 0001, quota 0001,
   docs/flags/memory/workspace/notifications). *(`apply_pg_catalog`)*
2. **Default workspace + `ws_default` schema template** — `gt_create_workspace_schema`; the
   per-tenant cloner every workspace is provisioned from.
3. **`reconcile_tenant_schemas`** — heals per-tenant drift (a template migration that landed
   after a tenant was created), so e.g. `rig/0003`'s `git_connection_ref` reaches existing
   `ws_<slug>` schemas (the `hq-vcs-connections.13` fix).
4. **`migrate_users_to_global`** then **`seed_admin`** — lift per-ws users into the global
   identity tables, then (re)seed the wildcard admin last so it wins. *(gated on `GT_ADMIN_*`.)*
5. **`seed_oauth_providers`** *(immediately after `seed_admin`, `#[cfg(feature="oauth")]`)* —
   replays `seeds/oauth-providers.json` into an **empty** `oauth_providers` table; secret-gated
   on `GT_SECRET_KEY` + the per-provider `GT_OAUTH_SEED_SECRET_<ID>`. (§4.2)
6. **`seed_rigs`** *(after `reconcile_tenant_schemas`, over a `ws_default`-scoped pool)* — replays
   `seeds/rigs.json` into an **empty** per-tenant `rigs` table. (§4.3)
7. **Role/skills Knowledge catalog** — `presets::workspace_seed_events` seeds prompts + models +
   skill bodies + role scopes into a workspace catalog **only when `catalog.is_empty()`**. (§4.1)
8. **Hook safety guards** — `safety_guard_hooks` (`rm -rf /`, `git push --force`) into an **empty**
   global hook registry.
9. **Blob bucket** `gt-documents` — ensured via the MinIO init (compose Job / k8s `minio-init`).
10. **Dolt `hq`** — `ensure_database` + `ensure_schema` + the catalog epic row.

After this the API is up with: admin login, default workspace, populated Knowledge, seeded login
provider (disabled until you supply its secret + flip enabled — this §4), populated rig catalog,
hook guards, and a working `/mcp` + `issues.*`.

---

## 4. Post-boot operator steps (the gaps that need a human or a secret)

These are the irreducible manual steps from [reference §4](./greenfield-seeds.md#4-gaps--state-applied-live-in-prod-not-reproducible-the-work-of-this-epic).
The boot seeds above got you 90% there; these close the rest.

### 4a. Enable an OAuth/IdP login provider (§4.2)

The seeded `google` provider lands **`enabled=false`** (faithful to prod) and its `client_secret`
is **never vendored**. To turn it into a login button:

1. Provide the cleartext secret as `GT_OAUTH_SEED_SECRET_GOOGLE` (compose `.env` /
   `gt-mcp-server.environment`; k8s Secret key on the API pods) **before** boot, so
   `seed_oauth_providers` seals it under `GT_SECRET_KEY` at rest. *(If the table was already
   seeded with the provider absent because the secret was unset, the seed is non-clobbering —
   set the secret then either re-create the empty row, or add the provider via `/admin/providers`.)*
2. Flip `enabled` — either set `"enabled": true` in `seeds/oauth-providers.json` and rebuild, or
   simplest post-boot: toggle it in the admin UI at `/admin/providers`.

> Registering an entirely new IdP is the same shape: add a non-secret block to the seed JSON +
> a `GT_OAUTH_SEED_SECRET_<ID>` env. See reference §4.2 + `scripts/extract-oauth-seed.py`.

### 4b. GitHub App — register + install (irreducible human action, §4.2)

The App's env wiring (`GT_GITHUB_APP_*`, §3) is declarative, but **two steps have no API** and
must be done by a human (skip entirely if no rig needs a **private-repo** clone — public SSH
clone needs none of this):

1. **Register** the GitHub App on GitHub (name, permissions, webhook URL `https://<domain>/api/v1/connection/github/webhook`). This yields the App ID, slug, a generated **private-key PEM**, and a **webhook secret**.
2. **Install** the App on each org/repo. The resulting `vcs_connections` row is written **at
   runtime** by the install/callback flow — it is **not** seeded (`public.vcs_connections` is
   empty by design).
3. Mount the App secrets (`GT_GITHUB_APP_ID`, `…_PRIVATE_KEY_FILE`, `…_WEBHOOK_SECRET_FILE`,
   `…_SLUG`) and restart the API.
4. **Re-bind** any rig that needs the private clone: the seeded rigs all have
   `git_connection_ref = null` (plain SSH). For a private-repo rig, bind its `git_connection_ref`
   to the new `vcs_connections` row out of band (`rig.*` / a connection-bind path). The seed
   cannot recreate a runtime install. (§4.3)

### 4c. Onboard ≥1 Claude account + run the rotation daemon (§4.4)

The account credentials are a per-account OAuth secret (never vendored); the **procedure** is
reproducible via `scripts/onboard-quota-account.sh`, with **one human OAuth handshake** per
account. The token you pass must carry `quota.write` (the seeded admin's PAT/JWT does).

```bash
# Drives POST /quota/onboard/start → (human opens URL, pastes OOB code) → /complete,
# then registers the account event-sourced. Idempotent on --email (GET /api/v1/quota/).
scripts/onboard-quota-account.sh \
  --url https://<domain> \
  --token "$GT_TOKEN" \
  --email account-to-add@example.com
```

Then bring up / restart the **rotation daemon** so `seed_claude_accounts` hydrates its keychain
by replaying the quota log:

```bash
# compose — orchd is behind a COST-GATE profile; needs GT_RIG_GIT_TOKEN for the boot clone:
cd /home/nixos/gt-app-proxy
docker compose -f docker-compose.yml -f compose.embeddings.yml --profile orchd up -d gt-orch-server
# (already running? pick up the new account with a restart:)
docker compose -f docker-compose.yml -f compose.embeddings.yml --profile orchd restart gt-orch-server
```

```text
# k8s — the daemons Deployment is the singleton (replicas=1, Recreate). It runs by default
# (daemons.enabled=true). Set its rig clone token (secrets.rigGitToken) then:
kubectl rollout restart deploy/gt-daemons      # re-hydrate keychain from the quota log
```

The daemon is gated by **`GT_RUN_DAEMONS`** (default ON; only `GT_RUN_DAEMONS=0` turns it off —
set `0` on scaled API replicas so a single tier owns each daemon tick). Confirm it logged
`claude keychain seeded with N account(s)`.

### `gh` auth seed (autonomous merges) — `gtcore-4c9c85`

The git-merge edge lands branches with a PR (`gh pr create` + `gh pr merge --auto`). `gh` stores
its login under `$HOME/.config/gh`, but the orchd's **`HOME=/tmp` is wiped on every pod
restart/redeploy** — so without a seed, *every* autonomous merge fails right after a restart with
`To get started with GitHub CLI, please run: gh auth login`, even though `git push` still works (the
rig remote carries an embedded token). Same systemic shape as the keychain creds.

The orchd self-seeds at boot: it sets **`GH_TOKEN`** in its own process env (no `gh auth login`, no
file on disk, survives any restart), resolving a token in priority order:

1. an already-present **`GH_TOKEN`** / **`GH_ENTERPRISE_TOKEN`** — left untouched (the
   chart-mounted-Secret hardening path; recommended, lives in a k8s Secret, never in the image);
2. **`GT_RIG_GIT_TOKEN`** — the same PAT used for the boot clone;
3. the token embedded in the rig checkout's `origin` remote (what an operator reads off a worktree
   by hand today).

Because `GT_RIG_GIT_TOKEN` is already set for the orchd boot clone, **no extra config is required** —
the seed works out of the box. For the cleanest hardening, mount a k8s Secret as `GH_TOKEN` on the
`gt-daemons` Deployment (a `gt-app-proxy` chart follow-up) so the token is decoupled from the rig
clone token. Confirm at boot with the log line `gh auth seeded — GH_TOKEN set from …`, and verify
`gh auth status` reports *Logged in to github.com … (GH_TOKEN)* with no manual `gh auth login`.
The seed touches only `gh`; `git push` (embedded remote token) and the keychain creds flow are
unaffected.

> **Bootstrap shortcut (first account, no restart):** pre-create a `CLAUDE_CONFIG_DIR` on the
> accounts-root volume, run `CLAUDE_CONFIG_DIR=<dir> claude auth login` (still the one human
> handshake), and set `GT_CLAUDE_ACCOUNTS=email=<dir>` before the daemon boots — it's promoted to
> a durable event on first start. (Reference §4.4.)

---

## 5. Verify — proof of "functional from zero"

Run these against the live stack. They mirror
[reference §6](./greenfield-seeds.md#6-verification-smoke--the-proof-of-functional-from-zero)
(which `hq-greenfield-seeds.7` automates). Export a base URL + an admin token first:

```bash
export GT_URL=https://<domain>
# Admin login → bearer (the seeded GT_ADMIN_*):
export GT_TOKEN="$(curl -fsS -X POST "$GT_URL/auth/login" \
  -H 'content-type: application/json' \
  -d '{"email":"admin@gt.local","password":"<pw>","workspace":"default"}' | jq -r .access_token)"
AUTH=(-H "Authorization: Bearer $GT_TOKEN")
```

```bash
# 1. Admin login worked → non-empty token (above).
test -n "$GT_TOKEN" && echo "OK: admin login"

# 2. Default workspace present:
curl -fsS "${AUTH[@]}" "$GT_URL/api/v1/workspace" | jq -e '.[]|select(.slug=="default")' >/dev/null \
  && echo "OK: default workspace"

# 3. A role resolves a NON-EMPTY prompt + scopes (the §4.1 Knowledge seed, not just the catalog).
#    There is NO `GET /api/v1/roles/{role}` route — role prompt+scopes live in the per-role
#    *bindings* returned by `GET /api/v1/skills` ({count, skills, bindings:[{role,prompt,scopes,…}]}).
curl -fsS "${AUTH[@]}" "$GT_URL/api/v1/skills" \
  | jq -e '.bindings[]|select(.role=="mayor")|(.prompt|length>0) and (.scopes|length>0)' >/dev/null \
  && echo "OK: role prompt+scopes non-empty"

# 4. ≥1 IdP on the (public) login page (the §4.2 OAuth seed + enabled flag):
curl -fsS "$GT_URL/auth/providers" | jq -e 'length>=1' >/dev/null \
  && echo "OK: >=1 IdP provider"

# 5. Rig catalog populated; graph.refresh clones + indexes a real commit:
curl -fsS "${AUTH[@]}" "$GT_URL/api/v1/rig" | jq -e 'length>=1' >/dev/null \
  && echo "OK: rig catalog populated"
#   (then call the `graph_refresh` MCP tool with {rig:"hq"} via a /mcp session — see check 7 —
#    and confirm a real commit sha, not "unknown")

# 6. Quota has >=1 Healthy account (the §4.4 procedure + hydrated daemon):
curl -fsS "${AUTH[@]}" "$GT_URL/api/v1/quota/" | jq -e '[.[]|select(.health=="Healthy")]|length>=1' >/dev/null \
  && echo "OK: >=1 healthy quota account"
#   and the orchd daemon log shows: "claude keychain seeded with N account(s)"

# 7. /mcp authenticated + the issues tracker operational.
#    The /mcp transport is rmcp streamable-HTTP: a bare `tools/call` 422s with
#    "Unexpected message, expect initialize request". A real call needs the full session
#    lifecycle — initialize (capture the `mcp-session-id` response header) → notifications/initialized
#    → tools/call — and the client MUST accept SSE (`Accept: application/json, text/event-stream`);
#    replies come back as `text/event-stream` events. Note the wire tool id uses UNDERSCORES
#    (`issues_list_execute`), NOT the dotted `issues.list.execute` prose form.
ACC=(-H 'content-type: application/json' -H 'accept: application/json, text/event-stream')
# initialize → grab the session id from the response header
SID="$(curl -fsS -D - -o /dev/null "${AUTH[@]}" "${ACC[@]}" -X POST "$GT_URL/mcp" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  | tr -d '\r' | awk -F': ' 'tolower($1)=="mcp-session-id"{print $2}')"
SES=(-H "mcp-session-id: $SID")
# notifications/initialized (no id) then the actual tool call
curl -fsS "${AUTH[@]}" "${ACC[@]}" "${SES[@]}" -X POST "$GT_URL/mcp" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null
curl -fsS "${AUTH[@]}" "${ACC[@]}" "${SES[@]}" -X POST "$GT_URL/mcp" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"issues_list_execute","arguments":{}}}' \
  | grep -q '"result"' && echo "OK: /mcp issues tracker works"
```

> The `curl ... | grep '"result"'` is a minimal check — the body arrives as SSE
> (`data: {…}` lines), so parse the JSON out of the `data:` frame if you need the rows. The
> reference smoke harness (`hq-greenfield-seeds.7`) implements this minimal MCP client end-to-end.

All seven green = **functional from zero**, no manual step outside this runbook.

---

## 6. Appendix — orchestrator-specific apply order

### docker compose (concretely runnable on this host)

```bash
cd /home/nixos/gt-app-proxy
cp .env.example .env                       # edit: DB creds, GT_ADMIN_*, GT_OIDC_REDIRECT_URI,
                                           #       GT_SECRET_KEY, GT_OAUTH_SEED_SECRET_GOOGLE
# ALWAYS both files (else :latest, no oauth):
C="docker compose -f docker-compose.yml -f compose.embeddings.yml"

$C up -d postgres dolt minio minio-createbucket    # 1. stores + bucket
$C up -d gt-mcp-server gt-web gt-docs proxy         # 2. API boots → auto-seeds (this §3)
$C logs -f gt-mcp-server                            # 3. watch seed lines; wait for /health
# 4. post-boot operator steps (this §4): enable OAuth provider, GitHub App (if private rig),
#    onboard quota account:
scripts/onboard-quota-account.sh --url "$GT_URL" --token "$GT_TOKEN" --email me@example.com
GT_RIG_GIT_TOKEN=<tok> $C --profile orchd up -d gt-orch-server   # 5. rotation daemon (cost gate)
# 6. verify (this §5)
```

### Talos / Kubernetes (Helm chart `gt-app-proxy/chart/gt`)

The chart renders the whole assembly (StatefulSets for PG/Dolt/MinIO, the eventlog/graph PVCs,
the API Deployment + the orchd singleton, Ingress, the Secret/ConfigMap, the `minio-init` +
`seed-job` post-install Jobs). Apply order is handled by Helm hooks, not by you:

```sh
# 1. cluster (local Talos to iterate, or real nodes).
talosctl cluster create
# 2. secrets — NEVER committed:
cp chart/gt/values-secret.yaml.example values-secret.yaml      # edit
# 3. install — post-install hooks (hook-weight: minio-init then seed-job) run BEFORE the API rolls;
#    the API Deployment rolls only once pods pass /health (maxUnavailable: 0).
helm install gt chart/gt -f values-secret.yaml --set storageClass=<csi-class>
# 4. post-boot operator steps (this §4): provide GT_OAUTH_SEED_SECRET_GOOGLE + enable the provider;
#    register/install the GitHub App + rebind a private rig's git_connection_ref; onboard a quota
#    account (port-forward the API or run the script against the Ingress host); the daemons
#    Deployment is the singleton (replicas=1) — `kubectl rollout restart deploy/gt-daemons` to
#    re-hydrate the keychain from the quota log.
# 5. verify (this §5) against the Ingress host.
```

> **Mapping note:** compose service → k8s object table is in `chart/gt/README.md` ("How the
> compose assembly maps to k8s"). The chart's `seed-job` is a **store-readiness wait** only — the
> knowledge/IdP/rig seeds are performed by the **API binary on boot** (this §3), not by the Job
> (the Job stays useful as the GitOps seam that orders store-readiness before the API rolls). The
> chart + `gt-app-proxy/DEPLOY.md` were reconciled to this reality in `hq-talos-migration.13`.

> **`GT_RUN_DAEMONS`:** the binary honours a single `GT_RUN_DAEMONS` gate (default ON; `0` turns
> every daemon loop off; `gt-mcp-server.rs::should_run_daemons`). On the scaled API tier set
> `GT_RUN_DAEMONS=0`; leave it ON (default) on the singleton. The chart already wires this
> (`=0` API / `=1` orchd).

---

## See also

- **[`greenfield-seeds.md`](./greenfield-seeds.md)** — the seed inventory + gap analysis +
  secrets matrix (§3) + per-seed regeneration recipes (§4.1–§4.4) + verification (§6).
- `gt-app-proxy/chart/gt/README.md` — the Helm chart, compose→k8s mapping, secrets handling.
- `gt-app-proxy/DEPLOY.md` — the master Talos install + bring-up + cutover runbook
  (`hq-talos-migration.8`); this doc is its detail reference for the API-boot seeds.
- `scripts/onboard-quota-account.sh`, `scripts/extract-{knowledge,oauth,rigs}-seed.py` — the
  helper + regeneration scripts referenced above.
