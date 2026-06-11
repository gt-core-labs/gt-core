#!/usr/bin/env bash
# greenfield-smoke.sh — reproducible E2E "functional from zero" smoke (hq-greenfield-seeds.7).
#
# Stands up a CLEAN, fully ISOLATED gt-core stack on fresh volumes + alt ports, lets the
# server's boot auto-seeds (hq-greenfield-seeds.2–.5) populate it per the runbook
# (docs/ops/greenfield-bringup.md), runs the §6 verification assertions, prints PASS/FAIL
# per check, and tears the stack down. Idempotent + safe to re-run.
#
# ISOLATION CONTRACT (never touches the prod `gt-app` project):
#   * COMPOSE_PROJECT_NAME=gtgf-smoke  → volumes/network namespaced as gtgf-smoke_*.
#   * Distinct HOST ports (default 18765) — never prod's 8765/80/443.
#   * Teardown = `docker compose -p gtgf-smoke down -v` — scoped to THIS project only.
#   * Pre-flight asserts NO gtgf-smoke volumes/containers exist before bringing the stack up.
#   * Read-only reuse of prod's RS256 PEMs (mounted :ro); they are never modified.
#
# The harness's OWN HTTP assertions use python3 urllib (no curl dependency on this host).
# The §6 runbook snippets use curl on the target; this script reproduces them with python3.
#
# Usage:
#   scripts/greenfield-smoke.sh              # up → assert → down (full cycle)
#   KEEP_UP=1 scripts/greenfield-smoke.sh    # leave the stack running for inspection
#   scripts/greenfield-smoke.sh teardown     # tear down a leftover gtgf-smoke stack only
set -euo pipefail

# ---- parameters (override via env) -----------------------------------------------------
PROJECT="${GT_SMOKE_PROJECT:-gtgf-smoke}"
MCP_PORT="${GT_SMOKE_MCP_PORT:-18765}"
ADMIN_EMAIL="${GT_SMOKE_ADMIN_EMAIL:-admin@gt.local}"
ADMIN_PASSWORD="${GT_SMOKE_ADMIN_PASSWORD:-smoke-admin-pw-$RANDOM}"
# 32-byte (64 hex) AES-256-GCM master key for the OAuth seal. Deterministic-random per run.
SECRET_KEY="${GT_SMOKE_SECRET_KEY:-$(head -c32 /dev/urandom | od -An -tx1 | tr -d ' \n')}"
READY_TIMEOUT="${GT_SMOKE_READY_TIMEOUT:-180}"
BASE_URL="http://127.0.0.1:${MCP_PORT}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/greenfield-smoke.compose.yml"
DC=(docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}")

export GT_SMOKE_MCP_PORT="${MCP_PORT}"
export GT_SMOKE_ADMIN_EMAIL="${ADMIN_EMAIL}"
export GT_SMOKE_ADMIN_PASSWORD="${ADMIN_PASSWORD}"
export GT_SMOKE_SECRET_KEY="${SECRET_KEY}"

# ---- guard rails -----------------------------------------------------------------------
if [[ "${PROJECT}" == gt-app* ]]; then
  echo "REFUSING: project name '${PROJECT}' collides with prod. Pick another." >&2; exit 2
fi

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

teardown() {
  log "TEARDOWN — removing ONLY the ${PROJECT} project (volumes + network)"
  "${DC[@]}" down -v --remove-orphans 2>/dev/null || true
  # Belt + suspenders: assert nothing of ours is left.
  local left
  left="$(docker volume ls -q | grep -E "^${PROJECT}_" || true)"
  if [[ -n "${left}" ]]; then
    echo "WARNING: residual ${PROJECT} volumes still present:" >&2; echo "${left}" >&2
  else
    note "no residual ${PROJECT} volumes/containers — clean."
  fi
}

# Standalone teardown subcommand.
if [[ "${1:-}" == "teardown" ]]; then teardown; exit 0; fi

# ---- pre-flight: assert a clean slate (never reuse a prod volume) -----------------------
log "PRE-FLIGHT — asserting clean ${PROJECT} state (prod gt-app untouched)"
if docker volume ls -q | grep -qE "^${PROJECT}_"; then
  echo "ABORT: ${PROJECT} volumes already exist; run '$0 teardown' first." >&2; exit 2
fi
if docker ps -a --format '{{.Names}}' | grep -q "^${PROJECT}-"; then
  echo "ABORT: ${PROJECT} containers already exist; run '$0 teardown' first." >&2; exit 2
fi
note "no pre-existing ${PROJECT} volumes/containers."
note "prod volumes present (left untouched): $(docker volume ls -q | grep -c '^gt-app_' || true) gt-app_* volumes"

trap '[[ "${KEEP_UP:-0}" == "1" ]] || teardown' EXIT

# ---- bring up the isolated stack on FRESH volumes --------------------------------------
log "BRING-UP — ${PROJECT} on port ${MCP_PORT} (fresh volumes; image :embeddings)"
IMAGE_ID="$(docker images --no-trunc --format '{{.ID}}' codecsrayo/gt-core-mcp-server:embeddings | head -1)"
note "image codecsrayo/gt-core-mcp-server:embeddings = ${IMAGE_ID:-<MISSING>}"
[[ -n "${IMAGE_ID}" ]] || { echo "ABORT: image :embeddings not present locally." >&2; exit 2; }

"${DC[@]}" up -d --wait smoke-postgres smoke-dolt smoke-minio || true
"${DC[@]}" up -d smoke-minio-init
"${DC[@]}" up -d smoke-mcp-server

# ---- wait for readiness ----------------------------------------------------------------
log "READINESS — waiting for ${BASE_URL}/health (<= ${READY_TIMEOUT}s)"
ready=0
for ((i=0; i<READY_TIMEOUT; i+=3)); do
  # The binary serves /health (the runbook's `/healthz` is drift — see smoke-result doc).
  # Probe both for forward-compat.
  if python3 - "$BASE_URL" <<'PY' 2>/dev/null
import sys, urllib.request
base = sys.argv[1]
for path in ("/health", "/healthz"):
    try:
        with urllib.request.urlopen(base + path, timeout=3) as r:
            if r.status == 200:
                sys.exit(0)
    except Exception:
        pass
sys.exit(1)
PY
  then ready=1; note "health OK after ~${i}s"; break; fi
  sleep 3
done
if [[ "${ready}" != "1" ]]; then
  echo "ABORT: server never became ready; recent logs:" >&2
  "${DC[@]}" logs --tail=80 smoke-mcp-server >&2 || true
  exit 1
fi

# ---- post-boot operator step (runbook §4a): flip the seeded google IdP enabled=true -----
# The §4.2 seed lands google enabled=false (faithful to prod). The runbook's documented
# post-boot step to make it a login button is to toggle it in /admin/providers. We do that
# step here (a runbook step, NOT a manual workaround) so check 4 reflects "from zero +
# runbook" rather than "from zero only".
log "RUNBOOK §4a — admin login + enable the seeded google IdP (documented post-boot step)"
SMOKE_OUT="$(python3 - <<PY
import json, sys, urllib.request, urllib.error

BASE = "${BASE_URL}"
EMAIL = "${ADMIN_EMAIL}"
PW = "${ADMIN_PASSWORD}"

def call(method, path, token=None, body=None, raw=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("content-type", "application/json")
    if token:
        req.add_header("authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            txt = r.read().decode()
            return r.status, (txt if raw else (json.loads(txt) if txt else None))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return 0, str(e)

results = {}

# (op) admin login → bearer
st, body = call("POST", "/auth/login",
                body={"email": EMAIL, "password": PW, "workspace": "default"})
token = body.get("access_token") if isinstance(body, dict) else None
results["login_status"] = st
results["have_token"] = bool(token)

# (op) enable the seeded google provider so the PUBLIC /auth/providers lists >=1.
if token:
    pst, _ = call("PATCH", "/auth/providers/google", token=token, body={"enabled": True})
    results["provider_enable_status"] = pst

print(json.dumps({"token": token or "", "results": results}))
PY
)"
TOKEN="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])' <<<"$SMOKE_OUT")"
note "admin login + provider-enable: $(python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin)["results"]))' <<<"$SMOKE_OUT")"

# ---- §6 ASSERTIONS ---------------------------------------------------------------------
log "SMOKE §6 — verification assertions"
ASSERT_JSON="$(TOKEN="$TOKEN" python3 - <<PY
import json, os, urllib.request, urllib.error

BASE = "${BASE_URL}"
TOKEN = os.environ["TOKEN"]
AUTH = {"authorization": "Bearer " + TOKEN} if TOKEN else {}

def http(method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("content-type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            t = r.read().decode()
            return r.status, (json.loads(t) if t else None)
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, None
    except Exception as e:
        return 0, {"error": str(e)}

# ---- minimal streamable-HTTP MCP client -----------------------------------------------
# /mcp is an rmcp streamable-HTTP transport: it requires the full session lifecycle
# (initialize → mcp-session-id → notifications/initialized → tools/call), and replies as
# SSE (text/event-stream). A bare tools/call (as the runbook §6 snippet curls) 422s with
# "Unexpected message, expect initialize request" — see the smoke-result doc's drift notes.
# Tool ids on the wire use UNDERSCORES (workspace_list, rig_list, quota_list,
# issues_list_execute), not the dotted ".execute" form the runbook prose uses.
class Mcp:
    def __init__(self):
        self.sid = None
    def _post(self, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(BASE + "/mcp", data=data, method="POST")
        req.add_header("content-type", "application/json")
        req.add_header("accept", "application/json, text/event-stream")
        for k, v in AUTH.items():
            req.add_header(k, v)
        if self.sid:
            req.add_header("mcp-session-id", self.sid)
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                sid = r.headers.get("mcp-session-id")
                if sid:
                    self.sid = sid
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()
        except Exception as e:
            return 0, str(e)
    @staticmethod
    def _sse(text):
        out = None
        for line in text.splitlines():
            if line.startswith("data:") and line[5:].strip():
                try: out = json.loads(line[5:].strip())
                except Exception: pass
        return out
    def init(self):
        st, _ = self._post({"jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                       "clientInfo": {"name": "greenfield-smoke", "version": "1"}}})
        if st == 200 and self.sid:
            self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return st == 200 and bool(self.sid)
    def call(self, tool, args=None):
        st, body = self._post({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                               "params": {"name": tool, "arguments": args or {}}})
        resp = self._sse(body)
        text = None
        if isinstance(resp, dict) and "result" in resp:
            try: text = resp["result"]["content"][0]["text"]
            except Exception: text = json.dumps(resp["result"])
        return st, resp, text

mcp_client = Mcp()
mcp_ready = mcp_client.init() if TOKEN else False

checks = []
def record(name, ok, detail=""):
    checks.append({"name": name, "ok": bool(ok), "detail": detail})

# 1. Admin login worked → non-empty bearer.
record("1 admin login", bool(TOKEN),
       "token present" if TOKEN else "no access_token from /auth/login")

# 2. Default workspace present (MCP workspace_list — table-backed, reliable).
ws_ok, ws_detail = False, "mcp session not established"
if mcp_ready:
    st, resp, text = mcp_client.call("workspace_list")
    ws_ok = bool(text) and ('"default"' in text or "Default Workspace" in (text or ""))
    ws_detail = f"workspace_list={text[:140]}" if text else f"status={st} resp={json.dumps(resp)[:140]}"
record("2 default workspace", ws_ok, ws_detail)

# 3. A role resolves a NON-EMPTY prompt + scopes (the §4.1 Knowledge seed, not just a catalog).
#    Ground truth: GET /api/v1/skills carries per-role bindings {role,prompt,scopes,...}.
st, body = http("GET", "/api/v1/skills", headers=AUTH)
role_ok, role_detail = False, f"status={st}"
if isinstance(body, dict):
    binds = body.get("bindings") or []
    chosen = None
    for b in binds:
        if b.get("role") == "mayor":
            chosen = b; break
    chosen = chosen or (binds[0] if binds else None)
    if chosen:
        plen = len(chosen.get("prompt") or "")
        slen = len(chosen.get("scopes") or [])
        role_ok = plen > 0 and slen > 0
        role_detail = f"role={chosen.get('role')} prompt_len={plen} scopes={slen}"
    else:
        role_detail = "no role bindings in /api/v1/skills"
record("3 role prompt+scopes non-empty", role_ok, role_detail)

# 4. >=1 IdP on the PUBLIC login page (the §4.2 OAuth seed + the §4a enable step).
st, body = http("GET", "/auth/providers")
idp_ok = isinstance(body, list) and len(body) >= 1
record("4 >=1 IdP provider", idp_ok,
       f"status={st} providers={[p.get('id') for p in body] if isinstance(body, list) else body}")

# 5. Rig catalog populated (the §4.3 rig seed) — via MCP rig_list.
rig_ok, rig_detail = False, "mcp session not established"
if mcp_ready:
    st, resp, text = mcp_client.call("rig_list")
    rigs = []
    try: rigs = json.loads(text).get("rigs", []) if text else []
    except Exception: pass
    rig_ok = len(rigs) >= 1
    rig_detail = (f"{len(rigs)} rig(s): {[r.get('prefix') for r in rigs]}" if rig_ok
                  else f"EMPTY rig catalog (seed_rigs produced 0 rows) text={text}")
record("5 rig catalog populated", rig_ok, rig_detail)

# 6. Quota account — onboarding the credentialed account needs a human OAuth handshake +
#    the cost-gated rotation daemon (runbook §4c), out of scope for an unattended smoke.
#    Assert the surface is OPERATIONAL (quota_list responds) and report whether >=1 *healthy*
#    account exists (the documented post-boot manual step). Honest: surface up != account on.
quota_surface_ok, healthy, quota_detail = False, 0, "mcp session not established"
if mcp_ready:
    st, resp, text = mcp_client.call("quota_list")
    quota_surface_ok = text is not None
    if text:
        healthy = text.count('"Healthy"')
    quota_detail = f"surface up; healthy_accounts={healthy} (onboard = human OAuth step, §4c)"
record("6 quota surface operational", quota_surface_ok, quota_detail)

# 7. /mcp authenticated + issues.* operational (the tracker).
mcp_ok, mcp_detail = False, "mcp session not established"
if mcp_ready:
    st, resp, text = mcp_client.call("issues_list_execute")
    mcp_ok = text is not None and ("rows" in text or "total" in text)
    mcp_detail = (f"issues_list ok ({text[:80]})" if mcp_ok
                  else f"status={st} resp={json.dumps(resp)[:140]}")
record("7 /mcp issues.* operational", mcp_ok, mcp_detail)

print(json.dumps({"checks": checks}))
PY
)"

# ---- report ----------------------------------------------------------------------------
log "RESULTS"
# Pass the JSON via env (never inline it into the heredoc — a `detail` string can carry
# quotes/braces that would corrupt a triple-quoted literal).
RC=0
ASSERT_JSON="$ASSERT_JSON" python3 - <<'PY' || RC=$?
import json, os, sys
d = json.loads(os.environ["ASSERT_JSON"])
checks = d["checks"]
fails = sum(1 for c in checks if not c["ok"])
for c in checks:
    status = "PASS" if c["ok"] else "FAIL"
    print(f"  [{status}] {c['name']:<34} {c['detail']}")
print()
print(f"  {len(checks)-fails}/{len(checks)} checks passed.")
sys.exit(1 if fails else 0)
PY

if [[ "${KEEP_UP:-0}" == "1" ]]; then
  log "KEEP_UP=1 — stack left running on ${BASE_URL} (tear down with: $0 teardown)"
  trap - EXIT
fi

exit $RC
