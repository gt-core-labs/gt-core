#!/usr/bin/env bash
# Onboard a Claude account into the quota rotation keychain (hq-greenfield-seeds.4).
#
# This drives the SAME mechanism the web "Add account" flow uses (the real live path,
# `crates/modules/gt-composition/src/onboard.rs`), but headless from a shell:
#
#   1. POST /api/v1/quota/onboard/start          → server allocates a per-account
#      CLAUDE_CONFIG_DIR under the accounts root, spawns `claude auth login`, and
#      returns {session_id, url}.
#   2. A HUMAN opens the URL, authenticates the Claude account, and copies the OOB code.
#      (This step is irreducibly interactive — the OAuth handshake has a human in the
#      middle. The script pauses here and prompts for the code.)
#   3. POST /api/v1/quota/onboard/complete {session_id, code} → the server writes the
#      code to the live login process, reads the account email via
#      `claude auth status --json`, and registers it event-sourced as
#      `quota.account_registered.v1` in the workspace quota log (the same event the
#      `quota.register` MCP tool / `POST /api/v1/quota/account` REST route emit). The
#      orchestration daemon (gt-orch-server, profile `orchd`) hydrates its rotation
#      keychain by replaying that log — so the account is picked up WITHOUT an env edit.
#
# Idempotent: before onboarding, the script lists the already-registered accounts
# (`GET /api/v1/quota/`) and skips when the target email is already present. The account
# id IS the login email (captured from the handshake, never typed), so pass --email only
# to make the idempotency check exact; with no --email the script onboards unconditionally
# and prints the email it captured.
#
# NEVER embeds a secret: no credential material is read, written, or echoed by this
# script. The CLAUDE_CONFIG_DIR contents live only on the server's accounts-root volume.
#
# Requirements: bash, curl, python3 (JSON parsing only — no extra deps).
#
# Usage:
#   scripts/onboard-quota-account.sh --url https://gt.example.com --token "$GT_TOKEN"
#   scripts/onboard-quota-account.sh --url https://gt.example.com --token "$GT_TOKEN" \
#       --email me@example.com           # skip if already registered
#
#   GT_URL / GT_TOKEN env vars are honored as defaults for --url / --token.
#
# The token must carry `quota.write` (or `*`); the seeded admin's PAT/JWT does. Mint one
# with `gt login` / a PAT from /security, or reuse the admin bearer.
set -euo pipefail

GT_URL="${GT_URL:-}"
GT_TOKEN="${GT_TOKEN:-}"
EMAIL=""

usage() {
  sed -n '2,46p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --url)   GT_URL="$2"; shift 2 ;;
    --token) GT_TOKEN="$2"; shift 2 ;;
    --email) EMAIL="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown arg: $1" >&2; usage 1 ;;
  esac
done

[ -n "$GT_URL" ]   || { echo "error: --url (or GT_URL) is required" >&2; exit 2; }
[ -n "$GT_TOKEN" ] || { echo "error: --token (or GT_TOKEN) is required" >&2; exit 2; }

# Strip a trailing slash so the joined paths are clean.
GT_URL="${GT_URL%/}"
AUTH=(-H "Authorization: Bearer ${GT_TOKEN}")

# --- json helpers (python3, stdlib only) -----------------------------------------------
jget() { python3 -c 'import sys,json; print(json.load(sys.stdin).get(sys.argv[1],""))' "$1"; }

# --- idempotency: is this email already a rotation candidate? ---------------------------
if [ -n "$EMAIL" ]; then
  echo ">> checking whether ${EMAIL} is already registered (GET /api/v1/quota/)..." >&2
  existing="$(curl -fsS "${AUTH[@]}" "${GT_URL}/api/v1/quota/" || true)"
  if printf '%s' "$existing" | python3 -c '
import sys, json
want = sys.argv[1]
try:
    accs = json.load(sys.stdin).get("accounts", [])
except Exception:
    sys.exit(1)
sys.exit(0 if any(a.get("id") == want for a in accs) else 1)
' "$EMAIL"; then
    echo "== ${EMAIL} is already registered in the rotation pool — nothing to do (idempotent skip)." >&2
    exit 0
  fi
fi

# --- 1) start the login: allocate the dir + spawn `claude auth login` -------------------
echo ">> POST /api/v1/quota/onboard/start ..." >&2
start_resp="$(curl -fsS -X POST "${AUTH[@]}" "${GT_URL}/api/v1/quota/onboard/start")"
session_id="$(printf '%s' "$start_resp" | jget session_id)"
login_url="$(printf '%s' "$start_resp" | jget url)"

if [ -z "$session_id" ] || [ -z "$login_url" ]; then
  echo "error: start did not return a session_id + url; got: $start_resp" >&2
  exit 1
fi

# --- 2) the irreducible human step: authenticate in the browser, paste the OOB code -----
cat >&2 <<EOF

  ============================================================================
  HUMAN STEP — authenticate the Claude account (OAuth, out-of-band):

    1. Open this URL in a browser and log in AS THE ACCOUNT YOU WANT TO ADD
       (use an incognito window to force the account chooser if needed):

       ${login_url}

    2. Approve the login. The page shows an authorization CODE.
    3. Paste that code below.

  The login process is held open on the server (session ${session_id});
  it will time out if you wait too long (~2 min after you submit the code).
  ============================================================================

EOF

printf 'Paste the OOB code: ' >&2
read -r code
code="$(printf '%s' "$code" | tr -d '[:space:]')"
[ -n "$code" ] || { echo "error: empty code" >&2; exit 1; }

# --- 3) complete: feed the code, capture the email, register into the quota log ---------
echo ">> POST /api/v1/quota/onboard/complete ..." >&2
complete_resp="$(curl -fsS -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"session_id": sys.argv[1], "code": sys.argv[2]}))' "$session_id" "$code")" \
  "${GT_URL}/api/v1/quota/onboard/complete")"

account="$(printf '%s' "$complete_resp" | jget account)"
config_dir="$(printf '%s' "$complete_resp" | jget config_dir)"

if [ -z "$account" ]; then
  echo "error: complete did not return an account; got: $complete_resp" >&2
  exit 1
fi

cat >&2 <<EOF

== Registered Claude account '${account}'
   config_dir: ${config_dir}
   (emitted quota.account_registered.v1 into the workspace quota log)

Next:
  - The orchestration daemon (gt-orch-server, profile 'orchd') hydrates its rotation
    keychain from this log on (re)start. Restart it if it was already running, or let
    the next boot pick up the account:
        docker compose --profile orchd restart gt-app-orchd
  - Confirm the pool sees it:
        curl -fsS -H "Authorization: Bearer \$GT_TOKEN" ${GT_URL}/api/v1/quota/
    or the MCP tool  quota.list  (look for id '${account}', status Healthy).
EOF
