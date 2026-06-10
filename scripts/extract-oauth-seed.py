#!/usr/bin/env python3
"""Regenerate the greenfield OAuth/IdP provider seed (hq-greenfield-seeds.3) from a live deploy.

The login providers (Google, …) are configured by hand in `/admin/providers` and live ONLY in the
running deploy's `public.oauth_providers` table — a clean cluster comes up with a blank login page.
This script extracts the NON-SECRET provider config from a live Postgres and rewrites
`crates/domain/platform/gt-auth/seeds/oauth-providers.json`, which `provider_seed::seed_providers`
embeds and the server replays into an EMPTY `oauth_providers` table at boot.

The OAuth `client_secret` is AES-256-GCM sealed at rest (GT_SECRET_KEY) and is NEVER extracted or
vendored: each seeded provider names the env var its cleartext secret is read from at boot
(`GT_OAUTH_SEED_SECRET_<ID>`, derived from the provider id), and a provider whose env is unset is
skipped — so re-running this script never leaks a credential into the repo.

Usage (docker compose — adjust the conn string / container for your deploy):
    docker exec gt-app-pg psql \
        "postgres://gtapp:gtapp@localhost:5432/gtapp" -tAc \
        "SELECT json_agg(row_to_json(t)) FROM (
            SELECT id, kind, display_name, client_id, issuer, authorize_endpoint,
                   token_endpoint, userinfo_endpoint, scopes, enabled, workspace_id
            FROM public.oauth_providers ORDER BY created_at) t;" \
      | python3 scripts/extract-oauth-seed.py

    # or point it at a file of that JSON array:
    python3 scripts/extract-oauth-seed.py providers.json

Then verify:
    cargo test -p gt-auth --features oauth provider_seed
"""
import json
import os
import re
import sys

OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "crates", "domain", "platform", "gt-auth", "seeds", "oauth-providers.json",
)


def secret_env_for(provider_id: str) -> str:
    """The env var a provider's cleartext client_secret is read from at boot.

    Mirrors the seed contract: GT_OAUTH_SEED_SECRET_<ID>, id upper-cased with every non-alphanumeric
    run collapsed to a single underscore (so e.g. `google` -> GT_OAUTH_SEED_SECRET_GOOGLE,
    `corp-sso` -> GT_OAUTH_SEED_SECRET_CORP_SSO).
    """
    suffix = re.sub(r"[^A-Za-z0-9]+", "_", provider_id).strip("_").upper()
    return f"GT_OAUTH_SEED_SECRET_{suffix}"


def main() -> int:
    raw = open(sys.argv[1]).read() if len(sys.argv) > 1 else sys.stdin.read()
    raw = raw.strip()
    if not raw or raw == "null":
        print("no providers in the source (empty oauth_providers table); nothing to write",
              file=sys.stderr)
        return 1
    rows = json.loads(raw)
    if isinstance(rows, dict):  # tolerate a single row_to_json object
        rows = [rows]

    providers = []
    for r in rows:
        pid = r["id"]
        providers.append({
            "id": pid,
            "kind": r["kind"],
            "display_name": r.get("display_name", "") or "",
            "client_id": r["client_id"],
            "issuer": r["issuer"],
            "authorize_endpoint": r["authorize_endpoint"],
            "token_endpoint": r["token_endpoint"],
            "userinfo_endpoint": r["userinfo_endpoint"],
            "scopes": r.get("scopes", "") or "",
            "enabled": bool(r.get("enabled", False)),
            "workspace_id": r.get("workspace_id"),
            # The secret is NEVER extracted — only the name of the env it is supplied from.
            "secret_env": secret_env_for(pid),
        })

    with open(OUT, "w") as fh:
        json.dump({"providers": providers}, fh, indent=2)
        fh.write("\n")
    print(f"wrote {len(providers)} provider(s) to {OUT}", file=sys.stderr)
    for p in providers:
        print(f"  - {p['id']} (kind={p['kind']}, secret <= {p['secret_env']})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
