#!/usr/bin/env python3
"""Regenerate the greenfield rig-catalog seed (hq-greenfield-seeds.5) from a live deploy.

The rigs (`gt`/`gt_core`/`gtmcp`/`gtproxy`/`gtweb`) are registered by hand with `rig.add` and live
ONLY in the running deploy's per-tenant `ws_default.rigs` table — a clean cluster comes up with an
empty catalog (no prefix routing, no dispatch) until an operator re-runs every `rig.add`. This
script extracts the declarative rig config from a live Postgres and rewrites
`crates/domain/platform/gt-rig/seeds/rigs.json`, which `rig_seed::seed_rigs` embeds and the server
replays into an EMPTY `rigs` table at boot.

`registered_at` is NOT extracted — it is a per-deploy artifact; the boot path stamps the seed's
`registered_at_secs` from the boot clock. `git_connection_ref` IS carried (it is part of a rig's
declarative identity), but a non-null ref names a `public.vcs_connections` row that is itself a
runtime GitHub-App install artifact (hq-greenfield-seeds.3) — so a seeded ref only resolves on a
deploy where that connection has been re-established. In prod all five rigs have it null (SSH clone).

Usage (docker compose — adjust the conn string / container / schema for your deploy):
    docker exec gt-app-pg psql \
        "postgres://gtapp:gtapp@localhost:5432/gtapp" -tAc \
        "SELECT json_agg(row_to_json(t)) FROM (
            SELECT name, prefix, git_url, push_url, upstream_url, default_branch,
                   worktree_root, git_connection_ref
            FROM ws_default.rigs ORDER BY name) t;" \
      | python3 scripts/extract-rigs-seed.py

    # or point it at a file of that JSON array:
    python3 scripts/extract-rigs-seed.py rigs.json

Then verify:
    cargo test -p gt-rig rig_seed
"""
import json
import os
import sys

OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "crates", "domain", "platform", "gt-rig", "seeds", "rigs.json",
)


def main() -> int:
    raw = open(sys.argv[1]).read() if len(sys.argv) > 1 else sys.stdin.read()
    raw = raw.strip()
    if not raw or raw == "null":
        print("no rigs in the source (empty rigs table); nothing to write", file=sys.stderr)
        return 1
    rows = json.loads(raw)
    if isinstance(rows, dict):  # tolerate a single row_to_json object
        rows = [rows]

    rigs = []
    for r in rows:
        rigs.append({
            "name": r["name"],
            "prefix": r["prefix"],
            "git_url": r["git_url"],
            "push_url": r.get("push_url"),
            "upstream_url": r.get("upstream_url"),
            "default_branch": r["default_branch"],
            "worktree_root": r.get("worktree_root"),
            # Carried as declarative identity, but a non-null value names a runtime
            # vcs_connections row (hq-greenfield-seeds.3) the target deploy must re-establish.
            "git_connection_ref": r.get("git_connection_ref"),
        })

    with open(OUT, "w") as fh:
        json.dump({"rigs": rigs}, fh, indent=2)
        fh.write("\n")
    print(f"wrote {len(rigs)} rig(s) to {OUT}", file=sys.stderr)
    for r in rigs:
        ref = r["git_connection_ref"]
        note = f", connection={ref}" if ref else ""
        print(f"  - {r['name']} (prefix={r['prefix']}{note})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
