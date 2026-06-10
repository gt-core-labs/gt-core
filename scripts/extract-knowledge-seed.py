#!/usr/bin/env python3
"""Regenerate the greenfield knowledge seed (hq-greenfield-seeds.2) from a live deploy.

The interactive-role Knowledge — role system prompts, model configs, and the `SKILL.md`
bodies a role's bound skills carry — is curated live via the Knowledge REST surface and is
NOT reproducible on a clean cluster on its own. This script replays a workspace's `skills.*`
event log to reconstruct the current catalog, then writes the role-functional subset (skills
bound to >=1 role) to `crates/domain/platform/gt-skills/seeds/knowledge.json`, which
`presets::workspace_seed_events` embeds and seeds into an empty catalog at boot.

Scopes are deliberately NOT emitted per role: they derive from the bound skills'
`default_scopes` via `SkillCatalog::role_scopes_migration` at seed time, exactly as the live
deploy resolved them.

Usage:
    # Point at the event-log dir for the source workspace (e.g. the prod `default` workspace).
    # Under docker compose the volume is gt-app_gt-eventlog; copy the workspace dir out first:
    #   docker cp gt-app-mcp-server:/var/lib/gt-core/default /tmp/knowledge-src
    python3 scripts/extract-knowledge-seed.py /tmp/knowledge-src

Replay semantics mirror `SkillState::apply` (last-write-wins; Retired drops the skill + its
bindings). Events are ordered by filename (events-YYYY-MM-DD.jsonl) then line order.
"""
import glob
import json
import os
import sys


def reconstruct(src_dir):
    skills = {}  # id -> {label, description, default_scopes, body, group}
    roles = {}   # role -> {prompt, enabled:set, model:{...}}

    def role(r):
        return roles.setdefault(r, {"prompt": "", "enabled": set(), "model": None})

    for path in sorted(glob.glob(os.path.join(src_dir, "events-*.jsonl"))):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not ev.get("type", "").startswith("skills."):
                    continue
                payload = ev["payload"]
                kind = next(iter(payload))
                v = payload[kind]
                if kind == "Registered":
                    skills[v["skill"]] = {
                        "label": v.get("label", ""),
                        "description": v.get("description", ""),
                        "default_scopes": v.get("default_scopes", []),
                        "body": v.get("body", ""),
                        "group": v.get("group", ""),
                    }
                elif kind == "Retired":
                    skills.pop(v["skill"], None)
                    for d in roles.values():
                        d["enabled"].discard(v["skill"])
                elif kind == "EnabledForRole":
                    role(v["role"])["enabled"].add(v["skill"])
                elif kind == "DisabledForRole":
                    role(v["role"])["enabled"].discard(v["skill"])
                elif kind == "RolePromptSet":
                    role(v["role"])["prompt"] = v.get("prompt", "")
                elif kind == "RoleModelSet":
                    role(v["role"])["model"] = {
                        "model": v.get("model", ""),
                        "permission_mode": v.get("permission_mode", ""),
                        "effort": v.get("effort", ""),
                    }
    return skills, roles


def build_seed(skills, roles):
    bound = set()
    for d in roles.values():
        bound |= d["enabled"]

    out_skills = []
    for sid in sorted(bound):
        if sid not in skills:
            continue  # bound to a since-retired skill — drop the dangling binding
        s = skills[sid]
        out_skills.append({
            "id": sid,
            "label": s["label"],
            "description": s["description"],
            "group": s["group"],
            "default_scopes": s["default_scopes"],
            "body": s["body"],
        })

    out_roles = []
    for r in sorted(roles):
        d = roles[r]
        m = d["model"] or {}
        out_roles.append({
            "role": r,
            "prompt": d["prompt"],
            "model": m.get("model", ""),
            "permission_mode": m.get("permission_mode", ""),
            "effort": m.get("effort", ""),
            "enabled": [s for s in sorted(d["enabled"]) if s in skills],
        })
    return out_skills, out_roles


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    src = sys.argv[1]
    here = os.path.dirname(os.path.abspath(__file__))
    dst = os.path.join(
        here, "..", "crates", "domain", "platform", "gt-skills", "seeds", "knowledge.json"
    )
    skills, roles = reconstruct(src)
    out_skills, out_roles = build_seed(skills, roles)
    doc = {
        "_comment": (
            "GENERATED extract of the curated interactive-role Knowledge "
            "(hq-greenfield-seeds.2). Regenerate with scripts/extract-knowledge-seed.py — see "
            "docs/ops/greenfield-seeds.md. Role-functional subset: only skills bound to >=1 "
            "role. Scopes are NOT stored per-role: they derive from skill default_scopes via "
            "role_scopes_migration at seed time."
        ),
        "skills": out_skills,
        "roles": out_roles,
    }
    with open(dst, "w") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
        fh.write("\n")
    print(f"wrote {os.path.normpath(dst)}: {len(out_skills)} skills, {len(out_roles)} roles")


if __name__ == "__main__":
    main()
