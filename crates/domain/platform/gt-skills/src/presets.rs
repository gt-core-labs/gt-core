//! Canonical least-privilege skill catalog for the autonomous agent roles
//! (`hq-agent-provisioning.4`).
//!
//! gt-skills' catalog is normally operator-driven (REST/MCP `skills.*`), persisted in the
//! [`SkillsRepository`](crate::SkillsRepository). But the orchestration daemon is a separate
//! process with no access to that store and no skills in the per-workspace event log, so it builds
//! its agent scope policy from THIS code preset: deterministic, versioned, reviewed in a PR — it
//! can never drift to `*`. The per-agent token minter (`hq-agent-provisioning.3`) resolves a slung
//! polecat/dog's scopes via [`SkillCatalog::scopes_for_roles`] against this catalog.
//!
//! Least-privilege per role. Caveat: RBAC scopes are `resource.verb`, not row-level, so a polecat's
//! `issues.write` authorizes the issues namespace, not literally "only its own bead" — bounding a
//! polecat to a single bead is a server-side claim check, not expressible in the JWT.

use serde::Deserialize;

use crate::{SkillCatalog, SkillEvent, SkillState};

/// The curated interactive-role Knowledge, extracted from a live workspace's `skills.*` log and
/// versioned here (`hq-greenfield-seeds.2`). On a greenfield deploy there is no prior state, so this
/// is what makes the seeded roles *functional* — real `SKILL.md` bodies, per-role system prompts and
/// model configs — instead of empty scope-carriers. See `docs/ops/greenfield-seeds.md` for how to
/// regenerate it from a running deploy (replay the workspace's `skills.*` events, role-functional
/// subset = skills bound to ≥1 role). Embedded as bytes so the seed travels with the binary
/// (orchestrator-agnostic — no external file under k8s).
const KNOWLEDGE_SEED_JSON: &str = include_str!("../seeds/knowledge.json");

/// A skill in the versioned seed: the catalog row plus its `SKILL.md` body and scope grant.
#[derive(Deserialize)]
struct SeedSkill {
    id: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    default_scopes: Vec<String>,
    #[serde(default)]
    body: String,
}

/// A role in the versioned seed: its system prompt, model config, and enabled-skill bindings. Scopes
/// are intentionally absent — they derive from the enabled skills' `default_scopes` via
/// [`SkillCatalog::role_scopes_migration`] at seed time, exactly as the live deploy resolved them.
#[derive(Deserialize)]
struct SeedRole {
    role: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    permission_mode: String,
    #[serde(default)]
    effort: String,
    #[serde(default)]
    enabled: Vec<String>,
}

#[derive(Deserialize)]
struct KnowledgeSeed {
    skills: Vec<SeedSkill>,
    roles: Vec<SeedRole>,
}

fn register(s: &mut SkillState, id: &str, scopes: &[&str]) {
    s.apply(&SkillEvent::Registered {
        skill: id.to_string(),
        label: id.to_string(),
        description: String::new(),
        default_scopes: scopes.iter().map(|x| x.to_string()).collect(),
        body: String::new(),
        group: String::new(),
        now_secs: 0,
    });
}

fn enable(s: &mut SkillState, role: &str, skill: &str) {
    s.apply(&SkillEvent::EnabledForRole {
        role: role.to_string(),
        skill: skill.to_string(),
        now_secs: 0,
    });
}

/// Build the canonical least-privilege catalog binding each automatic agent role to the minimal
/// scope set its work needs. No role is granted `*`; an unbound role (e.g. `overseer`) gets nothing.
///
/// - `polecat` → `issues.read`, `issues.write`, `merge.write` (work + claim + transition + submit merge).
/// - `sheriff` → `merge.read`, `merge.write` (drive merges / github).
/// - `refinery` → `merge.write` (submit MERGE_READY).
/// - `witness` → `issues.read` (observe only).
/// - `deacon` → `issues.read` (read-only supervisory).
pub fn agent_least_privilege_catalog() -> SkillCatalog {
    let mut s = SkillState::default();
    register(&mut s, "bead-work", &["issues.read", "issues.write"]);
    register(&mut s, "merge-ops", &["merge.read", "merge.write"]);
    register(&mut s, "merge-submit", &["merge.write"]);
    register(&mut s, "observe", &["issues.read"]);

    enable(&mut s, "polecat", "bead-work");
    enable(&mut s, "polecat", "merge-submit");
    enable(&mut s, "sheriff", "merge-ops");
    enable(&mut s, "refinery", "merge-submit");
    enable(&mut s, "witness", "observe");
    enable(&mut s, "deacon", "observe");

    // hq-role-scopes: a role's scopes are now read from `RoleBinding::scopes`, not derived from its
    // skills. Seed each role's scopes from its enabled-skill `default_scopes` via the one-shot
    // migration so the daemon's per-agent token minter (`scopes_for_roles`) resolves the same
    // least-privilege set it always did.
    for ev in s.catalog.role_scopes_migration(0) {
        s.apply(&ev);
    }

    s.catalog
}

/// The canonical INTERACTIVE-session role Knowledge, as `skills.*` events to seed a fresh workspace's
/// log at boot (`hq-role-mcp`, `hq-greenfield-seeds.2`). Unlike [`agent_least_privilege_catalog`]
/// (the daemon's in-memory polecat policy), this is event-sourced and operator-overridable via the
/// Knowledge REST surface — it just guarantees a clean deploy on a new machine comes up with
/// *functional* roles out of the box instead of empty tokens and blank prompts.
///
/// The content is the versioned extract in [`KNOWLEDGE_SEED_JSON`] (`seeds/knowledge.json`), pulled
/// from a live deploy — NOT invented here. Each role gets:
/// - its real `SKILL.md`-bearing skills ([`SkillEvent::Registered`] carries `body` + `default_scopes`),
/// - its enabled-skill bindings ([`SkillEvent::EnabledForRole`]),
/// - its system prompt ([`SkillEvent::RolePromptSet`] — the terminal writes it as `<workdir>/CLAUDE.md`),
/// - its model config ([`SkillEvent::RoleModelSet`] — model + permission-mode + effort the launch stamps).
///
/// Role scopes are NOT stored per-role: they derive from the enabled skills' `default_scopes` via
/// [`SkillCatalog::role_scopes_migration`], exactly as the live deploy resolved them (e.g. mayor's
/// `notify-ops` → `notifications.write`, `tracker-ops` → `workspace.member` + `graph.read`).
/// `workspace.member` expands to the operational namespace on MCP (`Scope::from_workspace_claim`) but
/// never to `workspace.create`, so no seeded role is an admin.
///
/// Seeded ONLY when the workspace catalog is empty, so it never clobbers a human-curated catalog
/// (the caller in `rest_modules.rs` gates on `catalog.is_empty()`).
pub fn workspace_seed_events(now: u64) -> Vec<SkillEvent> {
    let seed: KnowledgeSeed = serde_json::from_str(KNOWLEDGE_SEED_JSON)
        .expect("seeds/knowledge.json is a versioned, build-time-checked artifact");

    let mut events = Vec::new();
    for sk in &seed.skills {
        events.push(SkillEvent::Registered {
            skill: sk.id.clone(),
            label: sk.label.clone(),
            description: sk.description.clone(),
            default_scopes: sk.default_scopes.clone(),
            body: sk.body.clone(),
            group: sk.group.clone(),
            now_secs: now,
        });
    }
    for r in &seed.roles {
        for skill in &r.enabled {
            events.push(SkillEvent::EnabledForRole {
                role: r.role.clone(),
                skill: skill.clone(),
                now_secs: now,
            });
        }
        if !r.prompt.is_empty() {
            events.push(SkillEvent::RolePromptSet {
                role: r.role.clone(),
                prompt: r.prompt.clone(),
                now_secs: now,
            });
        }
        if !(r.model.is_empty() && r.permission_mode.is_empty() && r.effort.is_empty()) {
            events.push(SkillEvent::RoleModelSet {
                role: r.role.clone(),
                model: r.model.clone(),
                permission_mode: r.permission_mode.clone(),
                effort: r.effort.clone(),
                now_secs: now,
            });
        }
    }
    // hq-role-scopes: scopes are read from `RoleBinding::scopes`, not derived from skills on the live
    // read path — so a fresh workspace must also seed each role's scopes, else a seeded role would
    // have skills but an empty grant. Replay the register/enable events and append the one-shot
    // migration's `RoleScopesSet` per role (the union of its enabled skills' `default_scopes`), so the
    // seeded catalog resolves the same grants the live deploy did.
    let mut s = SkillState::default();
    for ev in &events {
        s.apply(ev);
    }
    events.extend(s.catalog.role_scopes_migration(now));
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scopes(c: &SkillCatalog, role: &str) -> Vec<String> {
        c.scopes_for_roles(&[role.to_string()])
    }

    #[test]
    fn each_role_gets_its_minimal_scopes_and_never_the_wildcard() {
        let c = agent_least_privilege_catalog();
        assert_eq!(scopes(&c, "polecat"), vec!["issues.read", "issues.write"]);
        assert_eq!(scopes(&c, "sheriff"), vec!["merge.read", "merge.write"]);
        assert_eq!(scopes(&c, "refinery"), vec!["merge.write"]);
        assert_eq!(scopes(&c, "witness"), vec!["issues.read"]);
        assert_eq!(scopes(&c, "deacon"), vec!["issues.read"]);
        // No automatic role is ever the operator wildcard.
        for role in [
            "polecat", "sheriff", "refinery", "witness", "deacon", "overseer",
        ] {
            assert!(
                !scopes(&c, role).iter().any(|s| s == "*"),
                "{role} must not carry '*'"
            );
        }
    }

    #[test]
    fn an_unbound_role_gets_nothing() {
        let c = agent_least_privilege_catalog();
        assert!(scopes(&c, "overseer").is_empty());
        assert!(scopes(&c, "nobody").is_empty());
    }

    #[test]
    fn workspace_seed_replays_the_curated_knowledge_extract() {
        // Replaying the versioned seed (`seeds/knowledge.json`) yields a catalog matching the live
        // deploy it was extracted from: real skill bodies, per-role prompts + models, and scopes that
        // derive from the bound skills — a greenfield deploy comes up with FUNCTIONAL roles
        // (hq-greenfield-seeds.2), not empty scope-carriers.
        let mut s = SkillState::default();
        for ev in workspace_seed_events(0) {
            s.apply(&ev);
        }
        let c = s.catalog;
        let binding = |role: &str| {
            c.bindings()
                .into_iter()
                .find(|b| b.role == role)
                .unwrap_or_else(|| panic!("seed must bind role `{role}`"))
        };

        // Scopes derive from the enabled skills' `default_scopes`, exactly as the live deploy resolved
        // them: notify-ops → notifications.write, tracker-ops → workspace.member + graph.read,
        // crew-commit → workspace.member, memory-access → memory.{read,write}.
        assert_eq!(
            scopes(&c, "mayor"),
            vec!["notifications.write", "workspace.member", "graph.read"],
        );
        assert_eq!(
            scopes(&c, "polecat"),
            vec!["workspace.member", "memory.read", "memory.write"],
        );
        assert_eq!(scopes(&c, "dog"), vec!["workspace.member"]);
        assert_eq!(scopes(&c, "refinery"), vec!["workspace.member"]);

        // Every role carries a non-empty system prompt + model config — the functional payload a
        // greenfield deploy was missing — and a real skill body travels with the catalog.
        for role in [
            "mayor", "sheriff", "overseer", "refinery", "polecat", "dog", "witness", "deacon",
        ] {
            let b = binding(role);
            assert!(!b.prompt.trim().is_empty(), "{role} must have a prompt");
            assert!(
                !b.model_config.is_unset(),
                "{role} must have a model config"
            );
            assert!(
                !b.enabled_skills.is_empty(),
                "{role} must have enabled skills"
            );
            assert!(
                !scopes(&c, role).iter().any(|s| s == "*"),
                "{role} must not carry '*'"
            );
        }
        // The bodies are the point of the seed: a known role-bound skill carries its `SKILL.md`.
        let mayor = binding("mayor");
        assert!(mayor.enabled_skills.contains("tracker-ops"));
        let tracker_ops = c.get("tracker-ops").expect("tracker-ops registered");
        assert!(
            !tracker_ops.body.trim().is_empty(),
            "skill bodies must be seeded, not blank"
        );
    }
}
