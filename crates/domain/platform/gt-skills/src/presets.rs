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

use crate::{SkillCatalog, SkillEvent, SkillState};

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
/// - `polecat` → `issues.read`, `issues.write` (work + claim + transition its bead).
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

/// The canonical INTERACTIVE-session role catalog, as `skills.*` events to seed a fresh workspace's
/// log at boot (`hq-role-mcp`). Unlike [`agent_least_privilege_catalog`] (the daemon's in-memory
/// polecat policy), this is event-sourced and operator-overridable via the Knowledge REST surface —
/// it just guarantees a clean deploy on a new machine gives each role a WORKING least-privilege MCP
/// grant out of the box, instead of an empty token. The per-role token minter (`terminal.rs`) folds
/// a role's enabled skills' `default_scopes` into the session's `.gt-config`, which `gt mcp` reads.
///
/// Seeded ONLY when the workspace catalog is empty, so it never clobbers a human-curated catalog.
/// Pure scope-carrier skills (no `SKILL.md` body); a role's grant is the union of its skills' scopes:
/// - `tracker-ops` → `workspace.member` + `graph.read` — full operate (file + dispatch + graph).
/// - `merge-ops`   → `merge.read` + `merge.write`.
/// - `bead-work`   → `issues.read` + `issues.write`.
/// - `observe`     → `issues.read`.
///
/// Bindings: mayor/sheriff/overseer operate; refinery merges; polecat/dog code beads; witness/deacon
/// observe. `workspace.member` expands to the operational namespace on MCP (`Scope::from_workspace_
/// claim`) but never to `workspace.create`, so no seeded role is an admin.
pub fn workspace_seed_events(now: u64) -> Vec<SkillEvent> {
    const SKILLS: &[(&str, &[&str])] = &[
        ("tracker-ops", &["workspace.member", "graph.read"]),
        ("merge-ops", &["merge.read", "merge.write"]),
        ("bead-work", &["issues.read", "issues.write"]),
        ("observe", &["issues.read"]),
    ];
    const BINDINGS: &[(&str, &str)] = &[
        ("mayor", "tracker-ops"),
        ("sheriff", "tracker-ops"),
        ("overseer", "tracker-ops"),
        ("refinery", "merge-ops"),
        ("polecat", "bead-work"),
        ("dog", "bead-work"),
        ("witness", "observe"),
        ("deacon", "observe"),
    ];
    let mut events = Vec::with_capacity(SKILLS.len() + BINDINGS.len());
    for (id, scopes) in SKILLS {
        events.push(SkillEvent::Registered {
            skill: (*id).to_string(),
            label: (*id).to_string(),
            description: String::new(),
            default_scopes: scopes.iter().map(|x| (*x).to_string()).collect(),
            body: String::new(),
            group: String::new(),
            now_secs: now,
        });
    }
    for (role, skill) in BINDINGS {
        events.push(SkillEvent::EnabledForRole {
            role: (*role).to_string(),
            skill: (*skill).to_string(),
            now_secs: now,
        });
    }
    // hq-role-scopes: scopes are read from `RoleBinding::scopes`, not derived from skills — so a
    // fresh workspace must also seed each role's scopes, else a seeded role would have skills but an
    // empty grant. Replay the register/enable events and append the one-shot migration's
    // `RoleScopesSet` per role, so the seeded catalog grants each role a working set out of the box.
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
    fn workspace_seed_grants_each_role_a_working_least_privilege_set() {
        // Replaying the seed events yields a catalog where every role resolves to a non-empty,
        // non-wildcard scope set — a fresh deploy operates out of the box (hq-role-mcp).
        let mut s = SkillState::default();
        for ev in workspace_seed_events(0) {
            s.apply(&ev);
        }
        let c = s.catalog;
        // mayor/sheriff/overseer operate via the member grant; never the wildcard.
        for role in ["mayor", "sheriff", "overseer"] {
            assert_eq!(
                scopes(&c, role),
                vec!["workspace.member", "graph.read"],
                "{role}"
            );
        }
        assert_eq!(scopes(&c, "refinery"), vec!["merge.read", "merge.write"]);
        assert_eq!(scopes(&c, "polecat"), vec!["issues.read", "issues.write"]);
        assert_eq!(scopes(&c, "dog"), vec!["issues.read", "issues.write"]);
        assert_eq!(scopes(&c, "witness"), vec!["issues.read"]);
        assert_eq!(scopes(&c, "deacon"), vec!["issues.read"]);
        for role in [
            "mayor", "sheriff", "overseer", "refinery", "polecat", "dog", "witness", "deacon",
        ] {
            let sc = scopes(&c, role);
            assert!(!sc.is_empty(), "{role} must have a grant");
            assert!(!sc.iter().any(|s| s == "*"), "{role} must not carry '*'");
        }
    }
}
