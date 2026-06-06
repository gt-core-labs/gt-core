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

    s.catalog
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
}
