//! Frontier authorization. The domain knows nothing about identity; the MCP server
//! (hq-core-host.3) resolves it at the start of the connection and attaches a [`Scope`]
//! to every dispatch, calling [`Scope::check`] before the tool runs.
//!
//! Patterns are exact or trailing-`*` globs over the dotted tool name (e.g. `agent.*`).
//!
//! Source of truth: [`RbacConfig`]. An actor absent from the config resolves to a
//! closed scope ([`Scope::denied`]) — deny by default, never admin.

use std::collections::BTreeSet;

use crate::config::RbacConfig;
use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct Scope {
    pub actor: String,
    pub allow: BTreeSet<String>,
    pub validate_only: bool,
}

impl Scope {
    pub fn admin(actor: impl Into<String>) -> Self {
        let mut allow = BTreeSet::new();
        allow.insert("*".into());
        Self {
            actor: actor.into(),
            allow,
            validate_only: false,
        }
    }

    pub fn read_only(actor: impl Into<String>) -> Self {
        let mut allow = BTreeSet::new();
        allow.insert("*".into());
        Self {
            actor: actor.into(),
            allow,
            validate_only: true,
        }
    }

    /// Closed scope: empty allow-list, so [`Scope::check`] rejects every tool. The
    /// default for an unknown actor or a missing config — deny first.
    pub fn denied(actor: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            allow: BTreeSet::new(),
            validate_only: true,
        }
    }

    /// Build a `Scope` from the unified RBAC config. Unknown actors fold to
    /// [`Scope::denied`] so the deny-by-default posture is preserved bit-for-bit.
    pub fn from_rbac(cfg: &RbacConfig, actor: &str) -> Self {
        match cfg.actor(actor) {
            Some(spec) => Self {
                actor: actor.to_string(),
                allow: spec.allow.clone(),
                validate_only: spec.validate_only,
            },
            None => Self::denied(actor),
        }
    }

    /// Returns `Ok` if the scope grants this `tool` and the action variant
    /// (`.validate` vs `.execute`) is permitted.
    pub fn check(&self, tool: &str) -> Result<(), AppError> {
        if self.validate_only && tool.ends_with(".execute") {
            return Err(AppError::Validation(format!(
                "scope {} is validate_only; cannot call {tool}",
                self.actor
            )));
        }
        let allowed = self.allow.iter().any(|pat| matches_pattern(pat, tool));
        if !allowed {
            return Err(AppError::Validation(format!(
                "tool {tool} not in scope for {}",
                self.actor
            )));
        }
        Ok(())
    }
}

fn matches_pattern(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return name == prefix || name.starts_with(&format!("{prefix}."));
    }
    pattern == name
}

/// Convenience trait — `cfg.resolve(actor)` returns the `Scope` directly. Kept as a
/// trait (rather than an inherent method on `RbacConfig`) so the config type stays a
/// pure data holder; the bridge lives here.
pub trait ResolveScope {
    fn resolve(&self, actor: &str) -> Scope;
}

impl ResolveScope for RbacConfig {
    fn resolve(&self, actor: &str) -> Scope {
        Scope::from_rbac(self, actor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(set: &[&str]) -> BTreeSet<String> {
        set.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn admin_passes_everything() {
        let s = Scope::admin("max");
        s.check("agent.add.execute").unwrap();
        s.check("scheduling.enqueue.validate").unwrap();
    }

    #[test]
    fn validate_only_blocks_execute() {
        let s = Scope::read_only("watcher");
        s.check("agent.transition.validate").unwrap();
        assert!(s.check("agent.transition.execute").is_err());
    }

    #[test]
    fn glob_matches_dotted_prefix() {
        let s = Scope {
            actor: "scoped".into(),
            allow: allow(&["agent.*"]),
            validate_only: false,
        };
        s.check("agent.add.execute").unwrap();
        assert!(s.check("scheduling.enqueue.execute").is_err());
    }

    #[test]
    fn denied_scope_rejects_everything() {
        let s = Scope::denied("ghost");
        assert!(s.check("agent.add.validate").is_err());
        assert!(s.check("agent.add.execute").is_err());
    }

    const TOML_CFG: &str = r#"
[actors.max]
allow = ["*"]

[actors.watcher]
allow = ["agent.*", "scheduling.enqueue.validate"]
validate_only = true
"#;

    const JSON_CFG: &str = r#"
{ "actors": {
    "max": { "allow": ["*"] },
    "watcher": { "allow": ["agent.*", "scheduling.enqueue.validate"], "validate_only": true }
} }
"#;

    fn assert_resolves(cfg: &RbacConfig) {
        let max = cfg.resolve("max");
        assert!(!max.validate_only);
        max.check("orch.launch_convoy.execute").unwrap();

        let watcher = cfg.resolve("watcher");
        assert!(watcher.validate_only);
        watcher.check("agent.transition.validate").unwrap();
        assert!(
            watcher.check("agent.transition.execute").is_err(),
            "validate_only blocks execute"
        );
        assert!(
            watcher.check("merge.submit.validate").is_err(),
            "not in allow list"
        );

        let ghost = cfg.resolve("ghost");
        assert!(ghost.check("agent.add.validate").is_err());
    }

    #[test]
    fn toml_and_json_configs_resolve_per_actor() {
        assert_resolves(&RbacConfig::from_toml(TOML_CFG).unwrap());
        assert_resolves(&RbacConfig::from_json(JSON_CFG).unwrap());
    }

    #[test]
    fn empty_config_denies_all_actors() {
        let cfg = RbacConfig::default();
        assert!(cfg.resolve("anyone").check("agent.add.validate").is_err());
    }

    #[test]
    fn unified_config_with_roles_block_still_resolves_mcp_scope() {
        let cfg = RbacConfig::from_toml(
            r#"
[actors.claude-host]
allow = ["*"]
roles = ["sheriff"]

[roles.sheriff]
scopes = ["beads.write"]
"#,
        )
        .unwrap();
        let scope = cfg.resolve("claude-host");
        scope.check("agent.add.execute").unwrap();
        scope.check("scheduling.enqueue.execute").unwrap();
    }
}
