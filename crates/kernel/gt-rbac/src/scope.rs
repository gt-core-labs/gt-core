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

/// JWT scope identifier for a workspace administrator — full tool authority within
/// the token's workspace (`hq-mcp-dispatch.9`). The workspace *confinement* (a token
/// for ws A cannot operate ws B) is enforced one tier up, at the MCP server boundary;
/// this constant names the *tool* grant the claim carries.
pub const WORKSPACE_ADMIN: &str = "workspace.admin";

/// JWT scope identifier for a workspace member — every operational tool except the
/// workspace-catalog mutations that provision/retire a tenant (an admin act).
pub const WORKSPACE_MEMBER: &str = "workspace.member";

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

    /// A workspace administrator's tool grant: every tool (the `*` allow-list),
    /// identical in tool-reach to [`Scope::admin`]. Named distinctly so the call
    /// site reads as "the admin of one workspace" — the tenant confinement is the
    /// MCP server's [`WorkspaceGuard`] (`hq-mcp-dispatch.9`), not this scope.
    pub fn workspace_admin(actor: impl Into<String>) -> Self {
        Self::admin(actor)
    }

    /// A workspace member's tool grant: every operational namespace, but **not** the
    /// workspace-catalog mutations (`workspace.create` — provisioning a tenant is an
    /// admin act). `workspace.info`/`workspace.list` reads stay allowed.
    pub fn workspace_member(actor: impl Into<String>) -> Self {
        let allow = [
            "issues.*",
            "meta.*",
            "rig.*",
            "quota.*",
            "merge.*",
            "convoy.*",
            "agent.*",
            "workspace.info",
            "workspace.list",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        Self {
            actor: actor.into(),
            allow,
            validate_only: false,
        }
    }

    /// Resolve a [`Scope`] from a JWT claim's granted scope strings — the MCP-side mirror of the
    /// REST guard, so one token authorizes both surfaces the same way (hq-rbac.2, REST parity).
    ///
    /// Precedence, deny-by-default throughout:
    ///
    /// 1. A bare `"*"` grant or a [`WORKSPACE_ADMIN`] role → the admin grant (every tool, execute
    ///    included — the reach `matches_pattern("*", _)` yields). Admin wins over anything else.
    /// 2. Otherwise the grant is **assembled from the granular scopes**: each
    ///    `<resource>.<verb>` scope the REST surface honours (`beads.write`, `issues.read`, …)
    ///    folds into a `<resource>.*` MCP allow pattern, so a token scoped to `issues` on REST
    ///    can call the `issues.*` tools on MCP instead of being denied wholesale. A
    ///    [`WORKSPACE_MEMBER`] role additionally unions the curated member preset
    ///    ([`workspace_member`](Self::workspace_member)).
    /// 3. No recognised scope at all → [`Scope::denied`].
    ///
    /// Granularity note: the grant is **per-resource namespace**. MCP has no read/write tool
    /// split (its `validate`/`execute` axis is dry-run vs commit, not read vs write), so a
    /// granular `issues.read` and `issues.write` both authorize the whole `issues.*` namespace
    /// here; the finer read/write gating stays a REST-surface refinement. `workspace.admin`/
    /// `workspace.member` are role labels, never namespace-expanded (a `workspace.member` token
    /// must not reach `workspace.create`). Unrecognised scope strings are ignored, exactly as the
    /// REST [`CallerScopes`](../../gt-module/src/routes.rs) bridge drops un-parseable scopes.
    pub fn from_workspace_claim(actor: &str, scopes: &[String]) -> Self {
        if scopes.iter().any(|s| s == "*" || s == WORKSPACE_ADMIN) {
            return Self::workspace_admin(actor);
        }
        let mut allow: BTreeSet<String> = BTreeSet::new();
        for s in scopes {
            if s == WORKSPACE_MEMBER {
                allow.extend(Self::workspace_member(actor).allow);
            } else if let Some((resource, verb)) = s.split_once('.') {
                // A granular `<resource>.<verb>` REST scope grants the matching MCP namespace.
                // `workspace.admin` is handled above; `workspace.member` above; any remaining
                // `workspace.*` (e.g. `workspace.write`) maps to its namespace like the rest.
                if crate::vocabulary::is_known_verb(verb) && !resource.is_empty() {
                    allow.insert(format!("{resource}.*"));
                }
            }
        }
        if allow.is_empty() {
            Self::denied(actor)
        } else {
            Self {
                actor: actor.to_string(),
                allow,
                validate_only: false,
            }
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

    #[test]
    fn workspace_member_allows_ops_but_not_workspace_create() {
        let s = Scope::workspace_member("member");
        s.check("issues.close.execute").unwrap();
        s.check("rig.add.execute").unwrap();
        s.check("workspace.info").unwrap();
        s.check("workspace.list").unwrap();
        // Provisioning a tenant is an admin act, outside the member grant.
        assert!(s.check("workspace.create").is_err());
    }

    #[test]
    fn workspace_admin_allows_everything_including_create() {
        let s = Scope::workspace_admin("admin");
        s.check("workspace.create").unwrap();
        s.check("issues.close.execute").unwrap();
    }

    #[test]
    fn from_workspace_claim_maps_scopes_deny_by_default() {
        let admin = Scope::from_workspace_claim("a", &[WORKSPACE_ADMIN.to_string()]);
        admin.check("workspace.create").unwrap();

        let member = Scope::from_workspace_claim("m", &[WORKSPACE_MEMBER.to_string()]);
        member.check("rig.add.execute").unwrap();
        assert!(member.check("workspace.create").is_err());

        // Admin wins when both are present.
        let both = Scope::from_workspace_claim(
            "b",
            &[WORKSPACE_MEMBER.to_string(), WORKSPACE_ADMIN.to_string()],
        );
        both.check("workspace.create").unwrap();

        // No recognised workspace scope -> closed.
        let none = Scope::from_workspace_claim("n", &["some.other.scope".to_string()]);
        assert!(none.check("issues.read").is_err());

        // A bare "*" grant (the seeded super-admin) is full admin — execute included.
        let star = Scope::from_workspace_claim("s", &["*".to_string()]);
        star.check("issues.create.execute").unwrap();
        star.check("workspace.create").unwrap();
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

    /// hq-docs-test-api.4 — a read-only documents actor (allowed only the read verbs) may
    /// list/search but is denied `documents.attach.execute`; the bundled `readonly` and
    /// `closed` profiles block every documents execute.
    #[test]
    fn documents_scope_gates_execute() {
        // Allow-list read-only: the list/search verbs only, no attach/update/remove.
        let reader = Scope {
            actor: "doc-reader".into(),
            allow: allow(&["documents.list.*", "documents.search.*"]),
            validate_only: false,
        };
        reader.check("documents.list.execute").unwrap();
        reader.check("documents.search.execute").unwrap();
        assert!(
            reader.check("documents.attach.execute").is_err(),
            "a read-only documents actor cannot attach"
        );
        assert!(reader.check("documents.remove.execute").is_err(), "nor remove");

        // The `readonly` profile (validate_only) blocks every execute, including documents.
        let ro = RbacConfig::from_profile("readonly").unwrap().expect("readonly profile");
        let ro_scope = ro.resolve("mcp-local");
        ro_scope.check("documents.attach.validate").unwrap();
        assert!(
            ro_scope.check("documents.attach.execute").is_err(),
            "readonly profile is validate_only — execute denied"
        );

        // The `closed` profile grants nothing, so even validate is denied.
        let closed = RbacConfig::from_profile("closed").unwrap().expect("closed profile");
        let closed_scope = closed.resolve("mcp-local");
        assert!(closed_scope.check("documents.attach.validate").is_err(), "closed denies all");
        assert!(closed_scope.check("documents.list.execute").is_err(), "closed denies reads too");

        // The `dev` profile (full local access) permits the documents execute path.
        let dev = RbacConfig::from_profile("dev").unwrap().expect("dev profile");
        dev.resolve("mcp-local").check("documents.attach.execute").unwrap();
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
