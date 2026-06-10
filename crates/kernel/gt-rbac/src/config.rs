//! Unified RBAC configuration shared by the MCP tool dispatch (per-actor allow-list)
//! and gt-web (per-actor JWT roles + scopes). Both live in the same file so a deploy
//! can't drift between "who can call which MCP tool" and "what roles ride in this
//! actor's JWT".
//!
//! Schema (TOML; JSON is structurally identical):
//!
//! ```toml
//! [actors.claude-host]
//! allow = ["*"]
//! validate_only = false
//! roles = ["sheriff"]
//!
//! [actors.watcher]
//! allow = ["agent.*", "scheduling.enqueue.validate"]
//! validate_only = true
//! roles = []
//!
//! [roles.sheriff]
//! scopes = ["beads.write", "merge.read", "merge.submit"]
//! ```
//!
//! Deny-by-default: an actor absent from the table resolves to a closed grant (no
//! MCP tools, no roles, no scopes). There is **no admin default**.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::scope::matches_pattern;
use crate::vocabulary::SCOPE_WILDCARD;

/// One actor's entry. `allow` and `validate_only` mirror the original
/// `mcp-scope.toml` shape; `roles` is consumed by gt-web.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ActorSpec {
    /// MCP tool patterns the actor may call (exact name or trailing-`*` glob). An empty
    /// set denies every MCP call.
    #[serde(default)]
    pub allow: BTreeSet<String>,
    /// If true, the actor may only call `*.validate` MCP tools — execute paths are
    /// rejected.
    #[serde(default)]
    pub validate_only: bool,
    /// Role names looked up in [`RbacConfig::roles`] when the actor signs in via gt-web.
    /// `Vec` (not `BTreeSet`) so config order is the order stamped into the JWT body.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Per-role capability bundle. Multiple roles per actor union their `scopes`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RoleSpec {
    /// HTTP/FE scope identifiers granted by this role (dotted `<domain>.<action>`).
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// Root config. Both subtables default to empty so a file with neither still parses.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RbacConfig {
    #[serde(default)]
    pub actors: BTreeMap<String, ActorSpec>,
    #[serde(default)]
    pub roles: BTreeMap<String, RoleSpec>,
}

/// gt-web's view of an actor's grant: the JWT body never carries the MCP allow-list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebGrant {
    /// Role names in config order. Stamped into the JWT body verbatim.
    pub roles: Vec<String>,
    /// Union of every role's `scopes`, dedup'd while preserving first-seen order.
    pub scopes: Vec<String>,
}

impl RbacConfig {
    pub fn from_toml(s: &str) -> Result<Self, AppError> {
        toml::from_str(s).map_err(|e| AppError::Validation(format!("rbac config (toml): {e}")))
    }

    pub fn from_json(s: &str) -> Result<Self, AppError> {
        serde_json::from_str(s)
            .map_err(|e| AppError::Validation(format!("rbac config (json): {e}")))
    }

    /// Load a built-in scope profile by name (`GT_MCP_SCOPE_PROFILE`). Profiles are TOML
    /// files embedded in the binary, so a deploy selects a policy with one env var instead
    /// of shipping a config file. `Ok(None)` for an unknown name (the caller reports the
    /// valid set). An operator who needs a bespoke policy still overrides with a file via
    /// `GT_MCP_SCOPE_CONFIG`, which takes precedence.
    ///
    /// Bundled: `dev` (full access for the default local actor), `readonly` (default actor
    /// may call only `*.validate`), `closed` (deny-by-default — bring your own file).
    pub fn from_profile(name: &str) -> Result<Option<Self>, AppError> {
        let raw = match name {
            "dev" => include_str!("../profiles/dev.toml"),
            "readonly" => include_str!("../profiles/readonly.toml"),
            "closed" => include_str!("../profiles/closed.toml"),
            _ => return Ok(None),
        };
        Self::from_toml(raw).map(Some)
    }

    /// The names [`from_profile`](Self::from_profile) accepts, for error messages.
    pub fn available_profiles() -> &'static [&'static str] {
        &["dev", "readonly", "closed"]
    }

    /// Load from disk; the parser is selected by extension (`.json` → JSON, otherwise
    /// TOML). I/O errors surface as [`AppError::Other`] so the boot path can log a
    /// useful path string before failing closed.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .map_err(|e| AppError::Other(format!("read rbac config {}: {e}", path.display())))?;
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => Self::from_json(&raw),
            _ => Self::from_toml(&raw),
        }
    }

    /// Look up the raw grant for `actor`. Returns `None` for unknown actors — the
    /// caller decides whether to deny or fall back.
    pub fn actor(&self, actor: &str) -> Option<&ActorSpec> {
        self.actors.get(actor)
    }

    /// Resolve `actor` to a [`WebGrant`]. Unknown actor → empty grant; missing role
    /// name → silently skipped.
    pub fn web_grant(&self, actor: &str) -> WebGrant {
        let Some(spec) = self.actors.get(actor) else {
            return WebGrant::default();
        };
        let roles = spec.roles.clone();
        let mut scopes: Vec<String> = Vec::new();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for role_name in &spec.roles {
            if let Some(role) = self.roles.get(role_name) {
                for scope in &role.scopes {
                    if seen.insert(scope.as_str()) {
                        scopes.push(scope.clone());
                    }
                }
            }
        }
        WebGrant { roles, scopes }
    }

    /// True when at least one actor declares a non-`*` allow pattern — i.e. a real
    /// least-privilege policy is in force, not a pure dev config where one actor holds
    /// `*`. The reachability audit ([`reach`](Self::reach)) is only meaningful then: a
    /// `*`-only config intends blanket access, so flagging every namespace as "superuser
    /// only" would be pure noise.
    pub fn has_least_privilege_actor(&self) -> bool {
        self.actors
            .values()
            .any(|s| s.allow.iter().any(|p| p != SCOPE_WILDCARD))
    }

    /// Classify how `tool` is reachable under the per-actor MCP allow-lists
    /// (hq-rbac-reachability). The same `matches_pattern` the live dispatch gate
    /// ([`Scope::check`]) uses, but asked across every actor at once — so the boot can tell
    /// a tool no least-privilege grant references (the failure that shipped `memory.*` live
    /// yet denied to every non-`*` actor) from one a real grant covers.
    ///
    /// A bare `*` actor matches everything, but that is *superuser* reach, not least-privilege:
    /// it is tracked separately so [`Reach::SuperuserOnly`] surfaces "callable, but only by a
    /// god-mode actor". `validate_only` is not modelled — this audits the allow *surface*, not
    /// the execute/validate split.
    pub fn reach(&self, tool: &str) -> Reach {
        let mut superuser = false;
        for spec in self.actors.values() {
            for pat in &spec.allow {
                if pat == SCOPE_WILDCARD {
                    superuser = true;
                    continue;
                }
                if matches_pattern(pat, tool) {
                    return Reach::Explicit;
                }
            }
        }
        if superuser {
            Reach::SuperuserOnly
        } else {
            Reach::Unreachable
        }
    }

    /// The subset of `advertised` tools no least-privilege actor can reach (i.e. [`reach`] is
    /// not [`Reach::Explicit`]) — the boot self-check and CI coverage test both fold this into
    /// a "namespace registered but ungranted" alarm. Input order is preserved.
    ///
    /// [`reach`]: Self::reach
    pub fn least_privilege_orphans<'a, I>(&self, advertised: I) -> Vec<&'a str>
    where
        I: IntoIterator<Item = &'a str>,
    {
        advertised
            .into_iter()
            .filter(|t| self.reach(t) != Reach::Explicit)
            .collect()
    }
}

/// How an MCP tool is reachable under an [`RbacConfig`]'s per-actor allow-lists
/// (hq-rbac-reachability). The boot self-check treats anything other than [`Explicit`](Self::Explicit)
/// as an orphan: a registered tool no least-privilege client can actually call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// A non-`*` actor grant matches the tool — a least-privilege client can call it.
    Explicit,
    /// Only a bare `*` superuser actor matches — no least-privilege grant references it.
    SuperuserOnly,
    /// No actor grant matches at all — the tool is callable by nobody.
    Unreachable,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
[actors.claude-host]
allow = ["*"]
roles = ["sheriff"]

[actors.watcher]
allow = ["agent.*"]
validate_only = true
roles = ["reader"]

[actors.legacy]
allow = ["scheduling.*"]

[roles.sheriff]
scopes = ["beads.write", "merge.read", "merge.submit"]

[roles.reader]
scopes = ["beads.read", "merge.read"]
"#;

    #[test]
    fn builtin_profiles_load_and_unknown_is_none() {
        let dev = RbacConfig::from_profile("dev").unwrap().expect("dev profile");
        assert!(dev.actor("mcp-local").unwrap().allow.contains("*"));
        let ro = RbacConfig::from_profile("readonly").unwrap().expect("readonly profile");
        assert!(ro.actor("mcp-local").unwrap().validate_only, "readonly is validate-only");
        let closed = RbacConfig::from_profile("closed").unwrap().expect("closed profile");
        assert!(closed.actors.is_empty(), "closed grants nothing");
        assert!(RbacConfig::from_profile("bogus").unwrap().is_none(), "unknown ⇒ None");
        for p in RbacConfig::available_profiles() {
            assert!(RbacConfig::from_profile(p).unwrap().is_some(), "advertised profile {p} loads");
        }
    }

    #[test]
    fn parses_actors_and_roles() {
        let cfg = RbacConfig::from_toml(FULL).unwrap();
        assert!(cfg.actors.contains_key("claude-host"));
        assert!(cfg.roles.contains_key("sheriff"));
        let sheriff = cfg.actor("claude-host").unwrap();
        assert_eq!(sheriff.roles, vec!["sheriff".to_string()]);
        assert!(sheriff.allow.contains("*"));
    }

    #[test]
    fn web_grant_flattens_role_scopes_in_order() {
        let cfg = RbacConfig::from_toml(FULL).unwrap();
        let grant = cfg.web_grant("claude-host");
        assert_eq!(grant.roles, vec!["sheriff".to_string()]);
        assert_eq!(
            grant.scopes,
            vec![
                "beads.write".to_string(),
                "merge.read".to_string(),
                "merge.submit".to_string()
            ]
        );
    }

    #[test]
    fn web_grant_dedupes_across_multiple_roles() {
        let cfg = RbacConfig::from_toml(
            r#"
[actors.dual]
allow = []
roles = ["sheriff", "reader"]

[roles.sheriff]
scopes = ["beads.write", "merge.read"]

[roles.reader]
scopes = ["beads.read", "merge.read"]
"#,
        )
        .unwrap();
        let grant = cfg.web_grant("dual");
        assert_eq!(
            grant.scopes,
            vec![
                "beads.write".to_string(),
                "merge.read".to_string(),
                "beads.read".to_string(),
            ]
        );
        assert_eq!(grant.roles.len(), 2);
    }

    #[test]
    fn unknown_actor_resolves_to_empty_web_grant() {
        let cfg = RbacConfig::from_toml(FULL).unwrap();
        let grant = cfg.web_grant("ghost");
        assert!(grant.roles.is_empty());
        assert!(grant.scopes.is_empty());
    }

    #[test]
    fn legacy_actor_without_roles_field_resolves_to_empty_grant() {
        let cfg = RbacConfig::from_toml(FULL).unwrap();
        let grant = cfg.web_grant("legacy");
        assert!(grant.roles.is_empty());
        assert!(grant.scopes.is_empty());
        let spec = cfg.actor("legacy").unwrap();
        assert!(spec.allow.contains("scheduling.*"));
    }

    const JSON_CFG: &str = r#"
{
  "actors": {
    "claude-host": { "allow": ["*"], "roles": ["sheriff"] }
  },
  "roles": {
    "sheriff": { "scopes": ["beads.write"] }
  }
}
"#;

    #[test]
    fn json_and_toml_parse_to_identical_structure() {
        let toml_cfg = RbacConfig::from_toml(
            r#"
[actors.claude-host]
allow = ["*"]
roles = ["sheriff"]

[roles.sheriff]
scopes = ["beads.write"]
"#,
        )
        .unwrap();
        let json_cfg = RbacConfig::from_json(JSON_CFG).unwrap();
        assert_eq!(
            toml_cfg.web_grant("claude-host"),
            json_cfg.web_grant("claude-host")
        );
    }

    // ----- reachability audit (hq-rbac-reachability) --------------------------------------

    /// The prod shape that shipped the bug: a least-privilege `admin` reaches issues/merge but
    /// NOT memory, plus a `*` superuser. `memory.*` is therefore reachable only by the superuser.
    const PROD_LIKE: &str = r#"
[actors.admin]
allow = ["issues.*", "merge.*"]

[actors.mcp-local]
allow = ["*"]
"#;

    #[test]
    fn reach_distinguishes_explicit_superuser_and_unreachable() {
        let cfg = RbacConfig::from_toml(PROD_LIKE).unwrap();
        // A least-privilege grant covers it (admin.allow `issues.*` matches the 3-seg tool).
        assert_eq!(cfg.reach("issues.list.execute"), Reach::Explicit);
        // No non-`*` actor grants memory — only the `*` superuser reaches it. THE BUG.
        assert_eq!(cfg.reach("memory.recall"), Reach::SuperuserOnly);
        assert_eq!(cfg.reach("memory.save"), Reach::SuperuserOnly);
    }

    #[test]
    fn unreachable_when_no_actor_and_no_wildcard() {
        // admin alone (no `*` actor): a namespace it does not grant is reachable by NOBODY.
        let cfg = RbacConfig::from_toml("[actors.admin]\nallow = [\"issues.*\"]\n").unwrap();
        assert_eq!(cfg.reach("memory.recall"), Reach::Unreachable);
        assert_eq!(cfg.reach("issues.list.execute"), Reach::Explicit);
    }

    #[test]
    fn least_privilege_orphans_flags_only_the_ungranted() {
        let cfg = RbacConfig::from_toml(PROD_LIKE).unwrap();
        let advertised = ["issues.list.execute", "merge.start", "memory.recall", "memory.save"];
        let orphans = cfg.least_privilege_orphans(advertised);
        // issues/merge are explicitly granted; both memory tools are orphans.
        assert_eq!(orphans, vec!["memory.recall", "memory.save"]);
    }

    #[test]
    fn granting_the_namespace_clears_the_orphan() {
        // Add `memory.*` to admin — the deploy-time fix the contract forces.
        let cfg = RbacConfig::from_toml(
            "[actors.admin]\nallow = [\"issues.*\", \"memory.*\"]\n[actors.mcp-local]\nallow = [\"*\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.reach("memory.recall"), Reach::Explicit);
        let orphans = cfg.least_privilege_orphans(["memory.recall", "issues.list.execute"]);
        assert!(orphans.is_empty(), "a granted namespace is no longer an orphan");
    }

    #[test]
    fn has_least_privilege_actor_is_false_for_pure_wildcard_config() {
        // The `dev` profile is `*`-only: the audit must SKIP it (blanket access is intended).
        let dev = RbacConfig::from_profile("dev").unwrap().unwrap();
        assert!(!dev.has_least_privilege_actor(), "a `*`-only config is not least-privilege");
        // PROD_LIKE has admin with explicit globs → least-privilege policy in force.
        assert!(RbacConfig::from_toml(PROD_LIKE).unwrap().has_least_privilege_actor());
    }
}
