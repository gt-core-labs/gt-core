//! Domain state + replay reducer for the global hook registry.
//!
//! [`HooksRegistry`] is the mutable state; [`HooksState`] is the version rebuilt from the log for
//! the deterministic-replay gate. Both share the `apply_*` helpers so the live and rebuilt states
//! match byte-for-byte.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Upper bound on a hook id length. Keeps a wedged registry from minting an unreadable label.
pub const MAX_HOOK_ID_LEN: usize = 64;

/// The Claude Code hook event types a hook may bind to (the closed set claude understands). The
/// terminal materialises matching hooks into `.claude/settings.json` under these keys.
pub const EVENT_TYPES: [&str; 6] = [
    "PreToolUse",
    "PostToolUse",
    "SessionStart",
    "Stop",
    "PreCompact",
    "UserPromptSubmit",
];

/// Which sessions a hook applies to. Each dimension is a whitelist; an **empty** dimension means
/// "matches any" (so an all-empty target = a global hook on every session). A session's
/// `(workspace, rig, role)` tuple is matched against these at terminal launch.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HookTarget {
    /// Workspaces the hook applies to; empty ⇒ all workspaces.
    #[serde(default)]
    pub workspaces: Vec<String>,
    /// Rigs the hook applies to; empty ⇒ all rigs.
    #[serde(default)]
    pub rigs: Vec<String>,
    /// Roles the hook applies to (`crew`, `witness`, `polecat`, …); empty ⇒ all roles.
    #[serde(default)]
    pub roles: Vec<String>,
}

impl HookTarget {
    /// `true` when this target applies to a session with the given `(workspace, rig, role)`. An
    /// empty dimension matches anything; a non-empty one requires membership.
    pub fn matches(&self, workspace: &str, rig: &str, role: &str) -> bool {
        dim_matches(&self.workspaces, workspace)
            && dim_matches(&self.rigs, rig)
            && dim_matches(&self.roles, role)
    }
}

/// One target dimension: empty whitelist ⇒ matches anything; otherwise membership.
fn dim_matches(whitelist: &[String], value: &str) -> bool {
    whitelist.is_empty() || whitelist.iter().any(|v| v == value)
}

/// One registry entry: a Claude Code hook (event + matcher + command) plus its target selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HookDef {
    pub id: String,
    /// The hook event type (one of [`EVENT_TYPES`]).
    pub event: String,
    /// The tool matcher; empty ⇒ every invocation of `event`.
    pub matcher: String,
    /// The shell command claude runs.
    pub command: String,
    /// Which sessions the hook applies to.
    pub target: HookTarget,
    pub registered_at_secs: u64,
}

/// Live global hook registry. `BTreeMap` keyed on id so iteration is sorted and deterministic.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HooksRegistry {
    hooks: BTreeMap<String, HookDef>,
}

impl HooksRegistry {
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&HookDef> {
        self.hooks.get(id)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.hooks.contains_key(id)
    }

    /// Owned snapshot of every registered hook, in sorted (by id) order.
    pub fn hooks(&self) -> Vec<HookDef> {
        self.hooks.values().cloned().collect()
    }

    /// Build the Claude Code `settings.json` value for a session, containing exactly the hooks whose
    /// target matches `(workspace, rig, role)`. Returns `None` when no hook matches (the caller then
    /// writes no settings file). The shape mirrors what claude reads:
    ///
    /// ```json
    /// { "hooks": { "PreToolUse": [ { "matcher": "Bash(rm -rf /*)",
    ///                                "hooks": [ { "type": "command", "command": "…" } ] } ] } }
    /// ```
    ///
    /// Hooks sharing an `(event, matcher)` are merged into one entry (their commands listed in id
    /// order); events iterate in [`EVENT_TYPES`] order and matchers in sorted order, so the file is
    /// byte-stable across replay.
    pub fn settings_json_for(&self, workspace: &str, rig: &str, role: &str) -> Option<Value> {
        // event -> matcher -> [command] (BTreeMap for deterministic matcher ordering).
        let mut grouped: BTreeMap<&str, BTreeMap<&str, Vec<&str>>> = BTreeMap::new();
        for def in self.hooks.values() {
            if !def.target.matches(workspace, rig, role) {
                continue;
            }
            grouped
                .entry(def.event.as_str())
                .or_default()
                .entry(def.matcher.as_str())
                .or_default()
                .push(def.command.as_str());
        }
        if grouped.is_empty() {
            return None;
        }
        let mut hooks_obj = serde_json::Map::new();
        // Iterate events in the canonical claude order for a stable file.
        for event in EVENT_TYPES {
            let Some(matchers) = grouped.get(event) else { continue };
            let entries: Vec<Value> = matchers
                .iter()
                .map(|(matcher, commands)| {
                    let cmds: Vec<Value> = commands
                        .iter()
                        .map(|c| json!({ "type": "command", "command": c }))
                        .collect();
                    json!({ "matcher": matcher, "hooks": cmds })
                })
                .collect();
            hooks_obj.insert(event.to_string(), Value::Array(entries));
        }
        Some(json!({ "hooks": Value::Object(hooks_obj) }))
    }

    // -- mutation helpers (shared by `commands::execute` and `HooksState::apply` so live + rebuilt
    //    state stay in lockstep).

    pub(crate) fn apply_register(&mut self, def: HookDef) {
        self.hooks.insert(def.id.clone(), def);
    }

    pub(crate) fn apply_retire(&mut self, id: &str) {
        self.hooks.remove(id);
    }
}

// ----------------------------------------------------------------------------
// Pure replay reducer.

/// Same data as [`HooksRegistry`] but rebuilt by replaying [`crate::HookEvent`]s.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HooksState {
    pub registry: HooksRegistry,
}

impl HooksState {
    pub fn apply(&mut self, event: &crate::events::HookEvent) {
        use crate::events::HookEvent;
        match event {
            HookEvent::Registered { id, event, matcher, command, target, now_secs } => {
                self.registry.apply_register(HookDef {
                    id: id.clone(),
                    event: event.clone(),
                    matcher: matcher.clone(),
                    command: command.clone(),
                    target: target.clone(),
                    registered_at_secs: *now_secs,
                });
            }
            HookEvent::Retired { id, .. } => {
                self.registry.apply_retire(id);
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Structural validators.

/// Validate a hook id. Conservative: ASCII alphanumeric + `-` + `_`, non-empty, no surrounding
/// whitespace, bounded length. Surfaces in the UI + SSE payloads — keep it parseable.
pub fn validate_hook_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("hook id is empty".into());
    }
    if id.len() > MAX_HOOK_ID_LEN {
        return Err(format!("hook id is longer than {MAX_HOOK_ID_LEN} bytes"));
    }
    if id.trim() != id {
        return Err("hook id has leading or trailing whitespace".into());
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!(
            "hook id {id:?} has invalid characters (allowed: ASCII alnum, '-', '_')"
        ));
    }
    Ok(())
}

/// Validate a hook event type against the closed [`EVENT_TYPES`] set.
pub fn validate_event(event: &str) -> Result<(), String> {
    if EVENT_TYPES.contains(&event) {
        Ok(())
    } else {
        Err(format!("event {event:?} is not one of {EVENT_TYPES:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::HookEvent;

    fn reg(id: &str, event: &str, matcher: &str, cmd: &str, target: HookTarget) -> HookEvent {
        HookEvent::Registered {
            id: id.into(),
            event: event.into(),
            matcher: matcher.into(),
            command: cmd.into(),
            target,
            now_secs: 1,
        }
    }

    #[test]
    fn register_and_retire_round_trip() {
        let mut s = HooksState::default();
        s.apply(&reg("guard-rm", "PreToolUse", "Bash(rm -rf /*)", "exit 2", HookTarget::default()));
        assert_eq!(s.registry.len(), 1);
        assert!(s.registry.contains("guard-rm"));
        s.apply(&HookEvent::Retired { id: "guard-rm".into(), now_secs: 2 });
        assert!(!s.registry.contains("guard-rm"));
    }

    #[test]
    fn target_matches_empty_dimensions_are_wildcards() {
        let t = HookTarget::default();
        assert!(t.matches("acme", "hq", "polecat"));
        let scoped = HookTarget {
            workspaces: vec!["acme".into()],
            rigs: vec![],
            roles: vec!["witness".into()],
        };
        assert!(scoped.matches("acme", "any-rig", "witness"));
        assert!(!scoped.matches("other", "any-rig", "witness")); // workspace excluded
        assert!(!scoped.matches("acme", "any-rig", "polecat")); // role excluded
    }

    #[test]
    fn settings_json_filters_by_target_and_merges_by_event_matcher() {
        let mut s = HooksState::default();
        // Global guard (applies everywhere).
        s.apply(&reg("guard-rm", "PreToolUse", "Bash(rm -rf /*)", "exit 2", HookTarget::default()));
        // A second command on the SAME event+matcher → merged into one entry.
        s.apply(&reg("guard-rm2", "PreToolUse", "Bash(rm -rf /*)", "log it", HookTarget::default()));
        // Witness-only hook on a different event.
        s.apply(&reg(
            "wit-stop",
            "Stop",
            "",
            "gt done",
            HookTarget { workspaces: vec![], rigs: vec![], roles: vec!["witness".into()] },
        ));

        // A polecat session sees only the two global PreToolUse commands (merged).
        let v = s.registry.settings_json_for("acme", "hq", "polecat").unwrap();
        let pre = &v["hooks"]["PreToolUse"];
        assert_eq!(pre.as_array().unwrap().len(), 1, "one entry per (event,matcher)");
        assert_eq!(pre[0]["matcher"], "Bash(rm -rf /*)");
        assert_eq!(pre[0]["hooks"].as_array().unwrap().len(), 2, "both commands merged");
        assert_eq!(pre[0]["hooks"][0]["type"], "command");
        assert!(v["hooks"].get("Stop").is_none(), "witness-only hook excluded for polecat");

        // A witness session also sees the Stop hook.
        let w = s.registry.settings_json_for("acme", "hq", "witness").unwrap();
        assert_eq!(w["hooks"]["Stop"][0]["hooks"][0]["command"], "gt done");

        // A session matched by no hook → None.
        let mut empty = HooksState::default();
        empty.apply(&reg(
            "only-x",
            "Stop",
            "",
            "x",
            HookTarget { workspaces: vec!["x".into()], rigs: vec![], roles: vec![] },
        ));
        assert!(empty.registry.settings_json_for("acme", "hq", "polecat").is_none());
    }

    #[test]
    fn replay_is_deterministic() {
        let events = vec![
            reg("b", "Stop", "", "two", HookTarget::default()),
            reg("a", "PreToolUse", "Bash(x)", "one", HookTarget::default()),
            HookEvent::Retired { id: "b".into(), now_secs: 9 },
        ];
        let mut a = HooksState::default();
        let mut b = HooksState::default();
        for e in &events {
            a.apply(e);
        }
        for e in &events {
            b.apply(e);
        }
        assert_eq!(a, b);
        assert_eq!(a.registry.len(), 1);
    }

    #[test]
    fn validators_reject_bad_id_and_event() {
        assert!(validate_hook_id("guard-rm").is_ok());
        assert!(validate_hook_id("").is_err());
        assert!(validate_hook_id(" x").is_err());
        assert!(validate_hook_id("dot.notation").is_err());
        assert!(validate_event("PreToolUse").is_ok());
        assert!(validate_event("Bogus").is_err());
    }
}
