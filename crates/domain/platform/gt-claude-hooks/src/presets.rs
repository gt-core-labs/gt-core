//! Seed hooks for a fresh registry (`hq-hooks`): the portable safety guards.
//!
//! These are the only gastown hooks that port to gt-core verbatim — pure shell, zero `gt`
//! dependency (the rest call gastown-specific `gt`/`bd` subcommands). They block the most
//! destructive Bash invocations on *every* session (a default, all-empty target). The composition
//! root seeds them into the global `hooks.*` log once, when the registry is empty.

use crate::events::HookEvent;
use crate::state::HookTarget;

/// The portable dangerous-command guards, as `hooks.registered.v1` events to seed an empty
/// registry. Each blocks its matcher with a non-zero exit (claude treats a `PreToolUse` exit 2 as
/// "deny the tool call") and an explanatory message. Target is all-empty ⇒ applies everywhere.
pub fn safety_guard_hooks(now_secs: u64) -> Vec<HookEvent> {
    let guard = |id: &str, matcher: &str, msg: &str| HookEvent::Registered {
        id: id.into(),
        event: "PreToolUse".into(),
        matcher: matcher.into(),
        command: format!("echo '❌ BLOCKED: {msg}' && exit 2"),
        target: HookTarget::default(),
        now_secs,
    };
    vec![
        guard("guard-rm-rf-root", "Bash(rm -rf /*)", "rm -rf on an absolute path"),
        guard("guard-git-push-force", "Bash(git push --force*)", "forced push"),
        guard("guard-git-push-f", "Bash(git push -f*)", "forced push (-f)"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::HooksState;

    #[test]
    fn seeds_three_global_pretooluse_guards() {
        let mut s = HooksState::default();
        for e in safety_guard_hooks(0) {
            s.apply(&e);
        }
        assert_eq!(s.registry.len(), 3);
        // Every session (any workspace/rig/role) sees all three.
        let v = s.registry.settings_json_for("any", "any", "any").unwrap();
        assert_eq!(v["hooks"]["PreToolUse"].as_array().unwrap().len(), 3);
    }
}
