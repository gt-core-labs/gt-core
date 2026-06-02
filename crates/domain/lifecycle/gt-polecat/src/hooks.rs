//! `GT_HOOK_BEAD` — the bead pinned into a polecat's environment at spawn.
//!
//! Port of the Go hook-pin path (`internal/polecat/session_manager.go` +
//! `internal/cmd/hook_env.go`). When a bead is slung to a polecat, its id is written into the
//! polecat session env as `GT_HOOK_BEAD` so `gt prime`/status can identify the hooked work
//! **without** a `bd` query. That bypass exists because upstream bd's embedded-scratch
//! auto-import can flip a bead's status (`hooked → open`) between the sling write and the
//! polecat's first read (memory gg-0nb). The env pin is authoritative even if a later `bd`
//! read disagrees, because it was set by the trusted sling path.
//!
//! **The bug being fixed (gg-0nb):** the Go sling deferred-spawn path shipped the env var
//! (340f499a) but dropped the bead on the deferred branch, so the polecat came up without a
//! pin. [`hook_bead_for_spawn`] is the single chokepoint that must never drop a present bead:
//! prefer the explicit hook bead, fall back to the direct issue, and only return `None` when
//! both are genuinely absent.

/// Session env variable carrying the pinned bead id.
pub const GT_HOOK_BEAD: &str = "GT_HOOK_BEAD";

/// Pick the bead to pin for a spawn. Mirrors Go `session_manager`: prefer an explicit
/// `hook_bead` (sling already attached the hook), else the direct `issue`. Blank strings are
/// treated as absent. Returning the bead whenever *either* input is present is the fix for the
/// dropped-issue bug — there is no path here that silently discards a supplied bead.
pub fn hook_bead_for_spawn(hook_bead: Option<&str>, issue: Option<&str>) -> Option<String> {
    fn non_blank(s: Option<&str>) -> Option<&str> {
        s.map(str::trim).filter(|s| !s.is_empty())
    }
    non_blank(hook_bead)
        .or_else(|| non_blank(issue))
        .map(str::to_string)
}

/// Build the `(GT_HOOK_BEAD, <bead>)` env entry to inject, or `None` when there is no bead.
pub fn hook_env(hook_bead: Option<&str>, issue: Option<&str>) -> Option<(String, String)> {
    hook_bead_for_spawn(hook_bead, issue).map(|bead| (GT_HOOK_BEAD.to_string(), bead))
}

/// Read the pinned bead id back out of an environment, via a caller-supplied getter (the
/// process env at the edge, or a fake in tests). Mirrors Go `resolveEnvHookBead` minus the
/// `bd` lookup: callers treat a non-empty result as the authoritative active hook regardless
/// of what a later status read claims.
pub fn resolve_env_hook_bead<F>(getenv: F) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    getenv(GT_HOOK_BEAD)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_hook_bead_over_issue() {
        assert_eq!(
            hook_bead_for_spawn(Some("hq-1"), Some("hq-2")).as_deref(),
            Some("hq-1")
        );
    }

    #[test]
    fn falls_back_to_issue() {
        assert_eq!(
            hook_bead_for_spawn(None, Some("hq-2")).as_deref(),
            Some("hq-2")
        );
    }

    #[test]
    fn blank_is_absent_never_dropped_silently() {
        // The dropped-issue bug: a blank hook bead must not mask a real issue.
        assert_eq!(
            hook_bead_for_spawn(Some("   "), Some("hq-2")).as_deref(),
            Some("hq-2")
        );
        assert!(hook_bead_for_spawn(Some(""), None).is_none());
        assert!(hook_bead_for_spawn(None, None).is_none());
    }

    #[test]
    fn env_entry_keyed_correctly() {
        assert_eq!(
            hook_env(Some("hq-9"), None),
            Some((GT_HOOK_BEAD.to_string(), "hq-9".to_string()))
        );
        assert_eq!(hook_env(None, None), None);
    }

    #[test]
    fn resolve_roundtrips() {
        let env = |k: &str| (k == GT_HOOK_BEAD).then(|| "hq-42".to_string());
        assert_eq!(resolve_env_hook_bead(env).as_deref(), Some("hq-42"));
        let empty = |_: &str| None;
        assert!(resolve_env_hook_bead(empty).is_none());
    }
}
