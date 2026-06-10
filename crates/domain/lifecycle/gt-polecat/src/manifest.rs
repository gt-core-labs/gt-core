//! Agent manifest computed at sling time (`hq-agent-observability.1`).
//!
//! The daemon already knows a polecat's worktree + its installed hook settings when it slings it
//! (see `gt_composition::polecat::PolecatSupervisorPlugin`). This module turns that into the two
//! *static* facts a frontend wants to show about the agent operating a bead — **which skills** it
//! has available and **which hooks** are loaded — with zero cooperation from the spawned `claude`
//! (no transcript parse, no extra hook). Both functions are pure reads, unit-tested here; the
//! sling edge calls them just before emitting the session/operator events (`.2`).

use std::path::Path;

use crate::install::polecat_settings_json;

/// The skills available to a polecat in `worktree` — the directory names under
/// `<worktree>/.claude/skills/` that carry a `SKILL.md` (the Agent-Skills discovery convention).
///
/// Returns a sorted, de-duplicated list so the manifest is deterministic regardless of filesystem
/// iteration order. A missing or unreadable skills directory is **not** an error — a polecat with
/// no project skills simply reports an empty list.
pub fn skills_from_worktree(worktree: &Path) -> Vec<String> {
    let skills_dir = worktree.join(".claude").join("skills");
    let mut names: Vec<String> = match std::fs::read_dir(&skills_dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|e| e.path().join("SKILL.md").is_file())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    };
    names.sort();
    names.dedup();
    names
}

/// The hook event kinds loaded into every polecat — the keys under `"hooks"` in the static
/// [`polecat_settings_json`] template (`SessionStart` / `UserPromptSubmit` / `PostToolUse` /
/// `Stop`). Derived from the template itself, never hard-coded, so a change to the installed hooks
/// is reflected in the manifest without a second edit.
///
/// Sorted for a deterministic manifest. The template is a compile-time constant document, so a
/// parse failure here is impossible in practice; it degrades to an empty list rather than panicking.
pub fn hooks_from_settings() -> Vec<String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&polecat_settings_json()) else {
        return Vec::new();
    };
    let mut kinds: Vec<String> = v
        .get("hooks")
        .and_then(|h| h.as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    kinds.sort();
    kinds
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, "x").unwrap();
    }

    #[test]
    fn skills_lists_named_dirs_with_a_skill_md_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".claude").join("skills");
        touch(&skills.join("graphify").join("SKILL.md"));
        touch(&skills.join("caveman").join("SKILL.md"));
        // A directory without SKILL.md is not a skill.
        std::fs::create_dir_all(skills.join("notaskill")).unwrap();
        // A stray file at the skills root is not a skill either.
        touch(&skills.join("loose.md"));

        assert_eq!(
            skills_from_worktree(dir.path()),
            vec!["caveman".to_string(), "graphify".to_string()]
        );
    }

    #[test]
    fn skills_is_empty_when_no_skills_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(skills_from_worktree(dir.path()).is_empty());
    }

    #[test]
    fn hooks_derive_from_the_template_kinds() {
        let hooks = hooks_from_settings();
        assert_eq!(
            hooks,
            vec![
                "PostToolUse".to_string(),
                "PreToolUse".to_string(),
                "SessionStart".to_string(),
                "Stop".to_string(),
                "UserPromptSubmit".to_string(),
            ],
            "the hook kinds the polecat settings template installs, sorted \
             (PreToolUse = memory guard hq-memory-mcp.6)"
        );
    }
}
