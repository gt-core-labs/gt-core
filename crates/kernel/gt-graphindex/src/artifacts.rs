//! Canonical VCS-ignore patterns for graph artifacts.
//!
//! The graph a tool writes under a repo must never be tracked. This module is the
//! **single source** of the ignore patterns so two call sites agree: a repo's own
//! `.gitignore` and the per-rig propagation that the deploy edge applies to every
//! attached repo (`hq-graphrig.11`).
//!
//! Patterns are **tool-driven**: [`patterns_for`] returns a neutral umbrella every
//! tool shares plus the concrete artifacts of the named tool. Swapping the graph
//! tool therefore swaps the propagated ignore set automatically — nothing else to
//! edit. [`GraphIndexer::ignore_patterns`](crate::GraphIndexer::ignore_patterns)
//! is the convenience that reads the active tool's name and calls through here.

/// Tool-neutral umbrella directory. New or alternative tools are encouraged to
/// write under `.graphindex/` so a single pattern covers them even before
/// [`patterns_for`] learns their name.
pub const NEUTRAL_UMBRELLA: &str = ".graphindex/";

/// The ignore patterns for `tool`: the [`NEUTRAL_UMBRELLA`] plus that tool's own
/// artifacts. An unknown tool gets just the umbrella, which is still correct as
/// long as the tool honors the `.graphindex/` convention.
pub fn patterns_for(tool: &str) -> Vec<String> {
    let mut out = vec![NEUTRAL_UMBRELLA.to_string()];
    if tool == "graphify" {
        out.extend(
            ["graphify-out/", ".graphify-venv/", ".graphify_*"]
                .into_iter()
                .map(str::to_string),
        );
    }
    out
}

/// Idempotently ensure `repo`'s git checkout ignores `tool`'s graph artifacts.
///
/// Writes the missing [`patterns_for`] lines into `<repo>/.git/info/exclude` — the
/// **local, uncommitted** ignore list — never the repo's tracked `.gitignore`. That is
/// deliberate: a rig may be a repo we do not own, so the per-rig propagation
/// (`hq-graphrig.11`) must not dirty its committed files. Re-running is a no-op once the
/// patterns are present.
///
/// Returns the patterns newly added (empty when everything was already excluded). If the
/// repo has no `.git` dir (not a checkout yet) the exclude file's parent is created so the
/// call still succeeds for a freshly-init'd repo.
pub fn ensure_ignored(repo: &std::path::Path, tool: &str) -> std::io::Result<Vec<String>> {
    let exclude = repo.join(".git").join("info").join("exclude");
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let existing = std::fs::read_to_string(&exclude).unwrap_or_default();
    let have: std::collections::HashSet<&str> =
        existing.lines().map(str::trim).collect();

    let missing: Vec<String> = patterns_for(tool)
        .into_iter()
        .filter(|p| !have.contains(p.as_str()))
        .collect();
    if missing.is_empty() {
        return Ok(missing);
    }

    let mut body = existing;
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    body.push_str("# graph artifacts (gt-graphindex, do not track)\n");
    for p in &missing {
        body.push_str(p);
        body.push('\n');
    }
    std::fs::write(&exclude, body)?;
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graphify_emits_its_artifacts_plus_umbrella() {
        let p = patterns_for("graphify");
        assert!(p.contains(&NEUTRAL_UMBRELLA.to_string()));
        assert!(p.contains(&"graphify-out/".to_string()));
        assert!(p.contains(&".graphify-venv/".to_string()));
        assert!(p.contains(&".graphify_*".to_string()));
    }

    #[test]
    fn unknown_tool_gets_umbrella_only() {
        assert_eq!(patterns_for("future-tool"), vec![NEUTRAL_UMBRELLA.to_string()]);
    }

    #[test]
    fn ensure_ignored_writes_then_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("gi-ignore-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(".git/info")).unwrap();

        let added = ensure_ignored(&dir, "graphify").unwrap();
        assert_eq!(added, patterns_for("graphify"));
        let body = std::fs::read_to_string(dir.join(".git/info/exclude")).unwrap();
        assert!(body.contains("graphify-out/"));
        assert!(body.contains(".graphindex/"));

        // Second run adds nothing.
        let again = ensure_ignored(&dir, "graphify").unwrap();
        assert!(again.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_ignored_preserves_existing_excludes() {
        let dir = std::env::temp_dir().join(format!("gi-ignore-keep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(".git/info")).unwrap();
        std::fs::write(dir.join(".git/info/exclude"), "*.log\n").unwrap();

        ensure_ignored(&dir, "graphify").unwrap();
        let body = std::fs::read_to_string(dir.join(".git/info/exclude")).unwrap();
        assert!(body.contains("*.log"), "pre-existing rule must survive");
        assert!(body.contains(".graphindex/"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
