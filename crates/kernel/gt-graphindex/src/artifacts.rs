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
}
