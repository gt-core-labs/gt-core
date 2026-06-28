//! Shared role-session materialisation (gtcore-ec24d2).
//!
//! Every autonomous role launch needs the same thing on disk before `claude` starts: the role's
//! enabled **skills** (`SKILL.md` bodies) and its **Knowledge** prompt as `CLAUDE.md`, both pulled
//! from the operator-managed `skills.*` catalog, plus the role's **model** config stamped onto the
//! launch args. The polecat sling and the terminal/role apparatus each grew their own copy of this;
//! the mayor launch had none and so opened with the repo's generic `CLAUDE.md` and no role skills.
//!
//! This module is the SINGLE pattern all role launches follow: [`materialize_role_session`] writes
//! the skills + `CLAUDE.md` into a workdir and returns the role's [`ModelConfig`] for the caller to
//! stamp via [`crate::polecat::apply_role_model`]. Best-effort throughout — a per-file IO failure
//! logs and never aborts the launch (the session still opens, just without that artefact).

use std::path::Path;

use gt_skills::{ModelConfig, SkillCatalog};

/// Materialise `role`'s enabled skills + Knowledge prompt into `workdir`, returning its model config.
///
/// - Each enabled skill's `SKILL.md` body → `<workdir>/.claude/skills/<id>/SKILL.md` (a skill bound
///   with no body is skipped — nothing to write).
/// - The role's Knowledge prompt → `<workdir>/CLAUDE.md`, with `render_pairs` substituted into its
///   placeholders (`<Token>` / `{{ .Token }}` forms) so the prompt names this session's real
///   workspace/rig/workdir.
/// - Returns the role's [`ModelConfig`] (if the catalog configures one) so the caller stamps
///   `--model`/`--effort` onto the launch args; `None` ⇒ leave the launch's default model.
///
/// Best-effort: any IO failure logs to stderr and is swallowed, so a launch is never blocked on a
/// skills/CLAUDE.md write.
pub fn materialize_role_session(
    catalog: &SkillCatalog,
    role: &str,
    workdir: &Path,
    render_pairs: &[(&str, String)],
) -> Option<ModelConfig> {
    // SKILL.md bodies → <workdir>/.claude/skills/<id>/SKILL.md. Per-skill best-effort: one write
    // failure never skips the rest.
    let skills_dir = workdir.join(".claude").join("skills");
    for id in catalog.skills_for_role(role) {
        let Some(skill) = catalog.get(&id) else {
            continue;
        };
        if skill.body.trim().is_empty() {
            continue; // a binding with no SKILL.md body has nothing to materialise
        }
        let dir = skills_dir.join(&id);
        if let Err(e) =
            std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(dir.join("SKILL.md"), &skill.body))
        {
            eprintln!("[role-session] skill {id} write for role {role} skipped: {e}");
        }
    }
    // Role Knowledge prompt → <workdir>/CLAUDE.md (rendered). claude auto-loads it as the project
    // instructions, replacing the repo's generic CLAUDE.md for this session.
    if let Some(prompt) = catalog.role_prompt(role) {
        let rendered = crate::terminal::render_prompt(&prompt, render_pairs);
        if let Err(e) = std::fs::write(workdir.join("CLAUDE.md"), &rendered) {
            eprintln!(
                "[role-session] CLAUDE.md write for role {role} in {} skipped: {e}",
                workdir.display()
            );
        }
    }
    catalog.role_model(role)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_skills::{SkillEvent, SkillState};

    /// Build a catalog by applying a sequence of `skills.*` events, the same way the launch paths
    /// replay it from the event log.
    fn catalog_from(events: Vec<SkillEvent>) -> SkillCatalog {
        let mut state = SkillState::default();
        for ev in &events {
            state.apply(ev);
        }
        state.catalog
    }

    #[test]
    fn writes_skill_bodies_and_rendered_claude_md() {
        let tmp = tempfile::tempdir().unwrap();
        let wd = tmp.path();
        // Register a skill WITH a body, bind it to the `mayor` role, give the role a Knowledge prompt.
        let catalog = catalog_from(vec![
            SkillEvent::Registered {
                skill: "orchestrate".into(),
                label: "Orchestrate".into(),
                description: "drive the frontier".into(),
                default_scopes: vec![],
                body: "# Orchestrate\nbody".into(),
                group: String::new(),
                now_secs: 1,
            },
            SkillEvent::EnabledForRole {
                role: "mayor".into(),
                skill: "orchestrate".into(),
                now_secs: 2,
            },
            SkillEvent::RolePromptSet {
                role: "mayor".into(),
                prompt: "You are the <RigName> mayor in <workspace>.".into(),
                now_secs: 3,
            },
        ]);

        let model = materialize_role_session(
            &catalog,
            "mayor",
            wd,
            &[
                ("workspace", "default".to_string()),
                ("RigName", "gtcore".to_string()),
            ],
        );

        // SKILL.md body landed.
        let skill = std::fs::read_to_string(wd.join(".claude/skills/orchestrate/SKILL.md")).unwrap();
        assert_eq!(skill, "# Orchestrate\nbody");
        // CLAUDE.md rendered with this session's workspace/rig.
        let claude = std::fs::read_to_string(wd.join("CLAUDE.md")).unwrap();
        assert_eq!(claude, "You are the gtcore mayor in default.");
        // No model configured ⇒ None.
        assert!(model.is_none());
    }

    #[test]
    fn no_role_prompt_writes_no_claude_md_and_is_best_effort() {
        let tmp = tempfile::tempdir().unwrap();
        let wd = tmp.path();
        // An empty catalog: the role has no skills and no Knowledge prompt.
        let catalog = catalog_from(vec![]);
        let model = materialize_role_session(&catalog, "mayor", wd, &[]);
        assert!(model.is_none());
        assert!(!wd.join("CLAUDE.md").exists(), "no prompt ⇒ no CLAUDE.md");
        assert!(!wd.join(".claude/skills").exists(), "no skills ⇒ no skills dir");
    }
}
