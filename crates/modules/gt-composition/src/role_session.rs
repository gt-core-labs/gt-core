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

use gt_skills::{default_role_permissions, ModelConfig, RolePermissions, SkillCatalog};

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

/// The role's claude permission model, read from the catalog (the DB) — the SINGLE source the launch
/// uses (gtcore-d175ec), like [`SkillCatalog::role_prompt`]/[`role_model`](SkillCatalog::role_model).
/// Falls back to the apparatus default ([`gt_skills::default_role_permissions`]) ONLY when the
/// catalog has no permissions for the role yet (a catalog seeded before the attribute existed and not
/// migrated on this read path): defense-in-depth so an agent is never launched without the
/// memory-guard `deny`. Once the one-shot migration backfills the DB, the value comes from there.
pub fn role_permissions_or_default(catalog: &SkillCatalog, role: &str) -> RolePermissions {
    catalog
        .role_permissions(role)
        .unwrap_or_else(default_role_permissions)
}

/// Overlay a role's `permissions` block into an existing claude `settings.json` at `path` (merge,
/// preserving claude's other keys). The launch paths that write a static settings template
/// (`install_polecat_hooks` for the polecat worktree, `seed_user_hooks` for the mayor account) call
/// this AFTER the template lands so the permission model comes from the catalog, not the template.
/// Best-effort: any IO/parse failure logs and the session still launches.
pub fn overlay_permissions(settings_path: &Path, perms: &RolePermissions) {
    let mut root = std::fs::read_to_string(settings_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = root.as_object_mut() else {
        return;
    };
    obj.insert("permissions".into(), perms.to_settings_json());
    match serde_json::to_string_pretty(&root) {
        Ok(body) => {
            if let Err(e) = std::fs::write(settings_path, body) {
                eprintln!(
                    "[role-session] permissions overlay write {} skipped: {e}",
                    settings_path.display()
                );
            }
        }
        Err(e) => eprintln!("[role-session] permissions overlay serialize skipped: {e}"),
    }
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
    fn permissions_read_from_catalog_with_apparatus_default_fallback_and_overlay() {
        // gtcore-d175ec: role_permissions_or_default reads the DB, falling back to the apparatus
        // default; overlay_permissions merges the block into an existing settings.json.
        let tmp = tempfile::tempdir().unwrap();
        let settings = tmp.path().join("settings.json");
        std::fs::write(&settings, r#"{"hasCompletedOnboarding":true}"#).unwrap();

        // Empty catalog ⇒ fallback to the apparatus default (never launch without the memory guard).
        let empty = catalog_from(vec![]);
        let perms = role_permissions_or_default(&empty, "mayor");
        assert_eq!(perms, gt_skills::default_role_permissions());

        overlay_permissions(&settings, &perms);
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&settings).unwrap()).unwrap();
        // The overlay merged (preserved the existing key) and wrote the permission model.
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["permissions"]["defaultMode"], "bypassPermissions");
        assert!(v["permissions"]["deny"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d == "Write(**/memory/**.md)"));

        // An operator-set value in the catalog (the DB) wins over the default.
        let catalog = catalog_from(vec![SkillEvent::RolePermissionsSet {
            role: "mayor".into(),
            default_mode: "bypassPermissions".into(),
            deny: vec!["Write(**/secret/**)".into()],
            now_secs: 1,
        }]);
        let custom = role_permissions_or_default(&catalog, "mayor");
        assert_eq!(custom.deny, vec!["Write(**/secret/**)".to_string()]);
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
