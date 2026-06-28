//! Per-polecat git worktree provisioning (`hq-orchd-deploy.9`).
//!
//! `SpawnTemplate::spec_for` points every polecat at the same `GT_RIG_PATH` checkout. That is a
//! data race: CLAUDE.md is explicit that the working tree's HEAD/branch is global, so two agents
//! doing `git`-work in one checkout race — a commit lands on the wrong branch, or a branch tip
//! vanishes between two commands. The mandated fix is one worktree per actor:
//!
//! ```text
//! git worktree add <root>/<session> -b <bead-id> main
//! ```
//!
//! This module is the edge that runs that for a slung polecat: [`provision`] creates the
//! per-polecat worktree off the rig checkout (idempotent — an existing path is reused, so a
//! re-sling lands in the same tree), and [`remove`] tears it down when the bead merges. The
//! composition layer owns this (not `gt-polecat`) because it is git I/O against the daemon's host
//! checkout, and it is gated on `GT_POLECAT_WORKTREE_ROOT`: unset ⇒ the legacy shared-checkout
//! behaviour, unchanged.

use std::path::Path;
use std::process::Command;

/// The remote ref every per-polecat branch is cut from (`hq-orchd-deploy.22`). The daemon's rig
/// checkout is cloned once and never pulled, so its local HEAD drifts behind the real `main`; cutting
/// from `origin/main` (after a fetch) keeps each polecat on the current tip so the git-merge edge
/// (`hq-orchd-deploy.12`) fast-forwards instead of rebasing every merge.
const REMOTE_BASE_REF: &str = "origin/main";

/// `git -C <base> fetch origin` argv — refresh the remote-tracking refs so the worktree below is cut
/// from the current `origin/main`, not a stale clone-time tip (data, asserted in tests).
fn fetch_argv() -> Vec<String> {
    vec!["fetch".to_string(), "origin".to_string()]
}

/// The `git worktree add` argv (relative to a `-C <base>` invocation), as data so it can be
/// asserted without running git. Branches `branch` off `base_ref` (the freshly-fetched
/// `origin/main`, not the base checkout's drifting HEAD — `hq-orchd-deploy.22`), mirroring
/// CLAUDE.md's `-b <bead-id> main`. `--force` lets a re-create after a crash reuse a
/// registered-but-stale path rather than abort the sling.
fn add_argv(path: &Path, branch: &str, base_ref: &str) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "add".to_string(),
        "--force".to_string(),
        path.display().to_string(),
        "-b".to_string(),
        branch.to_string(),
        base_ref.to_string(),
    ]
}

/// `git worktree add` argv cutting `branch` off the base checkout's current HEAD (no explicit start
/// point) — the fallback when `origin/main` is unavailable (no remote, e.g. a local-only rig).
fn add_off_head_argv(path: &Path, branch: &str) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "add".to_string(),
        "--force".to_string(),
        path.display().to_string(),
        "-b".to_string(),
        branch.to_string(),
    ]
}

/// `git worktree add` argv that attaches an EXISTING branch (no `-b`) — the fallback when the
/// branch already exists (a re-sling after the branch was created but the worktree was pruned).
fn add_existing_branch_argv(path: &Path, branch: &str) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "add".to_string(),
        "--force".to_string(),
        path.display().to_string(),
        branch.to_string(),
    ]
}

/// The `git worktree remove --force` argv for teardown.
fn remove_argv(path: &Path) -> Vec<String> {
    vec![
        "worktree".to_string(),
        "remove".to_string(),
        "--force".to_string(),
        path.display().to_string(),
    ]
}

/// Ensure a per-polecat worktree exists at `path`, branched `branch` off the current `origin/main`.
///
/// Idempotent: if `path` already exists (a re-sling, or a crash-restart) it is reused as-is and no
/// git runs. Otherwise it first `git -C base fetch origin` (best-effort) then
/// `git -C base worktree add --force <path> -b <branch> origin/main`, so the polecat starts from the
/// live `main` even though the daemon's rig checkout is cloned once and never pulled
/// (`hq-orchd-deploy.22`) — the git-merge edge then fast-forwards instead of rebasing every merge.
/// Falls back to cutting off the base HEAD when `origin/main` is unavailable (no remote), then to
/// attaching the branch if it already exists. Returns the path on success. Best-effort by contract:
/// the caller logs and falls back to the shared checkout on failure, so a git hiccup never strands a
/// sling.
pub fn provision(base_repo: &Path, path: &Path, branch: &str) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    let run = |argv: &[String]| -> std::io::Result<bool> {
        let status = Command::new("git")
            .arg("-C")
            .arg(base_repo)
            .args(argv)
            .status()?;
        Ok(status.success())
    };
    // Refresh origin/main so the branch is cut from the live tip, not the stale clone (best-effort:
    // a fetch failure just falls through to whatever origin/main is already known locally).
    let _ = run(&fetch_argv());
    if run(&add_argv(path, branch, REMOTE_BASE_REF))? {
        return Ok(());
    }
    // No remote / origin/main missing (local-only rig) — cut off the base checkout's HEAD instead.
    if run(&add_off_head_argv(path, branch))? {
        return Ok(());
    }
    // The `-b` forms fail if the branch already exists — attach it instead.
    if run(&add_existing_branch_argv(path, branch))? {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "git worktree add failed for {} (branch {branch}) off {}",
        path.display(),
        base_repo.display()
    )))
}

/// Tear down a polecat's worktree (best-effort): `git -C base worktree remove --force <path>`. The
/// branch itself is reaped separately by the branch-GC reactor on `merge.merged.v1`. A failure
/// (already gone, dirty tree) is swallowed by the caller — leftover worktrees are housekeeping, not
/// correctness.
pub fn remove(base_repo: &Path, path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let ok = Command::new("git")
        .arg("-C")
        .arg(base_repo)
        .args(remove_argv(path))
        .status()?
        .success();
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "git worktree remove failed for {}",
            path.display()
        )))
    }
}

/// Boot-time GC of orphaned worktrees (gtcore-acacfb): remove every `<root>/<child>` directory
/// whose name is NOT in `live`. A polecat's tmux dies with the orchd pod, so on restart every
/// worktree under the root is an orphan from a prior life (pass an empty `live` set). Left alone
/// they accumulate a full cargo `target/` each (tens of GB) until the disk fills — 107 trees /
/// 372 GB were found 2026-06-28, a co-cause of the etcd I/O-saturation incident. Plain `rm -rf`
/// (not `git worktree remove`) because the owning base repo varies per tree and the space is what
/// matters; `provision` re-creates with `--force`, so a stale `.git/worktrees` entry never blocks a
/// later sling. Best-effort: read/remove errors log and the sweep continues. Returns trees removed.
pub fn sweep_orphans(root: &Path, live: &std::collections::HashSet<String>) -> usize {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return 0, // root absent (no worktrees yet) — nothing to sweep
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if live.contains(&*name.to_string_lossy()) {
            continue;
        }
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {
                removed += 1;
                eprintln!("[polecat] worktree sweep: removed orphan {}", path.display());
            }
            Err(e) => eprintln!("[polecat] worktree sweep: rm {} failed: {e}", path.display()),
        }
    }
    removed
}

/// Seed the polecat's `.mcp.json` into a fresh worktree (`hq-orchd-deploy.10`). The file is
/// machine-local (untracked, so it does NOT come with the worktree's checkout) yet the polecat's
/// claude needs it to reach the `gt` MCP server. Copy it from the base rig checkout, which the
/// operator configures once. Best-effort: a missing source (operator didn't place one) or an
/// already-present target is a silent no-op — the caller treats MCP wiring as non-fatal.
///
/// Prefer [`write_mcp_json`] when the daemon has a live token: it writes a fresh, per-sling
/// `.mcp.json` instead of copying a static one whose token may be expired or rig-specific.
pub fn seed_mcp_config(base_repo: &Path, worktree: &Path) {
    let src = base_repo.join(".mcp.json");
    let dst = worktree.join(".mcp.json");
    if dst.exists() || !src.exists() {
        return;
    }
    if let Err(e) = std::fs::copy(&src, &dst) {
        eprintln!(
            "[polecat] .mcp.json seed {} -> {} skipped: {e}",
            src.display(),
            dst.display()
        );
    }
}

/// Write a fresh per-sling `.mcp.json` into `worktree` pointing at `url/mcp` and carrying the
/// per-session `token` in the `Authorization` header (`hq-polecat-rig-config.1`). Called instead
/// of [`seed_mcp_config`] when the daemon has minted a live token — the generated file is always
/// valid for THIS sling, regardless of what (if anything) the operator placed in the base checkout.
/// Best-effort: any write failure is logged and the caller falls back to [`seed_mcp_config`].
pub fn write_mcp_json(worktree: &Path, url: &str, workspace: &str, rig: &str, token: &str) -> bool {
    let dst = worktree.join(".mcp.json");
    let mcp_url = format!("{}/mcp", url.trim_end_matches('/'));
    let body = serde_json::json!({
        "mcpServers": {
            "gt": {
                "type": "http",
                "url": mcp_url,
                "headers": {
                    "Authorization": format!("Bearer {token}"),
                    "X-Workspace": workspace,
                    // hq-rig-isolation.6: carry the rig so the server can apply it as
                    // a default filter without the agent explicitly passing ?rig= each time.
                    "X-Rig": rig,
                }
            }
        }
    });
    match serde_json::to_string_pretty(&body) {
        Ok(s) => std::fs::write(&dst, s)
            .map_err(|e| eprintln!("[polecat] .mcp.json write {} skipped: {e}", dst.display()))
            .is_ok(),
        Err(e) => {
            eprintln!("[polecat] .mcp.json serialize skipped: {e}");
            false
        }
    }
}

/// Write a per-sling `.gt-config/` into `worktree` so the `gt` CLI running inside the polecat
/// reads the correct `rig`, `workspace`, and `server_url` without manual setup
/// (hq-rig-isolation.5). The config follows the same layout as `gt init` produces:
/// `config.toml` names the active profile (`default`) and `default.toml` holds the fields.
///
/// Best-effort: any write failure is logged and the caller continues — MCP still works via
/// `.mcp.json`; the rig just won't be auto-injected from the config.
pub fn write_gt_config(
    worktree: &Path,
    server_url: &str,
    workspace: &str,
    rig: &str,
    token: &str,
) -> bool {
    let dir = worktree.join(".gt-config");
    if std::fs::create_dir_all(&dir).is_err() {
        return false;
    }
    let profile = format!(
        "server_url = \"{server_url}\"\n\
         workspace = \"{workspace}\"\n\
         rig = \"{rig}\"\n\
         access_token = \"{token}\"\n\
         refresh_token = \"\"\n"
    );
    let ok = std::fs::write(dir.join("config.toml"), "active = \"default\"\n")
        .map_err(|e| eprintln!("[polecat] .gt-config/config.toml write skipped: {e}"))
        .is_ok()
        && std::fs::write(dir.join("default.toml"), profile)
            .map_err(|e| eprintln!("[polecat] .gt-config/default.toml write skipped: {e}"))
            .is_ok();
    ok
}

/// Install the polecat reporting hooks at the account's USER settings
/// (`CLAUDE_CONFIG_DIR/settings.json`) so they actually fire (`hq-orchd-deploy.15`). claude does not
/// run **project** hooks (`<worktree>/.claude/settings.json`) for an unapproved repo — so the
/// heartbeat + Stop→merge-ready hooks the daemon seeds into the worktree never executed, leaving the
/// polecat without a heartbeat (supervisor re-slings) and without a merge-ready drop (no push). User
/// settings are trusted, so their hooks run headlessly. Clobber-safe: only writes when the file is
/// absent or already gt-managed (carries the marker), never over a human's settings.
pub fn seed_user_hooks(config_dir: &Path) {
    // The dir may not exist yet when seeding a host-default `$HOME/.claude`
    // (hq-polecat-provisioning-20260608.2); create it so the write below lands. Best-effort.
    let _ = std::fs::create_dir_all(config_dir);
    let target = config_dir.join("settings.json");
    // Merge (don't clobber): claude writes its own keys here (e.g. skipDangerousModePermissionPrompt),
    // so overlay our managed keys (marker + onboarding + bypass + the reporting hooks) onto whatever
    // exists. The account dir is a dedicated polecat account — there is no human settings to protect.
    let mut root = std::fs::read_to_string(&target)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let tmpl: serde_json::Value = serde_json::from_str(&gt_polecat::polecat_settings_json())
        .expect("polecat settings template is valid json");
    match (root.as_object_mut(), tmpl.as_object()) {
        (Some(obj), Some(t)) => {
            for (k, v) in t {
                obj.insert(k.clone(), v.clone());
            }
        }
        _ => return,
    }
    match serde_json::to_string_pretty(&root) {
        Ok(s) => {
            if let Err(e) = std::fs::write(&target, s) {
                eprintln!(
                    "[polecat] user hooks seed {} skipped: {e}",
                    target.display()
                );
            }
        }
        Err(e) => eprintln!("[polecat] user hooks serialize skipped: {e}"),
    }
}

/// Pre-accept claude's onboarding in an account's `CLAUDE_CONFIG_DIR` so an INTERACTIVE polecat does
/// not stall on the first-run TUI (`hq-orchd-deploy.14`). A polecat must run interactive (not
/// `--print`) for its heartbeat + Stop→merge-ready hooks to fire, but a fresh config dir then stops
/// at three prompts — theme, folder-trust, and the bypass-permissions accept — and never works the
/// bead. This marks onboarding + bypass-mode accepted (global) and trusts the specific `worktree`
/// path (folder-trust is per-project). Merges into the existing `.claude.json` (claude owns other
/// keys like the oauth creds); an existing `theme` is preserved. Best-effort: any IO/parse failure
/// logs and the polecat still slings (it just shows the TUI again).
pub fn seed_claude_onboarding(config_dir: &Path, worktree: &Path) {
    // The dir may not exist yet when seeding a host-default `$HOME/.claude`
    // (hq-polecat-provisioning-20260608.2); create it so the write below lands. Best-effort.
    let _ = std::fs::create_dir_all(config_dir);
    let path = config_dir.join(".claude.json");
    let mut root = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = root.as_object_mut() else {
        eprintln!(
            "[polecat] .claude.json at {} is not an object — onboarding seed skipped",
            path.display()
        );
        return;
    };
    obj.insert(
        "hasCompletedOnboarding".into(),
        serde_json::Value::Bool(true),
    );
    obj.insert(
        "bypassPermissionsModeAccepted".into(),
        serde_json::Value::Bool(true),
    );
    // Disable auto-updater: polecats run in worktrees/containers where npm
    // prefix is read-only, causing noisy "no write permission" warnings.
    obj.insert(
        "autoUpdaterStatus".into(),
        serde_json::Value::String("disabled".into()),
    );
    obj.entry("theme")
        .or_insert_with(|| serde_json::Value::String("dark".into()));
    // Folder trust is per-project: mark THIS worktree path trusted + onboarded, and pre-enable the
    // project `.mcp.json` `gt` server HERE. claude 2.1.x reads project-MCP enablement from this
    // per-project entry in `.claude.json`, NOT from `<workdir>/.claude/settings.json`: without it the
    // server connects (its resources surface) but its TOOLS are withheld behind the "Use this MCP
    // server?" trust gate, so an autonomous session sees `gt://…` resources yet no `mcp__gt__*` tools
    // and stalls (observed live for a mayor session). Mirrors the polecat `enabledMcpjsonServers`
    // pre-trust (hq-polecat-provisioning-20260608.1), applied to the interactive role path too.
    let projects = obj
        .entry("projects")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(pobj) = projects.as_object_mut() {
        pobj.insert(
            worktree.display().to_string(),
            serde_json::json!({
                "hasTrustDialogAccepted": true,
                "hasCompletedProjectOnboarding": true,
                "enabledMcpjsonServers": ["gt"]
            }),
        );
    }
    match serde_json::to_string(&root) {
        Ok(s) => {
            if let Err(e) = std::fs::write(&path, s) {
                eprintln!(
                    "[polecat] onboarding seed write {} skipped: {e}",
                    path.display()
                );
            }
        }
        Err(e) => eprintln!("[polecat] onboarding seed serialize skipped: {e}"),
    }
}

/// Pre-accept the global claude onboarding flags (`bypassPermissionsModeAccepted` +
/// `hasCompletedOnboarding`) for an account's `CLAUDE_CONFIG_DIR` without needing a worktree path.
/// Called from the interactive terminal launch path when an active account is resolved, so a
/// freshly-registered account that never had a polecat slung into it still skips the bypass-
/// permissions confirmation prompt. Merges into the existing `.claude.json` — never overwrites
/// keys claude owns (oauth creds, theme, etc.). Best-effort.
pub fn seed_global_claude_flags(config_dir: &Path) {
    let _ = std::fs::create_dir_all(config_dir);
    let path = config_dir.join(".claude.json");
    let mut root = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = root.as_object_mut() else {
        return;
    };
    obj.insert("hasCompletedOnboarding".into(), serde_json::Value::Bool(true));
    obj.insert(
        "bypassPermissionsModeAccepted".into(),
        serde_json::Value::Bool(true),
    );
    if let Ok(s) = serde_json::to_string(obj) {
        let _ = std::fs::write(&path, s);
    }
}

/// Seed the FULL onboarding-complete state into a relogged-in account's `CLAUDE_CONFIG_DIR` so the
/// dir is immediately consumable by the sling, pre-authenticated — the same shape the working
/// `01KTWX*` accounts carry (gtcore-1fe9b4).
///
/// The wedge incident (`orchd-sling-provisioning-wedge-recovery`): an operator relogin left a dir
/// with a fresh `.credentials.json` but NO `oauthAccount` nor `hasCompletedOnboarding` in
/// `.claude.json`, so the slung polecat ran the first-run onboarding TUI → OAuth prompt → wedge.
/// After `quota.relogin` mints fresh creds, this seeds the missing IDENTITY/onboarding markers:
///
/// - `oauthAccount.emailAddress = email` — the identity the sling/quota registry keys by. Only the
///   email is forced; any richer `oauthAccount` object `claude /login` already wrote is preserved
///   (we merge into the existing object, never replace it).
/// - `hasCompletedOnboarding = true` + `bypassPermissionsModeAccepted = true` + a default `theme`
///   (via [`seed_global_claude_flags`]'s flags, applied here so one call leaves the dir complete).
///
/// Best-effort: any IO/parse failure logs and the relogin still reports success (the operator can
/// re-run; a missing marker only re-shows the TUI, it does not lose the fresh credential).
pub fn seed_account_onboarding_complete(config_dir: &Path, email: &str) {
    let _ = std::fs::create_dir_all(config_dir);
    let path = config_dir.join(".claude.json");
    let mut root = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = root.as_object_mut() else {
        eprintln!(
            "[relogin] .claude.json at {} is not an object — onboarding seed skipped",
            path.display()
        );
        return;
    };
    obj.insert("hasCompletedOnboarding".into(), serde_json::Value::Bool(true));
    obj.insert(
        "bypassPermissionsModeAccepted".into(),
        serde_json::Value::Bool(true),
    );
    obj.insert(
        "autoUpdaterStatus".into(),
        serde_json::Value::String("disabled".into()),
    );
    obj.entry("theme")
        .or_insert_with(|| serde_json::Value::String("dark".into()));
    // Ensure the oauthAccount carries the email (the id the system keys by). Merge into whatever
    // object `claude /login` already wrote — never clobber the richer identity it may have set.
    let oauth = obj
        .entry("oauthAccount")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(oobj) = oauth.as_object_mut() {
        oobj.insert(
            "emailAddress".into(),
            serde_json::Value::String(email.to_string()),
        );
    }
    match serde_json::to_string(&root) {
        Ok(s) => {
            if let Err(e) = std::fs::write(&path, s) {
                eprintln!(
                    "[relogin] onboarding seed write {} skipped: {e}",
                    path.display()
                );
            }
        }
        Err(e) => eprintln!("[relogin] onboarding seed serialize skipped: {e}"),
    }
}

/// `chown -R <user> <path>` argv, as data so it can be asserted without shelling.
fn chown_argv(path: &Path, user: &str) -> Vec<String> {
    vec![
        "-R".to_string(),
        user.to_string(),
        path.display().to_string(),
    ]
}

/// Hand a freshly-provisioned worktree to the non-root polecat user (`hq-quota-accounts.6`).
/// `git worktree add` runs as the root daemon, so the tree is root-owned; a polecat re-exec'd under
/// `GT_POLECAT_RUN_AS` could not read/write it. `chown -R <user>` fixes that. Best-effort: a failure
/// (no such user, not root) logs and the sling proceeds — the operator sees the cause. No-op for an
/// empty user.
pub fn chown_to(path: &Path, user: &str) -> std::io::Result<()> {
    if user.trim().is_empty() {
        return Ok(());
    }
    let ok = Command::new("chown")
        .args(chown_argv(path, user.trim()))
        .status()?
        .success();
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "chown -R {user} {} failed",
            path.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn seed_onboarding_marks_flags_and_trusts_the_worktree() {
        let cfg = tempfile::tempdir().unwrap();
        // Pre-existing .claude.json with creds + a user theme that must be preserved.
        std::fs::write(
            cfg.path().join(".claude.json"),
            r#"{"oauthAccount":{"x":1},"theme":"light"}"#,
        )
        .unwrap();
        let wt = PathBuf::from("/rig-wt/gt-hq-x.1");
        seed_claude_onboarding(cfg.path(), &wt);

        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.path().join(".claude.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["bypassPermissionsModeAccepted"], serde_json::json!(true));
        assert_eq!(v["theme"], serde_json::json!("light")); // preserved, not overwritten
        assert_eq!(v["oauthAccount"]["x"], serde_json::json!(1)); // creds untouched
        assert_eq!(
            v["projects"]["/rig-wt/gt-hq-x.1"]["hasTrustDialogAccepted"],
            serde_json::json!(true)
        );
        // The project `gt` MCP server is pre-enabled HERE so its tools (not just resources) surface
        // without the interactive trust prompt (hq-mcp-projtrust).
        assert_eq!(
            v["projects"]["/rig-wt/gt-hq-x.1"]["enabledMcpjsonServers"],
            serde_json::json!(["gt"])
        );
    }

    #[test]
    fn seed_user_hooks_merges_over_existing_claude_settings() {
        let cfg = tempfile::tempdir().unwrap();
        // claude's own settings the merge must preserve.
        std::fs::write(
            cfg.path().join("settings.json"),
            r#"{"skipDangerousModePermissionPrompt":true}"#,
        )
        .unwrap();
        seed_user_hooks(cfg.path());
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.path().join("settings.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            v["skipDangerousModePermissionPrompt"],
            serde_json::json!(true)
        ); // preserved
        assert_eq!(v["_gt_managed"], serde_json::json!("polecat-hooks")); // overlaid
        let stop = v["hooks"]["Stop"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(stop.contains("merge-ready"));
    }

    #[test]
    fn seed_onboarding_creates_config_when_absent() {
        let cfg = tempfile::tempdir().unwrap();
        seed_claude_onboarding(cfg.path(), &PathBuf::from("/wt/a"));
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.path().join(".claude.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["theme"], serde_json::json!("dark")); // default when none
    }

    #[test]
    fn seed_account_onboarding_complete_sets_identity_and_flags_preserving_oauth() {
        // gtcore-1fe9b4: after a relogin, the dir must carry oauthAccount.emailAddress +
        // hasCompletedOnboarding so the sling skips the first-run TUI — and any richer oauthAccount
        // claude already wrote (org, uuid) must be preserved, not clobbered.
        let cfg = tempfile::tempdir().unwrap();
        std::fs::write(
            cfg.path().join(".claude.json"),
            r#"{"oauthAccount":{"organizationName":"Org","accountUuid":"u-1"},"theme":"light"}"#,
        )
        .unwrap();
        seed_account_onboarding_complete(cfg.path(), "user@x.com");
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.path().join(".claude.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["bypassPermissionsModeAccepted"], serde_json::json!(true));
        assert_eq!(v["theme"], serde_json::json!("light")); // preserved
        assert_eq!(v["oauthAccount"]["emailAddress"], serde_json::json!("user@x.com"));
        assert_eq!(v["oauthAccount"]["organizationName"], serde_json::json!("Org")); // preserved
        assert_eq!(v["oauthAccount"]["accountUuid"], serde_json::json!("u-1")); // preserved
    }

    #[test]
    fn seed_account_onboarding_complete_creates_config_when_absent() {
        let cfg = tempfile::tempdir().unwrap();
        seed_account_onboarding_complete(cfg.path(), "fresh@x.com");
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.path().join(".claude.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(v["hasCompletedOnboarding"], serde_json::json!(true));
        assert_eq!(v["theme"], serde_json::json!("dark")); // default when none
        assert_eq!(v["oauthAccount"]["emailAddress"], serde_json::json!("fresh@x.com"));
    }

    #[test]
    fn seed_creates_the_config_dir_when_it_does_not_exist() {
        // hq-polecat-provisioning-20260608.2: seeding a host-default `$HOME/.claude` that was never
        // created must still land — both seeders create the dir first.
        let base = tempfile::tempdir().unwrap();
        let cfg = base.path().join("nonexistent").join(".claude");
        assert!(!cfg.exists());
        seed_claude_onboarding(&cfg, &PathBuf::from("/wt/a"));
        seed_user_hooks(&cfg);
        let onboarding: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(cfg.join(".claude.json")).unwrap())
                .unwrap();
        assert_eq!(
            onboarding["bypassPermissionsModeAccepted"],
            serde_json::json!(true)
        );
        let settings: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(cfg.join("settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["enabledMcpjsonServers"], serde_json::json!(["gt"]));
    }

    #[test]
    fn add_argv_branches_off_origin_main_with_force() {
        // hq-orchd-deploy.22: the start point is origin/main (the live tip), not the rig's HEAD.
        let argv = add_argv(&PathBuf::from("/wt/hq-gg-1"), "gg-1", REMOTE_BASE_REF);
        assert_eq!(
            argv,
            vec![
                "worktree",
                "add",
                "--force",
                "/wt/hq-gg-1",
                "-b",
                "gg-1",
                "origin/main"
            ]
        );
    }

    #[test]
    fn add_off_head_argv_has_no_start_point() {
        // The no-remote fallback cuts off the base HEAD (no explicit start ref).
        let argv = add_off_head_argv(&PathBuf::from("/wt/hq-gg-1"), "gg-1");
        assert_eq!(
            argv,
            vec!["worktree", "add", "--force", "/wt/hq-gg-1", "-b", "gg-1"]
        );
    }

    #[test]
    fn fetch_argv_refreshes_origin() {
        assert_eq!(fetch_argv(), vec!["fetch", "origin"]);
    }

    #[test]
    fn add_existing_branch_argv_omits_dash_b() {
        let argv = add_existing_branch_argv(&PathBuf::from("/wt/hq-gg-1"), "gg-1");
        assert_eq!(
            argv,
            vec!["worktree", "add", "--force", "/wt/hq-gg-1", "gg-1"]
        );
        assert!(!argv.iter().any(|a| a == "-b"));
    }

    #[test]
    fn chown_argv_is_recursive() {
        assert_eq!(
            chown_argv(&PathBuf::from("/wt/hq-gg-1"), "gtpolecat"),
            vec!["-R", "gtpolecat", "/wt/hq-gg-1"]
        );
    }

    #[test]
    fn chown_to_empty_user_is_noop() {
        chown_to(&PathBuf::from("/anything"), "   ").expect("blank user is a no-op, no chown");
    }

    #[test]
    fn remove_argv_forces() {
        let argv = remove_argv(&PathBuf::from("/wt/hq-gg-1"));
        assert_eq!(argv, vec!["worktree", "remove", "--force", "/wt/hq-gg-1"]);
    }

    #[test]
    fn provision_is_idempotent_when_path_exists() {
        // An existing path is reused without shelling git (the re-sling / restart case).
        let dir = std::env::temp_dir();
        assert!(dir.exists());
        provision(&PathBuf::from("/nonexistent-base"), &dir, "any-branch")
            .expect("existing path short-circuits before any git call");
    }

    #[test]
    fn remove_is_noop_when_path_absent() {
        let missing = std::env::temp_dir().join("gt-wt-definitely-absent-xyz");
        remove(&PathBuf::from("/nonexistent-base"), &missing)
            .expect("absent path is a no-op, no git call");
    }

    #[test]
    fn seed_mcp_config_copies_from_base_when_absent_in_worktree() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base = std::env::temp_dir().join(format!("gt-mcpseed-base-{uniq}"));
        let wt = std::env::temp_dir().join(format!("gt-mcpseed-wt-{uniq}"));
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(base.join(".mcp.json"), r#"{"mcpServers":{}}"#).unwrap();

        seed_mcp_config(&base, &wt);
        assert_eq!(
            std::fs::read_to_string(wt.join(".mcp.json")).unwrap(),
            r#"{"mcpServers":{}}"#,
            "the worktree got the base's .mcp.json"
        );

        // Idempotent + non-clobbering: a second call with a different source leaves it.
        std::fs::write(base.join(".mcp.json"), "CHANGED").unwrap();
        seed_mcp_config(&base, &wt);
        assert_eq!(
            std::fs::read_to_string(wt.join(".mcp.json")).unwrap(),
            r#"{"mcpServers":{}}"#,
            "existing target is not clobbered"
        );

        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&wt);
    }

    #[test]
    fn seed_mcp_config_is_silent_noop_without_a_source() {
        let wt = std::env::temp_dir().join(format!("gt-mcpseed-nosrc-{}", std::process::id()));
        std::fs::create_dir_all(&wt).unwrap();
        seed_mcp_config(&PathBuf::from("/nonexistent-base"), &wt);
        assert!(!wt.join(".mcp.json").exists());
        let _ = std::fs::remove_dir_all(&wt);
    }

    fn uniq(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "gt-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn sweep_orphans_removes_orphans_keeps_live() {
        let root = uniq("wt-sweep");
        for s in ["dead-a", "dead-b", "alive"] {
            std::fs::create_dir_all(root.join(s).join("target")).unwrap();
        }
        // A stray file (not a dir) must be ignored, not counted.
        std::fs::write(root.join("a-file"), "x").unwrap();

        let mut live = std::collections::HashSet::new();
        live.insert("alive".to_string());
        let removed = sweep_orphans(&root, &live);

        assert_eq!(removed, 2, "both dead trees swept, alive + file left");
        assert!(!root.join("dead-a").exists());
        assert!(!root.join("dead-b").exists());
        assert!(root.join("alive").exists(), "a live session's tree is kept");
        assert!(root.join("a-file").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn sweep_orphans_absent_root_is_noop() {
        let missing = uniq("wt-sweep-absent");
        assert_eq!(sweep_orphans(&missing, &std::collections::HashSet::new()), 0);
    }
}
