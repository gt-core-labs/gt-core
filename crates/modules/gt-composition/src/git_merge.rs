//! Git-merge **edge effect** (`hq-orchd-deploy.12`): the arm that lands a polecat branch on
//! `main`, closing the autonomous loop.
//!
//! Everything up to here is pure state: the refinery consumes a `MERGE_READY`, the merge actor
//! advances the slot `Ready → Merging` and emits `merge.started.v1`, but **nothing runs real
//! git** — so the commit stays on the branch and never reaches `main` (the deferred edge-effect
//! #6, see [`crate`] lib docs). [`GitMergePlugin`] is that edge: it observes `merge.started.v1`,
//! resolves the slot's branch from the live board, and executes the real merge per CLAUDE.md
//! flat-history rules — `git push origin <branch>:main` as a pure fast-forward, rebasing the
//! branch onto `origin/main` and retrying on divergence — then drives the slot to its terminal
//! state via the merge actor: [`MergeHandle::complete`] (→ `merge.merged.v1`) on success, or
//! [`MergeHandle::fail`] (→ `merge.failed.v1`) on conflict.
//!
//! It is the edge sibling of [`crate::polecat::PolecatSupervisorPlugin`] (the tmux sling): both
//! are real-I/O observers the **bin** registers on the hub relay, kept out of the pure-state
//! `daemon_root`. The merge is run on a blocking thread (`spawn_blocking`) — `git` is blocking
//! child-process I/O and must stay off the runtime workers.
//!
//! The push uses the rig checkout's already-configured `origin` remote (the container entrypoint
//! sets the `GT_RIG_GIT_TOKEN` https remote); this module shells `git`, it never handles the
//! token. Branches are checked out in per-bead worktrees ([`crate::worktree`]), so a rebase runs
//! `git -C <worktree> rebase origin/main` against the worktree that holds the branch, discovered
//! from `git worktree list`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;

use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_mcp_server::bead_prefix;
use gt_merge::actor::MergeHandle;
use gt_merge::MergeEvent;
use gt_plugin::Plugin;

/// `git -C <rig> fetch origin` argv, as data so it can be asserted without running git.
fn fetch_argv() -> Vec<String> {
    vec!["fetch".into(), "origin".into()]
}

/// `git -C <rig> push origin <branch>` argv — pushes the branch to origin so a PR can be created.
fn push_branch_argv(branch: &str) -> Vec<String> {
    vec!["push".into(), "origin".into(), branch.into()]
}

/// `git -C <rig> push origin <branch>:main` argv — a pure fast-forward of the remote `main`
/// (the repo config forces `merge.ff=only`; a non-ff push fails and we rebase + retry). Flat
/// history, no merge commit (CLAUDE.md).
fn push_argv(branch: &str) -> Vec<String> {
    vec!["push".into(), "origin".into(), format!("{branch}:main")]
}

/// `git -C <worktree> rebase origin/main` argv — replays the branch onto the advanced remote tip
/// so the retried push fast-forwards. Run in the worktree that holds the branch (the branch is
/// checked out there; rebasing it from the bare rig dir would refuse).
fn rebase_argv() -> Vec<String> {
    vec!["rebase".into(), "origin/main".into()]
}

/// `git -C <worktree> rebase --abort` argv — unwind a conflicted rebase so the worktree is left
/// clean (the merge is reported `failed`, not left mid-rebase).
fn rebase_abort_argv() -> Vec<String> {
    vec!["rebase".into(), "--abort".into()]
}

/// `git -C <rig> rev-parse refs/heads/<branch>` argv — the sha now on `main` after a successful
/// push (reported in `merge.merged.v1`).
fn rev_parse_argv(branch: &str) -> Vec<String> {
    vec!["rev-parse".into(), format!("refs/heads/{branch}")]
}

/// `git -C <rig> worktree list --porcelain` argv — to discover which worktree holds `branch`.
fn worktree_list_argv() -> Vec<String> {
    vec!["worktree".into(), "list".into(), "--porcelain".into()]
}

/// Run `git -C <dir> <argv...>`, returning the captured output. The `git` binary inherits the
/// daemon's environment (the entrypoint configured the authenticated `origin` remote).
fn git(dir: &Path, argv: &[String]) -> std::io::Result<std::process::Output> {
    Command::new("git").arg("-C").arg(dir).args(argv).output()
}

/// `true` + trimmed stdout when the command exited 0; else `false` + trimmed stderr — the shape
/// the merge sequence branches on.
fn run(dir: &Path, argv: &[String]) -> std::io::Result<(bool, String)> {
    let out = git(dir, argv)?;
    let text = if out.status.success() {
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    } else {
        String::from_utf8_lossy(&out.stderr).trim().to_string()
    };
    Ok((out.status.success(), text))
}

/// Parse `git worktree list --porcelain` for the worktree path that has `branch` checked out.
/// The porcelain format is blank-line-separated blocks of `key value` lines; a block with
/// `branch refs/heads/<name>` names the worktree on its `worktree <path>` line. The rig checkout
/// itself is one such block, so this also covers the legacy shared-checkout case (branch checked
/// out directly in the rig). Pure, so it is unit-tested against captured porcelain text.
fn worktree_for_branch(porcelain: &str, branch: &str) -> Option<PathBuf> {
    let want = format!("refs/heads/{branch}");
    let mut current: Option<&str> = None;
    for line in porcelain.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            current = Some(path);
        } else if let Some(b) = line.strip_prefix("branch ") {
            if b == want {
                return current.map(PathBuf::from);
            }
        } else if line.is_empty() {
            current = None;
        }
    }
    None
}

/// Run a non-git command, returning (success, output).
fn cmd(program: &str, args: &[&str], dir: &Path) -> std::io::Result<(bool, String)> {
    let out = Command::new(program).args(args).current_dir(dir).output()?;
    let text = if out.status.success() {
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    } else {
        String::from_utf8_lossy(&out.stderr).trim().to_string()
    };
    Ok((out.status.success(), text))
}

/// The outcome of a real merge attempt: the sha now on `main`, or a human reason for the failure.
type MergeOutcome = Result<String, String>;

/// Execute the real merge of `branch` into `main` from the `rig` checkout, per CLAUDE.md
/// flat-history: fetch, fast-forward push, and on divergence rebase the branch onto `origin/main`
/// and retry once. Returns the merged sha on success or a reason on conflict/error. Pure of the
/// domain — the caller maps the result onto the merge actor. Blocking (shells `git`); the plugin
/// runs it on a blocking thread.
fn merge_branch_to_main(rig: &Path, branch: &str) -> MergeOutcome {
    // 1) Refresh the remote tip so the ff check + any rebase see origin/main's latest.
    match run(rig, &fetch_argv()) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git fetch origin failed: {err}")),
        Err(e) => return Err(format!("git fetch origin error: {e}")),
    }

    // 2) Fast-forward push. The common case: the branch is already on top of origin/main.
    match run(rig, &push_argv(branch)) {
        Ok((true, _)) => return resolve_sha(rig, branch),
        Ok((false, _)) => {} // diverged → rebase + retry below
        Err(e) => return Err(format!("git push error: {e}")),
    }

    // 3) Diverged: rebase the branch onto the advanced origin/main, in the worktree that holds it.
    let porcelain = match run(rig, &worktree_list_argv()) {
        Ok((true, out)) => out,
        Ok((false, err)) => return Err(format!("git worktree list failed: {err}")),
        Err(e) => return Err(format!("git worktree list error: {e}")),
    };
    let Some(worktree) = worktree_for_branch(&porcelain, branch) else {
        return Err(format!(
            "branch {branch} diverged from main but no worktree holds it — cannot rebase"
        ));
    };
    match run(&worktree, &rebase_argv()) {
        Ok((true, _)) => {}
        Ok((false, err)) => {
            // Leave the worktree clean: unwind the conflicted rebase before reporting failure.
            let _ = run(&worktree, &rebase_abort_argv());
            return Err(format!("rebase onto origin/main conflicted: {err}"));
        }
        Err(e) => return Err(format!("git rebase error: {e}")),
    }

    // 4) Retry the now-fast-forwardable push.
    match run(rig, &push_argv(branch)) {
        Ok((true, _)) => resolve_sha(rig, branch),
        Ok((false, err)) => Err(format!("git push after rebase failed: {err}")),
        Err(e) => Err(format!("git push after rebase error: {e}")),
    }
}

/// CI-gated merge: push the branch, create a PR with auto-merge enabled.
/// Returns immediately with the PR URL — the actual merge happens when CI passes.
/// If CI fails, a GitHub webhook notifies orchd to re-dispatch.
fn merge_branch_via_pr(rig: &Path, branch: &str, bead: &str) -> MergeOutcome {
    // 1) Fetch so rebase has the latest origin/main.
    match run(rig, &fetch_argv()) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git fetch origin failed: {err}")),
        Err(e) => return Err(format!("git fetch origin error: {e}")),
    }

    // 2) Rebase onto origin/main so the PR is clean.
    let porcelain = match run(rig, &worktree_list_argv()) {
        Ok((true, out)) => out,
        Ok((false, err)) => return Err(format!("git worktree list failed: {err}")),
        Err(e) => return Err(format!("git worktree list error: {e}")),
    };
    if let Some(worktree) = worktree_for_branch(&porcelain, branch) {
        match run(&worktree, &rebase_argv()) {
            Ok((true, _)) => {}
            Ok((false, err)) => {
                let _ = run(&worktree, &rebase_abort_argv());
                return Err(format!("rebase onto origin/main conflicted: {err}"));
            }
            Err(e) => return Err(format!("git rebase error: {e}")),
        }
    }

    // 3) Push the branch to origin.
    match run(rig, &push_branch_argv(branch)) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git push origin {branch} failed: {err}")),
        Err(e) => return Err(format!("git push origin {branch} error: {e}")),
    }

    // 4) Create PR with auto-merge (rebase strategy for flat history).
    let title = format!("{bead}: {branch}");
    match cmd(
        "gh",
        &[
            "pr", "create",
            "--base", "main",
            "--head", branch,
            "--title", &title,
            "--body", &format!("Automated PR for bead `{bead}`.\n\nAuto-merge enabled — merges when CI passes."),
        ],
        rig,
    ) {
        Ok((true, url)) => {
            // Enable auto-merge with rebase strategy (flat history per CLAUDE.md).
            let _ = cmd("gh", &["pr", "merge", "--auto", "--rebase", &url], rig);
            eprintln!("[git-merge] {bead}: PR created with auto-merge → {url}");
            // Return the PR URL as the "sha" — the real sha comes when the PR merges.
            Ok(url)
        }
        Ok((false, err)) => {
            // PR might already exist (re-dispatch). Try to find it.
            match cmd("gh", &["pr", "view", branch, "--json", "url", "--jq", ".url"], rig) {
                Ok((true, url)) => {
                    let _ = cmd("gh", &["pr", "merge", "--auto", "--rebase", &url], rig);
                    eprintln!("[git-merge] {bead}: existing PR found, auto-merge enabled → {url}");
                    Ok(url)
                }
                _ => Err(format!("gh pr create failed: {err}")),
            }
        }
        Err(e) => Err(format!("gh pr create error: {e}")),
    }
}

/// The sha at the tip of `branch` (now on `main`) after a successful push.
fn resolve_sha(rig: &Path, branch: &str) -> MergeOutcome {
    match run(rig, &rev_parse_argv(branch)) {
        Ok((true, sha)) => Ok(sha),
        // The push landed; we just could not read the sha back. Report the push as merged with an
        // empty sha rather than flip a real success to failed.
        Ok((false, _)) => Ok(String::new()),
        Err(_) => Ok(String::new()),
    }
}

/// Poll a PR's status until it resolves (merged or CI-failed). Runs as a background task
/// spawned after creating a PR with auto-merge. Drives the merge slot to its terminal state
/// and, on failure, the `merge.failed.v1` event triggers re-dispatch through the reactor.
async fn poll_pr_until_resolved(merge: MergeHandle, rig: PathBuf, bead: String, branch: String) {
    const POLL_INTERVAL: Duration = Duration::from_secs(60);
    const MAX_POLLS: u32 = 30; // ~30 min max wait

    for attempt in 1..=MAX_POLLS {
        tokio::time::sleep(POLL_INTERVAL).await;
        let rig_c = rig.clone();
        let branch_c = branch.clone();
        let result = tokio::task::spawn_blocking(move || {
            cmd(
                "gh",
                &[
                    "pr", "view", &branch_c,
                    "--json", "state,mergeCommit,statusCheckRollup",
                ],
                &rig_c,
            )
        })
        .await;

        let Ok(Ok((true, json_str))) = result else {
            eprintln!("[ci-gate] {bead}: poll {attempt}/{MAX_POLLS} — gh pr view failed, retrying");
            continue;
        };

        let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) else {
            continue;
        };

        match v["state"].as_str() {
            Some("MERGED") => {
                let sha = v["mergeCommit"]
                    .get("oid")
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_string();
                eprintln!("[ci-gate] {bead}: PR merged @ {sha}");
                merge.complete(&bead, sha).await;
                return;
            }
            Some("CLOSED") => {
                eprintln!("[ci-gate] {bead}: PR closed without merge");
                merge.fail(&bead, "PR closed without merge").await;
                return;
            }
            _ => {}
        }

        // Check if any CI check failed (PR still open).
        if let Some(checks) = v["statusCheckRollup"].as_array() {
            let all_done = checks.iter().all(|c| {
                matches!(c["status"].as_str(), Some("COMPLETED"))
            });
            let any_failed = checks.iter().any(|c| {
                matches!(c["conclusion"].as_str(), Some("FAILURE") | Some("CANCELLED"))
            });
            if all_done && any_failed {
                let names: Vec<&str> = checks
                    .iter()
                    .filter(|c| matches!(c["conclusion"].as_str(), Some("FAILURE") | Some("CANCELLED")))
                    .filter_map(|c| c["name"].as_str())
                    .collect();
                let reason = format!("CI failed: {}", names.join(", "));
                eprintln!("[ci-gate] {bead}: {reason}");
                merge.fail(&bead, reason).await;
                return;
            }
        }

        eprintln!("[ci-gate] {bead}: poll {attempt}/{MAX_POLLS} — still pending");
    }

    eprintln!("[ci-gate] {bead}: gave up after {MAX_POLLS} polls");
    merge.fail(&bead, "CI gate timed out").await;
}

/// The git-merge edge-effect observer (`hq-orchd-deploy.12`). On `merge.started.v1` it resolves
/// the slot's branch from the live board and lands it on `main` from the rig checkout, then drives
/// the slot to `Merged`/`Failed` through the merge actor. Registered by the bin on the hub relay
/// (an edge effect, like the polecat sling — kept out of pure-state `daemon_root`).
pub struct GitMergePlugin {
    merge: MergeHandle,
    rig: PathBuf,
    /// Per-rig checkout paths keyed by bead prefix (`hq-c846f5`, epic hq-554308). A
    /// `merge.started.v1` for a bead whose prefix matches runs the ff-merge from THAT rig's
    /// checkout; an unknown prefix falls back to `rig` — legacy single-rig deployments
    /// (constructed via [`GitMergePlugin::new`]) are unchanged.
    rig_paths: HashMap<String, PathBuf>,
    /// When true, merges go through a PR with auto-merge instead of pushing directly to main.
    /// CI must pass before the PR merges. Failed CI triggers re-dispatch via webhook.
    ci_gated: bool,
}

impl GitMergePlugin {
    /// Wire the merge command handle (for branch resolution + the terminal transition) and the
    /// rig checkout the push runs from (`GT_RIG_PATH`).
    pub fn new(merge: MergeHandle, rig: PathBuf) -> Self {
        Self {
            merge,
            rig,
            rig_paths: HashMap::new(),
            ci_gated: false,
        }
    }

    /// Multi-rig constructor (`hq-c846f5`): route each merge to its rig checkout by bead
    /// prefix, falling back to `fallback_rig` for prefixes not in the map.
    pub fn with_rig_paths(
        merge: MergeHandle,
        rig_paths: HashMap<String, PathBuf>,
        fallback_rig: PathBuf,
    ) -> Self {
        Self {
            merge,
            rig: fallback_rig,
            rig_paths,
            ci_gated: false,
        }
    }

    /// Enable CI-gated merges: branches go through PRs with auto-merge instead of
    /// pushing directly to main. Set `GT_CI_GATED_MERGE=1` to activate.
    pub fn with_ci_gated(mut self, enabled: bool) -> Self {
        self.ci_gated = enabled;
        self
    }

    /// The checkout the ff-merge for `bead` runs from: its prefix's rig, else the fallback.
    fn rig_for(&self, bead: &str) -> &Path {
        self.rig_paths
            .get(bead_prefix(bead))
            .map(PathBuf::as_path)
            .unwrap_or(&self.rig)
    }
}

#[async_trait]
impl Plugin for GitMergePlugin {
    fn name(&self) -> &'static str {
        "git-merge"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind != "merge.started.v1" {
            return Ok(());
        }
        let MergeEvent::Started { bead } = record.decode::<MergeEvent>()? else {
            return Ok(());
        };
        // The `Started` event carries only the bead; read its branch from the live board (the slot
        // is in `Merging` and still carries it). Absent ⇒ nothing to merge, stay silent.
        let Some(branch) = self
            .merge
            .snapshot()
            .await
            .into_iter()
            .find(|s| s.bead == bead)
            .map(|s| s.branch)
        else {
            return Ok(());
        };

        // Shell git off the runtime workers; the result drives the terminal transition.
        // Routed by bead prefix (hq-c846f5): a gtweb bead's merge runs from the gtweb checkout.
        let rig = self.rig_for(&bead).to_path_buf();
        let branch_for_git = branch.clone();
        let ci_gated = self.ci_gated;
        let bead_for_git = bead.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            if ci_gated {
                merge_branch_via_pr(&rig, &branch_for_git, &bead_for_git)
            } else {
                merge_branch_to_main(&rig, &branch_for_git)
            }
        })
        .await
        .unwrap_or_else(|e| Err(format!("merge task panicked: {e}")));

        match outcome {
            Ok(ref sha_or_url) if ci_gated => {
                eprintln!("[git-merge] {bead}: PR submitted for CI gate → {sha_or_url}");
                let merge_for_poll = self.merge.clone();
                let rig_for_poll = self.rig_for(&bead).to_path_buf();
                let bead_for_poll = bead.clone();
                let branch_for_poll = branch.clone();
                tokio::spawn(poll_pr_until_resolved(
                    merge_for_poll,
                    rig_for_poll,
                    bead_for_poll,
                    branch_for_poll,
                ));
            }
            Ok(sha) => {
                eprintln!("[git-merge] {bead}: pushed {branch} → main @ {sha}");
                self.merge.complete(bead, sha).await;
            }
            Err(reason) => {
                eprintln!("[git-merge] {bead}: merge failed — {reason}");
                self.merge.fail(bead, reason).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_builders_are_flat_history() {
        assert_eq!(fetch_argv(), vec!["fetch", "origin"]);
        assert_eq!(
            push_argv("hq-x.1"),
            vec!["push", "origin", "hq-x.1:main"],
            "push is a ref-spec fast-forward of remote main, no merge commit"
        );
        assert_eq!(rebase_argv(), vec!["rebase", "origin/main"]);
        assert_eq!(rebase_abort_argv(), vec!["rebase", "--abort"]);
        assert_eq!(
            rev_parse_argv("hq-x.1"),
            vec!["rev-parse", "refs/heads/hq-x.1"]
        );
        assert_eq!(
            worktree_list_argv(),
            vec!["worktree", "list", "--porcelain"]
        );
    }

    #[test]
    fn worktree_for_branch_finds_the_holding_tree() {
        // Captured `git worktree list --porcelain`: the rig checkout on main + two per-bead trees.
        let porcelain = "\
worktree /rig
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /rig-wt/hq-pol-7
HEAD 2222222222222222222222222222222222222222
branch refs/heads/hq-orchd-deploy.12

worktree /rig-wt/hq-pol-8
HEAD 3333333333333333333333333333333333333333
branch refs/heads/hq-other.3
";
        assert_eq!(
            worktree_for_branch(porcelain, "hq-orchd-deploy.12"),
            Some(PathBuf::from("/rig-wt/hq-pol-7"))
        );
        assert_eq!(
            worktree_for_branch(porcelain, "main"),
            Some(PathBuf::from("/rig")),
            "legacy shared-checkout case: branch checked out directly in the rig"
        );
        assert_eq!(worktree_for_branch(porcelain, "hq-absent.9"), None);
    }

    #[tokio::test]
    async fn rig_for_routes_by_prefix_and_falls_back() {
        // hq-c846f5 (epic hq-554308): the merge for a gtweb bead runs from the gtweb checkout;
        // an unknown prefix keeps the legacy fallback rig.
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let merge = gt_merge::actor::spawn(gt_merge::InMemoryMergeRepo::default(), tx);
        let plugin = GitMergePlugin::with_rig_paths(
            merge,
            HashMap::from([("gtweb".to_string(), PathBuf::from("/rig-wt/gtweb"))]),
            PathBuf::from("/rig-wt/gtcore"),
        );
        assert_eq!(plugin.rig_for("gtweb-968172"), Path::new("/rig-wt/gtweb"));
        assert_eq!(plugin.rig_for("zz-1"), Path::new("/rig-wt/gtcore"));
        assert_eq!(
            plugin.rig_for("hq-mod-hooks.9"),
            Path::new("/rig-wt/gtcore"),
            "prefix is the FIRST dash token only"
        );
    }

    #[tokio::test]
    async fn merge_started_executes_ff_merge_from_routed_checkout() {
        // hq-c846f5: end-to-end — a merge.started.v1 for a gtweb-prefixed bead lands the branch
        // on origin/main FROM the gtweb checkout. The fallback rig is nonexistent, so only
        // correct routing can make the push succeed.
        use gt_events::Envelope;
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("gt-gitmerge-{uniq}"));
        let origin = root.join("origin.git");
        let gtweb = root.join("gtweb");
        std::fs::create_dir_all(&origin).unwrap();
        let sh = |dir: &Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?} in {}: {}",
                dir.display(),
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        sh(&origin, &["init", "-q", "--bare", "-b", "main"]);
        sh(&root, &["clone", "-q", origin.to_str().unwrap(), "gtweb"]);
        sh(&gtweb, &["config", "user.email", "t@t"]);
        sh(&gtweb, &["config", "user.name", "t"]);
        sh(&gtweb, &["checkout", "-q", "-b", "gtweb-1"]);
        std::fs::write(gtweb.join("f"), "x").unwrap();
        sh(&gtweb, &["add", "."]);
        sh(&gtweb, &["commit", "-qm", "work"]);
        let want_sha = sh(&gtweb, &["rev-parse", "gtweb-1"]);

        // A real merge actor whose board holds the slot (bead + branch) the plugin resolves.
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let merge = gt_merge::actor::spawn(gt_merge::InMemoryMergeRepo::default(), tx);
        merge.submit("gtweb-1", "gtweb-1", "01ABC").await;
        merge.start("gtweb-1").await;

        let plugin = GitMergePlugin::with_rig_paths(
            merge,
            HashMap::from([("gtweb".to_string(), gtweb.clone())]),
            root.join("nonexistent-fallback"),
        );
        let record = EventRecord::from_envelope(&Envelope::root(MergeEvent::Started {
            bead: "gtweb-1".into(),
        }))
        .unwrap();
        plugin.on_event(&record).await.unwrap();

        assert_eq!(
            sh(&origin, &["rev-parse", "main"]),
            want_sha,
            "the branch fast-forwarded origin/main from the ROUTED gtweb checkout"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn worktree_for_branch_handles_detached_blocks() {
        // A detached worktree has no `branch` line — it must not be mis-attributed to the next.
        let porcelain = "\
worktree /rig-wt/detached
HEAD 4444444444444444444444444444444444444444
detached

worktree /rig-wt/hq-pol-1
HEAD 5555555555555555555555555555555555555555
branch refs/heads/hq-z.1
";
        assert_eq!(
            worktree_for_branch(porcelain, "hq-z.1"),
            Some(PathBuf::from("/rig-wt/hq-pol-1"))
        );
        // The detached tree's HEAD is not a branch — nothing resolves to it.
        assert_eq!(worktree_for_branch(porcelain, ""), None);
    }
}
