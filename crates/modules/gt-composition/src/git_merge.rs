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

/// `git -C <rig> push --force-with-lease origin <branch>` argv — re-publishes a branch we just
/// rebased onto the advanced `origin/main` (strict-mode `BEHIND` recovery, gtcore-26a0bd). The
/// branch already exists on origin, so a plain push is rejected non-ff; `--force-with-lease` keeps
/// it safe against a concurrent writer (it refuses if origin moved under us).
fn push_force_branch_argv(branch: &str) -> Vec<String> {
    vec![
        "push".into(),
        "--force-with-lease".into(),
        "origin".into(),
        branch.into(),
    ]
}

/// `git -C <rig> rev-parse origin/main` argv — the sha now on `main` after a PR merged. Best-effort
/// reporting only (the real source of truth is GitHub); an empty sha is acceptable downstream.
fn rev_parse_origin_main_argv() -> Vec<String> {
    vec!["rev-parse".into(), "origin/main".into()]
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

/// Look up the worktree path that holds `branch`, returning `None` on any failure (the worktree
/// lookup is best-effort — callers that need the worktree for a hard step use their own error path).
fn find_worktree(rig: &Path, branch: &str) -> Option<PathBuf> {
    let (ok, porcelain) = run(rig, &worktree_list_argv()).ok()?;
    if !ok {
        return None;
    }
    worktree_for_branch(&porcelain, branch)
}

/// Auto-sync `Cargo.lock` when it drifts from the workspace's `Cargo.toml` set (gtcore-bfb203).
/// Runs `cargo generate-lockfile` in the worktree and, if the lock changed, commits it so the PR's
/// CI never fails with "cannot update the lock file because --locked was passed". Best-effort:
/// failures are logged and do not abort the merge — CI will catch any remaining desync.
fn cargo_lock_sync(worktree: &Path, branch: &str) {
    match cmd("cargo", &["generate-lockfile"], worktree) {
        Ok((true, _)) => {}
        Ok((false, err)) => {
            eprintln!("[git-merge] {branch}: cargo generate-lockfile failed: {err} — Cargo.lock may be unsync'd");
            return;
        }
        Err(e) => {
            // `cargo` not in PATH (e.g. an image without a Rust toolchain) — skip silently.
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("[git-merge] {branch}: cargo generate-lockfile I/O error: {e}");
            }
            return;
        }
    }
    // `git diff --quiet` exits 1 when there are unstaged changes, 0 when the tree is clean.
    let lock_dirty = matches!(
        run(worktree, &["diff".into(), "--quiet".into(), "--".into(), "Cargo.lock".into()]),
        Ok((false, _))
    );
    if !lock_dirty {
        return;
    }
    let _ = run(worktree, &["add".into(), "Cargo.lock".into()]);
    match run(worktree, &["commit".into(), "-m".into(), "chore: sync Cargo.lock (auto, gtcore-bfb203)".into()]) {
        Ok((true, _)) => eprintln!("[git-merge] {branch}: Cargo.lock synced and committed"),
        Ok((false, err)) => eprintln!("[git-merge] {branch}: Cargo.lock commit failed: {err}"),
        Err(e) => eprintln!("[git-merge] {branch}: Cargo.lock commit I/O error: {e}"),
    }
}

/// The outcome of a real merge attempt: the sha now on `main`, or a human reason for the failure.
type MergeOutcome = Result<String, String>;

/// What a `merge.started.v1` merge attempt resolved to, unifying the direct-push and CI-gated PR
/// paths so [`GitMergePlugin`]'s event handler drives the slot's terminal transition from one match.
enum MergeResult {
    /// The branch is on `main` now — either a direct fast-forward push (non-CI-gated), or a
    /// CI-gated PR that had no blocking required checks and so was merged immediately
    /// (gtcore-26a0bd). Carries the merged sha (best-effort; may be empty). The plugin calls
    /// [`MergeHandle::complete`].
    Merged(String),
    /// A CI-gated PR is up with auto-merge enabled and required checks still pending; the slot
    /// stays `Merging` until the GitHub → MCP-server → `/ci-gate/{merged,failed}` webhook drives
    /// the terminal transition. Carries the PR URL (informational). The plugin leaves the slot.
    AwaitingCi(String),
}

/// What to do with a freshly-created auto-merge PR, decided purely from GitHub's computed merge
/// state (gtcore-26a0bd). Separating the decision from the I/O keeps it unit-testable.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum GateAction {
    /// The PR is already merged (idempotent redelivery, or auto-merge raced ahead of us) — report
    /// success without touching git.
    AlreadyMerged,
    /// Mergeable with no blocking required checks (`CLEAN`) — complete the merge directly now.
    /// This is the gt-app-proxy case: a repo with no required CI never delivers the webhook, so
    /// without this the slot would wait forever.
    MergeNow,
    /// Strict-mode out-of-date (`BEHIND`): a sibling merge advanced `main` after we pushed — rebase
    /// onto `origin/main`, force-push, and re-evaluate.
    Rebase,
    /// Required checks/reviews still pending (`BLOCKED`/`UNSTABLE`/`HAS_HOOKS`/`DRAFT`/…) — enable
    /// auto-merge and let the CI-gate webhook complete the slot, exactly as before. This is the
    /// gt-core path (required `build-test`/`contract`): direct-merging here would bypass CI.
    AwaitCi,
    /// Conflicts (`DIRTY`/`CONFLICTING`) — the PR cannot merge; report failure.
    Conflict,
    /// GitHub has not finished computing mergeability (`UNKNOWN`) — poll again shortly.
    Pending,
}

/// Decide what to do with an open auto-merge PR from its GitHub `state`, `mergeStateStatus`, and
/// `mergeable`. Pure (string fields in, action out) so the gate is unit-tested without `gh`.
///
/// The crux (gtcore-26a0bd): only `CLEAN` is merged directly. `CLEAN` means mergeable AND every
/// required status check is satisfied (or none are configured) AND the branch is up to date — so
/// merging now never skips a required check. A repo with required checks that have not yet reported
/// is `BLOCKED`, not `CLEAN`, so the gt-core webhook flow is preserved untouched.
fn gate_action(state: &str, merge_state_status: &str, mergeable: &str) -> GateAction {
    // A merged PR is terminal regardless of the (now-stale) merge-state fields.
    if state.eq_ignore_ascii_case("MERGED") {
        return GateAction::AlreadyMerged;
    }
    // Conflicts dominate: a DIRTY/CONFLICTING PR can never merge, whatever the rollup says.
    if mergeable.eq_ignore_ascii_case("CONFLICTING")
        || merge_state_status.eq_ignore_ascii_case("DIRTY")
    {
        return GateAction::Conflict;
    }
    // Mergeability not yet computed by GitHub — caller polls again.
    if mergeable.eq_ignore_ascii_case("UNKNOWN") || mergeable.is_empty() {
        return GateAction::Pending;
    }
    match merge_state_status.to_ascii_uppercase().as_str() {
        "CLEAN" => GateAction::MergeNow,
        "BEHIND" => GateAction::Rebase,
        "UNKNOWN" | "" => GateAction::Pending,
        // BLOCKED, UNSTABLE, HAS_HOOKS, DRAFT, and any state GitHub adds later → defer to the
        // webhook. Conservative by construction: an unrecognized state never direct-merges.
        _ => GateAction::AwaitCi,
    }
}

/// Parse the space-joined `state mergeStateStatus mergeable` line produced by
/// `gh pr view --json state,mergeStateStatus,mergeable --jq '[...] | join(" ")'`. Missing fields
/// default to `UNKNOWN` so a partial/empty read maps to [`GateAction::Pending`] (poll again), never
/// to a spurious merge. Pure, so it is unit-tested against captured `gh` output.
fn parse_pr_state(line: &str) -> (String, String, String) {
    let mut it = line.split_whitespace();
    let state = it.next().unwrap_or("UNKNOWN").to_string();
    let mss = it.next().unwrap_or("UNKNOWN").to_string();
    let mergeable = it.next().unwrap_or("UNKNOWN").to_string();
    (state, mss, mergeable)
}

/// Bounded settle budget while GitHub computes mergeability after PR creation (gtcore-26a0bd). This
/// waits only for the (fast) mergeability computation, NOT for CI to run — distinct from the old
/// `poll_pr_until_resolved` loop removed in gtcore-52c9ec.
const GATE_SETTLE_TRIES: u32 = 15;
/// Delay between settle polls. `GATE_SETTLE_TRIES * GATE_SETTLE_DELAY` bounds the wait (~30s).
const GATE_SETTLE_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
/// Cap on strict-mode `BEHIND` rebase+force-push retries before deferring to auto-merge, so a busy
/// merge queue cannot spin us force-pushing indefinitely.
const MAX_REBASE_RETRIES: u32 = 3;

/// `git -C <rig> merge-base --is-ancestor <branch> origin/main` argv — exits 0 when `branch`
/// is already an ancestor of `origin/main` (merged via another path), exits 1 otherwise.
/// Used to short-circuit the merge for `ahead=0` branches (gtcore-e34546).
fn already_merged_argv(branch: &str) -> Vec<String> {
    vec![
        "merge-base".into(),
        "--is-ancestor".into(),
        branch.into(),
        "origin/main".into(),
    ]
}

/// Execute the real merge of `branch` into `main` from the `rig` checkout, per CLAUDE.md
/// flat-history: fetch, fast-forward push, and on divergence rebase the branch onto `origin/main`
/// and retry once. Returns the merged sha on success or a reason on conflict/error. Pure of the
/// domain — the caller maps the result onto the merge actor. Blocking (shells `git`); the plugin
/// runs it on a blocking thread.
fn merge_branch_to_main(rig: &Path, branch: &str) -> Result<MergeResult, String> {
    // 1) Refresh the remote tip so the ff check + any rebase see origin/main's latest.
    match run(rig, &fetch_argv()) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git fetch origin failed: {err}")),
        Err(e) => return Err(format!("git fetch origin error: {e}")),
    }

    // 1a) Short-circuit: branch already an ancestor of origin/main (gtcore-e34546).
    // Work arrived via another path (cherry-pick, another PR) — report merged immediately
    // rather than failing with a non-ff conflict and looping through the sheriff.
    if let Ok((true, _)) = run(rig, &already_merged_argv(branch)) {
        eprintln!("[git-merge] {branch}: already merged into origin/main — marking complete");
        let sha = match run(rig, &rev_parse_origin_main_argv()) {
            Ok((true, s)) => s,
            _ => String::new(),
        };
        return Ok(MergeResult::Merged(sha));
    }

    // 1b) Sync Cargo.lock before pushing (gtcore-bfb203): regenerate and commit if drifted.
    if let Some(wt) = find_worktree(rig, branch) {
        cargo_lock_sync(&wt, branch);
    }

    // 2) Fast-forward push. The common case: the branch is already on top of origin/main.
    match run(rig, &push_argv(branch)) {
        Ok((true, _)) => return resolve_sha(rig, branch).map(MergeResult::Merged),
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

    // 3b) Sync Cargo.lock again after rebase — a rebase may introduce new Cargo.toml changes.
    cargo_lock_sync(&worktree, branch);

    // 4) Retry the now-fast-forwardable push.
    match run(rig, &push_argv(branch)) {
        Ok((true, _)) => resolve_sha(rig, branch).map(MergeResult::Merged),
        Ok((false, err)) => Err(format!("git push after rebase failed: {err}")),
        Err(e) => Err(format!("git push after rebase error: {e}")),
    }
}

/// CI-gated merge (gtcore-26a0bd). Rebase, push the branch, open a PR, then resolve it from
/// GitHub's computed merge state rather than blindly enabling auto-merge and waiting:
///
/// - **No blocking required checks** (`CLEAN`) → merge the PR directly now and return
///   [`MergeResult::Merged`]. This is the fix: a repo with no required CI (gt-app-proxy has no
///   `.github/workflows`) never delivers the `/ci-gate` webhook, so the old always-await path left
///   such PRs `CLEAN` forever (the operator merged them by hand).
/// - **Required checks pending** (`BLOCKED`/`UNSTABLE`/…) → enable auto-merge and return
///   [`MergeResult::AwaitingCi`]; the GitHub→MCP-server→`/ci-gate` webhook completes the slot, exactly
///   as gt-core does today (required `build-test`/`contract` are never bypassed).
/// - **Strict-mode `BEHIND`** → rebase onto the advanced `origin/main`, force-push, and re-evaluate.
///
/// Idempotent: a re-dispatch re-uses the existing PR, and an already-`MERGED` PR reports success
/// without touching git. Blocking (shells `git`/`gh`); runs on the plugin's blocking thread.
fn merge_branch_via_pr(rig: &Path, branch: &str, bead: &str) -> Result<MergeResult, String> {
    // 1) Fetch so rebase has the latest origin/main.
    match run(rig, &fetch_argv()) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git fetch origin failed: {err}")),
        Err(e) => return Err(format!("git fetch origin error: {e}")),
    }

    // 1a) Short-circuit: branch already an ancestor of origin/main (gtcore-e34546).
    if let Ok((true, _)) = run(rig, &already_merged_argv(branch)) {
        eprintln!("[git-merge] {bead}: branch {branch} already merged into origin/main — marking complete");
        let sha = match run(rig, &rev_parse_origin_main_argv()) {
            Ok((true, s)) => s,
            _ => String::new(),
        };
        return Ok(MergeResult::Merged(sha));
    }

    // 2) Rebase onto origin/main so the PR is clean.
    rebase_branch_onto_main(rig, branch)?;

    // 2b) Sync Cargo.lock after rebase (gtcore-bfb203).
    if let Some(wt) = find_worktree(rig, branch) {
        cargo_lock_sync(&wt, branch);
    }

    // 3) Push the branch to origin.
    match run(rig, &push_branch_argv(branch)) {
        Ok((true, _)) => {}
        Ok((false, err)) => return Err(format!("git push origin {branch} failed: {err}")),
        Err(e) => return Err(format!("git push origin {branch} error: {e}")),
    }

    // 4) Create the PR (auto-merge is NOT enabled yet — we enable it only on the AwaitCi branch, so
    //    a CLEAN PR cannot race auto-merge against our direct merge below). A re-dispatch finds the
    //    existing PR instead.
    let url = open_pr(rig, branch, bead)?;

    // 5) Resolve the PR from GitHub's computed merge state. Poll briefly while mergeability is still
    //    UNKNOWN (a fast computation, not CI), rebase on strict-BEHIND, then act on the verdict.
    let mut rebases = 0u32;
    for _ in 0..GATE_SETTLE_TRIES {
        let (state, mss, mergeable) = read_pr_state(rig, branch);
        match gate_action(&state, &mss, &mergeable) {
            GateAction::AlreadyMerged => {
                eprintln!("[git-merge] {bead}: PR already merged → {url}");
                return Ok(MergeResult::Merged(merged_main_sha(rig)));
            }
            GateAction::MergeNow => {
                eprintln!("[git-merge] {bead}: PR is CLEAN with no required checks — merging directly → {url}");
                return complete_pr_merge(rig, branch, &url, bead);
            }
            GateAction::Rebase => {
                if rebases >= MAX_REBASE_RETRIES {
                    // The queue keeps re-staling us; hand off to auto-merge so GitHub serializes it.
                    enable_auto_merge(rig, &url);
                    eprintln!("[git-merge] {bead}: PR still BEHIND after {rebases} rebases — auto-merge enabled → {url} (awaiting webhook)");
                    return Ok(MergeResult::AwaitingCi(url));
                }
                rebases += 1;
                rebase_branch_onto_main(rig, branch)?;
                match run(rig, &push_force_branch_argv(branch)) {
                    Ok((true, _)) => {}
                    Ok((false, err)) => {
                        return Err(format!("git push --force-with-lease origin {branch} failed: {err}"))
                    }
                    Err(e) => return Err(format!("git push --force-with-lease error: {e}")),
                }
                // Loop re-reads the (recomputing) state.
            }
            GateAction::Conflict => {
                return Err(format!("PR for {branch} is not mergeable (conflicts) → {url}"))
            }
            GateAction::AwaitCi => {
                enable_auto_merge(rig, &url);
                eprintln!("[git-merge] {bead}: PR has required checks pending — auto-merge enabled → {url} (awaiting webhook)");
                return Ok(MergeResult::AwaitingCi(url));
            }
            GateAction::Pending => std::thread::sleep(GATE_SETTLE_DELAY),
        }
    }

    // Settle budget exhausted with mergeability still UNKNOWN: enable auto-merge and defer to the
    // webhook rather than risk a wrong direct merge.
    enable_auto_merge(rig, &url);
    eprintln!("[git-merge] {bead}: merge state unresolved after settle — auto-merge enabled → {url} (awaiting webhook)");
    Ok(MergeResult::AwaitingCi(url))
}

/// Rebase `branch` onto `origin/main` in the worktree that holds it (a no-op for the legacy
/// shared-checkout case where no per-bead worktree exists). Aborts a conflicted rebase to leave the
/// worktree clean before returning the error.
fn rebase_branch_onto_main(rig: &Path, branch: &str) -> Result<(), String> {
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
    Ok(())
}

/// Open the PR for `branch` (no auto-merge yet) and return its URL. On `gh pr create` failure the PR
/// usually already exists (re-dispatch) — fall back to `gh pr view` to recover the URL.
fn open_pr(rig: &Path, branch: &str, bead: &str) -> Result<String, String> {
    let title = format!("{bead}: {branch}");
    let body = format!(
        "Automated PR for bead `{bead}`.\n\nMerged by the refinery once mergeable (directly if no required checks, else via auto-merge when CI passes)."
    );
    match cmd(
        "gh",
        &[
            "pr", "create", "--base", "main", "--head", branch, "--title", &title, "--body", &body,
        ],
        rig,
    ) {
        Ok((true, url)) => {
            eprintln!("[git-merge] {bead}: PR created → {url}");
            Ok(url)
        }
        Ok((false, err)) => match cmd(
            "gh",
            &["pr", "view", branch, "--json", "url", "--jq", ".url"],
            rig,
        ) {
            Ok((true, url)) if !url.is_empty() => {
                eprintln!("[git-merge] {bead}: existing PR found → {url}");
                Ok(url)
            }
            _ => Err(format!("gh pr create failed: {err}")),
        },
        Err(e) => Err(format!("gh pr create error: {e}")),
    }
}

/// Read a PR's `(state, mergeStateStatus, mergeable)` from GitHub. A failed/empty `gh` read yields
/// all-`UNKNOWN`, which [`gate_action`] maps to [`GateAction::Pending`] (poll again) — never a merge.
fn read_pr_state(rig: &Path, branch: &str) -> (String, String, String) {
    match cmd(
        "gh",
        &[
            "pr",
            "view",
            branch,
            "--json",
            "state,mergeStateStatus,mergeable",
            "--jq",
            "[.state,.mergeStateStatus,.mergeable]|join(\" \")",
        ],
        rig,
    ) {
        Ok((true, line)) => parse_pr_state(&line),
        _ => (
            "UNKNOWN".to_string(),
            "UNKNOWN".to_string(),
            "UNKNOWN".to_string(),
        ),
    }
}

/// Enable auto-merge (rebase strategy, flat history per CLAUDE.md) so GitHub merges the PR once its
/// required checks pass. Best-effort: a failure here just means the webhook path is the only driver.
fn enable_auto_merge(rig: &Path, url: &str) {
    let _ = cmd("gh", &["pr", "merge", "--auto", "--rebase", url], rig);
}

/// Merge a CLEAN PR directly (rebase strategy, delete the branch). Idempotent against the auto-merge
/// race: if the immediate merge is refused but the PR is in fact already merged, report success.
fn complete_pr_merge(rig: &Path, branch: &str, url: &str, bead: &str) -> Result<MergeResult, String> {
    match cmd(
        "gh",
        &["pr", "merge", branch, "--rebase", "--delete-branch"],
        rig,
    ) {
        Ok((true, _)) => {
            eprintln!("[git-merge] {bead}: PR merged directly → {url}");
            Ok(MergeResult::Merged(merged_main_sha(rig)))
        }
        Ok((false, err)) => {
            // A racing auto-merge or a prior delivery may already have merged it.
            let (state, _, _) = read_pr_state(rig, branch);
            if state.eq_ignore_ascii_case("MERGED") {
                eprintln!("[git-merge] {bead}: PR already merged (race) → {url}");
                Ok(MergeResult::Merged(merged_main_sha(rig)))
            } else {
                Err(format!("gh pr merge --rebase failed: {err}"))
            }
        }
        Err(e) => Err(format!("gh pr merge error: {e}")),
    }
}

/// Best-effort sha now on `main` after a PR merged: fetch and read `origin/main`. Empty on any
/// failure — the merge already landed (GitHub is the source of truth), so we never flip success to
/// failed over an unreadable sha.
fn merged_main_sha(rig: &Path) -> String {
    let _ = run(rig, &fetch_argv());
    match run(rig, &rev_parse_origin_main_argv()) {
        Ok((true, sha)) => sha,
        _ => String::new(),
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
            Ok(MergeResult::AwaitingCi(url)) => {
                // The CI-gated PR is up with auto-merge enabled and required checks pending; the slot
                // stays `Merging`. The terminal transition is not polled here (gtcore-52c9ec) — GitHub
                // delivers `pull_request.closed` / `check_suite.completed` to the MCP-server webhook,
                // which forwards `/ci-gate/{merged,failed}` to orchd to `complete`/`fail` the slot.
                eprintln!("[git-merge] {bead}: PR submitted for CI gate → {url} (awaiting webhook)");
            }
            Ok(MergeResult::Merged(sha)) => {
                // Branch is on `main` now: a direct ff-push (non-CI-gated), or a CI-gated PR with no
                // blocking required checks merged immediately (gtcore-26a0bd). Drive the slot terminal.
                eprintln!("[git-merge] {bead}: {branch} → main @ {sha}");
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
        assert_eq!(
            already_merged_argv("gtcore-00325f"),
            vec!["merge-base", "--is-ancestor", "gtcore-00325f", "origin/main"],
            "exits 0 when branch is already an ancestor of main"
        );
        assert_eq!(
            push_force_branch_argv("hq-x.1"),
            vec!["push", "--force-with-lease", "origin", "hq-x.1"],
            "BEHIND recovery force-pushes the rebased branch (lease-guarded), not branch:main"
        );
        assert_eq!(rev_parse_origin_main_argv(), vec!["rev-parse", "origin/main"]);
    }

    #[test]
    fn gate_action_merges_clean_prs_directly() {
        // The gt-app-proxy case: no required checks → CLEAN → merge now (don't wait for a webhook
        // that never arrives). Case-insensitive on the enum spellings gh emits.
        assert_eq!(
            gate_action("OPEN", "CLEAN", "MERGEABLE"),
            GateAction::MergeNow
        );
        assert_eq!(
            gate_action("open", "clean", "mergeable"),
            GateAction::MergeNow
        );
    }

    #[test]
    fn gate_action_defers_pending_required_checks_to_webhook() {
        // The gt-core case: required build-test/contract not yet satisfied → BLOCKED → auto-merge +
        // webhook, never a direct merge (that would bypass CI). UNSTABLE/HAS_HOOKS/DRAFT likewise.
        for mss in ["BLOCKED", "UNSTABLE", "HAS_HOOKS", "DRAFT", "SOMETHING_NEW"] {
            assert_eq!(
                gate_action("OPEN", mss, "MERGEABLE"),
                GateAction::AwaitCi,
                "mergeStateStatus {mss} must defer to the CI-gate webhook"
            );
        }
    }

    #[test]
    fn gate_action_rebases_strict_behind() {
        // strict-mode: a sibling merge advanced main → BEHIND → rebase + re-evaluate.
        assert_eq!(gate_action("OPEN", "BEHIND", "MERGEABLE"), GateAction::Rebase);
    }

    #[test]
    fn gate_action_reports_conflicts() {
        // Conflicts dominate regardless of the rollup status.
        assert_eq!(
            gate_action("OPEN", "DIRTY", "CONFLICTING"),
            GateAction::Conflict
        );
        assert_eq!(
            gate_action("OPEN", "CLEAN", "CONFLICTING"),
            GateAction::Conflict,
            "a CONFLICTING mergeable can never merge even if the rollup hasn't caught up"
        );
    }

    #[test]
    fn gate_action_polls_while_unknown() {
        // GitHub computes mergeability async after PR creation — poll, never act on UNKNOWN.
        assert_eq!(gate_action("OPEN", "UNKNOWN", "UNKNOWN"), GateAction::Pending);
        assert_eq!(
            gate_action("OPEN", "CLEAN", "UNKNOWN"),
            GateAction::Pending,
            "mergeable not yet computed ⇒ poll again, do not direct-merge"
        );
        assert_eq!(gate_action("OPEN", "", ""), GateAction::Pending);
    }

    #[test]
    fn gate_action_already_merged_is_terminal() {
        // Idempotent redelivery / auto-merge raced ahead: MERGED short-circuits the stale fields.
        assert_eq!(
            gate_action("MERGED", "UNKNOWN", "UNKNOWN"),
            GateAction::AlreadyMerged
        );
        assert_eq!(
            gate_action("MERGED", "CLEAN", "MERGEABLE"),
            GateAction::AlreadyMerged
        );
    }

    #[test]
    fn parse_pr_state_splits_three_fields_and_defaults_missing() {
        assert_eq!(
            parse_pr_state("OPEN CLEAN MERGEABLE"),
            ("OPEN".into(), "CLEAN".into(), "MERGEABLE".into())
        );
        // A short/empty read defaults to UNKNOWN ⇒ Pending, never a spurious merge.
        assert_eq!(
            parse_pr_state("MERGED"),
            ("MERGED".into(), "UNKNOWN".into(), "UNKNOWN".into())
        );
        assert_eq!(
            parse_pr_state(""),
            ("UNKNOWN".into(), "UNKNOWN".into(), "UNKNOWN".into())
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

    /// A5 (gtcore-f3a016) — **the merge queue is the only path to `main`**, from the edge's
    /// side. `GitMergePlugin` is the sole code that runs `git push origin <branch>:main`, and it
    /// fires EXCLUSIVELY on `merge.started.v1` — the event the merge actor emits when the queue
    /// admits a slot (`Ready → Merging`). Any other event is a no-op: there is no path that pushes
    /// to main without the queue's `Started` signal. We feed `merge.ready.v1` (which carries the
    /// very same bead+branch a naive impl might merge directly) and an unrelated `merge.merged.v1`,
    /// and assert the slot is never advanced off-queue — the edge never touched git.
    #[tokio::test]
    async fn only_merge_started_drives_a_push_to_main() {
        use gt_events::Envelope;

        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let merge = gt_merge::actor::spawn(gt_merge::InMemoryMergeRepo::default(), tx);
        // A slot sitting in the queue at `Ready` — submitted but NOT yet started.
        merge.submit("hq-x.1", "hq-x.1", "01ABC").await;
        // A nonexistent rig: were the edge to (wrongly) run git, the push would fail and flip the
        // slot to `Failed` — so "still Ready" proves the edge stayed its hand.
        let plugin = GitMergePlugin::new(merge, PathBuf::from("/nonexistent-rig-a5"));

        // `merge.ready.v1` — the queue OBSERVATION, not the admit. Must NOT trigger a merge.
        let ready = EventRecord::from_envelope(&Envelope::root(MergeEvent::Ready {
            bead: "hq-x.1".into(),
            branch: "hq-x.1".into(),
            channel_msg_id: "01ABC".into(),
        }))
        .unwrap();
        plugin.on_event(&ready).await.unwrap();

        // An unrelated terminal event likewise no-ops.
        let merged = EventRecord::from_envelope(&Envelope::root(MergeEvent::Merged {
            bead: "hq-x.1".into(),
            sha: "deadbeef".into(),
        }))
        .unwrap();
        plugin.on_event(&merged).await.unwrap();

        // The slot is untouched: still `Ready` in the queue. No off-queue event advanced it, so
        // the only path to main remains the queue's `Started`.
        let snap = plugin.merge.snapshot().await;
        let slot = snap.iter().find(|s| s.bead == "hq-x.1").expect("slot present");
        assert_eq!(
            slot.state,
            gt_merge::MergeSlotState::Ready,
            "non-Started events never advance the slot — the queue is the only path to main"
        );
    }
}
