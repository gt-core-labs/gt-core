//! Continuation re-sling on context exhaustion (gtcore-3b2a68).
//!
//! When the polecat supervisor reports a death by context exhaustion
//! ([`WatchOutcome::ContextExhausted`](gt_polecat) — surfaced to the composition layer as an
//! `agent.killed.v1` whose reason starts with `context exhausted`, gtcore-91fdde), the bead is
//! NOT failed and NOT bounced back to `open`: the previous agent checkpointed its progress into
//! the bead notes (gtcore-2467b4) and left its work-in-progress committed on the branch. The
//! right recovery is to re-sling the SAME bead — still `working` — with a *continuation prompt*
//! that hands the next agent everything it needs to pick up where the last one stopped: the
//! checkpoint summary, the branch diff, and the acceptance criteria.
//!
//! This module is the pure, side-effect-light core of that recovery: parsing the checkpoint
//! notes, reading the branch diff via `git`, and assembling the continuation prompt. The event
//! wiring + the actual re-sling live in [`crate::polecat`].

use std::path::Path;
use std::process::Command;

/// Hard cap on the embedded `git diff` (bytes). A polecat that worked for a full context window
/// can leave a very large diff; the whole thing would blow the agent's kickoff prompt (and the
/// argv tmux passes), so the diff is truncated to this budget with a trailing marker. 24 KiB is
/// generous enough to orient the continuator while staying well under any argv limit.
pub const MAX_DIFF_BYTES: usize = 24 * 1024;

/// Hard cap on the embedded CI-failure log (bytes). The orchd snapshots a best-effort summary of
/// the failing PR checks ([`read_ci_checks`]) into the re-sling prompt; like the diff it is
/// truncated so the kickoff argv stays bounded. Smaller than the diff budget — the summary is a
/// pointer (which checks failed + how to pull the full log), not the whole CI output.
pub const MAX_CI_LOG_BYTES: usize = 8 * 1024;

/// The structured checkpoint a previous agent left in the bead notes (gtcore-2467b4). The
/// checkpoint protocol writes a `## Checkpoint` block with `### Completado` / `### Pendiente`
/// sub-sections; this is the parsed form the continuation prompt renders back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Body of the `### Completado` section — what the previous agent finished.
    pub completado: String,
    /// Body of the `### Pendiente` section — what is left to do.
    pub pendiente: String,
}

/// Parse the most-recent `## Checkpoint` block out of a bead's notes (gtcore-2467b4 format):
///
/// ```text
/// ## Checkpoint
/// ### Completado
/// - …
/// ### Pendiente
/// - …
/// ```
///
/// Notes accumulate (each checkpoint is appended), so the **last** `## Checkpoint` heading wins —
/// it is the freshest. Section bodies run until the next `###`/`##` heading. Returns `None` when
/// the notes carry no checkpoint block at all, so the caller can fall back to the raw notes.
pub fn parse_checkpoint(notes: &str) -> Option<Checkpoint> {
    // Find the last `## Checkpoint` heading (case-insensitive on the label), then scope parsing
    // to the lines after it so an earlier, stale checkpoint never shadows the latest.
    let lines: Vec<&str> = notes.lines().collect();
    let start = lines
        .iter()
        .rposition(|l| is_heading(l, 2, "checkpoint"))?;

    let mut completado: Vec<&str> = Vec::new();
    let mut pendiente: Vec<&str> = Vec::new();
    // 0 = before any sub-section, 1 = inside Completado, 2 = inside Pendiente.
    let mut section = 0u8;
    for line in &lines[start + 1..] {
        if is_heading(line, 2, "") {
            // A new top-level `##` block ends this checkpoint.
            break;
        }
        if is_heading(line, 3, "completado") {
            section = 1;
            continue;
        }
        if is_heading(line, 3, "pendiente") {
            section = 2;
            continue;
        }
        if is_heading(line, 3, "") {
            // Some other `###` sub-section — stop collecting into either bucket.
            section = 0;
            continue;
        }
        match section {
            1 => completado.push(line),
            2 => pendiente.push(line),
            _ => {}
        }
    }

    Some(Checkpoint {
        completado: trim_block(&completado),
        pendiente: trim_block(&pendiente),
    })
}

/// Is `line` a Markdown heading of exactly `level` (`#` count) whose text, lowercased and
/// trimmed, equals `label`? An empty `label` matches any heading at that level (used to detect
/// "some other heading ends the section").
fn is_heading(line: &str, level: usize, label: &str) -> bool {
    let trimmed = line.trim_start();
    let hashes = trimmed.chars().take_while(|&c| c == '#').count();
    if hashes != level {
        return false;
    }
    let rest = trimmed[hashes..].trim();
    // Reject `####…` (deeper heading) — the char after the run must not be another `#`.
    if rest.starts_with('#') {
        return false;
    }
    if label.is_empty() {
        return true;
    }
    rest.eq_ignore_ascii_case(label)
}

/// Join collected section lines and trim surrounding blank lines, leaving the inner text intact.
fn trim_block(lines: &[&str]) -> String {
    lines.join("\n").trim_matches('\n').to_string()
}

/// Read the work the previous agent already committed on this branch: `git diff <base>...HEAD`
/// (three-dot — changes on the branch since it diverged from `base`), prefixed with the
/// `--stat` summary for orientation. Runs in `workdir` (the bead's worktree). Best-effort: a
/// `git` failure (no such ref, not a repo) yields an empty string, so a missing diff degrades
/// the continuation prompt rather than aborting the re-sling. The result is truncated to
/// [`MAX_DIFF_BYTES`].
pub fn read_branch_diff(workdir: &Path, base: &str) -> String {
    let range = format!("{base}...HEAD");
    let stat = git_output(workdir, &["diff", "--stat", &range]);
    let patch = git_output(workdir, &["diff", &range]);
    let mut out = String::new();
    let stat = stat.trim();
    if !stat.is_empty() {
        out.push_str(stat);
        out.push_str("\n\n");
    }
    out.push_str(patch.trim_end());
    truncate_on_char_boundary(out.trim(), MAX_DIFF_BYTES)
}

/// Read a best-effort summary of the failing PR checks for `branch`, to embed in the CI-fix
/// re-sling prompt (gtcore-3a1bd4). Shells `gh pr checks <branch>` in `workdir` (the bead's
/// worktree, where the rig's authenticated `gh` works) and returns its output, truncated to
/// [`MAX_CI_LOG_BYTES`]. Best-effort: `gh` missing / unauthenticated / no PR yields an empty
/// string, so the prompt degrades to the failure reason + the self-fetch instructions rather than
/// aborting the re-sling. This is a POINTER, not the whole CI log — the prompt tells the agent to
/// pull the full failing job output itself (`gh run view --log-failed`), which is fresher and
/// unbounded by the kickoff argv.
pub fn read_ci_checks(workdir: &Path, branch: &str) -> String {
    let out = Command::new("gh")
        .arg("pr")
        .arg("checks")
        .arg(branch)
        .current_dir(workdir)
        .output();
    let text = match out {
        // `gh pr checks` exits non-zero when any check failed — exactly our case — so the output is
        // useful on BOTH success and failure exit; take stdout, falling back to stderr if empty.
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout).into_owned();
            if stdout.trim().is_empty() {
                String::from_utf8_lossy(&o.stderr).into_owned()
            } else {
                stdout
            }
        }
        Err(_) => String::new(),
    };
    truncate_on_char_boundary(text.trim(), MAX_CI_LOG_BYTES)
}

/// Run `git -C <workdir> <args…>` and return stdout (lossy UTF-8), or an empty string on any
/// failure (spawn error or non-zero exit).
fn git_output(workdir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(args)
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
        _ => String::new(),
    }
}

/// Truncate `s` to at most `max` bytes without splitting a UTF-8 char, appending a marker when
/// anything was dropped so the continuator knows the diff is partial.
fn truncate_on_char_boundary(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n\n… [diff truncated at {} KiB — inspect the branch directly for the full change]",
        &s[..end],
        max / 1024
    )
}

/// Assemble the continuation prompt handed to the re-slung agent (gtcore-3b2a68).
///
/// The prompt tells the next agent it is *continuing*, not restarting, and gives it the three
/// things the AC requires: the previous agent's checkpoint summary (parsed from `notes`, or the
/// raw notes when no `## Checkpoint` block is present), the branch `diff`, and the bead's
/// `acceptance_criteria`. It closes with the same completion signal the original sling prompt
/// carries ([`polecat_prompt`](gt_polecat::polecat_prompt)) so the continuator knows how to mark
/// the bead done. `branch` follows the repo convention of equalling the bead id.
pub fn build_continuation_prompt(
    bead: &str,
    notes: &str,
    diff: &str,
    acceptance_criteria: &str,
) -> String {
    let mut p = String::new();
    p.push_str(&format!(
        "You are a gt polecat CONTINUING work on bead `{bead}` on branch `{bead}`. The previous \
         agent ran out of context before finishing and checkpointed its progress. Do NOT restart \
         from scratch — pick up exactly where it left off, building on the work already committed \
         on the branch.\n\n"
    ));

    p.push_str("## Previous agent's checkpoint\n");
    match parse_checkpoint(notes) {
        Some(cp) => {
            p.push_str("### Completed so far\n");
            p.push_str(if cp.completado.is_empty() {
                "(the checkpoint recorded nothing as completed)"
            } else {
                cp.completado.as_str()
            });
            p.push_str("\n\n### Still pending\n");
            p.push_str(if cp.pendiente.is_empty() {
                "(the checkpoint recorded nothing as pending — derive remaining work from the \
                 acceptance criteria below)"
            } else {
                cp.pendiente.as_str()
            });
            p.push('\n');
        }
        None => {
            // No structured checkpoint — hand over whatever notes exist verbatim.
            let notes = notes.trim();
            p.push_str(if notes.is_empty() {
                "(the previous agent left no checkpoint notes — reconstruct the remaining work \
                 from the diff and acceptance criteria below)"
            } else {
                notes
            });
            p.push('\n');
        }
    }

    p.push_str("\n## Acceptance criteria (the bead is done when all are met)\n");
    let ac = acceptance_criteria.trim();
    p.push_str(if ac.is_empty() {
        "(none recorded on the bead — read it via the `gt` MCP tools)"
    } else {
        ac
    });
    p.push('\n');

    p.push_str("\n## Work already on the branch (`git diff main...HEAD`)\n");
    let diff = diff.trim();
    if diff.is_empty() {
        p.push_str(
            "(no diff captured — the previous agent may not have committed yet; inspect the \
             working tree)\n",
        );
    } else {
        p.push_str("```diff\n");
        p.push_str(diff);
        p.push_str("\n```\n");
    }

    p.push_str(&format!(
        "\nContinue implementing the pending work end-to-end on branch `{bead}`. Make frequent \
         conventional commits as you go. When the work is delivered and committed, signal \
         completion by calling the MCP tool `mcp__gt__merge_submit` with arguments \
         {{\"bead\":\"{bead}\",\"branch\":\"{bead}\"}}. If the MCP tool is unavailable, fall back \
         to the merge-ready channel as described in CLAUDE.md. Then stop."
    ));

    p
}

/// Assemble the re-sling prompt handed to a polecat after its PR failed CI (gtcore-3a1bd4).
///
/// This is the CI-failure sibling of [`build_continuation_prompt`]: the bead's work is already on
/// the branch (and a PR is open with auto-merge armed), but the PR's CI is red — typically Rust the
/// previous agent wrote without compiling locally (a missing `derive`, a type error, a stale entry
/// in a parity test). Rather than leaving the merge slot terminally `failed` for a human, the bead
/// is re-dispatched to a polecat with this prompt: the CI failure `reason`, a best-effort snapshot
/// of the failing checks (`ci_checks`, from [`read_ci_checks`]), the branch `diff`, and the
/// acceptance criteria. `attempt`/`max` tell the agent which retry this is so it knows the budget is
/// finite. It closes by telling the agent to fix, compile/test locally, push to the SAME branch, and
/// re-submit — which re-enters the merge queue (a `Failed` slot accepts re-submit, gtcore-3a1bd4).
pub fn build_ci_fix_prompt(
    bead: &str,
    reason: &str,
    ci_checks: &str,
    diff: &str,
    acceptance_criteria: &str,
    attempt: u32,
    max: u32,
) -> String {
    let mut p = String::new();
    p.push_str(&format!(
        "You are a gt polecat RESUMING work on bead `{bead}` (branch `{bead}`). Your branch's work \
         is already committed and a pull request is open, but the PR's CI is FAILING — the merge \
         cannot land until CI is green. This is automated retry {attempt} of {max}; after the last \
         retry the bead is escalated to a human, so fix the failure decisively now. Do NOT restart \
         from scratch — the implementation is on the branch; you are fixing what CI flagged.\n\n"
    ));

    p.push_str("## Why CI failed\n");
    let reason = reason.trim();
    p.push_str(if reason.is_empty() {
        "(no reason captured — inspect the checks below and the full logs)"
    } else {
        reason
    });
    p.push('\n');

    p.push_str("\n## Failing checks (snapshot)\n");
    let ci_checks = ci_checks.trim();
    if ci_checks.is_empty() {
        p.push_str(
            "(no check snapshot captured — pull them yourself with the commands below)\n",
        );
    } else {
        p.push_str("```\n");
        p.push_str(ci_checks);
        p.push_str("\n```\n");
    }

    p.push_str(
        "\n## Get the full CI log\n\
         The snapshot above is only a pointer. Read the actual failure (cargo build/test errors) \
         from your branch's PR before changing anything:\n\
         - `gh pr checks` — list the checks and their state\n\
         - `gh run list --branch ");
    p.push_str(bead);
    p.push_str(
        " --limit 1` then `gh run view <run-id> --log-failed` — the failing job's full output\n\
         Reproduce locally where you can: `cargo build --locked` and `cargo test --locked` catch \
         the common cases (missing `derive`, type errors, a parity-test entry you forgot).\n",
    );

    p.push_str("\n## Acceptance criteria (the bead is done when all are met)\n");
    let ac = acceptance_criteria.trim();
    p.push_str(if ac.is_empty() {
        "(none recorded on the bead — read it via the `gt` MCP tools)"
    } else {
        ac
    });
    p.push('\n');

    p.push_str("\n## Work already on the branch (`git diff main...HEAD`)\n");
    let diff = diff.trim();
    if diff.is_empty() {
        p.push_str(
            "(no diff captured — inspect the working tree and the branch directly)\n",
        );
    } else {
        p.push_str("```diff\n");
        p.push_str(diff);
        p.push_str("\n```\n");
    }

    p.push_str(&format!(
        "\nFix the CI failure on branch `{bead}`, committing your fix with a conventional message. \
         Push to the SAME branch so the existing PR re-runs CI. When the fix is committed and \
         pushed, signal completion by calling the MCP tool `mcp__gt__merge_submit` with arguments \
         {{\"bead\":\"{bead}\",\"branch\":\"{bead}\"}} (re-submitting a CI-failed bead is allowed — \
         it re-enters the merge queue). If the MCP tool is unavailable, fall back to the merge-ready \
         channel as described in CLAUDE.md. Then stop."
    ));

    p
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTES: &str = "Some earlier note.\n\
## Checkpoint\n\
### Completado\n\
- Added the parser module\n\
- Wired pub mod in lib.rs\n\
### Pendiente\n\
- Wire the re-sling handler\n\
- Add the integration test\n";

    #[test]
    fn parse_checkpoint_extracts_both_sections() {
        let cp = parse_checkpoint(NOTES).expect("checkpoint present");
        assert_eq!(
            cp.completado,
            "- Added the parser module\n- Wired pub mod in lib.rs"
        );
        assert_eq!(
            cp.pendiente,
            "- Wire the re-sling handler\n- Add the integration test"
        );
    }

    #[test]
    fn parse_checkpoint_takes_the_last_block() {
        // Notes accumulate; the freshest `## Checkpoint` wins.
        let notes = "## Checkpoint\n### Completado\n- old\n### Pendiente\n- old-pending\n\
## Checkpoint\n### Completado\n- new\n### Pendiente\n- new-pending\n";
        let cp = parse_checkpoint(notes).unwrap();
        assert_eq!(cp.completado, "- new");
        assert_eq!(cp.pendiente, "- new-pending");
    }

    #[test]
    fn parse_checkpoint_none_without_a_block() {
        assert_eq!(parse_checkpoint("just some freeform notes"), None);
        assert_eq!(parse_checkpoint(""), None);
    }

    #[test]
    fn parse_checkpoint_stops_at_next_top_level_heading() {
        let notes =
            "## Checkpoint\n### Completado\n- done\n### Pendiente\n- todo\n## Other\n- not pending\n";
        let cp = parse_checkpoint(notes).unwrap();
        assert_eq!(cp.pendiente, "- todo");
        assert!(!cp.pendiente.contains("not pending"));
    }

    #[test]
    fn build_prompt_includes_checkpoint_diff_and_ac() {
        let prompt = build_continuation_prompt(
            "gtcore-3b2a68",
            NOTES,
            "diff --git a/x b/x\n+added line",
            "- It compiles.\n- Tests pass.",
        );
        // Identifies the bead and frames it as a continuation, not a restart.
        assert!(prompt.contains("CONTINUING work on bead `gtcore-3b2a68`"));
        assert!(prompt.contains("Do NOT restart"));
        // Carries the checkpoint's completed + pending work.
        assert!(prompt.contains("- Added the parser module"));
        assert!(prompt.contains("- Wire the re-sling handler"));
        // Carries the acceptance criteria.
        assert!(prompt.contains("- It compiles."));
        // Embeds the branch diff in a fenced block.
        assert!(prompt.contains("```diff"));
        assert!(prompt.contains("+added line"));
        // Closes with the completion signal so the continuator can mark the bead done.
        assert!(prompt.contains("mcp__gt__merge_submit"));
        assert!(prompt.contains("\"bead\":\"gtcore-3b2a68\""));
    }

    #[test]
    fn build_prompt_falls_back_to_raw_notes_without_checkpoint() {
        let prompt = build_continuation_prompt(
            "gg-1",
            "freeform handover text with no checkpoint heading",
            "",
            "- AC one",
        );
        assert!(prompt.contains("freeform handover text"));
        // No diff captured → the explanatory fallback, not an empty fenced block.
        assert!(prompt.contains("no diff captured"));
        assert!(!prompt.contains("```diff"));
    }

    #[test]
    fn build_prompt_tolerates_empty_notes_and_ac() {
        let prompt = build_continuation_prompt("gg-2", "", "", "");
        assert!(prompt.contains("no checkpoint notes"));
        assert!(prompt.contains("none recorded on the bead"));
    }

    #[test]
    fn build_ci_fix_prompt_carries_reason_checks_diff_ac_and_retry_budget() {
        let prompt = build_ci_fix_prompt(
            "gtcore-3a1bd4",
            "CI failed: failure",
            "build-test  fail  https://github.com/o/r/actions/runs/1",
            "diff --git a/x b/x\n+missing derive",
            "- cargo test green.\n- CI --locked green.",
            2,
            3,
        );
        // Frames it as a CI-failure resume, not a restart, and names the bead/branch.
        assert!(prompt.contains("RESUMING work on bead `gtcore-3a1bd4`"));
        assert!(prompt.contains("Do NOT restart"));
        // Surfaces the retry budget so the agent knows it is finite.
        assert!(prompt.contains("retry 2 of 3"));
        // Carries the CI reason + the failing-checks snapshot.
        assert!(prompt.contains("CI failed: failure"));
        assert!(prompt.contains("build-test"));
        // Tells the agent how to pull the full failing log itself.
        assert!(prompt.contains("--log-failed"));
        // Carries the acceptance criteria and the branch diff.
        assert!(prompt.contains("cargo test green"));
        assert!(prompt.contains("```diff"));
        assert!(prompt.contains("+missing derive"));
        // Closes with the completion signal, noting a CI-failed bead may re-submit.
        assert!(prompt.contains("mcp__gt__merge_submit"));
        assert!(prompt.contains("\"bead\":\"gtcore-3a1bd4\""));
        assert!(prompt.contains("re-enters the merge queue"));
    }

    #[test]
    fn build_ci_fix_prompt_tolerates_empty_reason_checks_diff_and_ac() {
        let prompt = build_ci_fix_prompt("gg-9", "", "", "", "", 1, 3);
        assert!(prompt.contains("no reason captured"));
        assert!(prompt.contains("no check snapshot captured"));
        assert!(prompt.contains("no diff captured"));
        assert!(prompt.contains("none recorded on the bead"));
        // Still self-contained: the self-fetch instructions and completion signal are present.
        assert!(prompt.contains("--log-failed"));
        assert!(prompt.contains("mcp__gt__merge_submit"));
    }

    #[test]
    fn truncate_marks_oversized_diffs() {
        let big = "x".repeat(MAX_DIFF_BYTES + 10);
        let out = truncate_on_char_boundary(&big, MAX_DIFF_BYTES);
        assert!(out.len() <= MAX_DIFF_BYTES + 100);
        assert!(out.contains("diff truncated"));
    }

    #[test]
    fn truncate_leaves_small_diffs_untouched() {
        assert_eq!(truncate_on_char_boundary("small", MAX_DIFF_BYTES), "small");
    }

    #[test]
    fn read_branch_diff_on_a_real_repo_captures_committed_work() {
        // Build a throwaway repo with a base commit on `main` and a divergent branch commit, then
        // assert the diff reader returns the branch's change (three-dot vs main).
        let dir = std::env::temp_dir().join(format!(
            "gt-contdiff-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let git = |args: &[&str]| {
            Command::new("git")
                .arg("-C")
                .arg(&dir)
                .args(args)
                .output()
                .unwrap()
                .status
                .success()
        };
        assert!(git(&["init", "-q", "-b", "main"]));
        assert!(git(&["config", "user.email", "t@t"]));
        assert!(git(&["config", "user.name", "t"]));
        std::fs::write(dir.join("f.txt"), "base\n").unwrap();
        assert!(git(&["add", "."]));
        assert!(git(&["commit", "-qm", "base"]));
        assert!(git(&["checkout", "-q", "-b", "feature"]));
        std::fs::write(dir.join("f.txt"), "base\nfeature change\n").unwrap();
        assert!(git(&["commit", "-qam", "feature work"]));

        let diff = read_branch_diff(&dir, "main");
        assert!(diff.contains("feature change"), "diff: {diff}");
        assert!(diff.contains("f.txt"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_branch_diff_is_empty_on_a_non_repo() {
        let dir = std::env::temp_dir().join(format!("gt-contdiff-nonrepo-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        assert_eq!(read_branch_diff(&dir, "main"), "");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
