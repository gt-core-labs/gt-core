//! Interactive-dialog wedge detection (gtcore-2836bb).
//!
//! A production polecat is a `claude` coding agent in a detached tmux pane. The supervisor's
//! liveness check is `tmux has-session`: a session that still exists is treated as alive and its
//! restart budget is reset. But `claude` can be *alive yet wedged* — frozen at an interactive
//! dialog the orchestrator never navigates:
//!
//! - **"Do you trust this folder?"** — the onboarding/trust prompt. The dispatcher seeds
//!   `hasTrustDialogAccepted` into the account's `.claude.json` before the first sling, but a
//!   SUPERVISOR re-sling (gtcore-49198f) into a freshly-rotated account dir (or a new worktree
//!   path) may land in a config dir that was never seeded, so the prompt reappears and the polecat
//!   sits at it forever, burning a pool slot and producing nothing.
//! - **"Usage limit reached — 1. Stop and wait … 2. Upgrade"** — the CLI's own rate-limit dialog.
//!   The orchd quota subsystem rotates accounts at the API/predictor level, but it cannot reach
//!   into the pane to dismiss this modal; the polecat freezes against it.
//!
//! Both look identical to a healthy session through `has-session`, so a wedged polecat reports
//! "working" in false. This module is the PURE classifier: given the last lines captured off the
//! pane (`tmux capture-pane`, the same read the context-exhaustion detector already uses), decide
//! whether the polecat is wedged and on which dialog — no I/O, no clock. The supervisor edge reads
//! the pane and acts on the verdict (re-seed onboarding + re-sling for trust; rotate account +
//! re-sling for usage-limit; alert the operator either way).

/// A known interactive dialog a polecat can freeze on. The supervisor maps each to a recovery
/// action; the variant also tells the operator alert WHICH wall the agent hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WedgeDialog {
    /// claude's first-run "Do you trust this folder?" / onboarding trust prompt. Recovery:
    /// re-seed onboarding into the polecat's config dir, then re-sling.
    TrustPrompt,
    /// claude's "Usage limit reached" rate-limit modal (Stop and wait / Upgrade). Recovery:
    /// rotate the account off the limited one, then re-sling.
    UsageLimit,
}

impl WedgeDialog {
    /// Operator-facing reason fragment for the alert, naming the dialog the polecat is stuck on.
    pub fn reason(self) -> &'static str {
        match self {
            WedgeDialog::TrustPrompt => "trust-folder dialog",
            WedgeDialog::UsageLimit => "usage-limit dialog",
        }
    }

    /// The recovery the supervisor applies before re-slinging.
    pub fn recovery(self) -> WedgeRecovery {
        match self {
            // A trust prompt means the config dir was never seeded for this worktree — re-seed the
            // onboarding/trust flags and re-sling so the fresh session skips the dialog.
            WedgeDialog::TrustPrompt => WedgeRecovery::ReseedOnboarding,
            // A usage-limit modal means the backing account hit its wall — the quota rotation moves
            // the live pointer, so a plain re-sling lands on the rotated (healthy) account.
            WedgeDialog::UsageLimit => WedgeRecovery::RotateAccount,
        }
    }
}

/// What the supervisor should do before re-slinging a wedged polecat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WedgeRecovery {
    /// Re-seed the onboarding/trust flags into the polecat's claude config dir, then re-sling.
    ReseedOnboarding,
    /// Re-resolve the backing account (the quota rotation already flipped the live pointer off the
    /// limited one), then re-sling.
    RotateAccount,
}

/// Classify captured pane text into a wedge verdict.
///
/// Returns `Some(dialog)` only when the pane carries an UNAMBIGUOUS marker of a known interactive
/// dialog — the literal prompt text claude renders. The match is case-insensitive and tolerant of
/// the box-drawing/spacing tmux interleaves, so a wrapped or framed prompt still trips it. A pane
/// with no such marker (normal working output, a shell prompt, empty) is `None` — never a false
/// wedge, so a busy-but-healthy polecat is left alone.
///
/// Precedence: the usage-limit modal is checked first. A usage-limit dialog can appear over a
/// session that also onboarded earlier in the scrollback; the limit is the live blocker, so it
/// wins.
pub fn classify_wedge(pane: &str) -> Option<WedgeDialog> {
    let lower = pane.to_ascii_lowercase();
    if is_usage_limit(&lower) {
        return Some(WedgeDialog::UsageLimit);
    }
    if is_trust_prompt(&lower) {
        return Some(WedgeDialog::TrustPrompt);
    }
    None
}

/// True when the (already-lowercased) pane shows claude's usage-limit modal. Keys off the
/// distinctive prompt strings rather than a single word so an agent merely *discussing* usage
/// limits in its output never trips it: the modal pairs "usage limit reached" with its two
/// numbered choices.
fn is_usage_limit(lower: &str) -> bool {
    let has_header = lower.contains("usage limit reached") || lower.contains("approaching usage limit");
    // The interactive modal offers the Stop-and-wait / Upgrade choice; a passing log line about a
    // limit does not. Require the choice text so only the blocking modal counts.
    let has_choice = lower.contains("stop and wait for")
        || (lower.contains("wait for") && lower.contains("limit to reset"));
    has_header && has_choice
}

/// True when the (already-lowercased) pane shows claude's first-run trust/onboarding prompt.
/// Requires the distinctive "trust" + "folder" pairing of the dialog so ordinary output mentioning
/// either word in isolation does not trip it.
fn is_trust_prompt(lower: &str) -> bool {
    lower.contains("do you trust the files in this folder")
        || lower.contains("do you trust this folder")
        || (lower.contains("trust the files") && lower.contains("folder"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_the_trust_folder_prompt() {
        let pane = "\
╭──────────────────────────────────────────────╮
│ Do you trust the files in this folder?       │
│ /rig-wt/gt-hq-1                               │
│ ❯ 1. Yes, proceed                            │
│   2. No, exit                                │
╰──────────────────────────────────────────────╯";
        assert_eq!(classify_wedge(pane), Some(WedgeDialog::TrustPrompt));
        assert_eq!(
            classify_wedge(pane).unwrap().recovery(),
            WedgeRecovery::ReseedOnboarding
        );
    }

    #[test]
    fn detects_the_short_trust_prompt_variant() {
        assert_eq!(
            classify_wedge("Do you trust this folder? 1. Yes 2. No"),
            Some(WedgeDialog::TrustPrompt)
        );
    }

    #[test]
    fn detects_the_usage_limit_modal() {
        let pane = "\
Usage limit reached
❯ 1. Stop and wait for limit to reset
  2. Upgrade";
        assert_eq!(classify_wedge(pane), Some(WedgeDialog::UsageLimit));
        assert_eq!(
            classify_wedge(pane).unwrap().recovery(),
            WedgeRecovery::RotateAccount
        );
    }

    #[test]
    fn usage_limit_wins_over_an_earlier_trust_acceptance_in_scrollback() {
        // Scrollback is oldest-first: the session trusted the folder long ago, then hit the wall.
        // The live blocker is the usage-limit modal.
        let pane = "\
Do you trust this folder? (accepted)
…lots of work…
Usage limit reached
1. Stop and wait for limit to reset
2. Upgrade";
        assert_eq!(classify_wedge(pane), Some(WedgeDialog::UsageLimit));
    }

    #[test]
    fn healthy_working_pane_is_not_a_wedge() {
        assert_eq!(classify_wedge("Running tests… 12 passed\n$ "), None);
        assert_eq!(classify_wedge(""), None);
        // A normal context-used status line is not a dialog.
        assert_eq!(classify_wedge("⏵ 42% context used"), None);
    }

    #[test]
    fn mentioning_a_limit_in_passing_is_not_a_wedge() {
        // An agent reasoning about rate limits in its own output must NOT be read as the modal:
        // no numbered Stop-and-wait choice ⇒ no wedge.
        let pane = "I should be careful about the usage limit reached earlier in this rig.";
        assert_eq!(classify_wedge(pane), None);
    }

    #[test]
    fn mentioning_trust_in_passing_is_not_a_wedge() {
        let pane = "We trust the upstream maintainers of this folder's dependencies.";
        // "trust the" + "folder" could be near each other; the phrase here is not the prompt.
        // The exact-phrase guards keep it None.
        assert_eq!(classify_wedge(pane), None);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(
            classify_wedge("DO YOU TRUST THIS FOLDER?"),
            Some(WedgeDialog::TrustPrompt)
        );
        assert_eq!(
            classify_wedge("USAGE LIMIT REACHED\n1. STOP AND WAIT FOR LIMIT TO RESET"),
            Some(WedgeDialog::UsageLimit)
        );
    }
}
