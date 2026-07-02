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
    /// A feature-promo modal ("Yes, try it" / "Maybe later", gtcore-f396dc) — new claude releases
    /// gate features behind first-run promos the seeded flags don't cover yet. Recovery: re-seed
    /// (the seeder stamps the known promo flags), then re-sling.
    FeaturePromo,
    /// The session booted past every dialog but sits at the input box with NO turn ever started
    /// (gtcore-f396dc): the dialog consumed the positional kickoff prompt, so the agent idles at
    /// an empty prompt, heartbeating, indistinguishable from working via `has-session`. Only
    /// classified for FRESHLY slung sessions (age-gated in the supervisor tick) — an old session
    /// resting between turns always has output in its pane. Recovery: re-seed + re-sling (the
    /// fresh spawn re-passes the kickoff prompt).
    IdleEmptyPrompt,
}

impl WedgeDialog {
    /// Operator-facing reason fragment for the alert, naming the dialog the polecat is stuck on.
    pub fn reason(self) -> &'static str {
        match self {
            WedgeDialog::TrustPrompt => "trust-folder dialog",
            WedgeDialog::UsageLimit => "usage-limit dialog",
            WedgeDialog::FeaturePromo => "feature-promo dialog",
            WedgeDialog::IdleEmptyPrompt => "idle empty prompt (kickoff prompt lost)",
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
            // A promo modal (or its aftermath, the eaten kickoff prompt) is a seeding gap — re-seed
            // stamps the known promo flags; the re-sling re-delivers the kickoff prompt.
            WedgeDialog::FeaturePromo => WedgeRecovery::ReseedOnboarding,
            WedgeDialog::IdleEmptyPrompt => WedgeRecovery::ReseedOnboarding,
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
    if is_feature_promo(&lower) {
        return Some(WedgeDialog::FeaturePromo);
    }
    None
}

/// Classify a FRESHLY slung session's pane as [`WedgeDialog::IdleEmptyPrompt`] (gtcore-f396dc):
/// the TUI booted to its interactive input box, no turn is running, and NO agent output was ever
/// rendered — the positional kickoff prompt was consumed (by a dialog that has since closed, or
/// it landed in the box unsubmitted) and the polecat will idle forever.
///
/// Deliberately separate from [`classify_wedge`]: this verdict is only sound for a session in
/// its first minutes (a mature session resting between turns has output in its visible pane),
/// so the supervisor tick applies it behind an age gate. All three conditions must hold:
///
/// 1. **Booted** — the pane shows claude's interactive bottom bar (`bypass permissions` /
///    `? for shortcuts`), so a still-launching process is never classified.
/// 2. **Not running** — no `esc to interrupt` marker (a turn in flight renders it).
/// 3. **No output ever** — none of the response/thinking/tool markers (`●`, `✻`, `⎿`) appear in
///    the visible scrollback; any of them proves the kickoff prompt WAS accepted.
pub fn classify_fresh_idle(pane: &str) -> Option<WedgeDialog> {
    let lower = pane.to_ascii_lowercase();
    let booted = lower.contains("bypass permissions") || lower.contains("? for shortcuts");
    let running = lower.contains("esc to interrupt");
    let has_output = pane.contains('●') || pane.contains('✻') || pane.contains('⎿');
    if booted && !running && !has_output {
        Some(WedgeDialog::IdleEmptyPrompt)
    } else {
        None
    }
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

/// True when the (already-lowercased) pane shows a feature-promo modal (gtcore-f396dc). Keys off
/// the accept text PAIRED with a decline choice so an agent merely quoting "yes, try it" in its
/// own output never trips it — the modal always offers the opt-out alternative.
fn is_feature_promo(lower: &str) -> bool {
    lower.contains("yes, try it")
        && (lower.contains("maybe later") || lower.contains("not now") || lower.contains("no thanks"))
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
    fn detects_the_feature_promo_modal() {
        // gtcore-f396dc: the promo pairs the accept text with a decline choice.
        let pane = "\
╭──────────────────────────────────────────────╮
│ New: run background tasks while you work     │
│ ❯ 1. Yes, try it                             │
│   2. Maybe later                             │
╰──────────────────────────────────────────────╯";
        assert_eq!(classify_wedge(pane), Some(WedgeDialog::FeaturePromo));
        assert_eq!(
            classify_wedge(pane).unwrap().recovery(),
            WedgeRecovery::ReseedOnboarding
        );
        assert_eq!(
            classify_wedge("Try the new feature? 1. Yes, try it  2. Not now"),
            Some(WedgeDialog::FeaturePromo)
        );
    }

    #[test]
    fn quoting_the_promo_text_in_passing_is_not_a_wedge() {
        // The accept text without a decline choice is an agent talking, not the modal.
        assert_eq!(classify_wedge("The dialog said 'Yes, try it' and I accepted."), None);
    }

    #[test]
    fn fresh_idle_empty_prompt_is_classified() {
        // gtcore-f396dc: booted TUI, no turn running, no output ever — the kickoff was eaten.
        let pane = "\
 Welcome to Claude Code
────────────────────────
❯
────────────────────────
  ⏵⏵ bypass permissions on (shift+tab to cycle)";
        assert_eq!(classify_fresh_idle(pane), Some(WedgeDialog::IdleEmptyPrompt));
        // Same verdict when the prompt landed in the box but was never submitted.
        let stuck = "\
❯ fix the bug in foo.rs
──────────────────────
  ? for shortcuts";
        assert_eq!(classify_fresh_idle(stuck), Some(WedgeDialog::IdleEmptyPrompt));
    }

    #[test]
    fn fresh_idle_requires_booted_and_excludes_activity() {
        // Still launching (no bottom bar) → not classified; a later tick decides.
        assert_eq!(classify_fresh_idle("Loading…"), None);
        // A turn in flight → healthy.
        assert_eq!(
            classify_fresh_idle("✻ Thinking… esc to interrupt · bypass permissions on"),
            None
        );
        // Any rendered output proves the kickoff was accepted → healthy even if idle now.
        assert_eq!(
            classify_fresh_idle("● Done, 3 files changed\n❯\n  bypass permissions on"),
            None
        );
        assert_eq!(
            classify_fresh_idle("  ⎿ Read 40 lines\n❯\n  ? for shortcuts"),
            None
        );
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
