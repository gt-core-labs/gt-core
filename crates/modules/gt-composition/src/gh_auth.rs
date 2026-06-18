//! Boot-time `gh`-auth seed (`gtcore-4c9c85`): make the GitHub CLI authenticated the moment the
//! orchd process comes up, so the [`crate::git_merge::GitMergePlugin`] PR path (`gh pr create` /
//! `gh pr merge`) never fails with *"To get started with GitHub CLI, please run: gh auth login"*.
//!
//! ## The failure this closes
//!
//! `gh` stores its login in `$HOME/.config/gh/hosts.yml`. In the orchd container `HOME=/tmp` is
//! **not** persistent, so every pod redeploy/restart wipes that file and *all* autonomous merges
//! fail at the `gh pr create` step — even though `git push` keeps working, because the rig
//! checkout's `origin` remote carries an embedded `https://<token>@github.com/...` token. The
//! commit stays durable (pushed early) but the final PR+merge tramo never closes until an operator
//! re-seeds `gh auth` by hand. Same systemic shape as the keychain creds (already solved by mounting
//! them on a PVC).
//!
//! ## The fix
//!
//! `gh` honours the `GH_TOKEN` environment variable automatically — no `gh auth login`, no file on
//! disk, and it survives any restart. So at boot we resolve a token and `set_var("GH_TOKEN", …)` in
//! the orchd process. Every `gh` child the merge edge spawns inherits it (and `gh auth status`
//! reports *Logged in … (GH_TOKEN)*), with no manual intervention.
//!
//! Token resolution, in priority order:
//! 1. An already-present `GH_TOKEN` / `GH_ENTERPRISE_TOKEN` — the chart-mounted-Secret hardening
//!    path (a `gt-app-proxy` follow-up). If the operator/chart supplied one we never override it.
//! 2. `GT_RIG_GIT_TOKEN` — the same PAT the entrypoint already holds to build the boot clone's
//!    remote. The cheapest source; needs no worktree to exist yet.
//! 3. The token embedded in the rig checkout's `origin` remote URL — exactly what an operator reads
//!    off a worktree by hand today. Self-contained: works even when only the boot clone exists.
//!
//! Setting `GH_TOKEN` does not touch `git push` (git authenticates via the embedded remote token,
//! not `GH_TOKEN`) nor the keychain creds flow — both are independent.

use std::path::Path;
use std::process::Command;

/// GitHub token prefixes we recognise inside a remote URL or env var. `github_pat_` (fine-grained
/// PAT) is listed first so it wins over the shorter `ghp_`-style classic prefixes when scanning.
const TOKEN_PREFIXES: [&str; 6] = ["github_pat_", "ghp_", "gho_", "ghu_", "ghs_", "ghr_"];

/// Where the seeded `GH_TOKEN` came from — returned so the bin can log one clear line without ever
/// printing the token itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhAuthSeed {
    /// `GH_TOKEN`/`GH_ENTERPRISE_TOKEN` was already set (chart Secret); left untouched.
    AlreadyEnv(&'static str),
    /// Seeded `GH_TOKEN` from the `GT_RIG_GIT_TOKEN` env var.
    SeededFromRigToken,
    /// Seeded `GH_TOKEN` from the token embedded in the rig checkout's `origin` remote.
    SeededFromRemote,
    /// No token anywhere — `gh` stays unauthenticated and PR merges will fail until one is seeded.
    NoToken,
}

impl GhAuthSeed {
    /// True when `gh` ends up authenticated as a result of the seed (either we set `GH_TOKEN` or one
    /// was already present).
    pub fn authenticated(&self) -> bool {
        !matches!(self, GhAuthSeed::NoToken)
    }

    /// A human log line describing the outcome. Never includes the token.
    pub fn describe(&self) -> String {
        match self {
            GhAuthSeed::AlreadyEnv(var) => {
                format!("gh auth already provided via {var} env — left untouched")
            }
            GhAuthSeed::SeededFromRigToken => {
                "gh auth seeded — GH_TOKEN set from GT_RIG_GIT_TOKEN".to_string()
            }
            GhAuthSeed::SeededFromRemote => {
                "gh auth seeded — GH_TOKEN set from rig origin remote token".to_string()
            }
            GhAuthSeed::NoToken => {
                "gh auth NOT seeded — no GH_TOKEN/GT_RIG_GIT_TOKEN and no token in rig origin remote; \
                 autonomous PR merges will fail until one is seeded"
                    .to_string()
            }
        }
    }
}

/// Find the first GitHub token in `s` (e.g. a `https://<token>@github.com/...` remote URL). The
/// body runs over `[A-Za-z0-9_]` so it stops cleanly at the `@`/`:`/`/` that delimit a URL.
fn find_github_token(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if let Some(p) = TOKEN_PREFIXES
            .iter()
            .find(|p| b[i..].starts_with(p.as_bytes()))
        {
            let body_start = i + p.len();
            let mut j = body_start;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            // Require at least one body char beyond the prefix so a bare `gho_` is not a "token".
            if j > body_start {
                return std::str::from_utf8(&b[i..j]).ok().map(str::to_string);
            }
        }
        i += 1;
    }
    None
}

/// Read the `origin` remote URL of the rig checkout and extract an embedded token, if any.
fn remote_token(rig: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(rig)
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout);
    find_github_token(url.trim())
}

/// Seed `GH_TOKEN` into the current process env so `gh` is authenticated for the lifetime of the
/// orchd, surviving the wiped `$HOME/.config/gh`. Call once at boot, before the merge edge can fire.
/// Returns where the token came from for logging. Best-effort: never panics, never aborts boot.
pub fn seed_gh_auth(rig: &Path) -> GhAuthSeed {
    // 1) Respect an externally-provided token (chart-mounted Secret). gh uses it automatically;
    //    overriding it would defeat the operator's hardening choice.
    for var in ["GH_TOKEN", "GH_ENTERPRISE_TOKEN"] {
        if std::env::var(var).map(|v| !v.trim().is_empty()).unwrap_or(false) {
            return GhAuthSeed::AlreadyEnv(var);
        }
    }

    // 2) The PAT the entrypoint already holds for the boot clone — cheapest, no worktree needed.
    if let Ok(tok) = std::env::var("GT_RIG_GIT_TOKEN") {
        let tok = tok.trim();
        if !tok.is_empty() {
            std::env::set_var("GH_TOKEN", tok);
            return GhAuthSeed::SeededFromRigToken;
        }
    }

    // 3) The token embedded in the rig checkout's origin remote — what an operator reads by hand.
    if let Some(tok) = remote_token(rig) {
        std::env::set_var("GH_TOKEN", tok);
        return GhAuthSeed::SeededFromRemote;
    }

    GhAuthSeed::NoToken
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_classic_oauth_token_from_remote_url() {
        let url = "https://gho_abc123DEF456@github.com/gt-core-labs/gt-core.git";
        assert_eq!(find_github_token(url).as_deref(), Some("gho_abc123DEF456"));
    }

    #[test]
    fn extracts_fine_grained_pat_with_internal_underscores() {
        // Fine-grained PATs are github_pat_<id>_<secret>; the underscore must stay inside the token.
        let url = "https://x-access-token:github_pat_11ABCDEF0_aZ09bYx@github.com/o/r.git";
        assert_eq!(
            find_github_token(url).as_deref(),
            Some("github_pat_11ABCDEF0_aZ09bYx")
        );
    }

    #[test]
    fn extracts_token_from_user_colon_token_userinfo() {
        let url = "https://gt-bot:ghp_TOKEN12345@github.com/o/r.git";
        assert_eq!(find_github_token(url).as_deref(), Some("ghp_TOKEN12345"));
    }

    #[test]
    fn stops_at_the_at_sign_and_never_grabs_the_host() {
        let tok = find_github_token("https://gho_zzz@github.com/o/r").unwrap();
        assert!(!tok.contains('@'));
        assert!(!tok.contains("github.com"));
    }

    #[test]
    fn no_token_in_a_plain_remote() {
        assert_eq!(find_github_token("git@github.com:o/r.git"), None);
        assert_eq!(find_github_token("https://github.com/o/r.git"), None);
    }

    #[test]
    fn bare_prefix_without_a_body_is_not_a_token() {
        assert_eq!(find_github_token("https://gho_@github.com/o/r"), None);
    }

    #[test]
    fn describe_never_panics_and_marks_authentication_state() {
        assert!(GhAuthSeed::SeededFromRigToken.authenticated());
        assert!(GhAuthSeed::SeededFromRemote.authenticated());
        assert!(GhAuthSeed::AlreadyEnv("GH_TOKEN").authenticated());
        assert!(!GhAuthSeed::NoToken.authenticated());
        assert!(!GhAuthSeed::NoToken.describe().is_empty());
    }
}
