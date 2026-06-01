//! `gt-reconciler` — async repo-aware audit of closed beads (hq-core-mcp.4).
//!
//! The MCP server enforces `commit_sha` FORMAT on close but cannot verify the
//! sha EXISTS or that `surface_json` is fresh — its container has no repo mount
//! (a Dolt-fronting service shouldn't host source). This job fills that gap out
//! of band: with host/CI checkouts of both repos it scans the CLOSED beads and
//! reports
//!   - `commit_sha` recorded in the close note but absent from every repo
//!     (a fabricated / typo'd / unpushed sha), and
//!   - `surface_json` paths no longer present at HEAD (stale after a crate move).
//!
//! Env:
//! - `GT_DOLT_URL` (required) — e.g. `mysql://gastown@127.0.0.1:3307/hq`.
//! - `GT_RECON_REPOS` — comma-separated repo paths; default
//!   `/home/nixos/gt-core,/home/nixos/gastown`.
//!
//! Exit code is non-zero only when a recorded sha does not exist (the real
//! integrity violation); no-sha-note and stale-surface are reported as warnings
//! so the job stays informational about the (large, known) stale-surface set.

use std::process::Command;

use gt_store_dolt::{DoltIssues, IssueFilter, IssueRow};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dolt_url = std::env::var("GT_DOLT_URL")
        .map_err(|_| anyhow::anyhow!("GT_DOLT_URL is required"))?;
    let repos: Vec<String> = std::env::var("GT_RECON_REPOS")
        .unwrap_or_else(|_| "/home/nixos/gt-core,/home/nixos/gastown".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let store = DoltIssues::connect(&dolt_url)?;
    let filter = IssueFilter {
        status: vec!["closed".to_string()],
        full: true,
        limit: Some(5000),
        ..Default::default()
    };
    let rows = store.list(&filter).await?;

    let mut bad_sha: Vec<(String, String)> = Vec::new(); // recorded but missing
    let mut no_sha: Vec<String> = Vec::new(); // closed without a "closed @ <sha>" note
    let mut stale_surface: Vec<(String, String)> = Vec::new();

    for r in &rows {
        match extract_sha(r.notes.as_deref().unwrap_or("")) {
            Some(sha) => {
                if !sha_exists(&repos, &sha) {
                    bad_sha.push((r.id.clone(), sha));
                }
            }
            None => no_sha.push(r.id.clone()),
        }
        for path in surfaces(r) {
            if !path_at_head(&repos, &path) {
                stale_surface.push((r.id.clone(), path));
            }
        }
    }

    println!("gt-reconciler: {} closed beads scanned across {:?}", rows.len(), repos);
    println!(
        "  recorded-sha missing : {}   no-sha-note : {}   stale-surface paths : {}",
        bad_sha.len(),
        no_sha.len(),
        stale_surface.len()
    );
    if !bad_sha.is_empty() {
        println!("\nMISSING commit_sha (recorded but not in any repo) — integrity violation:");
        for (id, sha) in &bad_sha {
            println!("  {id}  sha={sha}");
        }
    }
    if !stale_surface.is_empty() {
        println!("\nSTALE surface_json paths (absent at HEAD) — repoint via issues.update:");
        for (id, path) in &stale_surface {
            println!("  {id}  {path}");
        }
    }

    // Only a fabricated/unreachable sha fails the job; stale surfaces and pre-rule
    // no-sha closes are warnings (the stale set is large and known).
    if bad_sha.is_empty() {
        Ok(())
    } else {
        std::process::exit(1);
    }
}

/// Pull the delivering sha out of a close note (`closed @ <sha> (by ...)`).
/// Returns the longest run of hex right after the marker, when 7+ chars.
fn extract_sha(notes: &str) -> Option<String> {
    let marker = "closed @ ";
    let start = notes.find(marker)? + marker.len();
    let sha: String = notes[start..]
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect();
    (sha.len() >= 7).then_some(sha)
}

/// The bead's `surface_json` parsed into a list of paths (empty on any error).
fn surfaces(row: &IssueRow) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(&row.surface_json).unwrap_or_default()
}

/// True when `<sha>^{commit}` resolves in ANY configured repo.
fn sha_exists(repos: &[String], sha: &str) -> bool {
    let arg = format!("{sha}^{{commit}}");
    repos.iter().any(|repo| {
        git_ok(repo, &["cat-file", "-e", &arg])
    })
}

/// True when `path` exists at HEAD in ANY configured repo (blob or tree).
fn path_at_head(repos: &[String], path: &str) -> bool {
    let spec = format!("HEAD:{path}");
    repos.iter().any(|repo| git_ok(repo, &["cat-file", "-e", &spec]))
}

/// Run `git -C <repo> <args>` and report whether it exited 0. Any spawn failure
/// (missing repo / git) counts as "not found" for that repo, not a crash.
fn git_ok(repo: &str, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
