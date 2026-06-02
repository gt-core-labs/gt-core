//! `gt-reconciler` — host-side, repo-aware dependency-graph completion
//! (hq-core-mcp.4 + .12).
//!
//! The MCP server cannot do this inline: its container has no repo mount
//! (mcp.4), so it can verify `commit_sha` FORMAT on close but not the Cargo graph,
//! the git history, or the source repos. This job runs on the host/CI with
//! checkouts of both repos (`GT_RECON_REPOS`) and makes the dependency graph
//! **sound, self-maintaining, and self-documenting** rather than repairing one
//! column. Four parts (docs/10 §S2/§S4, docs/11 §5):
//!
//! - **§A** — auto-derive `depends_on` edges from the Cargo crate graph
//!   (authoritative) + the surface→crate→owner mapping; MERGE the missing ones.
//! - **§A'** — stamp port provenance (`design = "port-origin: <repo>:<path>"`) on
//!   producer beads by resolving their `planned` crate in the source repos.
//! - **§B** — git-authoritative `delivered_sha` on closed tasks (a commit naming
//!   the bead-id AND touching a real surface); no commit → stays NULL.
//! - **§C** lives in `gt-issues` (`readiness.rs`): a closed *epic* dep satisfies.
//!
//! `--apply` performs the writes; the default is a **dry run** that only reports.
//! All writes are **idempotent** — a re-run after a clean apply is a no-op, and the
//! edge derivation only ever ADDS (never removes, so the dirty
//! `migrate.5 → migrate.4` edge is left alone).
//!
//! Env:
//! - `GT_DOLT_URL` (required) — e.g. `mysql://gastown@127.0.0.1:3307/hq`.
//! - `GT_RECON_REPOS` — comma-separated repo paths; default
//!   `/home/nixos/gt-core,/home/nixos/gastown`. The repo containing this crate is
//!   the gt-core workspace (Cargo graph + git history); the rest are port sources.

mod cargo_graph;
mod delivered;
mod edges;
mod health;
mod model;
mod provenance;

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use gt_issues::delivery::path_touches_surface;
use gt_issues::readiness::dep_satisfied;
use gt_issues::surface::parse_surface_json;
use gt_store_dolt::{DepFact, DoltIssues, IssueFilter, IssuePatch, IssueRow};

use cargo_graph::CrateGraph;
use delivered::{pick_delivered_sha, ShaCandidate};
use edges::{derive_missing_edges, filter_acyclic};
use health::{evaluate, HealthMetrics, Thresholds};
use model::Bead;
use provenance::{already_stamped, resolve_origin, SourceRepo};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let apply = std::env::args().any(|a| a == "--apply");
    let dolt_url =
        std::env::var("GT_DOLT_URL").map_err(|_| anyhow::anyhow!("GT_DOLT_URL is required"))?;
    let repos: Vec<String> = std::env::var("GT_RECON_REPOS")
        .unwrap_or_else(|_| "/home/nixos/gt-core,/home/nixos/gastown".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // The gt-core workspace = the repo that contains this crate; the rest are port
    // sources (gastown).
    let core_repo = repos
        .iter()
        .find(|r| {
            std::path::Path::new(r)
                .join("crates/domain/orchestration/gt-reconciler/Cargo.toml")
                .exists()
        })
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!("no repo in GT_RECON_REPOS contains the gt-core workspace")
        })?;
    let source_repos: Vec<&String> = repos.iter().filter(|r| **r != core_repo).collect();

    let store = DoltIssues::connect(&dolt_url)?;
    let rows = store
        .list(&IssueFilter {
            full: true,
            limit: Some(5000),
            ..Default::default()
        })
        .await?;
    let beads: Vec<Bead> = rows.iter().map(Bead::from_row).collect();
    let by_id: BTreeMap<&str, &Bead> = beads.iter().map(|b| (b.id.as_str(), b)).collect();

    println!(
        "gt-reconciler [{}]: {} beads, core={}, sources={:?}",
        if apply { "APPLY" } else { "dry-run" },
        rows.len(),
        core_repo,
        source_repos
    );

    // ── legacy audit (mcp.4): recorded-sha existence + stale non-planned surfaces.
    let (missing_shas, stale_surfaces) = audit(&rows, &repos);

    // ── §A — auto-derive depends_on edges from the Cargo graph.
    let graph = load_crate_graph(&core_repo)?;
    let derivation = derive_missing_edges(&graph, &beads);
    // The server rejects cyclic depends_on; drop any proposed edge that would
    // close a cycle against the existing graph (we only ever ADD, never remove the
    // pre-existing reverse edge).
    let (acyclic, skipped_cyclic) = filter_acyclic(&beads, &derivation.missing_edges);
    let mut edges_created = 0usize;
    // bead_id -> the new targets we are adding (for the unhidden calc).
    let mut added: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if acyclic.is_empty() {
        println!("\n§A edges: none missing (graph matches Cargo).");
    } else {
        println!("\n§A missing depends_on edges ({}):", acyclic.len());
        for (bead_id, target) in &acyclic {
            println!("  {bead_id} -> {target}");
            added.entry(bead_id.clone()).or_default().insert(target.clone());
        }
        if apply {
            for (bead_id, targets) in &added {
                let bead = by_id.get(bead_id.as_str()).expect("bead present");
                let mut merged: BTreeSet<String> = bead.depends_on.iter().cloned().collect();
                let before = merged.len();
                merged.extend(targets.iter().cloned());
                if merged.len() == before {
                    continue; // nothing new (idempotent guard).
                }
                let json = serde_json::to_string(&merged.into_iter().collect::<Vec<_>>())?;
                match store
                    .update(
                        bead_id,
                        &IssuePatch {
                            depends_on_json: Some(json),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(_) => edges_created += targets.len(),
                    Err(e) => eprintln!("  ! skip {bead_id} edge update: {e}"),
                }
            }
        }
    }
    if !skipped_cyclic.is_empty() {
        println!("\n§A SKIPPED (would create a cycle, existing reverse edge kept):");
        for (from, to) in &skipped_cyclic {
            println!("  {from} -> {to}");
        }
    }
    if !derivation.missing_producers.is_empty() {
        println!(
            "\n§A NO-PRODUCER crates ({}) — mint a producer bead (hq-core-port), not auto-created:",
            derivation.missing_producers.len()
        );
        for (krate, consumers) in &derivation.missing_producers {
            println!("  {krate}  consumed by {consumers:?}");
        }
    }

    // ── §A' — stamp port provenance on producer beads.
    let sources = build_source_repos(&source_repos);
    let mut provenance_stamped = 0usize;
    let mut prov_plan: Vec<(String, String)> = Vec::new(); // (id, marker)
    for (bead, row) in beads.iter().zip(rows.iter()) {
        if already_stamped(row.design.as_deref().unwrap_or("")) {
            continue;
        }
        if let Some(mark) = resolve_origin(bead, &sources) {
            prov_plan.push((bead.id.clone(), mark));
        }
    }
    if prov_plan.is_empty() {
        println!("\n§A' provenance: nothing to stamp.");
    } else {
        println!("\n§A' port-origin stamps ({}):", prov_plan.len());
        for (id, mark) in &prov_plan {
            println!("  {id}  design <- {mark}");
        }
        if apply {
            for (id, mark) in &prov_plan {
                let existing = rows
                    .iter()
                    .find(|r| &r.id == id)
                    .and_then(|r| r.design.clone())
                    .unwrap_or_default();
                // Prepend the structural marker, preserving any existing prose.
                let design = if existing.trim().is_empty() {
                    mark.clone()
                } else {
                    format!("{mark}\n{existing}")
                };
                match store
                    .update(
                        id,
                        &IssuePatch {
                            design: Some(design),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(_) => provenance_stamped += 1,
                    Err(e) => eprintln!("  ! skip {id} design stamp: {e}"),
                }
            }
        }
    }

    // ── §B — git-authoritative delivered_sha on closed tasks.
    let mut sha_plan: Vec<(String, String)> = Vec::new(); // (id, full_sha)
    let mut sha_skipped_no_commit = 0usize;
    for bead in &beads {
        if bead.status != "closed" || bead.issue_type != "task" || bead.delivered {
            continue;
        }
        let real = bead.real_surface_paths();
        if real.is_empty() {
            // No real surface to verify against → nothing to derive (correct NULL).
            sha_skipped_no_commit += 1;
            continue;
        }
        match derive_sha_from_git(&core_repo, &bead.id, &real) {
            Some(sha) => sha_plan.push((bead.id.clone(), sha)),
            None => sha_skipped_no_commit += 1,
        }
    }
    let mut sha_stamped = 0usize;
    if sha_plan.is_empty() {
        println!("\n§B delivered_sha: no closed task has a derivable commit.");
    } else {
        println!("\n§B git-derived delivered_sha ({}):", sha_plan.len());
        for (id, sha) in &sha_plan {
            println!("  {id}  delivered_sha <- {sha}");
        }
        if apply {
            for (id, sha) in &sha_plan {
                match store.set_delivered_sha(id, sha).await {
                    Ok(_) => sha_stamped += 1,
                    Err(e) => eprintln!("  ! skip {id} delivered_sha: {e}"),
                }
            }
        }
    }

    // ── beads-unhidden — open beads that flip not-ready→ready on the dep clause
    // (the only clause this job changes) due to the new sha stamps + the §C epic
    // rule. Phase/surface clauses are unchanged, so this is the dep-clause delta.
    let stamped_ids: BTreeSet<&str> = sha_plan.iter().map(|(id, _)| id.as_str()).collect();
    let unhidden = count_unhidden(&beads, &by_id, &added, &stamped_ids);

    println!("\n── counts ──");
    println!(
        "edges-created          : {}",
        if apply { edges_created } else { derivation.missing_edges.len() }
    );
    println!(
        "provenance-stamped     : {}",
        if apply { provenance_stamped } else { prov_plan.len() }
    );
    println!(
        "sha-stamped            : {}",
        if apply { sha_stamped } else { sha_plan.len() }
    );
    println!("sha-skipped-no-commit  : {sha_skipped_no_commit}");
    println!("beads-unhidden (est.)  : {unhidden}");
    if !apply {
        println!("\n(dry-run — re-run with --apply to write)");
    }

    // ── circuit-breaker (hq-auto.5): fold the pass's health signals through the
    // thresholds. A HALT verdict exits non-zero so the surrounding autonomous
    // loop stops *generating* and escalates to a human, before a slightly-wrong
    // model can amplify over many cycles.
    let metrics = HealthMetrics {
        cyclic_edges: skipped_cyclic.len(),
        no_producer_crates: derivation.missing_producers.len(),
        missing_shas,
        stale_surfaces,
    };
    let verdict = evaluate(&metrics, &Thresholds::default());
    println!("\n── health ── {}", verdict.label());
    for reason in verdict.reasons() {
        println!("  ! {reason}");
    }
    if verdict.should_halt() {
        eprintln!(
            "\ngt-reconciler: HALT — graph health degraded past circuit-breaker ceiling; \
             autonomous generation must stop and a human review the reasons above."
        );
        std::process::exit(2);
    }
    Ok(())
}

/// Load the gt-core crate graph via `cargo metadata --no-deps`.
fn load_crate_graph(core_repo: &str) -> anyhow::Result<CrateGraph> {
    let manifest = format!("{core_repo}/Cargo.toml");
    let out = Command::new("cargo")
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            &manifest,
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!("cargo metadata failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    CrateGraph::from_metadata(&String::from_utf8_lossy(&out.stdout), core_repo)
}

/// Build a [`SourceRepo`] index per source repo by scanning its tracked
/// `Cargo.toml` files for the package name (§A').
fn build_source_repos(source_repos: &[&String]) -> Vec<SourceRepo> {
    source_repos
        .iter()
        .map(|repo| {
            let label = std::path::Path::new(repo)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| (*repo).clone());
            let mut index = BTreeMap::new();
            let out = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["ls-files", "*Cargo.toml", "Cargo.toml"])
                .output();
            if let Ok(out) = out {
                for rel in String::from_utf8_lossy(&out.stdout).lines() {
                    let full = format!("{repo}/{rel}");
                    if let Ok(text) = std::fs::read_to_string(&full) {
                        if let Some(name) = package_name(&text) {
                            let dir = rel.strip_suffix("/Cargo.toml").unwrap_or(rel).to_string();
                            index.insert(name, dir);
                        }
                    }
                }
            }
            SourceRepo { label, index }
        })
        .collect()
}

/// Extract `name = "..."` from the `[package]` section of a Cargo manifest.
fn package_name(toml: &str) -> Option<String> {
    let mut in_package = false;
    for line in toml.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_package = t == "[package]";
            continue;
        }
        if in_package {
            if let Some(rest) = t.strip_prefix("name") {
                let rest = rest.trim_start().strip_prefix('=')?.trim();
                return Some(rest.trim_matches(|c| c == '"' || c == '\'').to_string());
            }
        }
    }
    None
}

/// Derive a closed task's `delivered_sha` from git (§B): the most recent commit on
/// `main` that names the bead-id AND touches one of its `real` (non-`planned`)
/// surface paths.
fn derive_sha_from_git(repo: &str, bead_id: &str, real_surfaces: &[String]) -> Option<String> {
    // Commits naming the bead-id, newest-first.
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["log", "main", &format!("--grep={bead_id}"), "--format=%H"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let candidates: Vec<ShaCandidate> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|s| s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|sha| {
            let touches = commit_touches_surface(repo, sha, real_surfaces);
            ShaCandidate { full_sha: sha.to_string(), touches_surface: touches }
        })
        .collect();
    pick_delivered_sha(&candidates)
}

/// True when commit `sha` changes a path at/under any of `surfaces`.
fn commit_touches_surface(repo: &str, sha: &str, surfaces: &[String]) -> bool {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["diff-tree", "--no-commit-id", "--name-only", "-r", sha])
        .output();
    let Ok(out) = out else { return false };
    if !out.status.success() {
        return false;
    }
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|changed| surfaces.iter().any(|s| path_touches_surface(changed, s)))
}

/// Estimate beads-unhidden: open beads whose dependency clause flips
/// not-satisfied → satisfied because of the new sha stamps + the §C epic rule.
/// Only the dep clause is considered (phase/surface are untouched by this job).
fn count_unhidden(
    beads: &[Bead],
    by_id: &BTreeMap<&str, &Bead>,
    added: &BTreeMap<String, BTreeSet<String>>,
    stamped: &BTreeSet<&str>,
) -> usize {
    // OLD semantics: a dep is satisfied iff it already carried delivered_sha (no
    // epic rule, pre-stamp). NEW: §C epic rule + post-stamp delivered set.
    let old_sat = |dep_id: &str| by_id.get(dep_id).map(|d| d.delivered).unwrap_or(false);
    let new_sat = |dep_id: &str| {
        by_id
            .get(dep_id)
            .map(|d| {
                let fact = DepFact {
                    issue_type: d.issue_type.clone(),
                    status: d.status.clone(),
                    delivered: d.delivered || stamped.contains(dep_id),
                };
                dep_satisfied(&fact)
            })
            .unwrap_or(false)
    };
    beads
        .iter()
        .filter(|b| b.status == "open")
        .filter(|b| {
            let old_ready = b.depends_on.iter().all(|d| old_sat(d));
            let mut new_deps: BTreeSet<&str> = b.depends_on.iter().map(String::as_str).collect();
            if let Some(extra) = added.get(&b.id) {
                new_deps.extend(extra.iter().map(String::as_str));
            }
            let new_ready = new_deps.iter().all(|d| new_sat(d));
            !old_ready && new_ready
        })
        .count()
}

/// Legacy mcp.4 audit: report recorded close-shas absent from every repo and
/// non-`planned` surface paths absent at HEAD. Informational (no exit-fail) — the
/// .12 parts are the actionable output. Returns `(missing_shas, stale_surfaces)`
/// so the circuit-breaker (hq-auto.5) can fold them into [`HealthMetrics`].
fn audit(rows: &[IssueRow], repos: &[String]) -> (usize, usize) {
    let mut bad_sha: Vec<(String, String)> = Vec::new();
    let mut stale_surface: Vec<(String, String)> = Vec::new();
    for r in rows {
        if r.status != "closed" {
            continue;
        }
        if let Some(sha) = extract_sha(r.notes.as_deref().unwrap_or("")) {
            if !sha_exists(repos, &sha) {
                bad_sha.push((r.id.clone(), sha));
            }
        }
        for e in parse_surface_json(&r.surface_json) {
            if !e.planned && !path_at_head(repos, &e.path) {
                stale_surface.push((r.id.clone(), e.path));
            }
        }
    }
    println!(
        "audit: recorded-sha missing {}   stale-surface paths {}",
        bad_sha.len(),
        stale_surface.len()
    );
    for (id, sha) in &bad_sha {
        println!("  MISSING sha {id} {sha}");
    }
    (bad_sha.len(), stale_surface.len())
}

/// Pull the delivering sha out of a close note (`closed @ <sha> (by ...)`).
fn extract_sha(notes: &str) -> Option<String> {
    let marker = "closed @ ";
    let start = notes.find(marker)? + marker.len();
    let sha: String = notes[start..]
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect();
    (sha.len() >= 7).then_some(sha)
}

/// True when `<sha>^{commit}` resolves in ANY configured repo.
fn sha_exists(repos: &[String], sha: &str) -> bool {
    let arg = format!("{sha}^{{commit}}");
    repos.iter().any(|repo| git_ok(repo, &["cat-file", "-e", &arg]))
}

/// True when `path` exists at HEAD in ANY configured repo (blob or tree).
fn path_at_head(repos: &[String], path: &str) -> bool {
    let spec = format!("HEAD:{path}");
    repos.iter().any(|repo| git_ok(repo, &["cat-file", "-e", &spec]))
}

/// Run `git -C <repo> <args>` and report whether it exited 0.
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
