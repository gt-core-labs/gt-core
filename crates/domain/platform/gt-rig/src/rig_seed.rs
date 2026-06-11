//! The versioned, reproducible rig-catalog seed (`hq-greenfield-seeds.5`).
//!
//! The rigs (`gt`, `gt_core`, `gtmcp`, `gtproxy`, `gtweb`) were registered by hand with `rig.add`
//! and lived ONLY in prod's per-tenant `ws_default.rigs` table — a clean cluster came up with an
//! empty rig catalog, so a fresh workspace could neither route work nor dispatch beads until an
//! operator re-ran every `rig.add`. This module makes the catalog reproducible: the live config is
//! extracted read-only from a running deploy and versioned in [`SEED_JSON`] (`seeds/rigs.json`),
//! embedded into the binary so the seed travels with it (orchestrator-agnostic — no external file
//! under k8s), and replayed into an EMPTY `rigs` table at boot (see `gt-mcp-server.rs::seed_rigs`).
//!
//! ## Shape mirrors the `.3` OAuth seed
//!
//! - **Idempotent / non-clobbering:** the caller seeds ONLY when the `rigs` table is EMPTY. A
//!   populated table (the already-curated prod `default`, or any deploy where an operator has
//!   registered a rig) is left exactly as-is — `rig.*` remains the source of truth there.
//! - **No secrets, no runtime artifacts vendored:** the seed carries only the stable, declarative
//!   identity of each rig (name / prefix / git_url / default_branch / …). `git_connection_ref` —
//!   a SOFT reference to a `public.vcs_connections` row, which is a runtime GitHub-App install
//!   artifact (`hq-greenfield-seeds.3`) — is carried only if it was set in the live extract; in
//!   prod all five rigs have it `null` (plain SSH clone), so the seed binds no connection. A rig
//!   that needs a private-repo clone token depends on its VCS connection existing first; that link
//!   is (re)bound out of band, not by this seed.
//! - **Boot-time timestamp:** `registered_at_secs` is NOT vendored (a prod-specific artifact);
//!   the caller stamps it from the boot clock via [`SeedRig::into_entry`], so a greenfield seed is
//!   self-consistent without copying prod's epochs.
//!
//! See `docs/ops/greenfield-seeds.md` §4.3 for the writeup and how to regenerate the seed
//! (`scripts/extract-rigs-seed.py`) from a running deploy — do NOT invent the content.

use serde::Deserialize;

use crate::state::RigEntry;

/// The versioned rig-catalog extract, embedded as a string so the seed travels with the binary.
/// Regenerate from a live deploy with `scripts/extract-rigs-seed.py` — do NOT invent the content
/// (the values are what prod's `ws_default.rigs` holds).
pub const SEED_JSON: &str = include_str!("../seeds/rigs.json");

/// One rig in the versioned seed: the stable, declarative identity of a catalog entry. This is the
/// orchestrator-relevant subset of [`RigEntry`] MINUS the runtime-stamped `registered_at_secs`,
/// which the caller supplies from the boot clock (the seed never vendors prod's epochs).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SeedRig {
    /// Catalog identity (the upsert key).
    pub name: String,
    /// Beads prefix this rig owns (unique across the catalog).
    pub prefix: String,
    /// Clone URL (fetch). Public/SSH URLs are safe to vendor; no token is embedded.
    pub git_url: String,
    /// Optional distinct push URL.
    #[serde(default)]
    pub push_url: Option<String>,
    /// Optional upstream (fork-parent) URL.
    #[serde(default)]
    pub upstream_url: Option<String>,
    /// Default branch tracked by the rig.
    pub default_branch: String,
    /// Optional explicit worktree-root override (`null` = convention default at the edge).
    #[serde(default)]
    pub worktree_root: Option<String>,
    /// Optional soft reference to a `public.vcs_connections.id`. `null` = no VCS connection bound
    /// (SSH/public clone). A non-null value names a connection that is itself a runtime install
    /// artifact (`hq-greenfield-seeds.3`); the seed only carries it if the live extract had it.
    #[serde(default)]
    pub git_connection_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SeedFile {
    rigs: Vec<SeedRig>,
}

impl SeedRig {
    /// Resolve this seed entry into a [`RigEntry`] ready to upsert, stamping `registered_at_secs`
    /// from the boot clock (`now_secs`) — the seed never vendors prod's registration epochs.
    pub fn into_entry(self, now_secs: u64) -> RigEntry {
        RigEntry {
            name: self.name,
            prefix: self.prefix,
            git_url: self.git_url,
            push_url: self.push_url,
            upstream_url: self.upstream_url,
            default_branch: self.default_branch,
            registered_at_secs: now_secs,
            worktree_root: self.worktree_root.map(std::path::PathBuf::from),
            git_connection_ref: self.git_connection_ref,
        }
    }
}

/// Parse the embedded [`SEED_JSON`] into its rig entries. The seed is a build-time-checked artifact,
/// so a parse failure is a programmer error (a malformed commit); it is surfaced as an `Err` so the
/// boot path can fail loudly rather than silently seed nothing.
pub fn seed_rigs() -> Result<Vec<SeedRig>, serde_json::Error> {
    let file: SeedFile = serde_json::from_str(SEED_JSON)?;
    Ok(file.rigs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_seed_parses_and_carries_the_five_extracted_rigs() {
        // The seed is the live-extracted catalog — assert the shape the prod rows had so a regen
        // that drops/garbles a rig is caught at build/test time, not on a greenfield deploy.
        let rigs = seed_rigs().expect("embedded seed parses");
        assert_eq!(rigs.len(), 5, "the five live rigs are vendored");

        let names: Vec<&str> = rigs.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["gt", "gt_core", "gtmcp", "gtproxy", "gtweb"]);

        // gt_core owns the `hq` prefix (the gt-core repo) — the most load-bearing mapping.
        let core = rigs.iter().find(|r| r.name == "gt_core").unwrap();
        assert_eq!(core.prefix, "hq");
        assert_eq!(core.git_url, "git@github.com:gt-core-labs/gt-core.git");
        assert_eq!(core.default_branch, "main");

        // Prefixes are unique across the catalog (the catalog enforces this; a dup would 500 a
        // greenfield boot at upsert time).
        let mut prefixes: Vec<&str> = rigs.iter().map(|r| r.prefix.as_str()).collect();
        prefixes.sort_unstable();
        let dedup_len = {
            let mut p = prefixes.clone();
            p.dedup();
            p.len()
        };
        assert_eq!(prefixes.len(), dedup_len, "rig prefixes are unique");

        // No runtime VCS connection is vendored — every live rig clones over SSH (ref is null).
        assert!(
            rigs.iter().all(|r| r.git_connection_ref.is_none()),
            "no rig binds a runtime vcs_connections ref in the seed"
        );
    }

    #[test]
    fn into_entry_stamps_boot_clock_and_preserves_identity() {
        let seed = SeedRig {
            name: "gt_core".into(),
            prefix: "hq".into(),
            git_url: "git@github.com:gt-core-labs/gt-core.git".into(),
            push_url: None,
            upstream_url: None,
            default_branch: "main".into(),
            worktree_root: None,
            git_connection_ref: None,
        };
        let entry = seed.into_entry(1_780_000_000);
        assert_eq!(entry.name, "gt_core");
        assert_eq!(entry.prefix, "hq");
        assert_eq!(entry.registered_at_secs, 1_780_000_000);
        assert!(entry.worktree_root.is_none());
        assert!(entry.git_connection_ref.is_none());
    }
}
