//! Git-authoritative `delivered_sha` derivation (hq-core-mcp.12 §B).
//!
//! The note-scrape approach was rejected (prose-as-data, ~20% coverage). Instead,
//! for a closed task with `delivered_sha` NULL, the delivering commit is the one
//! on `main` that **names the bead-id** (`git log --grep`) AND **touches a
//! non-`planned` surface path** — the same intersection `close` phase-2 (.10)
//! verifies inline. The most recent such commit wins. A bead with no qualifying
//! commit (the upstream-era closed beads that delivered nothing *here*) is left
//! NULL — correct, not a fallback to the note.
//!
//! This module is the pure picker: the IO layer (main) gathers the candidate
//! commits (already filtered to those naming the id, newest-first) annotated with
//! whether each touches a real surface, and this returns the sha to stamp.

/// A candidate delivering commit for a bead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShaCandidate {
    /// Full 40-hex sha.
    pub full_sha: String,
    /// Whether this commit touches one of the bead's non-`planned` surface paths.
    pub touches_surface: bool,
}

/// Pick the `delivered_sha` to stamp (§B): the first candidate (the caller passes
/// them newest-first) that touches a non-`planned` surface. `None` when no
/// candidate qualifies — the bead stays NULL.
pub fn pick_delivered_sha(candidates: &[ShaCandidate]) -> Option<String> {
    candidates
        .iter()
        .find(|c| c.touches_surface)
        .map(|c| c.full_sha.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(sha: &str, touches: bool) -> ShaCandidate {
        ShaCandidate { full_sha: sha.into(), touches_surface: touches }
    }

    #[test]
    fn task_with_touching_commit_is_stamped() {
        let cands = vec![cand("a".repeat(40).as_str(), true), cand("b".repeat(40).as_str(), true)];
        // newest-first: the first touching commit wins.
        assert_eq!(pick_delivered_sha(&cands), Some("a".repeat(40)));
    }

    #[test]
    fn task_without_commit_stays_null() {
        assert_eq!(pick_delivered_sha(&[]), None);
    }

    #[test]
    fn commit_naming_id_but_not_touching_surface_does_not_stamp() {
        // a commit that names the bead but touches no real surface is not a
        // delivery (e.g. a tracking edit) → NULL.
        let cands = vec![cand("c".repeat(40).as_str(), false)];
        assert_eq!(pick_delivered_sha(&cands), None);
    }
}
