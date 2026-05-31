//! NN-16 bead-taxonomy validation (`hq-mod-mcp.10`).
//!
//! The canonical hierarchy is `epic → sub-epic → bead` (docs/04 §16). Every
//! non-epic bead MUST carry `external_ref` equal to its **sub-epic**, and its
//! id MUST read `<external_ref>.<n>` where `<n>` is a positive integer. Epics
//! are exempt.
//!
//! This is the reusable frontier guard: the `gt-mcp` service calls
//! [`validate`] inside `issues.create` / `issues.update` and rejects a
//! malformed bead at the MCP boundary — the same way rule 15 rejects a spoofed
//! `workspace_id` — so `gt://issues?external_ref=<sub-epic>` stays a reliable
//! grouping query. A genuine gap in the rule goes through `meta.report_gap`;
//! the rule is never relaxed.

use std::fmt;

/// The taxonomy-bearing fields of a bead, borrowed for validation.
///
/// Construct from the incoming `issues.create` / `issues.update` payload and
/// hand to [`validate`]. `external_ref` is the empty string when absent (the
/// wire surface never distinguishes "missing" from "blank" — both are invalid
/// for a non-epic).
#[derive(Debug, Clone, Copy)]
pub struct BeadTaxonomy<'a> {
    /// Bead id (primary key), e.g. `hq-mod-mcp.10`.
    pub id: &'a str,
    /// `issue_type`: `epic` is exempt; everything else is a bead.
    pub issue_type: &'a str,
    /// `external_ref`: the sub-epic grouping key. Empty when absent.
    pub external_ref: &'a str,
}

/// Why a bead violates NN-16. Carries the offending values verbatim so the
/// agent sees exactly what the frontier rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaxonomyError {
    /// A non-epic bead with empty `external_ref`.
    MissingExternalRef {
        /// The rejected bead id.
        id: String,
    },
    /// A non-epic bead whose id is not `<external_ref>.<n>` (n a positive int).
    IdMismatch {
        /// The rejected bead id.
        id: String,
        /// The `external_ref` it failed to match.
        external_ref: String,
    },
}

impl fmt::Display for TaxonomyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaxonomyError::MissingExternalRef { id } => write!(
                f,
                "bead `{id}` has empty external_ref: every non-epic bead must \
                 carry external_ref = its sub-epic (NN-16)"
            ),
            TaxonomyError::IdMismatch { id, external_ref } => write!(
                f,
                "bead id `{id}` does not match `<external_ref>.<n>` for \
                 external_ref `{external_ref}` (n a positive integer, NN-16)"
            ),
        }
    }
}

impl std::error::Error for TaxonomyError {}

/// `issue_type` values treated as an epic (exempt from the bead rule).
fn is_epic(issue_type: &str) -> bool {
    issue_type.eq_ignore_ascii_case("epic")
}

/// Validate a bead against NN-16.
///
/// Epics pass unconditionally. A non-epic must have a non-empty `external_ref`
/// and an id of the form `<external_ref>.<n>` where `<n>` is a non-empty run of
/// ASCII digits (a positive integer — the suffix may not itself contain a dot).
///
/// ```
/// use gt_module_mcp::taxonomy::{validate, BeadTaxonomy};
///
/// // Well-formed bead.
/// assert!(validate(&BeadTaxonomy {
///     id: "hq-mod-mcp.10",
///     issue_type: "task",
///     external_ref: "hq-mod-mcp",
/// })
/// .is_ok());
///
/// // Epics are exempt — no external_ref required.
/// assert!(validate(&BeadTaxonomy {
///     id: "hq-mod",
///     issue_type: "epic",
///     external_ref: "",
/// })
/// .is_ok());
///
/// // Mismatched sub-epic is rejected at the frontier.
/// assert!(validate(&BeadTaxonomy {
///     id: "hq-mod-mcp.10",
///     issue_type: "task",
///     external_ref: "hq-mod-routes",
/// })
/// .is_err());
/// ```
pub fn validate(bead: &BeadTaxonomy<'_>) -> Result<(), TaxonomyError> {
    if is_epic(bead.issue_type) {
        return Ok(());
    }

    let external_ref = bead.external_ref.trim();
    if external_ref.is_empty() {
        return Err(TaxonomyError::MissingExternalRef {
            id: bead.id.to_string(),
        });
    }

    if !id_matches_sub_epic(bead.id, external_ref) {
        return Err(TaxonomyError::IdMismatch {
            id: bead.id.to_string(),
            external_ref: external_ref.to_string(),
        });
    }

    Ok(())
}

/// True when `id` is exactly `<external_ref>.<n>` with `<n>` a non-empty run of
/// ASCII digits and no trailing segment (so `hq-mod-mcp.1.2` fails for
/// sub-epic `hq-mod-mcp`).
fn id_matches_sub_epic(id: &str, external_ref: &str) -> bool {
    let Some(suffix) = id
        .strip_prefix(external_ref)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bead<'a>(id: &'a str, issue_type: &'a str, external_ref: &'a str) -> BeadTaxonomy<'a> {
        BeadTaxonomy {
            id,
            issue_type,
            external_ref,
        }
    }

    #[test]
    fn well_formed_bead_passes() {
        assert!(validate(&bead("hq-mod-mcp.10", "task", "hq-mod-mcp")).is_ok());
        assert!(validate(&bead("hq-mt-cli.7", "spike", "hq-mt-cli")).is_ok());
    }

    #[test]
    fn epic_is_exempt_even_without_external_ref() {
        assert!(validate(&bead("hq-mod", "epic", "")).is_ok());
        // Case-insensitive on issue_type.
        assert!(validate(&bead("hq-mt", "Epic", "")).is_ok());
    }

    #[test]
    fn non_epic_with_empty_external_ref_is_rejected() {
        let err = validate(&bead("hq-mod-mcp.10", "task", "")).unwrap_err();
        assert_eq!(
            err,
            TaxonomyError::MissingExternalRef {
                id: "hq-mod-mcp.10".into()
            }
        );
        // Whitespace-only counts as empty.
        assert!(matches!(
            validate(&bead("hq-mod-mcp.10", "task", "   ")),
            Err(TaxonomyError::MissingExternalRef { .. })
        ));
    }

    #[test]
    fn id_must_match_external_ref() {
        // Wrong sub-epic.
        assert!(matches!(
            validate(&bead("hq-mod-mcp.10", "task", "hq-mod-routes")),
            Err(TaxonomyError::IdMismatch { .. })
        ));
        // Bare epic as external_ref instead of the sub-epic.
        assert!(matches!(
            validate(&bead("hq-mod-mcp.10", "task", "hq-mod")),
            Err(TaxonomyError::IdMismatch { .. })
        ));
    }

    #[test]
    fn id_suffix_must_be_a_positive_integer() {
        // No numeric suffix.
        assert!(validate(&bead("hq-mod-mcp", "task", "hq-mod-mcp")).is_err());
        // Empty suffix after the dot.
        assert!(validate(&bead("hq-mod-mcp.", "task", "hq-mod-mcp")).is_err());
        // Non-numeric suffix.
        assert!(validate(&bead("hq-mod-mcp.x", "task", "hq-mod-mcp")).is_err());
        // Nested segment — suffix carries another dot.
        assert!(validate(&bead("hq-mod-mcp.1.2", "task", "hq-mod-mcp")).is_err());
    }

    #[test]
    fn prefix_must_be_followed_by_a_dot() {
        // `hq-mod-mcp10` shares the prefix bytes but has no `.` separator.
        assert!(validate(&bead("hq-mod-mcp10", "task", "hq-mod-mcp")).is_err());
        // Sibling sub-epic that extends the ref string is not a match.
        assert!(validate(&bead("hq-mod-mcp-x.1", "task", "hq-mod-mcp")).is_err());
    }
}
