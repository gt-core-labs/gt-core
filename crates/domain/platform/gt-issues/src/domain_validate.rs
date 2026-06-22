//! Runtime domain validation against the per-workspace catalog (gtcore-d81e77 H2).
//!
//! H1 (gtcore-55d5fb) landed the per-workspace `domain_catalog` table but left
//! the closed [`Domain`](crate::taxonomy::Domain) enum as the write-path arbiter.
//! H2 relaxes that: a bead's `domain[]` is validated at RUNTIME against the
//! catalog of the workspace the bead is written to — each domain must exist and be
//! `enabled` — so a *business* workspace can carry its own domains (sales, billing)
//! with no code change, and a value outside that workspace's set is rejected naming
//! the set (not the global enum).
//!
//! ## Un-seeded workspaces fall back to the enum (the H2→H3 window)
//!
//! H3 backfills catalogs for workspaces provisioned before H1; until then a live
//! workspace may have an EMPTY (or absent) catalog. Rejecting every domain there
//! would break bead creation, so an un-seeded catalog falls back to the technical
//! [`Domain::ALL`](crate::taxonomy::Domain) set — exactly the pre-H2 behaviour.
//! Once a catalog is seeded (the `default`/gtcore workspace is, at boot) it is the
//! sole arbiter. `is_seeded` (an empty table) is the same backfill probe H3 uses.

use std::collections::BTreeSet;

use gt_store_dolt::{AppError, DoltDomainCatalog};

use crate::taxonomy::Domain;

/// Validate a bead's `domains` against `catalog` (the bead workspace's
/// `domain_catalog`, reached through the same per-workspace Dolt pool the issues
/// store uses). Each domain must be a key present and `enabled` in the catalog.
///
/// When the catalog is un-seeded (no rows yet — a pre-H1 workspace pending the H3
/// backfill) the technical enum set stands in, preserving the pre-H2 behaviour so
/// bead creation never breaks mid-migration. An empty `domains` slice is a no-op
/// here (the `≥1 domain` shape rule is enforced earlier, in `validate()`).
pub async fn validate_domains(
    catalog: &DoltDomainCatalog,
    domains: &[String],
) -> Result<(), AppError> {
    if domains.is_empty() {
        return Ok(());
    }
    // `list_opt` is table-absent tolerant: `None` (no catalog table) and `Some([])`
    // (table exists but empty) both mean un-seeded → fall back to the closed enum
    // so a workspace pending the H3 backfill keeps creating beads (pre-H2 behaviour).
    let entries = catalog.list_opt().await?.unwrap_or_default();
    if entries.is_empty() {
        let enabled: BTreeSet<String> =
            Domain::ALL.iter().map(|d| d.as_str().to_string()).collect();
        return check_domains(domains, &enabled, &enabled);
    }
    // Seeded: the catalog is the arbiter. A present-but-disabled key is rejected
    // with a distinct message from an entirely-unknown one.
    let present: BTreeSet<String> = entries.iter().map(|e| e.key.clone()).collect();
    let enabled: BTreeSet<String> =
        entries.iter().filter(|e| e.enabled).map(|e| e.key.clone()).collect();
    check_domains(domains, &present, &enabled)
}

/// The pure decision, split out so it unit-tests without a Dolt server: every
/// domain must be in `enabled`. A domain in `present` but not `enabled` is
/// reported as disabled; one in neither is reported as unknown, listing the
/// workspace's enabled set so the caller sees exactly what is assignable.
fn check_domains(
    domains: &[String],
    present: &BTreeSet<String>,
    enabled: &BTreeSet<String>,
) -> Result<(), AppError> {
    for d in domains {
        if enabled.contains(d) {
            continue;
        }
        if present.contains(d) {
            return Err(AppError::Validation(format!(
                "domain '{d}' is disabled in this workspace's catalog; \
                 enabled domains: {}",
                join(enabled)
            )));
        }
        return Err(AppError::Validation(format!(
            "unknown domain '{d}' for this workspace; valid domains: {}",
            join(enabled)
        )));
    }
    Ok(())
}

/// Comma-join a sorted key set for an error message (`BTreeSet` is already sorted).
fn join(keys: &BTreeSet<String>) -> String {
    keys.iter().cloned().collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn accepts_a_domain_in_the_enabled_set() {
        let enabled = set(&["orch.merge", "store.dolt"]);
        assert!(check_domains(&["orch.merge".into()], &enabled, &enabled).is_ok());
        // Multiple, all enabled.
        assert!(check_domains(
            &["orch.merge".into(), "store.dolt".into()],
            &enabled,
            &enabled
        )
        .is_ok());
    }

    #[test]
    fn rejects_an_unknown_domain_naming_the_set() {
        let enabled = set(&["sales", "billing"]);
        let err = check_domains(&["orch.merge".into()], &enabled, &enabled).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown domain 'orch.merge'"), "got: {msg}");
        // The workspace's set is named so the caller can correct the value.
        assert!(msg.contains("sales") && msg.contains("billing"), "got: {msg}");
    }

    #[test]
    fn rejects_a_present_but_disabled_domain_distinctly() {
        let present = set(&["sales", "legacy"]);
        let enabled = set(&["sales"]);
        let err = check_domains(&["legacy".into()], &present, &enabled).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("disabled"), "got: {msg}");
        assert!(msg.contains("legacy"), "got: {msg}");
    }

    #[test]
    fn empty_domains_is_a_noop() {
        let enabled = set(&["sales"]);
        assert!(check_domains(&[], &enabled, &enabled).is_ok());
    }

    #[test]
    fn meta_gap_passes_when_in_the_enabled_set() {
        // The reserved gap domain is seeded enabled into every catalog, so the
        // auto-minted gap beads keep validating (gtcore-d81e77 acceptance).
        let enabled = set(&["meta.gap", "sales"]);
        assert!(check_domains(&["meta.gap".into()], &enabled, &enabled).is_ok());
    }
}
