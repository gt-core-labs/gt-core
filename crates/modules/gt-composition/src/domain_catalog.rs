//! Per-workspace domain-catalog seeding (gtcore-55d5fb H1 — the host half).
//!
//! The kernel crate [`gt_store_dolt`] owns the `domain_catalog` table, the CRUD
//! repo ([`DoltDomainCatalog`]) and the generic business template. What it
//! CANNOT own is the *technical* seed: that set is the [`Domain`] enum, which
//! lives in the `gt-issues` PLATFORM crate — a kernel crate may not depend on it
//! (docs/03 Rule 4). So this composition-tier module is the seam that turns
//! `Domain::ALL` into [`DomainEntry`] rows and drives the boot seed, keeping the
//! enum the single source of truth for the technical catalog.
//!
//! Two entry points:
//! - [`technical_entries`] — `Domain::ALL` as catalog rows (the `default`/gtcore
//!   seed). `meta.gap` is included because it is a `Domain` variant; it is marked
//!   reserved so a UI hides it from the editable set.
//! - [`seed_default_workspace`] — ensure the schema + seed the technical set on a
//!   default-workspace [`DoltIssues`] pool at boot. Idempotent.

use gt_issues::Domain;
use gt_store_dolt::{AppError, DoltDomainCatalog, DoltIssues, DomainEntry};

/// The technical domain set as catalog rows, built from [`Domain::ALL`] so the
/// catalog never diverges from the enum (gtcore-55d5fb: "Reutilizar la lista del
/// enum como fuente del seed"). The dotted key is the `Domain` wire form; the
/// label defaults to the key (a serviceable display name until an operator edits
/// it); the tier is the segment before the first `.`; `meta.gap` rides in as the
/// reserved row every catalog must carry.
pub fn technical_entries() -> Vec<DomainEntry> {
    Domain::ALL
        .iter()
        .map(|d| DomainEntry {
            key: d.as_str().to_string(),
            label: d.as_str().to_string(),
            tier: Some(d.tier().to_string()),
            enabled: true,
            reserved: d.is_reserved(),
        })
        .collect()
}

/// Ensure the `default` workspace's `domain_catalog` exists and is seeded with
/// the technical set. Runs at boot over the default-workspace [`DoltIssues`]
/// pool (`hq`). Idempotent: the schema guard is a no-op once the table exists,
/// and the seed re-upserts by key (refreshing labels/tiers without duplicating
/// rows). Re-seeds an empty table too, so a workspace whose catalog was created
/// but never seeded self-heals on the next boot.
pub async fn seed_default_workspace(store: &DoltIssues) -> Result<(), AppError> {
    let catalog = DoltDomainCatalog::new(store.pool().clone());
    catalog.ensure_schema().await?;
    catalog.seed(&technical_entries()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn technical_entries_cover_the_enum_and_carry_reserved_gap() {
        let rows = technical_entries();
        // One row per Domain variant — the catalog mirrors the enum 1:1.
        assert_eq!(rows.len(), Domain::ALL.len());
        // meta.gap is present and reserved; everything else is non-reserved.
        let gap = rows.iter().find(|e| e.key == "meta.gap").expect("meta.gap seeded");
        assert!(gap.reserved);
        assert_eq!(gap.tier.as_deref(), Some("meta"));
        assert_eq!(rows.iter().filter(|e| e.reserved).count(), 1);
        // Tier derivation: a dotted technical key splits on the first '.'.
        let merge = rows.iter().find(|e| e.key == "orch.merge").expect("orch.merge seeded");
        assert_eq!(merge.tier.as_deref(), Some("orch"));
        assert!(merge.enabled && !merge.reserved);
        // Keys are unique.
        let mut keys: Vec<&str> = rows.iter().map(|e| e.key.as_str()).collect();
        keys.sort_unstable();
        let before = keys.len();
        keys.dedup();
        assert_eq!(before, keys.len(), "duplicate technical domain key");
    }
}
