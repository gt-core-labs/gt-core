//! Semver enforcement over a module's surface (`hq-mod-contracts.4`).
//!
//! The drift check in [`crate::DriftCheck`] is deliberately coarse: it only asks
//! *did the surface move without any bump*. This module is the stricter half —
//! it classifies *how* the surface moved and demands the matching version bump:
//!
//! - an item **removed or changed** → a **breaking** change → the **major**
//!   version must increase;
//! - items only **added** → an **additive** change → at least the **minor**
//!   version must increase;
//! - no surface change → any non-regressing version is fine (patch bumps,
//!   or no bump at all).
//!
//! Unlike the hash-based [`crate::DriftCheck`], semver classification needs the
//! actual surface *item sets* (a hash can't tell a removal from an addition), so
//! [`enforce`] takes the old and new item lists directly.
//!
//! ## The per-major version file
//!
//! A breaking change starts a new contract major, and each major is frozen in
//! its own file so old consumers keep a stable reference. [`version_file_name`]
//! gives the convention (`contract.v{major}.json`); CI writes the new file when
//! a breaking change bumps major.

use crate::ContractVersion;
use std::collections::BTreeSet;

/// How a module's surface moved between two revisions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SurfaceChange {
    /// The surface item set is identical.
    Unchanged,
    /// Items were only added — nothing removed or changed. Backward compatible.
    Additive,
    /// An item was removed (or changed, which reads as a remove + add). Breaking.
    Breaking,
}

impl SurfaceChange {
    /// Classify the change from the old and new surface item sets.
    ///
    /// Items are compared as whole strings: a changed DTO surfaces as one item
    /// disappearing and a differently-spelled one appearing, so a *change* is
    /// caught as a removal and classified [`Breaking`](SurfaceChange::Breaking).
    pub fn classify<'a, O, N>(old: O, new: N) -> Self
    where
        O: IntoIterator<Item = &'a str>,
        N: IntoIterator<Item = &'a str>,
    {
        let old: BTreeSet<&str> = old.into_iter().collect();
        let new: BTreeSet<&str> = new.into_iter().collect();

        let removed = old.difference(&new).next().is_some();
        let added = new.difference(&old).next().is_some();

        match (removed, added) {
            (true, _) => SurfaceChange::Breaking,
            (false, true) => SurfaceChange::Additive,
            (false, false) => SurfaceChange::Unchanged,
        }
    }
}

/// The verdict of [`enforce`]: whether the version bump matches the surface change.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SemverVerdict {
    /// The version bump is consistent with the surface change.
    Ok,
    /// The new version is older than the old one — versions must never regress.
    Regressed,
    /// A breaking surface change that did not bump the major version.
    MajorRequired,
    /// An additive surface change that did not bump at least the minor version.
    MinorRequired,
}

impl SemverVerdict {
    /// Whether the verdict passes the CI gate.
    pub fn is_ok(self) -> bool {
        matches!(self, SemverVerdict::Ok)
    }

    /// A one-line message suitable for a CI log.
    pub fn report(self) -> &'static str {
        match self {
            SemverVerdict::Ok => "version bump matches the surface change",
            SemverVerdict::Regressed => "contract version regressed — the new version is older",
            SemverVerdict::MajorRequired => {
                "breaking surface change (an item was removed or changed) requires a major version bump"
            }
            SemverVerdict::MinorRequired => {
                "additive surface change requires at least a minor version bump"
            }
        }
    }
}

/// Enforce that the version bump matches the surface change.
///
/// Compares the old and new surface item sets, classifies the change via
/// [`SurfaceChange::classify`], and checks the version moved accordingly:
///
/// - [`Breaking`](SurfaceChange::Breaking) → `new.major > old.major`,
/// - [`Additive`](SurfaceChange::Additive) → the major or minor increased,
/// - [`Unchanged`](SurfaceChange::Unchanged) → `new >= old` (any/no bump).
pub fn enforce<'a, O, N>(
    old_version: ContractVersion,
    new_version: ContractVersion,
    old_surface: O,
    new_surface: N,
) -> SemverVerdict
where
    O: IntoIterator<Item = &'a str>,
    N: IntoIterator<Item = &'a str>,
{
    if new_version < old_version {
        return SemverVerdict::Regressed;
    }

    match SurfaceChange::classify(old_surface, new_surface) {
        SurfaceChange::Breaking => {
            if new_version.major > old_version.major {
                SemverVerdict::Ok
            } else {
                SemverVerdict::MajorRequired
            }
        }
        SurfaceChange::Additive => {
            let bumped_major_or_minor = new_version.major > old_version.major
                || (new_version.major == old_version.major
                    && new_version.minor > old_version.minor);
            if bumped_major_or_minor {
                SemverVerdict::Ok
            } else {
                SemverVerdict::MinorRequired
            }
        }
        SurfaceChange::Unchanged => SemverVerdict::Ok,
    }
}

/// The frozen-baseline file name for a contract major version.
///
/// Each major lives in its own file (`contract.v1.json`, `contract.v2.json`, …)
/// so a breaking bump adds a new file rather than overwriting the reference old
/// consumers still pin to.
pub fn version_file_name(version: ContractVersion) -> String {
    format!("contract.v{}.json", version.major)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "route:GET /beads";
    const B: &str = "event:bead.created.v1";
    const C: &str = "route:POST /beads";

    #[test]
    fn classify_unchanged() {
        assert_eq!(SurfaceChange::classify([A, B], [B, A]), SurfaceChange::Unchanged);
    }

    #[test]
    fn classify_additive() {
        assert_eq!(SurfaceChange::classify([A], [A, C]), SurfaceChange::Additive);
    }

    #[test]
    fn classify_removal_is_breaking() {
        assert_eq!(SurfaceChange::classify([A, B], [A]), SurfaceChange::Breaking);
    }

    #[test]
    fn classify_change_reads_as_breaking() {
        // "route:GET /beads" -> "route:GET /items" is a remove + add.
        assert_eq!(
            SurfaceChange::classify([A], ["route:GET /items"]),
            SurfaceChange::Breaking
        );
    }

    #[test]
    fn breaking_needs_major_bump() {
        let v1 = ContractVersion::new(1, 4, 0);
        // removed B → breaking, only minor bumped → fail.
        assert_eq!(
            enforce(v1, ContractVersion::new(1, 5, 0), [A, B], [A]),
            SemverVerdict::MajorRequired
        );
        // major bumped → ok.
        assert_eq!(
            enforce(v1, ContractVersion::new(2, 0, 0), [A, B], [A]),
            SemverVerdict::Ok
        );
    }

    #[test]
    fn additive_needs_at_least_minor_bump() {
        let v1 = ContractVersion::new(1, 4, 0);
        // added C → additive, only patch bumped → fail.
        assert_eq!(
            enforce(v1, ContractVersion::new(1, 4, 1), [A], [A, C]),
            SemverVerdict::MinorRequired
        );
        // minor bumped → ok.
        assert_eq!(
            enforce(v1, ContractVersion::new(1, 5, 0), [A], [A, C]),
            SemverVerdict::Ok
        );
        // major bumped also satisfies an additive change.
        assert_eq!(
            enforce(v1, ContractVersion::new(2, 0, 0), [A], [A, C]),
            SemverVerdict::Ok
        );
    }

    #[test]
    fn unchanged_surface_allows_any_non_regressing_version() {
        let v1 = ContractVersion::new(1, 4, 0);
        // no surface change, patch bump → ok.
        assert_eq!(
            enforce(v1, ContractVersion::new(1, 4, 1), [A, B], [A, B]),
            SemverVerdict::Ok
        );
        // no surface change, no bump → ok.
        assert_eq!(
            enforce(v1, v1, [A, B], [A, B]),
            SemverVerdict::Ok
        );
    }

    #[test]
    fn regression_fails_before_any_classification() {
        let v2 = ContractVersion::new(2, 0, 0);
        assert_eq!(
            enforce(v2, ContractVersion::new(1, 9, 0), [A], [A, C]),
            SemverVerdict::Regressed
        );
    }

    #[test]
    fn verdict_reports_are_actionable() {
        assert!(SemverVerdict::MajorRequired.report().contains("major"));
        assert!(SemverVerdict::MinorRequired.report().contains("minor"));
        assert!(SemverVerdict::Ok.is_ok());
        assert!(!SemverVerdict::MajorRequired.is_ok());
    }

    #[test]
    fn version_file_per_major() {
        assert_eq!(version_file_name(ContractVersion::new(1, 9, 3)), "contract.v1.json");
        assert_eq!(version_file_name(ContractVersion::new(2, 0, 0)), "contract.v2.json");
    }
}
