//! Frozen contract baseline + drift check (`hq-mod-contracts.3`).
//!
//! A module's contract identity is its declared [`ContractVersion`] paired with
//! the [`SurfaceHash`] of its live wire surface. This module lets a project
//! *freeze* that pairing into a checked-in [`ContractBaseline`] and, in CI,
//! recompute the live surface and compare — failing the build when the surface
//! drifted without a version bump.
//!
//! ## The rule CI enforces
//!
//! Given the frozen baseline and the live `(version, surface)`:
//!
//! | live vs baseline                          | outcome             | CI  |
//! |-------------------------------------------|---------------------|-----|
//! | version went backwards                    | [`Regressed`]       | ✗   |
//! | surface unchanged                         | [`Unchanged`]       | ✓   |
//! | surface changed **and** version bumped    | [`BumpedAndChanged`]| ✓   |
//! | surface changed, version **not** bumped   | [`UnbumpedDrift`]   | ✗   |
//!
//! [`Regressed`]: DriftCheck::Regressed
//! [`Unchanged`]: DriftCheck::Unchanged
//! [`BumpedAndChanged`]: DriftCheck::BumpedAndChanged
//! [`UnbumpedDrift`]: DriftCheck::UnbumpedDrift
//!
//! This is deliberately coarser than semver: it only asks *did the surface move
//! without any bump*. Whether a breaking change demands a **major** bump (not
//! just any bump) is the stricter check that lands in `hq-mod-contracts.4`.

use crate::{ContractVersion, SurfaceHash};

/// A frozen snapshot of a module's contract identity.
///
/// Serialize this to a checked-in file (one per module); CI deserializes it and
/// compares against the live surface via [`DriftCheck::evaluate`]. The pairing
/// is the whole point: a [`SurfaceHash`] alone can't tell an *intended* surface
/// change (version bumped alongside) from an accidental one (version untouched).
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct ContractBaseline {
    /// The declared contract version at freeze time.
    pub version: ContractVersion,
    /// The surface fingerprint at freeze time.
    pub surface: SurfaceHash,
}

impl ContractBaseline {
    /// Freeze a version + surface pairing into a baseline.
    pub const fn freeze(version: ContractVersion, surface: SurfaceHash) -> Self {
        Self { version, surface }
    }

    /// Compare a live contract against this baseline. See [`DriftCheck`].
    pub fn check(&self, live_version: ContractVersion, live_surface: SurfaceHash) -> DriftCheck {
        DriftCheck::evaluate(self, live_version, live_surface)
    }
}

/// The outcome of comparing a live contract against its frozen [`ContractBaseline`].
///
/// Use [`DriftCheck::is_ok`] for the pass/fail gate and [`DriftCheck::report`]
/// for a one-line CI log message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DriftCheck {
    /// Live surface matches the baseline — no contract change.
    Unchanged,
    /// Surface changed and the version was bumped past the baseline. Intended.
    BumpedAndChanged,
    /// Surface changed but the version did not move — the accidental drift CI
    /// exists to catch. Bump the version (and re-freeze the baseline).
    UnbumpedDrift,
    /// Live version is older than the baseline — versions must never regress.
    Regressed,
}

impl DriftCheck {
    /// Evaluate the drift rule. Order matters: a regressed version is reported
    /// even if the surface is otherwise unchanged, because going backwards is
    /// always a mistake.
    pub fn evaluate(
        baseline: &ContractBaseline,
        live_version: ContractVersion,
        live_surface: SurfaceHash,
    ) -> Self {
        if live_version < baseline.version {
            return DriftCheck::Regressed;
        }
        if live_surface == baseline.surface {
            return DriftCheck::Unchanged;
        }
        if live_version > baseline.version {
            DriftCheck::BumpedAndChanged
        } else {
            // Surface changed, version equal to baseline.
            DriftCheck::UnbumpedDrift
        }
    }

    /// Whether the check passes the CI gate.
    pub fn is_ok(self) -> bool {
        matches!(self, DriftCheck::Unchanged | DriftCheck::BumpedAndChanged)
    }

    /// A one-line message suitable for a CI log.
    pub fn report(self) -> &'static str {
        match self {
            DriftCheck::Unchanged => "contract unchanged",
            DriftCheck::BumpedAndChanged => "contract surface changed; version bumped — ok",
            DriftCheck::UnbumpedDrift => {
                "contract surface changed but version not bumped — bump the version and re-freeze the baseline"
            }
            DriftCheck::Regressed => "contract version regressed below the frozen baseline",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> ContractBaseline {
        ContractBaseline::freeze(
            ContractVersion::new(1, 2, 0),
            SurfaceHash::compute(["route:GET /beads", "event:bead.created.v1"]),
        )
    }

    #[test]
    fn unchanged_surface_passes() {
        let b = baseline();
        let check = b.check(
            ContractVersion::new(1, 2, 0),
            SurfaceHash::compute(["route:GET /beads", "event:bead.created.v1"]),
        );
        assert_eq!(check, DriftCheck::Unchanged);
        assert!(check.is_ok());
    }

    #[test]
    fn unchanged_surface_passes_even_with_patch_bump() {
        let b = baseline();
        // A patch bump with no surface change is fine — surface match wins.
        let check = b.check(
            ContractVersion::new(1, 2, 9),
            SurfaceHash::compute(["route:GET /beads", "event:bead.created.v1"]),
        );
        assert_eq!(check, DriftCheck::Unchanged);
    }

    #[test]
    fn surface_change_with_bump_passes() {
        let b = baseline();
        let check = b.check(
            ContractVersion::new(1, 3, 0),
            SurfaceHash::compute([
                "route:GET /beads",
                "event:bead.created.v1",
                "route:POST /beads",
            ]),
        );
        assert_eq!(check, DriftCheck::BumpedAndChanged);
        assert!(check.is_ok());
    }

    #[test]
    fn surface_change_without_bump_fails() {
        let b = baseline();
        let check = b.check(
            ContractVersion::new(1, 2, 0), // same version
            SurfaceHash::compute([
                "route:GET /beads",
                "event:bead.created.v1",
                "route:POST /beads", // surface grew
            ]),
        );
        assert_eq!(check, DriftCheck::UnbumpedDrift);
        assert!(!check.is_ok());
        assert!(check.report().contains("bump the version"));
    }

    #[test]
    fn version_regression_fails_even_if_surface_unchanged() {
        let b = baseline();
        let check = b.check(
            ContractVersion::new(1, 1, 0), // older than baseline 1.2.0
            SurfaceHash::compute(["route:GET /beads", "event:bead.created.v1"]),
        );
        assert_eq!(check, DriftCheck::Regressed);
        assert!(!check.is_ok());
    }

    #[test]
    fn baseline_serde_roundtrip() {
        let b = baseline();
        let json = serde_json::to_string(&b).unwrap();
        let back: ContractBaseline = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
    }

    #[test]
    fn frozen_baseline_json_is_stable() {
        // The serialized shape is what gets checked into the repo; pin it so an
        // accidental format change is caught here, not in every module's diff.
        let b = ContractBaseline::freeze(
            ContractVersion::new(2, 0, 1),
            SurfaceHash::compute(["x"]),
        );
        let json = serde_json::to_value(&b).unwrap();
        assert_eq!(json["version"]["major"], 2);
        assert_eq!(json["version"]["minor"], 0);
        assert_eq!(json["version"]["patch"], 1);
        // surface serializes as the raw u64 fingerprint.
        assert!(json["surface"].is_u64());
    }
}
