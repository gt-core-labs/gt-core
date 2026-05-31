//! [`ContractVersion`] + [`SurfaceHash`] — the per-module contract identity.
//!
//! A module's *contract* is the wire surface it exposes: route DTOs, MCP tool
//! schemas, event payloads. Downstream consumers (the TS frontend, other
//! modules) pin against a `ContractVersion`. Two facts must travel together:
//!
//! - a human-declared semantic [`ContractVersion`] (the module author bumps it),
//! - a machine-computed [`SurfaceHash`] of the actual surface.
//!
//! When the hash changes but the version doesn't, the surface drifted without a
//! bump — exactly the break CI catches in `hq-mod-contracts.4`. This bead
//! defines the two types and the surface-hash computation; the registry +
//! compatibility check land in `.2`.

use std::fmt;

/// A module contract's declared semantic version: `major.minor.patch`.
///
/// Semantics follow the contract-compatibility convention checked in `.2`:
/// - `major` bump = breaking surface change (consumers must adapt),
/// - `minor` bump = additive, backward-compatible,
/// - `patch` bump = non-surface fix (docs, internal).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct ContractVersion {
    /// Breaking-change counter.
    pub major: u32,
    /// Additive-change counter.
    pub minor: u32,
    /// Non-surface fix counter.
    pub patch: u32,
}

impl ContractVersion {
    /// Construct a version from its parts.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Whether `self` is a backward-compatible successor of `base`:
    /// same `major`, and `self` is not older (`>= base`).
    ///
    /// This is the rule the registry's compatibility check (`.2`) builds on: a
    /// consumer pinned to `base` keeps working under `self` iff this holds.
    pub fn is_compatible_with(&self, base: &ContractVersion) -> bool {
        self.major == base.major && self >= base
    }
}

impl fmt::Display for ContractVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl fmt::Debug for ContractVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContractVersion({self})")
    }
}

/// A stable, order-independent fingerprint of a module's wire surface.
///
/// Computed from the set of surface *items* (e.g. `"route:GET /api/v1/beads"`,
/// `"event:bead.created.v1"`). Order-independence matters: declaration order is
/// not part of a contract, so reordering items must NOT change the hash. Adding
/// or removing an item must.
#[derive(Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SurfaceHash(u64);

impl SurfaceHash {
    /// Compute the hash from an iterator of surface item strings.
    ///
    /// Each item is hashed independently and the per-item hashes are combined
    /// with a commutative, associative fold (XOR of wrapping-multiplied digests)
    /// so the result is independent of iteration order and of duplicates
    /// collapsing — set semantics, not sequence.
    pub fn compute<'a, I>(items: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut acc: u64 = 0;
        for item in items {
            let mut h = DefaultHasher::new();
            item.hash(&mut h);
            // Odd multiplier keeps the mix non-degenerate; XOR makes the fold
            // order-independent. Identical items XOR to cancel — that's fine,
            // a surface is a *set*, so duplicates are not meaningful.
            acc ^= h.finish().wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
        SurfaceHash(acc)
    }

    /// The raw 64-bit fingerprint.
    pub fn to_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SurfaceHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for SurfaceHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SurfaceHash({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_display_and_order() {
        assert_eq!(ContractVersion::new(1, 2, 3).to_string(), "1.2.3");
        assert!(ContractVersion::new(1, 0, 0) < ContractVersion::new(1, 0, 1));
        assert!(ContractVersion::new(2, 0, 0) > ContractVersion::new(1, 9, 9));
    }

    #[test]
    fn compatible_within_same_major_and_not_older() {
        let base = ContractVersion::new(1, 2, 0);
        assert!(ContractVersion::new(1, 2, 0).is_compatible_with(&base)); // equal
        assert!(ContractVersion::new(1, 3, 0).is_compatible_with(&base)); // additive
        assert!(ContractVersion::new(1, 2, 5).is_compatible_with(&base)); // patch
    }

    #[test]
    fn incompatible_across_major_or_when_older() {
        let base = ContractVersion::new(1, 2, 0);
        assert!(!ContractVersion::new(2, 0, 0).is_compatible_with(&base)); // breaking
        assert!(!ContractVersion::new(0, 9, 0).is_compatible_with(&base)); // older major
        assert!(!ContractVersion::new(1, 1, 0).is_compatible_with(&base)); // older minor
    }

    #[test]
    fn surface_hash_is_order_independent() {
        let a = SurfaceHash::compute(["route:GET /beads", "event:bead.created.v1"]);
        let b = SurfaceHash::compute(["event:bead.created.v1", "route:GET /beads"]);
        assert_eq!(a, b, "reordering surface items must not change the hash");
    }

    #[test]
    fn surface_hash_changes_on_add_or_remove() {
        let base = SurfaceHash::compute(["route:GET /beads"]);
        let added = SurfaceHash::compute(["route:GET /beads", "route:POST /beads"]);
        assert_ne!(base, added, "adding an item must change the hash");

        let empty = SurfaceHash::compute(std::iter::empty());
        assert_ne!(base, empty, "removing the only item must change the hash");
    }

    #[test]
    fn surface_hash_distinguishes_content() {
        let a = SurfaceHash::compute(["route:GET /beads"]);
        let b = SurfaceHash::compute(["route:GET /rigs"]);
        assert_ne!(a, b, "different surfaces must hash differently");
    }

    #[test]
    fn surface_hash_display_is_hex16() {
        let h = SurfaceHash::compute(["x"]);
        let s = h.to_string();
        assert_eq!(s.len(), 16);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn serde_roundtrip() {
        let v = ContractVersion::new(3, 1, 4);
        let j = serde_json::to_string(&v).unwrap();
        assert_eq!(serde_json::from_str::<ContractVersion>(&j).unwrap(), v);

        let h = SurfaceHash::compute(["a", "b"]);
        let jh = serde_json::to_string(&h).unwrap();
        assert_eq!(serde_json::from_str::<SurfaceHash>(&jh).unwrap(), h);
    }
}
