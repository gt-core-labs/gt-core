//! Feature flags — per-workspace module + capability toggles.
//!
//! Tenant-aware enable/disable for modules and features. This crate is pure
//! domain types + evaluation logic; the repository port + in-memory cache land
//! in `hq-mod-flags.2` and the Postgres adapter in `.3`.
//!
//! ## Landed so far
//!
//! - [`FlagKey`] — dotted-kebab flag identity (`module.beads`, `feature.dark-mode`).
//! - [`FeatureFlag`] — a flag definition with its default state.
//! - [`evaluate`] — resolve a flag's effective state from default + optional override.
//!
//! Part of the gt-core module system (`hq-mod`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod key;

pub use key::{FlagKey, FlagKeyError};

/// A feature flag definition: its identity plus the state it takes when no
/// override exists.
///
/// The default is the value baked into the build (a module ships enabled or
/// disabled by default). Per-workspace overrides — applied in [`evaluate`] —
/// come from the repository (`hq-mod-flags.2`); this type carries only the
/// definition, so it is cheap to clone and safe to surface in admin listings.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeatureFlag {
    /// Stable flag identity.
    pub key: FlagKey,
    /// State used when no per-workspace override is present.
    pub default_enabled: bool,
}

impl FeatureFlag {
    /// Construct a flag definition from an already-validated key.
    pub fn new(key: FlagKey, default_enabled: bool) -> Self {
        Self {
            key,
            default_enabled,
        }
    }

    /// Effective state given an optional per-workspace override.
    ///
    /// Convenience for [`evaluate`] using this flag's own default. `Some(_)`
    /// wins over the default; `None` falls back to it.
    pub fn is_enabled(&self, override_value: Option<bool>) -> bool {
        evaluate(self.default_enabled, override_value)
    }
}

/// Resolve a flag's effective state.
///
/// Precedence is explicit and total: a per-workspace `override_value` of
/// `Some(_)` always wins; `None` falls back to the build `default`. This is the
/// single evaluation rule the repository layers build on — keeping it a free
/// function makes it trivially testable and reusable without a `FeatureFlag`.
pub fn evaluate(default: bool, override_value: Option<bool>) -> bool {
    override_value.unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flag(key: &str, default_enabled: bool) -> FeatureFlag {
        FeatureFlag::new(FlagKey::new(key).unwrap(), default_enabled)
    }

    #[test]
    fn override_wins_over_default() {
        assert!(evaluate(false, Some(true)));
        assert!(!evaluate(true, Some(false)));
    }

    #[test]
    fn none_falls_back_to_default() {
        assert!(evaluate(true, None));
        assert!(!evaluate(false, None));
    }

    #[test]
    fn flag_is_enabled_uses_own_default() {
        let on_by_default = flag("module.beads", true);
        assert!(on_by_default.is_enabled(None));
        assert!(!on_by_default.is_enabled(Some(false)));

        let off_by_default = flag("feature.dark-mode", false);
        assert!(!off_by_default.is_enabled(None));
        assert!(off_by_default.is_enabled(Some(true)));
    }

    #[test]
    fn serde_roundtrip() {
        let f = flag("module.gt-rig", true);
        let json = serde_json::to_string(&f).unwrap();
        let back: FeatureFlag = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }
}
