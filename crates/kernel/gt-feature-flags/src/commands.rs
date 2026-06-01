//! Tool-argument structs for the `feature.*` MCP tools (hq-mod-flags.4).
//!
//! Each struct is the wire shape an MCP client sends; [`validate`](EnableFeature::validate)
//! is the shape-only guard the `*.validate` tools run and the `*.execute` tools
//! run before touching the store. The transport-free handlers in
//! [`crate::handlers`] drive them against a [`FeatureFlags`](crate::FeatureFlags)
//! adapter.
//!
//! ## Workspace is NOT a wire field
//!
//! Per docs/04 the workspace is **server-injected from the auth context**, never
//! carried in the MCP payload — a caller cannot toggle another tenant's flags by
//! spoofing a `workspace` field, because there is none. The handlers therefore
//! take the workspace as a separate, server-supplied argument (the same way the
//! issues handlers take the scope `actor`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::key::{FlagKey, FlagKeyError};
use crate::repo::FlagError;

/// Error from a `feature.*` command: a shape rejection (caller fault) or a
/// storage backend fault. The server bin maps the two onto its transport error
/// (invalid_request vs internal_error).
#[derive(Debug)]
pub enum FeatureCmdError {
    /// The argument shape was rejected — e.g. a malformed [`FlagKey`].
    Validation(String),
    /// The storage backend failed.
    Backend(String),
}

impl std::fmt::Display for FeatureCmdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureCmdError::Validation(m) => write!(f, "validation failed: {m}"),
            FeatureCmdError::Backend(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for FeatureCmdError {}

impl From<FlagKeyError> for FeatureCmdError {
    fn from(e: FlagKeyError) -> Self {
        FeatureCmdError::Validation(e.to_string())
    }
}

impl From<FlagError> for FeatureCmdError {
    fn from(e: FlagError) -> Self {
        FeatureCmdError::Backend(e.to_string())
    }
}

/// Input for `feature.enable` — turn a flag on for the calling workspace by
/// writing an `enabled = true` override.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EnableFeature {
    /// The dotted-kebab flag key, e.g. `module.beads` or `feature.dark-mode`.
    /// Validated as a [`FlagKey`] at the frontier so a malformed key never
    /// reaches storage.
    pub key: String,
}

impl EnableFeature {
    /// Shape-only validation: the key parses as a [`FlagKey`]. Returns the parsed
    /// key so the handler does not re-parse.
    pub fn validate(&self) -> Result<FlagKey, FeatureCmdError> {
        Ok(FlagKey::new(self.key.as_str())?)
    }
}

/// Input for `feature.disable` — turn a flag off for the calling workspace by
/// writing an `enabled = false` override. (Reverting to the build default is a
/// distinct *clear* operation, not exposed by this bead.)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DisableFeature {
    /// The dotted-kebab flag key to disable. Validated as a [`FlagKey`].
    pub key: String,
}

impl DisableFeature {
    /// Shape-only validation: the key parses as a [`FlagKey`].
    pub fn validate(&self) -> Result<FlagKey, FeatureCmdError> {
        Ok(FlagKey::new(self.key.as_str())?)
    }
}

/// Input for `feature.list` — snapshot the overrides set in the calling
/// workspace. Takes no arguments: the workspace is server-injected.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ListFeatures {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enable_accepts_well_formed_key() {
        let k = EnableFeature { key: "module.beads".into() }.validate().unwrap();
        assert_eq!(k.as_str(), "module.beads");
    }

    #[test]
    fn enable_rejects_malformed_key() {
        assert!(matches!(
            EnableFeature { key: "Bad.Key".into() }.validate(),
            Err(FeatureCmdError::Validation(_))
        ));
        assert!(EnableFeature { key: String::new() }.validate().is_err());
    }

    #[test]
    fn disable_validates_the_same_way() {
        assert!(DisableFeature { key: "feature.dark-mode".into() }.validate().is_ok());
        assert!(DisableFeature { key: "a..b".into() }.validate().is_err());
    }

    #[test]
    fn flag_error_maps_to_backend() {
        let e: FeatureCmdError = FlagError::Backend("boom".into()).into();
        assert!(matches!(e, FeatureCmdError::Backend(_)));
    }
}
