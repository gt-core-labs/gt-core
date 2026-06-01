//! Transport-free handlers for the `feature.*` tools (hq-mod-flags.4).
//!
//! Mirror the issues handlers (`gt-issues`): no rmcp/scope/audit concerns — those
//! belong to the server bin. Each handler runs `validate()` (always), then on the
//! execute path drives the [`FeatureFlags`] override store. The store is borrowed
//! as a trait object so the binary supplies whichever adapter
//! ([`PgFeatureFlags`](crate::PgFeatureFlags) in prod, the in-memory one in tests).
//!
//! `workspace` is the server-injected tenant (see [`crate::commands`]); it is a
//! handler argument, never a wire field.

use serde::Serialize;

use crate::commands::{DisableFeature, EnableFeature, FeatureCmdError, ListFeatures};
use crate::repo::FeatureFlags;

/// One override row in a [`run_list_features`] snapshot: the flag key and the
/// state it was overridden to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeatureOverride {
    /// The dotted-kebab flag key.
    pub key: String,
    /// The overridden state (`true` = forced on, `false` = forced off).
    pub enabled: bool,
}

/// `feature.enable`: validate the key, then (execute only) write an
/// `enabled = true` override for `workspace`.
pub async fn run_enable_feature(
    flags: &dyn FeatureFlags,
    args: &EnableFeature,
    workspace: &str,
    validate_only: bool,
) -> Result<(), FeatureCmdError> {
    let key = args.validate()?;
    if validate_only {
        return Ok(());
    }
    flags.set_override(workspace, &key, true).await?;
    Ok(())
}

/// `feature.disable`: validate the key, then (execute only) write an
/// `enabled = false` override for `workspace`.
pub async fn run_disable_feature(
    flags: &dyn FeatureFlags,
    args: &DisableFeature,
    workspace: &str,
    validate_only: bool,
) -> Result<(), FeatureCmdError> {
    let key = args.validate()?;
    if validate_only {
        return Ok(());
    }
    flags.set_override(workspace, &key, false).await?;
    Ok(())
}

/// `feature.list`: snapshot the overrides set in `workspace`. A read — no
/// validate/execute split. Keys come back already in stable order from the store.
pub async fn run_list_features(
    flags: &dyn FeatureFlags,
    _args: &ListFeatures,
    workspace: &str,
) -> Result<Vec<FeatureOverride>, FeatureCmdError> {
    let rows = flags.list_overrides(workspace).await?;
    Ok(rows
        .into_iter()
        .map(|(key, enabled)| FeatureOverride { key: key.into(), enabled })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryFeatureFlags;

    #[tokio::test]
    async fn enable_then_list_roundtrips() {
        let store = InMemoryFeatureFlags::new();
        run_enable_feature(&store, &EnableFeature { key: "module.beads".into() }, "acme", false)
            .await
            .unwrap();
        let listed = run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap();
        assert_eq!(listed, vec![FeatureOverride { key: "module.beads".into(), enabled: true }]);
    }

    #[tokio::test]
    async fn disable_writes_false_override() {
        let store = InMemoryFeatureFlags::new();
        run_disable_feature(&store, &DisableFeature { key: "module.beads".into() }, "acme", false)
            .await
            .unwrap();
        let listed = run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap();
        assert_eq!(listed, vec![FeatureOverride { key: "module.beads".into(), enabled: false }]);
    }

    #[tokio::test]
    async fn validate_only_writes_nothing() {
        let store = InMemoryFeatureFlags::new();
        run_enable_feature(&store, &EnableFeature { key: "module.beads".into() }, "acme", true)
            .await
            .unwrap();
        assert!(run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn validate_only_still_rejects_a_bad_key() {
        let store = InMemoryFeatureFlags::new();
        assert!(
            run_enable_feature(&store, &EnableFeature { key: "Bad.Key".into() }, "acme", true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn list_is_workspace_scoped() {
        let store = InMemoryFeatureFlags::new();
        run_enable_feature(&store, &EnableFeature { key: "module.beads".into() }, "acme", false)
            .await
            .unwrap();
        assert!(run_list_features(&store, &ListFeatures::default(), "other").await.unwrap().is_empty());
    }
}
