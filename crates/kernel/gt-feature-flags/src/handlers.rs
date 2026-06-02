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

use std::collections::HashSet;

use serde::Serialize;

use crate::commands::{DisableFeature, EnableFeature, FeatureCmdError, ListFeatures};
use crate::deps::{blocking_dependents, module_id_of, ModuleDepGraph};
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

/// `feature.disable`: validate the key and the **dependency safety** of the
/// disable, then (execute only) write an `enabled = false` override for
/// `workspace`.
///
/// ## Dep-aware rejection
///
/// `gt-module`'s builder rejects, at composition, a module that depends on a
/// disabled one (`hq-mod-core.7`). But `feature.disable` flips a flag at runtime,
/// after the graph was built: disabling `module.<x>` while an enabled
/// `module.<y>` still depends on `<x>` would strand `<y>`. When the composition
/// root supplies a [`ModuleDepGraph`] (`graph = Some(_)`) and the key targets a
/// module, this rejects the disable — on **both** the validate and execute paths,
/// before any write — if any still-enabled module directly depends on the target.
///
/// "Enabled" is resolved from override presence: a dependent counts as live
/// unless it carries its own `false` override (modules default to on, so absence
/// of a disable override means it is running). A caller holding the full flag
/// registry could resolve defaults precisely; the pure predicate
/// ([`blocking_dependents`]) takes the resolver as a closure so that refinement
/// needs no change here.
///
/// With `graph = None` (no graph wired, e.g. a non-module flag store) the check
/// is skipped and the disable behaves as before — back-compatible with callers
/// that have no module graph.
pub async fn run_disable_feature(
    flags: &dyn FeatureFlags,
    args: &DisableFeature,
    workspace: &str,
    graph: Option<&ModuleDepGraph>,
    validate_only: bool,
) -> Result<(), FeatureCmdError> {
    let key = args.validate()?;

    if let (Some(graph), Some(target)) = (graph, module_id_of(&key)) {
        let blocking = enabled_dependents(flags, workspace, graph, target).await?;
        if !blocking.is_empty() {
            return Err(FeatureCmdError::Validation(format!(
                "cannot disable module `{target}`: still required by enabled module(s) [{}] — \
                 disable the dependent(s) first",
                blocking.join(", ")
            )));
        }
    }

    if validate_only {
        return Ok(());
    }
    flags.set_override(workspace, &key, false).await?;
    Ok(())
}

/// The direct dependents of module `target` that are still enabled in
/// `workspace`, and would therefore be orphaned by disabling it.
///
/// Resolves "enabled" as "not explicitly disabled by a `false` override". Reads
/// the workspace's overrides once and treats every dependent without a disable
/// override as live (modules default to on). Returns owned ids for the rejection
/// message, in the deterministic order [`ModuleDepGraph`] keeps.
async fn enabled_dependents(
    flags: &dyn FeatureFlags,
    workspace: &str,
    graph: &ModuleDepGraph,
    target: &str,
) -> Result<Vec<String>, FeatureCmdError> {
    let overrides = flags.list_overrides(workspace).await?;
    let disabled: HashSet<&str> = overrides
        .iter()
        .filter(|(_, enabled)| !enabled)
        .filter_map(|(key, _)| module_id_of(key))
        .collect();
    Ok(blocking_dependents(graph, target, |dep| !disabled.contains(dep))
        .into_iter()
        .map(str::to_owned)
        .collect())
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
        run_disable_feature(
            &store,
            &DisableFeature { key: "module.beads".into() },
            "acme",
            None,
            false,
        )
        .await
        .unwrap();
        let listed = run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap();
        assert_eq!(listed, vec![FeatureOverride { key: "module.beads".into(), enabled: false }]);
    }

    /// Graph: `module.rigs` depends on `module.beads`.
    fn rigs_on_beads() -> ModuleDepGraph {
        ModuleDepGraph::from_edges([("rigs".to_string(), "beads".to_string())])
    }

    #[tokio::test]
    async fn disable_rejected_when_enabled_module_depends_on_it() {
        let store = InMemoryFeatureFlags::new();
        let graph = rigs_on_beads();
        // rigs is enabled (no override) and depends on beads → disabling beads
        // is rejected on the validate path, before any write.
        let err = run_disable_feature(
            &store,
            &DisableFeature { key: "module.beads".into() },
            "acme",
            Some(&graph),
            true,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("module `beads`"), "got: {msg}");
        assert!(msg.contains("rigs"), "got: {msg}");
        // Nothing was written.
        assert!(run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn disable_allowed_once_dependent_is_disabled() {
        let store = InMemoryFeatureFlags::new();
        let graph = rigs_on_beads();
        // Disable the dependent first (it has no dependents itself).
        run_disable_feature(
            &store,
            &DisableFeature { key: "module.rigs".into() },
            "acme",
            Some(&graph),
            false,
        )
        .await
        .unwrap();
        // Now disabling beads is dep-safe: rigs is already off.
        run_disable_feature(
            &store,
            &DisableFeature { key: "module.beads".into() },
            "acme",
            Some(&graph),
            false,
        )
        .await
        .unwrap();
        let listed = run_list_features(&store, &ListFeatures::default(), "acme").await.unwrap();
        assert_eq!(
            listed,
            vec![
                FeatureOverride { key: "module.beads".into(), enabled: false },
                FeatureOverride { key: "module.rigs".into(), enabled: false },
            ]
        );
    }

    #[tokio::test]
    async fn disable_safe_for_leaf_and_for_feature_keys() {
        let store = InMemoryFeatureFlags::new();
        let graph = rigs_on_beads();
        // rigs is a leaf (nothing depends on it) → safe.
        run_disable_feature(
            &store,
            &DisableFeature { key: "module.rigs".into() },
            "acme",
            Some(&graph),
            false,
        )
        .await
        .unwrap();
        // A non-module feature flag has no dependency edges → always safe.
        run_disable_feature(
            &store,
            &DisableFeature { key: "feature.dark-mode".into() },
            "acme",
            Some(&graph),
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn disable_dep_check_skipped_without_graph() {
        let store = InMemoryFeatureFlags::new();
        // No graph wired → the dependency check is skipped even though rigs would
        // depend on beads, preserving the pre-gap behaviour.
        run_disable_feature(
            &store,
            &DisableFeature { key: "module.beads".into() },
            "acme",
            None,
            false,
        )
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
