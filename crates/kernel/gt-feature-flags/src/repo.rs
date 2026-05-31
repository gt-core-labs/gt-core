//! [`FeatureFlags`] repository port + an in-memory adapter.
//!
//! Per-workspace overrides live behind a port (hexagonal): the domain depends on
//! the trait, real storage (Postgres) is a separate adapter in `gt-store-pg`
//! (`hq-mod-flags.3`). [`InMemoryFeatureFlags`] is the test/dev adapter shipped
//! with the domain crate.
//!
//! Workspaces are identified by an opaque `&str` here on purpose: this is a
//! kernel crate and must not depend on the `gt-workspace` domain crate
//! (one-way dep rule). Callers pass the workspace slug; this layer never parses
//! it.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::key::FlagKey;

/// Error surface for flag storage operations.
///
/// Deliberately small and `#[non_exhaustive]` so the Postgres adapter can add a
/// `Backend` variant without breaking callers. The in-memory adapter is
/// infallible today but the trait stays fallible so adapters share one signature.
#[derive(Debug)]
#[non_exhaustive]
pub enum FlagError {
    /// A storage backend failed. Carries a human-readable cause.
    Backend(String),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagError::Backend(msg) => write!(f, "feature-flag backend error: {msg}"),
        }
    }
}

impl std::error::Error for FlagError {}

/// Per-workspace feature-flag override storage.
///
/// Stores only *overrides* — the deviation from a flag's build default. Absence
/// of an override (`None`) means "use the default"; resolution against a
/// [`FeatureFlag`](crate::FeatureFlag)'s default is done by
/// [`is_enabled`](FeatureFlagsExt::is_enabled), built on
/// [`evaluate`](crate::evaluate). Keeping the store override-only means a flag
/// flips back to its default by *clearing*, never by writing the default value.
#[async_trait]
pub trait FeatureFlags: Send + Sync {
    /// Read the override for `key` in `workspace`, if any.
    async fn get_override(
        &self,
        workspace: &str,
        key: &FlagKey,
    ) -> Result<Option<bool>, FlagError>;

    /// Set (insert or replace) the override for `key` in `workspace`.
    async fn set_override(
        &self,
        workspace: &str,
        key: &FlagKey,
        enabled: bool,
    ) -> Result<(), FlagError>;

    /// Remove the override for `key` in `workspace`, reverting it to its default.
    /// Clearing a non-existent override is a no-op (idempotent).
    async fn clear_override(&self, workspace: &str, key: &FlagKey) -> Result<(), FlagError>;

    /// List all `(key, enabled)` overrides set in `workspace`.
    async fn list_overrides(&self, workspace: &str) -> Result<Vec<(FlagKey, bool)>, FlagError>;
}

/// Convenience resolution on top of any [`FeatureFlags`] store.
///
/// Provided as an extension trait (blanket-impl'd) so every adapter gets
/// `is_enabled` for free without re-implementing the default+override merge.
#[async_trait]
pub trait FeatureFlagsExt: FeatureFlags {
    /// Effective state of `flag` in `workspace`: its override if present, else
    /// the flag's build default. Single source of truth via [`crate::evaluate`].
    async fn is_enabled(
        &self,
        workspace: &str,
        flag: &crate::FeatureFlag,
    ) -> Result<bool, FlagError> {
        let ov = self.get_override(workspace, &flag.key).await?;
        Ok(crate::evaluate(flag.default_enabled, ov))
    }
}

#[async_trait]
impl<T: FeatureFlags + ?Sized> FeatureFlagsExt for T {}

/// In-memory [`FeatureFlags`] adapter for tests and single-process dev.
///
/// Keyed by `(workspace, FlagKey)`. A `Mutex` guards the map; contention is a
/// non-issue at dev scale and it keeps the adapter `Send + Sync` without async
/// locking. Never use in production — overrides vanish on restart.
#[derive(Default)]
pub struct InMemoryFeatureFlags {
    overrides: Mutex<HashMap<(String, FlagKey), bool>>,
}

impl InMemoryFeatureFlags {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FeatureFlags for InMemoryFeatureFlags {
    async fn get_override(
        &self,
        workspace: &str,
        key: &FlagKey,
    ) -> Result<Option<bool>, FlagError> {
        let map = self.overrides.lock().expect("flag mutex poisoned");
        Ok(map.get(&(workspace.to_owned(), key.clone())).copied())
    }

    async fn set_override(
        &self,
        workspace: &str,
        key: &FlagKey,
        enabled: bool,
    ) -> Result<(), FlagError> {
        let mut map = self.overrides.lock().expect("flag mutex poisoned");
        map.insert((workspace.to_owned(), key.clone()), enabled);
        Ok(())
    }

    async fn clear_override(&self, workspace: &str, key: &FlagKey) -> Result<(), FlagError> {
        let mut map = self.overrides.lock().expect("flag mutex poisoned");
        map.remove(&(workspace.to_owned(), key.clone()));
        Ok(())
    }

    async fn list_overrides(&self, workspace: &str) -> Result<Vec<(FlagKey, bool)>, FlagError> {
        let map = self.overrides.lock().expect("flag mutex poisoned");
        let mut out: Vec<(FlagKey, bool)> = map
            .iter()
            .filter(|((ws, _), _)| ws == workspace)
            .map(|((_, k), v)| (k.clone(), *v))
            .collect();
        // Stable, caller-friendly order.
        out.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FeatureFlag;

    fn key(s: &str) -> FlagKey {
        FlagKey::new(s).unwrap()
    }

    #[tokio::test]
    async fn absent_override_reads_none() {
        let store = InMemoryFeatureFlags::new();
        assert_eq!(store.get_override("acme", &key("module.beads")).await.unwrap(), None);
    }

    #[tokio::test]
    async fn set_then_get_roundtrips() {
        let store = InMemoryFeatureFlags::new();
        store.set_override("acme", &key("module.beads"), false).await.unwrap();
        assert_eq!(
            store.get_override("acme", &key("module.beads")).await.unwrap(),
            Some(false)
        );
    }

    #[tokio::test]
    async fn overrides_are_workspace_scoped() {
        let store = InMemoryFeatureFlags::new();
        store.set_override("acme", &key("module.beads"), false).await.unwrap();
        // A different workspace sees no override for the same key.
        assert_eq!(store.get_override("other", &key("module.beads")).await.unwrap(), None);
    }

    #[tokio::test]
    async fn clear_reverts_to_default() {
        let store = InMemoryFeatureFlags::new();
        store.set_override("acme", &key("module.beads"), false).await.unwrap();
        store.clear_override("acme", &key("module.beads")).await.unwrap();
        assert_eq!(store.get_override("acme", &key("module.beads")).await.unwrap(), None);
        // Clearing again is a no-op.
        store.clear_override("acme", &key("module.beads")).await.unwrap();
    }

    #[tokio::test]
    async fn list_is_scoped_and_sorted() {
        let store = InMemoryFeatureFlags::new();
        store.set_override("acme", &key("module.rigs"), true).await.unwrap();
        store.set_override("acme", &key("module.beads"), false).await.unwrap();
        store.set_override("other", &key("module.feed"), true).await.unwrap();

        let acme = store.list_overrides("acme").await.unwrap();
        assert_eq!(
            acme,
            vec![(key("module.beads"), false), (key("module.rigs"), true)]
        );
    }

    #[tokio::test]
    async fn is_enabled_merges_default_and_override() {
        let store = InMemoryFeatureFlags::new();
        let beads = FeatureFlag::new(key("module.beads"), true);

        // No override → default.
        assert!(store.is_enabled("acme", &beads).await.unwrap());

        // Override wins.
        store.set_override("acme", &beads.key, false).await.unwrap();
        assert!(!store.is_enabled("acme", &beads).await.unwrap());

        // Other workspace still on default.
        assert!(store.is_enabled("other", &beads).await.unwrap());
    }
}
