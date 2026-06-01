//! [`RootRegistry`] — resolves a [`WorkspaceId`] to its [`RootHandle`].

use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use gt_workspace::WorkspaceId;

use crate::RootHandle;

/// A per-workspace map from [`WorkspaceId`] to the composed [`RootHandle`].
///
/// One process serves many tenants; each resolves to its own composed
/// application. The registry hydrates a workspace's root lazily — on first
/// access ([`get_or_hydrate`](Self::get_or_hydrate)) — and lets the caller drop
/// an idle one ([`remove`](Self::remove)) so memory tracks active tenants, not
/// the catalog. Backed by a [`DashMap`] so concurrent requests for different
/// workspaces never serialize on a global lock.
///
/// This is the `hq-mt-routing.1` shell. Lazy *eviction policy* (idle teardown,
/// `hq-mt-routing.4`) and the actor lifecycle a hydrated handle owns
/// (`hq-mt-routing.2`) build on top without changing this surface.
#[derive(Default)]
pub struct RootRegistry {
    handles: DashMap<WorkspaceId, Arc<RootHandle>>,
}

impl RootRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        RootRegistry::default()
    }

    /// The handle for `workspace`, or `None` if it has not been hydrated.
    pub fn get(&self, workspace: &WorkspaceId) -> Option<Arc<RootHandle>> {
        self.handles.get(workspace).map(|h| Arc::clone(&h))
    }

    /// Whether `workspace` is currently hydrated.
    pub fn contains(&self, workspace: &WorkspaceId) -> bool {
        self.handles.contains_key(workspace)
    }

    /// Number of currently hydrated workspaces.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Whether no workspace is hydrated.
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// The ids of every currently hydrated workspace (for `/health`,
    /// `hq-mt-routing.8`). Order is unspecified.
    pub fn loaded(&self) -> Vec<WorkspaceId> {
        self.handles.iter().map(|e| e.key().clone()).collect()
    }

    /// Drop `workspace`'s handle (idle teardown), returning it if present. The
    /// returned `Arc` keeps the handle alive for any in-flight request that
    /// already resolved it; it drops when the last such request finishes.
    pub fn remove(&self, workspace: &WorkspaceId) -> Option<Arc<RootHandle>> {
        self.handles.remove(workspace).map(|(_, h)| h)
    }

    /// Invalidate `workspace`'s cached handle so the next access re-hydrates it
    /// from current configuration (`hq-mod-flags.6` hot-reload).
    ///
    /// The rebuild half of feature-flag hot-reload: when a workspace's flags
    /// flip, dropping its handle makes the next
    /// [`get_or_hydrate`](Self::get_or_hydrate) rebuild the `Root` with the new
    /// flag set (the hydrate closure calls `RootBuilder::build_with_flags`), so a
    /// dark-launched or rolled-back module appears or disappears without a
    /// process restart. An idle workspace rebuilds lazily on its next request; a
    /// hot one rebuilds on the next miss. Returns `true` if a handle was present.
    ///
    /// Same drop mechanics as [`remove`](Self::remove); the distinct name records
    /// intent — config change, not capacity teardown — and is the seam the
    /// flag-change trigger (PG `LISTEN/NOTIFY` or poll, Phase 4) calls. Draining
    /// in-flight requests before the swap arrives with the actor lifecycle
    /// (`hq-mt-routing.2`); until then an in-flight request keeps its
    /// already-resolved `Arc` and the next request gets the rebuilt one.
    pub fn invalidate(&self, workspace: &WorkspaceId) -> bool {
        self.handles.remove(workspace).is_some()
    }

    /// Return `workspace`'s handle, hydrating it with `hydrate` on first access.
    ///
    /// `hydrate` runs only when the workspace is absent. See
    /// [`get_or_try_hydrate`](Self::get_or_try_hydrate) for the fallible variant
    /// (hydration usually means building a [`Root`](gt_module::Root), which can
    /// fail).
    pub fn get_or_hydrate(
        &self,
        workspace: &WorkspaceId,
        hydrate: impl FnOnce() -> RootHandle,
    ) -> Arc<RootHandle> {
        // Infallible closures cannot fail, so the `Result` is always `Ok`.
        match self.get_or_try_hydrate(workspace, || Ok::<_, std::convert::Infallible>(hydrate())) {
            Ok(handle) => handle,
        }
    }

    /// Fallible [`get_or_hydrate`](Self::get_or_hydrate): return the cached
    /// handle, else build one with `hydrate`, propagating its error.
    ///
    /// Under a race two callers may both run `hydrate`, but exactly one handle
    /// is stored and both callers receive that same `Arc` — the loser's build is
    /// discarded. `hydrate` must therefore be free of observable side effects
    /// beyond producing the handle.
    pub fn get_or_try_hydrate<E>(
        &self,
        workspace: &WorkspaceId,
        hydrate: impl FnOnce() -> Result<RootHandle, E>,
    ) -> Result<Arc<RootHandle>, E> {
        // Fast path: already hydrated. The read guard is dropped before we take
        // the entry lock below, so the two never overlap on a shard.
        if let Some(existing) = self.handles.get(workspace) {
            return Ok(Arc::clone(&existing));
        }
        // Build outside the map lock (hydration can be expensive / fallible).
        let built = Arc::new(hydrate()?);
        // Re-check under the entry lock: a concurrent caller may have won.
        match self.handles.entry(workspace.clone()) {
            Entry::Occupied(occupied) => Ok(Arc::clone(occupied.get())),
            Entry::Vacant(vacant) => {
                vacant.insert(Arc::clone(&built));
                Ok(built)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use gt_module::{
        AllEnabled, DisabledModules, GtModule, ModuleId, ModuleMeta, RootBuilder,
    };
    use gt_workspace::WorkspaceId;
    use semver::Version;

    use super::RootRegistry;
    use crate::RootHandle;

    fn ws(slug: &str) -> WorkspaceId {
        WorkspaceId::new(slug).unwrap()
    }

    /// Smallest viable modules for the hot-reload test — identity only.
    struct ModA;
    struct ModB;
    impl GtModule for ModA {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(ModuleId::new("mod-a").unwrap(), "A", Version::new(1, 0, 0), "A")
        }
    }
    impl GtModule for ModB {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(ModuleId::new("mod-b").unwrap(), "B", Version::new(1, 0, 0), "B")
        }
    }

    /// An empty composed root bound to `slug` — enough to exercise the registry
    /// without any modules registered.
    fn handle(slug: &str) -> RootHandle {
        RootHandle::new(ws(slug), RootBuilder::new().build().unwrap())
    }

    #[test]
    fn get_absent_is_none() {
        let reg = RootRegistry::new();
        assert!(reg.get(&ws("acme")).is_none());
        assert!(!reg.contains(&ws("acme")));
        assert!(reg.is_empty());
    }

    #[test]
    fn hydrates_once_then_caches() {
        let reg = RootRegistry::new();
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            handle("acme")
        };

        let first = reg.get_or_hydrate(&ws("acme"), build);
        let second = reg.get_or_hydrate(&ws("acme"), build);

        assert_eq!(calls.load(Ordering::SeqCst), 1, "second access must not rebuild");
        assert!(Arc::ptr_eq(&first, &second), "same handle instance is returned");
        assert_eq!(reg.len(), 1);
        assert!(reg.contains(&ws("acme")));
    }

    #[test]
    fn distinct_workspaces_get_distinct_handles() {
        let reg = RootRegistry::new();
        let a = reg.get_or_hydrate(&ws("acme"), || handle("acme"));
        let b = reg.get_or_hydrate(&ws("globex"), || handle("globex"));
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(reg.len(), 2);
        let mut loaded: Vec<String> = reg.loaded().iter().map(|w| w.as_str().to_string()).collect();
        loaded.sort();
        assert_eq!(loaded, vec!["acme".to_string(), "globex".to_string()]);
    }

    #[test]
    fn try_hydrate_propagates_error_and_stores_nothing() {
        let reg = RootRegistry::new();
        let result = reg.get_or_try_hydrate(&ws("acme"), || Err::<RootHandle, &str>("boom"));
        assert_eq!(result.err(), Some("boom"));
        assert!(reg.is_empty(), "a failed hydration must not leave a partial entry");
    }

    #[test]
    fn remove_drops_the_handle() {
        let reg = RootRegistry::new();
        reg.get_or_hydrate(&ws("acme"), || handle("acme"));
        let removed = reg.remove(&ws("acme"));
        assert!(removed.is_some());
        assert!(reg.get(&ws("acme")).is_none());
        assert!(reg.is_empty());
    }

    #[test]
    fn concurrent_hydrate_yields_one_shared_instance() {
        let reg = Arc::new(RootRegistry::new());
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    reg.get_or_hydrate(&ws("acme"), || handle("acme"))
                })
            })
            .collect();
        let roots: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // Whoever raced, every caller observes the same stored instance.
        for r in &roots[1..] {
            assert!(Arc::ptr_eq(&roots[0], r), "all racers share one handle");
        }
        assert_eq!(reg.len(), 1, "exactly one entry stored");
    }

    #[test]
    fn invalidate_rebuilds_with_current_flags() {
        // hq-mod-flags.6: invalidate drops the cached handle so the next access
        // re-hydrates with the current flag set — a disabled module disappears
        // without a restart.
        let reg = RootRegistry::new();
        let mod_b = ModuleId::new("mod-b").unwrap();

        // Hydrate with both modules enabled.
        let build_all = || {
            let root = RootBuilder::new().module(ModA).module(ModB).build_with_flags(&AllEnabled).unwrap();
            RootHandle::new(ws("acme"), root)
        };
        let before = reg.get_or_hydrate(&ws("acme"), build_all);
        assert_eq!(before.root().len(), 2);
        assert!(before.root().module(&mod_b).is_some(), "mod-b live while enabled");

        // Flip: disable mod-b, then invalidate so the next access rebuilds.
        assert!(reg.invalidate(&ws("acme")), "a handle was present to invalidate");
        assert!(reg.is_empty(), "invalidate dropped the cached handle");

        let disabled = DisabledModules::new([mod_b.clone()]);
        let after = reg.get_or_hydrate(&ws("acme"), || {
            let root = RootBuilder::new().module(ModA).module(ModB).build_with_flags(&disabled).unwrap();
            RootHandle::new(ws("acme"), root)
        });
        assert_eq!(after.root().len(), 1, "rebuilt root dropped the disabled module");
        assert!(after.root().module(&mod_b).is_none(), "mod-b gone after flag flip + rebuild");
        assert!(!Arc::ptr_eq(&before, &after), "a new handle replaced the old one");
    }

    #[test]
    fn invalidate_absent_is_false() {
        let reg = RootRegistry::new();
        assert!(!reg.invalidate(&ws("ghost")));
    }
}
