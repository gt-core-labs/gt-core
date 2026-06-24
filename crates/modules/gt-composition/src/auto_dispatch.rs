//! Autonomous dispatch loop adapters (Fase 2 — A1).
//!
//! Wires the [`gt_runtime::Dispatcher`] to the production data sources:
//!
//! - [`FrontierSource`] implements [`ReadySource`] — polls `ready_for_auto()`
//!   against the Dolt issues store, returning the composite agent-dispatch
//!   frontier (readiness × auto × not locked × no surface overlap).
//! - [`SchedWorker`] implements [`Worker`] — seeds a bead as `Pending` on the
//!   scheduler and enqueues it, triggering the existing CAS-claim pump →
//!   `scheduling.dispatched.v1` → polecat sling. The dispatcher does NOT spawn
//!   agents directly — it feeds the scheduler, which owns capacity gating.
//!
//! The composition root (`gt-orch-server`) wires these into a
//! `Dispatcher<FrontierSource, SchedWorker>` and spawns the tick loop alongside
//! the patrol and quota timers, env-gated on `GT_AUTO_DISPATCH=1` +
//! `GT_DOLT_URL`.

use std::path::PathBuf;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use gt_beads::{Bead, BeadStatus};
use gt_eventlog::EventRecord;
use gt_events::AppError;
use gt_issues::{
    locked_roots, occupied_surfaces, ready_for_auto, session_like_actor, AllowAllTree, SurfaceTree,
};
use gt_plugin::Plugin;
use gt_rig::{DispatchMode, RigRepository};
use gt_runtime::{BeadId, DispatchError, Dispatcher, ReadySource, Worker};
use gt_scheduling::actor::SchedHandle;
use gt_store_dolt::DoltIssues;

/// Source of the rigs currently on dispatch hold (rig-hold H2, gtcore-1f5e67) — the rig-level
/// pause the frontier honours. `held()` is polled each dispatch tick; a rig it names has its
/// ready+auto beads excluded from the frontier. **Fail-open**: any read error returns an empty
/// set, so a rig-catalog blip never freezes dispatch (a missed hold is recoverable; a frozen
/// frontier stalls all work).
#[async_trait]
pub trait HeldRigs: Send + Sync {
    async fn held(&self) -> HashSet<String>;
}

/// A [`RigRepository`]-backed [`HeldRigs`]: the names of rigs whose catalog `dispatch_mode` is
/// [`DispatchMode::Hold`]. Reads the live catalog each poll so a `rig.hold`/`rig.resume` takes
/// effect on the next dispatch tick with no daemon restart.
pub struct CatalogHeldRigs<R: RigRepository> {
    rigs: R,
}

impl<R: RigRepository> CatalogHeldRigs<R> {
    pub fn new(rigs: R) -> Self {
        Self { rigs }
    }
}

#[async_trait]
impl<R: RigRepository + Send + Sync> HeldRigs for CatalogHeldRigs<R> {
    async fn held(&self) -> HashSet<String> {
        match self.rigs.list().await {
            Ok(entries) => entries
                .into_iter()
                .filter(|e| e.dispatch_mode == DispatchMode::Hold)
                .map(|e| e.name)
                .collect(),
            Err(e) => {
                eprintln!("[auto-dispatch] held-rigs read failed (fail-open, no holds applied): {e}");
                HashSet::new()
            }
        }
    }
}

/// [`ReadySource`] adapter: polls the `ready_for_auto` frontier from Dolt.
pub struct FrontierSource {
    store: Arc<DoltIssues>,
    repo_dir: Option<PathBuf>,
    rig: Option<String>,
    held_rigs: Option<Arc<dyn HeldRigs>>,
}

impl FrontierSource {
    pub fn new(store: Arc<DoltIssues>, repo_dir: Option<PathBuf>) -> Self {
        Self {
            store,
            repo_dir,
            rig: None,
            held_rigs: None,
        }
    }

    /// Scope the frontier to a specific rig (optional).
    pub fn with_rig(mut self, rig: impl Into<String>) -> Self {
        self.rig = Some(rig.into());
        self
    }

    /// Wire the rig-hold source (rig-hold H2). Absent ⇒ no rig is ever treated as held (the
    /// pre-feature behaviour), so an orchd without a rig catalog dispatches exactly as before.
    pub fn with_held_rigs(mut self, source: Arc<dyn HeldRigs>) -> Self {
        self.held_rigs = Some(source);
        self
    }

    fn surface_tree(&self) -> Box<dyn SurfaceTree + Send + Sync> {
        match &self.repo_dir {
            Some(dir) => gt_mcp_server::git_tree::surface_tree(Some(dir)),
            None => Box::new(AllowAllTree),
        }
    }
}

impl ReadySource for FrontierSource {
    async fn ready(&self) -> Vec<BeadId> {
        let filter = gt_store_dolt::IssueFilter {
            rig: self.rig.clone(),
            ..Default::default()
        };

        let rows = match self.store.list(&filter).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[auto-dispatch] frontier poll failed (list): {e}");
                return Vec::new();
            }
        };

        // Gather the data sets the frontier needs.
        let (deps_map, dep_facts, open_phase, parents, dispatch_raw, working_epics, working_surfs) =
            match tokio::try_join!(
                self.store.depends_on_edges(&filter),
                self.store.dep_index(),
                self.store.open_phase(),
                self.store.parent_map("", ""),
                self.store.dispatch_index(),
                self.store.working_epics(),
                self.store.working_surfaces(),
            ) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[auto-dispatch] frontier poll failed (maps): {e}");
                    return Vec::new();
                }
            };

        let tree = self.surface_tree();
        let locked = locked_roots(
            working_epics.iter().map(|(id, o)| (id.as_str(), o.as_str())),
            &|a| session_like_actor(a),
        );
        let occupied = occupied_surfaces(&working_surfs);

        // Rigs on dispatch hold (rig-hold H2): their ready+auto beads are excluded from the
        // frontier. Fail-open via the source itself; absent source ⇒ no holds.
        let held = match &self.held_rigs {
            Some(source) => source.held().await,
            None => HashSet::new(),
        };

        let dep_fact_fn = |id: &str| dep_facts.get(id).cloned();
        let frontier = ready_for_auto(
            rows,
            &deps_map,
            &dep_fact_fn,
            open_phase,
            tree.as_ref(),
            &parents,
            &dispatch_raw,
            &locked,
            &occupied,
            &held,
        );

        frontier
            .into_iter()
            .map(|r| BeadId::new(r.id))
            .collect()
    }
}

/// [`Worker`] adapter: seeds + enqueues a bead on the scheduler, triggering
/// the existing CAS-claim pump → polecat sling.
pub struct SchedWorker {
    sched: SchedHandle,
}

impl SchedWorker {
    pub fn new(sched: SchedHandle) -> Self {
        Self { sched }
    }
}

impl Worker for SchedWorker {
    async fn dispatch(&self, bead: &BeadId) -> Result<(), DispatchError> {
        let id = bead.as_str().to_string();
        let _ = self
            .sched
            .create_bead(Bead::new(id.clone(), id.clone(), BeadStatus::Pending, 1))
            .await;
        self.sched.enqueue(id, 1).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Completion observer: frees dispatcher slots on session end / bead close
// ---------------------------------------------------------------------------

/// Hub plugin that calls [`Dispatcher::complete`] when a bead finishes,
/// freeing its in-flight slot so the next tick can fill it.
///
/// Observes:
/// - `issues.closed.v1` — bead id directly from payload; the definitive
///   completion signal (working → closed).
/// - `patrol.lease-expired.v1` — the bead's lease expired (crashed agent);
///   the patrol bridge released the claim, so the bead is back to `open` and
///   no longer in-flight.
pub struct AutoDispatchCompletionPlugin<S: ReadySource, W: Worker> {
    dispatcher: Arc<Dispatcher<S, W>>,
}

impl<S: ReadySource, W: Worker> AutoDispatchCompletionPlugin<S, W> {
    pub fn new(dispatcher: Arc<Dispatcher<S, W>>) -> Self {
        Self { dispatcher }
    }
}

#[async_trait]
impl<S, W> Plugin for AutoDispatchCompletionPlugin<S, W>
where
    S: ReadySource + Send + Sync + 'static,
    W: Worker + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "auto-dispatch-completion"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "issues.closed.v1" => {
                if let Some(id) = record.payload.get("id").and_then(|v| v.as_str()) {
                    if self.dispatcher.complete(&BeadId::new(id)).await {
                        eprintln!("[auto-dispatch] slot freed — {id} closed");
                    }
                }
                Ok(())
            }
            "patrol.lease-expired.v1" => {
                // Externally-tagged: {"LeaseExpired": {"bead": "...", ...}}
                if let Some(bead) = record
                    .payload
                    .get("LeaseExpired")
                    .and_then(|v| v.get("bead"))
                    .and_then(|v| v.as_str())
                {
                    if self.dispatcher.complete(&BeadId::new(bead)).await {
                        eprintln!("[auto-dispatch] slot freed — {bead} lease expired");
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
