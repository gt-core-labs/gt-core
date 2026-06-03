//! Composition root: per-workspace assembly of the module `Root` + the cross-domain reactor as
//! **observer plugins** (approach A) + the role observers, exposed as [`compose_workspace`] (a
//! direct assembler returning the handles) and [`live_root`] (registry-hosted, owned + drained).
//!
//! This crate is the **app composition entry, not a `GtModule`**. gt-core has no single
//! `GtEvent` enum to match on (one event-kind namespace per module), so a cross-domain reaction
//! is just another `gt_plugin::Plugin` on the per-workspace event hub, like the role observers
//! (`gt_roles::DeaconPlugin` / `WitnessPlugin`). The two orchestration arms the gastown
//! `Reactor::react` expressed and that need no edge effects:
//!
//! - [`SchedulerPlugin`] — `patrol.lease-expired.v1` → re-enqueue; `merge.merged.v1` → free a slot.
//! - [`MergePlugin`] — `merge.ready.v1` → advance the slot to `Merging`.
//!
//! ## Tier placement
//!
//! The root must name `gt-scheduling` + `gt-patrol` + `gt-merge` + `gt-roles` together; the
//! docs/03 Rule 4 table permits only `roles`, `modules`, and `examples` to depend on all
//! `domain/*`. It lives in the `modules` tier as the dependency-legal production home
//! (`hq-mod-refactor.26`, owner call) even though it is not a `GtModule`; a dedicated
//! `crates/app/` tier would be the semantically-pure home but needs a Rule-2 tier approval
//! (open under `hq-mod-refactor.13`).
//!
//! ## What is still deferred
//!
//! The edge-effect arms (sling/rotate/`git merge`/lease release, function #6), hydration (#7),
//! and real PG/Dolt repos (#8) are not wired here — they are the remaining production I/O. The
//! pattern itself (functions #1–#5, #9) is complete: [`live_root`] resolves a workspace through
//! `RootRegistry::get_or_hydrate_async`, owns its actors via `Supervisor::anchor`, and drains
//! their events onto the hub via `RootHandle::drain_events_from`.

pub mod mcp;
pub mod stream;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use gt_beads::InMemoryBeads;
use gt_events::{AppError, Envelope};
use gt_eventlog::{EventRecord, EventStore, JsonlWriter};
use gt_merge::actor::MergeHandle;
use gt_merge::{InMemoryMergeRepo, MergeEvent};
use gt_module::RootBuilder;
use gt_patrol::PatrolEvent;
use gt_plugin::Plugin;
use gt_roles::{RoleSinks, RoleStack};
use gt_runtime::{RootHandle, RootRegistry};
use gt_scheduling::actor::SchedHandle;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;

/// Observer that drives the scheduler from other domains' events — the scheduler reactor arm:
/// `patrol.lease-expired.v1` → re-enqueue the freed bead (`SchedHandle::enqueue`);
/// `merge.merged.v1` → release the dispatch slot (`SchedHandle::capacity_freed`). The scheduler
/// actor owns its queue/governor and emits its own `SchedEvent`s, so this is a read-only
/// observer of OTHER domains.
pub struct SchedulerPlugin {
    sched: SchedHandle,
}

impl SchedulerPlugin {
    /// Wrap a scheduler command handle as a cross-domain observer.
    pub fn new(sched: SchedHandle) -> Self {
        Self { sched }
    }
}

#[async_trait]
impl Plugin for SchedulerPlugin {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "patrol.lease-expired.v1" => {
                if let PatrolEvent::LeaseExpired { bead, priority, .. } =
                    record.decode::<PatrolEvent>()?
                {
                    self.sched.enqueue(bead, priority).await;
                }
                Ok(())
            }
            "merge.merged.v1" => {
                self.sched.capacity_freed().await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Observer that advances a merge slot when it is observed ready — the merge reactor arm:
/// `merge.ready.v1` → `MergeHandle::start` (`Ready → Merging`). The slot must already exist
/// (submitted) for the transition to take; the merge actor validates that and stays silent
/// otherwise (the error lands in the relay dead-letter, never aborting the chain).
pub struct MergePlugin {
    merge: MergeHandle,
}

impl MergePlugin {
    /// Wrap a merge command handle as a cross-domain observer.
    pub fn new(merge: MergeHandle) -> Self {
        Self { merge }
    }
}

#[async_trait]
impl Plugin for MergePlugin {
    fn name(&self) -> &'static str {
        "merge"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind == "merge.ready.v1" {
            if let MergeEvent::Ready { bead, .. } = record.decode::<MergeEvent>()? {
                self.merge.start(bead).await;
            }
        }
        Ok(())
    }
}

/// Observer that **persists** every record on the workspace hub to the durable per-workspace
/// event log (`gt_eventlog`). This is what makes the autonomous daemon's [`live_root`] survive a
/// restart (`hq-orchd.2`): gt-core persists domain state by **event-sourcing this log** — the same
/// substrate the request-driven MCP handlers use (`mcp::eventlog`) — not via Dolt/PG domain tables.
/// The in-memory domain actors (the scheduler queue, the merge board) are projections; the log is
/// their source of truth, so boot hydration (`hq-orchd.5`) can rebuild them by replaying it.
///
/// The writer is a single rotated [`JsonlWriter`] bound to the workspace's log partition at
/// hydration time; each [`on_event`](Plugin::on_event) is one file-locked append. Registered ahead
/// of the reactor arms so the triggering event is durable before any observer reacts — a failed
/// append surfaces as the plugin's error (recorded to the relay dead-letter) rather than aborting
/// the reaction chain.
pub struct EventLogPlugin {
    writer: JsonlWriter,
}

impl EventLogPlugin {
    /// Bind a persistence sink to workspace `ws`'s log partition under `root` (creating
    /// `<root>/<ws>/` lazily). `root` is the production event-log volume (`GT_EVENTLOG_ROOT`,
    /// defaulting to `/var/lib/gt-core`); tests pass a tempdir.
    pub fn for_workspace_in(root: impl AsRef<std::path::Path>, ws: &str) -> Result<Self, AppError> {
        Ok(Self { writer: JsonlWriter::for_workspace_in(root, ws)? })
    }
}

#[async_trait]
impl Plugin for EventLogPlugin {
    fn name(&self) -> &'static str {
        "eventlog"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        self.writer.append(record)
    }
}

/// A fully assembled, started per-workspace root — the output of [`compose_workspace`]. Holds
/// the live [`RootHandle`] plus the domain handles + event sinks a caller (or a test) inspects.
/// Dropping it drops the role stack (the supervisor cancels the actors) and the hub sender (the
/// plugin relay then exits).
pub struct Composed {
    /// The composed application handle (event hub + supervisor) for the workspace.
    pub handle: RootHandle,
    /// Scheduler command handle.
    pub sched: SchedHandle,
    /// Merge command handle.
    pub merge: MergeHandle,
    /// Scheduler event stream (for inspection).
    pub sched_events: mpsc::Receiver<Envelope<SchedEvent>>,
    /// Merge event stream (for inspection).
    pub merge_events: mpsc::Receiver<Envelope<MergeEvent>>,
    /// The role-actor stack, kept alive for the root's lifetime.
    pub roles: RoleStack,
}

/// Assemble and start the per-workspace composition root: build the [`RootHandle`] (event hub),
/// spawn the scheduler + merge actors, register the role observers + the scheduler + merge
/// reactor arms onto the hub, and start the actor stack. One function = the whole
/// composition-root assembly (function #9), minus the edge effects + hydration the production
/// root adds.
///
/// Must run inside a Tokio runtime. Role event sinks are parked (the demo asserts through the
/// scheduler/merge sinks); the production root would drain every actor's events into the hub +
/// the event log.
pub async fn compose_workspace(ws: WorkspaceId) -> Composed {
    let handle = RootHandle::new(ws, RootBuilder::new().build().expect("empty root builds"));

    let (sched_tx, sched_events) = mpsc::channel(64);
    let sched = gt_scheduling::actor::spawn(InMemoryBeads::default(), sched_tx, 4);

    let (merge_tx, merge_events) = mpsc::channel(64);
    let merge = gt_merge::actor::spawn(InMemoryMergeRepo::default(), merge_tx);

    let (st, _sr) = mpsc::channel(16);
    let (wt, _wr) = mpsc::channel(16);
    let (dt, _dr) = mpsc::channel(16);
    let (mt, _mr) = mpsc::channel(16);
    let (rt, _rr) = mpsc::channel(16);
    let roles = RoleStack::register(
        handle.supervisor(),
        RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt },
    )
    .await
    .expect("role stack registers while the supervisor is Built");

    // The full observer registry: role observers (sheriff/deacon/witness) + the two
    // orchestration reactor arms, fanned out in registration order by the relay.
    let registry = roles
        .plugin_registry()
        .register(SchedulerPlugin::new(sched.clone()))
        .register(MergePlugin::new(merge.clone()));
    handle.spawn_plugins(Arc::new(registry));

    handle.start().await;

    Composed { handle, sched, merge, sched_events, merge_events, roles }
}

/// A read-only observer that records every kind it sees on the hub, in order — a test probe for
/// watching a reaction cascade flow through the event hub.
pub struct ProbePlugin {
    seen: Arc<Mutex<Vec<String>>>,
}

impl ProbePlugin {
    /// Record observed kinds into the shared `seen` log.
    pub fn new(seen: Arc<Mutex<Vec<String>>>) -> Self {
        Self { seen }
    }
}

#[async_trait]
impl Plugin for ProbePlugin {
    fn name(&self) -> &'static str {
        "probe"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        self.seen.lock().unwrap().push(record.kind.clone());
        Ok(())
    }
}

/// The production-shaped per-workspace composition root: resolve `ws` through the
/// [`RootRegistry`], hydrating it asynchronously on first access into a fully **owned** and
/// **drained** root. The hydrate closure assembles everything from the gt-core primitives:
///
/// - builds the [`RootHandle`] (event hub),
/// - spawns the scheduler + merge actors and **anchors** them to the handle's supervisor
///   (`Supervisor::anchor`) so they live for the workspace lifetime — the registry returns only
///   an `Arc<RootHandle>`, so nothing else holds them,
/// - **drains** each actor's event sink onto the hub (`RootHandle::drain_events_from`), so a
///   domain's output flows back through the hub to the observers (function #3),
/// - registers the role observers + the scheduler/merge reactor arms + the `ProbePlugin`, and
///   starts the actor stack.
///
/// The result is cached by the registry: a second call returns the same `Arc` without
/// re-hydrating. `probe` is wired into the hydrated root so a caller can watch the cascade; it
/// is ignored on a cache hit (the closure does not run).
///
/// ## Durability (`hq-orchd.2`)
///
/// When `log_root` is `Some`, an [`EventLogPlugin`] is registered ahead of the reactor arms so
/// every record on the hub is appended to the workspace's durable log under that root — the
/// autonomous daemon's state then survives a restart (boot hydration `hq-orchd.5` replays it).
/// `None` keeps the root purely in-memory (the capstone reaction test); production passes the
/// `GT_EVENTLOG_ROOT` volume. A non-writable root is a boot misconfiguration and panics here,
/// like the other infallible assembly steps in this closure.
pub async fn live_root(
    reg: &RootRegistry,
    ws: WorkspaceId,
    log_root: Option<PathBuf>,
    probe: Arc<Mutex<Vec<String>>>,
) -> Arc<RootHandle> {
    let key = ws.clone();
    reg.get_or_hydrate_async(&key, move || async move {
        let ws_slug = ws.as_str().to_string();
        let handle = RootHandle::new(ws, RootBuilder::new().build().expect("empty root builds"));

        let (sched_tx, sched_rx) = mpsc::channel(64);
        let sched = gt_scheduling::actor::spawn(InMemoryBeads::default(), sched_tx, 4);
        let (merge_tx, merge_rx) = mpsc::channel(64);
        let merge = gt_merge::actor::spawn(InMemoryMergeRepo::default(), merge_tx);

        // Own the domain actors for the workspace lifetime, and flow their output to the hub.
        handle
            .supervisor()
            .anchor(sched.clone())
            .await
            .expect("anchors while Built");
        handle
            .supervisor()
            .anchor(merge.clone())
            .await
            .expect("anchors while Built");
        handle.drain_events_from(sched_rx);
        handle.drain_events_from(merge_rx);

        let (st, _sr) = mpsc::channel(16);
        let (wt, _wr) = mpsc::channel(16);
        let (dt, _dr) = mpsc::channel(16);
        let (mt, _mr) = mpsc::channel(16);
        let (rt, _rr) = mpsc::channel(16);
        let roles = RoleStack::register(
            handle.supervisor(),
            RoleSinks { sheriff: st, witness: wt, deacon: dt, mayor: mt, refinery: rt },
        )
        .await
        .expect("role stack registers while the supervisor is Built");

        let mut registry = roles.plugin_registry();
        if let Some(root) = log_root {
            // Persist before the reactor arms observe, so the triggering event is durable even if
            // a downstream reaction crashes mid-chain.
            registry = registry.register(
                EventLogPlugin::for_workspace_in(&root, &ws_slug)
                    .expect("event log root is writable"),
            );
        }
        let registry = registry
            .register(SchedulerPlugin::new(sched))
            .register(MergePlugin::new(merge))
            .register(ProbePlugin::new(probe));
        handle.spawn_plugins(Arc::new(registry));
        handle.start().await;
        handle
    })
    .await
}
