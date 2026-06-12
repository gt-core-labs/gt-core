//! Composition root: per-workspace assembly of the module `Root` + the cross-domain reactor as
//! **observer plugins** (approach A) + the role observers, exposed as [`compose_workspace`] (a
//! direct assembler returning the handles) and [`live_root`] (registry-hosted, owned + drained).
//!
//! This crate is the **app composition entry, not a `GtModule`**. gt-core has no single
//! `GtEvent` enum to match on (one event-kind namespace per module), so a cross-domain reaction
//! is just another `gt_plugin::Plugin` on the per-workspace event hub, like the role observers
//! (`gt_roles::DeaconPlugin` / `WitnessPlugin`). The two orchestration arms the upstream
//! `Reactor::react` expressed and that need no edge effects:
//!
//! - [`SchedulerPlugin`] — `patrol.lease-expired.v1` → re-enqueue; `merge.merged.v1` → free a slot.
//! - [`MergePlugin`] — `merge.ready.v1` → advance the slot to `Merging`.
//!
//! The one arm that DOES need an edge effect — landing the branch on `main` — is
//! [`git_merge::GitMergePlugin`]: it observes `merge.started.v1` and runs the real `git push`
//! (rebasing on divergence), then drives the slot to `Merged`/`Failed`. Like the polecat sling it
//! is real I/O, so the **bin** registers it on the hub relay rather than `daemon_root`.
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
//! The remaining edge-effect arms (sling/rotate/lease release, function #6), hydration (#7),
//! and real PG/Dolt repos (#8) are not wired here — they are the remaining production I/O. The
//! `git merge` edge IS now wired ([`git_merge::GitMergePlugin`], `hq-orchd-deploy.12`). The
//! pattern itself (functions #1–#5, #9) is complete: [`live_root`] resolves a workspace through
//! `RootRegistry::get_or_hydrate_async`, owns its actors via `Supervisor::anchor`, and drains
//! their events onto the hub via `RootHandle::drain_events_from`.
//! [`git_merge::GitMergePlugin`] (edge git-merge) and [`witness_sweep::WitnessSweep`] (witness safety-net) are the wired edge effects that close the autonomous loop.

pub mod account_dirs;
pub mod anthropic_proxy;
pub mod auth;
pub mod denial_audit;
pub mod drift_reconcile;
pub mod email_outbox_drain;
pub mod git_merge;
pub mod hooks;
pub mod inbound_email;
pub mod kanban_rest;
pub mod mailbox;
pub mod mcp;
pub mod notifications;
pub mod onboard;
pub mod operator_event;
pub mod operator_resource;
pub mod polecat;
pub mod quota_rotation;
pub mod report_scheduler;
pub mod rest_modules;
pub mod scope_bridge;
pub mod session_reconcile;
pub mod stream;
pub mod system;
pub mod terminal;
pub mod usage_probe;
pub mod webhook;
pub mod witness_sweep;
pub mod worktree;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use gt_agent::AgentEvent;
use gt_beads::InMemoryBeads;
use gt_eventlog::{EventRecord, EventStore, JsonlWriter};
use gt_events::{AppError, Envelope};
use gt_merge::actor::MergeHandle;
use gt_merge::{
    BranchReaper, InMemoryBranchReaper, InMemoryMergeRepo, MergeBoard, MergeEvent, MergeState,
};
use gt_module::RootBuilder;
use gt_patrol::actor::PatrolHandle;
use gt_patrol::{InMemoryPatrolRepo, PatrolEvent};
use gt_plugin::{Plugin, SheriffPlugin};
use gt_quota::actor::QuotaHandle;
use gt_quota::{AccountRegistry, QuotaEvent, QuotaState};
use gt_roles::{RoleSinks, RoleStack};
use gt_runtime::{RootHandle, RootRegistry};
use gt_scheduling::actor::SchedHandle;
use gt_scheduling::SchedEvent;
use gt_workspace::WorkspaceId;

use crate::mcp::eventlog::EventLog;

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

/// Observer that reaps a delivered branch when its bead merges — the branch-GC reactor arm
/// (epic hq-branch-gc): `merge.merged.v1` → resolve the slot's branch from the merge board and
/// [`BranchReaper::reap`] it. This is why delivered branches stop piling up in Dolt, and it
/// keeps branch deletion **out** of the merge actor (which stays pure state) and **off** any
/// cron — the delivery event is the trigger. Reaping is idempotent, so a replayed `Merged`
/// (boot hydration re-emitting) drops nothing the second time.
///
/// The `Merged` event carries only `{ bead, sha }`, not the branch. Rather than widen the event
/// schema (a replay-determinism change), the reactor reads the branch from the live board: the
/// slot is still present in `Merged` state and still carries its `branch`.
pub struct BranchGcPlugin<R: BranchReaper> {
    merge: MergeHandle,
    reaper: R,
}

impl<R: BranchReaper> BranchGcPlugin<R> {
    /// Wire a merge handle (for bead → branch resolution) and a branch reaper as a cross-domain
    /// observer.
    pub fn new(merge: MergeHandle, reaper: R) -> Self {
        Self { merge, reaper }
    }
}

#[async_trait]
impl<R: BranchReaper + 'static> Plugin for BranchGcPlugin<R> {
    fn name(&self) -> &'static str {
        "branch-gc"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        if record.kind != "merge.merged.v1" {
            return Ok(());
        }
        let MergeEvent::Merged { bead, .. } = record.decode::<MergeEvent>()? else {
            return Ok(());
        };
        // Resolve the branch from the live board (the `Merged` event omits it). A slot may be
        // absent if the board was never hydrated for this bead — nothing to reap, stay silent.
        let branch = self
            .merge
            .snapshot()
            .await
            .into_iter()
            .find(|s| s.bead == bead)
            .map(|s| s.branch);
        if let Some(branch) = branch {
            self.reaper.reap(&branch).await?;
        }
        Ok(())
    }
}

/// Production [`BranchReaper`]: drops the branch from the workspace's Dolt database via
/// [`gt_store_dolt::delete_branch`]. Holds the per-workspace pool; constructed only when a Dolt
/// server is wired (else [`InMemoryBranchReaper`] stands in, same as the merge repo). Maps the
/// kernel store's `AppError` onto the domain `gt_events::AppError` at the boundary.
pub struct DoltBranchReaper {
    pool: mysql_async::Pool,
}

impl DoltBranchReaper {
    /// Wrap a per-workspace Dolt pool (e.g. from `WorkspacePools::pool_for`).
    pub fn new(pool: mysql_async::Pool) -> Self {
        Self { pool }
    }
}

impl BranchReaper for DoltBranchReaper {
    async fn reap(&self, branch: &str) -> Result<bool, AppError> {
        gt_store_dolt::delete_branch(&self.pool, branch)
            .await
            .map_err(|e| AppError::Other(e.to_string()))
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
        Ok(Self {
            writer: JsonlWriter::for_workspace_in(root, ws)?,
        })
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

/// Whole minutes between two RFC3339 record timestamps (`hq-orchd.6`). `None` if either fails to
/// parse or `end` precedes `start` — a malformed or out-of-order pair is skipped, never negative.
fn minutes_between(start_ts: &str, end_ts: &str) -> Option<u64> {
    let start = OffsetDateTime::parse(start_ts, &Rfc3339).ok()?;
    let end = OffsetDateTime::parse(end_ts, &Rfc3339).ok()?;
    let secs = (end - start).whole_seconds();
    (secs >= 0).then_some(secs as u64 / 60)
}

/// Observer that projects agent-session **durations** into the per-workspace
/// `gt_workspace_session_minutes` metric (`hq-orchd.6`, the third `hq-mt-deploy.8` cost counter).
///
/// On `agent.spawned.v1` it stashes the session's start timestamp (the record `ts`); on
/// `agent.session-end.v1` / `agent.killed.v1` it computes the wall-clock duration to the close
/// record's `ts`, rounds down to whole minutes, and bumps the counter for `workspace`. The
/// per-record `ts` is the source of truth, so **no `AgentEvent` change** is needed (and no
/// replay-gate impact) — the absence of a session-close-with-duration *event* is exactly what
/// blocked this counter; deriving it from the two record timestamps unblocks it without touching
/// the event vocabulary. Sessions still open at restart lose their start (in-memory map), so only
/// sessions that both open and close within one daemon run are counted — acceptable for a live
/// operational metric (matches the upstream in-memory session tracking).
pub struct SessionMinutesPlugin {
    workspace: String,
    /// session id → its `agent.spawned.v1` record `ts`, pending a close.
    starts: Mutex<HashMap<String, String>>,
}

impl SessionMinutesPlugin {
    /// Project session minutes for `workspace` (the daemon's single tenant).
    pub fn new(workspace: impl Into<String>) -> Self {
        Self {
            workspace: workspace.into(),
            starts: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Plugin for SessionMinutesPlugin {
    fn name(&self) -> &'static str {
        "session-minutes"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            "agent.spawned.v1" => {
                if let AgentEvent::Spawned { session, .. } = record.decode::<AgentEvent>()? {
                    self.starts
                        .lock()
                        .expect("starts mutex")
                        .insert(session, record.ts.clone());
                }
            }
            "agent.session-end.v1" | "agent.killed.v1" => {
                let session = match record.decode::<AgentEvent>()? {
                    AgentEvent::SessionEnd { session } => session,
                    AgentEvent::Killed { session, .. } => session,
                    _ => return Ok(()),
                };
                let start = self.starts.lock().expect("starts mutex").remove(&session);
                if let Some(start_ts) = start {
                    if let Some(minutes) = minutes_between(&start_ts, &record.ts) {
                        gt_telemetry::metrics::observe_workspace_session_minutes(
                            &self.workspace,
                            minutes,
                        );
                    }
                }
            }
            _ => {}
        }
        Ok(())
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
        RoleSinks {
            sheriff: st,
            witness: wt,
            deacon: dt,
            mayor: mt,
            refinery: rt,
        },
    )
    .await
    .expect("role stack registers while the supervisor is Built");

    // The full observer registry: role observers (sheriff/deacon/witness) + the two
    // orchestration reactor arms, fanned out in registration order by the relay.
    let registry = roles
        .plugin_registry()
        .register(SchedulerPlugin::new(sched.clone()))
        .register(MergePlugin::new(merge.clone()))
        .register(BranchGcPlugin::new(
            merge.clone(),
            InMemoryBranchReaper::new(),
        ));
    handle.spawn_plugins(Arc::new(registry));

    handle.start().await;

    Composed {
        handle,
        sched,
        merge,
        sched_events,
        merge_events,
        roles,
    }
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
/// Replay the workspace's scheduling log into the still-pending dispatch queue (`hq-orchd.5`).
///
/// Folds `scheduling.*` records keeping each bead's priority (the shared [`gt_scheduling`] replay
/// reducer drops it, so this keeps a local `(bead, priority)` accumulator) and drops any bead that
/// later dispatched / failed / timed out — what remains is the queue that was still waiting when
/// the daemon stopped. The result seeds [`gt_scheduling::actor::spawn_hydrated`] so a restart
/// restores the queue without re-emitting `Enqueue` events.
pub fn replay_scheduling_pending(
    log: &EventLog,
    ws: &str,
) -> Result<Vec<(String, u8)>, gt_store_dolt::AppError> {
    log.replay_domain::<Vec<(String, u8)>, SchedEvent, _>(
        Some(ws),
        "scheduling.",
        Vec::new(),
        |acc, e| match e {
            SchedEvent::Enqueue { bead, priority } => acc.push((bead.clone(), *priority)),
            SchedEvent::Dispatched { bead, .. }
            | SchedEvent::DispatchFailed { bead, .. }
            | SchedEvent::DispatchTimeout { bead } => acc.retain(|(b, _)| b != bead),
        },
    )
}

/// Replay the workspace's merge log into the in-flight [`MergeBoard`] (`hq-orchd.5`) through the
/// domain reducer, so a restart restores open merge slots before the actor starts processing
/// edge messages. Seeds [`gt_merge::actor::spawn_hydrated`].
pub fn replay_merge_board(log: &EventLog, ws: &str) -> Result<MergeBoard, gt_store_dolt::AppError> {
    let state = log.replay_domain::<MergeState, MergeEvent, _>(
        Some(ws),
        "merge.",
        MergeState::default(),
        MergeState::apply,
    )?;
    Ok(MergeBoard::from_state(&state))
}

/// Replay the workspace's quota log into [`QuotaState`] (`hq-quota-accounts.2`) through the domain
/// reducer. Carries the onboarded-account registry (`registered`: account → `CLAUDE_CONFIG_DIR`)
/// the daemon seeds its credential keychain from, plus the account windows/statuses that hydrate
/// the quota actor — so a live-registered account survives restart without the `GT_CLAUDE_ACCOUNTS`
/// env. Seeds [`gt_quota::actor::spawn_hydrated`].
pub fn replay_quota_state(log: &EventLog, ws: &str) -> Result<QuotaState, gt_store_dolt::AppError> {
    log.replay_domain::<QuotaState, QuotaEvent, _>(
        Some(ws),
        "quota.",
        QuotaState::default(),
        QuotaState::apply,
    )
}

/// The result is cached by the registry: a second call returns the same `Arc` without
/// re-hydrating. `probe` is wired into the hydrated root so a caller can watch the cascade; it
/// is ignored on a cache hit (the closure does not run).
///
/// ## Durability (`hq-orchd.2`) + boot hydration (`hq-orchd.5`)
///
/// On hydration the closure first replays the durable log (when `log_root` is `Some`) into the
/// pending scheduler queue + the merge board via [`replay_scheduling_pending`] /
/// [`replay_merge_board`], seeding the actors so in-flight work survives a restart.
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

        // Boot hydration (hq-orchd.5): when persisting, replay the durable log into the pending
        // scheduler queue + the in-flight merge board BEFORE spawning the actors, so a restart
        // resumes still-open work. A fresh/empty log yields empty state. `None` (in-memory test)
        // skips replay. A corrupt log at boot is fatal — fail loudly, like the other steps here.
        let (sched_pending, merge_board) = match &log_root {
            Some(root) => {
                let log = EventLog::new(Some(root.clone()));
                (
                    replay_scheduling_pending(&log, &ws_slug).expect("replay scheduling log"),
                    replay_merge_board(&log, &ws_slug).expect("replay merge log"),
                )
            }
            None => (Vec::new(), MergeBoard::default()),
        };

        let (sched_tx, sched_rx) = mpsc::channel(64);
        let sched = gt_scheduling::actor::spawn_hydrated(
            InMemoryBeads::default(),
            sched_tx,
            4,
            sched_pending,
        );
        let (merge_tx, merge_rx) = mpsc::channel(64);
        let merge =
            gt_merge::actor::spawn_hydrated(InMemoryMergeRepo::default(), merge_tx, merge_board);

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
            RoleSinks {
                sheriff: st,
                witness: wt,
                deacon: dt,
                mayor: mt,
                refinery: rt,
            },
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
            .register(MergePlugin::new(merge.clone()))
            .register(BranchGcPlugin::new(merge, InMemoryBranchReaper::new()))
            .register(ProbePlugin::new(probe));
        handle.spawn_plugins(Arc::new(registry));
        handle.start().await;
        handle
    })
    .await
}

/// The durable, hydrated production root for the autonomous daemon, plus the domain handles its
/// edge loops drive (`hq-orchd.4`). Unlike [`live_root`] (registry-cached, used by the capstone
/// test and shaped to return only the `Arc<RootHandle>`), [`daemon_root`] is built once for the
/// daemon's single workspace and **returns the actor handles** so the bin can drive the patrol
/// lease-expiry tick, the quota predictive auto-rotation tick, and submit MERGE_READY to the merge
/// actor from the Refinery channel loop — none of which can reach an actor through the hub alone.
pub struct DaemonRoot {
    /// The composed root (event hub + supervisor), owned for the process lifetime.
    pub handle: Arc<RootHandle>,
    /// Scheduler command handle — the dispatch-channel loop seeds + enqueues a requested bead here,
    /// which the dispatcher auto-dispatches (emitting `scheduling.dispatched.v1`, `hq-orchd-deploy.4`).
    pub sched: SchedHandle,
    /// Merge command handle — the Refinery loop submits MERGE_READY beads here.
    pub merge: MergeHandle,
    /// Patrol command handle — the lease-expiry ticker drives [`PatrolHandle::tick`].
    pub patrol: PatrolHandle,
    /// Quota command handle — the auto-rotation ticker drives [`QuotaHandle::tick`].
    pub quota: QuotaHandle,
}

/// Assemble the autonomous daemon's per-workspace root over the durable log (`hq-orchd.4`,
/// building on `.2`/`.5`). Same event-sourced substrate as [`live_root`] — hydrate scheduler +
/// merge from the log, anchor the actors to the supervisor, drain their output onto the hub,
/// register the persistence sink + role observers + the scheduler/merge reactor arms — and
/// additionally:
///
/// - spawns the **patrol** + **quota** actors (anchored + drained), so lease-expiry and quota
///   events flow through the hub (the [`SchedulerPlugin`] re-enqueues on `patrol.lease-expired.v1`);
/// - registers the **sheriff** observer ([`SheriffPlugin`]) on the hub;
/// - returns the merge/patrol/quota handles for the bin's edge timers + Refinery loop.
///
/// Patrol leases + quota accounts are spawned **fresh** (not hydrated): leases are short-lived and
/// re-registered as work dispatches; quota hydration is a follow-up to the `hq-orchd.5` pattern.
/// No registry — the daemon owns one root for one workspace.
pub async fn daemon_root(ws: WorkspaceId, log_root: PathBuf) -> DaemonRoot {
    let ws_slug = ws.as_str().to_string();
    let handle = Arc::new(RootHandle::new(
        ws,
        RootBuilder::new().build().expect("empty root builds"),
    ));

    // Boot hydration (hq-orchd.5): rebuild the pending scheduler queue + in-flight merge board
    // from the durable log before spawning the actors. A corrupt log at boot is fatal.
    let log = EventLog::new(Some(log_root.clone()));
    let sched_pending = replay_scheduling_pending(&log, &ws_slug).expect("replay scheduling log");
    let merge_board = replay_merge_board(&log, &ws_slug).expect("replay merge log");
    // Quota registry hydration (hq-quota-accounts.2): rebuild onboarded accounts + their windows
    // from the log so a restart keeps the rotation pool (and the daemon seeds its keychain from
    // `quota_state.registered`) without depending on the GT_CLAUDE_ACCOUNTS env.
    let quota_state = replay_quota_state(&log, &ws_slug).expect("replay quota log");

    let (sched_tx, sched_rx) = mpsc::channel(64);
    let sched =
        gt_scheduling::actor::spawn_hydrated(InMemoryBeads::default(), sched_tx, 4, sched_pending);
    let (merge_tx, merge_rx) = mpsc::channel(64);
    let merge =
        gt_merge::actor::spawn_hydrated(InMemoryMergeRepo::default(), merge_tx, merge_board);
    let (patrol_tx, patrol_rx) = mpsc::channel(64);
    let patrol = gt_patrol::actor::spawn(InMemoryPatrolRepo::default(), patrol_tx);
    let (quota_tx, quota_rx) = mpsc::channel(64);
    let quota = gt_quota::actor::spawn_hydrated(
        quota_tx,
        std::collections::HashMap::new(),
        AccountRegistry::from_state(&quota_state),
        quota_state.predictions.len(),
    );

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
    handle
        .supervisor()
        .anchor(patrol.clone())
        .await
        .expect("anchors while Built");
    handle
        .supervisor()
        .anchor(quota.clone())
        .await
        .expect("anchors while Built");
    handle.drain_events_from(sched_rx);
    handle.drain_events_from(merge_rx);
    handle.drain_events_from(patrol_rx);
    handle.drain_events_from(quota_rx);

    let (st, _sr) = mpsc::channel(16);
    let (wt, _wr) = mpsc::channel(16);
    let (dt, _dr) = mpsc::channel(16);
    let (mt, _mr) = mpsc::channel(16);
    let (rt, _rr) = mpsc::channel(16);
    let roles = RoleStack::register(
        handle.supervisor(),
        RoleSinks {
            sheriff: st,
            witness: wt,
            deacon: dt,
            mayor: mt,
            refinery: rt,
        },
    )
    .await
    .expect("role stack registers while the supervisor is Built");

    let registry = roles
        .plugin_registry()
        .register(
            EventLogPlugin::for_workspace_in(&log_root, &ws_slug)
                .expect("event log root is writable"),
        )
        .register(SchedulerPlugin::new(sched.clone()))
        .register(MergePlugin::new(merge.clone()))
        .register(BranchGcPlugin::new(
            merge.clone(),
            InMemoryBranchReaper::new(),
        ))
        .register(SheriffPlugin::new())
        .register(SessionMinutesPlugin::new(ws_slug.clone()));
    handle.spawn_plugins(Arc::new(registry));
    handle.start().await;

    DaemonRoot {
        handle,
        sched,
        merge,
        patrol,
        quota,
    }
}

#[cfg(test)]
mod session_minutes_tests {
    use super::*;
    use gt_plugin::Plugin;

    fn agent_record(kind: &str, event: AgentEvent, ts: &str) -> EventRecord {
        EventRecord {
            event_id: "e".into(),
            correlation_id: "c".into(),
            causation_id: None,
            ts: ts.into(),
            kind: kind.into(),
            payload: serde_json::to_value(&event).expect("encode agent event"),
        }
    }

    #[tokio::test]
    async fn projector_bumps_the_session_minutes_counter() {
        // End-to-end: a spawned→session-end pair 5 minutes apart drives the projector to add 5 to
        // gt_workspace_session_minutes for its workspace, which then surfaces in the /metrics text.
        gt_telemetry::metrics::ensure_registered();
        let p = SessionMinutesPlugin::new("acme-smt"); // unique label → deterministic assertion
        p.on_event(&agent_record(
            "agent.spawned.v1",
            AgentEvent::Spawned {
                session: "s1".into(),
                rig: "hq".into(),
                role: Default::default(),
                crew: None,
                skills: Vec::new(),
                hooks: Vec::new(),
                maintains_heartbeat: true,
                tmux_socket: None,
            },
            "2026-06-03T12:00:00Z",
        ))
        .await
        .unwrap();
        p.on_event(&agent_record(
            "agent.session-end.v1",
            AgentEvent::SessionEnd {
                session: "s1".into(),
            },
            "2026-06-03T12:05:00Z",
        ))
        .await
        .unwrap();

        let text = gt_telemetry::metrics::render_text().expect("render");
        assert!(
            text.contains("gt_workspace_session_minutes{workspace=\"acme-smt\"} 5"),
            "expected the projector to add 5 minutes; /metrics text was:\n{text}"
        );
    }

    #[test]
    fn whole_minutes_between_rfc3339_timestamps() {
        // 5 minutes (300s) → 5; 90s → 1 (floored); sub-minute → 0.
        assert_eq!(
            minutes_between("2026-06-03T12:00:00Z", "2026-06-03T12:05:00Z"),
            Some(5)
        );
        assert_eq!(
            minutes_between("2026-06-03T12:00:00Z", "2026-06-03T12:01:30Z"),
            Some(1)
        );
        assert_eq!(
            minutes_between("2026-06-03T12:00:00Z", "2026-06-03T12:00:45Z"),
            Some(0)
        );
    }

    #[test]
    fn out_of_order_or_malformed_is_skipped() {
        // end before start → None (never a negative duration).
        assert_eq!(
            minutes_between("2026-06-03T12:05:00Z", "2026-06-03T12:00:00Z"),
            None
        );
        assert_eq!(minutes_between("not-a-ts", "2026-06-03T12:00:00Z"), None);
    }
}
