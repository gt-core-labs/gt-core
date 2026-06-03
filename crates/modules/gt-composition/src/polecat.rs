//! Autonomous polecat supervision for the orchestration daemon (`hq-orchd.3`).
//!
//! This is the production caller-loop `gt-polecat` was missing: the library has the spawner
//! ([`spawn_tmux`] / [`SpawnTemplate`]), the death-detector ([`PolecatSupervisor`]), and the pure
//! admission core ([`PoolAllocator`]), but nothing wired them onto a running event hub. The daemon
//! does:
//!
//! - **Sling on dispatch** — [`PolecatSupervisorPlugin`] observes the workspace hub: a
//!   `scheduling.dispatched.v1` claims a pool slot and, if admitted, spawns a tmux-backed polecat
//!   for the dispatched bead and registers it with the supervisor. A `merge.merged.v1` means that
//!   bead's work landed, so the polecat is unwatched (never re-slung) and its slot released.
//! - **Re-sling dead ones** — the bin drives [`PolecatSupervisor::tick`] on a timer (the live half
//!   of supervision); a polecat whose tmux session died is re-slung with backoff until its work
//!   completes.
//! - **Track host capacity** — [`host_cap_from_metrics`] recomputes the host-wide admission cap
//!   from live CPU + RAM, which the bin feeds to [`PoolAllocator::set_host_cap`] on a timer so the
//!   ceiling tracks real headroom (`hq-mt-deploy.5`, folded here).
//!
//! Eviction on a shrunk cap is *not* done — admission is side-effect-free, running polecats finish
//! naturally (NN#2, mirrored from [`gt_polecat::pool`]).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::broadcast;

use gt_agent::{AgentEvent, SessionRole};
use gt_eventlog::EventRecord;
use gt_events::{AppError, Envelope};
use gt_merge::MergeEvent;
use gt_polecat::{spawn_tmux, PoolAllocator, PolecatSupervisor, SpawnTemplate, Tmux};
use gt_plugin::Plugin;
use gt_scheduling::SchedEvent;

/// Observer that turns the scheduler's dispatch decisions into live, supervised tmux polecats for
/// one workspace, bounded by a shared [`PoolAllocator`]. Registered on the daemon's event hub
/// alongside the reactor arms (`hq-orchd.3`).
pub struct PolecatSupervisorPlugin {
    workspace: String,
    tmux: Arc<dyn Tmux>,
    template: SpawnTemplate,
    supervisor: Arc<PolecatSupervisor>,
    allocator: Arc<Mutex<PoolAllocator>>,
    /// Hub sender for the agent session lifecycle events (`hq-orchd.6`). `None` ⇒ no emission
    /// (the `.3` tests construct the plugin without a hub).
    events: Option<broadcast::Sender<EventRecord>>,
}

impl PolecatSupervisorPlugin {
    /// Wire the dispatch→sling observer for `workspace`. `tmux` is the edge adapter (real
    /// [`TmuxCli`](gt_polecat::TmuxCli) in the daemon, a fake in tests); `template` is the rig's
    /// env-sourced [`SpawnTemplate`]; `supervisor` + `allocator` are shared with the bin's
    /// re-sling + capacity timers.
    pub fn new(
        workspace: impl Into<String>,
        tmux: Arc<dyn Tmux>,
        template: SpawnTemplate,
        supervisor: Arc<PolecatSupervisor>,
        allocator: Arc<Mutex<PoolAllocator>>,
    ) -> Self {
        Self {
            workspace: workspace.into(),
            tmux,
            template,
            supervisor,
            allocator,
            events: None,
        }
    }

    /// Emit the agent session lifecycle events (`agent.spawned.v1` on sling, `agent.session-end.v1`
    /// when the bead merges) onto `events` (`hq-orchd.6`). These flow through the hub to the
    /// session-minutes projector (and the durable log), so a slung polecat's runtime feeds the
    /// `gt_workspace_session_minutes` cost counter. Without this, the plugin only supervises.
    pub fn with_session_events(mut self, events: broadcast::Sender<EventRecord>) -> Self {
        self.events = Some(events);
        self
    }

    /// Publish an [`AgentEvent`] onto the hub if a sender is wired (best-effort: a closed hub or an
    /// encode failure is swallowed — session metrics are observational, never load-bearing).
    fn emit(&self, event: AgentEvent) {
        if let Some(tx) = &self.events {
            if let Ok(record) = EventRecord::from_envelope(&Envelope::root(event)) {
                let _ = tx.send(record);
            }
        }
    }
}

#[async_trait]
impl Plugin for PolecatSupervisorPlugin {
    fn name(&self) -> &'static str {
        "polecat-supervisor"
    }

    async fn on_event(&self, record: &EventRecord) -> Result<(), AppError> {
        match record.kind.as_str() {
            // A bead was dispatched → admit it against the pool, then sling a supervised polecat.
            "scheduling.dispatched.v1" => {
                let SchedEvent::Dispatched { bead, .. } = record.decode::<SchedEvent>()? else {
                    return Ok(());
                };
                // Admission first: a refused claim is backpressure, not an error — the bead stays
                // queued/dispatched in the log; capacity will free up as live polecats finish.
                if self.allocator.lock().expect("pool mutex").claim(&self.workspace).is_err() {
                    eprintln!(
                        "[polecat] sling skipped for {bead}: pool/host cap reached (workspace {})",
                        self.workspace
                    );
                    return Ok(());
                }
                let spec = self.template.spec_for(&self.workspace, &bead);
                if let Err(e) = spawn_tmux(self.tmux.as_ref(), &spec) {
                    // Spawn failed → undo the claim so the slot is not leaked.
                    self.allocator.lock().expect("pool mutex").release(&self.workspace);
                    eprintln!("[polecat] sling failed for {bead}: {e}");
                    return Ok(());
                }
                // Open the agent session (hq-orchd.6): the slung polecat IS the session; its start
                // timestamp anchors the session-minutes projection. Built before `watch` consumes
                // the spec.
                self.emit(AgentEvent::Spawned {
                    session: spec.session.clone(),
                    rig: spec.rig.clone(),
                    role: SessionRole::default(),
                    crew: spec.crew.clone(),
                });
                self.supervisor.watch(spec);
                Ok(())
            }
            // The bead's merge landed → its work is done: stop supervising it and free its slot.
            "merge.merged.v1" => {
                let MergeEvent::Merged { bead, .. } = record.decode::<MergeEvent>()? else {
                    return Ok(());
                };
                self.supervisor.unwatch_member(&bead);
                self.allocator.lock().expect("pool mutex").release(&self.workspace);
                // Close the agent session for that bead (hq-orchd.6): the session id is the
                // deterministic `spec_for` session, so it matches the `Spawned` emitted at sling.
                let session = self.template.spec_for(&self.workspace, &bead).session;
                self.emit(AgentEvent::SessionEnd { session });
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Compute the host-wide polecat admission cap from live metrics (`hq-orchd.3`): the lesser of the
/// CPU-core count and `MemAvailable / per-polecat budget`, floored at 1 so the daemon can always
/// make progress. Linux-native (`/proc/meminfo` + std), no external dependency — keeping the
/// dependency-light discipline of the kernel/domain tiers.
///
/// The per-polecat RAM budget is `GT_POLECAT_MEM_MB` (default 1024). When `/proc/meminfo` is
/// unreadable (non-Linux / sandbox), the cap falls back to the core count alone.
pub fn host_cap_from_metrics() -> usize {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let per_polecat_mb = std::env::var("GT_POLECAT_MEM_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&m| m > 0)
        .unwrap_or(1024);
    let cap_by_mem = match mem_available_mb() {
        Some(avail) => (avail / per_polecat_mb).max(1),
        None => cores,
    };
    cores.min(cap_by_mem).max(1)
}

/// `MemAvailable` from `/proc/meminfo`, in MiB. `None` when the file is absent or malformed.
fn mem_available_mb() -> Option<usize> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            // Format: `MemAvailable:   12345678 kB`.
            let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_eventlog::EventRecord;
    use gt_events::{Envelope, EventKind};
    use gt_polecat::{FakeTmux, RestartConfig};

    fn record<E: EventKind + serde::Serialize>(event: E) -> EventRecord {
        EventRecord::from_envelope(&Envelope::root(event)).expect("encode")
    }

    fn plugin(alloc: Arc<Mutex<PoolAllocator>>) -> (PolecatSupervisorPlugin, Arc<PolecatSupervisor>) {
        let tmux: Arc<dyn Tmux> = Arc::new(FakeTmux::new());
        let supervisor = Arc::new(PolecatSupervisor::new(
            tmux.clone(),
            RestartConfig::default(),
            8,
        ));
        let template = SpawnTemplate {
            rig: "hq".into(),
            prefix: "hq".into(),
            workdir: "/tmp".into(),
            command: "true".into(),
            args: vec![],
            base_env: vec![],
            heartbeat_dir: std::env::temp_dir(),
        };
        let p = PolecatSupervisorPlugin::new("acme", tmux, template, supervisor.clone(), alloc);
        (p, supervisor)
    }

    #[tokio::test]
    async fn dispatch_slings_and_watches_a_polecat_within_capacity() {
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched {
            bead: "gg-1".into(),
            worker: "w1".into(),
        }))
        .await
        .unwrap();

        assert_eq!(sup.watched_count(), 1, "dispatched bead is now supervised");
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 1, "a pool slot was claimed");

        // The merge of that bead frees the slot + stops supervision.
        p.on_event(&record(MergeEvent::Merged { bead: "gg-1".into(), sha: "abc".into() }))
            .await
            .unwrap();
        assert_eq!(sup.watched_count(), 0, "merged bead is unwatched");
        assert_eq!(alloc.lock().unwrap().in_flight("acme"), 0, "slot released");
    }

    #[tokio::test]
    async fn dispatch_beyond_capacity_is_backpressure_not_a_sling() {
        // host cap 1, default pool 1: the second dispatch must be refused, not spawned.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(1, 1)));
        let (p, sup) = plugin(alloc.clone());

        p.on_event(&record(SchedEvent::Dispatched { bead: "gg-1".into(), worker: "w1".into() }))
            .await
            .unwrap();
        p.on_event(&record(SchedEvent::Dispatched { bead: "gg-2".into(), worker: "w2".into() }))
            .await
            .unwrap();

        assert_eq!(sup.watched_count(), 1, "only the admitted polecat is supervised");
        assert_eq!(alloc.lock().unwrap().total_in_flight(), 1, "host cap held");
    }

    #[test]
    fn host_cap_is_at_least_one() {
        assert!(host_cap_from_metrics() >= 1);
    }

    #[tokio::test]
    async fn sling_and_merge_emit_agent_session_lifecycle_events() {
        // hq-orchd.6: with a hub wired, a dispatch opens the session (agent.spawned.v1) and the
        // bead's merge closes it (agent.session-end.v1) — the two records the projector pairs.
        let alloc = Arc::new(Mutex::new(PoolAllocator::new(10, 5)));
        let (p, _sup) = plugin(alloc);
        let (tx, mut rx) = broadcast::channel::<EventRecord>(16);
        let p = p.with_session_events(tx);

        p.on_event(&record(SchedEvent::Dispatched { bead: "gg-1".into(), worker: "w1".into() }))
            .await
            .unwrap();
        let opened = rx.try_recv().expect("a session-open record was emitted");
        assert_eq!(opened.kind, "agent.spawned.v1");

        p.on_event(&record(MergeEvent::Merged { bead: "gg-1".into(), sha: "abc".into() }))
            .await
            .unwrap();
        let closed = rx.try_recv().expect("a session-close record was emitted");
        assert_eq!(closed.kind, "agent.session-end.v1");
    }
}
