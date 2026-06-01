//! [`RootHandle`] — a composed application [`Root`] bound to one workspace.

use std::fmt;

use gt_module::Root;
use gt_workspace::WorkspaceId;

use crate::lifecycle::Supervisor;
use crate::Phase;

/// The live application composition for a single workspace.
///
/// Wraps the kernel's read-only [`Root`] (the assembled module registry) with
/// the [`WorkspaceId`] it was built for. It lives in the orchestration tier, not
/// the kernel, because the kernel may not name a [`WorkspaceId`] (one-way dep
/// direction, docs/03) and — as this type grows — must not spawn the actor tasks
/// a live root owns (`tokio::spawn` is forbidden in kernel crates).
///
/// It holds the composed `Root`, exposes it, and owns the [`Supervisor`] that
/// runs the workspace's actor stack (`hq-mt-routing.2`). The handle binds the two
/// for their shared lifetime: when the [`RootRegistry`](crate::RootRegistry)
/// evicts a workspace, dropping the `Arc<RootHandle>` drops the supervisor, which
/// cancels the actors. A freshly [`new`](Self::new) handle is in
/// [`Phase::Built`](crate::Phase::Built) with no actors running; the registry's
/// hydrate closure registers actors and calls [`start`](Self::start) before
/// publishing it. The registry shape is unchanged — it still holds
/// `Arc<RootHandle>` per workspace.
pub struct RootHandle {
    workspace: WorkspaceId,
    root: Root,
    supervisor: Supervisor,
}

impl RootHandle {
    /// Bind a composed [`Root`] to the workspace it was built for, with an
    /// unstarted actor [`Supervisor`].
    pub fn new(workspace: WorkspaceId, root: Root) -> Self {
        RootHandle { workspace, root, supervisor: Supervisor::new() }
    }

    /// The workspace this root serves.
    pub fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// The composed module registry for this workspace.
    pub fn root(&self) -> &Root {
        &self.root
    }

    /// The actor-stack lifecycle for this workspace — register actors and drive
    /// start/drain/shutdown through it. See [`Supervisor`].
    pub fn supervisor(&self) -> &Supervisor {
        &self.supervisor
    }

    /// Current lifecycle [`Phase`] of the actor stack. Convenience for
    /// `supervisor().phase()`.
    pub fn phase(&self) -> Phase {
        self.supervisor.phase()
    }

    /// Spawn the registered actors. Delegates to [`Supervisor::start`].
    pub async fn start(&self) -> bool {
        self.supervisor.start().await
    }

    /// Gracefully await in-flight actor work. Delegates to [`Supervisor::drain`].
    pub async fn drain(&self) {
        self.supervisor.drain().await
    }

    /// Cancel the actor stack and join. Delegates to [`Supervisor::shutdown`].
    pub async fn shutdown(&self) {
        self.supervisor.shutdown().await
    }
}

impl fmt::Debug for RootHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootHandle")
            .field("workspace", &self.workspace)
            .field("modules", &self.root.len())
            .field("phase", &self.supervisor.phase())
            .finish()
    }
}
