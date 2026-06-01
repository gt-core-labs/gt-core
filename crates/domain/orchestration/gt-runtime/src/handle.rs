//! [`RootHandle`] — a composed application [`Root`] bound to one workspace.

use std::fmt;

use gt_module::Root;
use gt_workspace::WorkspaceId;

/// The live application composition for a single workspace.
///
/// Wraps the kernel's read-only [`Root`] (the assembled module registry) with
/// the [`WorkspaceId`] it was built for. It lives in the orchestration tier, not
/// the kernel, because the kernel may not name a [`WorkspaceId`] (one-way dep
/// direction, docs/03) and — as this type grows — must not spawn the actor tasks
/// a live root owns (`tokio::spawn` is forbidden in kernel crates).
///
/// Today the handle is thin: it holds the composed `Root` and exposes it. Later
/// phases extend it with the running actor stack and `start`/`drain` lifecycle
/// (`hq-mt-routing.2`); the [`RootRegistry`](crate::RootRegistry) that holds
/// `Arc<RootHandle>` per workspace does not change shape when that lands.
pub struct RootHandle {
    workspace: WorkspaceId,
    root: Root,
}

impl RootHandle {
    /// Bind a composed [`Root`] to the workspace it was built for.
    pub fn new(workspace: WorkspaceId, root: Root) -> Self {
        RootHandle { workspace, root }
    }

    /// The workspace this root serves.
    pub fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// The composed module registry for this workspace.
    pub fn root(&self) -> &Root {
        &self.root
    }
}

impl fmt::Debug for RootHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootHandle")
            .field("workspace", &self.workspace)
            .field("modules", &self.root.len())
            .finish()
    }
}
