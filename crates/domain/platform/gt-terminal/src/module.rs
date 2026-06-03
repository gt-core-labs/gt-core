//! `TerminalModule` — the [`GtModule`] wrapper over the dashboard sidebar surface
//! (`hq-mod-refactor.9`).
//!
//! Like [`RigsModule`](../../gt-rig/src/module.rs) before it, this routes the sidebar's
//! *existing* contribution through the kernel's module seam **without changing any domain
//! logic**. The attach substrate still lives in [`crate::port`] / [`crate::tmux_attach`] /
//! [`crate::pty_attach`]; this file only *declares* what the surface already offers so the
//! `RootBuilder` can harvest it instead of the composition root hand-wiring scopes
//! (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `terminal`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `terminal.attach` and `worktrees.read`
//!   scopes the module owns (the dock-terminal WS and the sidebar Worktrees feed).
//!
//! ## Why `worktrees.read` lives here (the bead's sidebar bundle)
//!
//! `hq-mod-refactor.9` scopes "sidebar Worktrees / term WS" as one wrap. The dock terminal
//! (`terminal.attach`, `GET /api/sessions/:id/term`) is gt-terminal's own substrate; the
//! sidebar Worktrees feed (`worktrees.read`, `GET /api/worktrees` + `/api/worktrees/stream`)
//! is filesystem-scan logic with **no domain crate of its own** (it lives in `gt-web`'s
//! routes) and no separate refactor bead. Both are the dashboard's left-rail surface, so
//! this module is their declared home — it claims both scopes. There is a deliberate slight
//! stretch of the `id == single scope resource` convention (the module owns two resources,
//! `terminal` + `worktrees`); it is recorded here because the alternative is an orphan
//! `worktrees.read` scope no module owns.
//!
//! ## Why this wrap is thin (no logic change)
//!
//! - **no emitted kinds** — the attach substrate is a transport, not an event source; the
//!   worktrees feed is a filesystem snapshot. Neither emits.
//! - **no MCP tools** — both surfaces are HTTP/WS reads, not MCP commands.
//! - **no migrations** — no schema (live tmux/pty handles; on-disk worktree scan).
//! - **no routes here** — the `terminal.attach`/`worktrees.read` handlers are still generic
//!   over `gt-web`'s application state (the WS owns the async loop; the worktrees scan reads
//!   the deploy root). Detangling them into [`register_routes`](GtModule::register_routes)
//!   is `hq-mod-routes.5`'s job; moving them here would change logic, which this faithful
//!   wrap must not. The empty-router default stands until then — the scopes this module
//!   declares are what those guards already name.

use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The [`GtModule`] facade over the dashboard sidebar surface. Zero-sized: the live attach
/// handles are owned by the per-attach task the WS route spawns, and the worktrees feed is a
/// filesystem scan, so the unit struct is all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct TerminalModule;

impl TerminalModule {
    /// The module's stable id (`terminal`). Matches the existing `terminal.attach` scope
    /// resource and the dock-terminal route namespace. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("terminal").expect("`terminal` is a valid module id")
    }
}

impl GtModule for TerminalModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Terminal",
            Version::new(1, 0, 0),
            "Dashboard sidebar surface — the dock-terminal attach (`terminal.attach`, a duplex \
             WS over a live tmux session or fresh pty) and the sidebar Worktrees feed \
             (`worktrees.read`). Read/attach only; emits no events and owns no schema.",
        )
    }

    fn capability(&self) -> Capability {
        // The `terminal.attach` (dock-terminal WS) + `worktrees.read` (sidebar Worktrees
        // feed) scopes the module owns — the guards `gt-web` already names on
        // `GET /api/sessions/:id/term` and `GET /api/worktrees{,/stream}`. No `emitting`:
        // both surfaces are transports/snapshots, not event sources.
        Capability::empty().claiming_all([
            Scope::new("terminal.attach").expect("valid scope"),
            Scope::new("worktrees.read").expect("valid scope"),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::{EventKind, McpRegistry};

    #[test]
    fn meta_id_is_terminal() {
        let meta = TerminalModule.meta();
        assert_eq!(meta.id.as_str(), "terminal");
        assert_eq!(meta.id, TerminalModule::id());
        assert_eq!(meta.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_claims_terminal_attach_and_worktrees_read() {
        let cap = TerminalModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["terminal.attach", "worktrees.read"]);
    }

    #[test]
    fn emits_nothing() {
        // Transport substrate + filesystem snapshot — no event source.
        let cap = TerminalModule.capability();
        let kinds: Vec<&EventKind> = cap.emits().iter().collect();
        assert!(kinds.is_empty());
    }

    #[test]
    fn registers_no_mcp_tools() {
        // Both surfaces are HTTP/WS reads, not MCP commands.
        let mut reg = McpRegistry::new();
        TerminalModule.register_mcp_tools(&mut reg);
        assert!(reg.tools().is_empty());
    }

    #[test]
    fn owns_no_migration() {
        // No schema — live tmux/pty handles + on-disk worktree scan.
        assert!(TerminalModule.migrations().is_empty());
    }

    #[test]
    fn contributes_no_route_or_openapi_surface() {
        // The sidebar surface is still gt-web-state-coupled; harvesting it is
        // hq-mod-routes.5. The default empty router + no OpenAPI stand here.
        assert!(TerminalModule.openapi().is_none());
    }
}
