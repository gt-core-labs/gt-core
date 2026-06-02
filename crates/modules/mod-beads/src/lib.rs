//! `mod-beads` — the [`GtModule`] wrap of the dispatcher bead queue (`hq-mod-refactor.2`).
//!
//! A *bead* is the orchestrator's unit of work: the bead **is** the job, and the dispatch
//! queue is a view over beads in a given [`BeadStatus`] (see [`gt_beads`]). Beads also back
//! the kanban board the dashboard renders (`GET/POST /api/beads`), so per the docs/03 tier
//! flowchart — "user-facing feature (board/page/view) → modules/" — the feature lives in the
//! `modules/` tier rather than in the `gt-beads` kernel crate that holds only the vocabulary.
//!
//! This is the sibling of `hq-mod-refactor.1`'s `RigsModule`, but a **faithful wrap is
//! necessarily thin here**, because — unlike `gt-rig`, which is self-contained — the beads
//! domain's contributions are split across the workspace and only the vocabulary has been
//! ported up so far. What this module can declare today, and why the rest is deferred (each
//! additive, so the module keeps compiling as the pieces land):
//!
//! - **Identity** ([`GtModule::meta`]) — id `beads`, singular, matching the `beads.read` /
//!   `beads.write` scope namespace (`meta().id == scope resource`, the same coherence rule
//!   `RigsModule` follows).
//! - **Capability** ([`GtModule::capability`]) — the two scopes the beads surface owns, taken
//!   verbatim from the scopes the live `gt-web` routes already guard with. Declaring them now
//!   lets the `RootBuilder` attribute and conflict-check them before the routes themselves
//!   move over.
//! - **No event kinds** — beads are **not** event-sourced. State transitions mutate the Dolt
//!   store directly through [`gt_beads::BeadRepository`]'s CAS methods (`cas_claim` /
//!   `cas_release`), so there is no `bead.*` event contract to declare (contrast `RigsModule`,
//!   whose five `rig.*` kinds mirror a `RigEvent`).
//! - **No MCP tools** — the beads surface is REST (`/api/beads`), not MCP, so
//!   [`GtModule::register_mcp_tools`] keeps its no-op default (again the inverse of
//!   `RigsModule`, which is MCP-only).
//! - **No migration** — the bead store is **Dolt** (`gt-store-dolt`'s `beads_repo`,
//!   DB-per-workspace `hq_<ws>`), not a Postgres per-workspace projection. So this module owns
//!   no [`GtModule::migrations`]; the bead schema is the Dolt store's concern. (A "beads table
//!   in the `ws_default` PG template" — as `hq-mt-data.4` currently lists — would contradict
//!   the Dolt home and is flagged on that bead.)
//! - **Routes deferred** — the `/api/beads` handlers (list / create / bulk / patch /
//!   transition / comments) still live in the `gt-web` binary. Porting them into
//!   [`GtModule::register_routes`] is bins-decomposition work (`hq-mod-refactor.13`), not this
//!   bead; the empty-router default stands until then.

#![forbid(unsafe_code)]

use gt_beads::BeadStatus;
use gt_module::{Capability, GtModule, ModuleId, ModuleMeta, Scope};
use semver::Version;

/// The dispatch states the beads board surfaces, in kanban column order (`pending` first,
/// terminal states last). The queue is a view over [`gt_beads::Bead`]s in one of these
/// states; this is the column contract the `/api/beads` board renders against, anchored to
/// the wrapped domain's [`BeadStatus`] so a new variant cannot silently drop a column.
pub const QUEUE_COLUMNS: [BeadStatus; 5] = [
    BeadStatus::Pending,
    BeadStatus::Dispatched,
    BeadStatus::Working,
    BeadStatus::Done,
    BeadStatus::Failed,
];

/// The [`GtModule`] facade over the dispatcher bead queue. Zero-sized: the live queue is the
/// Dolt-backed [`gt_beads::BeadRepository`] supplied at the deploy edge, so the unit struct is
/// all the composition root registers.
#[derive(Clone, Copy, Debug, Default)]
pub struct BeadsModule;

impl BeadsModule {
    /// The module's stable id (`beads`). Singular-namespace match: the scopes are
    /// `beads.{read,write}`, so the id is the bare resource `beads`. The literal is a
    /// known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("beads").expect("`beads` is a valid module id")
    }
}

impl GtModule for BeadsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Beads",
            Version::new(1, 0, 0),
            "Dispatcher work-unit queue (the bead is the job; the queue is a view over beads \
             by status) and the kanban board behind it. Backed by Dolt.",
        )
    }

    fn capability(&self) -> Capability {
        // The two scopes the beads surface owns, named exactly as the live `gt-web` routes
        // guard them (`beads.read` on the GET/list paths, `beads.write` on create/patch/
        // transition). No emitted event kinds: beads are not event-sourced (CAS on the Dolt
        // store, no `bead.*` contract).
        Capability::empty().claiming_all([
            Scope::new("beads.read").expect("valid scope"),
            Scope::new("beads.write").expect("valid scope"),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_singular_beads() {
        let m = BeadsModule.meta();
        assert_eq!(m.id.as_str(), "beads");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_the_two_beads_scopes() {
        let cap = BeadsModule.capability();

        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["beads.read", "beads.write"]);

        // Beads are not event-sourced: the module declares no event kinds.
        assert!(cap.emits().is_empty(), "beads emit no versioned event kinds");
    }

    #[test]
    fn registers_no_mcp_tools_rest_surface_not_mcp() {
        let mut reg = gt_module::McpRegistry::new();
        BeadsModule.register_mcp_tools(&mut reg);
        assert!(reg.tools().iter().next().is_none(), "the beads surface is REST, not MCP");
    }

    #[test]
    fn owns_no_migration_dolt_backed_not_a_pg_projection() {
        // The bead store is Dolt (gt-store-dolt), so the PG migration set is empty here.
        assert!(BeadsModule.migrations().is_empty());
    }

    #[test]
    fn contributes_no_routes_until_gt_web_handlers_port() {
        // Routes live in gt-web today; porting them is hq-mod-refactor.13. The default
        // empty router stands until then. Smoke: the default impl builds a router.
        let _router = BeadsModule.register_routes();

        // Beads are MCP-toolless and event-less, so the module also contributes no OpenAPI.
        assert!(BeadsModule.openapi().is_none());
    }

    #[test]
    fn queue_columns_cover_every_bead_status() {
        // The board columns are anchored to the wrapped domain's lifecycle: each BeadStatus
        // variant must appear exactly once, so adding a state forces a column decision.
        for s in [
            BeadStatus::Pending,
            BeadStatus::Dispatched,
            BeadStatus::Working,
            BeadStatus::Done,
            BeadStatus::Failed,
        ] {
            assert!(QUEUE_COLUMNS.contains(&s), "missing board column for {:?}", s);
        }
        assert_eq!(QUEUE_COLUMNS.len(), 5, "no duplicate or extra columns");
    }
}
