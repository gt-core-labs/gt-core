//! `BoardModule` — the [`GtModule`] facade over the Kanban board projection
//! (hq-62130a, epic hq-56b5ee).
//!
//! MCP-FIRST (ADR hq-423a4b D6): the board ships as MCP tools on gt-core and the
//! REST/gt-web surface consumes the SAME transport-free handlers
//! ([`crate::board`]). The module owns the `board.read` / `board.write` scopes;
//! the server routes the `board.*` namespace to the issues dispatch (the board
//! is a projection over the same Dolt store — no store of its own).

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::board::{BoardList, BoardMove, BoardReorder, BoardScopes};

/// The [`GtModule`] facade for the board projection. Stateless for the MCP
/// harvest path, exactly like [`IssuesModule`](crate::IssuesModule); the live
/// store is the [`DoltIssues`](gt_store_dolt::DoltIssues) the server resolves
/// per call. Built [`with_http`](Self::with_http) it also serves the REST
/// surface under `/api/v1/board`.
#[derive(Clone, Default)]
pub struct BoardModule {
    /// REST state, present only when the binary opts into the HTTP surface.
    #[cfg(feature = "axum")]
    http: Option<crate::http::IssuesApiState>,
}

impl BoardModule {
    /// The module's stable id (`board`). Matches the `board.*` MCP-tool and
    /// `board.<verb>` scope namespaces.
    pub fn id() -> ModuleId {
        ModuleId::new("board").expect("`board` is a valid module id")
    }

    /// Build a module that also serves the REST surface, reusing the issues
    /// API state (same store resolution + actor + event sink).
    #[cfg(feature = "axum")]
    pub fn with_http(state: crate::http::IssuesApiState) -> Self {
        Self { http: Some(state) }
    }
}

impl GtModule for BoardModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Board",
            Version::new(1, 0, 0),
            "Kanban board projection over the issues tracker — list status columns \
             ordered by lexorank, move cards across columns (status transition + rank \
             in one atomic Dolt commit), reorder within a column. Every call is scoped \
             by (rig, workspace).",
        )
    }

    fn capability(&self) -> Capability {
        Capability::empty().claiming_all([
            Scope::new("board.read").expect("valid scope"),
            Scope::new("board.write").expect("valid scope"),
        ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        registry
            .tool_with_schema(
                "board.list.execute",
                "The Kanban board for one (rig, workspace): beads bucketed into status \
                 columns (open/working/closed) ordered by board_rank (lexorank), with \
                 optional epic filter and group_by lanes (assignee|epic|priority). \
                 Read-only — no state change. The board is a projection over hq.issues; \
                 cards ARE beads.",
                schema_for::<BoardList>(),
            )
            .tool_with_schema(
                "board.scopes.execute",
                "The distinct (rig, workspace) pairs present in the tracker — the REAL \
                 boards. Use it to offer/validate scope selectors against actual data: \
                 the catalog cross-product offers combinations with no board, and bead \
                 namespaces (e.g. `hq`) need not match rig-catalog names. Read-only.",
                schema_for::<BoardScopes>(),
            )
            .tool_with_schema(
                "board.move.validate",
                "Check whether a board move (column change + placement) would be \
                 accepted: id + (rig, workspace) scope present, target column in \
                 open/working/closed. No state change.",
                schema_for::<BoardMove>(),
            )
            .tool_with_schema(
                "board.move.execute",
                "Move a card to a column at a position: status transition + board_rank \
                 set in ONE atomic Dolt commit. Placement via after/before neighbor \
                 card ids (omit both to append at the bottom). The issues.transition \
                 state machine applies; a card outside the caller's (rig, workspace) \
                 scope is rejected. Returns the rank the card landed at.",
                schema_for::<BoardMove>(),
            )
            .tool_with_schema(
                "board.reorder.validate",
                "Check whether a rank-only reorder within the card's current column \
                 would be accepted (shape-only). No state change.",
                schema_for::<BoardReorder>(),
            )
            .tool_with_schema(
                "board.reorder.execute",
                "Reorder a card WITHIN its current column (board_rank only — no status \
                 change) via after/before neighbor card ids. Lexorank midpoint with \
                 automatic column rebalance on rank exhaustion; lazy backfill ranks an \
                 untouched column on first reorder. Scoped by (rig, workspace).",
                schema_for::<BoardReorder>(),
            );
    }

    /// The board REST routes, mounted by the builder under `/api/v1/board`
    /// behind the `board.read`/`board.write` scope guard. Present only when the
    /// module was built [`with_http`](Self::with_http).
    #[cfg(feature = "axum")]
    fn register_routes(&self) -> axum::Router {
        match &self.http {
            Some(state) => crate::board_http::board_router(state.clone()),
            None => axum::Router::new(),
        }
    }

    /// The OpenAPI spec for the board REST routes, contributed only with HTTP state.
    #[cfg(feature = "axum")]
    fn openapi(&self) -> Option<utoipa::openapi::OpenApi> {
        use utoipa::OpenApi;
        self.http.as_ref().map(|_| crate::board_http::BoardApiDoc::openapi())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_board() {
        let m = BoardModule::default().meta();
        assert_eq!(m.id.as_str(), "board");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_board_scopes() {
        let cap = BoardModule::default().capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["board.read", "board.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_the_board_tools() {
        let mut reg = McpRegistry::new();
        BoardModule::default().register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "board.list.execute",
                "board.scopes.execute",
                "board.move.validate",
                "board.move.execute",
                "board.reorder.validate",
                "board.reorder.execute",
            ]
        );
    }

    #[test]
    fn every_tool_is_namespaced_to_board() {
        let mut reg = McpRegistry::new();
        BoardModule::default().register_mcp_tools(&mut reg);
        for tool in reg.tools() {
            let (module, _action, verb) = tool.parse_name().expect("well-formed tool name");
            assert_eq!(module, "board", "tool {} not namespaced to board", tool.name);
            assert!(matches!(verb, "validate" | "execute"), "unexpected verb in {}", tool.name);
        }
    }

    #[test]
    fn tools_carry_object_input_schemas() {
        let mut reg = McpRegistry::new();
        BoardModule::default().register_mcp_tools(&mut reg);
        let mv = reg
            .tools()
            .iter()
            .find(|t| t.name == "board.move.execute")
            .expect("move.execute present");
        assert_eq!(mv.input_schema["type"], "object");
        assert!(mv.input_schema["properties"]["id"].is_object());
        assert!(mv.input_schema["properties"]["workspace"].is_object());
    }
}
