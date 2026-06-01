//! `IssuesModule` — the [`GtModule`] facade over the Dolt issues store
//! (hq-core-host.2).
//!
//! Routes the issues service's *existing* contributions through the kernel module
//! seam without changing any domain logic: the tool-arg structs ([`crate::commands`]),
//! the handlers ([`crate::handlers`]), and the resource reads ([`crate::resources`])
//! all stay put; this file only *declares* what the issues service offers so the
//! `RootBuilder` harvests it instead of the composition root hand-wiring tools
//! (`docs/03` rule 3).
//!
//! What it declares:
//!
//! - **Identity** ([`GtModule::meta`]) — id `issues`, semver, description.
//! - **Capability** ([`GtModule::capability`]) — the `issues.read` / `issues.write`
//!   scopes the module owns. The issues tools write Dolt directly (no domain-event
//!   bus traversal), so the module emits no event kinds.
//! - **MCP tools** ([`GtModule::register_mcp_tools`]) — the ten
//!   `issues.{create,update,transition,close,claim}.{validate,execute}` tools, names
//!   + descriptions verbatim from the gastown `gt-mcp` service, each carrying the
//!   JSON input schema generated from its arg struct.
//!
//! It does **not** override `register_routes`/`openapi`/`migrations`: issues are
//! managed over MCP only, and the `issues` table is owned by `bd` (the store's
//! `ensure_schema` adds the taxonomy columns idempotently), so the empty defaults
//! stand.

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use semver::Version;

use crate::commands::{ClaimIssue, CloseIssue, CreateIssue, TransitionIssue, UpdateIssue};

/// The [`GtModule`] facade over the issues store. Zero-sized: the module owns no
/// runtime state of its own (the live store is the [`DoltIssues`] the binary
/// supplies to the handlers), so the unit struct is all the composition root
/// registers.
///
/// [`DoltIssues`]: gt_store_dolt::DoltIssues
#[derive(Clone, Copy, Debug, Default)]
pub struct IssuesModule;

impl IssuesModule {
    /// The module's stable id (`issues`). Matches the `issues.*` MCP-tool and
    /// `issues.<verb>` scope namespaces. The literal is a known-valid slug.
    pub fn id() -> ModuleId {
        ModuleId::new("issues").expect("`issues` is a valid module id")
    }
}

impl GtModule for IssuesModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Issues",
            Version::new(1, 0, 0),
            "Issue tracker backed by Dolt — create/update/transition/close/claim beads \
             over MCP, with NN-16 taxonomy validation and optimistic concurrency.",
        )
    }

    fn capability(&self) -> Capability {
        // The `issues.read` / `issues.write` scopes the module owns (same
        // `<resource>.<verb>` convention `rig`/`merge`/`quota` follow). No event
        // kinds: the issues tools persist to Dolt directly, not through the bus.
        Capability::empty().claiming_all([
            Scope::new("issues.read").expect("valid scope"),
            Scope::new("issues.write").expect("valid scope"),
        ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        // The ten issues tools the `gt-mcp` service serves today — a `validate`
        // (no state change) and an `execute` per command — names + descriptions
        // verbatim, each carrying the JSON input schema derived from its arg
        // struct so an MCP `tools/list` (`meta.help`) entry is complete.
        registry
            .tool_with_schema(
                "issues.create.validate",
                "Check whether creating an issue in hq.issues would be accepted (shape-only; \
                 uniqueness enforced at execute). No state change.",
                schema_for::<CreateIssue>(),
            )
            .tool_with_schema(
                "issues.create.execute",
                "Insert a new row in hq.issues + atomic Dolt commit. Closes the docker-exec \
                 bypass for agent-created beads.",
                schema_for::<CreateIssue>(),
            )
            .tool_with_schema(
                "issues.update.validate",
                "Check whether patching hq.issues fields (title/description/design/\
                 acceptance_criteria/notes/priority/issue_type/assignee/owner/external_ref/\
                 domain/surface/depends_on) would be accepted. domain/surface/depends_on \
                 overwrite the JSON-array columns. Optional expected_version guards against \
                 concurrent clobber (optimistic concurrency). Empty patch rejected. No state \
                 change. On success, echoes the parsed params back so callers can confirm \
                 what the server received.",
                schema_for::<UpdateIssue>(),
            )
            .tool_with_schema(
                "issues.update.execute",
                "Patch editable hq.issues columns + atomic Dolt commit. Pass expected_version \
                 (from gt://issue/{id}) for optimistic concurrency — a stale write fails with \
                 'version conflict' instead of clobbering. On success returns the post-bump \
                 version so a follow-up edit can chain expected_version without re-reading. \
                 Status transitions go through issues.transition.*.",
                schema_for::<UpdateIssue>(),
            )
            .tool_with_schema(
                "issues.transition.validate",
                "Check whether a status transition (open ↔ working; either -> closed; \
                 closed -> open) would be accepted. No state change.",
                schema_for::<TransitionIssue>(),
            )
            .tool_with_schema(
                "issues.transition.execute",
                "Move hq.issues.status across the open/working/closed state machine + atomic \
                 Dolt commit. Illegal moves (e.g. closed -> working) are rejected by the \
                 backend.",
                schema_for::<TransitionIssue>(),
            )
            .tool_with_schema(
                "issues.close.validate",
                "Check whether closing an issue (status open|working -> closed) would be \
                 accepted, including the required commit_sha (7+ hex) delivered-code proof. \
                 No state change.",
                schema_for::<CloseIssue>(),
            )
            .tool_with_schema(
                "issues.close.execute",
                "Close hq.issues row + stamp closed_at + closed_by_session (defaults to MCP \
                 actor) + record delivering commit_sha (required, 7+ hex) as a note + atomic \
                 Dolt commit. Close without commit_sha is rejected (delivered-code proof). \
                 Already-closed rows reject; missing id returns NotFound.",
                schema_for::<CloseIssue>(),
            )
            .tool_with_schema(
                "issues.claim.validate",
                "Check whether an atomic claim would be accepted (shape only — id non-empty). \
                 No state change. The owner is the MCP scope actor, server-injected.",
                schema_for::<ClaimIssue>(),
            )
            .tool_with_schema(
                "issues.claim.execute",
                "Atomically claim an OPEN, unowned bead for the calling actor: single \
                 compare-and-swap that flips status open->working and stamps owner/assignee. \
                 Two agents racing the same bead cannot both win — the loser gets an error \
                 naming the holder. Re-claiming a bead you already hold is idempotent. On \
                 success returns the post-bump version so a follow-up issues.update can pass \
                 it as expected_version without re-reading. Missing id returns NotFound.",
                schema_for::<ClaimIssue>(),
            );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_identity_is_issues() {
        let m = IssuesModule.meta();
        assert_eq!(m.id.as_str(), "issues");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn capability_owns_issues_scopes_and_emits_nothing() {
        let cap = IssuesModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["issues.read", "issues.write"]);
        assert!(cap.emits().is_empty());
    }

    #[test]
    fn registers_the_ten_issues_tools() {
        let mut reg = McpRegistry::new();
        IssuesModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "issues.create.validate",
                "issues.create.execute",
                "issues.update.validate",
                "issues.update.execute",
                "issues.transition.validate",
                "issues.transition.execute",
                "issues.close.validate",
                "issues.close.execute",
                "issues.claim.validate",
                "issues.claim.execute",
            ]
        );
    }

    #[test]
    fn every_tool_name_is_well_formed_and_namespaced() {
        // Each name is `<module-id>.<action>.<verb>` with module id == `issues`.
        let mut reg = McpRegistry::new();
        IssuesModule.register_mcp_tools(&mut reg);
        for tool in reg.tools() {
            let (module, _action, verb) = tool.parse_name().expect("well-formed tool name");
            assert_eq!(module, "issues", "tool {} not namespaced to issues", tool.name);
            assert!(verb == "validate" || verb == "execute", "unexpected verb in {}", tool.name);
        }
    }

    #[test]
    fn execute_tools_carry_an_object_input_schema() {
        let mut reg = McpRegistry::new();
        IssuesModule.register_mcp_tools(&mut reg);
        let create = reg
            .tools()
            .iter()
            .find(|t| t.name == "issues.create.execute")
            .expect("create.execute present");
        assert_eq!(create.input_schema["type"], "object");
        assert!(create.input_schema["properties"]["id"].is_object());
    }

    #[test]
    fn contributes_no_routes_or_openapi() {
        // Issues are MCP-only; the empty-router / no-OpenAPI / no-migration
        // defaults stand.
        assert!(IssuesModule.openapi().is_none());
        assert!(IssuesModule.migrations().is_empty());
    }
}
