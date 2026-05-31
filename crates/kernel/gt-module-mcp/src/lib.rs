//! MCP integration for the module system (`hq-mod-mcp.3`).
//!
//! `gt-module` (the kernel foundation) owns the contribution seam — a
//! [`GtModule`](gt_module::GtModule) pushes [`McpTool`](gt_module::McpTool)s
//! into an [`McpRegistry`](gt_module::McpRegistry), the builder namespaces and
//! collects them, and a built [`Root`](gt_module::Root) exposes the enabled set
//! through [`Root::mcp_tools`](gt_module::Root::mcp_tools). That crate stays
//! dependency-light (serde only).
//!
//! This crate carries the heavier `schemars` + `serde_json` machinery used to
//! turn those declarations into the JSON an MCP client speaks:
//!
//! - [`schema_for`] generates a JSON input schema from a Rust type, so a module
//!   declares `tool_with_schema(name, desc, schema_for::<CreateBead>())` instead
//!   of hand-writing JSON Schema;
//! - [`meta_help`] assembles a `tools/list` (`meta.help`) response from a built
//!   `Root` — the enabled tools, in init order, each as
//!   `{ name, description, inputSchema }`.
//!
//! Wiring this into the `gt-mcp` service (replacing its hand-listed `#[tool]`
//! table) is `hq-mod-mcp.4`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use gt_module::Root;

/// Generate a JSON Schema [`Value`](serde_json::Value) for a type's MCP input.
///
/// Thin wrapper over `schemars` so a module declares a tool's argument shape
/// from a Rust struct rather than hand-written JSON:
///
/// ```
/// use schemars::JsonSchema;
///
/// #[derive(JsonSchema)]
/// struct CreateBead {
///     title: String,
/// }
///
/// let schema = gt_module_mcp::schema_for::<CreateBead>();
/// assert_eq!(schema["type"], "object");
/// ```
pub fn schema_for<T: schemars::JsonSchema>() -> serde_json::Value {
    let schema = schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>();
    // RootSchema serializes infallibly; fall back to the permissive empty-object
    // schema rather than panic if a future schemars version ever changes that.
    serde_json::to_value(schema).unwrap_or_else(|_| serde_json::json!({}))
}

/// Build the MCP `tools/list` (`meta.help`) payload from a built [`Root`].
///
/// Returns `{ "tools": [ { name, description, inputSchema }, ... ] }` over the
/// enabled modules' tools, in module init order then per-module declaration
/// order. Tools from feature-flag-disabled modules are already absent from the
/// `Root`, so they never appear here (`hq-mod-mcp.5` asserts this end to end).
pub fn meta_help(root: &Root) -> serde_json::Value {
    let tools: Vec<_> = root.mcp_tools().collect();
    serde_json::json!({ "tools": tools })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::{GtModule, McpRegistry, ModuleId, ModuleMeta, RootBuilder};
    use schemars::JsonSchema;
    use semver::Version;

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct CreateBead {
        title: String,
        priority: u8,
    }

    #[test]
    fn schema_for_describes_an_object() {
        let schema = schema_for::<CreateBead>();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["title"].is_object());
        assert!(schema["properties"]["priority"].is_object());
    }

    /// Module contributing one schema-bearing tool and one bare tool.
    struct Beads;
    impl GtModule for Beads {
        fn meta(&self) -> ModuleMeta {
            ModuleMeta::new(
                ModuleId::new("beads").unwrap(),
                "Beads",
                Version::new(1, 0, 0),
                "Issue tracking.",
            )
        }
        fn register_mcp_tools(&self, reg: &mut McpRegistry) {
            reg.tool_with_schema("beads.create.execute", "Create", schema_for::<CreateBead>())
                .tool("beads.list.execute", "List");
        }
    }

    #[test]
    fn meta_help_lists_tools_in_mcp_shape() {
        let root = RootBuilder::new().module(Beads).build().unwrap();
        let help = meta_help(&root);
        let tools = help["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);

        assert_eq!(tools[0]["name"], "beads.create.execute");
        assert_eq!(tools[0]["description"], "Create");
        assert_eq!(tools[0]["inputSchema"]["type"], "object");

        // Bare tool carries the empty-object schema.
        assert_eq!(tools[1]["name"], "beads.list.execute");
        assert_eq!(tools[1]["inputSchema"], serde_json::json!({}));
    }

    #[test]
    fn meta_help_of_empty_root_is_empty_list() {
        let root = RootBuilder::new().build().unwrap();
        assert_eq!(meta_help(&root), serde_json::json!({ "tools": [] }));
    }
}
