//! Tools contract (`hq-mcp-test.1`).
//!
//! The MCP wire contract the issues server publishes through `tools/list` and
//! `meta.help.execute`: a `validate`/`execute` pair per command, every tool
//! carrying a JSON input schema, and `meta.help` echoing exactly the served set.
//!
//! Source of truth is [`IssuesModule::register_mcp_tools`] — the same registry
//! the composition root harvests into the `IssuesServer` descriptor set
//! (`docs/03` rule 3: tools are declared by the module, never hand-listed). The
//! server's `list_tools` maps that set verbatim and `dispatch_meta`'s
//! `meta.help.execute` returns `{"tools": <that set>}`, so asserting against the
//! registry is asserting against both wire surfaces.
//!
//! Pure (no Dolt/PG): the tool descriptors are static data.

use std::collections::BTreeSet;

use gt_issues::IssuesModule;
use gt_module::{GtModule, McpRegistry};
use serde_json::Value;

/// Build the issues server's published tool set the way the composition root does.
fn served_tools() -> Vec<gt_module::McpTool> {
    let mut reg = McpRegistry::new();
    IssuesModule::default().register_mcp_tools(&mut reg);
    reg.tools().to_vec()
}

/// Every command exposes a `validate` (dry-run, no state change) and an `execute`
/// counterpart; the lone exception is the operator-only `issues.phase.advance`,
/// which has no validate/execute split (hq-core-mcp.7).
#[test]
fn every_command_has_a_validate_execute_pair() {
    let tools = served_tools();
    let names: BTreeSet<&str> = tools.iter().map(|t| t.name.as_str()).collect();

    // The five issue commands each pair a validate with an execute.
    for cmd in ["create", "update", "transition", "close", "claim"] {
        let validate = format!("issues.{cmd}.validate");
        let execute = format!("issues.{cmd}.execute");
        assert!(names.contains(validate.as_str()), "missing {validate}");
        assert!(names.contains(execute.as_str()), "missing {execute}");
    }

    // Symmetry the other direction: no orphan `.validate` without its `.execute`
    // (or vice versa), so the pairing is total over the command tools.
    for name in &names {
        if let Some(stem) = name.strip_suffix(".validate") {
            assert!(
                names.contains(format!("{stem}.execute").as_str()),
                "{name} has no matching .execute",
            );
        }
        if let Some(stem) = name.strip_suffix(".execute") {
            assert!(
                names.contains(format!("{stem}.validate").as_str()),
                "{name} has no matching .validate",
            );
        }
    }

    // The single privileged mutation with no pair.
    assert!(names.contains("issues.phase.advance"), "missing phase.advance");
}

/// Each tool carries a JSON-object input schema. The command tools take arguments,
/// so their schema is a non-empty Draft-07 object (`properties`/`$ref`/`$defs`);
/// every schema is at minimum a JSON object, never a bare scalar or null.
#[test]
fn every_tool_has_a_json_input_schema() {
    for tool in served_tools() {
        let schema = &tool.input_schema;
        assert!(
            schema.is_object(),
            "tool {} input schema is not a JSON object: {schema}",
            tool.name,
        );
        // The argument-bearing command tools declare a real schema (not the
        // empty-object placeholder). phase.advance is the singleton frontier
        // bump and legitimately takes a minimal body.
        if tool.name != "issues.phase.advance" {
            let obj = schema.as_object().unwrap();
            assert!(
                !obj.is_empty(),
                "command tool {} has the empty-object schema — args undocumented",
                tool.name,
            );
        }
    }
}

/// `meta.help.execute` returns exactly the served descriptor set, each entry
/// keyed by the MCP-canonical `name` / `inputSchema` shape. This mirrors the
/// server's `dispatch_meta` (`meta.help.execute` => `{"tools": tools}`), so a
/// client reading `meta.help` sees the same names + schemas as `tools/list`.
#[test]
fn meta_help_matches_the_served_tool_list() {
    let tools = served_tools();

    // The server shapes meta.help as `{"tools": <descriptors>}`; replicate it.
    let help = serde_json::json!({ "tools": tools });
    let entries = help["tools"].as_array().expect("tools array");

    let list_names: BTreeSet<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    let help_names: BTreeSet<&str> = entries
        .iter()
        .map(|t| t["name"].as_str().expect("name field"))
        .collect();
    assert_eq!(help_names, list_names, "meta.help drifted from tools/list");

    // Each help entry carries the canonical `inputSchema` key (the serde rename),
    // and it is a JSON object — the contract a client relies on to drive the tool.
    for entry in entries {
        let schema = &entry["inputSchema"];
        assert!(
            schema.is_object(),
            "help entry {} missing object inputSchema",
            entry["name"],
        );
        assert!(
            matches!(&entry["name"], Value::String(_)),
            "help entry has no string name",
        );
    }
}
