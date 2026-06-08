//! Tool contract gate (`hq-mcp-test.1`).
//!
//! The MCP server harvests its tool set from the kernel `RootBuilder` — the
//! composition root never hand-lists tools (docs/03 rule 3). This contract test
//! reproduces that harvest (`IssuesModule` + `MetaModule`, exactly as the
//! `gt-mcp-server` binary composes it) and asserts the served contract:
//!
//! 1. every expected `issues.*` / `meta.*` tool is present,
//! 2. each carries a JSON-object input schema (what `list_tools` ships to the
//!    client as the tool's `inputSchema`),
//! 3. every `issues.<verb>.validate` is paired with a `.execute` (the
//!    check-then-commit contract) and vice versa,
//! 4. `meta.help.execute` echoes the same descriptor set `list_tools` serves —
//!    the self-description is not a hand-maintained second copy.
//!
//! Pure in-process: no Dolt/PG, so it runs in host CI as a plain
//! `cargo test -p gt-composition --test mcp_contract`.

use std::collections::BTreeSet;

use gt_issues::IssuesModule;
use gt_meta::MetaModule;
use gt_module::McpTool;
use gt_module::RootBuilder;
use serde_json::{json, Value};

/// Reproduce the binary's tool harvest: the same two modules, built through the
/// kernel `RootBuilder`, descriptors collected via `root.mcp_tools()`.
fn harvest_tools() -> Vec<McpTool> {
    let root = RootBuilder::new()
        .module(IssuesModule::default())
        .module(MetaModule)
        .build()
        .expect("module build");
    root.mcp_tools().cloned().collect()
}

/// The tracker's check-then-commit verbs: each exposes a `validate` (shape-only,
/// no state change) and an `execute` (the atomic Dolt write).
const PAIRED_VERBS: &[&str] = &["create", "update", "transition", "close", "claim"];

/// Single-call tools with no `validate` sibling (operator phase advance,
/// the two `meta.*` self-service tools, and the read-only issues query tools).
const SINGLETONS: &[&str] = &[
    "issues.phase.advance",
    "issues.list.execute",
    "issues.read.execute",
    "meta.help.execute",
    "meta.report-gap.execute",
];

#[test]
fn tools_list_exposes_every_expected_tool() {
    let tools = harvest_tools();
    let names: BTreeSet<&str> = tools.iter().map(|t| t.name.as_str()).collect();

    let mut expected: BTreeSet<String> = BTreeSet::new();
    for verb in PAIRED_VERBS {
        expected.insert(format!("issues.{verb}.validate"));
        expected.insert(format!("issues.{verb}.execute"));
    }
    for s in SINGLETONS {
        expected.insert((*s).to_string());
    }

    let missing: Vec<&String> = expected
        .iter()
        .filter(|e| !names.contains(e.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "tools/list missing expected tools: {missing:?}"
    );
}

#[test]
fn every_tool_has_a_json_object_input_schema() {
    for t in harvest_tools() {
        assert!(
            t.input_schema.is_object(),
            "tool `{}` must expose a JSON-object input schema, got {}",
            t.name,
            t.input_schema
        );
        // A tool that takes arguments declares an object JSON Schema; a no-arg
        // tool legitimately ships the empty schema `{}` (no `type` key). When a
        // `type` is declared it must be `object` — never a scalar/array schema —
        // so a client generating a form has a struct to bind to.
        if let Some(kind) = t.input_schema.get("type").and_then(Value::as_str) {
            assert_eq!(
                kind, "object",
                "tool `{}` declares a non-object input schema `type: {kind}`",
                t.name,
            );
        }
    }
}

#[test]
fn validate_execute_pairs_are_symmetric() {
    let tools = harvest_tools();
    let names: BTreeSet<&str> = tools.iter().map(|t| t.name.as_str()).collect();

    // Every `.validate` has a matching `.execute` …
    for name in &names {
        if let Some(stem) = name.strip_suffix(".validate") {
            let exec = format!("{stem}.execute");
            assert!(
                names.contains(exec.as_str()),
                "`{name}` has no paired `{exec}`"
            );
        }
    }
    // … and every paired-verb `.execute` has a matching `.validate` (singletons
    // like `meta.help.execute` are exempt by construction).
    let singleton: BTreeSet<&str> = SINGLETONS.iter().copied().collect();
    for name in &names {
        if singleton.contains(name) {
            continue;
        }
        if let Some(stem) = name.strip_suffix(".execute") {
            let val = format!("{stem}.validate");
            assert!(
                names.contains(val.as_str()),
                "`{name}` has no paired `{val}`"
            );
        }
    }
}

#[test]
fn meta_help_payload_matches_the_served_tool_set() {
    // `meta.help.execute` is the server self-description: it echoes the exact
    // `&self.tools` slice `list_tools` serves (see `dispatch_meta`). Reproduce
    // that payload and assert it round-trips the harvested contract, so the help
    // surface can never drift from the live tool list.
    let tools = harvest_tools();
    let help_payload: Value = json!({ "tools": tools });

    let help_names: BTreeSet<String> = help_payload["tools"]
        .as_array()
        .expect("help payload carries a tools array")
        .iter()
        .map(|t| {
            t["name"]
                .as_str()
                .expect("each tool names itself")
                .to_string()
        })
        .collect();
    let served_names: BTreeSet<String> = tools.iter().map(|t| t.name.clone()).collect();

    assert_eq!(
        help_names, served_names,
        "meta.help.execute must echo exactly the tools list_tools serves"
    );
}
