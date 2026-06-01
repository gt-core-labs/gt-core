//! Gate test (hq-core-host.2 acceptance): a `Root` built with [`IssuesModule`]
//! exposes the ten `issues.*` tools in its `McpRegistry`, and `meta.help` lists
//! them in the MCP `tools/list` shape.

use gt_issues::IssuesModule;
use gt_module::RootBuilder;
use gt_module_mcp::meta_help;

const EXPECTED: [&str; 10] = [
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
];

#[test]
fn root_exposes_the_issues_tools() {
    let root = RootBuilder::new().module(IssuesModule).build().unwrap();
    let names: Vec<&str> = root.mcp_tools().map(|t| t.name.as_str()).collect();
    assert_eq!(names, EXPECTED);
}

#[test]
fn meta_help_lists_the_issues_tools_in_mcp_shape() {
    let root = RootBuilder::new().module(IssuesModule).build().unwrap();
    let help = meta_help(&root);
    let tools = help["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), EXPECTED.len());

    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, EXPECTED);

    // Each entry carries the MCP-canonical { name, description, inputSchema }.
    for t in tools {
        assert!(t["name"].is_string());
        assert!(t["description"].is_string());
        assert_eq!(t["inputSchema"]["type"], "object");
    }
}
