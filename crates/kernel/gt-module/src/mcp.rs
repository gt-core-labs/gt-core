//! MCP tool contribution — the [`McpRegistry`] a module pushes its tools into.
//!
//! Landed by [`hq-mod-mcp.1`]: the seam by which a [`GtModule`](crate::GtModule)
//! declares the MCP tools it serves, mirroring how routes attach in
//! `hq-mod-routes`. A module implements
//! [`register_mcp_tools`](crate::GtModule::register_mcp_tools) and pushes one
//! [`McpTool`] per tool; the [`RootBuilder`](crate::RootBuilder) collects them
//! eagerly at registration time (alongside `meta`/`capability`) and drops the
//! module value, so dispatch stays static and no `dyn` is retained
//! (non-negotiable #1).
//!
//! ## What this bead does and does not do
//!
//! `.1` defines the contribution type and wires collection into the builder. It
//! deliberately does **not** yet:
//!
//! - enforce the `<module-id>.<action>.<verb>` name convention — `hq-mod-mcp.2`;
//! - carry a JSON input schema or feed `meta.help` — `hq-mod-mcp.3`.
//!
//! The type is `#[non_exhaustive]` and the registry's API is additive so those
//! beads grow it (a schema field, a namespacing check) without breaking a module
//! written against `.1`.

/// A single MCP tool a module contributes.
///
/// Pure data: a name and a one-line description. The name is the fully
/// namespaced tool id a client invokes; its `<module-id>.<action>.<verb>` shape
/// is validated in `hq-mod-mcp.2` and an input schema is attached in `.3`. Cheap
/// to clone and free of handlers, so it is safe to surface in diagnostics and
/// (later) `meta.help`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpTool {
    /// Fully namespaced tool name a client invokes (e.g. `beads.create.execute`).
    pub name: String,
    /// One-line human-readable summary of what the tool does.
    pub description: String,
}

impl McpTool {
    /// Construct a tool descriptor from already-known parts.
    ///
    /// Module authors normally call [`McpRegistry::tool`] instead of building
    /// this directly; the constructor exists so diagnostics and tests can mint a
    /// descriptor, and because the `#[non_exhaustive]` attribute forbids struct
    /// literals outside this crate.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        McpTool {
            name: name.into(),
            description: description.into(),
        }
    }

    /// Validate the name against the `<module-id>.<action>.<verb>` rule and
    /// return its three segments (`hq-mod-mcp.2`).
    ///
    /// A well-formed name is exactly three lowercase kebab-case segments joined
    /// by single `.`s. The first segment is the owning module's id — the
    /// namespace; the [`RootBuilder`](crate::RootBuilder) additionally checks it
    /// matches the module that contributed the tool, so one module cannot squat
    /// another's namespace. This method only judges the *shape*; the
    /// prefix-binding check needs the module id and lives in the builder.
    pub fn parse_name(&self) -> Result<(&str, &str, &str), McpToolNameError> {
        let mut parts = self.name.split('.');
        let (Some(module), Some(action), Some(verb), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(McpToolNameError::NotThreeSegments);
        };
        for seg in [module, action, verb] {
            if !is_kebab_segment(seg) {
                return Err(McpToolNameError::BadSegment(seg.to_string()));
            }
        }
        Ok((module, action, verb))
    }
}

/// Reason an [`McpTool`] name violated the `<module-id>.<action>.<verb>` rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpToolNameError {
    /// The name was not exactly three `.`-separated segments.
    NotThreeSegments,
    /// A segment was empty or held a character outside lowercase kebab-case.
    BadSegment(String),
}

impl std::fmt::Display for McpToolNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            McpToolNameError::NotThreeSegments => {
                write!(f, "tool name must be exactly `<module-id>.<action>.<verb>`")
            }
            McpToolNameError::BadSegment(s) => {
                write!(f, "segment {s:?} is not lowercase kebab-case")
            }
        }
    }
}

impl std::error::Error for McpToolNameError {}

/// A non-empty lowercase kebab-case slug: `[a-z0-9]+` groups joined by single
/// `-`, no leading/trailing/doubled hyphen. Shared shape with [`ModuleId`] and
/// [`Scope`] segments (each crate carries its own copy to stay self-contained).
///
/// [`ModuleId`]: crate::ModuleId
/// [`Scope`]: crate::Scope
fn is_kebab_segment(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return false;
    }
    let mut prev_dash = false;
    for c in s.chars() {
        match c {
            'a'..='z' | '0'..='9' => prev_dash = false,
            '-' => {
                if prev_dash {
                    return false;
                }
                prev_dash = true;
            }
            _ => return false,
        }
    }
    true
}

/// Accumulates the MCP tools a single module contributes.
///
/// A fresh registry is handed to each module's
/// [`register_mcp_tools`](crate::GtModule::register_mcp_tools); the module pushes
/// its tools through the chainable [`tool`](McpRegistry::tool) method and the
/// builder harvests the result. Holds plain data only — no runtime handles — so
/// it is cheap and carries nothing across an `await`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpRegistry {
    tools: Vec<McpTool>,
}

impl McpRegistry {
    /// Start an empty registry.
    pub fn new() -> Self {
        McpRegistry::default()
    }

    /// Declare one MCP tool by name and description. Chainable.
    ///
    /// Pushes the tool in declaration order. The name is taken verbatim here;
    /// the `<module-id>.<action>.<verb>` convention is enforced later
    /// (`hq-mod-mcp.2`), so this method never rejects.
    pub fn tool(&mut self, name: impl Into<String>, description: impl Into<String>) -> &mut Self {
        self.tools.push(McpTool::new(name, description));
        self
    }

    /// The tools declared so far, in declaration order.
    pub fn tools(&self) -> &[McpTool] {
        &self.tools
    }

    /// Number of tools declared.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether no tools have been declared.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Consume the registry, yielding the collected tools. Used by the builder to
    /// move a module's tools into its [`ModuleEntry`](crate::RootBuilder) snapshot.
    pub(crate) fn into_tools(self) -> Vec<McpTool> {
        self.tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_has_no_tools() {
        let reg = McpRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        assert!(reg.tools().is_empty());
    }

    #[test]
    fn tool_pushes_in_declaration_order() {
        let mut reg = McpRegistry::new();
        reg.tool("beads.create.execute", "Create a bead")
            .tool("beads.close.execute", "Close a bead");
        assert_eq!(reg.len(), 2);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["beads.create.execute", "beads.close.execute"]);
    }

    #[test]
    fn tool_records_name_and_description() {
        let mut reg = McpRegistry::new();
        reg.tool("rigs.add.execute", "Register a rig");
        let t = &reg.tools()[0];
        assert_eq!(t.name, "rigs.add.execute");
        assert_eq!(t.description, "Register a rig");
    }

    #[test]
    fn into_tools_preserves_order() {
        let mut reg = McpRegistry::new();
        reg.tool("a.read.execute", "a").tool("b.read.execute", "b");
        let tools = reg.into_tools();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["a.read.execute", "b.read.execute"]);
    }

    #[test]
    fn parse_name_accepts_three_kebab_segments() {
        for n in [
            "beads.create.execute",
            "gt-rig.set-prefix.execute",
            "merge.submit.validate",
        ] {
            assert!(McpTool::new(n, "").parse_name().is_ok(), "expected {n:?} valid");
        }
    }

    #[test]
    fn parse_name_returns_segments() {
        let tool = McpTool::new("gt-rig.set-prefix.execute", "");
        let (m, a, v) = tool.parse_name().unwrap();
        assert_eq!((m, a, v), ("gt-rig", "set-prefix", "execute"));
    }

    #[test]
    fn parse_name_rejects_wrong_arity() {
        use McpToolNameError::NotThreeSegments;
        for n in ["beads", "beads.create", "beads.create.execute.extra", ""] {
            assert_eq!(McpTool::new(n, "").parse_name(), Err(NotThreeSegments), "name {n:?}");
        }
    }

    #[test]
    fn parse_name_rejects_bad_segments() {
        for n in ["Beads.create.execute", "beads.Create.execute", "beads..execute", "beads.create.-x"] {
            assert!(
                matches!(McpTool::new(n, "").parse_name(), Err(McpToolNameError::BadSegment(_))),
                "expected {n:?} bad-segment"
            );
        }
    }
}
