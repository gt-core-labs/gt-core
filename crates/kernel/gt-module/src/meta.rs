//! Module identity + metadata types.
//!
//! Landed by [`hq-mod-core.2`]. These are the descriptive facts a module
//! advertises about itself; behavioral contributions (routes, MCP tools,
//! events, migrations, hooks) are declared via [`Capability`](crate::Capability)
//! in later beads and wired by the `RootBuilder` (`hq-mod-core.4`).

use std::fmt;

use semver::Version;
use serde::{Deserialize, Serialize};

/// Stable, unique identifier for a module.
///
/// Used as the namespace prefix for the module's MCP tools
/// (`<module-id>.action.verb`), event kinds (`<module-id>.<noun>.vN`), HTTP
/// route prefix (`/api/v1/<module-id>`), and migration directory. Because the
/// id leaks into so many wire contracts, it is validated as a lowercase
/// kebab-case slug and is immutable for the life of the module.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ModuleId(String);

/// Reason a [`ModuleId`] string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModuleIdError {
    /// The candidate was empty.
    Empty,
    /// The candidate contained a character outside `[a-z0-9-]`.
    IllegalChar(char),
    /// A `-` appeared at the start, at the end, or doubled.
    MalformedSeparator,
}

impl fmt::Display for ModuleIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModuleIdError::Empty => write!(f, "module id is empty"),
            ModuleIdError::IllegalChar(c) => {
                write!(f, "illegal character {c:?} (expected lowercase kebab-case)")
            }
            ModuleIdError::MalformedSeparator => {
                write!(f, "'-' may not lead, trail, or repeat")
            }
        }
    }
}

impl std::error::Error for ModuleIdError {}

impl ModuleId {
    /// Validate and construct a [`ModuleId`].
    ///
    /// Accepts lowercase kebab-case slugs: one or more groups of `[a-z0-9]+`
    /// joined by single `-`. Rejects empty input, uppercase, underscores,
    /// leading/trailing/doubled hyphens.
    pub fn new(candidate: impl Into<String>) -> Result<Self, ModuleIdError> {
        let s = candidate.into();
        if s.is_empty() {
            return Err(ModuleIdError::Empty);
        }
        let bytes = s.as_bytes();
        if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
            return Err(ModuleIdError::MalformedSeparator);
        }
        let mut prev_dash = false;
        for c in s.chars() {
            match c {
                'a'..='z' | '0'..='9' => prev_dash = false,
                '-' => {
                    if prev_dash {
                        return Err(ModuleIdError::MalformedSeparator);
                    }
                    prev_dash = true;
                }
                other => return Err(ModuleIdError::IllegalChar(other)),
            }
        }
        Ok(ModuleId(s))
    }

    /// Borrow the underlying slug.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ModuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ModuleId({:?})", self.0)
    }
}

impl fmt::Display for ModuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ModuleId {
    type Error = ModuleIdError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        ModuleId::new(value)
    }
}

impl From<ModuleId> for String {
    fn from(value: ModuleId) -> Self {
        value.0
    }
}

/// Descriptive facts a module advertises about itself.
///
/// Returned by [`GtModule::meta`](crate::GtModule::meta). Pure data: holds no
/// handlers and performs no side effects, so it is cheap to clone and safe to
/// surface in diagnostics, `meta.help`, and admin UIs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleMeta {
    /// Stable namespace id (see [`ModuleId`]).
    pub id: ModuleId,
    /// Human-readable display name (free-form, may change between releases).
    pub name: String,
    /// Module version. Drives contract semver enforcement (`hq-mod-contracts`).
    pub version: Version,
    /// One-line summary shown in module listings and `meta.help`.
    pub description: String,
    /// Ids this module directly depends on, as declared via
    /// [`GtModule::dependencies`](crate::GtModule::dependencies). Empty for a
    /// standalone module. Stamped by [`RootBuilder`](crate::RootBuilder) at
    /// registration — module authors declare deps once in `dependencies()`, not
    /// here — so a built [`Root`](crate::Root) can surface the dependency graph at
    /// runtime (e.g. for `feature.disable` dep-safety; see
    /// [`Root::dependency_edges`](crate::Root::dependency_edges)).
    pub depends_on: Vec<ModuleId>,
}

impl ModuleMeta {
    /// Construct metadata from already-validated parts. `depends_on` starts empty;
    /// the [`RootBuilder`](crate::RootBuilder) stamps the module's declared
    /// dependencies on registration, so module `meta()` impls never repeat them.
    pub fn new(
        id: ModuleId,
        name: impl Into<String>,
        version: Version,
        description: impl Into<String>,
    ) -> Self {
        ModuleMeta {
            id,
            name: name.into(),
            version,
            description: description.into(),
            depends_on: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_slugs() {
        for s in ["beads", "gt-rig", "kanban", "a", "mod-3", "x1-y2-z3"] {
            assert!(ModuleId::new(s).is_ok(), "expected {s:?} to be valid");
        }
    }

    #[test]
    fn rejects_invalid_slugs() {
        use ModuleIdError::*;
        assert_eq!(ModuleId::new(""), Err(Empty));
        assert_eq!(ModuleId::new("-lead"), Err(MalformedSeparator));
        assert_eq!(ModuleId::new("trail-"), Err(MalformedSeparator));
        assert_eq!(ModuleId::new("double--dash"), Err(MalformedSeparator));
        assert_eq!(ModuleId::new("Upper"), Err(IllegalChar('U')));
        assert_eq!(ModuleId::new("under_score"), Err(IllegalChar('_')));
        assert_eq!(ModuleId::new("white space"), Err(IllegalChar(' ')));
    }

    #[test]
    fn display_roundtrips_slug() {
        let id = ModuleId::new("gt-rig").unwrap();
        assert_eq!(id.to_string(), "gt-rig");
        assert_eq!(id.as_str(), "gt-rig");
    }

    #[test]
    fn serde_roundtrip_uses_plain_string() {
        let id = ModuleId::new("kanban").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"kanban\"");
        let back: ModuleId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn serde_rejects_invalid_on_deserialize() {
        let err = serde_json::from_str::<ModuleId>("\"Bad_Id\"");
        assert!(err.is_err());
    }
}
