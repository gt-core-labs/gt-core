//! [`WorkspaceId`] — the tenant-boundary identifier.
//!
//! Landed by `hq-mt-core.1`. Every multi-tenant aggregate keys on a
//! `WorkspaceId`; it is the slug a tenant is addressed by in URLs
//! (`/w/<workspace-id>/...`), subdomains, JWT claims, and the per-workspace SSE
//! channel key (see `docs/02-sse-pattern.md`). Because it leaks into so many
//! wire contracts it is validated as a DNS-label-shaped slug and is immutable
//! for the life of the workspace.
//!
//! Shape is intentionally the same lowercase-kebab grammar as
//! [`ModuleId`](../../../gt-module) — one or more `[a-z0-9]+` groups joined by
//! single `-`, no leading/trailing/doubled hyphen — with an added 63-character
//! ceiling so a workspace id is always a legal DNS label.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum length of a workspace id, in bytes/ASCII chars.
///
/// 63 is the DNS label limit, so a workspace id can always be used as a
/// subdomain (`<workspace-id>.app.example.com`) without truncation.
pub const MAX_WORKSPACE_ID_LEN: usize = 63;

/// Stable, unique, tenant-boundary identifier for a workspace.
///
/// A lowercase kebab-case slug of 1..=63 chars. Owned so it round-trips through
/// the event log, JWT claims, and the API; serialized as the bare `"slug"`
/// string and validated on the way in.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "String", into = "String")]
pub struct WorkspaceId(String);

/// Reason a [`WorkspaceId`] string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceIdError {
    /// The candidate was empty.
    Empty,
    /// The candidate exceeded [`MAX_WORKSPACE_ID_LEN`]; holds the actual length.
    TooLong(usize),
    /// The candidate contained a character outside `[a-z0-9-]`.
    IllegalChar(char),
    /// A `-` appeared at the start, at the end, or doubled.
    MalformedSeparator,
}

impl fmt::Display for WorkspaceIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkspaceIdError::Empty => write!(f, "workspace id is empty"),
            WorkspaceIdError::TooLong(n) => {
                write!(f, "workspace id is {n} chars (max {MAX_WORKSPACE_ID_LEN})")
            }
            WorkspaceIdError::IllegalChar(c) => {
                write!(f, "illegal character {c:?} (expected lowercase kebab-case)")
            }
            WorkspaceIdError::MalformedSeparator => {
                write!(f, "'-' may not lead, trail, or repeat")
            }
        }
    }
}

impl std::error::Error for WorkspaceIdError {}

impl WorkspaceId {
    /// Validate and construct a [`WorkspaceId`].
    ///
    /// Accepts lowercase kebab-case slugs of 1..=63 chars: one or more groups of
    /// `[a-z0-9]+` joined by single `-`. Rejects empty input, anything over the
    /// length cap, uppercase, underscores, spaces, and leading/trailing/doubled
    /// hyphens.
    pub fn new(candidate: impl Into<String>) -> Result<Self, WorkspaceIdError> {
        let s = candidate.into();
        if s.is_empty() {
            return Err(WorkspaceIdError::Empty);
        }
        if s.len() > MAX_WORKSPACE_ID_LEN {
            return Err(WorkspaceIdError::TooLong(s.len()));
        }
        let bytes = s.as_bytes();
        if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
            return Err(WorkspaceIdError::MalformedSeparator);
        }
        let mut prev_dash = false;
        for c in s.chars() {
            match c {
                'a'..='z' | '0'..='9' => prev_dash = false,
                '-' => {
                    if prev_dash {
                        return Err(WorkspaceIdError::MalformedSeparator);
                    }
                    prev_dash = true;
                }
                other => return Err(WorkspaceIdError::IllegalChar(other)),
            }
        }
        Ok(WorkspaceId(s))
    }

    /// Borrow the underlying slug.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WorkspaceId({:?})", self.0)
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for WorkspaceId {
    type Error = WorkspaceIdError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        WorkspaceId::new(value)
    }
}

impl From<WorkspaceId> for String {
    fn from(value: WorkspaceId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_slugs() {
        for s in ["acme", "gastown", "a", "team-1", "x1-y2-z3"] {
            assert!(WorkspaceId::new(s).is_ok(), "expected {s:?} valid");
        }
    }

    #[test]
    fn accepts_max_length() {
        let s = "a".repeat(MAX_WORKSPACE_ID_LEN);
        assert!(WorkspaceId::new(&s).is_ok());
    }

    #[test]
    fn rejects_invalid_slugs() {
        use WorkspaceIdError::*;
        assert_eq!(WorkspaceId::new(""), Err(Empty));
        assert_eq!(WorkspaceId::new("-lead"), Err(MalformedSeparator));
        assert_eq!(WorkspaceId::new("trail-"), Err(MalformedSeparator));
        assert_eq!(WorkspaceId::new("double--dash"), Err(MalformedSeparator));
        assert_eq!(WorkspaceId::new("Upper"), Err(IllegalChar('U')));
        assert_eq!(WorkspaceId::new("under_score"), Err(IllegalChar('_')));
        assert_eq!(WorkspaceId::new("white space"), Err(IllegalChar(' ')));
    }

    #[test]
    fn rejects_over_length() {
        let s = "a".repeat(MAX_WORKSPACE_ID_LEN + 1);
        assert_eq!(
            WorkspaceId::new(&s),
            Err(WorkspaceIdError::TooLong(MAX_WORKSPACE_ID_LEN + 1))
        );
    }

    #[test]
    fn display_roundtrips_slug() {
        let id = WorkspaceId::new("gastown").unwrap();
        assert_eq!(id.to_string(), "gastown");
        assert_eq!(id.as_str(), "gastown");
    }

    #[test]
    fn serde_roundtrip_uses_plain_string() {
        let id = WorkspaceId::new("acme").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"acme\"");
        let back: WorkspaceId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn serde_rejects_invalid_on_deserialize() {
        assert!(serde_json::from_str::<WorkspaceId>("\"Bad_Id\"").is_err());
    }

    #[test]
    fn json_schema_is_a_string() {
        // The newtype must present to API/MCP consumers as a plain string.
        let schema = schemars::schema_for!(WorkspaceId);
        let json = serde_json::to_value(&schema).unwrap();
        assert_eq!(json["type"], "string");
    }
}
