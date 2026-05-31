//! Authorization scope — the `<resource>.<verb>` capability label.
//!
//! Ported from gastown's per-route RBAC convention (`gt-web/src/scope.rs`,
//! `gt-mcp/src/auth.rs`), where a scope is a `&'static str` such as
//! `beads.write` or `sessions.read`. gt-core owns the type so a module can
//! *declare* the scopes it enforces in its [`Capability`](crate::Capability),
//! and the builder can detect two modules claiming the same scope
//! (`hq-mod-core.6`).
//!
//! The wire form stays a plain string (`"beads.write"`) for JWT claim and audit
//! compatibility; validation guarantees exactly one `resource.verb` split with
//! lowercase kebab-case segments.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A `<resource>.<verb>` authorization scope (e.g. `beads.write`).
///
/// Two segments separated by a single `.`; each segment is a lowercase
/// kebab-case slug (`[a-z0-9]` groups joined by single `-`). Owned so it
/// round-trips through the event log and JWT claims; serialized as the bare
/// `"resource.verb"` string.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Scope(String);

/// Reason a [`Scope`] string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeError {
    /// The candidate did not contain exactly one `.` separator.
    NotResourceVerb,
    /// A segment was empty, or held a character outside lowercase kebab-case.
    BadSegment(String),
}

impl fmt::Display for ScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopeError::NotResourceVerb => {
                write!(f, "scope must be exactly `<resource>.<verb>`")
            }
            ScopeError::BadSegment(s) => {
                write!(f, "segment {s:?} is not lowercase kebab-case")
            }
        }
    }
}

impl std::error::Error for ScopeError {}

/// A non-empty lowercase kebab-case slug: `[a-z0-9]+` groups joined by single
/// `-`, no leading/trailing/doubled hyphen. Shared shape with `ModuleId`.
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

impl Scope {
    /// Validate and construct a [`Scope`] from a `resource.verb` string.
    pub fn new(candidate: impl Into<String>) -> Result<Self, ScopeError> {
        let s = candidate.into();
        let mut parts = s.split('.');
        let (Some(resource), Some(verb), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(ScopeError::NotResourceVerb);
        };
        if !is_kebab_segment(resource) {
            return Err(ScopeError::BadSegment(resource.to_string()));
        }
        if !is_kebab_segment(verb) {
            return Err(ScopeError::BadSegment(verb.to_string()));
        }
        Ok(Scope(s))
    }

    /// The full `resource.verb` label.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `resource` segment (before the `.`).
    pub fn resource(&self) -> &str {
        self.0.split('.').next().unwrap_or(&self.0)
    }

    /// The `verb` segment (after the `.`).
    pub fn verb(&self) -> &str {
        self.0.split('.').nth(1).unwrap_or("")
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scope({:?})", self.0)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Scope {
    type Error = ScopeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Scope::new(value)
    }
}

impl From<Scope> for String {
    fn from(value: Scope) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_resource_verb() {
        for s in ["beads.write", "sessions.read", "merge-queue.read", "rig.set-prefix"] {
            assert!(Scope::new(s).is_ok(), "expected {s:?} valid");
        }
    }

    #[test]
    fn rejects_wrong_arity() {
        assert_eq!(Scope::new("beads"), Err(ScopeError::NotResourceVerb));
        assert_eq!(Scope::new("a.b.c"), Err(ScopeError::NotResourceVerb));
        assert_eq!(Scope::new(""), Err(ScopeError::NotResourceVerb));
    }

    #[test]
    fn rejects_bad_segments() {
        assert!(matches!(Scope::new("Beads.write"), Err(ScopeError::BadSegment(_))));
        assert!(matches!(Scope::new("beads.Write"), Err(ScopeError::BadSegment(_))));
        assert!(matches!(Scope::new("beads."), Err(ScopeError::BadSegment(_))));
        assert!(matches!(Scope::new(".write"), Err(ScopeError::BadSegment(_))));
        assert!(matches!(Scope::new("be_ads.write"), Err(ScopeError::BadSegment(_))));
    }

    #[test]
    fn exposes_segments() {
        let s = Scope::new("merge-queue.read").unwrap();
        assert_eq!(s.resource(), "merge-queue");
        assert_eq!(s.verb(), "read");
    }

    #[test]
    fn serde_is_plain_string() {
        let s = Scope::new("beads.write").unwrap();
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"beads.write\"");
        let back: Scope = serde_json::from_str("\"beads.write\"").unwrap();
        assert_eq!(back, s);
        assert!(serde_json::from_str::<Scope>("\"nope\"").is_err());
    }
}
