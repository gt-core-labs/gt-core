//! Event kind — the versioned `<module>.<noun>.v<N>` identifier.
//!
//! Enforces non-negotiable #5 / guardrail Rule 5: every event kind is namespaced
//! by its owning module and carries an explicit version suffix so v1 and v2 can
//! coexist in an append-only log. A module declares the kinds it
//! [`emits`](crate::Capability::emits) and the kinds it
//! [`subscribes`](crate::Capability::subscribes) to in its
//! [`Capability`](crate::Capability); the full versioning machinery (parser,
//! deprecation, cross-module subscribe) is the `hq-mod-events` epic — this type
//! is the shared vocabulary both rely on.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A versioned event kind, e.g. `bead.created.v1`.
///
/// Shape: `<module>.<noun>.v<N>` where `<module>` and `<noun>` are lowercase
/// kebab-case slugs and `<N>` is a positive integer. The noun itself may contain
/// dots (`card.moved`), so parsing splits the version suffix off the end first,
/// then the module prefix off the front, leaving the noun in the middle.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventKind(String);

/// Reason an [`EventKind`] string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKindError {
    /// Missing the trailing `.v<N>` version segment.
    MissingVersion,
    /// Fewer than three segments (`module.noun.vN`).
    TooFewSegments,
    /// A non-version segment was not lowercase kebab-case.
    BadSegment(String),
}

impl fmt::Display for EventKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventKindError::MissingVersion => {
                write!(f, "event kind must end with `.v<N>` (e.g. `.v1`)")
            }
            EventKindError::TooFewSegments => {
                write!(f, "event kind must be `<module>.<noun>.v<N>`")
            }
            EventKindError::BadSegment(s) => {
                write!(f, "segment {s:?} is not lowercase kebab-case")
            }
        }
    }
}

impl std::error::Error for EventKindError {}

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

/// Parse the `v<N>` suffix into its numeric version. Returns `None` for anything
/// that is not `v` followed by a run of ASCII digits encoding a positive number.
fn parse_version(seg: &str) -> Option<u32> {
    let digits = seg.strip_prefix('v')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok().filter(|&n| n >= 1)
}

impl EventKind {
    /// Validate and construct an [`EventKind`].
    pub fn new(candidate: impl Into<String>) -> Result<Self, EventKindError> {
        let s = candidate.into();
        let segments: Vec<&str> = s.split('.').collect();
        if segments.len() < 3 {
            return Err(EventKindError::TooFewSegments);
        }
        let (version_seg, head) = segments.split_last().expect("len >= 3");
        if parse_version(version_seg).is_none() {
            return Err(EventKindError::MissingVersion);
        }
        for seg in head {
            if !is_kebab_segment(seg) {
                return Err(EventKindError::BadSegment(seg.to_string()));
            }
        }
        Ok(EventKind(s))
    }

    /// The full `module.noun.vN` string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The owning module prefix (first segment).
    pub fn module(&self) -> &str {
        self.0.split('.').next().unwrap_or(&self.0)
    }

    /// The numeric version from the `.vN` suffix.
    pub fn version(&self) -> u32 {
        self.0
            .rsplit('.')
            .next()
            .and_then(parse_version)
            .unwrap_or(0)
    }

    /// The noun between the module prefix and the version suffix
    /// (`bead.card.moved.v2` → `card.moved`).
    pub fn noun(&self) -> &str {
        let after_module = self.0.split_once('.').map(|(_, rest)| rest).unwrap_or("");
        after_module
            .rsplit_once('.')
            .map(|(noun, _ver)| noun)
            .unwrap_or(after_module)
    }
}

impl fmt::Debug for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EventKind({:?})", self.0)
    }
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for EventKind {
    type Error = EventKindError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        EventKind::new(value)
    }
}

impl From<EventKind> for String {
    fn from(value: EventKind) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_versioned_kinds() {
        for s in ["bead.created.v1", "rig.prefix-changed.v1", "kanban.card.moved.v2"] {
            assert!(EventKind::new(s).is_ok(), "expected {s:?} valid");
        }
    }

    #[test]
    fn rejects_unversioned() {
        // Three+ segments where the trailing one is not a valid `v<N>`.
        assert_eq!(EventKind::new("bead.created.x1"), Err(EventKindError::MissingVersion));
        assert_eq!(EventKind::new("bead.created.v0"), Err(EventKindError::MissingVersion));
        assert_eq!(EventKind::new("bead.created.v"), Err(EventKindError::MissingVersion));
        assert_eq!(EventKind::new("kanban.card.moved"), Err(EventKindError::MissingVersion));
    }

    #[test]
    fn rejects_too_few_segments() {
        // Fewer than three dot-separated segments, regardless of a `.vN` tail.
        assert_eq!(EventKind::new("bead.created"), Err(EventKindError::TooFewSegments));
        assert_eq!(EventKind::new("bead.v1"), Err(EventKindError::TooFewSegments));
        assert_eq!(EventKind::new("v1"), Err(EventKindError::TooFewSegments));
    }

    #[test]
    fn rejects_bad_segment() {
        assert!(matches!(EventKind::new("Bead.created.v1"), Err(EventKindError::BadSegment(_))));
        assert!(matches!(EventKind::new("bead.Created.v1"), Err(EventKindError::BadSegment(_))));
    }

    #[test]
    fn exposes_parts_simple() {
        let k = EventKind::new("bead.created.v1").unwrap();
        assert_eq!(k.module(), "bead");
        assert_eq!(k.noun(), "created");
        assert_eq!(k.version(), 1);
    }

    #[test]
    fn exposes_parts_dotted_noun() {
        let k = EventKind::new("kanban.card.moved.v2").unwrap();
        assert_eq!(k.module(), "kanban");
        assert_eq!(k.noun(), "card.moved");
        assert_eq!(k.version(), 2);
    }

    #[test]
    fn serde_is_plain_string() {
        let k = EventKind::new("bead.created.v1").unwrap();
        assert_eq!(serde_json::to_string(&k).unwrap(), "\"bead.created.v1\"");
        let back: EventKind = serde_json::from_str("\"bead.created.v1\"").unwrap();
        assert_eq!(back, k);
        assert!(serde_json::from_str::<EventKind>("\"bead.created\"").is_err());
    }
}
