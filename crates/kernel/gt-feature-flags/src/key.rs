//! [`FlagKey`] — the dotted-kebab identity of a feature flag.
//!
//! A flag key namespaces a toggle: `module.<module-id>` enables/disables a whole
//! module, `feature.<name>` a finer capability. The grammar is the same narrow
//! kebab-case used elsewhere in the kernel (see `ModuleId`), extended with `.`
//! separators so keys form a stable, sortable namespace that is safe to store as
//! a Postgres key and surface on the wire without escaping.

use std::fmt;

/// Validated dotted-kebab identity of a feature flag, e.g. `module.beads`.
///
/// Invariants (checked in [`FlagKey::new`] / on deserialize):
/// - non-empty, ≤ 128 bytes
/// - one or more `.`-separated segments
/// - each segment is `[a-z0-9]` with single internal hyphens, no leading /
///   trailing / doubled hyphen
/// - no leading / trailing / doubled `.`
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FlagKey(String);

/// Reason a [`FlagKey`] string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagKeyError {
    /// The candidate was empty.
    Empty,
    /// The candidate exceeded the byte limit.
    TooLong(usize),
    /// A character outside `[a-z0-9.-]` appeared.
    IllegalChar(char),
    /// A `-` led, trailed, or doubled within a segment.
    MalformedHyphen,
    /// A `.` led, trailed, doubled, or a segment was empty.
    MalformedSegment,
}

impl fmt::Display for FlagKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlagKeyError::Empty => write!(f, "flag key is empty"),
            FlagKeyError::TooLong(n) => write!(f, "flag key too long: {n} bytes (max 128)"),
            FlagKeyError::IllegalChar(c) => {
                write!(f, "illegal character {c:?} (expected lowercase dotted-kebab)")
            }
            FlagKeyError::MalformedHyphen => {
                write!(f, "'-' may not lead, trail, or repeat in a segment")
            }
            FlagKeyError::MalformedSegment => {
                write!(f, "'.' may not lead, trail, or repeat; segments must be non-empty")
            }
        }
    }
}

impl std::error::Error for FlagKeyError {}

/// Max key length. Generous enough for `module.<id>` / `feature.<name>` while
/// staying well under any storage identifier limit.
const MAX_KEY_LEN: usize = 128;

impl FlagKey {
    /// Validate and construct a [`FlagKey`].
    pub fn new(candidate: impl Into<String>) -> Result<Self, FlagKeyError> {
        let s = candidate.into();
        if s.is_empty() {
            return Err(FlagKeyError::Empty);
        }
        if s.len() > MAX_KEY_LEN {
            return Err(FlagKeyError::TooLong(s.len()));
        }
        // Reject any char outside the alphabet up front for a precise error.
        for c in s.chars() {
            if !matches!(c, 'a'..='z' | '0'..='9' | '-' | '.') {
                return Err(FlagKeyError::IllegalChar(c));
            }
        }
        // Split on '.', validate each segment is a well-formed kebab token.
        // `split` yields an empty string for leading/trailing/doubled dots,
        // which `validate_segment` rejects as MalformedSegment.
        for segment in s.split('.') {
            validate_segment(segment)?;
        }
        Ok(FlagKey(s))
    }

    /// Borrow the underlying key.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterate the dotted segments (`module.beads` → `["module", "beads"]`).
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

/// Validate one dotted segment as a kebab token: `[a-z0-9]+(-[a-z0-9]+)*`.
fn validate_segment(seg: &str) -> Result<(), FlagKeyError> {
    if seg.is_empty() {
        return Err(FlagKeyError::MalformedSegment);
    }
    let bytes = seg.as_bytes();
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(FlagKeyError::MalformedHyphen);
    }
    let mut prev_hyphen = false;
    for &b in bytes {
        match b {
            b'a'..=b'z' | b'0'..=b'9' => prev_hyphen = false,
            b'-' => {
                if prev_hyphen {
                    return Err(FlagKeyError::MalformedHyphen);
                }
                prev_hyphen = true;
            }
            // Non-alphabet chars are already filtered in `new`; a '.' here would
            // mean an empty segment, handled by the split. Anything else is a bug.
            _ => return Err(FlagKeyError::MalformedSegment),
        }
    }
    Ok(())
}

impl fmt::Debug for FlagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlagKey({:?})", self.0)
    }
}

impl fmt::Display for FlagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for FlagKey {
    type Error = FlagKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        FlagKey::new(value)
    }
}

impl From<FlagKey> for String {
    fn from(value: FlagKey) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_keys() {
        for s in [
            "module.beads",
            "feature.dark-mode",
            "a",
            "x.y.z",
            "module.gt-rig",
            "feature.x1-y2",
        ] {
            assert!(FlagKey::new(s).is_ok(), "expected {s:?} to be valid");
        }
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(FlagKey::new(""), Err(FlagKeyError::Empty));
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(MAX_KEY_LEN + 1);
        assert_eq!(FlagKey::new(long.clone()), Err(FlagKeyError::TooLong(long.len())));
        assert!(FlagKey::new("a".repeat(MAX_KEY_LEN)).is_ok());
    }

    #[test]
    fn rejects_illegal_chars() {
        assert_eq!(FlagKey::new("Module.beads"), Err(FlagKeyError::IllegalChar('M')));
        assert_eq!(FlagKey::new("a_b"), Err(FlagKeyError::IllegalChar('_')));
        assert_eq!(FlagKey::new("a b"), Err(FlagKeyError::IllegalChar(' ')));
    }

    #[test]
    fn rejects_bad_dots() {
        assert_eq!(FlagKey::new(".lead"), Err(FlagKeyError::MalformedSegment));
        assert_eq!(FlagKey::new("trail."), Err(FlagKeyError::MalformedSegment));
        assert_eq!(FlagKey::new("a..b"), Err(FlagKeyError::MalformedSegment));
    }

    #[test]
    fn rejects_bad_hyphens() {
        assert_eq!(FlagKey::new("module.-beads"), Err(FlagKeyError::MalformedHyphen));
        assert_eq!(FlagKey::new("module.beads-"), Err(FlagKeyError::MalformedHyphen));
        assert_eq!(FlagKey::new("module.be--ads"), Err(FlagKeyError::MalformedHyphen));
    }

    #[test]
    fn exposes_segments() {
        let k = FlagKey::new("module.gt-rig").unwrap();
        assert_eq!(k.segments().collect::<Vec<_>>(), vec!["module", "gt-rig"]);
    }

    #[test]
    fn display_roundtrips() {
        let k = FlagKey::new("feature.dark-mode").unwrap();
        assert_eq!(k.to_string(), "feature.dark-mode");
        assert_eq!(k.as_str(), "feature.dark-mode");
    }

    #[test]
    fn serde_roundtrip_uses_plain_string() {
        let k = FlagKey::new("module.beads").unwrap();
        let json = serde_json::to_string(&k).unwrap();
        assert_eq!(json, "\"module.beads\"");
        let back: FlagKey = serde_json::from_str(&json).unwrap();
        assert_eq!(back, k);
    }

    #[test]
    fn serde_rejects_invalid_on_deserialize() {
        assert!(serde_json::from_str::<FlagKey>("\"Bad.Key\"").is_err());
    }
}
