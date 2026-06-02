//! One memory: a single hard-won lesson parsed from a corpus markdown file.

use std::fmt;

/// The four memory kinds the corpus uses (see the repo's memory contract).
///
/// The kind drives **APPLY** semantics: `Feedback` memories are *hard operating
/// rules* the autonomous loop must obey; `Project` is mutable state; `Reference`
/// is a pointer to an external resource; `User` describes who the human is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryKind {
    User,
    Feedback,
    Project,
    Reference,
}

impl MemoryKind {
    /// Parse the `metadata.type` token. Unknown/empty → `None`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "user" => Some(Self::User),
            "feedback" => Some(Self::Feedback),
            "project" => Some(Self::Project),
            "reference" => Some(Self::Reference),
            _ => None,
        }
    }

    /// The wire token, matching `metadata.type` in the frontmatter.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Feedback => "feedback",
            Self::Project => "project",
            Self::Reference => "reference",
        }
    }

    /// Whether memories of this kind are **hard operating rules** the loop must
    /// apply as constraints (not merely consult). Only `Feedback` is — those are
    /// the corrections/confirmed-approaches (verify-build-before-commit,
    /// check-commits-before-claim, NN-16 taxonomy, …).
    pub fn is_operating_rule(self) -> bool {
        matches!(self, Self::Feedback)
    }
}

impl fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A parsed memory: frontmatter (`name`, `description`, `metadata.type`) plus
/// the markdown body holding the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Memory {
    /// Short kebab-case slug; the file's `name:` and its cross-link key
    /// (`[[name]]`).
    pub name: String,
    /// One-line summary — the field **RECALL** ranks against to decide
    /// relevance without loading every body.
    pub description: String,
    /// APPLY discriminator.
    pub kind: MemoryKind,
    /// The fact (markdown after the frontmatter), verbatim and trimmed of outer
    /// blank lines.
    pub body: String,
}

/// Why a memory file failed to parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// No leading `---` frontmatter fence (e.g. the `MEMORY.md` index itself).
    MissingFrontmatter,
    /// Frontmatter opened but never closed with a second `---`.
    UnterminatedFrontmatter,
    /// A required key (`name`, `description`, or a valid `type`) was absent.
    MissingField(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::MissingFrontmatter => write!(f, "no leading '---' frontmatter fence"),
            ParseError::UnterminatedFrontmatter => write!(f, "frontmatter '---' never closed"),
            ParseError::MissingField(k) => write!(f, "missing required frontmatter field: {k}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl Memory {
    /// Parse one memory file's text: a leading `---` … `---` YAML-ish frontmatter
    /// followed by the body.
    ///
    /// The frontmatter grammar this corpus uses is small and regular, so it is
    /// parsed line-by-line without a YAML dependency: top-level `name:` and
    /// `description:`, plus the nested `type:` under `metadata:`. `description`
    /// may be quoted; the quotes are stripped.
    pub fn parse(text: &str) -> Result<Self, ParseError> {
        let text = text.strip_prefix('\u{feff}').unwrap_or(text); // tolerate BOM
        let rest = text
            .strip_prefix("---\n")
            .or_else(|| text.strip_prefix("---\r\n"))
            .ok_or(ParseError::MissingFrontmatter)?;
        let end = find_fence(rest).ok_or(ParseError::UnterminatedFrontmatter)?;
        let (front, body) = rest.split_at(end);

        let mut name = None;
        let mut description = None;
        let mut kind = None;
        for line in front.lines() {
            let trimmed = line.trim();
            if let Some(v) = trimmed.strip_prefix("name:") {
                name = Some(unquote(v.trim()).to_string());
            } else if let Some(v) = trimmed.strip_prefix("description:") {
                description = Some(unquote(v.trim()).to_string());
            } else if let Some(v) = trimmed.strip_prefix("type:") {
                // Nested under `metadata:`; the key is unique in the corpus.
                kind = MemoryKind::parse(v);
            }
        }

        let name = name.filter(|s| !s.is_empty()).ok_or(ParseError::MissingField("name"))?;
        let description = description
            .filter(|s| !s.is_empty())
            .ok_or(ParseError::MissingField("description"))?;
        let kind = kind.ok_or(ParseError::MissingField("type"))?;

        // Body = everything after the closing fence line.
        let body = body
            .trim_start_matches("---")
            .trim_start_matches(['\r', '\n'])
            .trim()
            .to_string();

        Ok(Memory { name, description, kind, body })
    }

    /// Whether this memory is a hard operating rule (see
    /// [`MemoryKind::is_operating_rule`]).
    pub fn is_operating_rule(&self) -> bool {
        self.kind.is_operating_rule()
    }

    /// Render back to the corpus markdown format (frontmatter + body) so the
    /// meta-loop (hq-auto.7) can **write back** a learned memory to disk.
    /// Round-trips with [`Memory::parse`].
    pub fn to_markdown(&self) -> String {
        format!(
            "---\nname: {}\ndescription: {}\nmetadata:\n  type: {}\n---\n\n{}\n",
            self.name,
            self.description,
            self.kind.as_str(),
            self.body.trim()
        )
    }
}

/// Locate the byte offset of the line that closes the frontmatter (a line that
/// is exactly `---`). Returns the offset of that line's start.
fn find_fence(s: &str) -> Option<usize> {
    let mut offset = 0;
    for line in s.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            return Some(offset);
        }
        offset += line.len();
    }
    None
}

/// Strip a single pair of surrounding `"` or `'` quotes, if present.
fn unquote(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let (a, b) = (bytes[0], bytes[bytes.len() - 1]);
        if (a == b'"' && b == b'"') || (a == b'\'' && b == b'\'') {
            return &s[1..s.len() - 1];
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "---\nname: verify-build-before-commit\ndescription: \"run cargo test as its own call before merge\"\nmetadata:\n  type: feedback\n---\n\nGate on `N passed; 0 failed`. See [[feedback-command-output-tail]].\n";

    #[test]
    fn parses_frontmatter_and_body() {
        let m = Memory::parse(SAMPLE).unwrap();
        assert_eq!(m.name, "verify-build-before-commit");
        assert_eq!(m.description, "run cargo test as its own call before merge");
        assert_eq!(m.kind, MemoryKind::Feedback);
        assert!(m.body.starts_with("Gate on"));
        assert!(m.is_operating_rule());
    }

    #[test]
    fn round_trips_through_markdown() {
        let m = Memory::parse(SAMPLE).unwrap();
        let again = Memory::parse(&m.to_markdown()).unwrap();
        assert_eq!(m, again);
    }

    #[test]
    fn rejects_index_without_frontmatter() {
        assert_eq!(
            Memory::parse("- [Title](file.md) — hook\n"),
            Err(ParseError::MissingFrontmatter)
        );
    }

    #[test]
    fn rejects_unterminated_frontmatter() {
        assert_eq!(
            Memory::parse("---\nname: x\ndescription: y\n"),
            Err(ParseError::UnterminatedFrontmatter)
        );
    }

    #[test]
    fn rejects_missing_type() {
        assert_eq!(
            Memory::parse("---\nname: x\ndescription: y\n---\nbody\n"),
            Err(ParseError::MissingField("type"))
        );
    }

    #[test]
    fn only_feedback_is_an_operating_rule() {
        assert!(MemoryKind::Feedback.is_operating_rule());
        for k in [MemoryKind::User, MemoryKind::Project, MemoryKind::Reference] {
            assert!(!k.is_operating_rule());
        }
    }
}
