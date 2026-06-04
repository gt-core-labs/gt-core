//! Tool-argument structs for the `documents.*` MCP tools (hq-docs-api.1, docs/11).
//!
//! Each struct is the wire shape an MCP client sends; `validate()` is the shape-only guard
//! the `validate` tool runs and the `execute` tool re-runs before touching the stores. The
//! transport-free execute handlers that drive [`DocumentsRepository`](gt_store_pg) live in
//! hq-docs-api.2; this module is the contract only.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A shape-rule violation, surfaced verbatim so the agent sees exactly what was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError(pub String);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validation failed: {}", self.0)
    }
}

impl std::error::Error for ValidationError {}

fn err(msg: impl Into<String>) -> ValidationError {
    ValidationError(msg.into())
}

/// Default search page size when `limit` is omitted.
fn default_limit() -> i64 {
    20
}

/// Input for `documents.attach`: add a document to an owner bead.
///
/// Two content classes (docs/11): `kind="md"` carries `body_md` (markdown, model-readable
/// as-is); `kind="blob"` carries base64 `data` (a binary the blob store keeps + the
/// extractor turns into text). Exactly one content payload is required, matching `kind`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AttachDoc {
    /// Owning bead class: `epic` | `skill` | `spec`.
    pub owner_type: String,
    /// Owning bead id.
    pub owner_id: String,
    /// `md` | `blob`.
    pub kind: String,
    /// Display filename (e.g. `spec.md`, `design.pdf`). Required.
    pub filename: String,
    /// MIME type. Required for `blob` (drives extraction); optional for `md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// `kind="md"`: the markdown body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_md: Option<String>,
    /// `kind="blob"`: the binary contents, base64-encoded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
    /// Who is attaching it (agent or operator). Required.
    pub created_by: String,
}

impl AttachDoc {
    /// Shape-only validation (uniqueness/storage errors surface at execute time).
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.owner_type.is_empty() {
            return Err(err("owner_type is empty"));
        }
        if self.owner_id.is_empty() {
            return Err(err("owner_id is empty"));
        }
        if self.filename.is_empty() {
            return Err(err("filename is empty"));
        }
        if self.created_by.is_empty() {
            return Err(err("created_by is empty"));
        }
        match self.kind.as_str() {
            "md" => {
                if self.body_md.as_deref().unwrap_or("").is_empty() {
                    return Err(err("kind=md requires a non-empty body_md"));
                }
                if self.data_base64.is_some() {
                    return Err(err("kind=md must not carry data_base64"));
                }
            }
            "blob" => {
                if self.data_base64.as_deref().unwrap_or("").is_empty() {
                    return Err(err("kind=blob requires non-empty data_base64"));
                }
                if self.content_type.as_deref().unwrap_or("").is_empty() {
                    return Err(err("kind=blob requires content_type"));
                }
                if self.body_md.is_some() {
                    return Err(err("kind=blob must not carry body_md"));
                }
            }
            other => return Err(err(format!("unknown kind `{other}` (expected md|blob)"))),
        }
        Ok(())
    }
}

/// Input for `documents.update`: edit a document under an optimistic version guard.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpdateDoc {
    /// Document id.
    pub id: String,
    /// The `version` the caller last read; a stale value is rejected at execute time.
    pub expected_version: i64,
    /// New filename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// New markdown body (`kind="md"` documents).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_md: Option<String>,
    /// Who is editing.
    pub edited_by: String,
}

impl UpdateDoc {
    /// Shape-only validation.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.id.is_empty() {
            return Err(err("document id is empty"));
        }
        if self.expected_version < 0 {
            return Err(err("expected_version must be >= 0"));
        }
        if self.filename.is_none() && self.body_md.is_none() {
            return Err(err("update is a no-op: set at least one of filename/body_md"));
        }
        if self.edited_by.is_empty() {
            return Err(err("edited_by is empty"));
        }
        Ok(())
    }
}

/// Input for `documents.remove`: soft-delete a document under a version guard.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RemoveDoc {
    /// Document id.
    pub id: String,
    /// The `version` the caller last read.
    pub expected_version: i64,
}

impl RemoveDoc {
    /// Shape-only validation.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.id.is_empty() {
            return Err(err("document id is empty"));
        }
        if self.expected_version < 0 {
            return Err(err("expected_version must be >= 0"));
        }
        Ok(())
    }
}

/// Input for `documents.list`: the live documents of one owner.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListDocs {
    /// Owning bead class.
    pub owner_type: String,
    /// Owning bead id.
    pub owner_id: String,
}

impl ListDocs {
    /// Shape-only validation.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.owner_type.is_empty() {
            return Err(err("owner_type is empty"));
        }
        if self.owner_id.is_empty() {
            return Err(err("owner_id is empty"));
        }
        Ok(())
    }
}

/// Input for `documents.search`: phase-1 full-text over `body_md`/`extracted_text`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchDocs {
    /// Free-text query (websearch syntax).
    pub query: String,
    /// Optional owner narrowing: both must be set together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_type: Option<String>,
    /// Optional owner narrowing (paired with `owner_type`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    /// Max hits. Defaults to 20.
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl SearchDocs {
    /// Shape-only validation.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.query.trim().is_empty() {
            return Err(err("query is empty"));
        }
        if self.limit <= 0 {
            return Err(err("limit must be > 0"));
        }
        if self.owner_type.is_some() != self.owner_id.is_some() {
            return Err(err("owner_type and owner_id must be set together"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md() -> AttachDoc {
        AttachDoc {
            owner_type: "epic".into(),
            owner_id: "hq-docs".into(),
            kind: "md".into(),
            filename: "spec.md".into(),
            content_type: None,
            body_md: Some("# spec".into()),
            data_base64: None,
            created_by: "agent".into(),
        }
    }

    #[test]
    fn md_attach_requires_body_and_rejects_data() {
        assert!(md().validate().is_ok());
        let mut d = md();
        d.body_md = None;
        assert!(d.validate().is_err(), "md needs body_md");
        let mut d = md();
        d.data_base64 = Some("AA==".into());
        assert!(d.validate().is_err(), "md must not carry data");
    }

    #[test]
    fn blob_attach_requires_data_and_content_type() {
        let blob = AttachDoc {
            kind: "blob".into(),
            body_md: None,
            data_base64: Some("AA==".into()),
            content_type: Some("application/pdf".into()),
            ..md()
        };
        assert!(blob.validate().is_ok());
        let mut d = blob.clone();
        d.content_type = None;
        assert!(d.validate().is_err(), "blob needs content_type");
        let mut d = blob.clone();
        d.data_base64 = None;
        assert!(d.validate().is_err(), "blob needs data");
    }

    #[test]
    fn unknown_kind_rejected() {
        let mut d = md();
        d.kind = "exe".into();
        assert!(d.validate().is_err());
    }

    #[test]
    fn update_must_change_something() {
        let noop = UpdateDoc {
            id: "d1".into(),
            expected_version: 0,
            filename: None,
            body_md: None,
            edited_by: "e".into(),
        };
        assert!(noop.validate().is_err());
    }

    #[test]
    fn search_owner_fields_are_paired() {
        let s = SearchDocs {
            query: "spec".into(),
            owner_type: Some("epic".into()),
            owner_id: None,
            limit: 20,
        };
        assert!(s.validate().is_err(), "owner_type without owner_id is rejected");
    }
}
