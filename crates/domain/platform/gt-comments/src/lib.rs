//! `gt-comments` — threaded comments for Kanban cards + documents as a
//! [`GtModule`] (hq-57042e, epic hq-56b5ee).
//!
//! One small polymorphic capability: a comment targets a board card (= a bead)
//! or a document, threads via `parent_id`, and an `@mention` in the body
//! dispatches a notification to the named workspace member.
//!
//! This crate is the CONTRACT: the `comments.*` tool-arg structs with their
//! shape-only `validate()`, the [`mentions`] parser, and the [`CommentsModule`]
//! facade. The PG-backed dispatch (target existence against the Dolt tracker /
//! document store, mention → notification) lives in the composition root over
//! the `CommentsRepository` port (`gt_store_pg::comments`), mirroring the
//! gt-documents split.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use gt_module::{Capability, GtModule, McpRegistry, ModuleId, ModuleMeta, Scope};
use gt_module_mcp::schema_for;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};

/// A shape-rule violation, surfaced verbatim so the agent sees exactly what was
/// rejected.
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

/// The closed set of comment target kinds.
pub const TARGET_KINDS: &[&str] = &["card", "doc"];

fn check_target(kind: &str, id: &str) -> Result<(), ValidationError> {
    if !TARGET_KINDS.contains(&kind) {
        return Err(err(format!(
            "unknown target_kind `{kind}` (expected card|doc)"
        )));
    }
    if id.trim().is_empty() {
        return Err(err("target_id is required"));
    }
    Ok(())
}

fn check_body(body: &str) -> Result<(), ValidationError> {
    if body.trim().is_empty() {
        return Err(err("comment body must not be empty"));
    }
    Ok(())
}

/// Input for `comments.create` — add a comment (or threaded reply) to a card or
/// document. The author is the server-injected scope actor, never a wire field.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CreateComment {
    /// Target class: `card` (a bead on the board) | `doc` (a document).
    pub target_kind: String,
    /// The bead id (`card`) or document id (`doc`).
    pub target_id: String,
    /// The comment text. `@handle` tokens notify the named workspace member.
    pub body: String,
    /// Reply threading: the parent comment's id. Omit for a top-level comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Board workspace override (gtcore-… comments-workspace-arg): when set,
    /// targets that workspace's schema instead of the caller's session scope —
    /// mirrors the `workspace` wire field the `issues.*` tools carry, so a
    /// default-scoped caller can comment on another workspace's bead. Absent ⇒
    /// the caller's session workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl CreateComment {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), ValidationError> {
        check_target(&self.target_kind, &self.target_id)?;
        check_body(&self.body)?;
        if matches!(&self.parent_id, Some(p) if p.trim().is_empty()) {
            return Err(err("parent_id must be non-empty when present (omit for top-level)"));
        }
        Ok(())
    }
}

/// Input for `comments.list` — the live thread of one target, chronological.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListComments {
    /// Target class: `card` | `doc`.
    pub target_kind: String,
    /// The bead id (`card`) or document id (`doc`).
    pub target_id: String,
    /// Board workspace override (mirrors `issues.*`): the workspace whose
    /// comment schema to read. Absent ⇒ the caller's session workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl ListComments {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), ValidationError> {
        check_target(&self.target_kind, &self.target_id)
    }
}

/// Input for `comments.update` — overwrite a comment's body (stamps `edited_at`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpdateComment {
    /// The comment id.
    pub id: String,
    /// The new body. Newly-added `@handle` tokens notify on edit too.
    pub body: String,
    /// Board workspace override (mirrors `issues.*`): the workspace whose
    /// comment schema the comment lives in. Absent ⇒ the caller's session
    /// workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl UpdateComment {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.id.trim().is_empty() {
            return Err(err("comment id is required"));
        }
        check_body(&self.body)
    }
}

/// Input for `comments.delete` — soft-delete a comment (replies stay anchored).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeleteComment {
    /// The comment id.
    pub id: String,
    /// Board workspace override (mirrors `issues.*`): the workspace whose
    /// comment schema the comment lives in. Absent ⇒ the caller's session
    /// workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl DeleteComment {
    /// Shape-only guard.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.id.trim().is_empty() {
            return Err(err("comment id is required"));
        }
        Ok(())
    }
}

/// `@mention` parsing.
pub mod mentions {
    /// Extract the unique `@handle` tokens from a comment body, in first-seen
    /// order. A handle is `@` followed by email-ish characters
    /// (`[A-Za-z0-9._+-]`, must start alphanumeric); a bare `@` or an `@` glued
    /// to a preceding word character (e.g. `user@host` in prose) is not a
    /// mention. Trailing dots are trimmed so a sentence-ending `@ana.` mentions
    /// `ana`.
    pub fn parse_mentions(body: &str) -> Vec<String> {
        let bytes = body.as_bytes();
        let mut out: Vec<String> = Vec::new();
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'@'
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
            {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'.' | b'_' | b'+' | b'-'))
                {
                    end += 1;
                }
                let mut handle = &body[start..end];
                while handle.ends_with('.') {
                    handle = &handle[..handle.len() - 1];
                }
                if !handle.is_empty() && handle.as_bytes()[0].is_ascii_alphanumeric() {
                    if !out.iter().any(|h| h == handle) {
                        out.push(handle.to_string());
                    }
                }
                i = end;
            } else {
                i += 1;
            }
        }
        out
    }
}

/// The [`GtModule`] facade for the comments capability. Stateless — the live
/// store is the per-workspace `CommentsRepository` the composition handler
/// resolves per call.
#[derive(Clone, Copy, Default)]
pub struct CommentsModule;

impl CommentsModule {
    /// The module's stable id (`comments`).
    pub fn id() -> ModuleId {
        ModuleId::new("comments").expect("`comments` is a valid module id")
    }
}

impl GtModule for CommentsModule {
    fn meta(&self) -> ModuleMeta {
        ModuleMeta::new(
            Self::id(),
            "Comments",
            Version::new(1, 0, 0),
            "Threaded comments on Kanban cards (beads) and documents — one polymorphic \
             per-workspace store; @mention dispatches a notification to the named member.",
        )
    }

    fn capability(&self) -> Capability {
        Capability::empty().claiming_all([
            Scope::new("comments.read").expect("valid scope"),
            Scope::new("comments.write").expect("valid scope"),
        ])
    }

    fn register_mcp_tools(&self, registry: &mut McpRegistry) {
        registry
            .tool_with_schema(
                "comments.create.validate",
                "Check whether creating a comment would be accepted: target_kind in card|doc, \
                 target_id + body non-empty, parent_id (when present) non-empty. No state change.",
                schema_for::<CreateComment>(),
            )
            .tool_with_schema(
                "comments.create.execute",
                "Add a comment (or threaded reply via parent_id) to a Kanban card (bead) or \
                 document. The author is the calling actor. @handle tokens in the body resolve \
                 against the workspace members and emit a notification each. Target existence \
                 is verified (card -> tracker bead, doc -> live document).",
                schema_for::<CreateComment>(),
            )
            .tool_with_schema(
                "comments.list.validate",
                "Check whether a comment-thread read would be accepted (shape-only). No state change.",
                schema_for::<ListComments>(),
            )
            .tool_with_schema(
                "comments.list.execute",
                "The live comments of one card|doc target in chronological order; reassemble \
                 the thread tree from parent_id. Read-only — no state change.",
                schema_for::<ListComments>(),
            )
            .tool_with_schema(
                "comments.update.validate",
                "Check whether a comment body edit would be accepted (id + body non-empty). \
                 No state change.",
                schema_for::<UpdateComment>(),
            )
            .tool_with_schema(
                "comments.update.execute",
                "Overwrite a comment's body and stamp edited_at. Only the comment's author \
                 (or an admin actor) may edit. Newly-added @mentions notify.",
                schema_for::<UpdateComment>(),
            )
            .tool_with_schema(
                "comments.delete.validate",
                "Check whether a comment soft-delete would be accepted (id non-empty). \
                 No state change.",
                schema_for::<DeleteComment>(),
            )
            .tool_with_schema(
                "comments.delete.execute",
                "Soft-delete a comment (replies stay anchored to the thread). Only the \
                 comment's author (or an admin actor) may delete.",
                schema_for::<DeleteComment>(),
            );
    }
}

#[cfg(test)]
mod tests {
    use super::mentions::parse_mentions;
    use super::*;

    #[test]
    fn create_validates_target_body_and_parent() {
        let mut c = CreateComment {
            target_kind: "card".into(),
            target_id: "hq-1".into(),
            body: "hola".into(),
            parent_id: None,
            workspace: None,
        };
        assert!(c.validate().is_ok());
        c.target_kind = "doc".into();
        assert!(c.validate().is_ok());
        c.target_kind = "epic".into();
        assert!(c.validate().is_err());
        c.target_kind = "card".into();
        c.body = "  ".into();
        assert!(c.validate().is_err());
        c.body = "ok".into();
        c.parent_id = Some(" ".into());
        assert!(c.validate().is_err());
        c.target_id = "".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn update_and_delete_require_id() {
        assert!(UpdateComment { id: "c1".into(), body: "x".into(), workspace: None }.validate().is_ok());
        assert!(UpdateComment { id: "".into(), body: "x".into(), workspace: None }.validate().is_err());
        assert!(UpdateComment { id: "c1".into(), body: " ".into(), workspace: None }.validate().is_err());
        assert!(DeleteComment { id: "c1".into(), workspace: None }.validate().is_ok());
        assert!(DeleteComment { id: "".into(), workspace: None }.validate().is_err());
    }

    #[test]
    fn mentions_parse_handles_and_dedupe() {
        assert_eq!(
            parse_mentions("hola @ana revisa con @bob.perez y @ana"),
            vec!["ana".to_string(), "bob.perez".to_string()]
        );
        // Email-ish handle with + and -.
        assert_eq!(parse_mentions("cc @dev+ops-1"), vec!["dev+ops-1".to_string()]);
        // Sentence-ending dot is trimmed.
        assert_eq!(parse_mentions("gracias @ana."), vec!["ana".to_string()]);
        // user@host in prose is NOT a mention; bare @ is ignored.
        assert!(parse_mentions("mail a root@host o @ nada").is_empty());
        // Must start alphanumeric.
        assert!(parse_mentions("raro @.punto").is_empty());
    }

    #[test]
    fn registers_the_eight_comment_tools() {
        let mut reg = McpRegistry::new();
        CommentsModule.register_mcp_tools(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names.len(), 8, "four commands × validate/execute");
        for base in ["create", "list", "update", "delete"] {
            assert!(names.contains(&format!("comments.{base}.validate").as_str()));
            assert!(names.contains(&format!("comments.{base}.execute").as_str()));
        }
        for tool in reg.tools() {
            let (module, _action, verb) = tool.parse_name().expect("well-formed");
            assert_eq!(module, "comments");
            assert!(matches!(verb, "validate" | "execute"));
        }
    }

    #[test]
    fn capability_owns_comment_scopes() {
        let cap = CommentsModule.capability();
        let scopes: Vec<&str> = cap.scopes().iter().map(Scope::as_str).collect();
        assert_eq!(scopes, ["comments.read", "comments.write"]);
    }
}
