//! `comments.*` domain dispatch (hq-57042e, epic hq-56b5ee).
//!
//! Routes the threaded-comments tools — `comments.{create,list,update,delete}` in
//! `validate`/`execute` pairs — onto the per-workspace [`PgComments`] repository.
//!
//! Target existence is verified here (the port stays a plain row store): a
//! `card` target must be a live bead in the caller's Dolt tracker, a `doc`
//! target a live row in the tenant's `documents`. Threading parents must belong
//! to the same target. Edits/deletes are restricted to the comment's author
//! (the server-injected scope actor) — an `admin`/`*` actor may moderate.
//!
//! `@mention` (ADR low-coupling): handles parsed from the body resolve against
//! the tenant's member mirror (`ws_<slug>.users`); each resolved member gets a
//! `notifications` row + the `notification.created.v1` SSE event — the same
//! shape `notify.send` produces, so the dashboard bell just works.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;

use gt_comments::{
    mentions::parse_mentions, CommentsModule, CreateComment, DeleteComment, ListComments,
    UpdateComment,
};
use gt_events::EventKind;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::{GtModule, McpRegistry, McpTool};
use gt_store_dolt::{AppError, DoltIssues, WorkspacePools};
use gt_store_pg::{
    Comment, CommentError, CommentsRepository, DocumentsRepository, NewComment, PgComments,
    PgDocuments,
};

use super::eventlog::EventLog;
use super::pools::WsPools;

/// PG-backed handler for the `comments.*` tool namespace.
pub struct CommentsHandler {
    /// Per-workspace PG pools (comments + documents + member mirror).
    pools: Arc<WsPools>,
    /// The default-workspace Dolt tracker (card-existence fallback).
    dolt: Arc<DoltIssues>,
    /// Per-workspace Dolt pools when multi-tenant routing is on (`GT_DOLT_BASE_URL`).
    dolt_workspaces: Option<Arc<WorkspacePools>>,
    /// The shared `public.notifications` pool the mention dispatch writes to.
    notifications: PgPool,
    /// SSE event log — mention notifications emit `notification.created.v1`.
    log: Arc<EventLog>,
}

/// Mirror of the `notify.send` SSE frame so a mention lights the same bell.
struct MentionNotification {
    id: String,
    workspace: String,
    from_role: String,
    title: String,
    body: String,
    kind: String,
}

impl serde::Serialize for MentionNotification {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("MentionNotification", 6)?;
        st.serialize_field("id", &self.id)?;
        st.serialize_field("workspace", &self.workspace)?;
        st.serialize_field("from_role", &self.from_role)?;
        st.serialize_field("title", &self.title)?;
        st.serialize_field("body", &self.body)?;
        st.serialize_field("kind", &self.kind)?;
        st.end()
    }
}

impl EventKind for MentionNotification {
    fn kind(&self) -> &'static str {
        "notification.created.v1"
    }
}

/// One comment mutation, broadcast post-commit to the per-workspace SSE feed
/// (hq-0c8fe1). `target_kind`/`target_id` ride at the TOP level so the stream's
/// Kanban topics match without unpacking: `board:{rig}:{ws}` picks up
/// `target_kind=card` comments by target-id rig prefix, `doc:{id}` picks up
/// `target_kind=doc` ones. The producer (this handler) only appends to the
/// event log — it never references the SSE stream or any subscriber (ADR D7
/// low-coupling: consumers attach to the log, not to producers).
#[derive(serde::Serialize)]
struct CommentEvent {
    verb: &'static str,
    id: String,
    target_kind: String,
    target_id: String,
    author: String,
    parent_id: Option<String>,
    actor: String,
    /// The body, present on create/update so a client patches in place.
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

impl EventKind for CommentEvent {
    fn kind(&self) -> &'static str {
        match self.verb {
            "created" => "comments.created.v1",
            "updated" => "comments.updated.v1",
            _ => "comments.deleted.v1",
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

fn val(e: gt_comments::ValidationError) -> AppError {
    AppError::Validation(e.0)
}

fn comment_err(e: CommentError) -> AppError {
    match e {
        CommentError::NotFound(id) => AppError::NotFound(format!("comment {id}")),
        CommentError::Db(e) => AppError::Other(format!("comments store: {e}")),
    }
}

fn comment_json(c: &Comment) -> Value {
    json!({
        "id": c.id,
        "target_kind": c.target_kind,
        "target_id": c.target_id,
        "author": c.author,
        "body": c.body,
        "parent_id": c.parent_id,
        "created_at": c.created_at.to_rfc3339(),
        "edited_at": c.edited_at.map(|t| t.to_rfc3339()),
    })
}

impl CommentsHandler {
    /// Wire the per-workspace PG pools, the Dolt tracker (default store +
    /// optional per-workspace pools), the notifications pool, and the SSE log.
    pub fn new(
        pools: Arc<WsPools>,
        dolt: Arc<DoltIssues>,
        dolt_workspaces: Option<Arc<WorkspacePools>>,
        notifications: PgPool,
        log: Arc<EventLog>,
    ) -> Self {
        Self { pools, dolt, dolt_workspaces, notifications, log }
    }

    async fn repo(&self, ws: Option<&str>) -> Result<PgComments, AppError> {
        Ok(PgComments::new(self.pools.get(ws).await?))
    }

    /// The caller's Dolt tracker: the tenant's own `hq_<ws>` store in
    /// multi-tenant mode, else the shared default store.
    async fn tracker(&self, ws: Option<&str>) -> Result<Arc<DoltIssues>, AppError> {
        match (&self.dolt_workspaces, ws) {
            (Some(pools), Some(ws)) => {
                Ok(Arc::new(DoltIssues::new(pools.ensured_pool(ws).await?)))
            }
            _ => Ok(self.dolt.clone()),
        }
    }

    /// Reject a comment whose target does not exist (live) in this workspace.
    async fn check_target(
        &self,
        ws: Option<&str>,
        kind: &str,
        id: &str,
    ) -> Result<(), AppError> {
        match kind {
            "card" => {
                let tracker = self.tracker(ws).await?;
                if tracker.get_detail(id).await?.is_none() {
                    return Err(AppError::NotFound(format!("card (bead) {id}")));
                }
            }
            "doc" => {
                let docs = PgDocuments::new(self.pools.get(ws).await?);
                let live = docs
                    .get(id)
                    .await
                    .map_err(|e| AppError::Other(format!("documents store: {e}")))?
                    .map(|d| d.deleted_at.is_none())
                    .unwrap_or(false);
                if !live {
                    return Err(AppError::NotFound(format!("document {id}")));
                }
            }
            other => return Err(AppError::Validation(format!("unknown target_kind `{other}`"))),
        }
        Ok(())
    }

    /// Author/moderator gate for update/delete: the comment's author, or an
    /// elevated actor (`admin` / the boot `gt-core` operator), may mutate it.
    fn check_author(actor: &str, comment: &Comment) -> Result<(), AppError> {
        if comment.author == actor || actor == "admin" || actor == "gt-core" {
            return Ok(());
        }
        Err(AppError::Validation(format!(
            "comment {} belongs to {}; only its author (or an admin) may modify it",
            comment.id, comment.author
        )))
    }

    /// Broadcast one comment mutation to the SSE feed, post-commit (hq-0c8fe1).
    /// Best-effort: the write already succeeded; a feed failure only logs.
    fn emit_comment_event(
        &self,
        ws: Option<&str>,
        verb: &'static str,
        comment: &Comment,
        actor: &str,
        with_body: bool,
    ) {
        let ev = CommentEvent {
            verb,
            id: comment.id.clone(),
            target_kind: comment.target_kind.clone(),
            target_id: comment.target_id.clone(),
            author: comment.author.clone(),
            parent_id: comment.parent_id.clone(),
            actor: actor.to_string(),
            body: with_body.then(|| comment.body.clone()),
        };
        if let Err(e) = self.log.append(ws, ev) {
            eprintln!("[comments] SSE emit failed: {e}");
        }
    }

    /// Best-effort `@mention` dispatch: resolve each handle against the tenant
    /// member mirror; every hit gets a notifications row + SSE event. A failure
    /// here never fails the comment write (the comment IS the source of truth).
    async fn dispatch_mentions(
        &self,
        ws: Option<&str>,
        repo: &PgComments,
        actor: &str,
        comment: &Comment,
    ) {
        let workspace = ws.unwrap_or("default").to_string();
        for handle in parse_mentions(&comment.body) {
            let member = match repo.resolve_mention(&handle).await {
                Ok(Some(email)) => email,
                Ok(None) => continue, // unknown/ambiguous handle: notify nobody
                Err(e) => {
                    eprintln!("[comments] mention resolve `{handle}` failed: {e}");
                    continue;
                }
            };
            let title = format!("@{handle}: {actor} te mencionó en un comentario");
            let body = format!(
                "{} {} · {}: {}",
                comment.target_kind, comment.target_id, member, comment.body
            );
            let row: Result<(String,), sqlx::Error> = sqlx::query_as(
                "INSERT INTO notifications (workspace, from_role, title, body, kind) \
                 VALUES ($1, $2, $3, $4, $5) RETURNING id::text",
            )
            .bind(&workspace)
            .bind(actor)
            .bind(&title)
            .bind(&body)
            .bind("info")
            .fetch_one(&self.notifications)
            .await;
            match row {
                Ok((id,)) => {
                    let ev = MentionNotification {
                        id,
                        workspace: workspace.clone(),
                        from_role: actor.to_string(),
                        title,
                        body,
                        kind: "info".into(),
                    };
                    let ws_opt = (workspace != "default").then_some(workspace.as_str());
                    if let Err(e) = self.log.append(ws_opt, ev) {
                        eprintln!("[comments] mention SSE emit failed: {e}");
                    }
                }
                Err(e) => eprintln!("[comments] mention notification insert failed: {e}"),
            }
        }
    }
}

#[async_trait]
impl DomainHandler for CommentsHandler {
    fn namespace(&self) -> &'static str {
        "comments"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        // Single source of truth: harvest the GtModule's tool contract so the
        // advertised schemas and the verbs dispatched below never drift.
        let mut reg = McpRegistry::new();
        CommentsModule.register_mcp_tools(&mut reg);
        reg.tools().to_vec()
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "comments.create.validate" => {
                parse::<CreateComment>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "comments.create.execute" => {
                let cmd = parse::<CreateComment>(ctx.args)?;
                cmd.validate().map_err(val)?;
                // Optional `workspace` arg overrides the session scope (mirrors
                // issues.*), so a default-scoped caller can target another
                // workspace's bead. Held in a local to outlive the moved `cmd`.
                let ws_override = cmd.workspace.clone();
                let ws = ws_override.as_deref().or(ctx.workspace);
                self.check_target(ws, &cmd.target_kind, &cmd.target_id)
                    .await?;
                let repo = self.repo(ws).await?;
                // A reply must thread under a live comment of the SAME target.
                if let Some(parent_id) = &cmd.parent_id {
                    let parent = repo.get(parent_id).await.map_err(comment_err)?;
                    if parent.target_kind != cmd.target_kind || parent.target_id != cmd.target_id {
                        return Err(AppError::Validation(format!(
                            "parent comment {parent_id} belongs to {} {}, not {} {}",
                            parent.target_kind, parent.target_id, cmd.target_kind, cmd.target_id
                        )));
                    }
                }
                let comment = repo
                    .insert(NewComment {
                        id: ulid::Ulid::new().to_string(),
                        target_kind: cmd.target_kind,
                        target_id: cmd.target_id,
                        author: ctx.actor.to_string(),
                        body: cmd.body,
                        parent_id: cmd.parent_id,
                    })
                    .await
                    .map_err(comment_err)?;
                self.emit_comment_event(ws, "created", &comment, ctx.actor, true);
                self.dispatch_mentions(ws, &repo, ctx.actor, &comment)
                    .await;
                Ok(comment_json(&comment))
            }
            "comments.list.validate" => {
                parse::<ListComments>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "comments.list.execute" => {
                let cmd = parse::<ListComments>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let ws = cmd.workspace.as_deref().or(ctx.workspace);
                let repo = self.repo(ws).await?;
                let comments = repo
                    .list_for_target(&cmd.target_kind, &cmd.target_id)
                    .await
                    .map_err(comment_err)?;
                Ok(json!({
                    "target_kind": cmd.target_kind,
                    "target_id": cmd.target_id,
                    "comments": comments.iter().map(comment_json).collect::<Vec<_>>(),
                }))
            }
            "comments.update.validate" => {
                parse::<UpdateComment>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "comments.update.execute" => {
                let cmd = parse::<UpdateComment>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let ws = cmd.workspace.as_deref().or(ctx.workspace);
                let repo = self.repo(ws).await?;
                let existing = repo.get(&cmd.id).await.map_err(comment_err)?;
                Self::check_author(ctx.actor, &existing)?;
                let updated = repo
                    .update_body(&cmd.id, &cmd.body)
                    .await
                    .map_err(comment_err)?;
                // Handles newly added by the edit notify too; resolution dedup
                // is acceptable noise (best-effort, mirrors live chat tools).
                self.emit_comment_event(ws, "updated", &updated, ctx.actor, true);
                self.dispatch_mentions(ws, &repo, ctx.actor, &updated)
                    .await;
                Ok(comment_json(&updated))
            }
            "comments.delete.validate" => {
                parse::<DeleteComment>(ctx.args)?.validate().map_err(val)?;
                Ok(json!({ "ok": true }))
            }
            "comments.delete.execute" => {
                let cmd = parse::<DeleteComment>(ctx.args)?;
                cmd.validate().map_err(val)?;
                let ws = cmd.workspace.as_deref().or(ctx.workspace);
                let repo = self.repo(ws).await?;
                let existing = repo.get(&cmd.id).await.map_err(comment_err)?;
                Self::check_author(ctx.actor, &existing)?;
                repo.soft_delete(&cmd.id).await.map_err(comment_err)?;
                self.emit_comment_event(ws, "deleted", &existing, ctx.actor, false);
                Ok(json!({ "ok": true, "id": cmd.id, "removed": true }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
