//! `invite.*` domain dispatch — collaborator invites + the seguimiento contract
//! (hq-4231c1, epic hq-56b5ee).
//!
//! An owner/admin mints an invite (`invite.create`: email + role + TTL); the
//! one-shot token travels by email THROUGH THE OUTBOX (hq-f24599 — no direct
//! transport), and `invite.accept` consumes it exactly once, binding the
//! gt-login identity to the workspace membership via
//! [`gt_auth::PgUsers::add_workspace_member`] — identity is ALWAYS gt-login,
//! never a second auth system.
//!
//! Roles are the Kanban collaborator ladder (viewer < commenter < editor <
//! admin), recorded on the membership; the session shell (`kanban-only` for
//! every non-admin collaborator) and per-action UI gating are the board UI's
//! contract (hq-95c2bb) over the role this flow binds.
//!
//! Seguimiento: the per-user activity feed is the EXISTING `audit.tail` tool
//! filtered by `actor` — every MCP/REST mutation is already audited per
//! workspace, so no new store is needed (ADR: no second event store).

use async_trait::async_trait;
use serde_json::{json, Value};
use chrono::Duration;
use sqlx::types::chrono::Utc;
use sqlx::PgPool;

use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::AppError;
use gt_store_pg::{Invite, InviteError, InvitesRepository, NewEmail, NewInvite, PgEmailOutbox, PgInvites};

use super::util::{descriptor, now_secs, opt, req, str_arg};

const DEFAULT_WORKSPACE: &str = "default";
/// Default invite TTL when the admin names none.
const DEFAULT_TTL_HOURS: i64 = 72;
/// The Kanban collaborator role ladder (ADR + bead design; ratify-once).
const ROLES: &[&str] = &["viewer", "commenter", "editor", "admin"];

/// PG-backed handler for the `invite.*` tool namespace.
pub struct InvitesHandler {
    pool: PgPool,
    /// Base public URL the accept link in the invite email points at
    /// (`GT_PUBLIC_URL`); absent ⇒ the email carries the bare token.
    public_url: Option<String>,
}

impl InvitesHandler {
    /// Wire the shared public-schema pool (invites + outbox + gt-auth users all
    /// live there) and the optional public base URL for accept links.
    pub fn new(pool: PgPool, public_url: Option<String>) -> Self {
        Self { pool, public_url }
    }

    fn repo(&self) -> PgInvites {
        PgInvites::new(self.pool.clone())
    }

    fn users(&self) -> gt_auth::PgUsers {
        gt_auth::PgUsers::new(self.pool.clone(), "default")
    }

    /// Enqueue the invite email through the outbox (hq-f24599) — never a
    /// transport call. Best-effort: a queueing failure is reported in the
    /// response (`email_queued: false`) but the invite stands; the admin can
    /// hand the token over out-of-band.
    async fn mail_invite(&self, invite: &Invite) -> bool {
        let accept_hint = match &self.public_url {
            Some(base) => format!("{}/invite/accept?token={}", base.trim_end_matches('/'), invite.token),
            None => format!("token: {}", invite.token),
        };
        let outbox = PgEmailOutbox::new(self.pool.clone());
        let email = NewEmail {
            id: ulid::Ulid::new().to_string(),
            workspace: invite.workspace.clone(),
            recipient: invite.email.clone(),
            cc: vec![],
            subject: format!(
                "Invitación al tablero del workspace {} (rol {})",
                invite.workspace, invite.role
            ),
            body: format!(
                "Has sido invitado a colaborar en el workspace `{}` con rol `{}`.\n\
                 Acepta iniciando sesión (gt-login) y consumiendo tu invitación:\n{}\n\
                 La invitación expira el {}.",
                invite.workspace,
                invite.role,
                accept_hint,
                invite.expires_at.to_rfc3339(),
            ),
            template_ref: None,
            send_at: None,
            created_by: invite.created_by.clone(),
        };
        match gt_store_pg::EmailOutboxRepository::enqueue(&outbox, email).await {
            Ok(_) => true,
            Err(e) => {
                eprintln!("[invites] outbox enqueue failed for {}: {e}", invite.id);
                false
            }
        }
    }
}

fn invite_err(e: InviteError) -> AppError {
    match e {
        InviteError::NotFound(m) => AppError::NotFound(format!("invite: {m}")),
        InviteError::NotConsumable(m) => AppError::Validation(format!("invite not consumable: {m}")),
        InviteError::Db(e) => AppError::Other(format!("invites store: {e}")),
    }
}

/// The invite as served — the token is included ONLY at mint time (the caller
/// must deliver it); listings drop it so a later reader cannot harvest live
/// capabilities.
fn invite_json(i: &Invite, with_token: bool) -> Value {
    let mut v = json!({
        "id": i.id,
        "workspace": i.workspace,
        "email": i.email,
        "role": i.role,
        "status": i.status,
        "expires_at": i.expires_at.to_rfc3339(),
        "created_by": i.created_by,
        "created_at": i.created_at.to_rfc3339(),
        "accepted_at": i.accepted_at.map(|t| t.to_rfc3339()),
        "accepted_by": i.accepted_by,
    });
    if with_token {
        v["token"] = json!(i.token);
    }
    v
}

/// A 192-bit URL-safe random token: unguessable, never sequential.
fn mint_token() -> Result<String, AppError> {
    let mut bytes = [0u8; 24];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| AppError::Other(format!("token entropy unavailable: {e}")))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[async_trait]
impl DomainHandler for InvitesHandler {
    fn namespace(&self) -> &'static str {
        "invite"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "invite.create",
                "Invite a collaborator to the caller's workspace: mints a one-shot token \
                 bound to (email, role viewer|commenter|editor|admin, TTL hours default 72) \
                 and mails it via the email outbox. The accept binds the gt-login identity \
                 to the membership. Admin-scoped. Returns the invite INCLUDING the token \
                 (the only time it is served).",
                &[req("email", "string"), req("role", "string"), opt("ttl_hours", "integer")],
            ),
            descriptor(
                "invite.list",
                "The caller workspace's invites, newest first (tokens omitted). Pending \
                 invites past expiry show — and are flipped — expired. Read-only.",
                &[],
            ),
            descriptor(
                "invite.revoke",
                "Revoke a still-pending invite of this workspace (a consumed/expired one \
                 cannot be revoked).",
                &[req("id", "string")],
            ),
            descriptor(
                "invite.accept",
                "Consume an invite token exactly once and bind the invited gt-login \
                 identity to the workspace membership with the invite's role \
                 (workspace_member-add). Double-accept, revoked, expired, and unknown \
                 tokens are each rejected with their reason. The invited user must \
                 already exist in gt-login (sign in/up first).",
                &[req("token", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let workspace = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE);
        match tool {
            "invite.create" => {
                let email = str_arg(&ctx.args, "email")?.trim().to_string();
                let role = str_arg(&ctx.args, "role")?.trim().to_string();
                if !email.contains('@') {
                    return Err(AppError::Validation(format!(
                        "`email` must be an email address, got `{email}`"
                    )));
                }
                if !ROLES.contains(&role.as_str()) {
                    return Err(AppError::Validation(format!(
                        "unknown role `{role}` (viewer|commenter|editor|admin)"
                    )));
                }
                let ttl = ctx
                    .args
                    .get("ttl_hours")
                    .and_then(Value::as_i64)
                    .unwrap_or(DEFAULT_TTL_HOURS)
                    .clamp(1, 24 * 30);
                let invite = self
                    .repo()
                    .create(NewInvite {
                        id: ulid::Ulid::new().to_string(),
                        token: mint_token()?,
                        workspace: workspace.to_string(),
                        email,
                        role,
                        expires_at: Utc::now() + Duration::hours(ttl),
                        created_by: ctx.actor.to_string(),
                    })
                    .await
                    .map_err(invite_err)?;
                let email_queued = self.mail_invite(&invite).await;
                Ok(json!({
                    "ok": true,
                    "invite": invite_json(&invite, true),
                    "email_queued": email_queued,
                }))
            }
            "invite.list" => {
                let invites = self.repo().list(workspace).await.map_err(invite_err)?;
                Ok(json!({
                    "workspace": workspace,
                    "invites": invites.iter().map(|i| invite_json(i, false)).collect::<Vec<_>>(),
                }))
            }
            "invite.revoke" => {
                let id = str_arg(&ctx.args, "id")?;
                self.repo().revoke(id, workspace).await.map_err(invite_err)?;
                Ok(json!({ "ok": true, "id": id, "revoked": true }))
            }
            "invite.accept" => {
                let token = str_arg(&ctx.args, "token")?;
                // One-shot consume FIRST (the CAS is the double-accept guard)…
                let invite = self.repo().accept(token, ctx.actor).await.map_err(invite_err)?;
                // …then bind the gt-login identity to the membership. The invited
                // email must already be a global user (signed up through gt-login);
                // otherwise the consume is rolled back so the token survives the
                // user finishing sign-up.
                let bound = self
                    .users()
                    .add_workspace_member(&invite.email, &invite.workspace, &invite.role, now_secs())
                    .await
                    .map_err(|e| AppError::Other(format!("membership bind failed: {e}")))?;
                if !bound {
                    // Roll the consume back: the identity does not exist yet.
                    sqlx::query(
                        "UPDATE workspace_invites SET status = 'pending', accepted_at = NULL, \
                         accepted_by = NULL WHERE id = $1",
                    )
                    .bind(&invite.id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| AppError::Other(format!("invite rollback failed: {e}")))?;
                    return Err(AppError::Validation(format!(
                        "no gt-login identity exists for {} — sign in/up first, then accept again \
                         (the token is still valid)",
                        invite.email
                    )));
                }
                Ok(json!({
                    "ok": true,
                    "workspace": invite.workspace,
                    "email": invite.email,
                    "role": invite.role,
                    // The board UI contract (hq-95c2bb): every non-admin
                    // collaborator session is kanban-only.
                    "kanban_only": invite.role != "admin",
                }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
