//! Command mailbox + seguimiento subscriptions daemon (hq-8a521a, epic hq-56b5ee).
//!
//! The inbound counterpart of the email outbox: a loop that polls the
//! [`InboundMail`] seam (file source today, IMAP/webhook when the server
//! exists), authenticates each sender against the workspace member mirror, and
//! executes the order through the SAME [`DomainRouter`] every other surface
//! uses — never a private code path.
//!
//! ## Security (this is a remote-command surface)
//!
//! - **Verified members only.** The `From:` must resolve to a row of the
//!   tenant's `users` mirror; anything else is QUARANTINED (kept for review)
//!   and executes nothing — no reply either, so an unknown sender learns
//!   nothing.
//! - **Role ceiling.** Commands run under the sender's membership role
//!   (viewer < commenter < editor < admin, hq-4231c1); the mailbox checks the
//!   ceiling BEFORE dispatching, and the dispatch itself runs with the
//!   sender's email as the actor — never an elevated identity.
//! - **Destructive orders confirm.** `close <card>` only stages a pending
//!   action and replies with a one-shot token; the move executes when the
//!   member replies `confirm <token>` (tokens expire).
//! - **Email is untrusted input.** Free-form text is NEVER executed: it is
//!   recorded as a bead (created_by = the sender) for the normal agent
//!   pipeline (convoy/orchd) to pick up under its own gates.
//! - **Audited.** Every executed command lands in the MCP audit trail
//!   (`mailbox.<intent>`), so `audit.tail(actor=email)` — the seguimiento
//!   feed — shows it.
//!
//! ## Subscriptions
//!
//! Members subscribe by mail (`subscribe card hq-…`); the daemon tails the
//! per-workspace event log and fans matching `issues.*`/`comments.*`/
//! `documents.*` frames out as outbox emails (`template_ref = sub:{kind}:{id}`).
//! Replying to one of those routes back here and posts a comment on the same
//! target — the email↔comment bridge.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use sqlx::PgPool;

use gt_audit::{AuditRecord, AuditSink};
use gt_mcp_server::{DomainCtx, DomainRouter};
use gt_notify::{InboundEmail, InboundMail};
use gt_store_pg::{
    EmailOutboxRepository, NewEmail, PgEmailOutbox, PgSubscriptions, SubscriptionsRepository,
};

use crate::mcp::{EventLog, WsPools};

/// The board project key every mailbox order targets (hq-62130a backfill).
const BOARD_WORKSPACE: &str = "default";
/// Pending destructive confirmations expire after this.
const CONFIRM_TTL: Duration = Duration::from_secs(15 * 60);

/// One parsed order. Pure data — parsing is unit-tested without IO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Intent {
    /// `move <card> to <open|working|closed>` (closed stages a confirm).
    Move { card: String, column: String },
    /// `comment <card|doc> <id>: <text>`
    Comment { kind: String, id: String, text: String },
    /// `report` — generate + mail the operator spreadsheet.
    Report,
    /// `subscribe <card|doc|board> <id>` / `unsubscribe …`
    Subscribe { kind: String, id: String },
    Unsubscribe { kind: String, id: String },
    /// `confirm <token>` — execute a staged destructive order.
    Confirm { token: String },
    /// `help`
    Help,
    /// Anything else — recorded as a bead for the agent pipeline, never executed.
    Freeform { text: String },
}

/// Parse subject+body into an [`Intent`]. The subject carries the command; an
/// empty subject falls back to the body's first line.
pub fn parse_intent(subject: &str, body: &str) -> Intent {
    let line = {
        let s = subject.trim();
        if s.is_empty() { body.lines().next().unwrap_or("").trim() } else { s }
    };
    let lower = line.to_lowercase();
    let words: Vec<&str> = line.split_whitespace().collect();

    if lower == "help" || lower == "ayuda" {
        return Intent::Help;
    }
    if lower == "report" || lower == "reporte" {
        return Intent::Report;
    }
    if let Some(rest) = lower.strip_prefix("confirm ") {
        return Intent::Confirm { token: rest.trim().to_string() };
    }
    // move <card> to <column>
    if words.len() == 4 && lower.starts_with("move ") && words[2].eq_ignore_ascii_case("to") {
        let column = words[3].to_lowercase();
        if ["open", "working", "closed"].contains(&column.as_str()) {
            return Intent::Move { card: words[1].to_string(), column };
        }
    }
    // close <card> = move to closed (destructive path)
    if words.len() == 2 && lower.starts_with("close ") {
        return Intent::Move { card: words[1].to_string(), column: "closed".into() };
    }
    // comment <kind> <id>: <text>   (text from the rest of line or the body)
    if words.len() >= 3 && lower.starts_with("comment ") {
        let kind = words[1].to_lowercase();
        if ["card", "doc"].contains(&kind.as_str()) {
            let id = words[2].trim_end_matches(':').to_string();
            let after = line.splitn(2, ':').nth(1).map(str::trim).unwrap_or("");
            let text = if after.is_empty() { body.trim() } else { after };
            if !text.is_empty() {
                return Intent::Comment { kind, id, text: text.to_string() };
            }
        }
    }
    // (un)subscribe <kind> <id>
    if words.len() == 3 {
        let kind = words[1].to_lowercase();
        if ["card", "doc", "board"].contains(&kind.as_str()) {
            if lower.starts_with("subscribe ") {
                return Intent::Subscribe { kind, id: words[2].to_string() };
            }
            if lower.starts_with("unsubscribe ") {
                return Intent::Unsubscribe { kind, id: words[2].to_string() };
            }
        }
    }
    Intent::Freeform { text: format!("{line}\n\n{body}").trim().to_string() }
}

/// The role ceiling (hq-4231c1 ladder). `admin`/`*` may do everything; the
/// check is BEFORE dispatch so a denied order never reaches a tool.
pub fn role_allows(roles: &[String], intent: &Intent) -> bool {
    let has = |r: &str| roles.iter().any(|x| x == r || x == "*" || x == "admin");
    match intent {
        Intent::Help | Intent::Report | Intent::Subscribe { .. } | Intent::Unsubscribe { .. } => {
            // Any verified member may read/report/subscribe.
            !roles.is_empty()
        }
        Intent::Comment { .. } => has("commenter") || has("editor"),
        Intent::Move { .. } | Intent::Confirm { .. } => has("editor"),
        Intent::Freeform { .. } => has("editor"),
    }
}

const HELP: &str = "Comandos del buzón gt Kanban:\n\
    move <card> to <open|working|closed>\n\
    close <card>            (pide confirmación)\n\
    confirm <token>\n\
    comment card <id>: <texto>\n\
    comment doc <id>: <texto>\n\
    subscribe card|doc|board <id>\n\
    unsubscribe card|doc|board <id>\n\
    report\n\
    help\n\
    Cualquier otro texto se registra como tarea para los agentes.";

/// A staged destructive order awaiting `confirm <token>`.
struct Pending {
    sender: String,
    intent: Intent,
    expires: SystemTime,
}

/// The mailbox daemon state.
pub struct Mailbox {
    inbox: Arc<dyn InboundMail>,
    domains: Arc<DomainRouter>,
    pool: PgPool,
    ws_pools: Arc<WsPools>,
    audit: Arc<dyn AuditSink + Send + Sync>,
    event_log: Arc<EventLog>,
    workspace: String,
    pending: Mutex<HashMap<String, Pending>>,
    /// Event-log cursor for the subscriptions fan-out.
    last_ts: Mutex<Option<String>>,
}

impl Mailbox {
    /// Wire the seams. `workspace` is the tenant the mailbox serves (the
    /// single-address deploy maps board@… to the default tenant).
    pub fn new(
        inbox: Arc<dyn InboundMail>,
        domains: Arc<DomainRouter>,
        pool: PgPool,
        ws_pools: Arc<WsPools>,
        audit: Arc<dyn AuditSink + Send + Sync>,
        event_log: Arc<EventLog>,
        workspace: String,
    ) -> Self {
        Self {
            inbox,
            domains,
            pool,
            ws_pools,
            audit,
            event_log,
            workspace,
            pending: Mutex::new(HashMap::new()),
            last_ts: Mutex::new(None),
        }
    }

    fn ws_opt(&self) -> Option<&str> {
        (self.workspace != "default").then_some(self.workspace.as_str())
    }

    /// Resolve a sender against the tenant member mirror → membership roles.
    async fn member_roles(&self, email: &str) -> Option<Vec<String>> {
        let pool = self.ws_pools.get(self.ws_opt()).await.ok()?;
        let row: Option<(Vec<String>,)> =
            sqlx::query_as("SELECT roles FROM users WHERE email = $1")
                .bind(email)
                .fetch_optional(pool.pool())
                .await
                .ok()?;
        row.map(|(roles,)| roles)
    }

    /// Threaded reply through the outbox (never a transport call). The
    /// `template_ref` carries `reply:<message_id>` so a future SMTP transport
    /// can set In-Reply-To from it.
    async fn reply(&self, msg: &InboundEmail, subject: &str, body: &str) {
        let outbox = PgEmailOutbox::new(self.pool.clone());
        let _ = outbox
            .enqueue(NewEmail {
                id: ulid::Ulid::new().to_string(),
                workspace: self.workspace.clone(),
                recipient: msg.from.clone(),
                subject: format!("Re: {subject}"),
                body: body.to_string(),
                template_ref: Some(format!("reply:{}", msg.message_id)),
                send_at: None,
                created_by: "mailbox".into(),
            })
            .await;
    }

    fn audit_command(&self, actor: &str, intent_label: &str, args: Value) {
        let _ = self.audit.record(
            AuditRecord::invoked(actor, &format!("mailbox.{intent_label}"), args)
                .in_workspace(&self.workspace),
        );
    }

    async fn dispatch(&self, actor: &str, tool: &str, args: Value) -> Result<Value, String> {
        let ctx = DomainCtx { workspace: Some(self.workspace.as_str()), actor, args };
        match self.domains.dispatch(tool, ctx).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(format!("tool `{tool}` not wired")),
            Err(e) => Err(e.to_string()),
        }
    }

    fn rig_of(card: &str) -> &str {
        card.split('-').next().unwrap_or(card)
    }

    /// Execute one verified, role-cleared intent; returns the reply body.
    async fn execute(&self, sender: &str, intent: Intent) -> String {
        match intent {
            Intent::Help => HELP.to_string(),
            Intent::Move { card, column } if column == "closed" => {
                // Destructive: stage + token; executes only on confirm.
                let token = format!("{:06x}", nanos() & 0xFF_FFFF);
                self.pending.lock().unwrap().insert(
                    token.clone(),
                    Pending {
                        sender: sender.to_string(),
                        intent: Intent::Move { card: card.clone(), column },
                        expires: SystemTime::now() + CONFIRM_TTL,
                    },
                );
                format!(
                    "Cerrar {card} es irreversible desde el buzón. Responde:\nconfirm {token}\n(válido 15 minutos)"
                )
            }
            Intent::Confirm { token } => {
                let staged = {
                    let mut map = self.pending.lock().unwrap();
                    map.retain(|_, p| p.expires > SystemTime::now());
                    map.remove(&token)
                };
                match staged {
                    Some(p) if p.sender == sender => {
                        let Intent::Move { card, column } = p.intent else {
                            return "Orden confirmada desconocida.".into();
                        };
                        self.run_move(sender, &card, &column).await
                    }
                    Some(_) => "Ese token pertenece a otra orden/remitente.".into(),
                    None => "Token desconocido o expirado — repite la orden.".into(),
                }
            }
            Intent::Move { card, column } => self.run_move(sender, &card, &column).await,
            Intent::Comment { kind, id, text } => {
                self.audit_command(sender, "comment", json!({ "kind": kind, "id": id }));
                match self
                    .dispatch(
                        sender,
                        "comments.create.execute",
                        json!({ "target_kind": kind, "target_id": id, "body": text }),
                    )
                    .await
                {
                    Ok(v) => format!(
                        "Comentario publicado en {kind} {id} (id {}).",
                        v.get("id").and_then(Value::as_str).unwrap_or("?")
                    ),
                    Err(e) => format!("No se pudo comentar: {e}"),
                }
            }
            Intent::Report => {
                self.audit_command(sender, "report", json!({}));
                // The default board scope; mail the result back to the sender.
                match self
                    .dispatch(
                        sender,
                        "report.generate",
                        json!({
                            "rig": "hq",
                            "workspace": BOARD_WORKSPACE,
                            "format": "csv",
                            "email_to": sender,
                        }),
                    )
                    .await
                {
                    Ok(v) => format!(
                        "Reporte generado: {} ({} tareas, TOTAL {} h). Llega por correo.",
                        v.get("filename").and_then(Value::as_str).unwrap_or("?"),
                        v.get("rows").and_then(Value::as_i64).unwrap_or(0),
                        v.get("total_horas").and_then(Value::as_f64).unwrap_or(0.0),
                    ),
                    Err(e) => format!("No se pudo generar el reporte: {e}"),
                }
            }
            Intent::Subscribe { kind, id } => {
                self.audit_command(sender, "subscribe", json!({ "kind": kind, "id": id }));
                let subs = PgSubscriptions::new(self.pool.clone());
                match subs.subscribe(&self.workspace, sender, &kind, &id).await {
                    Ok(()) => format!("Suscrito a {kind} {id}. Recibirás cambios por correo; responder a uno publica un comentario."),
                    Err(e) => format!("No se pudo suscribir: {e}"),
                }
            }
            Intent::Unsubscribe { kind, id } => {
                self.audit_command(sender, "unsubscribe", json!({ "kind": kind, "id": id }));
                let subs = PgSubscriptions::new(self.pool.clone());
                match subs.unsubscribe(&self.workspace, sender, &kind, &id).await {
                    Ok(()) => format!("Suscripción a {kind} {id} retirada."),
                    Err(e) => format!("No se pudo retirar: {e}"),
                }
            }
            Intent::Freeform { text } => {
                // NEVER executed as code: recorded for the operator/agent
                // pipeline under the sender's name.
                self.audit_command(sender, "freeform", json!({ "chars": text.len() }));
                let title: String =
                    text.lines().next().unwrap_or("orden por correo").chars().take(70).collect();
                self.record_freeform(sender, &title, &text).await
            }
        }
    }

    /// Freeform order → an operator notification (decision) + the audit row.
    /// The mailbox NEVER executes natural language: turning it into a bead the
    /// agent pipeline runs stays an explicit operator decision in the bell.
    async fn record_freeform(&self, sender: &str, title: &str, text: &str) -> String {
        // The issues namespace is not on the DomainRouter; the daemon holds no
        // Dolt store by design (orders execute through tools). Record the order
        // as a notification for the operator + the audit trail instead, and
        // tell the sender how to make it actionable.
        let row: Result<(String,), sqlx::Error> = sqlx::query_as(
            "INSERT INTO notifications (workspace, from_role, title, body, kind) \
             VALUES ($1, $2, $3, $4, 'decision') RETURNING id::text",
        )
        .bind(&self.workspace)
        .bind(sender)
        .bind(format!("Orden por correo: {title}"))
        .bind(text)
        .fetch_one(&self.pool)
        .await;
        match row {
            Ok(_) => format!(
                "Tu orden quedó registrada para el operador: «{title}». \
                 Para acciones directas usa los comandos estructurados (envía `help`)."
            ),
            Err(e) => format!("No se pudo registrar la orden: {e}"),
        }
    }

    async fn run_move(&self, sender: &str, card: &str, column: &str) -> String {
        self.audit_command(sender, "move", json!({ "card": card, "column": column }));
        match self
            .dispatch(
                sender,
                "board.move.execute",
                json!({
                    "id": card,
                    "rig": Self::rig_of(card),
                    "workspace": BOARD_WORKSPACE,
                    "column": column,
                }),
            )
            .await
        {
            Ok(v) => format!(
                "{card} → {column} (rank {}).",
                v.get("rank").and_then(Value::as_str).unwrap_or("?")
            ),
            Err(e) => format!("No se pudo mover {card}: {e}"),
        }
    }

    /// The email→comment bridge: a reply to one of our subscription emails
    /// posts the body as a comment on the email's target.
    async fn bridge_reply(&self, sender: &str, msg: &InboundEmail, outbox_ref: &str) -> Option<String> {
        // Resolve the outbox row the member replied to → its template_ref.
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT template_ref FROM email_outbox WHERE id = $1")
                .bind(outbox_ref)
                .fetch_optional(&self.pool)
                .await
                .ok()?;
        let template_ref = row?.0?;
        let target = template_ref.strip_prefix("sub:")?;
        let (kind, id) = target.split_once(':')?;
        if !["card", "doc"].contains(&kind) {
            return Some("Las respuestas a un board no publican comentario.".into());
        }
        let text = msg.body.trim();
        if text.is_empty() {
            return Some("Respuesta vacía — nada que comentar.".into());
        }
        self.audit_command(sender, "bridge-comment", json!({ "kind": kind, "id": id }));
        Some(
            match self
                .dispatch(
                    sender,
                    "comments.create.execute",
                    json!({ "target_kind": kind, "target_id": id, "body": text }),
                )
                .await
            {
                Ok(_) => format!("Comentario publicado en {kind} {id}."),
                Err(e) => format!("No se pudo publicar el comentario: {e}"),
            },
        )
    }

    /// Process one inbound message end to end.
    async fn process(&self, msg: InboundEmail) {
        // 1. Sender verification — the hard gate.
        let Some(roles) = self.member_roles(&msg.from).await else {
            eprintln!("[mailbox] quarantining mail from unknown sender {}", msg.from);
            self.inbox.quarantine(&msg, "sender is not a workspace member");
            return;
        };

        // 2. Reply-to-subscription? The bridge wins over the parser.
        if let Some(reply_to) = msg.in_reply_to.clone() {
            // Comment bridge requires commenter+; below that, fall through to parse.
            if role_allows(&roles, &Intent::Comment { kind: String::new(), id: String::new(), text: String::new() }) {
                if let Some(outcome) = self.bridge_reply(&msg.from, &msg, &reply_to).await {
                    self.reply(&msg, &msg.subject, &outcome).await;
                    return;
                }
            }
        }

        // 3. Parse + role ceiling + execute.
        let intent = parse_intent(&msg.subject, &msg.body);
        if !role_allows(&roles, &intent) {
            self.reply(
                &msg,
                &msg.subject,
                "Tu rol no permite esa orden (escala viewer < commenter < editor < admin).",
            )
            .await;
            return;
        }
        let outcome = self.execute(&msg.from, intent).await;
        self.reply(&msg, &msg.subject, &outcome).await;
    }

    /// Fan subscription events out as outbox emails: tail the workspace event
    /// log past the cursor and mail every subscriber of a touched target.
    async fn fan_out(&self) {
        let since = self.last_ts.lock().unwrap().clone();
        let batch = self
            .event_log
            .read_since(self.ws_opt(), None, since.as_deref(), 256)
            .unwrap_or_default();
        let subs = PgSubscriptions::new(self.pool.clone());
        let outbox = PgEmailOutbox::new(self.pool.clone());
        for record in batch {
            *self.last_ts.lock().unwrap() = Some(record.ts.clone());
            let payload = &record.payload;
            let str_of = |k: &str| payload.get(k).and_then(Value::as_str).map(str::to_string);
            // Target resolution per event family.
            let targets: Vec<(String, String)> = if record.kind.starts_with("comments.") {
                match (str_of("target_kind"), str_of("target_id")) {
                    (Some(k), Some(i)) => vec![(k, i)],
                    _ => vec![],
                }
            } else if record.kind.starts_with("issues.") {
                match (str_of("id"), str_of("rig")) {
                    (Some(id), Some(rig)) => vec![("card".into(), id), ("board".into(), rig)],
                    (Some(id), None) => vec![("card".into(), id)],
                    _ => vec![],
                }
            } else if record.kind.starts_with("documents.") {
                match str_of("id") {
                    Some(id) => vec![("doc".into(), id)],
                    None => vec![],
                }
            } else {
                vec![]
            };
            for (kind, id) in targets {
                let Ok(watchers) = subs.subscribers(&self.workspace, &kind, &id).await else {
                    continue;
                };
                for email in watchers {
                    let _ = outbox
                        .enqueue(NewEmail {
                            id: ulid::Ulid::new().to_string(),
                            workspace: self.workspace.clone(),
                            recipient: email,
                            subject: format!("[gt] {} · {kind} {id}", record.kind),
                            body: format!(
                                "Cambio en {kind} {id}: {}\n\n{}\n\nResponde a este correo para comentar.",
                                record.kind, record.payload
                            ),
                            template_ref: Some(format!("sub:{kind}:{id}")),
                            send_at: None,
                            created_by: "mailbox".into(),
                        })
                        .await;
                }
            }
        }
    }

    /// The daemon loop: poll the inbox + fan subscriptions out, every `interval`.
    pub async fn run(self: Arc<Self>, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match self.inbox.poll() {
                Ok(batch) => {
                    for msg in batch {
                        self.process(msg).await;
                    }
                }
                Err(e) => eprintln!("[mailbox] inbound poll failed: {e}"),
            }
            self.fan_out().await;
        }
    }
}

fn nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.subsec_nanos() as u64).unwrap_or(0)
        ^ (std::process::id() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_structured_command_set() {
        assert_eq!(
            parse_intent("move hq-1 to working", ""),
            Intent::Move { card: "hq-1".into(), column: "working".into() }
        );
        assert_eq!(
            parse_intent("close hq-1", ""),
            Intent::Move { card: "hq-1".into(), column: "closed".into() }
        );
        assert_eq!(
            parse_intent("comment card hq-1: bien hecho", ""),
            Intent::Comment { kind: "card".into(), id: "hq-1".into(), text: "bien hecho".into() }
        );
        // Body carries the text when the subject only names the target.
        assert_eq!(
            parse_intent("comment doc 01ABC", "texto en el cuerpo"),
            Intent::Comment { kind: "doc".into(), id: "01ABC".into(), text: "texto en el cuerpo".into() }
        );
        assert_eq!(
            parse_intent("subscribe board hq", ""),
            Intent::Subscribe { kind: "board".into(), id: "hq".into() }
        );
        assert_eq!(
            parse_intent("unsubscribe card hq-1", ""),
            Intent::Unsubscribe { kind: "card".into(), id: "hq-1".into() }
        );
        assert_eq!(parse_intent("confirm abc123", ""), Intent::Confirm { token: "abc123".into() });
        assert_eq!(parse_intent("report", ""), Intent::Report);
        assert_eq!(parse_intent("help", ""), Intent::Help);
        // Subject empty → first body line.
        assert_eq!(parse_intent("", "report\nlo demás"), Intent::Report);
        // Garbage → freeform (recorded, never executed).
        assert!(matches!(parse_intent("hazme un resumen del sprint", ""), Intent::Freeform { .. }));
        // Invalid column → freeform, not a move.
        assert!(matches!(parse_intent("move hq-1 to done", ""), Intent::Freeform { .. }));
    }

    #[test]
    fn role_ceiling_gates_by_the_ladder() {
        let viewer = vec!["viewer".to_string()];
        let commenter = vec!["commenter".to_string()];
        let editor = vec!["editor".to_string()];
        let admin = vec!["admin".to_string()];
        let nobody: Vec<String> = vec![];

        let mv = Intent::Move { card: "hq-1".into(), column: "working".into() };
        let cm = Intent::Comment { kind: "card".into(), id: "hq-1".into(), text: "x".into() };
        let sub = Intent::Subscribe { kind: "card".into(), id: "hq-1".into() };
        let ff = Intent::Freeform { text: "x".into() };

        assert!(!role_allows(&nobody, &sub), "no membership, no nothing");
        assert!(role_allows(&viewer, &sub) && role_allows(&viewer, &Intent::Help));
        assert!(!role_allows(&viewer, &cm) && !role_allows(&viewer, &mv));
        assert!(role_allows(&commenter, &cm) && !role_allows(&commenter, &mv));
        assert!(role_allows(&editor, &mv) && role_allows(&editor, &ff));
        assert!(role_allows(&admin, &mv) && role_allows(&admin, &cm));
    }
}
