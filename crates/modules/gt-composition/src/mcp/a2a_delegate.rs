//! `a2a.*` MCP tools — inter-agent delegation, peer discovery, and messaging.
//!
//! ## Tools
//!
//! - **`a2a.delegate`** (A3): delegates a sub-task to another agent via the A2A
//!   intake pipeline without an HTTP round-trip. Stamps the caller's MCP session
//!   id as `created_by` (A2 attribution).
//!
//! - **`a2a.discover`** (A7, gtcore-3a3557): returns the Agent Card skills for
//!   every rig in the caller's workspace, so an agent can decide which peer to
//!   delegate to based on capabilities (tags, repo, branch). Each skill's tags are
//!   the rig's bead prefix plus its semantic capability tags (B3, gtcore-1caa48 —
//!   set via `rig.set-tags`); the optional `tag` argument filters on either,
//!   case-insensitively, for skill matching.
//!
//! - **`a2a.status`** (A7, gtcore-3a3557): queries the tracker for a previously
//!   delegated bead, returning its status + title so the delegating agent can
//!   poll the outcome without leaving the MCP surface.
//!
//! - **`a2a.send`**: post a message to a running agent session's inbox. The
//!   sender's MCP session id is stamped automatically.
//!
//! - **`a2a.inbox`**: read unacknowledged messages for the caller's session
//!   (or an explicit target). Returns newest-first, capped at `limit`.
//!
//! - **`a2a.ack`**: acknowledge a message by id so it stops appearing in inbox.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_a2a::AgentSkill;
use gt_channel::DispatchSink;
use gt_issues::handlers::run_create_issue;
use gt_issues::{CreateIssue, Domain, IssueType, SurfaceTree};
use gt_mcp_server::{DomainCtx, DomainHandler, WorkspaceStores};
use gt_module::McpTool;
use gt_rig::{PgRigs, RigRepository};
use gt_store_dolt::{AppError, DoltIssues};

use crate::delegation::{rfc3339_now, DelegationEvent, DEFAULT_TIMEOUT_SECS};
use crate::delegation_http::{A2aPeerClient, PeerRegistry};

use super::a2a_msg::{A2aMessageHandler, MessageEvent};
use super::cross_ws::CrossWsGrants;
use super::eventlog::EventLog;
use super::pools::WsPools;
use super::util::{descriptor, opt, req, str_arg};

/// The workspace a call resolves to when its [`DomainCtx::workspace`] is `None` —
/// the single-tenant default. Mirrors [`super::pools`]'s `DEFAULT_WORKSPACE` so the
/// origin slug the grant check sees matches the tenant the pool cache keys on.
const DEFAULT_WORKSPACE: &str = "default";

/// Intake beads carry no surface, so existence checks are vacuous.
struct NoSurfaces;
impl SurfaceTree for NoSurfaces {
    fn contains(&self, _path: &str) -> bool {
        true
    }
}

/// MCP handler for the `a2a` namespace.
///
/// Wired in `build_domain_router` when ALL of the required A2A envs are set:
/// `GT_A2A_DEFAULT_RIG`, `GT_A2A_INTAKE_EPIC`, `GT_DOLT_URL`, and a dispatch
/// channel (file or PG, same as convoy/agent).
pub struct A2aDelegateHandler {
    store: Arc<DoltIssues>,
    sink: Arc<DispatchSink>,
    default_rig: String,
    default_parent: String,
    /// Per-workspace Postgres pool cache for rig discovery (A7). `None` when
    /// `GT_PG_URL` is unset — `a2a.discover` returns an empty skill list.
    pools: Option<Arc<WsPools>>,
    /// Event log a `delegation.requested.v1` is appended to so the daemon-side
    /// callback/timeout machinery (B5) can push the outcome back to the parent
    /// instead of the parent polling `a2a.status`. `None` ⇒ delegations still
    /// dispatch, just without push-callback tracking.
    delegation_log: Option<Arc<EventLog>>,
    /// Default completion timeout stamped on a delegation when the caller passes
    /// no `timeout_secs` (B5). `0` disables the timeout.
    default_timeout_secs: u64,
    /// Per-workspace Dolt store resolver (B4, gtcore-3f3c57). `Some` enables
    /// cross-workspace delegation: a `workspace` arg naming a tenant other than the
    /// caller's mints the child bead in *that* tenant's `hq_<dest>` tracker. `None`
    /// ⇒ a cross-workspace `workspace` arg is rejected (only same-workspace
    /// delegation against [`store`](Self::store) works, exactly as before).
    stores: Option<Arc<WorkspaceStores>>,
    /// Explicit cross-workspace delegation grants (B4). Deny-by-default: a
    /// cross-tenant hop requires a matching `origin->dest` grant. Default empty ⇒
    /// no cross-workspace delegation, only same-workspace.
    cross_ws_grants: CrossWsGrants,
    /// Event-log backed message channel. `None` when the event log is not
    /// wired — `a2a.send`/`a2a.inbox`/`a2a.ack` return errors.
    msg: Option<A2aMessageHandler>,
    /// Direct rig→rig HTTP delegation (A7, gtcore-3a3557). `None` ⇒ a `peer` arg
    /// on `a2a.delegate` is rejected and only the in-process intake path runs
    /// (the legacy behaviour). `Some` wires the peer endpoint catalog, the
    /// deny-by-default RBAC grant list (`origin -> peer`), and the HTTP client
    /// that POSTs `tasks/send` straight to the peer — orchd never relaying.
    peers: Option<PeerDelegation>,
    /// Rig-level RBAC for the in-process intake path (A5, gtcore-f3a016). An
    /// `origin -> rig` allow-list (same grant syntax as the cross-workspace /
    /// peer grants, but matched WITHOUT the same-name self-allow): when `Some`,
    /// an agent may only delegate work onto a rig its origin holds a grant for —
    /// an ungranted rig is rejected and audited with the origin identity
    /// (acceptance: "agente sin permiso RBAC para un rig recibe rechazo; el audit
    /// lo registra con identidad origen"). `None` ⇒ no rig gate (the legacy
    /// behaviour, byte-for-byte) so existing single-rig deploys are unaffected.
    rig_grants: Option<CrossWsGrants>,
}

/// The wiring a direct rig→rig hop needs: where peers live, who may reach them,
/// and the HTTP client that does it.
struct PeerDelegation {
    registry: Arc<PeerRegistry>,
    /// `origin -> peer name` allow-list, deny-by-default (same shape + tested
    /// semantics as the cross-workspace grants).
    grants: CrossWsGrants,
    client: Arc<A2aPeerClient>,
}

impl A2aDelegateHandler {
    pub fn new(
        store: Arc<DoltIssues>,
        sink: Arc<DispatchSink>,
        default_rig: impl Into<String>,
        default_parent: impl Into<String>,
    ) -> Self {
        Self {
            store,
            sink,
            default_rig: default_rig.into(),
            default_parent: default_parent.into(),
            pools: None,
            delegation_log: None,
            default_timeout_secs: DEFAULT_TIMEOUT_SECS,
            stores: None,
            cross_ws_grants: CrossWsGrants::empty(),
            msg: None,
            peers: None,
            rig_grants: None,
        }
    }

    /// Wire the per-workspace pool cache so `a2a.discover` can read the rig
    /// catalog. Without this, discovery returns an empty skill list.
    pub fn with_pools(mut self, pools: Arc<WsPools>) -> Self {
        self.pools = Some(pools);
        self
    }

    /// Wire push-callback tracking (B5): a successful `a2a.delegate` appends a
    /// `delegation.requested.v1` to `log`, which the daemon-side
    /// [`DelegationCallbackPlugin`](crate::delegation::DelegationCallbackPlugin)
    /// and [`DelegationTimeoutTicker`](crate::delegation::DelegationTimeoutTicker)
    /// consume to push the outcome back to the parent. `default_timeout_secs`
    /// (0 disables) applies when the caller passes no `timeout_secs`.
    pub fn with_delegation_log(
        mut self,
        log: Arc<EventLog>,
        default_timeout_secs: u64,
    ) -> Self {
        self.delegation_log = Some(log);
        self.default_timeout_secs = default_timeout_secs;
        self
    }

    /// Enable cross-workspace delegation (B4, gtcore-3f3c57). With a
    /// [`WorkspaceStores`] resolver and a non-empty [`CrossWsGrants`] allow-list,
    /// a `workspace` arg on `a2a.delegate` naming a tenant other than the caller's
    /// mints the child bead in *that* tenant's `hq_<dest>` tracker — provided the
    /// caller's workspace holds an explicit `origin->dest` grant. The minted bead
    /// carries `parent_id = "<origin_ws>:<parent>"`, the lineage back to the
    /// delegating tenant. Without this wiring (or with an empty grant set) a
    /// cross-workspace `workspace` arg is rejected; same-workspace delegation is
    /// unaffected.
    pub fn with_cross_ws(mut self, stores: Arc<WorkspaceStores>, grants: CrossWsGrants) -> Self {
        self.stores = Some(stores);
        self.cross_ws_grants = grants;
        self
    }

    /// Wire the event log so `a2a.send`/`a2a.inbox`/`a2a.ack` work. Without
    /// this, the message tools return an error.
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.msg = Some(A2aMessageHandler::new(log));
        self
    }

    /// Enable direct rig→rig HTTP delegation (A7, gtcore-3a3557). With a
    /// [`PeerRegistry`] (the `name → base URL` catalog), a deny-by-default
    /// `origin -> peer` [`CrossWsGrants`] allow-list, and an [`A2aPeerClient`]
    /// (carrying the outbound bearer), a `peer` arg on `a2a.delegate` routes the
    /// sub-task **straight to that peer's `POST /a2a`** — discovered via its Agent
    /// Card — with orchd never relaying the message. Without this wiring a `peer`
    /// arg is rejected and only the in-process intake path runs.
    pub fn with_peers(
        mut self,
        registry: Arc<PeerRegistry>,
        grants: CrossWsGrants,
        client: Arc<A2aPeerClient>,
    ) -> Self {
        self.peers = Some(PeerDelegation { registry, grants, client });
        self
    }

    /// Enable rig-level RBAC on the in-process intake path (A5, gtcore-f3a016). With a non-empty
    /// `origin -> rig` allow-list, an agent may only delegate work onto a rig its origin holds a
    /// grant for; an ungranted rig is rejected before any bead is minted and audited with the
    /// origin identity. An empty grant set is treated as "no gate" (legacy behaviour).
    pub fn with_rig_grants(mut self, grants: CrossWsGrants) -> Self {
        self.rig_grants = (!grants.is_empty()).then_some(grants);
        self
    }

    /// Append a delegation event to the wired log (best-effort: the audit/tracking
    /// trail must never fail the hop, which has already happened on the wire).
    fn audit(&self, workspace: Option<&str>, event: DelegationEvent) {
        if let Some(log) = &self.delegation_log {
            if let Err(e) = log.append(workspace, event) {
                eprintln!("[a2a.delegate] delegation audit append failed: {e}");
            }
        }
    }

    /// A7 (gtcore-3a3557): direct rig→rig delegation. Resolve the peer, enforce the
    /// `origin -> peer` RBAC grant (deny-by-default, audited with both identities
    /// on a refusal), discover its Agent Card, and POST `tasks/send` straight to
    /// the peer's endpoint — orchd never touches the message. The peer mints the
    /// child bead and answers with its id, which we return verbatim.
    async fn delegate_peer(&self, peer: &str, ctx: &DomainCtx<'_>) -> Result<Value, AppError> {
        let peers = self.peers.as_ref().ok_or_else(|| {
            AppError::Validation(
                "peer-to-peer delegation is not configured on this server \
                 (set GT_A2A_PEERS / GT_A2A_PEER_GRANTS)"
                    .into(),
            )
        })?;

        // The origin identity: the caller's authoritative tenant drives the grant
        // (a stable, config-keyable name), while the precise actor (MCP session id)
        // is what the audit records as the origin — A7 wants BOTH endpoints named.
        let origin = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE);
        let actor = ctx.actor;

        // Resolve the endpoint base URL first so a refusal can name where it would
        // have gone (destination identity in the audit).
        let peer_url = peers.registry.resolve(peer);

        // RBAC gate (deny-by-default). A refused hop mints nothing and is audited
        // with origin actor + destination peer (A7: "rechazado y auditado —
        // identidad origen + destino").
        if !peers.grants.allows(origin, peer) {
            let reason = format!(
                "peer delegation from `{origin}` to `{peer}` is not granted \
                 (set GT_A2A_PEER_GRANTS)"
            );
            self.audit(
                ctx.workspace,
                DelegationEvent::Denied {
                    origin: actor.to_string(),
                    dest: peer.to_string(),
                    peer_url: peer_url.clone(),
                    reason: reason.clone(),
                    at: rfc3339_now(),
                },
            );
            return Err(AppError::Validation(reason));
        }

        let base_url = peer_url.ok_or_else(|| {
            AppError::Validation(format!(
                "unknown peer `{peer}` — not in GT_A2A_PEERS and not an http(s):// origin"
            ))
        })?;

        let title = ctx
            .args
            .get("title")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Validation("`title` is required".into()))?
            .to_string();
        let description = ctx
            .args
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or(&title)
            .to_string();
        let rig = ctx.args.get("rig").and_then(Value::as_str).map(str::to_owned);
        let priority = ctx
            .args
            .get("priority")
            .and_then(Value::as_u64)
            .map(|p| p.min(2))
            .unwrap_or(1);

        // Discovery via the peer's Agent Card — the JSON-RPC target is whatever the
        // card advertises as `url`, not an assumed path.
        let card = peers
            .client
            .discover(&base_url)
            .await
            .map_err(|e| AppError::Other(format!("peer discovery: {e}")))?;

        // When the caller named a rig, require the peer's card to actually advertise
        // it (skill id OR a tag) — discovery is load-bearing, not decorative. A card
        // with no skills at all is accepted (the peer routes by its own default).
        if let Some(rig) = &rig {
            if !card.skills.is_empty()
                && !card
                    .skills
                    .iter()
                    .any(|s| &s.id == rig || s.tags.iter().any(|t| t == rig))
            {
                return Err(AppError::Validation(format!(
                    "peer `{peer}` advertises no skill for rig `{rig}` (Agent Card at {base_url})"
                )));
            }
        }

        // The message body: title on the first line (the peer's gateway splits it
        // back into title/description), full description after. Metadata carries
        // the routing hints the peer's gateway already reads, plus `caller` so the
        // peer stamps the ORIGIN actor as the bead's `created_by` (A2 attribution
        // crosses the hop).
        let text = if description == title {
            title.clone()
        } else {
            format!("{title}\n\n{description}")
        };
        let mut meta = json!({ "priority": priority, "caller": actor });
        if let Some(rig) = &rig {
            meta["rig"] = json!(rig);
        }

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let client_task_id = format!("a2a-{origin}-{nanos}");

        let task = peers
            .client
            .send(&card.url, &client_task_id, &text, meta)
            .await
            .map_err(|e| AppError::Other(format!("peer tasks/send: {e}")))?;

        // Record the successful hop for observability. `timeout_secs = 0` disables
        // the local timeout ticker — the child bead lives in the PEER's tracker, so
        // its terminal events never reach this hub and a non-zero timeout would
        // spuriously auto-escalate a delegation that is in fact progressing there.
        self.audit(
            ctx.workspace,
            DelegationEvent::Requested {
                child: task.id.clone(),
                parent: actor.to_string(),
                rig: rig.clone().unwrap_or_else(|| peer.to_string()),
                at: rfc3339_now(),
                timeout_secs: 0,
            },
        );

        let state = serde_json::to_value(task.status.state).unwrap_or(Value::Null);
        Ok(json!({
            "id": task.id,
            "peer": peer,
            "peer_url": base_url,
            "endpoint": card.url,
            "transport": "http",
            "status": "submitted",
            "state": state,
        }))
    }
}

#[async_trait]
impl DomainHandler for A2aDelegateHandler {
    fn namespace(&self) -> &'static str {
        "a2a"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "a2a.delegate",
                "Delegate a sub-task to another agent via the A2A intake pipeline. \
                 Mints a tracker bead (child of the default intake epic) and dispatches it \
                 onto the scheduler. The calling agent's session id is stamped as `created_by`. \
                 You do NOT need to poll a2a.status: when the child reaches a terminal state the \
                 daemon pushes a callback (delegation.completed.v1 + operator bell), and a \
                 `timeout_secs` (default applies) auto-escalates a stuck delegation. \
                 Pass `workspace` to delegate into ANOTHER tenant (cross-workspace): the bead is \
                 minted in that tenant's tracker and `parent_id` records the origin lineage \
                 (`<origin>:<parent>`). Cross-workspace delegation requires an explicit operator \
                 grant; without one it is rejected. \
                 Pass `peer` (a configured peer name or an http(s):// origin) to delegate \
                 PEER-TO-PEER: the task is sent straight to that rig's own A2A endpoint — \
                 discovered via its Agent Card — over a direct HTTP `tasks/send`, with orchd \
                 NOT relaying. The returned `id` is the bead the PEER minted. A peer hop \
                 requires an explicit `origin->peer` grant; an ungranted hop is rejected and \
                 audited.",
                &[
                    req("title", "string"),
                    opt("description", "string"),
                    opt("rig", "string"),
                    opt("parent_id", "string"),
                    opt("priority", "number"),
                    opt("timeout_secs", "number"),
                    opt("workspace", "string"),
                    opt("peer", "string"),
                ],
            ),
            descriptor(
                "a2a.discover",
                "Discover peer agent capabilities. Returns one Agent Card skill per rig in \
                 the workspace catalog, so you can decide which peer to delegate to based on \
                 name, tags, and repo. Each skill's `tags` carry the rig's bead prefix plus its \
                 semantic capability tags (e.g. rust, frontend, infra; set via rig.set-tags). \
                 Pass `tag` to filter by prefix OR semantic tag (case-insensitive). Pass \
                 `workspace` to discover the rigs of ANOTHER tenant you hold a cross-workspace \
                 delegation grant for (an ungranted tenant is rejected).",
                &[opt("tag", "string"), opt("workspace", "string")],
            ),
            descriptor(
                "a2a.status",
                "One-shot read of a previously delegated bead's current status, title, and \
                 timestamps. Polling is no longer required: a2a.delegate registers a push \
                 callback that reports the terminal outcome (delegation.completed.v1 / bell). \
                 Use this only for an on-demand check.",
                &[req("id", "string")],
            ),
            descriptor(
                "a2a.send",
                "Send a message to a running agent session. The message appears in \
                 the target's inbox (polled via `a2a.inbox`). Use this for operator \
                 instructions, clarifications, or inter-agent coordination without \
                 killing the session.",
                &[
                    req("to", "string"),
                    req("body", "string"),
                    opt("in_reply_to", "string"),
                ],
            ),
            descriptor(
                "a2a.inbox",
                "Read unacknowledged messages for a session. Defaults to the caller's \
                 own session; pass `session` to read another agent's inbox (operator \
                 use). Returns newest-first, capped at `limit` (default 10).",
                &[
                    opt("session", "string"),
                    opt("limit", "integer"),
                ],
            ),
            descriptor(
                "a2a.ack",
                "Acknowledge a message by id so it no longer appears in the inbox. \
                 Idempotent: acknowledging an already-acked message is a no-op.",
                &[req("id", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "a2a.delegate" => {
                // A7 (gtcore-3a3557): a `peer` arg routes the hop straight to that
                // rig's own A2A endpoint over direct HTTP — orchd never relays.
                // Everything below is the in-process intake path (no `peer`).
                if let Some(peer) = ctx
                    .args
                    .get("peer")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return self.delegate_peer(peer, &ctx).await;
                }

                let title = ctx
                    .args
                    .get("title")
                    .and_then(Value::as_str)
                    .ok_or_else(|| AppError::Validation("`title` is required".into()))?
                    .to_string();
                let description = ctx
                    .args
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(&title)
                    .to_string();
                let rig = ctx
                    .args
                    .get("rig")
                    .and_then(Value::as_str)
                    .unwrap_or(&self.default_rig)
                    .to_string();

                // B4 (gtcore-3f3c57): cross-workspace delegation. `origin` is the
                // caller's authoritative tenant (resolved by the server's leak gate,
                // so it cannot be spoofed); `dest` is the optional `workspace` arg.
                // A `dest` other than `origin` mints the bead in that tenant's
                // tracker — gated by an explicit grant.
                let origin = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE);

                // A5 (gtcore-f3a016): rig-level RBAC. When rig grants are wired, an agent may
                // only delegate work onto a rig its origin holds an explicit `origin -> rig`
                // grant for. Deny-by-default among the configured rules: an ungranted rig is
                // rejected BEFORE any bead is minted and audited with the origin identity + the
                // rig as destination (acceptance: "agente sin permiso RBAC para un rig recibe
                // rechazo; el audit lo registra con identidad origen"). Unconfigured ⇒ no gate.
                if let Some(grants) = &self.rig_grants {
                    if !grants.allows_target(origin, &rig) {
                        let reason = format!(
                            "delegation from `{origin}` onto rig `{rig}` is not granted \
                             (set GT_A2A_RIG_GRANTS)"
                        );
                        self.audit(
                            ctx.workspace,
                            DelegationEvent::Denied {
                                origin: ctx.actor.to_string(),
                                dest: rig.clone(),
                                peer_url: None,
                                reason: reason.clone(),
                                at: rfc3339_now(),
                            },
                        );
                        return Err(AppError::Validation(reason));
                    }
                }

                let dest = ctx
                    .args
                    .get("workspace")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(origin)
                    .to_string();
                let cross_ws = dest != origin;

                // parent_id lineage: a same-workspace delegation keeps the bead-local
                // intake epic; a cross-workspace one stamps `<origin_ws>:<parent>` so
                // the child — living in the destination tenant — references the origin
                // it was delegated from (B4 acceptance: "parent_id referencia al origen").
                let parent_arg = ctx
                    .args
                    .get("parent_id")
                    .and_then(Value::as_str)
                    .unwrap_or(&self.default_parent);
                let parent_id = if cross_ws {
                    format!("{origin}:{parent_arg}")
                } else {
                    parent_arg.to_string()
                };

                // Resolve the tracker the bead is minted in. Same-workspace uses the
                // wired default store unchanged; cross-workspace requires BOTH an
                // explicit `origin->dest` grant AND a [`WorkspaceStores`] resolver,
                // else the call is rejected (deny-by-default).
                let dest_store: DoltIssues;
                let store: &DoltIssues = if cross_ws {
                    if !self.cross_ws_grants.allows(origin, &dest) {
                        return Err(AppError::Validation(format!(
                            "cross-workspace delegation from `{origin}` into `{dest}` is not \
                             granted (set GT_A2A_CROSS_WS_GRANTS)"
                        )));
                    }
                    let stores = self.stores.as_ref().ok_or_else(|| {
                        AppError::Validation(
                            "cross-workspace delegation is not configured on this server".into(),
                        )
                    })?;
                    dest_store = stores
                        .store_for(&dest)
                        .await
                        .map_err(|e| AppError::Other(format!("destination workspace `{dest}`: {e}")))?;
                    &dest_store
                } else {
                    &*self.store
                };

                let priority = ctx
                    .args
                    .get("priority")
                    .and_then(Value::as_u64)
                    .map(|p| p.min(2) as u8)
                    .unwrap_or(1);
                // B5: per-delegation completion timeout (0 disables). Defaults to
                // the deploy default when the caller omits it.
                let timeout_secs = ctx
                    .args
                    .get("timeout_secs")
                    .and_then(Value::as_u64)
                    .unwrap_or(self.default_timeout_secs);

                let args = CreateIssue {
                    id: None,
                    rig: rig.clone(),
                    title,
                    description,
                    design: String::new(),
                    acceptance_criteria: String::new(),
                    notes: String::new(),
                    priority,
                    issue_type: IssueType::Task,
                    // A2 attribution (Fase 2): stamp the delegating agent's MCP
                    // session id so the bead is traceable to the specific agent.
                    created_by: ctx.actor.to_string(),
                    parent_id: Some(parent_id),
                    assignee: None,
                    owner: None,
                    // Beads minted via delegation carry no own dispatch policy —
                    // they inherit the intake epic's (same as the HTTP A2A path).
                    // H2 (gtcore-d81e77): the wire `domain` is now free strings
                    // validated against the workspace catalog. `meta.gap` is
                    // reserved in every catalog, so delegation gap beads validate.
                    domain: vec![Domain::MetaGap.as_str().to_string()],
                    surface: Vec::new(),
                    depends_on: Vec::new(),
                    role_scope: None,
                    phase: None,
                    // Tenant isolation is the `store` (`hq_<dest>` on a cross-ws hop,
                    // the default store otherwise) — NOT this column, which is the
                    // intra-DB board scope. Left empty so the card lands on the
                    // destination tenant's default board, exactly like a normal
                    // `issues.create` in that tenant (which never stamps the slug here).
                    workspace: String::new(),
                    dispatch: None,
                };

                let bead = run_create_issue(store, &args, &NoSurfaces, false)
                    .await
                    .map_err(|e| AppError::Validation(e.to_string()))?;

                // The dispatch payload carries the destination workspace on a
                // cross-workspace delegation so the scheduler/sling can route the
                // bead to the right tenant; same-workspace stays byte-identical.
                let payload = if cross_ws {
                    json!({"bead": bead, "priority": priority, "workspace": dest})
                } else {
                    json!({"bead": bead, "priority": priority})
                }
                .to_string();
                self.sink
                    .emit(payload.as_bytes())
                    .map_err(|e| AppError::Validation(format!("dispatch: {e}")))?;

                // B5: register the delegation so the daemon pushes the outcome
                // back to the parent instead of the parent polling a2a.status.
                // Best-effort — the bead is already minted + dispatched, so a
                // tracking-log failure must not fail the delegation. The tracking
                // event lands in the log of the workspace the bead LIVES in (the
                // destination on a cross-workspace hop), where its terminal
                // merge/agent facts (B5) will also appear.
                let bead_ws = if cross_ws { Some(dest.as_str()) } else { ctx.workspace };
                if let Some(log) = &self.delegation_log {
                    if let Err(e) = log.append(
                        bead_ws,
                        DelegationEvent::Requested {
                            child: bead.clone(),
                            parent: ctx.actor.to_string(),
                            rig: rig.clone(),
                            at: rfc3339_now(),
                            timeout_secs,
                        },
                    ) {
                        eprintln!("[a2a.delegate] delegation tracking append failed for {bead}: {e}");
                    }
                }

                Ok(json!({
                    "id": bead,
                    "rig": rig,
                    "status": "submitted",
                    "timeout_secs": timeout_secs,
                    // B4: echo where the bead landed so a cross-workspace caller can
                    // confirm the destination tenant and the origin lineage.
                    "workspace": dest,
                    "cross_workspace": cross_ws,
                    "parent_id": args.parent_id,
                }))
            }

            // A7 (gtcore-3a3557): peer discovery — list rig capabilities as Agent
            // Card skills so an agent can choose whom to delegate to.
            "a2a.discover" => {
                let tag_filter = ctx.args.get("tag").and_then(Value::as_str);

                // B4 (gtcore-3f3c57): optional cross-workspace discovery. Default to
                // the caller's own tenant; a `workspace` arg naming another tenant is
                // honoured only when the caller holds an explicit delegation grant for
                // it, mirroring `a2a.delegate`'s gate (deny-by-default).
                let origin = ctx.workspace.unwrap_or(DEFAULT_WORKSPACE);
                let target = ctx
                    .args
                    .get("workspace")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(origin);
                if target != origin && !self.cross_ws_grants.allows(origin, target) {
                    return Err(AppError::Validation(format!(
                        "cross-workspace discovery of `{target}` from `{origin}` is not granted \
                         (set GT_A2A_CROSS_WS_GRANTS)"
                    )));
                }

                let skills: Vec<Value> = match &self.pools {
                    Some(pools) => {
                        let pool = pools.get(Some(target)).await?;
                        let repo = PgRigs::new(pool.pool().clone());
                        let rigs = repo.list().await.map_err(|e| {
                            AppError::Other(format!("rig catalog: {e}"))
                        })?;
                        // B3 (gtcore-1caa48): normalise the filter the same way tags are stored
                        // (lowercase) so `tag=Rust` matches a rig tagged `rust`.
                        let tag_filter = tag_filter.map(|t| t.trim().to_ascii_lowercase());
                        rigs.iter()
                            .map(|r| AgentSkill {
                                id: r.name.clone(),
                                name: r.name.clone(),
                                description: Some(if r.git_url.is_empty() {
                                    format!("dispatch work onto rig `{}`", r.name)
                                } else {
                                    format!(
                                        "dispatch work onto rig `{}` ({}; default branch {})",
                                        r.name, r.git_url, r.default_branch
                                    )
                                }),
                                tags: crate::a2a::skill_tags(r),
                            })
                            .filter(|s| match &tag_filter {
                                // Match on the bead prefix + any semantic tag (B3), or the rig
                                // name itself; the merged `tags` already carries prefix first.
                                Some(tag) => s.tags.iter().any(|t| t == tag) || s.id == *tag,
                                None => true,
                            })
                            .map(|s| json!({
                                "id": s.id,
                                "name": s.name,
                                "description": s.description,
                                "tags": s.tags,
                            }))
                            .collect()
                    }
                    None => Vec::new(),
                };

                Ok(json!({ "skills": skills, "workspace": target }))
            }

            // A7 (gtcore-3a3557): delegation status — poll a delegated bead's
            // tracker state so the parent agent knows when the child finishes.
            "a2a.status" => {
                let id = ctx
                    .args
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| AppError::Validation("`id` is required".into()))?;

                match self.store.get_detail(id).await {
                    Ok(Some(detail)) => Ok(json!({
                        "id": detail.id,
                        "title": detail.title,
                        "status": detail.status,
                        "assignee": detail.assignee,
                        "created_at": detail.created_at,
                        "updated_at": detail.updated_at,
                        "closed_at": detail.closed_at,
                    })),
                    Ok(None) => Err(AppError::NotFound(format!("bead {id}"))),
                    Err(e) => Err(AppError::Other(format!("tracker: {e}"))),
                }
            }

            // Inter-agent messaging: send/inbox/ack. Delegated to the
            // A2aMessageHandler when an event log is wired.
            "a2a.send" | "a2a.inbox" | "a2a.ack" | "a2a.threads" => {
                let msg = self.msg.as_ref().ok_or_else(|| {
                    AppError::Validation(
                        "a2a messaging not available (event log not wired)".into(),
                    )
                })?;
                msg.dispatch(tool, ctx).await
            }

            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}
