//! `A2aGateway` — the up-tier implementation of [`gt_a2a::A2aHandler`]
//! (gtcore-c7bbef, epic gtcore-155917).
//!
//! `gt-a2a` owns the protocol contract; this module owns the cross-domain
//! integration that contract cannot name (docs/03 Rule 4): an inbound A2A
//! `tasks/send` becomes a real bead (`gt-issues`) and a dispatch order on the
//! orchd channel — the SAME ingestion path the MCP `issues.create` tool and the
//! `.event` file watcher already drive, so A2A adds an ingress, not a second
//! work pipeline.
//!
//! ## Ports, not adapters
//!
//! The gateway depends on two seams, mirroring `gt-runtime`'s
//! `ReadySource`/`Worker` split so the policy is testable with fakes:
//!
//! - [`BeadIntake`] — *what creating the bead means*. Production:
//!   [`DoltIntake`] over `run_create_issue` (the minted bead id comes back —
//!   it IS the A2A task id, decision #2 of the plan).
//! - [`DispatchSink`] — *what dispatching means*. Production:
//!   [`ChannelDispatch`], the `{bead,priority}` tmp→rename drop on the orchd
//!   channel dir (mirrors `gt-agent`'s auto-dispatch bridge).
//! - [`TaskStore`] — *reading and terminating the bead* (gtcore-f5f7c7, B4).
//!   Production: [`DoltTaskStore`] over `get_detail` + `run_transition_issue`.
//! - [`SessionControl`] — *observing and killing the live session*. Production:
//!   [`RestSessionControl`] against the orchd agent REST surface — tmux lives
//!   in the orchd pod, so the kill MUST go through `POST /:id/kill` there (the
//!   only place that both records `agent.killed.v1` and reaps the process).
//!
//! ## State projection (decision #2: the event log stays the source of truth)
//!
//! `tasks/get` projects, never stores: bead `closed` → `completed`; a `killed`
//! session over a non-closed bead → `canceled` (exactly the residue `cancel`
//! leaves, so a follow-up `get` agrees with it); otherwise
//! [`TaskState::from_bead_status`]. `failed` is a merge fact and lands with the
//! B3 stream — this projection never invents it.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_a2a::{
    A2aError, A2aHandler, JsonRpcError, Part, Task, TaskIdParams, TaskSendParams, TaskState,
    TaskStatus,
};
use gt_issues::handlers::{run_create_issue, run_transition_issue};
use gt_issues::{CreateIssue, Domain, IssueType, SurfaceTree, TransitionIssue};
use gt_store_dolt::DoltIssues;

/// What an inbound A2A task asks the tracker to mint. Already shaped by the
/// gateway (title/description split, rig and parent resolved), so an intake
/// implementation only persists.
#[derive(Clone, Debug, PartialEq)]
pub struct IntakeRequest {
    pub rig: String,
    pub parent_id: String,
    pub title: String,
    pub description: String,
}

/// Port: persist the bead and return the minted id.
#[async_trait]
pub trait BeadIntake: Send + Sync {
    async fn create(&self, req: IntakeRequest) -> Result<String, String>;
}

/// Port: enqueue the bead for the scheduler.
pub trait DispatchSink: Send + Sync {
    fn dispatch(&self, bead: &str, priority: u8) -> Result<(), String>;
}

/// What `tasks/get`/`tasks/cancel` need to know about a bead: its tracker
/// status drives the state projection, its rig names the session
/// (`<rig>-<bead>`, the polecat sling convention).
#[derive(Clone, Debug, PartialEq)]
pub struct BeadSnapshot {
    pub status: String,
    pub rig: String,
}

/// Port: read a bead and flip it terminal on cancel.
#[async_trait]
pub trait TaskStore: Send + Sync {
    /// `None` ⇒ no such bead (`-32001 TaskNotFound` at the protocol edge).
    async fn fetch(&self, bead: &str) -> Result<Option<BeadSnapshot>, String>;
    /// Mark the bead terminal after a cancel, leaving `reason` as the audit
    /// breadcrumb.
    async fn terminate(&self, bead: &str, reason: &str) -> Result<(), String>;
}

/// Port: observe and kill the live session a bead runs in.
#[async_trait]
pub trait SessionControl: Send + Sync {
    /// Registry state (`spawned`/`working`/`done`/`killed`), `None` when the
    /// bead was never slung.
    async fn state(&self, session: &str) -> Result<Option<String>, String>;
    async fn kill(&self, session: &str, reason: &str) -> Result<(), String>;
}

/// Deploy-fixed defaults an A2A task does not carry on the wire.
#[derive(Clone, Debug)]
pub struct A2aGatewayConfig {
    /// Default rig for minted beads; `metadata.rig` overrides per task.
    pub rig: String,
    /// The intake epic minted beads hang off (`child_of`, NN-16 requires a
    /// parent for non-epics); `metadata.parent_id` overrides per task.
    pub parent_id: String,
    /// Attribution for `created_by` (e.g. `"a2a"`).
    pub created_by: String,
    /// Taxonomy stamped on minted beads (a bead must carry ≥1 domain).
    pub domain: Vec<Domain>,
}

/// The A2A operations server: `tasks/send` → bead + dispatch; `tasks/get` →
/// projection; `tasks/cancel` → session kill + terminal bead.
pub struct A2aGateway {
    intake: Arc<dyn BeadIntake>,
    sink: Arc<dyn DispatchSink>,
    store: Arc<dyn TaskStore>,
    sessions: Arc<dyn SessionControl>,
    config: A2aGatewayConfig,
}

impl A2aGateway {
    pub fn new(
        intake: Arc<dyn BeadIntake>,
        sink: Arc<dyn DispatchSink>,
        store: Arc<dyn TaskStore>,
        sessions: Arc<dyn SessionControl>,
        config: A2aGatewayConfig,
    ) -> Self {
        Self { intake, sink, store, sessions, config }
    }

    /// Bead + session facts for `id`, or `-32001` when the bead does not exist.
    async fn snapshot(&self, id: &str) -> Result<(BeadSnapshot, Option<String>), A2aError> {
        let bead = self
            .store
            .fetch(id)
            .await
            .map_err(|e| A2aError::from(JsonRpcError::internal(format!("bead fetch: {e}"))))?
            .ok_or_else(|| A2aError::from(JsonRpcError::task_not_found(id)))?;
        let session = self
            .sessions
            .state(&session_of(&bead.rig, id))
            .await
            .map_err(|e| A2aError::from(JsonRpcError::internal(format!("session state: {e}"))))?;
        Ok((bead, session))
    }
}

/// The polecat sling convention: a bead's session is `<rig>-<bead id>`.
fn session_of(rig: &str, bead: &str) -> String {
    format!("{rig}-{bead}")
}

/// Bead status + session state → task state. Conservative: only facts the
/// tracker/registry actually hold (see the module doc).
fn project_state(bead_status: &str, session_state: Option<&str>) -> TaskState {
    if bead_status == "closed" {
        return TaskState::Completed;
    }
    if session_state == Some("killed") {
        return TaskState::Canceled;
    }
    TaskState::from_bead_status(bead_status)
}

/// RFC3339 "now", stamped here because the contract crate is clock-free.
fn rfc3339_now() -> Option<String> {
    OffsetDateTime::now_utc().format(&Rfc3339).ok()
}

#[async_trait]
impl A2aHandler for A2aGateway {
    async fn send(&self, params: TaskSendParams) -> Result<Task, A2aError> {
        let text = first_text(&params).ok_or_else(|| {
            A2aError::from(JsonRpcError::invalid_params(
                "message must carry at least one non-empty text part",
            ))
        })?;
        let (title, description) = split_title(text);
        let meta = |key: &str| {
            params
                .metadata
                .as_ref()
                .and_then(|m| m.get(key))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        let req = IntakeRequest {
            rig: meta("rig").unwrap_or_else(|| self.config.rig.clone()),
            parent_id: meta("parent_id").unwrap_or_else(|| self.config.parent_id.clone()),
            title,
            description,
        };
        let priority = params
            .metadata
            .as_ref()
            .and_then(|m| m.get("priority"))
            .and_then(|v| v.as_u64())
            .map(|p| p.min(2) as u8)
            .unwrap_or(1);

        let bead = self
            .intake
            .create(req)
            .await
            .map_err(|e| A2aError::from(JsonRpcError::internal(format!("bead intake: {e}"))))?;
        self.sink
            .dispatch(&bead, priority)
            .map_err(|e| A2aError::from(JsonRpcError::internal(format!("dispatch: {e}"))))?;

        Ok(Task {
            id: bead,
            session_id: params.session_id,
            status: TaskStatus { state: TaskState::Submitted, timestamp: rfc3339_now(), message: None },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }

    async fn get(&self, params: TaskIdParams) -> Result<Task, A2aError> {
        let (bead, session) = self.snapshot(&params.id).await?;
        Ok(Task {
            id: params.id,
            session_id: None,
            status: TaskStatus {
                state: project_state(&bead.status, session.as_deref()),
                timestamp: rfc3339_now(),
                message: None,
            },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }

    async fn cancel(&self, params: TaskIdParams) -> Result<Task, A2aError> {
        let (bead, session) = self.snapshot(&params.id).await?;
        if project_state(&bead.status, session.as_deref()).is_terminal() {
            return Err(JsonRpcError::task_not_cancelable(&params.id).into());
        }
        // Reap the live process first — terminating the bead while the polecat
        // keeps running would leave work charging ahead on a canceled task. A
        // session that is absent or already done has nothing to kill.
        const REASON: &str = "canceled via A2A tasks/cancel";
        if matches!(session.as_deref(), Some("spawned") | Some("working")) {
            self.sessions
                .kill(&session_of(&bead.rig, &params.id), REASON)
                .await
                .map_err(|e| A2aError::from(JsonRpcError::internal(format!("session kill: {e}"))))?;
        }
        self.store
            .terminate(&params.id, REASON)
            .await
            .map_err(|e| A2aError::from(JsonRpcError::internal(format!("bead terminate: {e}"))))?;
        Ok(Task {
            id: params.id,
            session_id: None,
            status: TaskStatus { state: TaskState::Canceled, timestamp: rfc3339_now(), message: None },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }
}

/// First non-empty text part of the message, trimmed.
fn first_text(params: &TaskSendParams) -> Option<&str> {
    params.message.parts.iter().find_map(|p| match p {
        Part::Text { text, .. } => {
            let t = text.trim();
            (!t.is_empty()).then_some(t)
        }
    })
}

/// First line (≤72 chars, word-clipped) as the bead title; the full text as the
/// description so nothing the client sent is lost.
fn split_title(text: &str) -> (String, String) {
    let first_line = text.lines().next().unwrap_or(text).trim();
    let title = if first_line.chars().count() <= 72 {
        first_line.to_string()
    } else {
        let clipped: String = first_line.chars().take(69).collect();
        let cut = clipped.rfind(' ').unwrap_or(clipped.len());
        format!("{}…", &clipped[..cut])
    };
    (title, text.to_string())
}

// ---------------------------------------------------------------------------
// Production adapters
// ---------------------------------------------------------------------------

/// Intake beads carry no surface, so existence checks are vacuous; this tree
/// satisfies the handler signature without a git read per A2A call.
struct NoSurfaces;
impl SurfaceTree for NoSurfaces {
    fn contains(&self, _path: &str) -> bool {
        true
    }
}

/// [`BeadIntake`] over the Dolt store — the same `run_create_issue` handler the
/// MCP `issues.create.execute` tool drives, so A2A-minted beads get identical
/// validation (NN-16 parent, closed-set domain) and the atomic Dolt commit.
pub struct DoltIntake {
    issues: Arc<DoltIssues>,
    created_by: String,
    domain: Vec<Domain>,
}

impl DoltIntake {
    pub fn new(issues: Arc<DoltIssues>, created_by: String, domain: Vec<Domain>) -> Self {
        Self { issues, created_by, domain }
    }
}

#[async_trait]
impl BeadIntake for DoltIntake {
    async fn create(&self, req: IntakeRequest) -> Result<String, String> {
        let args = CreateIssue {
            id: None,
            rig: req.rig,
            title: req.title,
            description: req.description,
            design: String::new(),
            acceptance_criteria: String::new(),
            notes: String::new(),
            priority: 2,
            issue_type: IssueType::Task,
            created_by: self.created_by.clone(),
            parent_id: Some(req.parent_id),
            assignee: None,
            owner: None,
            domain: self.domain.clone(),
            surface: Vec::new(),
            depends_on: Vec::new(),
            role_scope: None,
            phase: None,
            workspace: String::new(),
        };
        run_create_issue(&self.issues, &args, &NoSurfaces, false)
            .await
            .map_err(|e| e.to_string())
    }
}

/// [`DispatchSink`] onto the orchd channel dir: atomic write (tmp → rename) of
/// the `{bead,priority}` order, the exact shape the scheduler's watcher consumes
/// — mirrors `gt-agent`'s auto-dispatch bridge (hq-agent-auto-dispatch.1).
pub struct ChannelDispatch {
    channel: PathBuf,
}

impl ChannelDispatch {
    pub fn new(channel: PathBuf) -> Self {
        Self { channel }
    }
}

/// [`TaskStore`] over the Dolt store: `fetch` is the `gt://issue/{id}` detail
/// read; `terminate` leaves the audit breadcrumb and flips the bead `closed`
/// through `run_transition_issue` — the same frontier the MCP
/// `issues.transition` tool drives (a canceled bead has no delivered sha, so
/// `issues.close`'s code-surface gate is the wrong door).
pub struct DoltTaskStore {
    issues: Arc<DoltIssues>,
}

impl DoltTaskStore {
    pub fn new(issues: Arc<DoltIssues>) -> Self {
        Self { issues }
    }
}

#[async_trait]
impl TaskStore for DoltTaskStore {
    async fn fetch(&self, bead: &str) -> Result<Option<BeadSnapshot>, String> {
        self.issues
            .get_detail(bead)
            .await
            .map(|d| d.map(|d| BeadSnapshot { status: d.status, rig: d.rig }))
            .map_err(|e| e.to_string())
    }

    async fn terminate(&self, bead: &str, reason: &str) -> Result<(), String> {
        self.issues.append_notes(bead, reason).await.map_err(|e| e.to_string())?;
        let args = TransitionIssue { id: bead.into(), target: "closed".into(), context: None };
        run_transition_issue(&self.issues, &args, false, false)
            .await
            .map_err(|e| e.to_string())
    }
}

/// [`SessionControl`] against the orchd agent REST surface
/// (`/api/v1/agent/...`): the tmux server lives in the orchd pod, and its
/// `POST /:id/kill` is the one place that both records `agent.killed.v1` and
/// reaps the process — killing from here would record without reaping.
pub struct RestSessionControl {
    /// Origin of the orchd REST surface, e.g. `http://gt-orchd:8080`.
    base: String,
    /// Bearer for the `agent.read`/`agent.write` scope guard.
    token: Option<String>,
    client: reqwest::Client,
}

impl RestSessionControl {
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self { base: base.into(), token, client: reqwest::Client::new() }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self.client.request(method, format!("{}{path}", self.base));
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }
}

#[async_trait]
impl SessionControl for RestSessionControl {
    async fn state(&self, session: &str) -> Result<Option<String>, String> {
        let resp = self
            .request(reqwest::Method::GET, &format!("/api/v1/agent/{session}"))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body: serde_json::Value =
            resp.error_for_status().map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?;
        Ok(body.get("state").and_then(|s| s.as_str()).map(str::to_owned))
    }

    async fn kill(&self, session: &str, reason: &str) -> Result<(), String> {
        self.request(reqwest::Method::POST, &format!("/api/v1/agent/{session}/kill"))
            .json(&serde_json::json!({ "reason": reason }))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

impl DispatchSink for ChannelDispatch {
    fn dispatch(&self, bead: &str, priority: u8) -> Result<(), String> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let id = format!("{ts:016x}{:08x}{seq:08x}", std::process::id());
        let tmp = self.channel.join(format!(".{id}.tmp"));
        let final_ = self.channel.join(format!("{id}.event"));
        let payload = serde_json::json!({"bead": bead, "priority": priority}).to_string();
        std::fs::create_dir_all(&self.channel)
            .and_then(|_| std::fs::write(&tmp, payload.as_bytes()))
            .and_then(|_| std::fs::rename(&tmp, &final_))
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_a2a::{Message, Role};
    use std::sync::Mutex;

    struct FakeIntake {
        seen: Mutex<Vec<IntakeRequest>>,
        fail: bool,
    }

    #[async_trait]
    impl BeadIntake for FakeIntake {
        async fn create(&self, req: IntakeRequest) -> Result<String, String> {
            if self.fail {
                return Err("dolt unavailable".into());
            }
            self.seen.lock().unwrap().push(req);
            Ok("gtcore-abc123".into())
        }
    }

    struct FakeSink(Mutex<Vec<(String, u8)>>);

    impl DispatchSink for FakeSink {
        fn dispatch(&self, bead: &str, priority: u8) -> Result<(), String> {
            self.0.lock().unwrap().push((bead.into(), priority));
            Ok(())
        }
    }

    /// Tracker fake: a fixed snapshot (None ⇒ unknown bead) + terminate trail.
    #[derive(Default)]
    struct FakeStore {
        snapshot: Option<BeadSnapshot>,
        terminated: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl TaskStore for FakeStore {
        async fn fetch(&self, _bead: &str) -> Result<Option<BeadSnapshot>, String> {
            Ok(self.snapshot.clone())
        }
        async fn terminate(&self, bead: &str, reason: &str) -> Result<(), String> {
            self.terminated.lock().unwrap().push((bead.into(), reason.into()));
            Ok(())
        }
    }

    /// Session fake: a fixed registry state (None ⇒ never slung) + kill trail.
    #[derive(Default)]
    struct FakeSessions {
        state: Option<String>,
        killed: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl SessionControl for FakeSessions {
        async fn state(&self, _session: &str) -> Result<Option<String>, String> {
            Ok(self.state.clone())
        }
        async fn kill(&self, session: &str, reason: &str) -> Result<(), String> {
            self.killed.lock().unwrap().push((session.into(), reason.into()));
            Ok(())
        }
    }

    fn config() -> A2aGatewayConfig {
        A2aGatewayConfig {
            rig: "gtcore".into(),
            parent_id: "gtcore-intake".into(),
            created_by: "a2a".into(),
            domain: vec![Domain::MetaGap],
        }
    }

    fn send_params(text: &str) -> TaskSendParams {
        TaskSendParams {
            id: "client-1".into(),
            session_id: None,
            message: Message {
                role: Role::User,
                parts: vec![Part::Text { text: text.into(), metadata: None }],
                metadata: None,
            },
            history_length: None,
            metadata: None,
        }
    }

    fn gateway(fail: bool) -> (A2aGateway, Arc<FakeIntake>, Arc<FakeSink>) {
        let intake = Arc::new(FakeIntake { seen: Mutex::new(vec![]), fail });
        let sink = Arc::new(FakeSink(Mutex::new(vec![])));
        let gw = A2aGateway::new(
            intake.clone(),
            sink.clone(),
            Arc::new(FakeStore::default()),
            Arc::new(FakeSessions::default()),
            config(),
        );
        (gw, intake, sink)
    }

    /// Gateway wired for the get/cancel paths: bead status + session state fakes.
    fn projection_gateway(
        bead: Option<(&str, &str)>,
        session: Option<&str>,
    ) -> (A2aGateway, Arc<FakeStore>, Arc<FakeSessions>) {
        let store = Arc::new(FakeStore {
            snapshot: bead.map(|(status, rig)| BeadSnapshot {
                status: status.into(),
                rig: rig.into(),
            }),
            terminated: Mutex::new(vec![]),
        });
        let sessions = Arc::new(FakeSessions {
            state: session.map(str::to_owned),
            killed: Mutex::new(vec![]),
        });
        let gw = A2aGateway::new(
            Arc::new(FakeIntake { seen: Mutex::new(vec![]), fail: false }),
            Arc::new(FakeSink(Mutex::new(vec![]))),
            store.clone(),
            sessions.clone(),
            config(),
        );
        (gw, store, sessions)
    }

    fn id_params(id: &str) -> TaskIdParams {
        TaskIdParams { id: id.into(), history_length: None, metadata: None }
    }

    #[tokio::test]
    async fn send_mints_bead_dispatches_and_answers_submitted() {
        let (gw, intake, sink) = gateway(false);
        let task = gw
            .send(send_params("fix the flaky login test\n\nIt fails on CI only."))
            .await
            .unwrap();
        assert_eq!(task.id, "gtcore-abc123");
        assert_eq!(task.status.state, TaskState::Submitted);
        // RFC3339 timestamp stamped here (the contract crate is clock-free).
        let ts = task.status.timestamp.expect("timestamp");
        assert!(OffsetDateTime::parse(&ts, &Rfc3339).is_ok(), "bad RFC3339: {ts}");

        let seen = intake.seen.lock().unwrap();
        assert_eq!(seen[0].title, "fix the flaky login test");
        assert!(seen[0].description.contains("CI only"));
        assert_eq!(seen[0].rig, "gtcore");
        assert_eq!(seen[0].parent_id, "gtcore-intake");
        assert_eq!(sink.0.lock().unwrap()[0], ("gtcore-abc123".into(), 1));
    }

    #[tokio::test]
    async fn metadata_overrides_rig_parent_and_priority() {
        let (gw, intake, sink) = gateway(false);
        let mut p = send_params("port the navbar");
        p.metadata = Some(serde_json::json!({
            "rig": "gtweb", "parent_id": "gtweb-epic1", "priority": 0
        }));
        gw.send(p).await.unwrap();
        let seen = intake.seen.lock().unwrap();
        assert_eq!(seen[0].rig, "gtweb");
        assert_eq!(seen[0].parent_id, "gtweb-epic1");
        assert_eq!(sink.0.lock().unwrap()[0].1, 0);
    }

    #[tokio::test]
    async fn empty_message_is_invalid_params() {
        let (gw, _, _) = gateway(false);
        let err = gw.send(send_params("   ")).await.unwrap_err();
        assert_eq!(err.0.code, -32602);
    }

    #[tokio::test]
    async fn intake_failure_is_internal_not_panic() {
        let (gw, _, sink) = gateway(true);
        let err = gw.send(send_params("do the thing")).await.unwrap_err();
        assert_eq!(err.0.code, -32603);
        // No dispatch order for a bead that was never minted.
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_unknown_bead_is_task_not_found() {
        let (gw, _, _) = projection_gateway(None, None);
        assert_eq!(gw.get(id_params("gtcore-nope")).await.unwrap_err().0.code, -32001);
    }

    #[tokio::test]
    async fn get_projects_bead_and_session_state() {
        for (bead, session, want) in [
            (("open", "gtcore"), None, TaskState::Submitted),
            (("working", "gtcore"), Some("working"), TaskState::Working),
            (("closed", "gtcore"), Some("done"), TaskState::Completed),
            // The residue cancel leaves: live bead, killed session.
            (("working", "gtcore"), Some("killed"), TaskState::Canceled),
        ] {
            let (gw, _, _) = projection_gateway(Some(bead), session);
            let task = gw.get(id_params("gtcore-abc123")).await.unwrap();
            assert_eq!(task.status.state, want, "bead {bead:?} session {session:?}");
            assert!(task.status.timestamp.is_some());
        }
    }

    #[tokio::test]
    async fn cancel_kills_live_session_and_terminates_bead() {
        let (gw, store, sessions) = projection_gateway(Some(("working", "gtweb")), Some("working"));
        let task = gw.cancel(id_params("gtweb-abc123")).await.unwrap();
        assert_eq!(task.status.state, TaskState::Canceled);
        // The session is named by the sling convention and reaped first.
        let killed = sessions.killed.lock().unwrap();
        assert_eq!(killed[0].0, "gtweb-gtweb-abc123");
        let terminated = store.terminated.lock().unwrap();
        assert_eq!(terminated[0].0, "gtweb-abc123");
        assert!(terminated[0].1.contains("canceled via A2A"));
    }

    #[tokio::test]
    async fn cancel_never_slung_bead_skips_kill_but_terminates() {
        let (gw, store, sessions) = projection_gateway(Some(("open", "gtcore")), None);
        let task = gw.cancel(id_params("gtcore-abc123")).await.unwrap();
        assert_eq!(task.status.state, TaskState::Canceled);
        assert!(sessions.killed.lock().unwrap().is_empty(), "nothing to kill");
        assert_eq!(store.terminated.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancel_terminal_task_is_not_cancelable() {
        // Closed bead and already-killed session are both terminal.
        for (bead, session) in [(("closed", "gtcore"), None), (("working", "gtcore"), Some("killed"))] {
            let (gw, store, sessions) = projection_gateway(Some(bead), session);
            let err = gw.cancel(id_params("gtcore-abc123")).await.unwrap_err();
            assert_eq!(err.0.code, -32002, "bead {bead:?} session {session:?}");
            assert!(sessions.killed.lock().unwrap().is_empty());
            assert!(store.terminated.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn cancel_unknown_bead_is_task_not_found() {
        let (gw, _, _) = projection_gateway(None, None);
        assert_eq!(gw.cancel(id_params("gtcore-nope")).await.unwrap_err().0.code, -32001);
    }

    #[test]
    fn long_first_line_is_word_clipped_with_full_description() {
        let long = "a ".repeat(60) + "tail";
        let (title, desc) = split_title(&long);
        assert!(title.chars().count() <= 72);
        assert!(title.ends_with('…'));
        assert_eq!(desc, long);
    }

    #[test]
    fn channel_dispatch_writes_atomic_event_file() {
        let dir = std::env::temp_dir().join(format!("a2a-disp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let sink = ChannelDispatch::new(dir.clone());
        sink.dispatch("gtcore-abc123", 1).unwrap();
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.path().extension().is_some_and(|x| x == "event"))
            .collect();
        assert_eq!(entries.len(), 1, "exactly one .event, no .tmp leftovers");
        let body: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(entries[0].path()).unwrap()).unwrap();
        assert_eq!(body["bead"], "gtcore-abc123");
        assert_eq!(body["priority"], 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
