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
//!
//! `tasks/get` / `tasks/cancel` answer `-32004 UnsupportedOperation` until
//! gtcore-f5f7c7 (B4) completes them.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_a2a::{
    A2aError, A2aHandler, JsonRpcError, Part, Task, TaskIdParams, TaskSendParams, TaskState,
    TaskStatus,
};
use gt_issues::handlers::run_create_issue;
use gt_issues::{CreateIssue, Domain, IssueType, SurfaceTree};
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

/// The A2A operations server: `tasks/send` → bead + dispatch.
pub struct A2aGateway {
    intake: Arc<dyn BeadIntake>,
    sink: Arc<dyn DispatchSink>,
    config: A2aGatewayConfig,
}

impl A2aGateway {
    pub fn new(
        intake: Arc<dyn BeadIntake>,
        sink: Arc<dyn DispatchSink>,
        config: A2aGatewayConfig,
    ) -> Self {
        Self { intake, sink, config }
    }
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
            status: TaskStatus {
                state: TaskState::Submitted,
                timestamp: OffsetDateTime::now_utc().format(&Rfc3339).ok(),
                message: None,
            },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }

    async fn get(&self, _params: TaskIdParams) -> Result<Task, A2aError> {
        Err(JsonRpcError::unsupported_operation("tasks/get lands with gtcore-f5f7c7 (B4)").into())
    }

    async fn cancel(&self, _params: TaskIdParams) -> Result<Task, A2aError> {
        Err(JsonRpcError::unsupported_operation("tasks/cancel lands with gtcore-f5f7c7 (B4)")
            .into())
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
        (A2aGateway::new(intake.clone(), sink.clone(), config()), intake, sink)
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
    async fn get_and_cancel_are_unsupported_until_b4() {
        let (gw, _, _) = gateway(false);
        let p = TaskIdParams { id: "gtcore-x".into(), history_length: None, metadata: None };
        assert_eq!(gw.get(p.clone()).await.unwrap_err().0.code, -32004);
        assert_eq!(gw.cancel(p).await.unwrap_err().0.code, -32004);
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
