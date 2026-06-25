//! Direct rig→rig (peer-to-peer) A2A delegation over real HTTP (A7, gtcore-3a3557
//! — epic gtcore-155917).
//!
//! The acceptance criterion is a P2P hop with **orchd not in the loop**: an agent
//! discovers a peer via its Agent Card and sends `tasks/send` straight to that
//! peer's `POST /a2a`. This test stands a REAL `gt_a2a::a2a_router` peer up on a
//! loopback `TcpListener` (so the wire is genuine HTTP, not an in-process tower
//! `oneshot`) and drives it through the production [`A2aPeerClient`]: discovery →
//! `tasks/send` → the peer-minted task id comes back. No orchd channel, no
//! dispatch sink, no shared process state sits between client and peer.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use gt_a2a::{
    a2a_router, A2aError, A2aHandler, AgentAuthentication, AgentCapabilities, AgentCard,
    AgentProvider, AgentSkill, EventStream, JsonRpcError, Part, StreamEvent, Task, TaskIdParams,
    TaskSendParams, TaskState, TaskStatus, TaskStatusUpdateEvent,
};
use gt_composition::delegation_http::{A2aPeerClient, PeerRegistry};

/// A peer whose `tasks/send` records the inbound request and mints a fixed bead
/// id — the stand-in for the destination rig's own A2A gateway. Recording lets
/// the test assert the message + metadata crossed the wire intact.
struct PeerAgent {
    received: Mutex<Vec<(String, Option<serde_json::Value>)>>,
}

#[async_trait]
impl A2aHandler for PeerAgent {
    async fn send(&self, params: TaskSendParams) -> Result<Task, A2aError> {
        let text = params
            .message
            .parts
            .iter()
            .find_map(|p| match p {
                Part::Text { text, .. } => Some(text.clone()),
            })
            .unwrap_or_default();
        self.received.lock().unwrap().push((text, params.metadata.clone()));
        Ok(Task {
            id: "gtweb-peer42".into(),
            session_id: None,
            status: TaskStatus {
                state: TaskState::Submitted,
                timestamp: None,
                message: None,
            },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }

    async fn get(&self, p: TaskIdParams) -> Result<Task, A2aError> {
        Ok(Task {
            id: p.id,
            session_id: None,
            status: TaskStatus { state: TaskState::Working, timestamp: None, message: None },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        })
    }

    async fn cancel(&self, _p: TaskIdParams) -> Result<Task, A2aError> {
        Err(JsonRpcError::task_not_cancelable("n/a").into())
    }

    async fn send_subscribe(&self, params: TaskSendParams) -> Result<EventStream, A2aError> {
        // Record the subscribe like a send, then play working → completed (final),
        // the shape a real peer's gateway streams off its event hub.
        let text = params
            .message
            .parts
            .iter()
            .find_map(|p| match p {
                Part::Text { text, .. } => Some(text.clone()),
            })
            .unwrap_or_default();
        self.received.lock().unwrap().push((text, params.metadata.clone()));
        let frame = |state: TaskState, is_final: bool| {
            StreamEvent::Status(TaskStatusUpdateEvent {
                id: "gtweb-peer42".into(),
                status: TaskStatus { state, timestamp: None, message: None },
                is_final,
                metadata: None,
            })
        };
        Ok(Box::pin(async_stream::stream! {
            yield frame(TaskState::Working, false);
            yield frame(TaskState::Completed, true);
        }))
    }
}

/// Build the peer's Agent Card pointing its JSON-RPC `url` at the bound address,
/// advertising one skill for the `gtweb` rig (so discovery is load-bearing — the
/// client checks the rig is advertised before sending).
fn peer_card(addr: SocketAddr) -> AgentCard {
    AgentCard {
        name: "gt-gtweb".into(),
        description: Some("peer rig".into()),
        url: format!("http://{addr}/a2a"),
        version: "0.1.0".into(),
        provider: Some(AgentProvider { organization: "gt-core-labs".into(), url: None }),
        capabilities: AgentCapabilities { streaming: true, ..Default::default() },
        authentication: Some(AgentAuthentication { schemes: vec!["bearer".into()] }),
        default_input_modes: vec!["text".into()],
        default_output_modes: vec!["text".into()],
        skills: vec![AgentSkill {
            id: "gtweb".into(),
            name: "gtweb".into(),
            description: Some("dispatch work onto rig `gtweb`".into()),
            tags: vec!["gtw".into(), "frontend".into()],
        }],
        signature: None,
    }
}

/// Spawn the peer router on an ephemeral loopback port; return its address + the
/// recording handle so the test can inspect what the peer received.
async fn spawn_peer() -> (SocketAddr, Arc<PeerAgent>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let agent = Arc::new(PeerAgent { received: Mutex::new(vec![]) });
    let app = a2a_router(peer_card(addr), agent.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, agent)
}

#[tokio::test]
async fn discover_then_send_completes_a_direct_peer_hop() {
    let (addr, peer) = spawn_peer().await;
    let base = format!("http://{addr}");

    // The registry resolves the configured peer name to its base URL — exactly the
    // resolution the a2a.delegate handler does before dialing.
    let registry = PeerRegistry::parse(&format!("gtweb={base}"));
    let resolved = registry.resolve("gtweb").expect("peer resolves");
    assert_eq!(resolved, base);

    let client = A2aPeerClient::new(None);

    // 1) Discovery via the Agent Card — over real HTTP to /.well-known/agent.json.
    let card = client.discover(&resolved).await.expect("discover peer card");
    assert_eq!(card.url, format!("{base}/a2a"));
    assert!(card.skills.iter().any(|s| s.id == "gtweb"), "peer advertises the gtweb skill");

    // 2) Direct tasks/send to the card's endpoint — no orchd, no dispatch channel.
    let meta = serde_json::json!({ "rig": "gtweb", "priority": 1, "caller": "gtcore-sess-1" });
    let task = client
        .send(&card.url, "a2a-default-123", "port the navbar\n\ndetails here", meta)
        .await
        .expect("peer tasks/send");

    // The PEER is authoritative for the id — the client gets the bead the peer minted.
    assert_eq!(task.id, "gtweb-peer42");
    assert_eq!(task.status.state, TaskState::Submitted);

    // The message + routing metadata crossed the wire intact.
    let received = peer.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(received[0].0.starts_with("port the navbar"));
    let md = received[0].1.as_ref().expect("metadata forwarded");
    assert_eq!(md["rig"], "gtweb");
    assert_eq!(md["caller"], "gtcore-sess-1");
}

#[tokio::test]
async fn discover_surfaces_a_transport_error_for_a_dead_peer() {
    // Nothing is listening here — the client must report the failure, not hang or
    // panic, so the handler can map it to a clean JSON-RPC error.
    let client = A2aPeerClient::new(None);
    let err = client.discover("http://127.0.0.1:1").await.unwrap_err();
    assert!(err.contains("discover"), "error names the discovery step: {err}");
}

#[tokio::test]
async fn send_propagates_a_peer_jsonrpc_error() {
    // A peer that rejects every tasks/send (cancel-style error) must surface as an
    // Err on the client, carrying the peer's code/message — never a silent success.
    struct Rejecting;
    #[async_trait]
    impl A2aHandler for Rejecting {
        async fn send(&self, _p: TaskSendParams) -> Result<Task, A2aError> {
            Err(JsonRpcError::invalid_params("nope").into())
        }
        async fn get(&self, _p: TaskIdParams) -> Result<Task, A2aError> {
            Err(JsonRpcError::task_not_found("x").into())
        }
        async fn cancel(&self, _p: TaskIdParams) -> Result<Task, A2aError> {
            Err(JsonRpcError::task_not_cancelable("x").into())
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = a2a_router(peer_card(addr), Arc::new(Rejecting));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = A2aPeerClient::new(None);
    let endpoint = format!("http://{addr}/a2a");
    let err = client
        .send(&endpoint, "a2a-x", "do it", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(err.contains("peer rejected"), "surfaces the peer error: {err}");
}

#[tokio::test]
async fn send_subscribe_streams_peer_states_to_terminal() {
    // The streaming arm of the P2P hop: a direct tasks/sendSubscribe over real
    // HTTP, draining the peer's SSE feed to the final frame — no orchd in between.
    let (addr, _peer) = spawn_peer().await;
    let endpoint = format!("http://{addr}/a2a");
    let client = A2aPeerClient::new(None);

    let frames = client
        .send_subscribe(&endpoint, "a2a-s1", "stream it", serde_json::json!({ "rig": "gtweb" }))
        .await
        .expect("peer tasks/sendSubscribe");

    // The peer pushed working then a terminal completed.
    let states: Vec<&str> = frames
        .iter()
        .filter_map(|f| f["status"]["state"].as_str())
        .collect();
    assert_eq!(states, vec!["working", "completed"]);
}
