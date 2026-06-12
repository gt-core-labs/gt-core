//! In-process integration of the A2A ingress (gtcore-c7bbef, epic gtcore-155917):
//! the REAL `gt_a2a::a2a_router` mounted over the REAL [`A2aGateway`], with the
//! tracker faked at the [`BeadIntake`] port and the dispatch channel as a
//! tempdir — so the assertion covers the full wire path the acceptance criteria
//! name (`POST /a2a` JSON-RPC → bead minted → `.event` order → `Task{submitted}`)
//! without a Dolt server in the loop.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tower::ServiceExt;

use gt_a2a::{a2a_router, AgentCapabilities, AgentCard};
use gt_composition::a2a::{
    A2aGateway, A2aGatewayConfig, BeadIntake, BeadSnapshot, ChannelDispatch, EventFeed,
    FeedRecord, IntakeRequest, SessionControl, TaskStore,
};
use gt_issues::Domain;

/// Tracker fake: records the shaped intake and mints a fixed bead id.
struct RecordingIntake(Mutex<Vec<IntakeRequest>>);

#[async_trait]
impl BeadIntake for RecordingIntake {
    async fn create(&self, req: IntakeRequest) -> Result<String, String> {
        self.0.lock().unwrap().push(req);
        Ok("gtcore-minted1".into())
    }
}

/// Bead store fake: one known working bead + a terminate trail.
struct OneBeadStore(Mutex<Vec<String>>);

#[async_trait]
impl TaskStore for OneBeadStore {
    async fn fetch(&self, bead: &str) -> Result<Option<BeadSnapshot>, String> {
        Ok((bead == "gtcore-minted1")
            .then(|| BeadSnapshot { status: "working".into(), rig: "gtcore".into() }))
    }
    async fn terminate(&self, bead: &str, _reason: &str) -> Result<(), String> {
        self.0.lock().unwrap().push(bead.into());
        Ok(())
    }
}

/// Session fake: the known bead runs a live session + a kill trail.
struct OneSession(Mutex<Vec<String>>);

#[async_trait]
impl SessionControl for OneSession {
    async fn state(&self, session: &str) -> Result<Option<String>, String> {
        Ok((session == "gtcore-gtcore-minted1").then(|| "working".into()))
    }
    async fn kill(&self, session: &str, _reason: &str) -> Result<(), String> {
        self.0.lock().unwrap().push(session.into());
        Ok(())
    }
}

fn card() -> AgentCard {
    AgentCard {
        name: "gt".into(),
        description: None,
        url: "http://test/a2a".into(),
        version: "0.1.0".into(),
        provider: None,
        capabilities: AgentCapabilities::default(),
        authentication: None,
        default_input_modes: vec!["text".into()],
        default_output_modes: vec!["text".into()],
        skills: vec![],
        signature: None,
    }
}

async fn rpc(app: axum::Router, body: Value) -> Value {
    let resp = app
        .oneshot(
            axum::http::Request::post("/a2a")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Feed fake: the subscribe wire path is exercised in the unit suite; the
/// router tests only need the port satisfied.
struct EmptyFeed;

#[async_trait]
impl EventFeed for EmptyFeed {
    async fn tail(&self, _since: Option<&str>) -> Result<Vec<FeedRecord>, String> {
        Ok(vec![])
    }
}

fn test_config() -> A2aGatewayConfig {
    A2aGatewayConfig {
        rig: "gtcore".into(),
        parent_id: "gtcore-intake".into(),
        created_by: "a2a".into(),
        domain: vec![Domain::MetaGap],
        poll: std::time::Duration::from_millis(5),
    }
}

#[tokio::test]
async fn post_a2a_send_mints_bead_drops_event_and_answers_submitted() {
    let channel = tempfile::tempdir().unwrap();
    let intake = Arc::new(RecordingIntake(Mutex::new(vec![])));
    let gateway = A2aGateway::new(
        intake.clone(),
        Arc::new(ChannelDispatch::new(channel.path().to_path_buf())),
        Arc::new(OneBeadStore(Mutex::new(vec![]))),
        Arc::new(OneSession(Mutex::new(vec![]))),
        Arc::new(EmptyFeed),
        test_config(),
    );
    let app = a2a_router(card(), Arc::new(gateway));

    let v = rpc(
        app,
        json!({"jsonrpc": "2.0", "id": 7, "method": "tasks/send", "params": {
            "id": "client-task-1",
            "message": {"role": "user", "parts": [
                {"type": "text", "text": "port the planning view\n\nGroup modules by child_of."}
            ]}
        }}),
    )
    .await;

    // JSON-RPC result is the Task projection: minted bead id, submitted state.
    assert_eq!(v["id"], 7);
    assert_eq!(v["result"]["id"], "gtcore-minted1");
    assert_eq!(v["result"]["status"]["state"], "submitted");
    assert!(v["result"]["status"]["timestamp"].is_string(), "root stamps RFC3339");

    // The tracker saw the shaped intake (title = first line, full text kept).
    let seen = intake.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].title, "port the planning view");
    assert!(seen[0].description.contains("child_of"));

    // Exactly one atomic dispatch order on the channel, for the minted bead.
    let events: Vec<_> = std::fs::read_dir(channel.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "event"))
        .collect();
    assert_eq!(events.len(), 1);
    let order: Value =
        serde_json::from_str(&std::fs::read_to_string(&events[0]).unwrap()).unwrap();
    assert_eq!(order, json!({"bead": "gtcore-minted1", "priority": 1}));
}

/// get/cancel router app over the one-bead fakes; returns the trails.
fn projection_app() -> (axum::Router, Arc<OneBeadStore>, Arc<OneSession>) {
    let channel = std::env::temp_dir(); // unused by get/cancel
    let store = Arc::new(OneBeadStore(Mutex::new(vec![])));
    let sessions = Arc::new(OneSession(Mutex::new(vec![])));
    let gateway = A2aGateway::new(
        Arc::new(RecordingIntake(Mutex::new(vec![]))),
        Arc::new(ChannelDispatch::new(channel)),
        store.clone(),
        sessions.clone(),
        Arc::new(EmptyFeed),
        test_config(),
    );
    (a2a_router(card(), Arc::new(gateway)), store, sessions)
}

#[tokio::test]
async fn post_a2a_get_projects_live_task_and_404s_unknown() {
    let (app, _, _) = projection_app();
    let v = rpc(
        app.clone(),
        json!({"jsonrpc": "2.0", "id": 8, "method": "tasks/get",
               "params": {"id": "gtcore-minted1"}}),
    )
    .await;
    assert_eq!(v["result"]["status"]["state"], "working");

    let v = rpc(
        app,
        json!({"jsonrpc": "2.0", "id": 9, "method": "tasks/get",
               "params": {"id": "gtcore-ghost"}}),
    )
    .await;
    assert_eq!(v["error"]["code"], -32001);
}

#[tokio::test]
async fn post_a2a_cancel_kills_session_and_answers_canceled() {
    let (app, store, sessions) = projection_app();
    let v = rpc(
        app,
        json!({"jsonrpc": "2.0", "id": 10, "method": "tasks/cancel",
               "params": {"id": "gtcore-minted1"}}),
    )
    .await;
    assert_eq!(v["result"]["status"]["state"], "canceled");
    assert_eq!(*sessions.0.lock().unwrap(), vec!["gtcore-gtcore-minted1".to_string()]);
    assert_eq!(*store.0.lock().unwrap(), vec!["gtcore-minted1".to_string()]);
}
