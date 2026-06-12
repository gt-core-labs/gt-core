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
    A2aGateway, A2aGatewayConfig, BeadIntake, ChannelDispatch, IntakeRequest,
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

#[tokio::test]
async fn post_a2a_send_mints_bead_drops_event_and_answers_submitted() {
    let channel = tempfile::tempdir().unwrap();
    let intake = Arc::new(RecordingIntake(Mutex::new(vec![])));
    let gateway = A2aGateway::new(
        intake.clone(),
        Arc::new(ChannelDispatch::new(channel.path().to_path_buf())),
        A2aGatewayConfig {
            rig: "gtcore".into(),
            parent_id: "gtcore-intake".into(),
            created_by: "a2a".into(),
            domain: vec![Domain::MetaGap],
        },
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

#[tokio::test]
async fn post_a2a_get_is_unsupported_until_b4() {
    let channel = tempfile::tempdir().unwrap();
    let gateway = A2aGateway::new(
        Arc::new(RecordingIntake(Mutex::new(vec![]))),
        Arc::new(ChannelDispatch::new(channel.path().to_path_buf())),
        A2aGatewayConfig {
            rig: "gtcore".into(),
            parent_id: "gtcore-intake".into(),
            created_by: "a2a".into(),
            domain: vec![Domain::MetaGap],
        },
    );
    let app = a2a_router(card(), Arc::new(gateway));
    let v = rpc(
        app,
        json!({"jsonrpc": "2.0", "id": 8, "method": "tasks/get",
               "params": {"id": "gtcore-minted1"}}),
    )
    .await;
    assert_eq!(v["error"]["code"], -32004);
}
