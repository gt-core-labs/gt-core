//! The off-by-default `axum` adapter (B1, gtcore-fbf90e / epic gtcore-155917): `POST /a2a`
//! (JSON-RPC dispatch) + `GET /.well-known/agent.json` (discovery).
//!
//! Auth is NOT here — the composition root mounts this router behind the same
//! PAT guard as the MCP surface, exactly as the other domain routers. Errors
//! follow JSON-RPC-over-HTTP convention: HTTP 200 with an `error` member.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde_json::Value;

use crate::handler::A2aHandler;
use crate::jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};
use crate::sse::sse_frame;
use crate::types::AgentCard;

/// Shared route state: the discovery card + the up-tier handler.
#[derive(Clone)]
pub struct A2aApiState {
    pub card: Arc<AgentCard>,
    pub handler: Arc<dyn A2aHandler>,
}

/// Build the A2A router. The composition root nests it at `/` so the
/// well-known path lands at the URL the spec mandates.
pub fn a2a_router(card: AgentCard, handler: Arc<dyn A2aHandler>) -> Router {
    let state = A2aApiState { card: Arc::new(card), handler };
    Router::new()
        .route("/.well-known/agent.json", get(agent_card))
        .route("/a2a", post(rpc))
        .with_state(state)
}

async fn agent_card(State(state): State<A2aApiState>) -> Json<AgentCard> {
    Json((*state.card).clone())
}

async fn rpc(State(state): State<A2aApiState>, body: String) -> Response {
    let req: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(_) => {
            return Json(JsonRpcResponse::err(Value::Null, JsonRpcError::parse_error()))
                .into_response()
        }
    };
    let id = req.id.clone().unwrap_or(Value::Null);
    if req.jsonrpc != crate::jsonrpc::JSONRPC_VERSION {
        return Json(JsonRpcResponse::err(id, JsonRpcError::invalid_request())).into_response();
    }

    match req.method.as_str() {
        "tasks/send" => unary(id, req.params, |p| state.handler.send(p)).await,
        "tasks/get" => unary(id, req.params, |p| state.handler.get(p)).await,
        "tasks/cancel" => unary(id, req.params, |p| state.handler.cancel(p)).await,
        "tasks/sendSubscribe" => subscribe(state.clone(), id, req.params).await,
        // The push-notification config methods exist in the spec but gt does
        // not support them (capabilities.pushNotifications = false).
        m if m.starts_with("tasks/pushNotification") => {
            Json(JsonRpcResponse::err(id, JsonRpcError::push_notifications_unsupported()))
                .into_response()
        }
        m => Json(JsonRpcResponse::err(id, JsonRpcError::method_not_found(m))).into_response(),
    }
}

/// Decode params, run one handler op, wrap the outcome. Generic over the op so
/// send/get/cancel stay one line each in the dispatch match.
async fn unary<P, F, Fut>(id: Value, params: Value, op: F) -> Response
where
    P: serde::de::DeserializeOwned,
    F: FnOnce(P) -> Fut,
    Fut: std::future::Future<Output = Result<crate::types::Task, crate::handler::A2aError>>,
{
    let p: P = match serde_json::from_value(params) {
        Ok(p) => p,
        Err(e) => {
            return Json(JsonRpcResponse::err(id, JsonRpcError::invalid_params(e.to_string())))
                .into_response()
        }
    };
    match op(p).await {
        Ok(task) => Json(JsonRpcResponse::ok(
            id,
            serde_json::to_value(task).expect("task serializes infallibly"),
        ))
        .into_response(),
        Err(e) => Json(JsonRpcResponse::err(id, e.0)).into_response(),
    }
}

async fn subscribe(state: A2aApiState, id: Value, params: Value) -> Response {
    let p = match serde_json::from_value(params) {
        Ok(p) => p,
        Err(e) => {
            return Json(JsonRpcResponse::err(id, JsonRpcError::invalid_params(e.to_string())))
                .into_response()
        }
    };
    let mut events = match state.handler.send_subscribe(p).await {
        Ok(s) => s,
        Err(e) => return Json(JsonRpcResponse::err(id, e.0)).into_response(),
    };
    // Each SSE data: field is a full JSON-RPC response (sse::sse_frame); a
    // final status event ends the stream per spec.
    let stream = async_stream::stream! {
        while let Some(ev) =
            std::future::poll_fn(|cx| events.as_mut().poll_next(cx)).await
        {
            let is_final = ev.is_final();
            yield Ok::<_, Infallible>(Event::default().data(sse_frame(&id, &ev)));
            if is_final {
                break;
            }
        }
    };
    Sse::new(stream).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{A2aError, EventStream, StreamEvent};
    use crate::types::*;
    use async_trait::async_trait;
    use serde_json::json;
    use tower::ServiceExt;

    /// Fake up-tier handler: send mints a bead id; subscribe plays a
    /// working → completed script.
    struct Fake;

    fn task(id: &str, state: TaskState) -> Task {
        Task {
            id: id.into(),
            session_id: None,
            status: TaskStatus { state, timestamp: None, message: None },
            artifacts: vec![],
            history: vec![],
            metadata: None,
        }
    }

    #[async_trait]
    impl A2aHandler for Fake {
        async fn send(&self, _p: TaskSendParams) -> Result<Task, A2aError> {
            Ok(task("hq-minted", TaskState::Submitted))
        }
        async fn get(&self, p: TaskIdParams) -> Result<Task, A2aError> {
            if p.id == "hq-missing" {
                return Err(JsonRpcError::task_not_found(&p.id).into());
            }
            Ok(task(&p.id, TaskState::Working))
        }
        async fn cancel(&self, p: TaskIdParams) -> Result<Task, A2aError> {
            Ok(task(&p.id, TaskState::Canceled))
        }
        async fn send_subscribe(&self, _p: TaskSendParams) -> Result<EventStream, A2aError> {
            let status = |state: TaskState, is_final| {
                StreamEvent::Status(TaskStatusUpdateEvent {
                    id: "hq-minted".into(),
                    status: TaskStatus { state, timestamp: None, message: None },
                    is_final,
                    metadata: None,
                })
            };
            Ok(Box::pin(async_stream::stream! {
                yield status(TaskState::Working, false);
                yield status(TaskState::Completed, true);
            }))
        }
    }

    fn card() -> AgentCard {
        AgentCard {
            name: "gt".into(),
            description: None,
            url: "http://test/a2a".into(),
            version: "0.1.0".into(),
            provider: None,
            capabilities: AgentCapabilities { streaming: true, ..Default::default() },
            authentication: None,
            default_input_modes: vec!["text".into()],
            default_output_modes: vec!["text".into()],
            skills: vec![],
        }
    }

    fn app() -> Router {
        a2a_router(card(), Arc::new(Fake))
    }

    async fn rpc_call(app: Router, body: Value) -> (axum::http::StatusCode, Value) {
        let resp = app
            .oneshot(
                axum::http::Request::post("/a2a")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn agent_card_is_served_at_well_known() {
        let resp = app()
            .oneshot(
                axum::http::Request::get("/.well-known/agent.json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["name"], "gt");
        assert_eq!(v["capabilities"]["streaming"], true);
    }

    #[tokio::test]
    async fn send_returns_minted_task() {
        let (status, v) = rpc_call(
            app(),
            json!({"jsonrpc": "2.0", "id": 1, "method": "tasks/send",
                   "params": {"id": "client-1", "message": {"role": "user", "parts": [{"type": "text", "text": "go"}]}}}),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(v["result"]["id"], "hq-minted");
        assert_eq!(v["result"]["status"]["state"], "submitted");
    }

    #[tokio::test]
    async fn get_missing_task_maps_to_spec_error() {
        let (_, v) = rpc_call(
            app(),
            json!({"jsonrpc": "2.0", "id": 2, "method": "tasks/get", "params": {"id": "hq-missing"}}),
        )
        .await;
        assert_eq!(v["error"]["code"], -32001);
    }

    #[tokio::test]
    async fn unknown_method_and_push_notifications() {
        let (_, v) = rpc_call(
            app(),
            json!({"jsonrpc": "2.0", "id": 3, "method": "tasks/resubscribe", "params": {}}),
        )
        .await;
        assert_eq!(v["error"]["code"], -32601);

        let (_, v) = rpc_call(
            app(),
            json!({"jsonrpc": "2.0", "id": 4, "method": "tasks/pushNotification/set", "params": {}}),
        )
        .await;
        assert_eq!(v["error"]["code"], -32003);
    }

    #[tokio::test]
    async fn bad_params_and_bad_envelope() {
        let (_, v) = rpc_call(
            app(),
            json!({"jsonrpc": "2.0", "id": 5, "method": "tasks/send", "params": {"nope": 1}}),
        )
        .await;
        assert_eq!(v["error"]["code"], -32602);

        let (_, v) = rpc_call(
            app(),
            json!({"jsonrpc": "1.0", "id": 6, "method": "tasks/send", "params": {}}),
        )
        .await;
        assert_eq!(v["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn send_subscribe_streams_jsonrpc_frames_until_final() {
        let resp = app()
            .oneshot(
                axum::http::Request::post("/a2a")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({"jsonrpc": "2.0", "id": 9, "method": "tasks/sendSubscribe",
                               "params": {"id": "client-1", "message": {"role": "user", "parts": [{"type": "text", "text": "go"}]}}})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.headers()["content-type"].to_str().unwrap(),
            "text/event-stream"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        let frames: Vec<Value> = body
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .map(|d| serde_json::from_str(d).unwrap())
            .collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["id"], 9);
        assert_eq!(frames[0]["result"]["status"]["state"], "working");
        assert_eq!(frames[1]["result"]["status"]["state"], "completed");
        assert_eq!(frames[1]["result"]["final"], true);
    }
}
