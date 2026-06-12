//! SSE framing for `tasks/sendSubscribe` (plans/a2a-checklist.md B1).
//!
//! Per spec, each SSE `data:` field carries a **complete JSON-RPC response**
//! whose `result` is the update event — not the bare event. This module owns
//! that envelope so the axum adapter and any future transport frame identically.

use serde_json::Value;

use crate::handler::StreamEvent;
use crate::jsonrpc::JsonRpcResponse;

/// Wrap a stream event in the JSON-RPC envelope of the originating request and
/// serialize it for an SSE `data:` field. `request_id` is the JSON-RPC `id` of
/// the `tasks/sendSubscribe` call this stream answers.
pub fn sse_frame(request_id: &Value, event: &StreamEvent) -> String {
    let result = match event {
        StreamEvent::Status(s) => serde_json::to_value(s),
        StreamEvent::Artifact(a) => serde_json::to_value(a),
    }
    .expect("update events serialize infallibly");
    let resp = JsonRpcResponse::ok(request_id.clone(), result);
    serde_json::to_string(&resp).expect("envelope serializes infallibly")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TaskState, TaskStatus, TaskStatusUpdateEvent};
    use serde_json::json;

    #[test]
    fn frame_is_a_full_jsonrpc_response() {
        let ev = StreamEvent::Status(TaskStatusUpdateEvent {
            id: "hq-x".into(),
            status: TaskStatus { state: TaskState::Working, timestamp: None, message: None },
            is_final: false,
            metadata: None,
        });
        let frame = sse_frame(&json!(7), &ev);
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 7);
        assert_eq!(v["result"]["status"]["state"], "working");
        assert_eq!(v["result"]["final"], false);
        // Single line — safe as one SSE data: field.
        assert!(!frame.contains('\n'));
    }
}
