//! JSON-RPC 2.0 envelope + the A2A error space (B1, gtcore-fbf90e / epic gtcore-155917).
//!
//! A2A rides plain JSON-RPC 2.0: one `POST /a2a` per call, `method` selects the
//! operation. Error codes: the standard -32600..-32700 band plus the A2A-specific
//! -32001..-32006 band from the spec's error table.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    /// `tasks/send` | `tasks/sendSubscribe` | `tasks/get` | `tasks/cancel` | …
    pub method: String,
    #[serde(default)]
    pub params: Value,
    /// Absent on notifications; A2A calls always carry one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Value,
}

impl JsonRpcResponse {
    pub fn ok(id: Value, result: Value) -> Self {
        JsonRpcResponse { jsonrpc: JSONRPC_VERSION.into(), result: Some(result), error: None, id }
    }

    pub fn err(id: Value, error: JsonRpcError) -> Self {
        JsonRpcResponse { jsonrpc: JSONRPC_VERSION.into(), result: None, error: Some(error), id }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        JsonRpcError { code, message: message.into(), data: None }
    }

    // Standard JSON-RPC band.
    pub fn parse_error() -> Self {
        Self::new(-32700, "Parse error")
    }
    pub fn invalid_request() -> Self {
        Self::new(-32600, "Invalid Request")
    }
    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("Method not found: {method}"))
    }
    pub fn invalid_params(detail: impl Into<String>) -> Self {
        Self::new(-32602, detail)
    }
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(-32603, detail)
    }

    // A2A-specific band (spec error table).
    pub fn task_not_found(id: &str) -> Self {
        Self::new(-32001, format!("Task not found: {id}"))
    }
    pub fn task_not_cancelable(id: &str) -> Self {
        Self::new(-32002, format!("Task cannot be canceled: {id}"))
    }
    pub fn push_notifications_unsupported() -> Self {
        Self::new(-32003, "Push Notification is not supported")
    }
    pub fn unsupported_operation(detail: impl Into<String>) -> Self {
        Self::new(-32004, detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trip() {
        let req: JsonRpcRequest = serde_json::from_str(
            r#"{"jsonrpc": "2.0", "id": 1, "method": "tasks/send", "params": {"id": "t1"}}"#,
        )
        .unwrap();
        assert_eq!(req.method, "tasks/send");
        assert_eq!(req.id, Some(json!(1)));
    }

    #[test]
    fn error_codes_match_spec() {
        assert_eq!(JsonRpcError::task_not_found("x").code, -32001);
        assert_eq!(JsonRpcError::task_not_cancelable("x").code, -32002);
        assert_eq!(JsonRpcError::push_notifications_unsupported().code, -32003);
        assert_eq!(JsonRpcError::unsupported_operation("x").code, -32004);
        assert_eq!(JsonRpcError::method_not_found("x").code, -32601);
    }

    #[test]
    fn response_has_exactly_one_of_result_or_error() {
        let ok = JsonRpcResponse::ok(json!(1), json!({"id": "t"}));
        let v = serde_json::to_value(&ok).unwrap();
        assert!(v.get("result").is_some() && v.get("error").is_none());

        let err = JsonRpcResponse::err(json!(1), JsonRpcError::invalid_request());
        let v = serde_json::to_value(&err).unwrap();
        assert!(v.get("error").is_some() && v.get("result").is_none());
    }
}
