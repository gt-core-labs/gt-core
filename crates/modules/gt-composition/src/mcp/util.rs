//! Shared helpers for the domain dispatch handlers (`hq-mcp-dispatch.3..7`):
//! argument parsing, the server clock, and the `gt-events` → MCP error map.

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_store_dolt::AppError;

/// Pull a required string argument, rejecting a missing/non-string value as a
/// validation fault (not an internal error).
pub fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, AppError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation(format!("missing string argument `{key}`")))
}

/// Server-side epoch-seconds clock for command timestamps (the clock is the
/// edge's to supply, never the model's).
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Deserialize a command struct from the tool args verbatim. A malformed payload
/// is a validation fault.
pub fn parse<T: DeserializeOwned>(args: Value) -> Result<T, AppError> {
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Deserialize a command struct from the tool args, stamping `now_secs` with the
/// server clock when the caller omits it. A malformed payload is a validation
/// fault.
pub fn parse_cmd<T: DeserializeOwned>(mut args: Value) -> Result<T, AppError> {
    if let Value::Object(map) = &mut args {
        map.entry("now_secs").or_insert_with(|| json!(now_secs()));
    }
    serde_json::from_value(args)
        .map_err(|e| AppError::Validation(format!("invalid arguments: {e}")))
}

/// Map a `gt-events` domain error onto the `gt-store-dolt` error space the MCP
/// server maps from (the two enums mirror each other).
pub fn ev_err(e: gt_events::AppError) -> AppError {
    use gt_events::AppError as E;
    match e {
        E::NotFound(s) => AppError::NotFound(s),
        E::Validation(s) => AppError::Validation(s),
        E::InvalidTransition(s) => AppError::InvalidTransition(s),
        E::Handler(s) => AppError::Handler(s),
        other => AppError::Other(other.to_string()),
    }
}
