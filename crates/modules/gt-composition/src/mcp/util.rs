//! Shared helpers for the domain dispatch handlers (`hq-mcp-dispatch.3..7`):
//! argument parsing, the server clock, and the `gt-events` → MCP error map.

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use gt_module::McpTool;
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

/// One input-schema property: its name, JSON Schema type, and whether the handler
/// requires it. Server-injected args (`workspace_id`) and server-stamped ones
/// (`now_secs`) are never fields — they are not the caller's to supply.
pub struct Field {
    /// The argument key, as the handler reads it from the payload.
    pub name: &'static str,
    /// The JSON Schema `type` (`"string"`, `"integer"`, `"array"`, …).
    pub ty: &'static str,
    /// Whether the handler rejects the call when the key is absent.
    pub required: bool,
}

/// A required field of the given JSON Schema type.
pub fn req(name: &'static str, ty: &'static str) -> Field {
    Field { name, ty, required: true }
}

/// An optional field of the given JSON Schema type.
pub fn opt(name: &'static str, ty: &'static str) -> Field {
    Field { name, ty, required: false }
}

/// Build a tool descriptor (hq-mcp-native.3) with an object input schema derived
/// from `fields`, mirroring the arguments the handler's `dispatch` reads. The
/// server folds these into `tools/list` + `meta.help` so the domain tool is
/// discoverable, not just dispatchable. The schema documents the call for a native
/// client; the handler still validates the payload on dispatch (discovery, not
/// enforcement).
pub fn descriptor(name: &str, description: &str, fields: &[Field]) -> McpTool {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for f in fields {
        properties.insert(f.name.to_string(), json!({ "type": f.ty }));
        if f.required {
            required.push(json!(f.name));
        }
    }
    let schema = json!({
        "type": "object",
        "properties": Value::Object(properties),
        "required": Value::Array(required),
    });
    McpTool::with_schema(name, description, schema)
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
