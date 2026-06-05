//! One-shot MCP client calls over the streamable-HTTP transport.
//!
//! These back the `gt mcp call|list|resources|resource` shell surface: each opens a short
//! authenticated MCP session (bearer + `X-Workspace`), issues a single request, and returns
//! the result as a [`serde_json::Value`] so the CLI can print it without depending on rmcp's
//! model types. Distinct from [`crate::proxy`], which keeps a session open and forwards a
//! stdio peer's traffic.

use anyhow::{Context, Result};
use http::{HeaderName, HeaderValue};
use rmcp::model::{CallToolRequestParams, JsonObject, ReadResourceRequestParams};
use rmcp::serve_client;
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use serde_json::Value;

/// Open an authenticated MCP session to `{server_url}/mcp`. Kept private — callers below run a
/// single request and drop it.
async fn connect(
    server_url: &str,
    access_token: &str,
    workspace: &str,
) -> Result<RunningService<RoleClient, ()>> {
    let uri = format!("{}/mcp", server_url.trim_end_matches('/'));
    let mut conf = StreamableHttpClientTransportConfig::with_uri(uri);
    conf.auth_header = Some(access_token.to_string());
    conf.custom_headers.insert(
        HeaderName::from_static("x-workspace"),
        HeaderValue::from_str(workspace).context("workspace is not a valid header value")?,
    );
    let transport = StreamableHttpClientTransport::from_config(conf);
    serve_client((), transport)
        .await
        .context("connect to the upstream gt-mcp-server")
}

/// Call a tool. `arguments` is an optional JSON object matching the tool's input schema.
/// Returns the `CallToolResult` as JSON (carries `content` + `isError`).
pub async fn call_tool(
    server_url: &str,
    access_token: &str,
    workspace: &str,
    tool: &str,
    arguments: Option<Value>,
) -> Result<Value> {
    let client = connect(server_url, access_token, workspace).await?;
    let mut params = CallToolRequestParams::default();
    params.name = tool.to_string().into();
    params.arguments = match arguments {
        Some(Value::Object(map)) => Some(map as JsonObject),
        Some(_) => anyhow::bail!("tool arguments must be a JSON object"),
        None => None,
    };
    let result = client.call_tool(params).await.context("call_tool")?;
    serde_json::to_value(result).context("serialize call_tool result")
}

/// List the server's tools (name + description + input schema) as JSON.
pub async fn list_tools(server_url: &str, access_token: &str, workspace: &str) -> Result<Value> {
    let client = connect(server_url, access_token, workspace).await?;
    let tools = client.list_all_tools().await.context("list_tools")?;
    serde_json::to_value(tools).context("serialize tools")
}

/// List the server's resources as JSON.
pub async fn list_resources(server_url: &str, access_token: &str, workspace: &str) -> Result<Value> {
    let client = connect(server_url, access_token, workspace).await?;
    let resources = client.list_all_resources().await.context("list_resources")?;
    serde_json::to_value(resources).context("serialize resources")
}

/// Read one resource by URI (e.g. `gt://issues?limit=10`), returning its contents as JSON.
pub async fn read_resource(
    server_url: &str,
    access_token: &str,
    workspace: &str,
    uri: &str,
) -> Result<Value> {
    let client = connect(server_url, access_token, workspace).await?;
    // ReadResourceRequestParams is #[non_exhaustive] with no constructor — build it through serde.
    let params: ReadResourceRequestParams =
        serde_json::from_value(serde_json::json!({ "uri": uri }))
            .context("build read_resource params")?;
    let result = client.read_resource(params).await.context("read_resource")?;
    serde_json::to_value(result).context("serialize resource")
}
