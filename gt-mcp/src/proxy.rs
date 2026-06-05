//! `gt mcp` — the stdio MCP entrypoint an agent spawns (hq-gt-cli.4).
//!
//! A transparent proxy: it serves a stdio MCP server to the spawning agent and
//! forwards every request/notification to the remote `gt-mcp-server` over the
//! streamable-HTTP `/mcp` transport, injecting the active config's bearer token and
//! `X-Workspace` header. Because the forward is generic ([`Peer::send_request`] takes
//! the whole [`ClientRequest`] and returns the [`ServerResult`] unchanged), the proxy
//! needs no per-tool knowledge — new server tools appear automatically.
//!
//! `.mcp.json`: `{ "command": "gt", "args": ["mcp"] }`.
//!
//! Token refresh: the access token is fixed for the process lifetime. When it expires
//! the server returns 401 and the agent should re-run `gt init` (or `gt config use`)
//! to mint a fresh pair — transparent in-proxy refresh is a follow-up.

use std::sync::Arc;

use anyhow::{Context, Result};
use http::{HeaderName, HeaderValue};
use rmcp::model::{ClientNotification, ClientRequest, ErrorData as McpError, ServerInfo, ServerResult};
use rmcp::service::{
    NotificationContext, RequestContext, RoleClient, RoleServer, RunningService, Service,
};
use rmcp::transport::io::stdio;
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::{serve_client, serve_server};

/// Forwards stdio MCP traffic to the remote server. Holds the live client service
/// (kept alive so its background task keeps running) and the remote's advertised
/// `ServerInfo`, which it re-presents to the spawning agent on initialize.
struct Proxy {
    remote: Arc<RunningService<RoleClient, ()>>,
    info: ServerInfo,
}

impl Service<RoleServer> for Proxy {
    fn handle_request(
        &self,
        request: ClientRequest,
        _ctx: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ServerResult, McpError>> + Send + '_ {
        let remote = self.remote.clone();
        async move {
            remote
                .send_request(request)
                .await
                .map_err(|e| McpError::internal_error(format!("upstream MCP call failed: {e}"), None))
        }
    }

    fn handle_notification(
        &self,
        notification: ClientNotification,
        _ctx: NotificationContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send + '_ {
        let remote = self.remote.clone();
        async move {
            // A failed forward of a fire-and-forget notification must not tear the
            // session down — log to stderr (stdout is the JSON-RPC channel) and continue.
            if let Err(e) = remote.send_notification(notification).await {
                eprintln!("[gt mcp] dropped notification to upstream: {e}");
            }
            Ok(())
        }
    }

    fn get_info(&self) -> ServerInfo {
        self.info.clone()
    }
}

/// Run the stdio proxy against `server_url`, authenticating every upstream request with
/// `access_token` (a bearer, no `Bearer ` prefix) and tagging it with `X-Workspace`.
/// Blocks until the stdio peer closes. Caller (the `gt` CLI) supplies these from the
/// active `.gt-config` entry, so this crate stays free of the CLI's config types.
pub async fn run(server_url: &str, access_token: &str, workspace: &str) -> Result<()> {
    let uri = format!("{}/mcp", server_url.trim_end_matches('/'));

    // Auth + tenant injection: the bearer (without the `Bearer ` prefix — the transport
    // adds it) and `X-Workspace` go on every upstream request.
    let mut conf = StreamableHttpClientTransportConfig::with_uri(uri.clone());
    conf.auth_header = Some(access_token.to_string());
    conf.custom_headers.insert(
        HeaderName::from_static("x-workspace"),
        HeaderValue::from_str(workspace).context("workspace is not a valid header value")?,
    );
    let transport = StreamableHttpClientTransport::from_config(conf);

    eprintln!("[gt mcp] connecting upstream {uri} (workspace={workspace})");
    let remote = serve_client((), transport)
        .await
        .context("connect to the upstream gt-mcp-server")?;
    let info = remote.peer_info().cloned().unwrap_or_default();

    let proxy = Proxy {
        remote: Arc::new(remote),
        info,
    };
    eprintln!("[gt mcp] proxy ready on stdio");
    let server = serve_server(proxy, stdio())
        .await
        .context("serve the stdio MCP transport")?;
    server.waiting().await.context("stdio MCP serve loop")?;
    Ok(())
}
