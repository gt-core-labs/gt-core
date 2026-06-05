# gt-mcp

Client + stdio↔HTTP MCP proxy for a [gt-core](https://github.com/gt-core-labs/gt-core)
`gt-mcp-server`. It isolates the server-comms logic from the `gt` CLI:

- **`Client`** — the REST surface: `POST /auth/login`, `/auth/refresh`, and the
  `GET /api/v1/workspace` + `/api/v1/rig` catalogs.
- **`proxy::run(server_url, access_token, workspace)`** — a transparent stdio MCP
  proxy. It serves an MCP server over stdio and forwards every request/notification to
  the remote `/mcp` streamable-HTTP transport, injecting the bearer token and an
  `X-Workspace` header. The forward is generic, so new server tools appear
  automatically — no per-tool code.

## Example

```rust,no_run
# async fn run() -> anyhow::Result<()> {
let client = gt_mcp::Client::new("https://gt.example.com")?;
let tokens = client.login("me@example.com", "secret").await?;
let workspaces = client.list_workspaces(&tokens.access_token).await?;

// Bridge a stdio MCP client (e.g. an agent) to the remote server:
gt_mcp::proxy::run("https://gt.example.com", &tokens.access_token, "default").await?;
# Ok(()) }
```

Licensed under MIT OR Apache-2.0.
