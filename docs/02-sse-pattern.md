# SSE pattern

How Server-Sent Events work in Gas Town. **Read this before adding any `/api/*` endpoint that streams.**

## When to use SSE vs alternatives

| Mechanism | Use for | Don't use for |
|-----------|---------|---------------|
| **SSE** | Read-only fan-out, server → client (feed, activity, board updates, dog status, log tail) | Bidirectional interaction |
| **WebSocket** | Bidirectional (terminal, collaborative editing, presence) | Simple read streams (overkill) |
| **Polling** | Cheap state where staleness up to N seconds is fine | High-frequency updates |
| **HTTP one-shot** | Snapshot reads | Anything that updates after page load |

Default to SSE for "show me changes to X as they happen". Only escalate to WS when the client must send.

## Auth: cookie, not header

`EventSource` does NOT support custom HTTP headers (`Authorization`, `X-GT-Workspace`, etc.). Use cookie + JWT claim.

```
GET /api/v1/<module>/stream HTTP/1.1
Cookie: gt_web_token=<jwt>
Accept: text/event-stream
```

Backend reads JWT from `gt_web_token` cookie (mirror of localStorage bearer for browser nav). `WorkspaceGuard` middleware enforces scope + workspace BEFORE the SSE upgrade returns headers.

**Never** put the token in the query string — leaks into proxy/access logs.

## Per-workspace channel keying

Every SSE handler resolves `RootHandle` from `WorkspaceContext`, then subscribes to a `tokio::sync::broadcast::Receiver` keyed by `(workspace_id, channel_name)`. Publisher fans out only to subscribers whose `workspace_id` matches.

```rust
let root = state.workspaces.resolve(&ctx.workspace).await?;
let rx = root.events().subscribe::<BeadEvent>();   // already scoped to this RootHandle
let stream = BroadcastStream::new(rx).filter_map(|r| match r {
    Ok(ev) => Some(Ok(Event::default()
        .event(ev.kind())                            // "bead.created.v1"
        .id(ev.seq())                                 // for Last-Event-ID
        .data(serde_json::to_string(&ev).unwrap()))),
    Err(BroadcastStreamRecvError::Lagged(n)) => Some(Ok(Event::default()
        .event("system.lagged")
        .data(format!(r#"{{"skipped":{n}}}"#)))),
});
Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
```

**Cross-workspace check happens at `RootRegistry::resolve(&ctx.workspace).await`.** Never accept a workspace_id from the URL or body — always from auth ctx.

## Reconnect + Last-Event-ID

Client reconnects with `Last-Event-ID: <seq>` header (EventSource sends automatically). Server reads the header, replays missed events from the event log (or rejects with 410 Gone if seq is older than retention), then resumes live broadcast.

```rust
let resume_from: Option<u64> = headers
    .get("last-event-id")
    .and_then(|v| v.to_str().ok())
    .and_then(|s| s.parse().ok());

let replay = match resume_from {
    Some(seq) => event_log.read_from(workspace_id, seq).await?,  // bounded
    None => Vec::new(),
};
let stream = stream::iter(replay.into_iter().map(Ok)).chain(live_stream);
```

If `seq` is older than the workspace's retention window, return `410 Gone` so the client knows to drop in-memory cache and refetch via snapshot endpoint.

## Heartbeat

Traefik/nginx proxies drop idle connections after ~60s. Send a comment line every 15s:

```rust
Sse::new(stream).keep_alive(
    KeepAlive::new()
        .interval(Duration::from_secs(15))
        .text("keep-alive")
)
```

axum `KeepAlive` emits `: keep-alive\n\n` which is ignored by the browser but resets the proxy timer.

## Event versioning

Event name = `<module>.<noun>.v<N>`:

```
event: bead.created.v1
data: {"id":"hq-mod-core.1","title":"..."}

event: bead.created.v2
data: {"id":"hq-mod-core.1","title":"...","extra":"..."}
```

Clients subscribe by event name:

```ts
es.addEventListener("bead.created.v1", (ev) => { ... });
es.addEventListener("bead.created.v2", (ev) => { ... });  // additive
```

**Never** delete v1 once v2 ships. Both coexist forever (per `hq-mod-events` rule).

## Backend boilerplate (axum)

```rust
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;

pub async fn feed_stream(
    State(state): State<AppState>,
    Extension(ctx): Extension<WorkspaceContext>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let root = state.workspaces.resolve(&ctx.workspace).await?;
    let resume_from = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok()?.parse().ok());
    let stream = root.feed().stream(resume_from).await?;
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(Duration::from_secs(15)),
    ))
}
```

## Frontend boilerplate (Svelte)

```ts
import { workspace } from "$lib/workspace";
import { writable } from "svelte/store";

export function createFeedStore() {
  const events = writable<FeedEvent[]>([]);
  let es: EventSource | null = null;

  const connect = (ws: string) => {
    es?.close();
    es = new EventSource(`/api/v1/feed/stream`, { withCredentials: true });
    es.addEventListener("bead.created.v1", (ev) => {
      const data = JSON.parse(ev.data);
      events.update((xs) => [...xs, { kind: "bead.created", data, id: ev.lastEventId }]);
    });
    es.addEventListener("system.lagged", (ev) => {
      // server skipped events — refetch snapshot
      refetchSnapshot();
    });
    es.onerror = () => {
      // EventSource auto-reconnects with Last-Event-ID
    };
  };

  // Reconnect on workspace switch
  workspace.subscribe(connect);

  return { subscribe: events.subscribe };
}
```

`withCredentials: true` is critical — sends the `gt_web_token` cookie.

## Anti-patterns

- ❌ Putting JWT in query string (`?token=...`)
- ❌ Reading workspace_id from URL or body (must come from auth ctx)
- ❌ Skipping `KeepAlive` (proxies kill idle streams)
- ❌ Reusing one broadcast channel across all workspaces (cross-tenant leak)
- ❌ Emitting an event without version suffix (`event: bead.created`)
- ❌ Sending events back to the publisher's own SSE subscriber (echo loop; filter by client_id if needed)
- ❌ Heavy work per-subscriber on event fire (clone-on-write; mutate shared state once, fan out lightweight diff)

## Reference modules using this pattern (when shipped)

- `feed` module — Activity stream
- `sessions` module — Session list updates
- `merge` module — Merge queue state
- `dog` module — Dog status changes
- `kanban` module (future) — Card moves
