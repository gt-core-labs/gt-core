//! Direct rig→rig A2A delegation over HTTP (A7, gtcore-3a3557 — epic gtcore-155917).
//!
//! The in-process `a2a.delegate` tool ([`crate::mcp::a2a_delegate`]) mints a child
//! bead in *this* server's tracker and drops a dispatch order on the local orchd
//! channel — orchd is the substrate the work runs on. A7 adds the **peer-to-peer**
//! transport the bead asks for: an agent on rig A delegates straight to rig B's
//! own A2A endpoint, discovered via B's Agent Card, with **orchd never relaying
//! the message** — the JSON-RPC `tasks/send` is a direct HTTP round-trip from this
//! process to the peer's `POST /a2a`.
//!
//! Three small pieces, mirroring the deny-by-default config shape the rest of the
//! A2A surface already uses ([`CrossWsGrants`](crate::mcp::cross_ws::CrossWsGrants)):
//!
//! - [`PeerRegistry`] — the operator-configured `name → base URL` map (env
//!   `GT_A2A_PEERS`). It resolves a `peer` argument that is either a configured
//!   name or a bare `http(s)://` origin; an unknown name resolves to `None`, so a
//!   typo cannot silently widen reach.
//! - [`A2aPeerClient`] — the HTTP client: [`discover`](A2aPeerClient::discover)
//!   fetches `{base}/.well-known/agent.json`, [`send`](A2aPeerClient::send) POSTs a
//!   spec `tasks/send` to the card's `url`. Both carry the outbound bearer so the
//!   peer's guarded `POST /a2a` accepts the call.
//!
//! The RBAC gate itself reuses `CrossWsGrants` keyed on `origin → peer name`, so
//! the deny-by-default + wildcard semantics are the audited, tested ones already.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};

use gt_a2a::{AgentCard, JsonRpcResponse, Task, JSONRPC_VERSION};

/// The well-known discovery path every A2A peer serves its Agent Card at.
const WELL_KNOWN: &str = "/.well-known/agent.json";

/// Operator-configured peer endpoints: a `name → base URL` map parsed from the
/// `GT_A2A_PEERS` env (`name=https://host, other=https://host2`). The base URL is
/// the peer's *origin* — discovery hangs `/.well-known/agent.json` off it and the
/// JSON-RPC target is whatever its Agent Card advertises as `url`.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    peers: HashMap<String, String>,
}

impl PeerRegistry {
    /// An empty registry — no named peers. A bare `http(s)://` `peer` arg still
    /// resolves (it is its own address); a named peer never does.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Parse the `GT_A2A_PEERS` spec: comma-separated `name=base_url` pairs.
    /// Whitespace is trimmed; an entry without a single `=`, or with an empty
    /// side, is dropped (a malformed clause can never register a phantom peer).
    /// A trailing `/` on a base URL is normalised away so joins are clean.
    pub fn parse(spec: &str) -> Self {
        let peers = spec
            .split(',')
            .filter_map(|entry| {
                let (name, url) = entry.split_once('=')?;
                let name = name.trim();
                let url = url.trim().trim_end_matches('/');
                if name.is_empty() || url.is_empty() {
                    return None;
                }
                Some((name.to_string(), url.to_string()))
            })
            .collect();
        Self { peers }
    }

    /// Resolve a `peer` argument to its base URL. A configured **name** maps to its
    /// URL; a bare `http://`/`https://` value is its own base URL (so an operator
    /// can target an ad-hoc peer without registering it first). Anything else —
    /// an unknown name — is `None`.
    pub fn resolve(&self, peer: &str) -> Option<String> {
        let peer = peer.trim();
        if let Some(url) = self.peers.get(peer) {
            return Some(url.clone());
        }
        if peer.starts_with("http://") || peer.starts_with("https://") {
            return Some(peer.trim_end_matches('/').to_string());
        }
        None
    }

    /// `true` when no named peers are configured (diagnostics / startup log).
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Number of configured named peers (startup log line).
    pub fn len(&self) -> usize {
        self.peers.len()
    }
}

/// HTTP client for the peer-to-peer A2A path. Holds one reusable [`reqwest::Client`]
/// and the outbound bearer the peer's guarded `POST /a2a` requires (peers share the
/// platform's RS256 verifier, so a token minted here verifies there).
pub struct A2aPeerClient {
    client: reqwest::Client,
    /// Bearer presented to the peer. `None` ⇒ unauthenticated calls (only a peer
    /// that left `POST /a2a` open would accept them).
    token: Option<String>,
}

impl A2aPeerClient {
    /// Build a client with a sane request timeout and the outbound bearer.
    pub fn new(token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { client, token }
    }

    /// Inject the bearer when one is configured.
    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    /// Fetch and parse the peer's Agent Card from `{base}/.well-known/agent.json`.
    /// The discovery document is public per spec, but we still present the bearer
    /// (a peer is free to guard it). Errors are stringly-typed to match the rest
    /// of the A2A port adapters.
    pub async fn discover(&self, base_url: &str) -> Result<AgentCard, String> {
        let url = format!("{}{WELL_KNOWN}", base_url.trim_end_matches('/'));
        let resp = self
            .authed(self.client.get(&url))
            .send()
            .await
            .map_err(|e| format!("discover {url}: {e}"))?;
        let resp = resp
            .error_for_status()
            .map_err(|e| format!("discover {url}: {e}"))?;
        resp.json::<AgentCard>()
            .await
            .map_err(|e| format!("parse agent card from {url}: {e}"))
    }

    /// Send a `tasks/send` to the peer's JSON-RPC endpoint (`card.url`) and return
    /// the peer's [`Task`] — its `id` is the bead the peer minted (the A2A
    /// task-identity rule: the server is authoritative for the id).
    ///
    /// `task_id` is the client-minted id the spec requires on the request; the
    /// peer answers with its own canonical id. `metadata` rides the message params
    /// so the peer's gateway can read `rig`/`priority`/`caller` exactly as it does
    /// for any other inbound `tasks/send`.
    pub async fn send(
        &self,
        endpoint: &str,
        task_id: &str,
        text: &str,
        metadata: Value,
    ) -> Result<Task, String> {
        let params = json!({
            "id": task_id,
            "message": {
                "role": "user",
                "parts": [{ "type": "text", "text": text }],
            },
            "metadata": metadata,
        });
        let body = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": task_id,
            "method": "tasks/send",
            "params": params,
        });
        let resp = self
            .authed(self.client.post(endpoint))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("tasks/send to {endpoint}: {e}"))?;
        let resp = resp
            .error_for_status()
            .map_err(|e| format!("tasks/send to {endpoint}: {e}"))?;
        let rpc: JsonRpcResponse = resp
            .json()
            .await
            .map_err(|e| format!("decode tasks/send response from {endpoint}: {e}"))?;
        if let Some(err) = rpc.error {
            return Err(format!(
                "peer rejected tasks/send ({}): {}",
                err.code, err.message
            ));
        }
        let result = rpc
            .result
            .ok_or_else(|| format!("peer {endpoint} returned neither result nor error"))?;
        serde_json::from_value::<Task>(result)
            .map_err(|e| format!("decode task from {endpoint}: {e}"))
    }

    /// Stream a `tasks/sendSubscribe` to the peer and drain its SSE feed to the
    /// terminal frame, returning every `result` object the peer pushed in order
    /// (status updates + artifact chunks). The peer ends the stream on the final
    /// event (`final: true`), so reading the body to EOF yields the full sequence
    /// without an open-ended poll.
    ///
    /// Returned as raw [`Value`]s — the caller (or a future `a2a.subscribe` tool)
    /// reads `["status"]["state"]` / `["artifact"]` as it needs; collapsing the
    /// SSE plumbing here keeps the in-process MCP surface free of a streaming
    /// return type it cannot express.
    pub async fn send_subscribe(
        &self,
        endpoint: &str,
        task_id: &str,
        text: &str,
        metadata: Value,
    ) -> Result<Vec<Value>, String> {
        let body = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": task_id,
            "method": "tasks/sendSubscribe",
            "params": {
                "id": task_id,
                "message": {
                    "role": "user",
                    "parts": [{ "type": "text", "text": text }],
                },
                "metadata": metadata,
            },
        });
        let resp = self
            .authed(self.client.post(endpoint))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("tasks/sendSubscribe to {endpoint}: {e}"))?;
        let resp = resp
            .error_for_status()
            .map_err(|e| format!("tasks/sendSubscribe to {endpoint}: {e}"))?;
        // The stream is finite (ends on the final frame), so the full SSE payload
        // is bounded — read it to EOF and split out the `data:` lines.
        let payload = resp
            .text()
            .await
            .map_err(|e| format!("read SSE stream from {endpoint}: {e}"))?;
        let mut frames = Vec::new();
        for line in payload.lines() {
            let Some(data) = line.strip_prefix("data: ") else { continue };
            let data = data.trim();
            if data.is_empty() {
                continue;
            }
            let rpc: JsonRpcResponse = serde_json::from_str(data)
                .map_err(|e| format!("decode SSE frame from {endpoint}: {e}"))?;
            if let Some(err) = rpc.error {
                return Err(format!(
                    "peer streamed an error ({}): {}",
                    err.code, err.message
                ));
            }
            if let Some(result) = rpc.result {
                frames.push(result);
            }
        }
        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_parses_name_url_pairs() {
        let r = PeerRegistry::parse("gtweb=https://gtweb.example.com, infra=http://infra:8080/");
        assert_eq!(r.len(), 2);
        // Named peers resolve to their configured base; the trailing slash is gone.
        assert_eq!(r.resolve("gtweb").as_deref(), Some("https://gtweb.example.com"));
        assert_eq!(r.resolve("infra").as_deref(), Some("http://infra:8080"));
        // An unknown name is None — a typo never widens reach.
        assert_eq!(r.resolve("nope"), None);
    }

    #[test]
    fn registry_resolves_bare_urls_without_registration() {
        let r = PeerRegistry::empty();
        assert!(r.is_empty());
        assert_eq!(
            r.resolve("https://adhoc.example.com/").as_deref(),
            Some("https://adhoc.example.com")
        );
        assert_eq!(r.resolve("http://h:9/").as_deref(), Some("http://h:9"));
        // A non-URL, non-registered name still resolves to nothing.
        assert_eq!(r.resolve("gtweb"), None);
    }

    #[test]
    fn registry_drops_malformed_entries() {
        // No `=`, empty sides, stray whitespace, blank clause — all dropped.
        let r = PeerRegistry::parse("noeq, =url, name=, , good=https://h");
        assert_eq!(r.len(), 1);
        assert_eq!(r.resolve("good").as_deref(), Some("https://h"));
    }

    #[test]
    fn empty_spec_parses_to_empty() {
        assert!(PeerRegistry::parse("").is_empty());
        assert!(PeerRegistry::parse("   ").is_empty());
    }
}
