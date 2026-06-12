//! Passthrough proxy for the Anthropic API (hq-284842): per-call, zero-extra-request
//! quota truth.
//!
//! Polecats' `claude` CLI points here via `ANTHROPIC_BASE_URL`; every request is forwarded
//! verbatim to the real upstream and, on the way back, the response is mined for the
//! provider's own accounting — no probing, no estimating:
//!
//! - `anthropic-ratelimit-unified-status` → `allowed_warning` parks the account `Limited`
//!   (draining: finish current bead, take no new), `rejected` parks it `Blocked` with the
//!   header's exact reset instant — rotation reacts within ONE call of the wall.
//! - `anthropic-ratelimit-tokens-*` → the classic [`ProbeWindow`] reconciliation
//!   (`QuotaHandle::probe`), same path the usage-endpoint prober feeds.
//! - The `/v1/messages` response BODY carries the API-reported `usage` block: recorded via
//!   `QuotaHandle::sample` so local counters match real spend exactly (hq-46cdf4's input).
//!
//! Attribution rides two request headers the polecat sling stamps through
//! `ANTHROPIC_CUSTOM_HEADERS`: `x-gt-account` (keychain account id) and optionally
//! `x-gt-session`. Both are STRIPPED before forwarding — the upstream never sees them.
//! Without `x-gt-account` the proxy still forwards faithfully; it just records nothing.
//!
//! Streaming (SSE) responses flow through chunk-by-chunk via a tee that accumulates the
//! transcript and parses usage when the stream completes — the client sees the upstream
//! bytes unmodified and unbuffered. All parsing is pure in `gt_quota` (`probe.rs`,
//! `body_usage.rs`); this module owns I/O only.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use futures_core::Stream;

use gt_quota::{
    parse_anthropic_ratelimit, parse_messages_json_usage, parse_messages_sse_usage,
    parse_unified_ratelimit, QuotaHandle, UnifiedStatus,
};

/// Default upstream — the real API.
const DEFAULT_UPSTREAM: &str = "https://api.anthropic.com";
/// Attribution headers (stamped by the polecat sling, stripped before forwarding).
pub const ACCOUNT_HEADER: &str = "x-gt-account";
pub const SESSION_HEADER: &str = "x-gt-session";

pub struct AnthropicProxy {
    http: reqwest::Client,
    quota: QuotaHandle,
    upstream: String,
}

impl AnthropicProxy {
    /// `GT_ANTHROPIC_UPSTREAM` overrides the upstream base URL (tests, alt deployments).
    pub fn new(quota: QuotaHandle) -> Self {
        let http = reqwest::Client::builder()
            // Long ceiling: a streaming /v1/messages call can legitimately run minutes.
            .timeout(Duration::from_secs(600))
            .build()
            .expect("reqwest client");
        Self {
            http,
            quota,
            upstream: std::env::var("GT_ANTHROPIC_UPSTREAM")
                .unwrap_or_else(|_| DEFAULT_UPSTREAM.to_string())
                .trim_end_matches('/')
                .to_string(),
        }
    }

    /// Catch-all router: every method/path forwards.
    pub fn router(self: Arc<Self>) -> Router {
        Router::new().fallback(forward).with_state(self)
    }
}

async fn forward(
    State(proxy): State<Arc<AnthropicProxy>>,
    req: axum::extract::Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let account = header_str(&parts.headers, ACCOUNT_HEADER);
    let session = header_str(&parts.headers, SESSION_HEADER).unwrap_or_else(|| "proxy".into());
    let path_q = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", proxy.upstream, path_q);

    let body_bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("proxy: read body: {e}")).into_response(),
    };

    let mut out_headers = parts.headers.clone();
    // Hop-by-hop + recomputed-by-reqwest + our attribution headers never travel upstream.
    for h in ["host", "content-length", "connection", "transfer-encoding", ACCOUNT_HEADER, SESSION_HEADER] {
        out_headers.remove(h);
    }

    let upstream_resp = match proxy
        .http
        .request(parts.method.clone(), &url)
        .headers(out_headers)
        .body(body_bytes)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("proxy: upstream: {e}")).into_response()
        }
    };

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();

    // --- Per-call quota truth, before the body even starts flowing -----------------------
    if let Some(account) = &account {
        record_headers(&proxy.quota, account, &resp_headers).await;
    }

    let is_messages = parts.uri.path().ends_with("/v1/messages");
    let is_sse = resp_headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("text/event-stream"))
        .unwrap_or(false);

    let mut builder = Response::builder().status(status);
    for (k, v) in resp_headers.iter() {
        // axum recomputes framing; forwarding these can desync the client.
        if k == "transfer-encoding" || k == "content-length" {
            continue;
        }
        builder = builder.header(k, v);
    }

    let body = if is_sse {
        // Stream through unbuffered; tee the transcript for the end-of-stream sample.
        let stream = upstream_resp.bytes_stream();
        match (account.as_ref().filter(|_| is_messages), status.is_success()) {
            (Some(acct), true) => {
                let quota = proxy.quota.clone();
                let acct = acct.clone();
                let session = session.clone();
                Body::from_stream(TeeStream::new(stream, move |transcript: Vec<u8>| {
                    let text = String::from_utf8_lossy(&transcript).into_owned();
                    tokio::spawn(async move {
                        sample_body(&quota, &acct, &session, None, Some(&text)).await;
                    });
                }))
            }
            _ => Body::from_stream(stream),
        }
    } else {
        match upstream_resp.bytes().await {
            Ok(bytes) => {
                if is_messages && status.is_success() {
                    if let Some(acct) = &account {
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        sample_body(&proxy.quota, acct, &session, Some(&text), None).await;
                    }
                }
                Body::from(bytes)
            }
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("proxy: upstream body: {e}"))
                    .into_response()
            }
        }
    };

    builder
        .body(body)
        .unwrap_or_else(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("proxy: {e}")).into_response())
}

/// Apply the response headers' verdicts to the quota actor: unified status first (the
/// reactive signal), then the tokens-family probe (window reconciliation).
async fn record_headers(quota: &QuotaHandle, account: &str, headers: &HeaderMap) {
    let pairs: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect();
    let now = now_secs();
    if let Some(unified) = parse_unified_ratelimit(&pairs) {
        match unified.status {
            UnifiedStatus::Rejected => {
                eprintln!(
                    "[anthropic-proxy] {account}: REJECTED (resets {:?}) — blocking account",
                    unified.resets_at_secs
                );
                quota.blocked(account, unified.resets_at_secs, now).await;
            }
            UnifiedStatus::AllowedWarning => {
                eprintln!("[anthropic-proxy] {account}: allowed_warning — draining account");
                quota.limited(account, now).await;
            }
            UnifiedStatus::Allowed => {}
        }
    }
    if let Some(probe) = parse_anthropic_ratelimit(&pairs, account, now) {
        quota
            .probe(
                probe.account,
                probe.remaining,
                probe.resets_at_secs,
                probe.weekly_remaining,
                probe.weekly_resets_at_secs,
                probe.now_secs,
            )
            .await;
    }
}

/// Record the API-reported usage block from a response body (JSON or SSE transcript).
async fn sample_body(
    quota: &QuotaHandle,
    account: &str,
    session: &str,
    json: Option<&str>,
    sse: Option<&str>,
) {
    let usage = json
        .and_then(parse_messages_json_usage)
        .or_else(|| sse.and_then(parse_messages_sse_usage));
    let Some(u) = usage else { return };
    quota
        .sample(
            account,
            session,
            &u.model,
            u.input,
            u.output,
            u.cache_read,
            u.cache_creation,
            now_secs(),
        )
        .await;
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Pass chunks through untouched while accumulating a copy; fires `on_done` with the full
/// transcript when the upstream stream ends. A client disconnect drops the stream without
/// firing — a partial transcript has no trustworthy usage block anyway.
struct TeeStream<S, F>
where
    F: FnOnce(Vec<u8>) + Send,
{
    inner: Pin<Box<S>>,
    buf: Vec<u8>,
    on_done: Option<F>,
}

impl<S, F> TeeStream<S, F>
where
    F: FnOnce(Vec<u8>) + Send,
{
    fn new(inner: S, on_done: F) -> Self {
        Self {
            inner: Box::pin(inner),
            buf: Vec::new(),
            on_done: Some(on_done),
        }
    }
}

impl<S, E, F> Stream for TeeStream<S, F>
where
    S: Stream<Item = Result<Bytes, E>>,
    F: FnOnce(Vec<u8>) + Send + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.buf.extend_from_slice(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(None) => {
                if let Some(f) = this.on_done.take() {
                    f(std::mem::take(&mut this.buf));
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tee must hand back every chunk untouched and fire exactly once with the full
    /// accumulated transcript at end-of-stream.
    #[tokio::test]
    async fn tee_passes_chunks_through_and_fires_on_done_with_transcript() {
        use std::sync::Mutex;
        let chunks: Vec<Result<Bytes, std::convert::Infallible>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ];
        let inner = futures_util_stream(chunks);
        let got: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let got2 = got.clone();
        let mut tee = TeeStream::new(inner, move |buf| {
            *got2.lock().unwrap() = Some(buf);
        });

        let mut out = Vec::new();
        // Poll manually without futures-util: a noop waker loop suffices for a ready stream.
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut tee).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(b))) => out.extend_from_slice(&b),
                Poll::Ready(Some(Err(_))) => unreachable!(),
                Poll::Ready(None) => break,
                Poll::Pending => unreachable!("test stream is always ready"),
            }
        }
        assert_eq!(out, b"hello world");
        assert_eq!(got.lock().unwrap().as_deref(), Some(b"hello world".as_ref()));
    }

    /// Minimal always-ready stream from a Vec, no futures-util dependency.
    fn futures_util_stream<T: Unpin>(items: Vec<T>) -> impl Stream<Item = T> {
        struct VecStream<T>(std::vec::IntoIter<T>);
        impl<T> Stream for VecStream<T>
        where
            T: Unpin,
        {
            type Item = T;
            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Poll::Ready(self.0.next())
            }
        }
        VecStream(items.into_iter())
    }
}
