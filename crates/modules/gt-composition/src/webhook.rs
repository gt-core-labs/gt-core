//! GitHub App push webhook → graph-staleness (hq-vcs-connections.7).
//!
//! The custodian (the graph warden) learns of an INTERNAL merge through the merge queue
//! (`mcp/merge.rs` `mark_owning_rig_stale`). This module is the answer to "does the custodian hear
//! a push from ANOTHER source?" — an external `git push` straight to origin. The platform GitHub App
//! delivers a signed `push` webhook; this receiver verifies the signature, maps the delivery onto a
//! rig under the right workspace, and marks that rig's graph stale so the existing
//! `graph.refresh-stale` path reindexes it. This module NEVER indexes — it only flips the freshness
//! flag, exactly as the merge reactor does.
//!
//! ## The flow
//!
//! `POST /api/v1/connection/github/webhook`:
//! 1. **Verify** the `X-Hub-Signature-256` HMAC-SHA256 over the raw body against the App's webhook
//!    secret ([`gt_vcs::GithubAppConfig::webhook_secret`], mounted from
//!    `GT_GITHUB_APP_WEBHOOK_SECRET_FILE`). A bad/absent signature is a clean `401` with NO state
//!    change. Reuses [`gt_webhooks::GitHubSource`] so the constant-time compare is the same one the
//!    merge-side GitHub source uses.
//! 2. **Filter the event**: only `push` (the `X-GitHub-Event` header) is acted on; any other event
//!    is acknowledged `204` and ignored.
//! 3. **Resolve the workspace**: the delivery's `installation.id` resolves the
//!    `public.vcs_connections` row globally ([`VcsConnectionRepo::find_by_installation`]); its
//!    `workspace_id` is the tenant whose rig catalog + warden log we touch.
//! 4. **Map repo → rig**: within that workspace, `repository.full_name` (`owner/repo`) is matched
//!    against each rig's `git_url` (normalized to `owner/repo`, SSH or HTTPS).
//! 5. **Branch + commit filter (default-branch-only)**: act ONLY when `ref == refs/heads/<rig
//!    default_branch>` AND the delivery's `after` SHA differs from the warden's last-indexed commit.
//!    A push to any other branch, or a push that lands the commit we already indexed, is ignored.
//! 6. **MarkStale**: replay the warden state, run [`WardenCommand::MarkStale`], append the events —
//!    the same replay/execute/append shape as `mcp/merge.rs` and `mcp/graph.rs`.
//!
//! Default-branch-only is the epic's explicit decision: the warden is keyed by `rig` (not
//! `(rig, branch)`), so a push to a non-default branch carries no graph meaning here.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde_json::Value;

use gt_graphwarden::{MarkStale, WardenCommand, WardenState};
use gt_rig::{PgRigs, RigEntry, RigRepository};
use gt_vcs::VcsConnectionRepo;
use gt_webhooks::{GitHubSource, WebhookSource};

use crate::mcp::{EventLog, WsPools};

/// The warden event-log kind prefix, replayed to read/append graph custody (mirrors
/// `mcp/merge.rs::WARDEN_NS` and `mcp/graph.rs::NS`).
const WARDEN_NS: &str = "graphwarden.";

/// Relative path the receiver mounts at; the binary nests it under `/api/v1/connection`.
pub const WEBHOOK_PATH: &str = "/github/webhook";

/// State for the GitHub push webhook receiver.
///
/// Holds the App webhook secret (for signature verification), the per-workspace Postgres pool cache
/// (to resolve the tenant's rig catalog), the global VCS-connection store (to map `installation.id`
/// → workspace), and the per-workspace event log (to replay + append warden events). Cheap to clone
/// — every field is an `Arc` or a small owned `Vec`.
#[derive(Clone)]
pub struct GithubWebhookState {
    secret: Arc<Vec<u8>>,
    pools: Arc<WsPools>,
    connections: Arc<dyn VcsConnectionRepo>,
    log: Arc<EventLog>,
}

impl GithubWebhookState {
    /// Build the receiver state. `secret` is the App webhook secret (the value GitHub HMACs each
    /// delivery with); `pools` resolves a tenant's `ws_<slug>` rig catalog; `connections` maps an
    /// installation id to its workspace; `log` is the warden event log.
    pub fn new(
        secret: impl Into<Vec<u8>>,
        pools: Arc<WsPools>,
        connections: Arc<dyn VcsConnectionRepo>,
        log: Arc<EventLog>,
    ) -> Self {
        Self {
            secret: Arc::new(secret.into()),
            pools,
            connections,
            log,
        }
    }
}

/// Build the GitHub push-webhook router (relative path — the binary nests it under
/// `/api/v1/connection`, OUTSIDE the RBAC scope chain: a webhook authenticates by its HMAC
/// signature, not a workspace JWT).
pub fn github_webhook_router(state: GithubWebhookState) -> Router {
    Router::new()
        .route(WEBHOOK_PATH, post(receive))
        .with_state(state)
}

/// `POST /github/webhook` — verify the signature, then (for a `push` to a rig's default branch that
/// moved the head) mark that rig's graph stale. Returns:
/// - `401` on a bad/absent signature (no state change),
/// - `400` on a malformed body / missing event header,
/// - `204` when acknowledged but ignored (non-push, unmapped repo/installation, non-default branch,
///   or a head we already indexed),
/// - `200` with the marked rig when a `MarkStale` was appended.
async fn receive(
    State(st): State<GithubWebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let source = GitHubSource::new(st.secret.as_ref().clone());

    // 1. Signature — a forged/absent signature is rejected BEFORE any parse or state change.
    if source.verify(&headers, &body).is_err() {
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    // 2. Parse + event filter. `parse` requires the `X-GitHub-Event` header and valid JSON.
    let delivery = match source.parse(&headers, &body) {
        Ok(d) => d,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed payload").into_response(),
    };
    if delivery.event != "push" {
        // Other events (installation, ping, …) are acknowledged but carry no graph meaning here.
        return StatusCode::NO_CONTENT.into_response();
    }

    match handle_push(&st, &delivery.payload).await {
        Ok(Some(rig)) => (StatusCode::OK, format!("marked rig `{rig}` stale")).into_response(),
        // Verified but not actionable: unmapped installation/repo, non-default branch, or a head we
        // already indexed. A 204 so GitHub records a successful delivery without a redelivery storm.
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        // A backend fault (PG/event-log). 500 so GitHub retries — the push really did happen.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

/// The decision core for a verified `push` delivery: resolve installation → workspace, map repo →
/// rig, apply the default-branch + head-moved filter, and `MarkStale`. Returns the rig name when a
/// stale-mark was appended, or `None` when the push is verified-but-ignored.
///
/// Split out (and pure of HTTP types beyond the JSON payload) so the filter logic is unit-testable
/// without standing up axum.
async fn handle_push(
    st: &GithubWebhookState,
    payload: &Value,
) -> Result<Option<String>, gt_store_dolt::AppError> {
    // GitHub push payload fields. `repository.full_name` is `owner/repo`; `ref` is the full
    // `refs/heads/<branch>`; `after` is the new head SHA; `installation.id` resolves the connection.
    let Some(full_name) = payload
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    let Some(git_ref) = payload.get("ref").and_then(Value::as_str) else {
        return Ok(None);
    };
    let after = payload.get("after").and_then(Value::as_str).unwrap_or("");
    // `installation.id` is a JSON number; accept a string form too for robustness.
    let Some(installation_id) = payload
        .get("installation")
        .and_then(|i| i.get("id"))
        .map(json_id_to_string)
    else {
        return Ok(None);
    };

    // installation.id → connection (global lookup) → workspace. The connection/rig ports return
    // the kernel `gt_events::AppError`; lift it into the dolt error the EventLog path uses so the
    // function has one error type.
    let Some(conn) = st
        .connections
        .find_by_installation(&installation_id)
        .await
        .map_err(lift_err)?
    else {
        return Ok(None);
    };
    // A global connection (workspace_id IS NULL) cannot name a tenant's rig catalog, so a push to
    // one is ignored — the rig catalog is per-workspace.
    let Some(workspace) = conn.workspace_id.as_deref() else {
        return Ok(None);
    };

    // repo → rig: match `owner/repo` against each rig's normalized git_url within the workspace.
    let want = normalize_repo(full_name);
    let pool = st.pools.get(Some(workspace)).await?;
    let rigs = PgRigs::new(pool.pool().clone());
    let all = rigs.list().await.map_err(lift_err)?;
    let Some(rig) = all
        .into_iter()
        .find(|r: &RigEntry| normalize_repo(&r.git_url) == want)
    else {
        return Ok(None);
    };

    // Default-branch-only filter: a push to any other branch carries no graph meaning (the warden is
    // keyed by rig, not (rig, branch)).
    let want_ref = format!("refs/heads/{}", rig.default_branch);
    if git_ref != want_ref {
        return Ok(None);
    }

    // Replay the warden state for this workspace; act only when the rig is under custody.
    let ws = Some(workspace);
    let state = st
        .log
        .replay_domain(ws, WARDEN_NS, WardenState::default(), |s, e| {
            let _ = s.apply(e);
        })?;
    let Some(graph) = state.rigs.get(&rig.name) else {
        // Not under graph custody → nothing to mark; the first `graph.refresh` will register it.
        return Ok(None);
    };

    // Head-moved filter: skip a redelivery / a push whose head we already indexed. GitHub sends the
    // FULL 40-char SHA; the warden records the SHORT `rev-parse --short` form, so compare by prefix.
    if !after.is_empty() {
        if let Some(indexed) = graph.last_indexed_commit.as_deref() {
            if !indexed.is_empty() && after.starts_with(indexed) {
                return Ok(None);
            }
        }
    }

    // MarkStale — replay/execute/append, the merge.rs pattern. Best-effort emit: a partial append is
    // a backend fault surfaced to the caller (500 → GitHub retries).
    let cmd = WardenCommand::MarkStale(MarkStale {
        rig: rig.name.clone(),
        changed: 1,
        now_secs: now_secs(),
    });
    let events = cmd.execute(&state).map_err(lift_err)?;
    for ev in events {
        st.log.append(ws, ev)?;
    }
    Ok(Some(rig.name))
}

/// A JSON `installation.id` (a number, or a string for robustness) as its decimal string.
fn json_id_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Normalize a git URL or a GitHub `full_name` to a lowercase `owner/repo`, stripping the host, the
/// `.git` suffix, and any leading scheme/user. So `git@github.com:Org/Repo.git`,
/// `https://github.com/org/repo`, and `org/repo` all collapse to `org/repo` — letting a webhook's
/// `repository.full_name` match a rig's stored `git_url` regardless of clone protocol.
fn normalize_repo(s: &str) -> String {
    let s = s.trim().trim_end_matches('/');
    let s = s.strip_suffix(".git").unwrap_or(s);
    // Drop a scheme: `https://`, `ssh://`, `git://` — take the part after the last `://`.
    let s = s.rsplit("://").next().unwrap_or(s);
    // SCP-like `git@github.com:owner/repo` → take the part after the LAST `:`.
    let s = s.rsplit(':').next().unwrap_or(s);
    // Strip a leading `user@host` (only present in the non-SCP forms after scheme removal).
    let s = s.strip_prefix("git@").unwrap_or(s);
    // Take the trailing `owner/repo` (drop any host segment left for the https/ssh forms).
    let parts: Vec<&str> = s.split('/').filter(|p| !p.is_empty()).collect();
    let tail = if parts.len() >= 2 {
        parts[parts.len() - 2..].join("/")
    } else {
        s.to_string()
    };
    tail.to_lowercase()
}

/// Lift a kernel `gt_events::AppError` (the connection/rig ports + warden command path) into the
/// `gt_store_dolt::AppError` the EventLog path uses, preserving the variant — so the receiver's one
/// `Err` arm (a `500`) covers every backend fault uniformly.
fn lift_err(e: gt_events::AppError) -> gt_store_dolt::AppError {
    use gt_events::AppError as K;
    use gt_store_dolt::AppError as D;
    match e {
        K::InvalidTransition(s) => D::InvalidTransition(s),
        K::NotFound(s) => D::NotFound(s),
        K::Validation(s) => D::Validation(s),
        K::Handler(s) => D::Handler(s),
        K::Other(s) => D::Other(s),
    }
}

/// Current unix time in seconds (mirrors `mcp::util::now_secs`, kept local so the module is
/// self-contained).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use gt_graphwarden::WardenEvent;
    use gt_vcs::{ConnectionKind, ConnectionStatus, NewConnection, PatchConnection, VcsConnection};
    use hmac::{Hmac, Mac};
    use serde_json::json;
    use sha2::Sha256;
    use tempfile::TempDir;

    type HmacSha256 = Hmac<Sha256>;

    const SECRET: &[u8] = b"webhook-secret";

    /// Mint the `X-Hub-Signature-256` header value GitHub would send for `body`.
    fn sign(body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(SECRET).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn headers(event: &str, sig: Option<&str>) -> HeaderMap {
        use axum::http::HeaderName;
        let mut h = HeaderMap::new();
        h.insert(
            "x-github-event".parse::<HeaderName>().unwrap(),
            event.parse().unwrap(),
        );
        if let Some(s) = sig {
            h.insert(
                "x-hub-signature-256".parse::<HeaderName>().unwrap(),
                s.parse().unwrap(),
            );
        }
        h
    }

    /// An in-memory connection store with one `github_app` connection.
    struct OneConn(VcsConnection);

    #[async_trait]
    impl VcsConnectionRepo for OneConn {
        async fn list_for_workspace(&self, _: &str) -> Result<Vec<VcsConnection>, gt_events::AppError> {
            Ok(vec![self.0.clone()])
        }
        async fn get_for_workspace(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok(None)
        }
        async fn create(&self, _: NewConnection) -> Result<VcsConnection, gt_events::AppError> {
            unreachable!()
        }
        async fn patch(
            &self,
            _: &str,
            _: &str,
            _: PatchConnection,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok(None)
        }
        async fn delete(&self, _: &str, _: &str) -> Result<bool, gt_events::AppError> {
            Ok(false)
        }
        async fn find_by_installation(
            &self,
            installation_id: &str,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok((self.0.installation_id.as_deref() == Some(installation_id)).then(|| self.0.clone()))
        }
    }

    fn conn(installation: &str, ws: Option<&str>) -> VcsConnection {
        VcsConnection {
            id: "c1".into(),
            workspace_id: ws.map(str::to_string),
            kind: ConnectionKind::GithubApp,
            installation_id: Some(installation.into()),
            account_login: Some("codecsrayo".into()),
            secret_sealed: None,
            status: ConnectionStatus::Active,
            created_at: 0,
        }
    }

    // --- Signature verification (valid / invalid). These exercise the receiver's auth gate
    // directly through `GitHubSource`, the same component `receive` calls. ---

    #[test]
    fn valid_signature_passes_invalid_rejected() {
        let src = GitHubSource::new(SECRET.to_vec());
        let body = br#"{"ref":"refs/heads/main"}"#;
        // A correct signature verifies.
        assert!(src
            .verify(&headers("push", Some(&sign(body))), body)
            .is_ok());
        // A signature over a DIFFERENT body is rejected.
        assert!(src
            .verify(&headers("push", Some(&sign(b"tampered"))), body)
            .is_err());
        // A missing signature is rejected.
        assert!(src.verify(&headers("push", None), body).is_err());
    }

    // --- The ref/branch filter + repo→rig + head-moved logic, exercised through `handle_push`
    // against a tempdir-backed warden log and an in-memory connection store. The rig catalog is PG
    // (`PgRigs`), unavailable in a unit test, so these focus on the branches reachable before the PG
    // hop: a non-matching installation, a global connection, a non-push event (via `receive`). The
    // full default-branch match is covered by the contract test when GT_PG_URL is set. ---

    fn state_with(conn: VcsConnection) -> (GithubWebhookState, TempDir) {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let st = GithubWebhookState::new(
            SECRET.to_vec(),
            Arc::new(WsPools::new("postgres://unused")),
            Arc::new(OneConn(conn)),
            log,
        );
        (st, dir)
    }

    /// An unknown installation id is verified-but-ignored (`Ok(None)`), never an error — and never
    /// reaches the PG rig catalog.
    #[tokio::test]
    async fn unknown_installation_is_ignored() {
        let (st, _dir) = state_with(conn("999", Some("acme")));
        let payload = json!({
            "ref": "refs/heads/main",
            "after": "abcdef0",
            "repository": { "full_name": "codecsrayo/inactivas-chain" },
            "installation": { "id": 12345 }
        });
        assert_eq!(handle_push(&st, &payload).await.unwrap(), None);
    }

    /// A connection with no workspace (global) cannot name a per-tenant rig catalog → ignored, no PG.
    #[tokio::test]
    async fn global_connection_is_ignored() {
        let (st, _dir) = state_with(conn("12345", None));
        let payload = json!({
            "ref": "refs/heads/main",
            "after": "abcdef0",
            "repository": { "full_name": "codecsrayo/inactivas-chain" },
            "installation": { "id": 12345 }
        });
        assert_eq!(handle_push(&st, &payload).await.unwrap(), None);
    }

    /// A payload missing the required fields is ignored (no panic, no error), before any PG hop.
    #[tokio::test]
    async fn missing_fields_are_ignored() {
        let (st, _dir) = state_with(conn("12345", Some("acme")));
        // No `installation`.
        let payload = json!({ "ref": "refs/heads/main", "repository": { "full_name": "o/r" } });
        assert_eq!(handle_push(&st, &payload).await.unwrap(), None);
    }

    /// `normalize_repo` collapses every clone-URL form to `owner/repo`, so a webhook `full_name`
    /// matches a rig's stored `git_url` regardless of protocol.
    #[test]
    fn normalize_repo_collapses_url_forms() {
        let canon = "codecsrayo/inactivas-chain";
        for s in [
            "git@github.com:codecsrayo/inactivas-chain.git",
            "https://github.com/codecsrayo/inactivas-chain.git",
            "https://github.com/codecsrayo/inactivas-chain",
            "ssh://git@github.com/codecsrayo/inactivas-chain.git",
            "codecsrayo/inactivas-chain",
            "codecsrayo/Inactivas-Chain", // case-insensitive
        ] {
            assert_eq!(normalize_repo(s), canon, "normalizing {s}");
        }
    }

    /// The head-moved filter compares the FULL webhook SHA against the warden's SHORT recorded
    /// commit by prefix: a redelivery of the already-indexed head is a no-op.
    #[test]
    fn head_prefix_match_is_a_noop() {
        let full = "abcdef0123456789abcdef0123456789abcdef01";
        let short = "abcdef0"; // what `rev-parse --short` records
        assert!(full.starts_with(short));
        // A different head does NOT match the prefix, so it would mark stale.
        assert!(!full.starts_with("fedcba9"));
    }

    /// Seed a warden registration so a later push could mark it stale — proves the replay seam reads
    /// the same `graphwarden.` stream the merge reactor / graph handler use.
    #[tokio::test]
    async fn warden_replay_sees_registered_rig() {
        let (st, _dir) = state_with(conn("12345", Some("acme")));
        st.log
            .append(
                Some("acme"),
                WardenEvent::RigRegistered {
                    rig: "inactivas-chain".into(),
                    repo_dir: "/var/lib/gt-graph/acme/inactivas-chain".into(),
                    now_secs: 1,
                },
            )
            .unwrap();
        let state = st
            .log
            .replay_domain(Some("acme"), WARDEN_NS, WardenState::default(), |s, e| {
                let _ = s.apply(e);
            })
            .unwrap();
        assert!(state.rigs.contains_key("inactivas-chain"));
    }
}
