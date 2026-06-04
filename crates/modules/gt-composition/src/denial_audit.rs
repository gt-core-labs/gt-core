//! Audit-on-denial for the HTTP auth/scope guard chain (`hq-auth-guard.3`).
//!
//! Every *denial* in the guard chain leaves a trail in the [`AuditSink`]; a successful
//! request does not — only refusals are recorded on this HTTP path. The denial vocabulary
//! is exact: `401` is **unauthenticated** (no token, or a token we could not verify), `403`
//! is **forbidden** (a verified identity that lacks the required scope). Two denial points
//! feed one record shape:
//!
//! - **401 — invalid/malformed token.** Produced by [`authenticate`](crate::auth) *before*
//!   any identity exists. It records the denial inline (it holds the sink) because the
//!   rejected response short-circuits and never reaches a downstream layer.
//! - **401 (anonymous) / 403 (missing scope).** Produced by the kernel scope guard
//!   ([`guard_module_scopes`](gt_module::guard_module_scopes)). The [`audit_denials`] layer
//!   sits just *inside* [`authenticate`](crate::auth::authenticate) (so the verified
//!   [`JwtClaims`] are already in the request) and just *outside* the guard (so it observes
//!   the guard's response). It records iff the response carries the kernel's [`ScopeDenied`]
//!   marker — the precise signal that the refusal is the guard's, never a handler's own
//!   `401`/`403`.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::Response;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use gt_audit::{AuditRecord, AuditSink, Outcome};
use gt_auth::JwtClaims;
use gt_module::ScopeDenied;

/// A shareable audit sink handle the guard chain records denials into.
pub type SharedAudit = Arc<dyn AuditSink + Send + Sync>;

/// Actor label for a request that carries no verified identity (no token, or a token the
/// verifier rejected — a credential we could not trust names no one).
pub(crate) const ANONYMOUS: &str = "anonymous";

/// Tower layer that audits the kernel scope guard's denials. Wire it with
/// [`axum::middleware::from_fn_with_state`] passing a [`SharedAudit`], layered to run
/// *after* [`authenticate`](crate::auth::authenticate) and *before* the kernel guard.
///
/// It records a denial iff the downstream response carries [`ScopeDenied`]; a `200` (or any
/// non-guard response) is left untouched, so successful access is not audited here.
pub async fn audit_denials(State(sink): State<SharedAudit>, req: Request, next: Next) -> Response {
    // Snapshot what the record needs before the request is consumed. The verified identity
    // (if any) was injected upstream by `authenticate`; an anonymous request has none —
    // exactly the kernel's `401` case.
    let (actor, workspace) = match req.extensions().get::<JwtClaims>() {
        Some(c) => (Some(c.sub.clone()), Some(c.workspace.clone())),
        None => (None, None),
    };
    let method = req.method().clone();
    let uri = req.uri().clone();

    let resp = next.run(req).await;

    // Only the kernel scope guard stamps `ScopeDenied`; its presence means THIS response is
    // a guard denial, so a handler's own `401`/`403` is never mistaken for one.
    if let Some(denied) = resp.extensions().get::<ScopeDenied>() {
        let required = denied.required.as_str().to_string();
        let status = denied.status;
        record_denial(
            sink.as_ref(),
            actor.as_deref().unwrap_or(ANONYMOUS),
            workspace.as_deref(),
            &method,
            &uri,
            Some(&required),
            status,
        );
    }
    resp
}

/// Append one denial to the sink. Shared by the auth layer (token rejection) and the
/// scope-guard audit layer. Best-effort: a sink error must never fail the request, so it is
/// dropped — the denial already happened and is itself the security-relevant outcome.
pub(crate) fn record_denial(
    sink: &dyn AuditSink,
    actor: &str,
    workspace: Option<&str>,
    method: &Method,
    uri: &Uri,
    required_scope: Option<&str>,
    status: StatusCode,
) {
    // route/method as the audited "tool"; required scope + verdict in the type-erased args,
    // mirroring how the MCP dispatch path records its unauthorized attempts.
    let mut record = AuditRecord::new(
        actor,
        format!("{method} {}", uri.path()),
        serde_json::json!({
            "required_scope": required_scope,
            "status": status.as_u16(),
            "verdict": verdict(status),
        }),
        Outcome::Unauthorized,
        now_rfc3339(),
    );
    if let Some(ws) = workspace {
        record = record.in_workspace(ws);
    }
    let _ = sink.record(record);
}

/// The denial vocabulary in words: `401` ⇒ *unauthenticated*, `403` ⇒ *forbidden*. The guard
/// chain only ever produces these two; any other status is labelled generically.
fn verdict(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "unauthenticated",
        StatusCode::FORBIDDEN => "forbidden",
        _ => "denied",
    }
}

/// Current time as an RFC3339 string for the record `ts`. The kernel audit crate stays
/// clock-free; this tier (the server) stamps the time, as the MCP dispatch path does.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{authenticate, AuthState, SharedAuthenticator};
    use crate::scope_bridge::bridge_scopes;
    use axum::body::Body;
    use axum::http::header::AUTHORIZATION;
    use axum::routing::get;
    use axum::Router;
    use gt_audit::InMemoryAudit;
    use gt_auth::{InMemoryAuthenticator, JwtClaims};
    use gt_module::{guard_module_scopes, ModuleId, Scope};
    use tower::ServiceExt; // oneshot

    fn claims(scopes: &[&str]) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            workspace: "acme".into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            exp: 9_999_999_999,
            nbf: None,
            iat: 0,
        }
    }

    /// The full guard chain over a `beads` module that claims `beads.read`/`beads.write`:
    /// `authenticate` → `audit_denials` → `bridge_scopes` → kernel guard → handler. Both
    /// denial-recording layers share one in-memory sink.
    fn app(audit: SharedAudit, granted: &[&str]) -> Router {
        let verifier: SharedAuthenticator =
            Arc::new(InMemoryAuthenticator::new().with_token("good", claims(granted)));
        let scopes: Vec<Scope> = ["beads.read", "beads.write"]
            .into_iter()
            .map(|s| Scope::new(s).unwrap())
            .collect();
        let guarded = guard_module_scopes(
            Router::new().route("/list", get(|| async { "ok" })),
            &ModuleId::new("beads").unwrap(),
            &scopes,
        );
        guarded
            .layer(axum::middleware::from_fn(bridge_scopes))
            .layer(axum::middleware::from_fn_with_state(audit.clone(), audit_denials))
            .layer(axum::middleware::from_fn_with_state(
                AuthState::new(verifier, audit),
                authenticate,
            ))
    }

    async fn call(audit: SharedAudit, granted: &[&str], header: Option<&str>) -> StatusCode {
        let mut builder = Request::builder().uri("/list");
        if let Some(h) = header {
            builder = builder.header(AUTHORIZATION, h);
        }
        app(audit, granted)
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn no_token_audits_a_401_unauthenticated_denial() {
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        // No Authorization header: anonymous → kernel guard 401.
        let status = call(audit.clone(), &["beads.read"], None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1, "exactly one denial recorded");
        let r = &rows[0];
        assert_eq!(r.outcome, Outcome::Unauthorized);
        assert_eq!(r.actor, ANONYMOUS);
        assert_eq!(r.tool, "GET /list");
        assert_eq!(r.args["status"], 401);
        assert_eq!(r.args["verdict"], "unauthenticated");
        assert_eq!(r.args["required_scope"], "beads.read");
    }

    #[tokio::test]
    async fn invalid_token_audits_a_401_unauthenticated_denial() {
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        // A present-but-unverifiable token is rejected by `authenticate` itself (401),
        // short of the scope guard, so it records the denial inline.
        let status = call(audit.clone(), &["beads.read"], Some("Bearer nope")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.outcome, Outcome::Unauthorized);
        assert_eq!(r.actor, ANONYMOUS);
        assert_eq!(r.tool, "GET /list");
        assert_eq!(r.args["status"], 401);
        assert_eq!(r.args["verdict"], "unauthenticated");
        // No scope was ever resolved — the token failed before the guard.
        assert_eq!(r.args["required_scope"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn valid_token_without_scope_audits_a_403_forbidden_denial() {
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        // Verified identity holding `rig.read` hits a `beads.read`-gated route → 403.
        let status = call(audit.clone(), &["rig.read"], Some("Bearer good")).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let rows = audit.read_all().unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.outcome, Outcome::Unauthorized);
        assert_eq!(r.actor, "alice");
        assert_eq!(r.workspace_id, "acme");
        assert_eq!(r.tool, "GET /list");
        assert_eq!(r.args["status"], 403);
        assert_eq!(r.args["verdict"], "forbidden");
        assert_eq!(r.args["required_scope"], "beads.read");
    }

    #[tokio::test]
    async fn authorized_request_audits_no_denial() {
        let audit: SharedAudit = Arc::new(InMemoryAudit::new());
        // Verified identity holding `beads.read` → 200, and no denial is recorded.
        let status = call(audit.clone(), &["beads.read"], Some("Bearer good")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(audit.read_all().unwrap().is_empty(), "success is not audited here");
    }
}
