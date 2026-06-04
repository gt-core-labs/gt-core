//! [`WorkspaceContext`] — the axum extractor that resolves a request's tenant
//! boundary (`hq-mt-core.8`). Compiled only under the `axum` feature.
//!
//! Every multi-tenant handler needs the [`WorkspaceId`] it operates under, and
//! docs/04 rule 15 is non-negotiable: that id is taken from the request's auth
//! context, **never** from a URL path or body (those are spoofable). This
//! extractor is the single injection point — a handler that takes
//! [`WorkspaceContext`] cannot accidentally read the workspace from anywhere
//! else.
//!
//! ## Resolution sources
//!
//! Two sources resolve the tenant: the `X-GT-Workspace` header and a verified JWT
//! claim. [`from_request_parts`](WorkspaceContext::from_request_parts) tries the
//! header first, then falls back to a [`WorkspaceClaim`] left in the request
//! extensions. A request that carries neither is
//! [`Missing`](WorkspaceContextRejection::Missing).
//!
//! ### Why the JWT claim arrives as an extension, not a token (`hq-auth-context.1`)
//!
//! This crate must not depend on `gt-auth` — `platform → platform` deps are
//! forbidden (docs/03 Rule 4) — so it never sees a bearer token or the
//! signature-verification machinery. Instead the **auth middleware one tier up**
//! (orchestration / composition root, which may depend on both `gt-auth` and this
//! crate) verifies the token and drops the asserted workspace slug into the
//! request extensions as a [`WorkspaceClaim`]. The extractor reads that
//! gt-workspace-owned type — a plain slug — so the tier boundary stays inward and
//! the spoof-proof invariant (docs/04 rule 15: never from URL/body) holds: the
//! claim is server-injected, never client-supplied. Reconciling a header that
//! *disagrees* with the claim is `hq-auth-context.2`; this bead only adds the
//! header-or-claim fallback.

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::{WorkspaceId, WorkspaceIdError};

/// Request header carrying the target workspace slug.
pub const WORKSPACE_HEADER: &str = "X-GT-Workspace";

/// The workspace slug a verified JWT asserted, injected into the request
/// extensions by the upstream auth middleware (`hq-auth-context.1`).
///
/// gt-workspace owns this type so the extractor can read a verified claim without
/// depending on `gt-auth` (Rule 4). The middleware that mints it lives one tier up
/// and is the *only* sanctioned producer — a handler never constructs one from
/// client input, preserving the server-injected invariant (docs/04 rule 15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceClaim(pub String);

impl WorkspaceClaim {
    /// The asserted workspace slug.
    pub fn slug(&self) -> &str {
        &self.0
    }
}

/// The resolved tenant boundary for a request.
///
/// Hold one in a handler signature to require — and obtain — the request's
/// [`WorkspaceId`] through the sanctioned path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceContext {
    workspace: WorkspaceId,
}

impl WorkspaceContext {
    /// Borrow the resolved workspace id.
    pub fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// Consume the context, yielding the owned workspace id.
    pub fn into_workspace(self) -> WorkspaceId {
        self.workspace
    }
}

/// Why a request's [`WorkspaceContext`] could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkspaceContextRejection {
    /// Neither an `X-GT-Workspace` header nor a [`WorkspaceClaim`] extension was
    /// present — no tenant could be resolved (see module docs).
    Missing,
    /// The header value was not valid ASCII / printable header text.
    InvalidHeaderEncoding,
    /// The header value was present but not a valid workspace slug.
    InvalidId(WorkspaceIdError),
}

impl std::fmt::Display for WorkspaceContextRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkspaceContextRejection::Missing => {
                write!(f, "missing {WORKSPACE_HEADER} header")
            }
            WorkspaceContextRejection::InvalidHeaderEncoding => {
                write!(f, "{WORKSPACE_HEADER} header is not valid text")
            }
            WorkspaceContextRejection::InvalidId(e) => {
                write!(f, "invalid workspace id: {e}")
            }
        }
    }
}

impl std::error::Error for WorkspaceContextRejection {}

impl IntoResponse for WorkspaceContextRejection {
    fn into_response(self) -> Response {
        // A missing or malformed workspace selector is a client error; a
        // mismatch with auth scope (once JWT lands) will be the spoof-rejection
        // path docs/04 rule 15 describes.
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for WorkspaceContext
where
    S: Send + Sync,
{
    type Rejection = WorkspaceContextRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Header first. Its presence (even if malformed) is an explicit selector,
        // so a bad header surfaces its own rejection rather than silently falling
        // through to the claim.
        if let Some(raw) = parts.headers.get(WORKSPACE_HEADER) {
            let slug = raw
                .to_str()
                .map_err(|_| WorkspaceContextRejection::InvalidHeaderEncoding)?;
            let workspace =
                WorkspaceId::new(slug).map_err(WorkspaceContextRejection::InvalidId)?;
            return Ok(WorkspaceContext { workspace });
        }
        // Fallback: the verified JWT claim the upstream auth middleware injected.
        if let Some(claim) = parts.extensions.get::<WorkspaceClaim>() {
            let workspace =
                WorkspaceId::new(claim.slug()).map_err(WorkspaceContextRejection::InvalidId)?;
            return Ok(WorkspaceContext { workspace });
        }
        Err(WorkspaceContextRejection::Missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    /// Run the extractor over a request carrying `header` (or none).
    async fn extract(
        header: Option<&str>,
    ) -> Result<WorkspaceContext, WorkspaceContextRejection> {
        extract_with(header, None).await
    }

    /// Run the extractor over a request carrying `header` and/or an injected
    /// [`WorkspaceClaim`] extension.
    async fn extract_with(
        header: Option<&str>,
        claim: Option<&str>,
    ) -> Result<WorkspaceContext, WorkspaceContextRejection> {
        let mut builder = Request::builder();
        if let Some(value) = header {
            builder = builder.header(WORKSPACE_HEADER, value);
        }
        let req = builder.body(()).unwrap();
        let (mut parts, _) = req.into_parts();
        if let Some(slug) = claim {
            parts.extensions.insert(WorkspaceClaim(slug.to_string()));
        }
        WorkspaceContext::from_request_parts(&mut parts, &()).await
    }

    #[tokio::test]
    async fn valid_header_resolves_workspace() {
        let ctx = extract(Some("acme")).await.unwrap();
        assert_eq!(ctx.workspace().as_str(), "acme");
    }

    #[tokio::test]
    async fn missing_header_and_claim_is_rejected() {
        assert_eq!(extract(None).await, Err(WorkspaceContextRejection::Missing));
    }

    #[tokio::test]
    async fn jwt_claim_resolves_when_no_header() {
        let ctx = extract_with(None, Some("acme")).await.unwrap();
        assert_eq!(ctx.workspace().as_str(), "acme");
    }

    #[tokio::test]
    async fn header_takes_precedence_over_claim() {
        // hq-auth-context.2 will *reconcile* a disagreement; for now the header,
        // an explicit selector, simply wins.
        let ctx = extract_with(Some("acme"), Some("other")).await.unwrap();
        assert_eq!(ctx.workspace().as_str(), "acme");
    }

    #[tokio::test]
    async fn an_invalid_claim_slug_is_rejected() {
        let err = extract_with(None, Some("Bad_Id")).await.unwrap_err();
        assert!(matches!(err, WorkspaceContextRejection::InvalidId(_)));
    }

    #[tokio::test]
    async fn a_malformed_header_does_not_fall_through_to_a_valid_claim() {
        // A present-but-bad header surfaces its own rejection rather than silently
        // using the claim — the header is an explicit, deliberate selector.
        let err = extract_with(Some("Bad_Id"), Some("acme")).await.unwrap_err();
        assert!(matches!(err, WorkspaceContextRejection::InvalidId(_)));
    }

    #[tokio::test]
    async fn invalid_slug_is_rejected() {
        let err = extract(Some("Bad_Id")).await.unwrap_err();
        assert!(matches!(err, WorkspaceContextRejection::InvalidId(_)));
    }

    #[tokio::test]
    async fn empty_header_is_rejected_as_invalid_id() {
        let err = extract(Some("")).await.unwrap_err();
        assert!(matches!(err, WorkspaceContextRejection::InvalidId(WorkspaceIdError::Empty)));
    }

    #[test]
    fn rejection_maps_to_400() {
        let resp = WorkspaceContextRejection::Missing.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
