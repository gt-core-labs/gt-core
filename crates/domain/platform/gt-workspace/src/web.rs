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
//! Two sources are specified: the `X-GT-Workspace` header and a JWT claim. Only
//! the header path is implemented — the JWT/auth layer is not yet ported to
//! gt-core (it lives in gastown). When that lands,
//! [`from_request_parts`](WorkspaceContext::from_request_parts) gains a fallback:
//! header first, then the verified JWT claim. Until then a request without the
//! header is [`Missing`](WorkspaceContextRejection::Missing).

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::{WorkspaceId, WorkspaceIdError};

/// Request header carrying the target workspace slug.
pub const WORKSPACE_HEADER: &str = "X-GT-Workspace";

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
    /// No `X-GT-Workspace` header (and no JWT fallback yet — see module docs).
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
        // Header path. JWT-claim fallback is deferred until the auth layer ports
        // to gt-core (gap arch.hq-mt-core.8.web-context-tier-and-jwt).
        let raw = parts
            .headers
            .get(WORKSPACE_HEADER)
            .ok_or(WorkspaceContextRejection::Missing)?;
        let slug = raw
            .to_str()
            .map_err(|_| WorkspaceContextRejection::InvalidHeaderEncoding)?;
        let workspace =
            WorkspaceId::new(slug).map_err(WorkspaceContextRejection::InvalidId)?;
        Ok(WorkspaceContext { workspace })
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
        let mut builder = Request::builder();
        if let Some(value) = header {
            builder = builder.header(WORKSPACE_HEADER, value);
        }
        let (mut parts, _) = builder.body(()).unwrap().into_parts();
        WorkspaceContext::from_request_parts(&mut parts, &()).await
    }

    #[tokio::test]
    async fn valid_header_resolves_workspace() {
        let ctx = extract(Some("acme")).await.unwrap();
        assert_eq!(ctx.workspace().as_str(), "acme");
    }

    #[tokio::test]
    async fn missing_header_is_rejected() {
        assert_eq!(extract(None).await, Err(WorkspaceContextRejection::Missing));
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
