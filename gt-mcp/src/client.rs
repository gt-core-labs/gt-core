//! Thin REST client over gt-mcp-server (hq-gt-cli.2): login + the workspace/rig
//! catalogs `gt init` lets the user pick from. Endpoints, all on the same origin the
//! `.gt-config` `server_url` names:
//!
//! - `POST /auth/login`        → `{ access_token, refresh_token, .. }`
//! - `POST /auth/refresh`      → same, rotating the pair (used by `gt mcp` on a 401)
//! - `GET  /api/v1/workspace`  → `{ "workspaces": [ { id, name, status } ] }`
//! - `GET  /api/v1/rig`        → `{ "rigs": [ { name, prefix, .. } ] }` (per the token's tenant)

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// A minted token pair, mirroring the server's `TokenResponse` (only the two fields
/// the CLI persists).
#[derive(Debug, Clone, Deserialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
}

/// One workspace from `GET /api/v1/workspace` — `id` is the `X-Workspace` value.
#[derive(Debug, Clone, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub status: String,
}

/// One rig from `GET /api/v1/rig` — `prefix` is what `issues.create` routes on.
#[derive(Debug, Clone, Deserialize)]
pub struct Rig {
    pub name: String,
    pub prefix: String,
}

#[derive(Deserialize)]
struct Workspaces {
    workspaces: Vec<Workspace>,
}

#[derive(Deserialize)]
struct Rigs {
    rigs: Vec<Rig>,
}

/// A reqwest client bound to one server base URL (trailing slash trimmed).
pub struct Client {
    http: reqwest::Client,
    base: String,
}

impl Client {
    pub fn new(base: &str) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .build()
                .context("build HTTP client")?,
            base: base.trim_end_matches('/').to_string(),
        })
    }

    /// `POST /auth/login`. A non-2xx (e.g. 401 bad creds) surfaces the server's body.
    pub async fn login(&self, email: &str, password: &str) -> Result<Tokens> {
        let url = format!("{}/auth/login", self.base);
        let resp = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "email": email, "password": password }))
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        Self::json_or_err(resp, "login").await
    }

    /// `POST /auth/refresh`, exchanging an opaque refresh token for a fresh pair.
    /// Wired for the planned in-proxy refresh-on-401 (`gt mcp`); see [`crate::mcp`].
    #[allow(dead_code)]
    pub async fn refresh(&self, refresh_token: &str) -> Result<Tokens> {
        let url = format!("{}/auth/refresh", self.base);
        let resp = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "refresh_token": refresh_token }))
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        Self::json_or_err(resp, "refresh").await
    }

    /// `GET /api/v1/workspace` — the catalog the wizard offers as the first choice.
    pub async fn list_workspaces(&self, access_token: &str) -> Result<Vec<Workspace>> {
        let url = format!("{}/api/v1/workspace", self.base);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(access_token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let body: Workspaces = Self::json_or_err(resp, "list workspaces").await?;
        Ok(body.workspaces)
    }

    /// `GET /api/v1/rig` for the chosen workspace. The tenant is taken from the token
    /// claim; `X-Workspace` is sent too so a non-default pick is honored where the
    /// server reads it.
    pub async fn list_rigs(&self, access_token: &str, workspace: &str) -> Result<Vec<Rig>> {
        let url = format!("{}/api/v1/rig", self.base);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(access_token)
            .header("X-Workspace", workspace)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let body: Rigs = Self::json_or_err(resp, "list rigs").await?;
        Ok(body.rigs)
    }

    /// Deserialize a 2xx body, or fail with `<what> failed (HTTP <code>): <body>` so the
    /// user sees the server's own message (bad creds, missing scope, …).
    async fn json_or_err<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
        what: &str,
    ) -> Result<T> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("{what} failed (HTTP {}): {}", status.as_u16(), text.trim());
        }
        serde_json::from_str(&text).with_context(|| format!("parse {what} response: {text}"))
    }
}
