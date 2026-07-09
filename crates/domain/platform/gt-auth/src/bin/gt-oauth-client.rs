//! `gt-oauth-client` — admin CLI to register / list / revoke OAuth clients (gtcore-95f950).
//!
//! gt-core acts as an authorization SERVER for downstream relying parties (e.g. Claude.ai); this
//! CLI is the admin surface that manages the `public.oauth_clients` registry the `/oauth/authorize`
//! + `/oauth/token` endpoints validate against. It is the "MCP tool or CLI" the bead's acceptance
//! criteria call for, kept a thin wrapper over the [`OauthClientRepo`](gt_auth::OauthClientRepo)
//! port so the same registry backs a future MCP/HTTP surface unchanged.
//!
//! ## Environment
//!
//! - `GT_PG_URL` — the Postgres the registry lives in (required).
//! - `GT_SECRET_KEY` — the 32-byte AES-256-GCM master key (base64/hex) that seals each client
//!   secret at rest (required for `register`; the secret is sealed before it touches the DB).
//!
//! ## Commands
//!
//! ```text
//! gt-oauth-client register --client-id <id> --secret <secret> [--name <label>] \
//!                          --redirect-uri <url> [--redirect-uri <url> ...] \
//!                          [--scope <scope> ...]
//! gt-oauth-client list
//! gt-oauth-client revoke --client-id <id>
//! ```
//!
//! `list` prints each client with its secret REDACTED — the sealed blob never leaves the process.
//! `register` enforces exact-match, wildcard-free redirect URIs (relative/`*`/fragment rejected).

use std::process::ExitCode;

use gt_auth::{migrations, NewOauthClient, OauthClientRepo, PgOauthClientRepo};
use sqlx::PgPool;

const USAGE: &str = "\
gt-oauth-client — manage the OAuth clients that authenticate against gt-core.

USAGE:
    gt-oauth-client register --client-id <id> --secret <secret> [--name <label>] \\
                             --redirect-uri <url> [--redirect-uri <url> ...] \\
                             [--scope <scope> ...]
    gt-oauth-client list
    gt-oauth-client revoke --client-id <id>

ENV:
    GT_PG_URL       Postgres connection string (required).
    GT_SECRET_KEY   32-byte AES-256-GCM master key, base64 or hex (required for register).";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_default();
    let rest: Vec<String> = args.collect();

    match cmd.as_str() {
        "register" => register(&rest).await,
        "list" => list().await,
        "revoke" => revoke(&rest).await,
        "help" | "-h" | "--help" => {
            println!("{USAGE}");
            Ok(())
        }
        "" => Err(format!("a command is required\n\n{USAGE}")),
        other => Err(format!("unknown command: {other}\n\n{USAGE}")),
    }
}

/// Connect to `GT_PG_URL` and ensure `public.oauth_clients` exists (idempotent `IF NOT EXISTS`), so
/// the CLI works against a fresh database without a separate migration step.
async fn pool() -> Result<PgPool, String> {
    let url = std::env::var("GT_PG_URL")
        .map_err(|_| "GT_PG_URL is required (the Postgres connection string)".to_string())?;
    let pool = PgPool::connect(&url)
        .await
        .map_err(|e| format!("connect GT_PG_URL: {e}"))?;
    sqlx::query(migrations::CREATE_OAUTH_CLIENTS)
        .execute(&pool)
        .await
        .map_err(|e| format!("ensure oauth_clients table: {e}"))?;
    Ok(pool)
}

async fn register(args: &[String]) -> Result<(), String> {
    let mut client_id = None;
    let mut secret = None;
    let mut display_name = String::new();
    let mut redirect_uris = Vec::new();
    let mut scopes = Vec::new();

    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--client-id" => client_id = Some(value(&mut it, flag)?),
            "--secret" => secret = Some(value(&mut it, flag)?),
            "--name" => display_name = value(&mut it, flag)?,
            "--redirect-uri" => redirect_uris.push(value(&mut it, flag)?),
            "--scope" => scopes.push(value(&mut it, flag)?),
            other => return Err(format!("unexpected argument: {other}\n\n{USAGE}")),
        }
    }

    let new = NewOauthClient {
        client_id: client_id.ok_or("register: --client-id is required")?,
        client_secret: secret.ok_or("register: --secret is required")?,
        display_name,
        redirect_uris,
        scopes,
    };

    let repo = PgOauthClientRepo::new(pool().await?);
    let stored = repo
        .register(new)
        .await
        .map_err(|e| format!("register: {e}"))?;
    // Echo the redacted record — the secret was sealed and is never printed back.
    println!("registered oauth client:");
    print_client(&stored.redacted());
    Ok(())
}

async fn list() -> Result<(), String> {
    let repo = PgOauthClientRepo::new(pool().await?);
    let clients = repo.list().await.map_err(|e| format!("list: {e}"))?;
    if clients.is_empty() {
        println!("no oauth clients registered");
        return Ok(());
    }
    println!("{} oauth client(s):", clients.len());
    for c in &clients {
        // Render the SECRET-REDACTED view — the sealed blob never leaves the process.
        print_client(&c.redacted());
    }
    Ok(())
}

async fn revoke(args: &[String]) -> Result<(), String> {
    let mut client_id = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--client-id" => client_id = Some(value(&mut it, flag)?),
            other => return Err(format!("unexpected argument: {other}\n\n{USAGE}")),
        }
    }
    let client_id = client_id.ok_or("revoke: --client-id is required")?;
    let repo = PgOauthClientRepo::new(pool().await?);
    if repo
        .revoke(&client_id)
        .await
        .map_err(|e| format!("revoke: {e}"))?
    {
        println!("revoked oauth client: {client_id}");
        Ok(())
    } else {
        Err(format!("no such oauth client: {client_id}"))
    }
}

/// Pull the value following a `--flag`, erroring if it is missing.
fn value(it: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    it.next()
        .cloned()
        .ok_or_else(|| format!("{flag} expects a value"))
}

/// Print a secret-free client view (never the sealed secret).
fn print_client(v: &gt_auth::OauthClientView) {
    println!("  client_id:     {}", v.client_id);
    println!("  display_name:  {}", v.display_name);
    println!("  enabled:       {}", v.enabled);
    println!("  redirect_uris: {}", v.redirect_uris.join(", "));
    println!("  scopes:        {}", v.scopes.join(", "));
}
