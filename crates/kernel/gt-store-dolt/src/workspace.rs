//! Dolt database-per-workspace helpers (hq-mt-data.6).
//!
//! The multi-tenant storage model is **DB-per-workspace**: every tenant gets
//! its own Dolt database `hq_<ws>` (mirroring the PG `schema-per-workspace`
//! decision in docs/04 §15 — see the `hq-mt-data` partitioning resolution).
//! A workspace's issues, beads and version history live in an isolated Dolt
//! database so a tenant can be cloned, branched or dropped without touching its
//! neighbours.
//!
//! This module is deliberately thin and slug-typed: `gt-store-dolt` is a
//! **kernel** crate, so it cannot depend on [`WorkspaceId`] (which lives in the
//! `gt-workspace` *platform* crate — that would be a forbidden downward dep,
//! docs/03 Rule 4). The host tier holds the `WorkspaceId` and passes its
//! validated slug (`WorkspaceId::as_str`) down here. We re-validate the slug
//! locally as defence-in-depth before it is ever interpolated into DDL.

use mysql_async::prelude::*;
use mysql_async::Pool;

use crate::conn::map_err;
use crate::error::AppError;

/// Prefix every workspace Dolt database name carries. Keeps tenant databases in
/// one obvious namespace and out of the way of Dolt's own `information_schema` /
/// `mysql` system databases.
const DB_PREFIX: &str = "hq_";

/// Map a workspace slug to its Dolt database name: `acme` → `hq_acme`.
///
/// Pure and infallible — the caller is expected to pass a slug that already
/// satisfies the [`WorkspaceId`](../../gt-workspace) grammar. The name is *not*
/// quoted here; emitters that splice it into SQL must backtick it (workspace
/// slugs may contain `-`, which is legal inside a backticked identifier but not
/// a bare one). Use [`validate_slug`] first if the input is untrusted.
pub fn dolt_db_name(ws: &str) -> String {
    format!("{DB_PREFIX}{ws}")
}

/// Re-validate a workspace slug against the same lowercase-kebab grammar
/// [`WorkspaceId`](../../gt-workspace) enforces, so a malformed or hostile slug
/// can never reach the DDL string. One or more `[a-z0-9]+` groups joined by
/// single `-`, 1..=63 chars, no leading/trailing/doubled hyphen.
///
/// This duplicates `WorkspaceId::new` on purpose: the kernel may not import the
/// platform type, and DDL interpolation is exactly where a second guard earns
/// its keep.
pub(crate) fn validate_slug(ws: &str) -> Result<(), AppError> {
    if ws.is_empty() {
        return Err(AppError::Validation("workspace slug is empty".into()));
    }
    if ws.len() > 63 {
        return Err(AppError::Validation(format!(
            "workspace slug is {} chars (max 63)",
            ws.len()
        )));
    }
    let bytes = ws.as_bytes();
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(AppError::Validation(
            "workspace slug '-' may not lead or trail".into(),
        ));
    }
    let mut prev_dash = false;
    for c in ws.chars() {
        match c {
            'a'..='z' | '0'..='9' => prev_dash = false,
            '-' => {
                if prev_dash {
                    return Err(AppError::Validation(
                        "workspace slug '-' may not repeat".into(),
                    ));
                }
                prev_dash = true;
            }
            other => {
                return Err(AppError::Validation(format!(
                    "illegal character {other:?} in workspace slug (expected lowercase kebab-case)"
                )))
            }
        }
    }
    Ok(())
}

/// Provision a workspace's Dolt database: `CREATE DATABASE IF NOT EXISTS
/// hq_<ws>`.
///
/// Idempotent — safe to call on every workspace boot. `CREATE DATABASE IF NOT
/// EXISTS` is a no-op when the database already exists. The slug is validated
/// before any string interpolation so this can never become a DDL-injection
/// vector.
///
/// Note Dolt/MySQL DDL does not bind identifiers as parameters, so the database
/// name is interpolated; [`validate_slug`] guarantees it is `[a-z0-9-]` only and
/// it is backticked in the emitted statement.
///
/// No explicit `GRANT` is emitted: the connecting server user is the deploy's
/// application account (`gtapp`, which holds `GRANT ALL ON *.* WITH GRANT
/// OPTION`), so it already reaches a freshly-created `hq_<ws>`. The previous
/// hardcoded `GRANT ... TO 'gastown'@'%'` was brittle — in the live deploy no
/// `gastown` user exists, so that statement ERRORed and aborted the whole
/// provision. Relying on the server user's existing wildcard privilege keeps
/// provisioning grantee-agnostic and non-fatal.
pub async fn create_workspace_dolt(pool: &Pool, ws: &str) -> Result<(), AppError> {
    validate_slug(ws)?;
    let db = dolt_db_name(ws);
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db}`"))
        .await
        .map_err(map_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_name_prefixes_slug() {
        assert_eq!(dolt_db_name("acme"), "hq_acme");
        assert_eq!(dolt_db_name("gastown"), "hq_gastown");
        assert_eq!(dolt_db_name("team-1"), "hq_team-1");
    }

    #[test]
    fn validate_accepts_valid_slugs() {
        for s in ["acme", "gastown", "a", "team-1", "x1-y2-z3"] {
            assert!(validate_slug(s).is_ok(), "expected {s:?} valid");
        }
    }

    #[test]
    fn validate_accepts_max_length() {
        let s = "a".repeat(63);
        assert!(validate_slug(&s).is_ok());
    }

    #[test]
    fn validate_rejects_bad_slugs() {
        for s in [
            "",
            "-lead",
            "trail-",
            "double--dash",
            "Upper",
            "under_score",
            "white space",
            "semi;colon",
            "back`tick",
        ] {
            assert!(validate_slug(s).is_err(), "expected {s:?} rejected");
        }
    }

    #[test]
    fn validate_rejects_over_length() {
        let s = "a".repeat(64);
        assert!(validate_slug(&s).is_err());
    }
}
