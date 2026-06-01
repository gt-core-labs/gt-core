//! Per-workspace Postgres schema isolation (`hq-mt-data.1`).
//!
//! Multi-tenant storage partitions each workspace into its own Postgres schema:
//! a connection scoped to workspace `acme` resolves unqualified table names
//! against the `ws_acme` schema, so the same `beads`/`sessions`/… tables exist
//! once per tenant with no `workspace_id` predicate on every read.
//!
//! Two pieces:
//! - [`schema_for`] maps a workspace slug to its (sanitized) schema name.
//! - [`WorkspacePool`] (under the `pg` feature) wraps a [`sqlx::PgPool`] and runs
//!   `SET search_path` on every physical connection as it is opened, so every
//!   query checked out of the pool already resolves against the tenant schema.
//!
//! ## Why a prefix, and why `public` is reserved
//!
//! The shared [`SHARED_SCHEMA`] (`public`) holds cross-tenant tables — most
//! importantly `workspaces` itself, the catalog every per-workspace schema is
//! provisioned from. Per-workspace schemas therefore must never alias it. The
//! [`SCHEMA_PREFIX`] (`ws_`) guarantees that: a workspace slug can never produce
//! the bare name `public` (or a `pg_*` system schema), and a leading-digit slug
//! (`1team`) becomes a legal unquoted identifier (`ws_1team`).
//!
//! Kernel-tier note: this crate must not depend on the `gt-workspace` domain
//! crate (one-way dep direction, docs/03), so [`schema_for`] takes the raw slug
//! `&str` rather than a `WorkspaceId`. The slug is already validated to the
//! lowercase-kebab DNS-label grammar upstream; the mapping here is the defensive
//! translation from that grammar to a Postgres identifier.

/// The shared schema for cross-tenant tables (the `workspaces` catalog, etc.).
///
/// Reserved: [`schema_for`] never yields this name for any workspace.
pub const SHARED_SCHEMA: &str = "public";

/// Prefix applied to every per-workspace schema name.
///
/// Keeps per-workspace schemas out of the reserved [`SHARED_SCHEMA`] and `pg_*`
/// namespaces and makes leading-digit slugs legal unquoted identifiers.
pub const SCHEMA_PREFIX: &str = "ws_";

/// Longest workspace slug that yields a schema name within Postgres' 63-byte
/// identifier limit, given [`SCHEMA_PREFIX`].
///
/// `WorkspaceId` caps slugs at 63 chars; provisioning code should reject (or the
/// catalog should never mint) a slug longer than this so [`schema_for`] cannot
/// produce a name Postgres would silently truncate.
pub const MAX_WORKSPACE_SLUG_LEN: usize = 63 - SCHEMA_PREFIX.len();

/// Map a workspace slug to its Postgres schema name.
///
/// The slug is expected to match the lowercase-kebab DNS-label grammar
/// (`[a-z0-9]+(-[a-z0-9]+)*`) that `WorkspaceId` enforces. Each `-` becomes `_`
/// (hyphens are illegal in unquoted Postgres identifiers) and the result is
/// prefixed with [`SCHEMA_PREFIX`].
///
/// The mapping is total and injective over valid slugs: `-` and `_` cannot both
/// appear in a valid slug, so distinct slugs never collide on the same schema.
///
/// ```
/// assert_eq!(gt_store_pg::schema_for("acme"), "ws_acme");
/// assert_eq!(gt_store_pg::schema_for("team-1"), "ws_team_1");
/// ```
pub fn schema_for(workspace: &str) -> String {
    let mut name = String::with_capacity(SCHEMA_PREFIX.len() + workspace.len());
    name.push_str(SCHEMA_PREFIX);
    for c in workspace.chars() {
        name.push(if c == '-' { '_' } else { c });
    }
    name
}

#[cfg(feature = "pg")]
mod pool {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;

    use super::{schema_for, SHARED_SCHEMA};

    /// A [`PgPool`] scoped to a single workspace's schema.
    ///
    /// Every physical connection the pool opens runs `SET search_path` to the
    /// workspace schema (falling back to [`SHARED_SCHEMA`] for cross-tenant
    /// tables) via `sqlx`'s `after_connect` hook, so a connection checked out of
    /// [`pool`](Self::pool) already resolves unqualified names against the tenant
    /// schema — callers write plain `SELECT … FROM beads`, never a qualified name.
    ///
    /// Cloning is cheap: [`PgPool`] is an `Arc` over the connection pool.
    #[derive(Clone)]
    pub struct WorkspacePool {
        pool: PgPool,
        schema: String,
    }

    impl WorkspacePool {
        /// Connect a workspace-scoped pool to `url`.
        ///
        /// The schema is expected to already exist (provisioned at workspace
        /// creation). `search_path` is set per physical connection, so a schema
        /// created after connect is picked up by connections opened afterward.
        pub async fn connect(url: &str, workspace: &str) -> Result<Self, sqlx::Error> {
            let schema = schema_for(workspace);
            // `schema` is `[a-z0-9_]` (prefix + sanitized slug), so it is a safe
            // bare identifier — no quoting or injection surface.
            let set_path = format!("SET search_path TO {schema}, {SHARED_SCHEMA}");
            let pool = PgPoolOptions::new()
                .after_connect(move |conn, _meta| {
                    let set_path = set_path.clone();
                    Box::pin(async move {
                        sqlx::query(&set_path).execute(conn).await?;
                        Ok(())
                    })
                })
                .connect(url)
                .await?;
            Ok(WorkspacePool { pool, schema })
        }

        /// Borrow the underlying pool; connections are already `search_path`-scoped.
        pub fn pool(&self) -> &PgPool {
            &self.pool
        }

        /// The workspace schema name this pool resolves against.
        pub fn schema(&self) -> &str {
            &self.schema
        }
    }
}

#[cfg(feature = "pg")]
pub use pool::WorkspacePool;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_and_passes_through_simple_slug() {
        assert_eq!(schema_for("acme"), "ws_acme");
        assert_eq!(schema_for("gastown"), "ws_gastown");
    }

    #[test]
    fn hyphens_become_underscores() {
        assert_eq!(schema_for("team-1"), "ws_team_1");
        assert_eq!(schema_for("x1-y2-z3"), "ws_x1_y2_z3");
    }

    #[test]
    fn leading_digit_slug_becomes_legal_identifier() {
        // `1team` is a valid WorkspaceId but an illegal bare Postgres identifier;
        // the prefix fixes that.
        let schema = schema_for("1team");
        assert_eq!(schema, "ws_1team");
        assert!(schema.as_bytes()[0].is_ascii_alphabetic(), "must not start with a digit");
    }

    #[test]
    fn never_yields_the_reserved_shared_schema() {
        for slug in ["public", "pg-catalog", "default", "ws"] {
            assert_ne!(schema_for(slug), SHARED_SCHEMA);
            assert!(schema_for(slug).starts_with(SCHEMA_PREFIX));
        }
    }

    #[test]
    fn distinct_valid_slugs_do_not_collide() {
        // `-` and `_` never coexist in a valid slug, so the hyphen→underscore
        // map cannot alias two different slugs onto one schema.
        assert_ne!(schema_for("a-b"), schema_for("a"));
        assert_ne!(schema_for("ab"), schema_for("a-b"));
    }

    #[test]
    fn result_is_a_legal_unquoted_identifier() {
        for slug in ["acme", "team-1", "1team", "x1-y2-z3"] {
            let schema = schema_for(slug);
            assert!(
                schema.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "schema {schema:?} must be [a-z0-9_]",
            );
            assert!(!schema.as_bytes()[0].is_ascii_digit());
        }
    }

    #[test]
    fn max_slug_len_fits_postgres_identifier_limit() {
        let slug = "a".repeat(MAX_WORKSPACE_SLUG_LEN);
        assert_eq!(schema_for(&slug).len(), 63, "longest allowed slug fills the identifier");
    }

    /// Contract test: a [`WorkspacePool`] checks out connections whose
    /// `search_path` resolves the workspace schema first. No-op without
    /// `GT_PG_URL` (developer box / CI without Postgres).
    #[cfg(feature = "pg")]
    #[tokio::test]
    async fn search_path_set_on_checkout() {
        let Some(url) = std::env::var("GT_PG_URL").ok() else {
            eprintln!("GT_PG_URL unset; skipping WorkspacePool search_path test");
            return;
        };
        let wp = WorkspacePool::connect(&url, "acme-pg-test")
            .await
            .expect("connect workspace pool");
        assert_eq!(wp.schema(), "ws_acme_pg_test");

        let path: String = sqlx::query_scalar("SHOW search_path")
            .fetch_one(wp.pool())
            .await
            .expect("SHOW search_path");
        assert!(
            path.contains("ws_acme_pg_test"),
            "search_path {path:?} must include the workspace schema",
        );
    }
}
