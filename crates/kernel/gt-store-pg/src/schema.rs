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

    /// Schema-clone completeness invariant (`hq-mt-data.10`).
    ///
    /// `gt_create_workspace_schema(ws)` (hq-mt-data.2) must materialize a tenant
    /// schema that *reproduces the template* (`ws_default`): every table the
    /// template carries reappears in the provisioned schema with the same
    /// columns, types, nullability, defaults, identity, CHECK constraints, and
    /// indexes — but with foreign keys *absent*, because `CREATE TABLE … LIKE …
    /// INCLUDING ALL` never copies FK constraints (docs/04 §15: per-tenant tables
    /// are independent copies). The function must also be idempotent.
    ///
    /// This is the live counterpart to the string-shape test in `lib.rs`: it
    /// applies the real migration function against Postgres and introspects the
    /// catalog to prove the clone is structurally complete. No-op without
    /// `GT_PG_URL`.
    ///
    /// The probe table is created in the shared `ws_default` template, so the
    /// test drops it again on the way out — otherwise every *real* future
    /// provision would copy the leftover. All assertions run *after* teardown
    /// (against captured values) so a failed assertion still cleans up; a
    /// defensive teardown also runs first to clear any leak from a crashed run.
    #[cfg(feature = "pg")]
    #[tokio::test]
    async fn provisioned_schema_reproduces_template() {
        use crate::{WORKSPACE_0001_SQL, WORKSPACE_0002_SQL};
        use sqlx::PgPool;

        let Some(url) = std::env::var("GT_PG_URL").ok() else {
            eprintln!("GT_PG_URL unset; skipping schema-clone completeness test");
            return;
        };

        // Unique-ish names keep the test from colliding with real tenant data or
        // a parallel session on a shared Postgres.
        let probe = "_clone_probe_mtdata10";
        let ws_slug = "clone-probe-mtdata10";
        let clone_schema = schema_for(ws_slug); // ws_clone_probe_mtdata10
        let tmpl_table = format!("ws_default.{probe}");
        let clone_table = format!("{clone_schema}.{probe}");

        // Catalog introspection shared by both schemas.
        type ColRow = (String, String, String, Option<String>, String, Option<String>);
        async fn columns(pool: &PgPool, schema: &str, table: &str) -> Vec<ColRow> {
            sqlx::query_as(
                "SELECT column_name, data_type, is_nullable, column_default, \
                        is_identity, identity_generation \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY column_name",
            )
            .bind(schema)
            .bind(table)
            .fetch_all(pool)
            .await
            .expect("introspect columns")
        }
        async fn constraint_count(pool: &PgPool, qualified: &str, contype: &str) -> i64 {
            sqlx::query_scalar(
                "SELECT count(*) FROM pg_constraint \
                 WHERE conrelid = $1::regclass AND contype = $2",
            )
            .bind(qualified)
            .bind(contype)
            .fetch_one(pool)
            .await
            .expect("count constraints")
        }
        async fn index_count(pool: &PgPool, schema: &str, table: &str) -> i64 {
            sqlx::query_scalar(
                "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
            )
            .bind(schema)
            .bind(table)
            .fetch_one(pool)
            .await
            .expect("count indexes")
        }

        let pool = PgPool::connect(&url).await.expect("connect postgres");

        // Defensive teardown: clear any leak from a previously crashed run.
        let _ = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {clone_schema} CASCADE"))
            .execute(&pool)
            .await;
        let _ = sqlx::raw_sql(&format!("DROP TABLE IF EXISTS {tmpl_table}"))
            .execute(&pool)
            .await;

        // Catalog (`workspaces`, FK target) + provisioning function must exist.
        sqlx::raw_sql(WORKSPACE_0001_SQL)
            .execute(&pool)
            .await
            .expect("apply workspaces migration");
        sqlx::raw_sql(WORKSPACE_0002_SQL)
            .execute(&pool)
            .await
            .expect("install provisioning function");
        sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS ws_default")
            .execute(&pool)
            .await
            .expect("ensure template schema");

        // Probe table exercising every LIKE-copied attribute plus a FK (which
        // must NOT survive the clone).
        sqlx::raw_sql(&format!(
            "CREATE TABLE {tmpl_table} ( \
                id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
                ws_id   TEXT NOT NULL REFERENCES public.workspaces (id) ON DELETE CASCADE, \
                label   TEXT NOT NULL DEFAULT 'x', \
                amount  INTEGER CHECK (amount >= 0) \
            ); \
            CREATE INDEX {probe}_label_idx ON {tmpl_table} (label);"
        ))
        .execute(&pool)
        .await
        .expect("create probe template table");

        // Provision the tenant schema (the unit under test) — twice, to prove
        // idempotency. A second call must not error.
        sqlx::query("SELECT gt_create_workspace_schema($1)")
            .bind(ws_slug)
            .execute(&pool)
            .await
            .expect("first provision");
        let second = sqlx::query("SELECT gt_create_workspace_schema($1)")
            .bind(ws_slug)
            .execute(&pool)
            .await;

        // Capture everything BEFORE teardown so assertions cannot leak state.
        let tmpl_cols = columns(&pool, "ws_default", probe).await;
        let clone_cols = columns(&pool, &clone_schema, probe).await;
        let tmpl_checks = constraint_count(&pool, &tmpl_table, "c").await;
        let clone_checks = constraint_count(&pool, &clone_table, "c").await;
        let tmpl_fks = constraint_count(&pool, &tmpl_table, "f").await;
        let clone_fks = constraint_count(&pool, &clone_table, "f").await;
        let tmpl_idx = index_count(&pool, "ws_default", probe).await;
        let clone_idx = index_count(&pool, &clone_schema, probe).await;

        // Teardown — runs regardless of the assertion outcomes below.
        let _ = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {clone_schema} CASCADE"))
            .execute(&pool)
            .await;
        let _ = sqlx::raw_sql(&format!("DROP TABLE IF EXISTS {tmpl_table}"))
            .execute(&pool)
            .await;

        // --- Completeness invariant ---------------------------------------
        second.expect("provisioning is idempotent (second call must not error)");
        assert!(!tmpl_cols.is_empty(), "probe template table must have columns");
        assert_eq!(
            clone_cols, tmpl_cols,
            "clone columns (name/type/nullable/default/identity) must reproduce the template",
        );
        assert!(tmpl_checks > 0, "template must carry a CHECK constraint");
        assert_eq!(clone_checks, tmpl_checks, "CHECK constraints must be reproduced");
        assert!(tmpl_idx > 0, "template must carry indexes (PK + label)");
        assert_eq!(clone_idx, tmpl_idx, "indexes (incl. PRIMARY KEY) must be reproduced");
        // The crux of schema-per-ws independence: FKs are dropped by LIKE, so a
        // per-tenant table has no cross-schema reference back to the template.
        assert_eq!(tmpl_fks, 1, "template probe table declares one FK");
        assert_eq!(clone_fks, 0, "clone must NOT carry the template's foreign key");
    }
}
