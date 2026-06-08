//! Postgres storage adapters for gt-core domains.
//!
//! This crate hosts the SQL migrations for domains that carry no persistence of
//! their own and, from `hq-mt-core.6`, the concrete `WorkspaceRepository`
//! Postgres adapter. This bead (`hq-mt-core.5`) lands the `workspaces` table
//! migration and its bootstrap default row.
//!
//! A module owns its schema and declares it as a list of
//! [`Migration`](gt_module::Migration)s; the kernel's `RootBuilder` aggregates
//! every module's migrations into one ordered plan and `gt_module_migrate::apply`
//! runs it. The migrations live under `migrations/<module-id>/NNNN_*.sql` and are
//! embedded at compile time so the binary is self-contained.

#![forbid(unsafe_code)]

use gt_module::Migration;

mod schema;
pub use schema::{schema_for, MAX_WORKSPACE_SLUG_LEN, SCHEMA_PREFIX, SHARED_SCHEMA};
#[cfg(feature = "pg")]
pub use schema::WorkspacePool;

#[cfg(feature = "pg")]
pub mod documents;
#[cfg(feature = "pg")]
pub use documents::{
    DocError, Document, DocumentPatch, DocumentShare, DocumentsRepository, NewDocument,
    PgDocuments, SharesRepository,
};

/// Canonical id of the bootstrap default workspace.
///
/// A single workspace is seeded by the initial migration so the platform is
/// usable before any tenant is provisioned; multi-tenant resolution can fall
/// back to this id when no workspace is selected.
pub const DEFAULT_WORKSPACE_ID: &str = "default";

/// Slug of the bootstrap default workspace.
pub const DEFAULT_WORKSPACE_SLUG: &str = "default";

/// Initial migration: `workspaces` table + bootstrap default row.
const WORKSPACE_0001_SQL: &str =
    include_str!("../migrations/gt-workspace/0001_workspaces.sql");

/// Migration #2: `gt_create_workspace_schema(ws)` provisioning function that
/// clones the default workspace's schema structure into `ws_<slug>`.
const WORKSPACE_0002_SQL: &str =
    include_str!("../migrations/gt-workspace/0002_create_workspace_schema_fn.sql");

/// Migrations for the `gt-workspace` catalog, in ascending apply order.
///
/// The `gt-workspace` domain crate exposes only the `WorkspaceRepository` port
/// and carries no schema of its own; its table lives here in the Postgres
/// adapter crate. The owning module surfaces these through `GtModule::migrations`
/// so the kernel orders them deterministically alongside every other module's
/// schema.
pub fn workspace_migrations() -> Vec<Migration> {
    vec![
        Migration::new(1, "0001_workspaces", WORKSPACE_0001_SQL),
        Migration::new(2, "0002_create_workspace_schema_fn", WORKSPACE_0002_SQL),
    ]
}

/// Initial migration: per-workspace `flag_overrides` table.
const FEATURE_FLAGS_0001_SQL: &str =
    include_str!("../migrations/gt-feature-flags/0001_flag_overrides.sql");

/// Migrations for the `gt-feature-flags` override store, in ascending apply order.
///
/// `gt-feature-flags` is a kernel crate exposing only the `FeatureFlags` port
/// (override-only, keyed by workspace + `FlagKey`); like `gt-workspace` it carries
/// no schema of its own, so its Postgres table lives here. The owning module
/// surfaces these through `GtModule::migrations` so the kernel orders them after
/// `workspaces` (the FK target) alongside every other module's schema.
pub fn feature_flags_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_flag_overrides", FEATURE_FLAGS_0001_SQL)]
}

/// Migration #1: per-workspace `documents` table in the `ws_default` template.
const DOCS_0001_SQL: &str = include_str!("../migrations/gt-docs/0001_documents.sql");

/// Migration #2: per-workspace `document_versions` history in the `ws_default` template.
const DOCS_0002_SQL: &str = include_str!("../migrations/gt-docs/0002_document_versions.sql");

/// Migration #3: pgvector `embedding` column + HNSW index (phase-2 semantic search).
const DOCS_0003_SQL: &str = include_str!("../migrations/gt-docs/0003_embedding.sql");

/// Migration #4: per-workspace `document_shares` table — public capability-URL share links
/// (hq-web-extras.9).
const DOCS_0004_SQL: &str = include_str!("../migrations/gt-docs/0004_document_shares.sql");

/// Migrations for the `gt-docs` per-workspace document store (hq-docs-store.1), in
/// ascending apply order.
///
/// Like `gt-rig`'s `rigs`, the `documents`/`document_versions` tables are per-workspace
/// projection data defined ONCE in the `ws_default` template schema; the catalog runner
/// applies them on boot and `gt_create_workspace_schema` clones them into every tenant
/// (docs/11, docs/04 §15). The owning `gt-documents` domain crate (hq-docs-api.1) will
/// surface these through `GtModule::migrations`; until it lands they live here beside the
/// other port-only schemas (`gt-workspace`, `gt-feature-flags`).
pub fn docs_migrations() -> Vec<Migration> {
    vec![
        Migration::new(1, "0001_documents", DOCS_0001_SQL),
        Migration::new(2, "0002_document_versions", DOCS_0002_SQL),
        Migration::new(3, "0003_embedding", DOCS_0003_SQL),
        Migration::new(4, "0004_document_shares", DOCS_0004_SQL),
    ]
}

/// Initial migration: `notifications` table in the public schema.
const NOTIFICATIONS_0001_SQL: &str =
    include_str!("../migrations/notifications/0001_notifications.sql");

/// Migrations for the `notifications` store.
///
/// The `notifications` table lives in the public schema (not per-workspace): a single
/// table keyed by `workspace` column, like `mcp_audit`. Agents (e.g. `mayor`) write
/// here via `notify.send.execute`; the web UI polls or streams these to render the
/// bell icon panel for the human operator.
pub fn notifications_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_notifications", NOTIFICATIONS_0001_SQL)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_versioned_migrations_in_order() {
        let migrations = workspace_migrations();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version, 1);
        assert_eq!(migrations[0].name, "0001_workspaces");
        assert_eq!(migrations[1].version, 2);
        assert_eq!(migrations[1].name, "0002_create_workspace_schema_fn");
    }

    #[test]
    fn schema_fn_clones_template_structure_idempotently() {
        let sql = &workspace_migrations()[1].sql;
        assert!(
            sql.contains("FUNCTION gt_create_workspace_schema"),
            "defines the provisioning function",
        );
        assert!(sql.to_uppercase().contains("CREATE SCHEMA IF NOT EXISTS"), "creates the schema");
        assert!(
            sql.to_uppercase().contains("LIKE") && sql.to_uppercase().contains("INCLUDING ALL"),
            "clones structure with LIKE ... INCLUDING ALL",
        );
        assert!(
            sql.to_uppercase().contains("CREATE TABLE IF NOT EXISTS"),
            "table clone is idempotent",
        );
        // Names the schema with the same `ws_` convention as `schema_for`.
        assert!(sql.contains(SCHEMA_PREFIX), "uses the schema_for prefix");
        assert!(sql.contains("ws_default"), "templates from the default workspace schema");
    }

    #[test]
    fn migration_creates_table_and_seeds_default_row() {
        let sql = &workspace_migrations()[0].sql;
        assert!(sql.contains("CREATE TABLE"), "must create the table");
        assert!(sql.contains("workspaces"), "table name present");
        // Bootstrap default row, seeded idempotently.
        assert!(sql.contains(DEFAULT_WORKSPACE_ID), "default id seeded");
        assert!(sql.contains(DEFAULT_WORKSPACE_SLUG), "default slug seeded");
        assert!(
            sql.to_uppercase().contains("ON CONFLICT"),
            "seed must be idempotent on manual re-apply",
        );
    }

    #[test]
    fn docs_migrations_define_template_tables_in_order() {
        let migs = docs_migrations();
        assert_eq!(migs.len(), 4);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "0001_documents");
        assert_eq!(migs[1].version, 2);
        assert_eq!(migs[1].name, "0002_document_versions");
        assert_eq!(migs[2].version, 3);
        assert_eq!(migs[2].name, "0003_embedding");
        assert_eq!(migs[3].version, 4);
        assert_eq!(migs[3].name, "0004_document_shares");
        // Per-workspace share table: in the ws_default template, FKs the live doc, no workspace_id.
        let shares = &migs[3].sql;
        assert!(shares.contains("ws_default.document_shares"), "share table in template");
        assert!(shares.contains("REFERENCES ws_default.documents"), "FKs the live doc");
        assert!(!shares.contains("workspace_id TEXT"), "no workspace_id column");
        // Phase-2 embedding migration: pgvector extension + vector column + ANN index.
        let emb = &migs[2].sql;
        assert!(emb.contains("CREATE EXTENSION IF NOT EXISTS vector"), "enables pgvector");
        assert!(emb.contains("embedding vector(384)"), "384-dim embedding column");
        assert!(emb.to_lowercase().contains("hnsw"), "ANN index");

        // Per-workspace projection: defined in the ws_default template, structural
        // isolation (no workspace_id / FK), idempotent like the other template migrations.
        let create = &migs[0].sql;
        assert!(create.contains("CREATE SCHEMA IF NOT EXISTS ws_default"), "bootstraps template");
        assert!(create.contains("ws_default.documents"), "table in template schema");
        assert!(create.to_uppercase().contains("CREATE TABLE IF NOT EXISTS"), "idempotent");
        // Structural isolation: no `workspace_id` *column* (the prose comment may name it).
        assert!(!create.contains("workspace_id TEXT"), "no workspace_id column");
        assert!(!create.to_uppercase().contains("REFERENCES WORKSPACES"), "no FK to workspaces");
        // Phase-1 shape: full-text column present.
        assert!(create.contains("tsv"), "phase-1 full-text column present");
        // The locked design decisions are realized in the schema.
        assert!(create.contains("version"), "optimistic-concurrency token");
        assert!(create.contains("deleted_at"), "soft-delete column");
        assert!(create.contains("sha256"), "dedup key");
        assert!(create.contains("kind IN ('md', 'blob')"), "two content classes");

        let versions = &migs[1].sql;
        assert!(versions.contains("ws_default.document_versions"), "history table in template");
        assert!(versions.contains("REFERENCES ws_default.documents"), "FKs the live row");
    }

    #[test]
    fn check_constraint_matches_domain_status_variants() {
        // The CHECK constraint must allow exactly the snake_case WorkspaceStatus
        // variants.
        let sql = &workspace_migrations()[0].sql;
        for status in ["active", "suspended", "archived"] {
            assert!(sql.contains(status), "status variant `{status}` must be allowed");
        }
    }

    #[test]
    fn flags_exposes_one_versioned_migration() {
        let migrations = feature_flags_migrations();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, 1);
        assert_eq!(migrations[0].name, "0001_flag_overrides");
    }

    #[test]
    fn flags_migration_creates_override_table_with_columns() {
        let sql = &feature_flags_migrations()[0].sql;
        assert!(sql.contains("CREATE TABLE"), "must create the table");
        assert!(sql.contains("flag_overrides"), "table name present");
        for col in ["workspace_id", "flag_key", "enabled", "since", "set_by"] {
            assert!(sql.contains(col), "column `{col}` must be present");
        }
        // Override-only store: one row per (workspace, key).
        assert!(
            sql.to_uppercase().contains("PRIMARY KEY (WORKSPACE_ID, FLAG_KEY)"),
            "composite PK keys overrides by (workspace, flag_key)",
        );
    }

    #[test]
    fn flags_migration_fks_workspaces_with_cascade() {
        // docs/04 rule 14: projection tables FK workspaces.id; removing a
        // workspace must take its overrides with it.
        let sql = feature_flags_migrations()[0].sql.to_uppercase();
        assert!(sql.contains("REFERENCES WORKSPACES (ID)"), "FK to workspaces.id");
        assert!(sql.contains("ON DELETE CASCADE"), "cascade on workspace removal");
    }
}
