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
    PgDocuments, SharesRepository, VectorStore,
};

#[cfg(feature = "pg")]
pub mod comments;
#[cfg(feature = "pg")]
pub use comments::{Comment, CommentError, CommentsRepository, NewComment, PgComments};

#[cfg(feature = "pg")]
pub mod doc_chunks;
#[cfg(feature = "pg")]
pub use doc_chunks::{ChunkError, DocChunksRepository, NewChunk, PgDocChunks, RetrievedChunk};

#[cfg(feature = "pg")]
pub mod email_outbox;
#[cfg(feature = "pg")]
pub mod email_subscriptions;
#[cfg(feature = "pg")]
pub use email_subscriptions::{PgSubscriptions, SubscriptionError, SubscriptionsRepository};
#[cfg(feature = "pg")]
pub use email_outbox::{EmailOutboxRepository, NewEmail, OutboxEntry, OutboxError, PgEmailOutbox};
#[cfg(feature = "pg")]
pub mod report_subscriptions;
#[cfg(feature = "pg")]
pub use report_subscriptions::{
    PgReportSubscriptions, ReportSubscriber, ReportSubscriptionError,
    ReportSubscriptionsRepository,
};

#[cfg(feature = "pg")]
pub mod report_schedules;
#[cfg(feature = "pg")]
pub use report_schedules::{
    PgReportSchedules, ReportScheduleError, ReportScheduleRow, ReportSchedulesRepository,
};

#[cfg(feature = "pg")]
pub mod invites;
#[cfg(feature = "pg")]
pub use invites::{Invite, InviteError, InvitesRepository, NewInvite, PgInvites};

#[cfg(feature = "pg")]
pub mod memory_store;
#[cfg(feature = "pg")]
pub use memory_store::{
    ConflictPair, MemoryError, MemoryRepository, MemoryRow, NewMemory, PgMemory,
};

#[cfg(feature = "pg")]
pub mod memory_import;
#[cfg(feature = "pg")]
pub use memory_import::{import_corpus, ImportError, ImportReport};

/// Fail-closed guard for PG-backed tests (gtcore-d43955).
///
/// Call this BEFORE opening any connection to a test database to ensure the URL is ephemeral.
/// A URL is accepted when the host is `localhost`, `127.0.0.1`, or `::1`, or when the URL
/// carries `gt_test_only=1` (the explicit opt-in for CI service URLs with non-loopback hosts).
/// Everything else panics with a descriptive error before any SQL runs — making it impossible
/// for tests to accidentally reach production/dev Postgres.
///
/// # Panics
/// Panics when `url` does not look ephemeral, with a message that names the DSN and explains
/// how to mark a CI service URL safe.
pub fn assert_ephemeral_pg_url(url: &str) {
    let ephemeral = url.contains("@localhost")
        || url.contains("@127.0.0.1")
        || url.contains("@[::1]")
        || url.contains("gt_test_only=1");
    if !ephemeral {
        panic!(
            "GT_PG_URL does not look ephemeral — refusing to run tests against \
             a non-throwaway Postgres (production/dev wipe risk, see gtcore-d43955).\n\
             URL host is not localhost / 127.0.0.1. To allow a CI service URL with \
             a non-loopback host, append `?gt_test_only=1` to the DSN.\n\
             Offending DSN: {url}"
        );
    }
}

#[cfg(test)]
mod dsn_guard_tests {
    use super::assert_ephemeral_pg_url;

    #[test]
    fn localhost_passes() {
        assert_ephemeral_pg_url("postgres://user:pw@localhost:5432/db");
        assert_ephemeral_pg_url("postgresql://user@localhost/test");
    }

    #[test]
    fn loopback_ipv4_passes() {
        assert_ephemeral_pg_url("postgres://user:pw@127.0.0.1:5433/db");
    }

    #[test]
    fn loopback_ipv6_passes() {
        assert_ephemeral_pg_url("postgres://user:pw@[::1]:5432/db");
    }

    #[test]
    fn explicit_marker_passes() {
        assert_ephemeral_pg_url("postgres://user:pw@pg-service:5432/db?gt_test_only=1");
    }

    #[test]
    #[should_panic(expected = "does not look ephemeral")]
    fn prod_host_panics() {
        assert_ephemeral_pg_url("postgres://gtapp:secret@gt-postgres:5432/gtapp");
    }

    #[test]
    #[should_panic(expected = "does not look ephemeral")]
    fn kubernetes_service_without_marker_panics() {
        assert_ephemeral_pg_url("postgres://user:pw@postgres.svc.cluster.local:5432/db");
    }
}

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

/// Migration #5: per-workspace `doc_chunks` — chunk-level embeddings for agent RAG (hq-c488cb).
const DOCS_0005_SQL: &str = include_str!("../migrations/gt-docs/0005_doc_chunks.sql");

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
        Migration::new(5, "0005_doc_chunks", DOCS_0005_SQL),
    ]
}

/// Migration #1: per-workspace `comments` table in the `ws_default` template.
const COMMENTS_0001_SQL: &str = include_str!("../migrations/gt-comments/0001_comments.sql");

/// Migrations for the `gt-comments` per-workspace threaded-comments store
/// (hq-57042e), in ascending apply order.
///
/// Like `gt-docs`' `documents`, the `comments` table is per-workspace projection
/// data defined ONCE in the `ws_default` template schema; the catalog runner
/// applies it on boot and `gt_create_workspace_schema` clones it into every
/// tenant (docs/04 §15). Polymorphic target (card|doc) — no FK, target existence
/// is the handler's check (a `card` lives in Dolt, not this database).
pub fn comments_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_comments", COMMENTS_0001_SQL)]
}

/// Migration #1: per-workspace `memories` table in the `ws_default` template.
const MEMORY_0001_SQL: &str = include_str!("../migrations/gt-memory/0001_memories.sql");

/// Migrations for the `gt-memory` per-workspace semantic memory store (hq-memory-mcp.1),
/// in ascending apply order.
///
/// Like `gt-docs`' `documents`, the `memories` table is per-workspace projection data
/// defined ONCE in the `ws_default` template schema; the catalog runner applies it on
/// boot and `gt_create_workspace_schema` clones it into every tenant (docs/11, docs/04
/// §15). It ships its `tsv` full-text + `embedding` pgvector columns in the same
/// migration (no phase split), so a memory is recallable by meaning from row zero.
pub fn memory_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_memories", MEMORY_0001_SQL)]
}

/// The complete, idempotent DDL that re-creates every per-workspace projection table this
/// crate owns in the `ws_default` template schema: `comments`, the `documents` family
/// (`documents` + `document_versions` + `document_shares` + `doc_chunks`, plus the pgvector
/// `embedding` column), and `memories`. It is the concatenation, in apply order, of every
/// [`comments_migrations`] / [`docs_migrations`] / [`memory_migrations`] migration's SQL —
/// derived from the migration history so the self-heal can never drift from it (the same
/// belt-and-suspenders [`gt_rig::RigsModule::template_ensure_sql`] builds for `rigs`).
///
/// Why this exists, given the migrations are already idempotent and applied at boot: the
/// migration tracking table (`_gt_schema_migrations`) lives in the shared `public` schema and
/// SURVIVES a `DROP SCHEMA ws_default CASCADE` (the tenant-reprovision / data-wipe path,
/// gtcore-c9b292). After such a drop the loader still sees `0001_comments`, `0001_documents`,
/// … recorded as applied and SKIPS them, so the dropped `ws_default.comments` /
/// `ws_default.documents` / `ws_default.memories` are never recreated — the bookkeeping is
/// desynced from reality and the comments/documents/memory features 500 with
/// `relation "…" does not exist` forever. Replaying this DDL UNCONDITIONALLY on boot
/// guarantees the template tables are present regardless of what the tracking table claims.
///
/// Every statement is `CREATE …/ALTER … IF NOT EXISTS`, so replaying it when the tables are
/// already present is a cheap catalog-check no-op that never destroys data or clobbers a row.
/// The caller runs it under an advisory lock — concurrent `CREATE … IF NOT EXISTS` against the
/// same object races in Postgres across replicas (and parallel tests).
pub fn projection_template_ensure_sql() -> String {
    comments_migrations()
        .iter()
        .chain(docs_migrations().iter())
        .chain(memory_migrations().iter())
        .map(|m| m.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Initial migration: `email_outbox` table in the public schema.
const EMAIL_0001_SQL: &str = include_str!("../migrations/email/0001_email_outbox.sql");

/// Migrations for the transport-agnostic email outbox (hq-f24599).
///
/// Public schema (like `notifications`): one table keyed by a `workspace`
/// column. Producers enqueue; the drain daemon delivers through the
/// `gt_notify::EmailTransport` seam — the SMTP server stays pluggable.
/// Migration #2: `email_subscriptions` — seguimiento subscriptions (hq-8a521a).
const EMAIL_0002_SQL: &str = include_str!("../migrations/email/0002_email_subscriptions.sql");
/// Migration #3: `report_subscriptions` — scheduled-report recipients (hq-562e0b).
const EMAIL_0003_SQL: &str = include_str!("../migrations/email/0003_report_subscriptions.sql");
/// Migration #4: `email_outbox.cc` — carbon-copy recipients (gtcore-ecf70d).
const EMAIL_0004_SQL: &str = include_str!("../migrations/email/0004_email_outbox_cc.sql");
/// Migration #5: `report_schedules` — durable, DB-backed schedule list
/// (gtcore-915232; was lost on every redeploy in `system_config.json`).
const EMAIL_0005_SQL: &str = include_str!("../migrations/email/0005_report_schedules.sql");

pub fn email_migrations() -> Vec<Migration> {
    vec![
        Migration::new(1, "0001_email_outbox", EMAIL_0001_SQL),
        Migration::new(2, "0002_email_subscriptions", EMAIL_0002_SQL),
        Migration::new(3, "0003_report_subscriptions", EMAIL_0003_SQL),
        Migration::new(4, "0004_email_outbox_cc", EMAIL_0004_SQL),
        Migration::new(5, "0005_report_schedules", EMAIL_0005_SQL),
    ]
}

/// Initial migration: `workspace_invites` table in the public schema.
const INVITES_0001_SQL: &str = include_str!("../migrations/invites/0001_workspace_invites.sql");

/// Migrations for the collaborator-invite store (hq-4231c1).
///
/// Public schema: the invite is a one-shot capability token an admin mints and
/// the gt-login-authenticated accept consumes; the membership itself lives in
/// gt-auth's `user_workspaces` + `ws_<slug>.users` mirror, never here.
pub fn invites_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_workspace_invites", INVITES_0001_SQL)]
}

/// Initial migration: `notifications` table in the public schema.
const NOTIFICATIONS_0001_SQL: &str =
    include_str!("../migrations/notifications/0001_notifications.sql");

/// Migration #2: dedup + lifecycle columns (`fingerprint`, `state`, `count`,
/// `last_seen_at`) so a dedup-aware writer collapses a recurring finding into one
/// row instead of one-per-tick (gtcore-7a707a).
const NOTIFICATIONS_0002_SQL: &str =
    include_str!("../migrations/notifications/0002_notifications_dedup.sql");

/// Migrations for the `notifications` store.
///
/// The `notifications` table lives in the public schema (not per-workspace): a single
/// table keyed by `workspace` column, like `mcp_audit`. Agents (e.g. `mayor`) write
/// here via `notify.send.execute`; the web UI polls or streams these to render the
/// bell icon panel for the human operator.
pub fn notifications_migrations() -> Vec<Migration> {
    vec![
        Migration::new(1, "0001_notifications", NOTIFICATIONS_0001_SQL),
        Migration::new(2, "0002_notifications_dedup", NOTIFICATIONS_0002_SQL),
    ]
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
        assert_eq!(migs.len(), 5);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "0001_documents");
        assert_eq!(migs[1].version, 2);
        assert_eq!(migs[1].name, "0002_document_versions");
        assert_eq!(migs[2].version, 3);
        assert_eq!(migs[2].name, "0003_embedding");
        assert_eq!(migs[3].version, 4);
        assert_eq!(migs[3].name, "0004_document_shares");
        assert_eq!(migs[4].version, 5);
        assert_eq!(migs[4].name, "0005_doc_chunks");
        // Chunk index (hq-c488cb): template table + pgvector ANN, no workspace col.
        assert!(migs[4].sql.contains("ws_default.doc_chunks"));
        assert!(migs[4].sql.contains("vector(384)"));
        assert!(migs[4].sql.to_lowercase().contains("hnsw"));
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

    #[test]
    fn memory_migration_defines_template_table_with_semantic_columns() {
        let migs = memory_migrations();
        assert_eq!(migs.len(), 1);
        assert_eq!(migs[0].version, 1);
        assert_eq!(migs[0].name, "0001_memories");

        let sql = &migs[0].sql;
        // Per-workspace projection: defined in the ws_default template, idempotent.
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS ws_default"), "bootstraps template");
        assert!(sql.contains("ws_default.memories"), "table in template schema");
        assert!(sql.to_uppercase().contains("CREATE TABLE IF NOT EXISTS"), "idempotent");
        // Structural isolation: no workspace_id column / FK (the prose may name it).
        assert!(!sql.contains("workspace_id TEXT"), "no workspace_id column");
        assert!(!sql.to_uppercase().contains("REFERENCES WORKSPACES"), "no FK to workspaces");
        // Upsert-by-name natural key + the four recall classes.
        assert!(sql.contains("name        TEXT        PRIMARY KEY"), "name is the natural key");
        assert!(
            sql.contains("kind IN ('feedback', 'project', 'reference', 'user')"),
            "four recall classes",
        );
        // Hybrid retrieval: generated full-text vector + pgvector embedding (mirrors documents).
        assert!(sql.contains("tsv"), "generated full-text column");
        assert!(sql.contains("GENERATED ALWAYS AS"), "tsv is generated, not a trigger");
        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS vector"), "enables pgvector");
        assert!(sql.contains("embedding   vector(384)"), "384-dim embedding column");
        assert!(sql.to_lowercase().contains("hnsw"), "ANN index");
        assert!(sql.contains("memories_kind_idx"), "per-kind index for list/operating_rules");
        // Locked decisions realized in the schema.
        assert!(sql.contains("version"), "optimistic-concurrency token");
    }

    #[test]
    fn projection_ensure_is_the_complete_idempotent_superset() {
        // gtcore-c9b292: the boot self-heal must replay EVERY per-workspace template table this
        // crate owns, so a `DROP SCHEMA ws_default CASCADE` is fully repaired on the next boot.
        let ddl = projection_template_ensure_sql();

        // Every comments/docs/memory migration's SQL is present verbatim, so the self-heal can
        // never drift from the migration history.
        for m in comments_migrations()
            .iter()
            .chain(docs_migrations().iter())
            .chain(memory_migrations().iter())
        {
            assert!(
                ddl.contains(m.sql.as_str()),
                "ensure DDL must contain migration `{}` verbatim",
                m.name,
            );
        }

        // The three projection tables the data-wipe dropped are all (re)created in the template.
        assert!(ddl.contains("ws_default.comments"), "comments table");
        assert!(ddl.contains("ws_default.documents"), "documents table");
        assert!(ddl.contains("ws_default.document_versions"), "document_versions table");
        assert!(ddl.contains("ws_default.document_shares"), "document_shares table");
        assert!(ddl.contains("ws_default.doc_chunks"), "doc_chunks table");
        assert!(ddl.contains("ws_default.memories"), "memories table");

        // Purely additive + idempotent: only `CREATE … IF NOT EXISTS` / `ALTER … ADD COLUMN IF
        // NOT EXISTS`, never a destructive `DROP`/`TRUNCATE`/`DELETE`, so replaying it against an
        // intact DB is a no-op that cannot wipe data (acceptance criterion: additive DDL only).
        let upper = ddl.to_uppercase();
        for verb in ["DROP TABLE", "DROP SCHEMA", "TRUNCATE", "DELETE FROM"] {
            assert!(!upper.contains(verb), "ensure DDL must not contain `{verb}`");
        }
        assert!(upper.contains("CREATE TABLE IF NOT EXISTS"), "idempotent table creates");
    }
}
