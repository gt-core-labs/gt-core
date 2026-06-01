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

/// Migrations for the `gt-workspace` catalog, in ascending apply order.
///
/// The `gt-workspace` domain crate exposes only the `WorkspaceRepository` port
/// and carries no schema of its own; its table lives here in the Postgres
/// adapter crate. The owning module surfaces these through `GtModule::migrations`
/// so the kernel orders them deterministically alongside every other module's
/// schema.
pub fn workspace_migrations() -> Vec<Migration> {
    vec![Migration::new(1, "0001_workspaces", WORKSPACE_0001_SQL)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_one_versioned_migration() {
        let migrations = workspace_migrations();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, 1);
        assert_eq!(migrations[0].name, "0001_workspaces");
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
    fn check_constraint_matches_domain_status_variants() {
        // The CHECK constraint must allow exactly the snake_case WorkspaceStatus
        // variants.
        let sql = &workspace_migrations()[0].sql;
        for status in ["active", "suspended", "archived"] {
            assert!(sql.contains(status), "status variant `{status}` must be allowed");
        }
    }
}
