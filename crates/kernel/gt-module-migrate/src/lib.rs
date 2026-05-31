//! sqlx multi-source migration loader (`hq-mod-migrate.2`).
//!
//! A module owns its schema and declares its migrations through
//! [`GtModule::migrations`](gt_module::GtModule::migrations). The
//! [`RootBuilder`](gt_module::RootBuilder) merges every module's migrations into
//! one ordered apply plan — [`Root::migrations`](gt_module::Root::migrations) —
//! pairing each [`Migration`] with the [`ModuleId`] that owns it. This crate is
//! the Postgres sink for that plan: it applies the not-yet-applied migrations, in
//! plan order, against a live database.
//!
//! ## Multi-source
//!
//! "Multi-source" because the plan is aggregated from many independent modules
//! (the sources). Each module's versions are numbered independently and
//! namespaced by [`ModuleId`], so the loader tracks applied state per
//! `(module_id, version)` — adding or removing a module never disturbs another's
//! history. The plan arrives already ordered (modules in dependency-first init
//! order, ascending [`Migration::version`] within a module), so the loader
//! preserves that order rather than re-deriving it.
//!
//! ## Idempotence
//!
//! Applied migrations are recorded in a tracking table
//! ([`MIGRATIONS_TABLE`]). On each run the loader reads what is already applied
//! and skips it, so re-running an unchanged plan is a no-op. Each pending
//! migration's SQL and its tracking-row insert run in a single transaction: a
//! migration is recorded as applied only if its SQL committed, so a mid-plan
//! failure leaves the database at a clean prefix of the plan.
//!
//! ## Scope
//!
//! `.2` is the loader itself. Disable-safe retention (`.3`), backfilling existing
//! migrations under module folders (`.4`), and the apply/disable/reactivate/purge
//! integration test (`.5`) build on this.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashSet;

use gt_module::{Migration, ModuleId};
use sqlx::{PgPool, Postgres, Row, Transaction};

/// The table that records which `(module_id, version)` migrations have been
/// applied. Created on demand by [`apply`]; carries the module namespace, the
/// version, the migration name (for human-readable audit), and the apply time.
pub const MIGRATIONS_TABLE: &str = "_gt_schema_migrations";

/// Outcome of an [`apply`] run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// `(module_id, version, name)` for each migration applied by this run, in
    /// the order they were applied. Empty when the database was already current.
    pub applied: Vec<(String, u32, String)>,
    /// Count of plan entries skipped because they were already recorded as
    /// applied.
    pub skipped: usize,
}

impl MigrationReport {
    /// Whether this run applied at least one migration.
    pub fn changed(&self) -> bool {
        !self.applied.is_empty()
    }
}

/// What can go wrong while applying a plan.
#[derive(Debug)]
pub enum MigrateError {
    /// A database error tied to one migration — executing its SQL, beginning or
    /// committing its transaction, or recording it. Carries the offending
    /// `(module_id, version)`.
    Apply {
        /// Owning module of the migration whose application failed.
        module: String,
        /// Version of the migration whose application failed.
        version: u32,
        /// The underlying sqlx error.
        source: sqlx::Error,
    },
    /// A database error not attributable to a single migration (creating the
    /// tracking table or reading the applied set).
    Loader(sqlx::Error),
}

impl From<sqlx::Error> for MigrateError {
    fn from(e: sqlx::Error) -> Self {
        MigrateError::Loader(e)
    }
}

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrateError::Apply { module, version, source } => {
                write!(f, "migration {module}/{version} failed: {source}")
            }
            MigrateError::Loader(source) => write!(f, "migration loader: {source}"),
        }
    }
}

impl std::error::Error for MigrateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MigrateError::Apply { source, .. } | MigrateError::Loader(source) => Some(source),
        }
    }
}

/// Apply `plan` against `pool`, skipping migrations already recorded.
///
/// `plan` is [`Root::migrations`](gt_module::Root::migrations): an ordered slice
/// of `(module_id, migration)`. The loader ensures the tracking table exists,
/// reads the applied `(module_id, version)` set, and applies the remainder in
/// plan order. Each applied migration's SQL plus its tracking insert run in one
/// transaction. Returns a [`MigrationReport`] describing what changed.
pub async fn apply(
    pool: &PgPool,
    plan: &[(&ModuleId, &Migration)],
) -> Result<MigrationReport, MigrateError> {
    ensure_table(pool).await?;
    let applied = applied_set(pool).await?;

    let mut report = MigrationReport::default();
    for (module, migration) in pending(&applied, plan) {
        apply_one(pool, module, migration).await?;
        report
            .applied
            .push((module.as_str().to_owned(), migration.version, migration.name.clone()));
    }
    report.skipped = plan.len() - report.applied.len();
    Ok(report)
}

/// The subset of `plan` not present in `applied`, preserving plan order.
///
/// Pure and database-free so the selection logic is unit-testable without
/// Postgres. `applied` holds `(module_id, version)` pairs already recorded.
pub fn pending<'a>(
    applied: &HashSet<(String, u32)>,
    plan: &[(&'a ModuleId, &'a Migration)],
) -> Vec<(&'a ModuleId, &'a Migration)> {
    plan.iter()
        .filter(|(module, m)| !applied.contains(&(module.as_str().to_owned(), m.version)))
        .copied()
        .collect()
}

/// Create the tracking table if it does not yet exist.
async fn ensure_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (\
            module_id  TEXT        NOT NULL, \
            version    BIGINT      NOT NULL, \
            name       TEXT        NOT NULL, \
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
            PRIMARY KEY (module_id, version)\
        )"
    );
    sqlx::query(&ddl).execute(pool).await?;
    Ok(())
}

/// Read the `(module_id, version)` pairs already recorded as applied.
async fn applied_set(pool: &PgPool) -> Result<HashSet<(String, u32)>, sqlx::Error> {
    let select = format!("SELECT module_id, version FROM {MIGRATIONS_TABLE}");
    let rows = sqlx::query(&select).fetch_all(pool).await?;
    let mut set = HashSet::with_capacity(rows.len());
    for row in rows {
        let module: String = row.try_get("module_id")?;
        let version: i64 = row.try_get("version")?;
        set.insert((module, version as u32));
    }
    Ok(set)
}

/// Apply one migration and record it, atomically.
async fn apply_one(
    pool: &PgPool,
    module: &ModuleId,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let map_err = |source: sqlx::Error| MigrateError::Apply {
        module: module.as_str().to_owned(),
        version: migration.version,
        source,
    };

    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(map_err)?;

    sqlx::query(&migration.sql)
        .execute(&mut *tx)
        .await
        .map_err(map_err)?;

    let insert =
        format!("INSERT INTO {MIGRATIONS_TABLE} (module_id, version, name) VALUES ($1, $2, $3)");
    sqlx::query(&insert)
        .bind(module.as_str())
        .bind(migration.version as i64)
        .bind(&migration.name)
        .execute(&mut *tx)
        .await
        .map_err(map_err)?;

    tx.commit().await.map_err(map_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::Migration;

    fn id(s: &str) -> ModuleId {
        ModuleId::new(s).expect("valid module id")
    }

    #[test]
    fn pending_returns_all_when_nothing_applied() {
        let (beads, rigs) = (id("beads"), id("rigs"));
        let (m1, m2) = (Migration::new(1, "a", "SELECT 1"), Migration::new(1, "b", "SELECT 1"));
        let plan = vec![(&beads, &m1), (&rigs, &m2)];
        let applied = HashSet::new();

        let out = pending(&applied, &plan);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn pending_skips_already_applied_and_keeps_order() {
        let beads = id("beads");
        let (m1, m2, m3) = (
            Migration::new(1, "a", "SELECT 1"),
            Migration::new(2, "b", "SELECT 1"),
            Migration::new(3, "c", "SELECT 1"),
        );
        let plan = vec![(&beads, &m1), (&beads, &m2), (&beads, &m3)];
        let mut applied = HashSet::new();
        applied.insert(("beads".to_owned(), 2u32));

        let out = pending(&applied, &plan);
        let versions: Vec<u32> = out.iter().map(|(_, m)| m.version).collect();
        assert_eq!(versions, vec![1, 3]);
    }

    #[test]
    fn pending_namespaces_version_by_module() {
        // Same version number in two modules is two distinct migrations: applying
        // beads/1 must not mask rigs/1.
        let (beads, rigs) = (id("beads"), id("rigs"));
        let (m_beads, m_rigs) = (Migration::new(1, "a", "SELECT 1"), Migration::new(1, "b", "SELECT 1"));
        let plan = vec![(&beads, &m_beads), (&rigs, &m_rigs)];
        let mut applied = HashSet::new();
        applied.insert(("beads".to_owned(), 1u32));

        let out = pending(&applied, &plan);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0.as_str(), "rigs");
    }

    #[test]
    fn report_changed_reflects_applied() {
        let mut report = MigrationReport::default();
        assert!(!report.changed());
        report.applied.push(("beads".to_owned(), 1, "a".to_owned()));
        assert!(report.changed());
    }
}
