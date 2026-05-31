//! Per-module SQL migration descriptor + directory convention (`hq-mod-migrate.1`).
//!
//! A module owns its schema. It declares the migrations it needs by overriding
//! [`GtModule::migrations`](crate::GtModule::migrations); the
//! [`RootBuilder`](crate::RootBuilder) collects them (alongside routes and MCP
//! tools) and exposes an ordered apply plan through
//! [`Root::migrations`](crate::Root::migrations). The composition root hand-wires
//! nothing (see `docs/03-architecture-guardrails.md` rule 3).
//!
//! ## Directory convention
//!
//! A module's migrations are namespaced under a directory named for its
//! [`ModuleId`](crate::ModuleId), one file per migration:
//!
//! ```text
//! migrations/<module-id>/<version:04>__<name>.sql
//! e.g. migrations/beads/0001__create_beads.sql
//! ```
//!
//! Namespacing by module id means two modules' version numbers are independent —
//! `beads` version 1 and `rigs` version 1 never collide, so adding or removing a
//! module never renumbers another's history. Within a module, [`version`] is
//! unique and migrations apply in ascending order; across modules they apply in
//! dependency-first init order (the same order
//! [`Root::modules`](crate::Root::modules) reports).
//!
//! [`version`]: Migration::version
//!
//! ## What this bead does and does not do
//!
//! `.1` is the kernel-side contract only: the [`Migration`] descriptor, the
//! filename/dir convention, the [`GtModule::migrations`](crate::GtModule::migrations)
//! hook, and the builder-side collection, ordering, and duplicate-version check.
//! It performs no I/O and pulls in no database crate. Applying the plan against
//! Postgres (the `sqlx` multi-source loader, disable-safe retention, backfill)
//! lands in `hq-mod-migrate.2..5` in the `gt-store-pg` adapter.

/// One SQL migration a module contributes.
///
/// Carries the SQL inline (a module typically supplies it with `include_str!`)
/// so the descriptor is self-contained and the kernel needs no filesystem
/// access. The owning module is implied by which module returned it from
/// [`GtModule::migrations`](crate::GtModule::migrations); the builder pairs the
/// two in the apply plan.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Migration {
    /// Monotonic version within the contributing module, unique per module.
    /// Migrations apply in ascending `version` order.
    pub version: u32,
    /// Short snake_case name describing the change, used in the filename and
    /// in apply logs (e.g. `create_beads`).
    pub name: String,
    /// The SQL statements to apply.
    pub sql: String,
}

impl Migration {
    /// Construct a migration descriptor.
    pub fn new(version: u32, name: impl Into<String>, sql: impl Into<String>) -> Self {
        Migration {
            version,
            name: name.into(),
            sql: sql.into(),
        }
    }

    /// The conventional filename for this migration: `<version:04>__<name>.sql`
    /// (e.g. `0001__create_beads.sql`). The leading zero-padding keeps lexical
    /// and numeric order aligned for the first 9999 migrations.
    pub fn filename(&self) -> String {
        format!("{:04}__{}.sql", self.version, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_zero_pads_version() {
        let m = Migration::new(1, "create_beads", "CREATE TABLE beads ();");
        assert_eq!(m.filename(), "0001__create_beads.sql");
    }

    #[test]
    fn filename_keeps_wide_versions() {
        let m = Migration::new(12345, "x", "");
        assert_eq!(m.filename(), "12345__x.sql");
    }

    #[test]
    fn descriptor_records_parts() {
        let m = Migration::new(3, "add_index", "CREATE INDEX ...;");
        assert_eq!(m.version, 3);
        assert_eq!(m.name, "add_index");
        assert_eq!(m.sql, "CREATE INDEX ...;");
    }
}
