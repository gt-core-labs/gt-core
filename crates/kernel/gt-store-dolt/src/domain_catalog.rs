//! Per-workspace domain catalog (gtcore-55d5fb H1 — the BASE of the
//! per-workspace domain model, child of gtcore-ebd547).
//!
//! ## What this is
//!
//! Today a bead's `domain[]` is validated against the closed Rust enum
//! [`Domain`](../../gt-issues/taxonomy) — one global, technical taxonomy baked
//! into the binary. A *business* workspace ("acme") has no use for
//! `kernel.events` / `orch.merge`; it wants its own domains (sales, billing, …).
//! This table makes the domain set **data, per workspace**: tenant `ws` lives in
//! Dolt database `hq_<ws>` (exactly like `hq.issues`, reached through the same
//! [`WorkspacePools`](crate::WorkspacePools)) and owns a `domain_catalog` table.
//!
//! H1 ships only the MODEL: the table, an internal CRUD repo, and the seed. The
//! enum stays the *source* of the technical seed (H2 relaxes the write-path
//! VALIDATION so the catalog — not the enum — becomes the arbiter; H3 backfills
//! catalogs for workspaces provisioned before this landed). Existing beads are
//! untouched: their `domain_json` stays free strings the read path already
//! tolerates.
//!
//! ## Seed
//!
//! Two seed shapes, both idempotent:
//!
//! - **Generic template** ([`generic_template`]) — a small, editable placeholder
//!   set (`producto`, `datos`, …) a fresh *business* workspace receives as a
//!   reusable base. Kernel-local data: it carries no enum meaning.
//! - **Technical set** — the ~45 [`Domain`] variants. The kernel crate may not
//!   import the platform `Domain` enum (docs/03 Rule 4), so the HOST builds the
//!   entry list from `Domain::ALL` and passes it to [`DoltDomainCatalog::seed`].
//!   This keeps the enum the single source and the catalog from diverging.
//!
//! `meta.gap` is RESERVED and seeded into EVERY catalog (technical and template
//! alike) because the auto-created gap beads carry it — a reserved row a UI hides
//! from the editable set.

use mysql_async::prelude::*;
use mysql_async::Pool;
use serde::{Deserialize, Serialize};

use crate::conn::map_err;
use crate::error::AppError;

/// One row of a workspace's `domain_catalog`.
///
/// `key` is the dotted/slug identifier (`orch.merge`, `producto`) unique within
/// the workspace; `label` is the human display name; `tier` the optional coarse
/// grouping (the segment before the first `.` for technical keys, `NULL` for a
/// flat business domain); `enabled` whether it may be assigned to new beads;
/// `reserved` whether it is a system-owned domain present in every catalog
/// (`meta.gap`) that the operator may not delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainEntry {
    /// Stable per-workspace identifier, e.g. `orch.merge` or `producto`.
    pub key: String,
    /// Human-readable display name.
    pub label: String,
    /// Optional coarse tier (`kernel`, `orch`, …); `None` for a flat domain.
    pub tier: Option<String>,
    /// Whether the domain may be assigned to new beads.
    pub enabled: bool,
    /// Whether the domain is system-reserved (present in every catalog, not
    /// operator-deletable). `meta.gap` is the one reserved seed.
    pub reserved: bool,
}

impl DomainEntry {
    /// Build an enabled, non-reserved entry, deriving `label` from `key` when no
    /// explicit label is wanted (the key doubles as a serviceable display name).
    pub fn new(key: impl Into<String>, label: impl Into<String>, tier: Option<String>) -> Self {
        Self { key: key.into(), label: label.into(), tier, enabled: true, reserved: false }
    }
}

/// The editable generic template a fresh business workspace receives as its
/// reusable base (gtcore-55d5fb). Placeholder, adjustable later — these are not
/// enum-backed, they are plain data. `meta.gap` is appended by the seed path as
/// the reserved domain, so it is intentionally NOT listed here.
///
/// `(key, label)`; tier is `None` (flat business domains).
pub const GENERIC_TEMPLATE: &[(&str, &str)] = &[
    ("producto", "Producto"),
    ("datos", "Datos"),
    ("backend", "Backend"),
    ("frontend", "Frontend"),
    ("integracion", "Integración"),
    ("reportes", "Reportes"),
    ("infra", "Infraestructura"),
    ("seguridad-cumplimiento", "Seguridad y cumplimiento"),
    ("docs", "Documentación"),
    ("proceso", "Proceso"),
];

/// The reserved domain present in EVERY catalog — the key the auto-created gap
/// beads carry (`meta.gap`). The seed paths append it so a template workspace and
/// a technical workspace both carry it.
pub const RESERVED_GAP_KEY: &str = "meta.gap";

/// Materialise [`GENERIC_TEMPLATE`] (+ the reserved `meta.gap`) as [`DomainEntry`]
/// rows ready for [`DoltDomainCatalog::seed`]. Used to provision a business
/// workspace with the reusable base.
pub fn generic_template() -> Vec<DomainEntry> {
    let mut rows: Vec<DomainEntry> = GENERIC_TEMPLATE
        .iter()
        .map(|(k, l)| DomainEntry::new(*k, *l, None))
        .collect();
    rows.push(reserved_gap_entry());
    rows
}

/// The reserved `meta.gap` entry every catalog carries.
pub fn reserved_gap_entry() -> DomainEntry {
    DomainEntry {
        key: RESERVED_GAP_KEY.to_string(),
        label: "Gap (reservado)".to_string(),
        tier: Some("meta".to_string()),
        enabled: true,
        reserved: true,
    }
}

/// The base `domain_catalog` table DDL. One row per (workspace-implicit, key);
/// the workspace is the Dolt database (`hq_<ws>`), so the table needs no
/// `workspace` column — isolation is the database, mirroring `hq.issues`.
const CATALOG_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS domain_catalog (
    `key`      VARCHAR(255) PRIMARY KEY,
    label      VARCHAR(500) NOT NULL,
    tier       VARCHAR(64) NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    reserved   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)";

/// Repo for a single workspace's `domain_catalog`, over that tenant's Dolt pool.
///
/// Construction is cheap (`Pool` is an `Arc` handle); the host resolves the pool
/// per workspace through [`WorkspacePools`](crate::WorkspacePools), exactly as it
/// does for [`DoltIssues`](crate::DoltIssues).
pub struct DoltDomainCatalog {
    pool: Pool,
}

impl DoltDomainCatalog {
    /// Wrap a per-workspace Dolt pool.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// The underlying pool handle.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Create the `domain_catalog` table if absent (idempotent). Mirrors
    /// [`DoltIssues::ensure_schema`](crate::DoltIssues::ensure_schema): a guard on
    /// `information_schema.tables`, a `CREATE TABLE IF NOT EXISTS`, and a
    /// `DOLT_COMMIT` so the new table lands in history immediately. A second run
    /// is a no-op. Does NOT seed — seeding is an explicit [`seed`](Self::seed)
    /// call so the host decides which set (technical vs template) a workspace gets.
    pub async fn ensure_schema(&self) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let present: Option<i64> = conn
            .query_first(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = 'domain_catalog' LIMIT 1",
            )
            .await
            .map_err(map_err)?;
        if present.is_none() {
            conn.query_drop(CATALOG_TABLE_SQL).await.map_err(map_err)?;
            commit(&mut conn, "gt-core boot seed: domain_catalog table (gtcore-55d5fb)").await?;
        }
        Ok(())
    }

    /// Whether the catalog has any rows yet — the migration probe (gtcore-55d5fb:
    /// "si un workspace no tiene catálogo, queda pendiente de backfill (H3)").
    /// A workspace whose table exists but is EMPTY is treated as un-seeded.
    pub async fn is_seeded(&self) -> Result<bool, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let any: Option<i64> = conn
            .query_first("SELECT 1 FROM domain_catalog LIMIT 1")
            .await
            .map_err(map_err)?;
        Ok(any.is_some())
    }

    /// Idempotently seed `entries` into the catalog via per-row [`upsert`]
    /// semantics, then ONE `DOLT_COMMIT` for the batch. Re-running refreshes
    /// labels/tiers without duplicating rows (the PK is `key`). Used at boot for
    /// the technical set (host passes `Domain::ALL`-derived entries) and on
    /// business-workspace provision for [`generic_template`].
    ///
    /// [`upsert`]: Self::upsert
    pub async fn seed(&self, entries: &[DomainEntry]) -> Result<u64, AppError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let mut written = 0u64;
        for e in entries {
            Self::upsert_on(&mut conn, e).await?;
            written += 1;
        }
        commit(&mut conn, "domain_catalog seed (gtcore-55d5fb)").await?;
        Ok(written)
    }

    /// List the workspace's catalog, ordered for a stable UI (`tier` then `key`).
    pub async fn list(&self) -> Result<Vec<DomainEntry>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let rows: Vec<(String, String, Option<String>, bool, bool)> = conn
            .query(
                "SELECT `key`, label, tier, enabled, reserved
                 FROM domain_catalog
                 ORDER BY COALESCE(tier, ''), `key`",
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(key, label, tier, enabled, reserved)| DomainEntry {
                key,
                label,
                tier,
                enabled,
                reserved,
            })
            .collect())
    }

    /// Fetch one entry by key, or `None` when absent.
    pub async fn get(&self, key: &str) -> Result<Option<DomainEntry>, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        let row: Option<(String, String, Option<String>, bool, bool)> = conn
            .exec_first(
                "SELECT `key`, label, tier, enabled, reserved
                 FROM domain_catalog WHERE `key` = :key",
                mysql_async::params! { "key" => key },
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(|(key, label, tier, enabled, reserved)| DomainEntry {
            key,
            label,
            tier,
            enabled,
            reserved,
        }))
    }

    /// Insert-or-update one entry by `key` (the PK), then commit. The label,
    /// tier, enabled and reserved fields are overwritten on conflict; `updated_at`
    /// is re-stamped.
    pub async fn upsert(&self, entry: &DomainEntry) -> Result<(), AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        Self::upsert_on(&mut conn, entry).await?;
        commit(&mut conn, &format!("domain_catalog upsert {}", entry.key)).await
    }

    /// The upsert body, on a caller-held connection (so [`seed`](Self::seed) can
    /// batch many rows under one commit). Uses MySQL/Dolt
    /// `ON DUPLICATE KEY UPDATE` — a single statement, no read-modify-write race.
    async fn upsert_on(conn: &mut mysql_async::Conn, entry: &DomainEntry) -> Result<(), AppError> {
        conn.exec_drop(
            "INSERT INTO domain_catalog (`key`, label, tier, enabled, reserved)
             VALUES (:key, :label, :tier, :enabled, :reserved)
             ON DUPLICATE KEY UPDATE
                label = VALUES(label),
                tier = VALUES(tier),
                enabled = VALUES(enabled),
                reserved = VALUES(reserved),
                updated_at = CURRENT_TIMESTAMP",
            mysql_async::params! {
                "key" => &entry.key,
                "label" => &entry.label,
                "tier" => entry.tier.clone(),
                "enabled" => entry.enabled,
                "reserved" => entry.reserved,
            },
        )
        .await
        .map_err(map_err)
    }

    /// Delete one entry by key, committing the change. Returns `true` when a row
    /// was removed. A RESERVED row (`meta.gap`) is protected: deleting it is a
    /// validation error, since every catalog must carry it.
    pub async fn delete(&self, key: &str) -> Result<bool, AppError> {
        let mut conn = self.pool.get_conn().await.map_err(map_err)?;
        // Guard the reserved invariant before touching the row.
        let reserved: Option<bool> = conn
            .exec_first(
                "SELECT reserved FROM domain_catalog WHERE `key` = :key",
                mysql_async::params! { "key" => key },
            )
            .await
            .map_err(map_err)?;
        match reserved {
            None => return Ok(false),
            Some(true) => {
                return Err(AppError::Validation(format!(
                    "domain '{key}' is reserved and may not be deleted"
                )))
            }
            Some(false) => {}
        }
        let res = conn
            .exec_iter(
                "DELETE FROM domain_catalog WHERE `key` = :key",
                mysql_async::params! { "key" => key },
            )
            .await
            .map_err(map_err)?;
        let affected = res.affected_rows();
        let _ = res.drop_result().await.map_err(map_err)?;
        if affected > 0 {
            commit(&mut conn, &format!("domain_catalog delete {key}")).await?;
        }
        Ok(affected > 0)
    }
}

/// `DOLT_COMMIT('-A', '-m', msg)` so a write lands in history immediately, the
/// same discipline every `DoltIssues` write path uses. A "nothing to commit"
/// from a no-op write is tolerated (mirrors the issues repo's canon-update path).
async fn commit(conn: &mut mysql_async::Conn, msg: &str) -> Result<(), AppError> {
    let res = conn
        .exec_drop(
            "CALL DOLT_COMMIT('-A', '-m', :msg)",
            mysql_async::params! { "msg" => msg },
        )
        .await;
    if let Err(e) = res {
        if e.to_string().contains("nothing to commit") {
            return Ok(());
        }
        return Err(map_err(e));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_template_carries_reserved_gap() {
        let rows = generic_template();
        // The 10 placeholder domains + the reserved meta.gap.
        assert_eq!(rows.len(), GENERIC_TEMPLATE.len() + 1);
        let gap = rows.iter().find(|e| e.key == RESERVED_GAP_KEY).expect("meta.gap present");
        assert!(gap.reserved, "meta.gap must be reserved");
        assert_eq!(gap.tier.as_deref(), Some("meta"));
        // The business placeholders are flat (no tier) and not reserved.
        let prod = rows.iter().find(|e| e.key == "producto").unwrap();
        assert!(!prod.reserved);
        assert_eq!(prod.tier, None);
        assert!(prod.enabled);
        // Exactly one reserved row in the template.
        assert_eq!(rows.iter().filter(|e| e.reserved).count(), 1);
    }

    #[test]
    fn entry_new_is_enabled_unreserved() {
        let e = DomainEntry::new("sales", "Sales", None);
        assert!(e.enabled);
        assert!(!e.reserved);
        assert_eq!(e.label, "Sales");
        assert_eq!(e.tier, None);
    }
}
