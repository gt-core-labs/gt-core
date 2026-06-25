//! `domain.catalog.*` domain dispatch (gtcore-b37400 H4 — child of gtcore-ebd547).
//!
//! The per-workspace domain catalog (the `domain_catalog` table H1/gtcore-55d5fb
//! introduced) is now operator-EDITABLE through five tools:
//!
//! | Tool                       | Effect                                            |
//! |----------------------------|---------------------------------------------------|
//! | `domain.catalog.list`      | read the active workspace's catalog               |
//! | `domain.catalog.add`       | insert a new (enabled, non-reserved) domain       |
//! | `domain.catalog.rename`    | change a domain's display label                   |
//! | `domain.catalog.set-enabled` | toggle whether a domain is assignable to beads  |
//! | `domain.catalog.remove`    | delete a domain                                   |
//!
//! ## Workspace resolution
//!
//! The catalog lives in the SAME Dolt store the issues surface writes to, so this
//! handler resolves the pool EXACTLY as the server's `resolve_store` does
//! ([`server.rs`](../../../gt-mcp-server/src/server.rs)): in multi-tenant mode
//! (`GT_DOLT_BASE_URL`) the active workspace's `hq_<ws>` pool, otherwise the shared
//! default `hq` store. That guarantees a `domain.catalog.add` is read back by the
//! bead-create domain validation (gtcore-d81e77 H2), which builds its
//! [`DoltDomainCatalog`] over the very same resolved pool — the loop the bead's
//! "el selector usa el catálogo del workspace activo" criterion closes.
//!
//! ## Authorization
//!
//! Editing the catalog is an ADMIN operation. The MCP scope gate folds a granted
//! `domain.<verb>` REST scope into the `domain.*` allow pattern, and only an
//! admin/`*` token (or one explicitly granted `domain.read`/`domain.write`) carries
//! it — the agent presets never do, so a polecat cannot mutate the catalog. The
//! reserved `meta.gap` is protected one layer deeper, in the store: it is neither
//! editable nor deletable, since the auto-minted gap beads must always carry it.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_store_dolt::{
    validate_domain_key, AppError, DoltDomainCatalog, DoltIssues, DomainEntry, WorkspacePools,
    RESERVED_GAP_KEY,
};

use super::util::{descriptor, opt, req, str_arg};

/// Dolt-backed handler for the `domain.catalog.*` tool namespace.
///
/// Holds the shared default-workspace store (the single-tenant `hq` fallback) and,
/// when multi-tenant Dolt routing is configured, the per-workspace pool registry —
/// the same two halves the server's `resolve_store` chooses between.
pub struct DomainCatalogHandler {
    /// The shared default-workspace store (`GT_DOLT_URL` → `hq`), used when there is
    /// no multi-tenant routing or the call resolved to the default workspace.
    store: Arc<DoltIssues>,
    /// Per-workspace Dolt pools (`GT_DOLT_BASE_URL`). `Some` ⇒ a non-default active
    /// workspace selects its own `hq_<ws>` catalog; `None` ⇒ single-tenant.
    dolt: Option<Arc<WorkspacePools>>,
}

impl DomainCatalogHandler {
    /// Wrap the shared default store. Single-tenant until [`with_dolt`](Self::with_dolt)
    /// attaches the per-workspace pool registry.
    pub fn new(store: Arc<DoltIssues>) -> Self {
        Self { store, dolt: None }
    }

    /// Attach the per-workspace Dolt pools so a non-default active workspace edits its
    /// own `hq_<ws>` catalog (multi-tenant routing, `GT_DOLT_BASE_URL`).
    pub fn with_dolt(mut self, dolt: Arc<WorkspacePools>) -> Self {
        self.dolt = Some(dolt);
        self
    }

    /// Resolve the `domain_catalog` repo for the call's active workspace, mirroring
    /// the server's `resolve_store`: the tenant's `hq_<ws>` pool in multi-tenant
    /// mode, else the shared default store. The slug is re-validated inside
    /// [`WorkspacePools::pool_for`] before any database is selected.
    async fn catalog(&self, ws: Option<&str>) -> Result<DoltDomainCatalog, AppError> {
        let pool = match (&self.dolt, ws) {
            (Some(pools), Some(slug)) => pools.ensured_pool(slug).await?,
            _ => self.store.pool().clone(),
        };
        Ok(DoltDomainCatalog::new(pool))
    }
}

#[async_trait]
impl DomainHandler for DomainCatalogHandler {
    fn namespace(&self) -> &'static str {
        "domain"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "domain.catalog.list",
                "List the active workspace's domain catalog: every entry \
                 { key, label, tier, enabled, reserved }. Read-only.",
                &[],
            ),
            descriptor(
                "domain.catalog.add",
                "Add a new domain to the active workspace's catalog (enabled, \
                 non-reserved). `key` is the stable identifier beads carry; `label` \
                 defaults to the key, `tier` is an optional coarse grouping. A \
                 duplicate key is rejected.",
                &[req("key", "string"), opt("label", "string"), opt("tier", "string")],
            ),
            descriptor(
                "domain.catalog.rename",
                "Change a domain's display label, leaving its key untouched (the key \
                 is the identifier beads carry). The reserved `meta.gap` is not editable.",
                &[req("key", "string"), req("label", "string")],
            ),
            descriptor(
                "domain.catalog.set-enabled",
                "Enable or disable a domain. A disabled domain stays in the catalog \
                 but is rejected for new beads. The reserved `meta.gap` is not editable.",
                &[req("key", "string"), req("enabled", "boolean")],
            ),
            descriptor(
                "domain.catalog.remove",
                "Delete a domain from the active workspace's catalog. The reserved \
                 `meta.gap` may not be deleted.",
                &[req("key", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let catalog = self.catalog(ctx.workspace).await?;
        match tool {
            "domain.catalog.list" => {
                // `list_opt` is table-absent tolerant: an un-seeded workspace (no
                // `domain_catalog` table yet — pending the H3 backfill) reads as an
                // empty catalog rather than a 500, the same degrade the REST read uses.
                let entries = catalog.list_opt().await?.unwrap_or_default();
                Ok(json!({ "domains": entries.iter().map(entry_json).collect::<Vec<_>>() }))
            }
            "domain.catalog.add" => {
                let key = validate_domain_key(str_arg(&ctx.args, "key")?)?;
                if key == RESERVED_GAP_KEY {
                    return Err(AppError::Validation(format!(
                        "domain '{RESERVED_GAP_KEY}' is reserved and seeded into every catalog; \
                         it cannot be added"
                    )));
                }
                // Label defaults to the key (a serviceable display name until edited);
                // tier is optional (flat business domains carry none).
                let label = opt_str(&ctx.args, "label").unwrap_or(&key).to_string();
                let tier = opt_str(&ctx.args, "tier").map(str::to_string);
                let entry = DomainEntry::new(key.clone(), label, tier);
                catalog.add(&entry).await?;
                Ok(json!({ "ok": true, "key": key, "added": entry_json(&entry) }))
            }
            "domain.catalog.rename" => {
                let key = str_arg(&ctx.args, "key")?;
                let label = str_arg(&ctx.args, "label")?;
                catalog.rename(key, label).await?;
                Ok(json!({ "ok": true, "key": key, "label": label }))
            }
            "domain.catalog.set-enabled" => {
                let key = str_arg(&ctx.args, "key")?;
                let enabled = ctx.args.get("enabled").and_then(Value::as_bool).ok_or_else(|| {
                    AppError::Validation("missing boolean argument `enabled`".to_string())
                })?;
                catalog.set_enabled(key, enabled).await?;
                Ok(json!({ "ok": true, "key": key, "enabled": enabled }))
            }
            "domain.catalog.remove" => {
                let key = str_arg(&ctx.args, "key")?;
                let removed = catalog.delete(key).await?;
                if !removed {
                    return Err(AppError::NotFound(format!(
                        "domain '{key}' not found in this workspace's catalog"
                    )));
                }
                Ok(json!({ "ok": true, "key": key, "removed": true }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

/// Shape one catalog entry as the dispatch payload (matches the REST `GET /domains`
/// projection so the two transports return identical rows).
fn entry_json(e: &DomainEntry) -> Value {
    json!({
        "key": e.key,
        "label": e.label,
        "tier": e.tier,
        "enabled": e.enabled,
        "reserved": e.reserved,
    })
}

/// Pull an OPTIONAL string argument: `Some(&str)` when present and a non-empty
/// string, `None` otherwise (a blank value falls back to the default).
fn opt_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn handler() -> DomainCatalogHandler {
        // A never-connected store is fine for the pre-I/O argument-validation tests:
        // they assert the validation fault fires before any pool is dialed.
        let store = Arc::new(DoltIssues::connect("mysql://x@127.0.0.1:1/none").unwrap());
        DomainCatalogHandler::new(store)
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx { workspace: None, actor: "tester", args }
    }

    #[tokio::test]
    async fn add_rejects_reserved_gap_before_any_io() {
        let err = handler()
            .dispatch("domain.catalog.add", ctx(json!({ "key": "meta.gap" })))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn add_rejects_a_malformed_key_before_any_io() {
        let err = handler()
            .dispatch("domain.catalog.add", ctx(json!({ "key": "Bad Key" })))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn add_requires_a_key() {
        let err = handler()
            .dispatch("domain.catalog.add", ctx(json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[tokio::test]
    async fn set_enabled_requires_a_boolean() {
        let err = handler()
            .dispatch("domain.catalog.set-enabled", ctx(json!({ "key": "producto" })))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[tokio::test]
    async fn unknown_verb_is_validation() {
        let err = handler()
            .dispatch("domain.catalog.frobnicate", ctx(json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn descriptors_cover_the_five_tools() {
        let names: Vec<String> =
            handler().descriptors().into_iter().map(|t| t.name).collect();
        for want in [
            "domain.catalog.list",
            "domain.catalog.add",
            "domain.catalog.rename",
            "domain.catalog.set-enabled",
            "domain.catalog.remove",
        ] {
            assert!(names.iter().any(|n| n == want), "missing descriptor {want}");
        }
    }
}
