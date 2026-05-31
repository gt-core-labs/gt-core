//! HTTP path namespacing for module routers (`hq-mod-routes.2`).
//!
//! Every module's routes are mounted under a per-module prefix so two modules
//! can declare the same relative path (`/`, `/{id}`) without colliding, and so
//! a URL names its owning module. The builder applies this automatically in
//! [`Root::into_router`](crate::Root::into_router); a module author writes plain
//! relative routes and never types the prefix.
//!
//! The shape is fixed by `docs/03-architecture-guardrails.md` (rule: URL is
//! `/api/v1/<module>/...`, workspace comes from the JWT, never the path).

use crate::meta::ModuleId;

/// The version-pinned base every module's routes mount under.
///
/// Bumping the API version is a deliberate, cross-module event; keeping the base
/// in one place means a future `/api/v2` migration touches exactly this constant
/// and the beads that opt modules into it — not every call site.
pub const API_BASE: &str = "/api/v1";

/// The mount prefix for a module's routes: `/api/v1/<module-id>`.
///
/// `id` is a validated [`ModuleId`] (a lowercase slug), so the result is always a
/// well-formed, single-segment path suffix with no normalization needed. A
/// module that contributes routes for `beads` is mounted at `/api/v1/beads`, and
/// a relative `/` route it declares answers at `/api/v1/beads/`.
pub fn module_prefix(id: &ModuleId) -> String {
    format!("{API_BASE}/{}", id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_api_base_plus_id() {
        let id = ModuleId::new("beads").unwrap();
        assert_eq!(module_prefix(&id), "/api/v1/beads");
    }

    #[test]
    fn base_is_version_pinned() {
        assert_eq!(API_BASE, "/api/v1");
    }

    #[test]
    fn distinct_modules_get_distinct_prefixes() {
        let beads = module_prefix(&ModuleId::new("beads").unwrap());
        let rigs = module_prefix(&ModuleId::new("rigs").unwrap());
        assert_ne!(beads, rigs);
        assert!(beads.starts_with(API_BASE));
        assert!(rigs.starts_with(API_BASE));
    }
}
