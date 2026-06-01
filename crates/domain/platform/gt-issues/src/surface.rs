//! Surface referential integrity for the `issues.*` write path (hq-core-mcp.9,
//! docs/10 §S3).
//!
//! A bead's `surface` is the set of physical paths (crates / repo paths) it
//! touches. Historically each entry was a bare string and nothing rejected a
//! stale (`apps/api/...`) or not-yet-existing (`migrations/gt-rig`) path, so the
//! graph drifted. S3 fixes the data model two ways:
//!
//! 1. **Per-path intent.** Each entry is now `{ "path": "...", "planned": bool }`.
//!    `planned:false` means "must exist on `main` now"; `planned:true` means "this
//!    bead will create it" — accepted unconditionally but NOT counted as existing
//!    by the readiness check until a delivery flips it to `planned:false`.
//! 2. **Back-compat.** A bare-string entry deserializes as `planned:false`,
//!    mirroring the `events.2` legacy-bare→v1 adapter, so existing rows and old
//!    callers keep working; each write normalizes them to the object form.
//!
//! The existence check reads the in-process git tree of the gt-core repo the
//! server already serves (no extra clone) through the [`SurfaceTree`] port; the
//! git adapter lives in the server bin (orchestration tier) where shelling out is
//! allowed. The domain crate stays free of process/IO so it tests against a fake.

use gt_store_dolt::AppError;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

/// One entry of a bead's impact surface (docs/10 §S3). Serializes to the object
/// form `{"path":"...","planned":false}`; deserializes from EITHER that object
/// OR a bare string (read as `planned:false`) for back-compat with the legacy
/// `["path", …]` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SurfaceEntry {
    /// Crate name or repo path the bead touches. Non-empty.
    pub path: String,
    /// `false` (default): the path must already exist on `main` — the validator
    /// rejects a stale/missing one. `true`: the bead will create it, so it is
    /// accepted without an existence check and stays uncounted by readiness until
    /// a delivery flips it to `false`.
    #[serde(default)]
    pub planned: bool,
}

impl<'de> Deserialize<'de> for SurfaceEntry {
    /// Accept the object form `{path, planned}` or a bare string. The bare-string
    /// branch is the legacy `["path"]` adapter: it reads as `planned:false`.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `untagged` tries the string arm first, then the struct arm, so a JSON
        // string maps to `Bare` and a JSON object to `Obj`. An object missing
        // `path` fails both arms and surfaces as a deserialization error.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bare(String),
            Obj {
                path: String,
                #[serde(default)]
                planned: bool,
            },
        }
        Ok(match Raw::deserialize(deserializer)? {
            Raw::Bare(path) => SurfaceEntry { path, planned: false },
            Raw::Obj { path, planned } => SurfaceEntry { path, planned },
        })
    }
}

/// Read model of the `main` git tree, used for surface existence checks (S3).
/// The port the server's git adapter implements; the domain crate depends only
/// on this trait so it never shells out to `git` itself.
pub trait SurfaceTree {
    /// True when the tree contains a blob at, or any path under, `path` (so a
    /// surface entry naming a directory like `crates/domain/platform/gt-issues`
    /// matches the files beneath it). `path` is trimmed and de-slashed by callers.
    fn contains(&self, path: &str) -> bool;
}

/// A permissive [`SurfaceTree`] that accepts every path. Used when no repo is
/// wired (the live container has no gt-core checkout) so surface validation
/// degrades to today's behaviour instead of rejecting every write, and as the
/// fake in unit tests that exercise the `planned` short-circuit.
pub struct AllowAllTree;

impl SurfaceTree for AllowAllTree {
    fn contains(&self, _path: &str) -> bool {
        true
    }
}

/// Reject an empty path or a duplicate path in a surface list. Mirrors the old
/// `check_no_empty_or_dup` but operates on the typed [`SurfaceEntry`] so the
/// `planned` flag is preserved. Returns the offending value verbatim.
pub fn check_surface_shape(items: &[SurfaceEntry]) -> Result<(), AppError> {
    let mut seen = std::collections::HashSet::new();
    for e in items {
        if e.path.trim().is_empty() {
            return Err(AppError::Validation("surface contains an empty path".into()));
        }
        if !seen.insert(e.path.as_str()) {
            return Err(AppError::Validation(format!(
                "surface lists `{}` more than once",
                e.path
            )));
        }
    }
    Ok(())
}

/// Assert every `planned:false` surface path exists on `main` (S3). A
/// `planned:true` entry is skipped — the bead will create it. Reads the tree
/// through the [`SurfaceTree`] port so the check is identical whether the tree is
/// the live git adapter or a test fake.
pub fn check_surface_existence(
    items: &[SurfaceEntry],
    tree: &(dyn SurfaceTree + Sync),
) -> Result<(), AppError> {
    for e in items {
        if e.planned {
            continue;
        }
        if !tree.contains(e.path.trim().trim_end_matches('/')) {
            return Err(AppError::Validation(format!(
                "surface path `{}` does not exist on main — mark it planned:true if this bead will create it (docs/10 §S3)",
                e.path
            )));
        }
    }
    Ok(())
}

/// JSON-array string for a surface list, in the canonical object form
/// `[{"path":"x","planned":false}, …]`. `"[]"` on the (infallible) serialize
/// error path, matching the store's NOT-NULL default.
pub fn surface_to_json(items: &[SurfaceEntry]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a stored `surface_json` string into typed entries. Tolerates both the
/// new object form and the legacy bare-string form (each bare string reads as
/// `planned:false`); a malformed value yields an empty surface rather than an
/// error so a read path never fails on a historical row.
pub fn parse_surface_json(surface_json: &str) -> Vec<SurfaceEntry> {
    serde_json::from_str(surface_json).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_bare_string_as_not_planned() {
        let v: Vec<SurfaceEntry> = serde_json::from_str(r#"["crates/a", "crates/b"]"#).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], SurfaceEntry { path: "crates/a".into(), planned: false });
        assert!(!v[1].planned);
    }

    #[test]
    fn deserializes_object_form() {
        let v: Vec<SurfaceEntry> =
            serde_json::from_str(r#"[{"path":"x","planned":true},{"path":"y"}]"#).unwrap();
        assert_eq!(v[0], SurfaceEntry { path: "x".into(), planned: true });
        // `planned` defaults to false when omitted.
        assert_eq!(v[1], SurfaceEntry { path: "y".into(), planned: false });
    }

    #[test]
    fn deserializes_mixed_bare_and_object() {
        let v: Vec<SurfaceEntry> =
            serde_json::from_str(r#"["bare", {"path":"obj","planned":true}]"#).unwrap();
        assert_eq!(v[0], SurfaceEntry { path: "bare".into(), planned: false });
        assert_eq!(v[1], SurfaceEntry { path: "obj".into(), planned: true });
    }

    #[test]
    fn serializes_to_object_form() {
        let items = vec![SurfaceEntry { path: "x".into(), planned: true }];
        assert_eq!(surface_to_json(&items), r#"[{"path":"x","planned":true}]"#);
    }

    #[test]
    fn empty_surface_serializes_to_empty_array() {
        assert_eq!(surface_to_json(&[]), "[]");
    }

    #[test]
    fn object_missing_path_is_rejected() {
        assert!(serde_json::from_str::<Vec<SurfaceEntry>>(r#"[{"planned":true}]"#).is_err());
    }

    #[test]
    fn shape_rejects_empty_and_duplicate() {
        assert!(check_surface_shape(&[SurfaceEntry { path: "  ".into(), planned: false }]).is_err());
        let dup = vec![
            SurfaceEntry { path: "x".into(), planned: false },
            SurfaceEntry { path: "x".into(), planned: true },
        ];
        assert!(check_surface_shape(&dup).is_err());
    }

    /// A tree that contains only the paths it is told about (prefix-aware), so the
    /// existence check can be exercised without a real git repo.
    struct FakeTree(Vec<&'static str>);
    impl SurfaceTree for FakeTree {
        fn contains(&self, path: &str) -> bool {
            self.0
                .iter()
                .any(|t| *t == path || t.starts_with(&format!("{path}/")))
        }
    }

    #[test]
    fn existence_rejects_missing_non_planned_path() {
        let tree = FakeTree(vec!["crates/domain/platform/gt-issues/src/lib.rs"]);
        // Present (directory prefix) → ok.
        assert!(check_surface_existence(
            &[SurfaceEntry { path: "crates/domain/platform/gt-issues".into(), planned: false }],
            &tree
        )
        .is_ok());
        // Missing + planned:false → rejected.
        assert!(check_surface_existence(
            &[SurfaceEntry { path: "migrations/gt-rig".into(), planned: false }],
            &tree
        )
        .is_err());
        // Missing + planned:true → accepted (the bead will create it).
        assert!(check_surface_existence(
            &[SurfaceEntry { path: "migrations/gt-rig".into(), planned: true }],
            &tree
        )
        .is_ok());
    }

    #[test]
    fn allow_all_tree_accepts_everything() {
        assert!(check_surface_existence(
            &[SurfaceEntry { path: "anything/at/all".into(), planned: false }],
            &AllowAllTree
        )
        .is_ok());
    }
}
