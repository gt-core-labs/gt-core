//! Plugin sync + drift detection — port of the Go `internal/plugin/sync.go`.
//!
//! Copies plugin directories from a source tree into a target tree atomically (per-plugin
//! tmp + rename), with optional clean mode that removes target plugins absent from source.
//! [`DirHash`](dir_hash) reproduces the Go content hash (sha256 over relative-path bytes plus
//! file bytes) so a Rust sync run is idempotent against a hash computed by the Go binary.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Result of [`sync_plugins`]: which plugins were copied, removed, skipped, or hit errors.
/// String error entries match the Go format `"<plugin>: <details>"` so consumer logs stay
/// stable across the two implementations.
#[derive(Debug, Default, Serialize)]
pub struct SyncResult {
    pub copied: Vec<String>,
    pub removed: Vec<String>,
    pub skipped: Vec<String>,
    pub errors: Vec<String>,
}

/// Copy each `plugin.md`-bearing directory from `source_dir` into `target_dir`. With
/// `clean = true`, removes target plugin directories whose name does not exist in source.
pub fn sync_plugins(
    source_dir: &Path,
    target_dir: &Path,
    clean: bool,
) -> io::Result<SyncResult> {
    let src_meta = fs::metadata(source_dir).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("source directory {}: {e}", source_dir.display()),
        )
    })?;
    if !src_meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("source is not a directory: {}", source_dir.display()),
        ));
    }

    fs::create_dir_all(target_dir)?;

    let mut result = SyncResult::default();
    let mut src_plugins: Vec<String> = Vec::new();

    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let src_plugin_dir = entry.path();
        if !src_plugin_dir.join("plugin.md").is_file() {
            continue;
        }
        src_plugins.push(name.clone());

        let dst_plugin_dir = target_dir.join(&name);
        if dirs_match(&src_plugin_dir, &dst_plugin_dir) {
            result.skipped.push(name);
            continue;
        }
        match copy_dir(&src_plugin_dir, &dst_plugin_dir) {
            Ok(()) => result.copied.push(name),
            Err(e) => result.errors.push(format!("{name}: {e}")),
        }
    }

    if clean {
        if let Ok(entries) = fs::read_dir(target_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    continue;
                }
                if !matches!(entry.file_type(), Ok(ft) if ft.is_dir()) {
                    continue;
                }
                if src_plugins.iter().any(|p| p == &name) {
                    continue;
                }
                let dst = target_dir.join(&name);
                match fs::remove_dir_all(&dst) {
                    Ok(()) => result.removed.push(name),
                    Err(e) => result.errors.push(format!("removing {name}: {e}")),
                }
            }
        }
    }

    Ok(result)
}

/// Drift report between a source and target plugin tree (Go `DetectDrift`).
#[derive(Debug, Default, Serialize)]
pub struct DriftReport {
    pub source: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drifted: Vec<DriftEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DriftEntry {
    pub name: String,
    pub source_hash: String,
    pub target_hash: String,
}

impl DriftReport {
    pub fn has_drift(&self) -> bool {
        !self.drifted.is_empty() || !self.missing.is_empty()
    }
}

/// Compute a drift report by walking source and target.
pub fn detect_drift(source_dir: &Path, target_dir: &Path) -> io::Result<DriftReport> {
    let mut report = DriftReport {
        source: source_dir.to_string_lossy().into_owned(),
        target: target_dir.to_string_lossy().into_owned(),
        ..DriftReport::default()
    };

    let src_entries = fs::read_dir(source_dir)?;

    let mut tgt_plugins: Vec<String> = Vec::new();
    if let Ok(t_entries) = fs::read_dir(target_dir) {
        for entry in t_entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if matches!(entry.file_type(), Ok(ft) if ft.is_dir()) {
                tgt_plugins.push(name);
            }
        }
    }

    for entry in src_entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        if !matches!(entry.file_type(), Ok(ft) if ft.is_dir()) {
            continue;
        }
        let src_dir = entry.path();
        if !src_dir.join("plugin.md").is_file() {
            continue;
        }
        let dst_dir = target_dir.join(&name);

        if let Some(pos) = tgt_plugins.iter().position(|p| p == &name) {
            tgt_plugins.swap_remove(pos);
            let src_hash = dir_hash(&src_dir);
            let dst_hash = dir_hash(&dst_dir);
            if src_hash != dst_hash {
                report.drifted.push(DriftEntry {
                    name,
                    source_hash: src_hash,
                    target_hash: dst_hash,
                });
            }
        } else {
            report.missing.push(name);
        }
    }

    for name in tgt_plugins {
        if target_dir.join(&name).join("plugin.md").is_file() {
            report.extra.push(name);
        }
    }

    Ok(report)
}

/// Look for a gastown source repo's `plugins/` directory. Mirrors Go `FindGastownSource`:
///  1. Walk up from `cwd` for a sibling `go.mod` declaring a `gastown` module + `plugins/`.
///  2. `<town_root>/gastown/crew/den/plugins/`
///  3. `<town_root>/gastown/plugins/`
pub fn find_gastown_source(town_root: &Path) -> io::Result<PathBuf> {
    if let Ok(cwd) = std::env::current_dir() {
        if let Some(src) = find_source_from_dir(&cwd) {
            return Ok(src);
        }
    }
    for candidate in [
        town_root.join("gastown/crew/den/plugins"),
        town_root.join("gastown/plugins"),
    ] {
        if has_plugins(&candidate) {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "could not locate gastown plugin source; pass an explicit source",
    ))
}

fn find_source_from_dir(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let plugins_dir = current.join("plugins");
        let go_mod = current.join("go.mod");
        if has_plugins(&plugins_dir) && is_gastown_module(&go_mod) {
            return Some(plugins_dir);
        }
        match current.parent() {
            Some(p) if p != current => current = p.to_path_buf(),
            _ => return None,
        }
    }
}

fn is_gastown_module(go_mod: &Path) -> bool {
    let Ok(file) = fs::File::open(go_mod) else {
        return false;
    };
    use std::io::BufRead;
    let reader = io::BufReader::new(file);
    for line in reader.lines().map_while(Result::ok) {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("module ") {
            return rest.ends_with("/gastown") || rest == "gastown";
        }
    }
    false
}

fn has_plugins(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') {
            continue;
        }
        if matches!(entry.file_type(), Ok(ft) if ft.is_dir())
            && entry.path().join("plugin.md").is_file()
        {
            return true;
        }
    }
    false
}

fn dirs_match(src: &Path, dst: &Path) -> bool {
    let s = dir_hash(src);
    let d = dir_hash(dst);
    !s.is_empty() && s == d
}

/// SHA-256 over relative-path bytes plus file contents for every entry inside `dir`.
/// Replicates the Go `DirHash` byte-for-byte so a Rust-computed hash compares equal to a
/// Go-computed hash of the same tree (gate for cross-impl idempotence).
pub fn dir_hash(dir: &Path) -> String {
    let mut hasher = Sha256::new();
    let mut walker_state = Ok(());
    walk_sorted(dir, dir, &mut |rel, full, is_dir| -> io::Result<()> {
        hasher.update(rel.to_string_lossy().as_bytes());
        if is_dir {
            return Ok(());
        }
        let mut f = fs::File::open(full)?;
        let mut buf = [0u8; 8192];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(())
    }, &mut walker_state);
    if walker_state.is_err() {
        return String::new();
    }
    format!("{:x}", hasher.finalize())
}

fn walk_sorted<F>(
    base: &Path,
    current: &Path,
    f: &mut F,
    state: &mut io::Result<()>,
) where
    F: FnMut(&Path, &Path, bool) -> io::Result<()>,
{
    if state.is_err() {
        return;
    }
    // Visit the directory itself (matches Go WalkDir which fires for the root too).
    let rel_self = match current.strip_prefix(base) {
        Ok(r) => r.to_path_buf(),
        Err(_) => PathBuf::new(),
    };
    if let Err(e) = f(&rel_self, current, true) {
        *state = Err(e);
        return;
    }
    let entries = match fs::read_dir(current) {
        Ok(e) => e,
        Err(e) => {
            *state = Err(e);
            return;
        }
    };
    let mut names: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    names.sort();
    for path in names {
        let is_dir = path.is_dir();
        if is_dir {
            walk_sorted(base, &path, f, state);
            if state.is_err() {
                return;
            }
        } else {
            let rel = path
                .strip_prefix(base)
                .map(Path::to_path_buf)
                .unwrap_or_default();
            if let Err(e) = f(&rel, &path, false) {
                *state = Err(e);
                return;
            }
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) -> io::Result<()> {
    let parent = dst
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent"))?;
    fs::create_dir_all(parent)?;

    let tmp = tempfile::Builder::new()
        .prefix(".plugin-sync-")
        .tempdir_in(parent)?;
    let tmp_path = tmp.path().to_path_buf();

    copy_tree(src, &tmp_path)?;

    // Atomic swap: drop old destination then rename temp into place.
    if dst.exists() {
        fs::remove_dir_all(dst)?;
    }
    let kept_tmp = tmp.keep();
    fs::rename(&kept_tmp, dst)?;
    Ok(())
}

fn copy_tree(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&from, &to)?;
        } else if file_type.is_symlink() {
            // Mirror the Go behavior of resolving and copying contents, not link target.
            let mut sf = fs::File::open(&from)?;
            let mut buf = Vec::new();
            sf.read_to_end(&mut buf)?;
            let mut df = fs::File::create(&to)?;
            df.write_all(&buf)?;
        } else {
            let meta = entry.metadata()?;
            let mut sf = fs::File::open(&from)?;
            let mut df = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&to)?;
            io::copy(&mut sf, &mut df)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode();
                fs::set_permissions(&to, fs::Permissions::from_mode(mode))?;
            }
            #[cfg(not(unix))]
            {
                let _ = meta;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_plugin(dir: &Path, name: &str, body: &str) {
        fs::create_dir_all(dir.join(name)).unwrap();
        fs::write(dir.join(name).join("plugin.md"), body).unwrap();
    }

    #[test]
    fn sync_copies_new_plugins_and_skips_unchanged() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        write_plugin(&src, "alpha", "+++\nname = \"alpha\"\nversion = 1\n+++\nbody\n");
        write_plugin(&src, "beta", "+++\nname = \"beta\"\nversion = 1\n+++\nbody\n");

        let r1 = sync_plugins(&src, &dst, false).unwrap();
        let mut copied = r1.copied;
        copied.sort();
        assert_eq!(copied, vec!["alpha".to_string(), "beta".into()]);
        assert!(r1.skipped.is_empty());
        assert!(r1.removed.is_empty());

        // Re-running should skip everything (hashes match).
        let r2 = sync_plugins(&src, &dst, false).unwrap();
        let mut skipped = r2.skipped;
        skipped.sort();
        assert_eq!(skipped, vec!["alpha".to_string(), "beta".into()]);
        assert!(r2.copied.is_empty());
    }

    #[test]
    fn clean_mode_removes_target_only_entries() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        write_plugin(&src, "alpha", "+++\nname = \"alpha\"\nversion = 1\n+++\nbody\n");
        // Pre-existing target-only plugin.
        write_plugin(&dst, "stale", "+++\nname = \"stale\"\nversion = 1\n+++\nbody\n");

        let r = sync_plugins(&src, &dst, true).unwrap();
        assert_eq!(r.copied, vec!["alpha".to_string()]);
        assert_eq!(r.removed, vec!["stale".to_string()]);
        assert!(!dst.join("stale").exists());
    }

    #[test]
    fn dir_hash_is_stable_and_detects_changes() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("p");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("plugin.md"), "v1").unwrap();

        let h1 = dir_hash(&dir);
        let h2 = dir_hash(&dir);
        assert!(!h1.is_empty());
        assert_eq!(h1, h2);

        fs::write(dir.join("plugin.md"), "v2").unwrap();
        let h3 = dir_hash(&dir);
        assert_ne!(h1, h3);
    }

    #[test]
    fn detect_drift_reports_missing_extra_and_changed() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();

        write_plugin(&src, "alpha", "+++\nname = \"alpha\"\nversion = 1\n+++\nv1\n");
        write_plugin(&src, "gamma", "+++\nname = \"gamma\"\nversion = 1\n+++\nv1\n");
        write_plugin(&dst, "alpha", "+++\nname = \"alpha\"\nversion = 1\n+++\nv2\n");
        write_plugin(&dst, "beta", "+++\nname = \"beta\"\nversion = 1\n+++\nv1\n");

        let report = detect_drift(&src, &dst).unwrap();
        assert_eq!(report.drifted.len(), 1);
        assert_eq!(report.drifted[0].name, "alpha");
        assert_eq!(report.missing, vec!["gamma".to_string()]);
        assert_eq!(report.extra, vec!["beta".to_string()]);
        assert!(report.has_drift());
    }

    #[test]
    fn copy_dir_preserves_subdirectories() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(src.join("nested")).unwrap();
        fs::write(src.join("plugin.md"), "+++\nname = \"x\"\nversion = 1\n+++\n").unwrap();
        fs::write(src.join("nested").join("note.txt"), "hi").unwrap();
        copy_dir(&src, &dst).unwrap();
        assert!(dst.join("plugin.md").is_file());
        assert!(dst.join("nested/note.txt").is_file());
    }
}
