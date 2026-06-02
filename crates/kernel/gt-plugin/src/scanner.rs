//! Plugin discovery — port of the Go `internal/plugin/scanner.go`.
//!
//! Layout (same as Go):
//! - Town-level:  `<town_root>/plugins/<name>/plugin.md` (+ optional `run.sh`)
//! - Rig-level:   `<town_root>/<rig>/plugins/<name>/plugin.md` (+ optional `run.sh`)
//!
//! Rig-level entries shadow town-level entries by name. The `plugin.md` body uses TOML
//! frontmatter delimited by `+++` (matching the Go `parsePluginMD` exactly so existing
//! plugin corpora load unchanged).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::descriptor::{Location, Plugin, PluginFrontmatter};

const FRONTMATTER_DELIMITER: &str = "+++";

#[derive(Debug, Error)]
pub enum ScanError {
    #[error("scanning plugin directory {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("parsing plugin.md at {path}: {reason}")]
    Parse { path: PathBuf, reason: String },
}

/// Scanner over a town root plus the rig names that may host rig-level plugin dirs.
pub struct Scanner {
    town_root: PathBuf,
    rig_names: Vec<String>,
}

impl Scanner {
    pub fn new(town_root: impl Into<PathBuf>, rig_names: Vec<String>) -> Self {
        Self {
            town_root: town_root.into(),
            rig_names,
        }
    }

    /// Discover every plugin. Town entries are scanned first; rig entries with the same name
    /// override them (same precedence as Go's `Scanner.DiscoverAll`).
    pub fn discover_all(&self) -> Result<Vec<Plugin>, ScanError> {
        let mut by_name: HashMap<String, Plugin> = HashMap::new();
        for p in self.scan_town()? {
            by_name.insert(p.name.clone(), p);
        }
        for rig in &self.rig_names {
            for p in self.scan_rig(rig)? {
                by_name.insert(p.name.clone(), p);
            }
        }
        let mut out: Vec<Plugin> = by_name.into_values().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Locate one plugin by name, rig-first then town (Go `GetPlugin`).
    pub fn get_plugin(&self, name: &str) -> Result<Option<Plugin>, ScanError> {
        for rig in &self.rig_names {
            let dir = self.town_root.join(rig).join("plugins").join(name);
            if let Some(p) = load_plugin(&dir, Location::Rig, rig)? {
                return Ok(Some(p));
            }
        }
        let dir = self.town_root.join("plugins").join(name);
        load_plugin(&dir, Location::Town, "")
    }

    /// Discover only `exec-wrapper` plugins.
    pub fn discover_exec_wrappers(&self) -> Result<Vec<Plugin>, ScanError> {
        Ok(self
            .discover_all()?
            .into_iter()
            .filter(Plugin::is_exec_wrapper)
            .collect())
    }

    /// Town + per-rig plugin directories (for tooling that wants to walk them itself).
    pub fn list_plugin_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = vec![self.town_root.join("plugins")];
        for rig in &self.rig_names {
            dirs.push(self.town_root.join(rig).join("plugins"));
        }
        dirs
    }

    fn scan_town(&self) -> Result<Vec<Plugin>, ScanError> {
        scan_directory(&self.town_root.join("plugins"), Location::Town, "")
    }

    fn scan_rig(&self, rig: &str) -> Result<Vec<Plugin>, ScanError> {
        scan_directory(&self.town_root.join(rig).join("plugins"), Location::Rig, rig)
    }
}

fn scan_directory(dir: &Path, location: Location, rig_name: &str) -> Result<Vec<Plugin>, ScanError> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ScanError::Io {
                path: dir.to_path_buf(),
                source,
            })
        }
    };

    let mut plugins = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| ScanError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if name_str.starts_with('.') {
            continue;
        }
        let file_type = entry.file_type().map_err(|source| ScanError::Io {
            path: entry.path(),
            source,
        })?;
        if !file_type.is_dir() {
            continue;
        }
        match load_plugin(&entry.path(), location, rig_name) {
            Ok(Some(p)) => plugins.push(p),
            Ok(None) => {}
            // Mirror the Go behavior: log to stderr and keep going so one malformed plugin
            // does not poison discovery.
            Err(e) => eprintln!("[gt-plugin] warning: {e}"),
        }
    }
    Ok(plugins)
}

fn load_plugin(
    plugin_dir: &Path,
    location: Location,
    rig_name: &str,
) -> Result<Option<Plugin>, ScanError> {
    let plugin_md = plugin_dir.join("plugin.md");
    let content = match fs::read_to_string(&plugin_md) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ScanError::Io {
                path: plugin_md,
                source,
            })
        }
    };

    let (frontmatter, body) = split_frontmatter(&content).map_err(|reason| ScanError::Parse {
        path: plugin_md.clone(),
        reason,
    })?;

    let fm: PluginFrontmatter = toml::from_str(frontmatter).map_err(|e| ScanError::Parse {
        path: plugin_md.clone(),
        reason: format!("toml: {e}"),
    })?;

    if fm.name.is_empty() {
        return Err(ScanError::Parse {
            path: plugin_md,
            reason: "missing required field: name".to_string(),
        });
    }

    let has_run_script = plugin_dir.join("run.sh").is_file();

    Ok(Some(Plugin {
        name: fm.name,
        description: fm.description,
        version: fm.version,
        location,
        path: plugin_dir.to_string_lossy().into_owned(),
        rig_name: rig_name.to_string(),
        gate: fm.gate,
        tracking: fm.tracking,
        execution: fm.execution,
        instructions: body.trim().to_string(),
        has_run_script,
    }))
}

fn split_frontmatter(content: &str) -> Result<(&str, &str), String> {
    let start = content
        .find(FRONTMATTER_DELIMITER)
        .ok_or_else(|| "missing TOML frontmatter (no opening +++)".to_string())?;
    let after_open = start + FRONTMATTER_DELIMITER.len();
    let end_rel = content[after_open..]
        .find(FRONTMATTER_DELIMITER)
        .ok_or_else(|| "missing TOML frontmatter (no closing +++)".to_string())?;
    let end = after_open + end_rel;
    Ok((&content[after_open..end], &content[end + FRONTMATTER_DELIMITER.len()..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    use crate::descriptor::{ExecutionType, GateType};

    fn write_plugin(dir: &Path, name: &str, body: &str) {
        fs::create_dir_all(dir.join(name)).unwrap();
        fs::write(dir.join(name).join("plugin.md"), body).unwrap();
    }

    #[test]
    fn discover_returns_town_plugin() {
        let tmp = tempdir().unwrap();
        let town = tmp.path();
        let body = r#"+++
name = "recording"
description = "watch events"
version = 1

[gate]
type = "cooldown"
duration = "1h"

[execution]
type = "agent"
+++
# Instructions
Do stuff
"#;
        write_plugin(&town.join("plugins"), "recording", body);

        let scanner = Scanner::new(town, Vec::new());
        let plugins = scanner.discover_all().unwrap();

        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "recording");
        assert_eq!(plugins[0].location, Location::Town);
        assert_eq!(plugins[0].gate.as_ref().unwrap().kind, GateType::Cooldown);
        assert_eq!(
            plugins[0].execution.as_ref().unwrap().kind,
            ExecutionType::Agent
        );
        assert!(plugins[0].instructions.starts_with("# Instructions"));
    }

    #[test]
    fn rig_overrides_town_by_name() {
        let tmp = tempdir().unwrap();
        let town = tmp.path();
        let body_town = "+++\nname = \"recording\"\ndescription = \"town\"\nversion = 1\n+++\nbody\n";
        let body_rig = "+++\nname = \"recording\"\ndescription = \"rig\"\nversion = 2\n+++\nbody\n";
        write_plugin(&town.join("plugins"), "recording", body_town);
        write_plugin(&town.join("rig-a").join("plugins"), "recording", body_rig);

        let scanner = Scanner::new(town, vec!["rig-a".into()]);
        let plugins = scanner.discover_all().unwrap();

        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].version, 2);
        assert_eq!(plugins[0].location, Location::Rig);
        assert_eq!(plugins[0].rig_name, "rig-a");
    }

    #[test]
    fn missing_plugins_dir_is_ok() {
        let tmp = tempdir().unwrap();
        let scanner = Scanner::new(tmp.path(), Vec::new());
        assert!(scanner.discover_all().unwrap().is_empty());
    }

    #[test]
    fn run_sh_marks_has_run_script() {
        let tmp = tempdir().unwrap();
        let town = tmp.path();
        let plugin_dir = town.join("plugins").join("scripted");
        fs::create_dir_all(&plugin_dir).unwrap();
        fs::write(
            plugin_dir.join("plugin.md"),
            "+++\nname = \"scripted\"\ndescription = \"\"\nversion = 1\n+++\n",
        )
        .unwrap();
        fs::write(plugin_dir.join("run.sh"), "#!/bin/sh\necho hi\n").unwrap();

        let scanner = Scanner::new(town, Vec::new());
        let plugins = scanner.discover_all().unwrap();

        assert_eq!(plugins.len(), 1);
        assert!(plugins[0].has_run_script);
    }

    #[test]
    fn get_plugin_searches_rig_first_then_town() {
        let tmp = tempdir().unwrap();
        let town = tmp.path();
        write_plugin(
            &town.join("plugins"),
            "alpha",
            "+++\nname = \"alpha\"\ndescription = \"town\"\nversion = 1\n+++\n",
        );
        write_plugin(
            &town.join("rig-a").join("plugins"),
            "alpha",
            "+++\nname = \"alpha\"\ndescription = \"rig\"\nversion = 9\n+++\n",
        );
        let scanner = Scanner::new(town, vec!["rig-a".into()]);
        let p = scanner.get_plugin("alpha").unwrap().unwrap();
        assert_eq!(p.version, 9);
        assert_eq!(p.location, Location::Rig);

        // Unknown plugin → None
        assert!(scanner.get_plugin("nope").unwrap().is_none());
    }

    #[test]
    fn list_plugin_dirs_includes_town_and_rigs() {
        let scanner = Scanner::new("/town", vec!["rig-a".into(), "rig-b".into()]);
        let dirs = scanner.list_plugin_dirs();
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/town/plugins"),
                PathBuf::from("/town/rig-a/plugins"),
                PathBuf::from("/town/rig-b/plugins"),
            ]
        );
    }

    #[test]
    fn missing_name_is_a_parse_error() {
        let tmp = tempdir().unwrap();
        write_plugin(
            &tmp.path().join("plugins"),
            "noname",
            "+++\ndescription = \"x\"\nversion = 1\n+++\n",
        );
        let scanner = Scanner::new(tmp.path(), Vec::new());
        // The bad plugin is skipped with a warning; discovery succeeds with zero plugins.
        let plugins = scanner.discover_all().unwrap();
        assert!(plugins.is_empty());
    }
}
