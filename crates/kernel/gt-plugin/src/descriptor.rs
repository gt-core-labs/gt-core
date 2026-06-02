//! Plugin descriptor types — port of the Go `internal/plugin/types.go` vocabulary.
//!
//! These mirror the on-disk shape of a `plugin.md` file (TOML frontmatter + markdown body)
//! plus the runtime metadata the scanner attaches (`location`, `path`, `rig_name`,
//! `has_run_script`, `instructions`). The dispatcher concerns (rendering instructions for a
//! Dog worker, executing `run.sh`) belong to the future Sheriff/Deacon roles (Paso 9.D/9.E),
//! not here — this module is pure data.

use serde::{Deserialize, Serialize};

/// Where a plugin was discovered. Town-level entries are shared across rigs; rig-level
/// entries shadow them by name (`Scanner::discover_all`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Location {
    Town,
    Rig,
}

/// Gate kind controlling when a plugin should run. Mirrors Go's `GateType`; unknown values
/// from old plugin.md files deserialize as [`GateType::Manual`] (the safest no-op default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GateType {
    Cooldown,
    Cron,
    Condition,
    Event,
    Manual,
}

impl Default for GateType {
    fn default() -> Self {
        Self::Manual
    }
}

/// Gate frontmatter: which fields are populated depends on `type`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gate {
    #[serde(rename = "type")]
    pub kind: GateType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<String>,
}

/// Tracking metadata applied to execution wisps / receipt beads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tracking {
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub digest: bool,
}

/// Execution kind. `Agent` is the default (a Dog interprets the markdown), `Script` runs the
/// sibling `run.sh`, `ExecWrapper` wraps a session-startup command (see Go `ExecTypeExecWrapper`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionType {
    Agent,
    Script,
    ExecWrapper,
}

impl Default for ExecutionType {
    fn default() -> Self {
        Self::Agent
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Execution {
    #[serde(rename = "type", default)]
    pub kind: ExecutionType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, rename = "notify_on_failure")]
    pub notify_on_failure: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wrapper: Vec<String>,
}

/// TOML frontmatter as written in `plugin.md`. The scanner combines this with the markdown
/// body and on-disk metadata to produce a [`Plugin`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PluginFrontmatter {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub gate: Option<Gate>,
    #[serde(default)]
    pub tracking: Option<Tracking>,
    #[serde(default)]
    pub execution: Option<Execution>,
}

/// A discovered plugin: frontmatter + markdown body + on-disk context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Plugin {
    pub name: String,
    pub description: String,
    pub version: u32,
    pub location: Location,
    pub path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub rig_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gate: Option<Gate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracking: Option<Tracking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<Execution>,
    pub instructions: String,
    #[serde(default)]
    pub has_run_script: bool,
}

impl Plugin {
    pub fn is_exec_wrapper(&self) -> bool {
        matches!(
            self.execution.as_ref().map(|e| e.kind),
            Some(ExecutionType::ExecWrapper)
        )
    }

    /// Wrapper command tokens for an `exec-wrapper` plugin (empty for any other kind).
    pub fn exec_wrapper_args(&self) -> &[String] {
        match self.execution.as_ref() {
            Some(e) if e.kind == ExecutionType::ExecWrapper => &e.wrapper,
            _ => &[],
        }
    }

    pub fn summary(&self) -> PluginSummary {
        PluginSummary {
            name: self.name.clone(),
            description: self.description.clone(),
            location: self.location,
            rig_name: self.rig_name.clone(),
            gate_type: self.gate.as_ref().map(|g| g.kind).unwrap_or_default(),
            execution_type: self.execution.as_ref().map(|e| e.kind).unwrap_or_default(),
            path: self.path.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginSummary {
    pub name: String,
    pub description: String,
    pub location: Location,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub rig_name: String,
    pub gate_type: GateType,
    pub execution_type: ExecutionType,
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_parses_a_minimal_plugin() {
        let toml_src = r#"
name = "recording"
description = "Watch domain events and record them"
version = 1

[gate]
type = "cooldown"
duration = "1h"

[tracking]
labels = ["type:plugin-run"]
digest = true

[execution]
type = "agent"
timeout = "5m"
notify_on_failure = true
"#;
        let fm: PluginFrontmatter = toml::from_str(toml_src).expect("parse");
        assert_eq!(fm.name, "recording");
        assert_eq!(fm.version, 1);
        assert_eq!(fm.gate.as_ref().unwrap().kind, GateType::Cooldown);
        assert_eq!(fm.gate.as_ref().unwrap().duration.as_deref(), Some("1h"));
        assert!(fm.tracking.as_ref().unwrap().digest);
        assert!(fm.execution.as_ref().unwrap().notify_on_failure);
    }

    #[test]
    fn exec_wrapper_args_returns_tokens_only_for_exec_wrapper() {
        let mut p = Plugin {
            name: "shadow".into(),
            description: String::new(),
            version: 1,
            location: Location::Town,
            path: "/x".into(),
            rig_name: String::new(),
            gate: None,
            tracking: None,
            execution: Some(Execution {
                kind: ExecutionType::ExecWrapper,
                timeout: None,
                notify_on_failure: false,
                severity: None,
                wrapper: vec!["exitbox".into(), "run".into()],
            }),
            instructions: String::new(),
            has_run_script: false,
        };
        assert!(p.is_exec_wrapper());
        assert_eq!(p.exec_wrapper_args(), &["exitbox".to_string(), "run".into()]);

        p.execution.as_mut().unwrap().kind = ExecutionType::Agent;
        assert!(!p.is_exec_wrapper());
        assert!(p.exec_wrapper_args().is_empty());
    }
}
