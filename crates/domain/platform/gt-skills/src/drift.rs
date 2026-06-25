//! Seed-vs-live drift detection (`gtcore-63bb20`).
//!
//! The interactive-role Knowledge — each role's **system prompt**, **model config**, **scope**
//! grant, skill **bindings**, and the bound skills' **`SKILL.md` bodies** — ships TWICE:
//!
//! 1. **Embedded** in the binary as the versioned greenfield seed
//!    ([`crate::presets::workspace_seed_events`] over `seeds/knowledge.json`), and
//! 2. **Live** in each workspace's event-sourced `skills.*` log, which operators edit via the
//!    Knowledge REST surface ([`crate::http`]) with no redeploy.
//!
//! These diverge over time: prod's curated Knowledge moves on while the embedded snapshot stays
//! frozen at the last `extract-knowledge-seed.py` run. Until now that drift was **invisible** — a
//! greenfield deploy would silently bootstrap stale prompts/skills, and nobody would know the
//! shipped binary no longer matches the curated catalog.
//!
//! ## The single refresh path (the decision this module commits to)
//!
//! The **live `skills.*` log is the runtime source of truth**; the embedded seed is ONLY the
//! greenfield bootstrap (the boot path seeds it solely when the workspace catalog
//! [`is_empty`](SkillCatalog::is_empty) — see `rest_modules.rs`). A running role already reads the
//! live catalog (prompt/model/scopes/skills), so **updating a prompt never needs a re-release** —
//! an operator edits the live log and the next launch picks it up. We do NOT load the embedded seed
//! dynamically (it is a frozen bootstrap, not a live source) and we do NOT regenerate it on every
//! build. Instead we keep the seed embedded and **guard it**: this module reports drift on demand
//! (the REST `GET /api/v1/skills/drift` surface), a boot scan warns + rings the operator bell when a
//! populated deploy's seed has drifted, and a CI test fails if the embedded seed stops being
//! internally consistent. When the report shows drift, the fix is the documented
//! `scripts/extract-knowledge-seed.py` regeneration (see `docs/ops/greenfield-seeds.md` §4.1).
//!
//! Everything here is a **pure** comparison over two [`SkillCatalog`] snapshots — no I/O, no clock —
//! so it replays deterministically and the edge (REST handler, boot scan) owns delivery.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::presets::workspace_seed_events;
use crate::state::{SkillCatalog, SkillState};

/// Build the catalog the **embedded** greenfield seed (`seeds/knowledge.json`) encodes, by replaying
/// [`workspace_seed_events`] exactly as the boot path does into an empty workspace. `now_secs` is
/// stamped `0` (the seed carries no real wall-clock — its skills' `registered_at_secs` are a
/// bootstrap artifact, never compared as a drift field), so this is a deterministic, side-effect-free
/// snapshot of "what a clean deploy would come up with".
pub fn seed_catalog() -> SkillCatalog {
    let mut s = SkillState::default();
    for ev in workspace_seed_events(0) {
        s.apply(&ev);
    }
    s.catalog
}

/// Why a role or skill appears in the drift report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftStatus {
    /// In the embedded seed but absent from the live catalog — the seed would bootstrap a role/skill
    /// prod has since retired.
    MissingInLive,
    /// In the live catalog but absent from the embedded seed — prod added a role/skill a clean deploy
    /// would NOT come up with until the seed is regenerated.
    MissingInSeed,
    /// In both, but one or more attributes differ ([`RoleDrift::fields`] / [`SkillDrift::fields`]).
    Changed,
}

impl DriftStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DriftStatus::MissingInLive => "missing_in_live",
            DriftStatus::MissingInSeed => "missing_in_seed",
            DriftStatus::Changed => "changed",
        }
    }
}

/// Per-role divergence between the embedded seed and the live catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDrift {
    pub role: String,
    pub status: DriftStatus,
    /// The attributes that differ — a subset of `prompt`, `model`, `scopes`, `skills`. Empty for a
    /// pure presence drift ([`DriftStatus::MissingInLive`] / [`DriftStatus::MissingInSeed`]).
    pub fields: Vec<String>,
    /// Best-effort "since when": the newest `registered_at_secs` among the LIVE skills this role
    /// enables — a proxy for how recently the live catalog moved under this role (the catalog
    /// snapshot does not timestamp prompt/model/scope edits on their own). `0` when the role has no
    /// live skills.
    pub live_skill_age_secs: u64,
}

/// Per-skill divergence between the embedded seed and the live catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillDrift {
    pub skill: String,
    pub status: DriftStatus,
    /// The attributes that differ — a subset of `body`, `scopes`, `group`, `label`, `description`.
    /// `registered_at_secs` is deliberately NOT compared (the seed's is a `0` bootstrap stamp, the
    /// live one is a real epoch — they always differ and say nothing about semantic drift).
    pub fields: Vec<String>,
    /// The LIVE skill's `registered_at_secs` — "since when" the live body/scopes have stood. `0` when
    /// the skill is [`DriftStatus::MissingInLive`] (no live row to date).
    pub live_registered_at_secs: u64,
}

/// The full embedded-seed-vs-live drift report — the structured answer to "what differs, and how
/// recently did the live catalog move".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftReport {
    pub roles: Vec<RoleDrift>,
    pub skills: Vec<SkillDrift>,
    /// The newest `registered_at_secs` anywhere in the LIVE catalog — the high-water mark the frozen
    /// embedded seed sits behind. `0` when the live catalog is empty.
    pub newest_live_secs: u64,
}

impl DriftReport {
    /// `true` when the embedded seed and the live catalog agree (no role/skill divergence).
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty() && self.skills.is_empty()
    }

    /// `true` when the drift is worth **paging an operator** about, as opposed to benign presence
    /// drift. A populated curated catalog legitimately carries skills the seed omits on purpose (the
    /// unbound design-library skills, `docs/ops/greenfield-seeds.md` §4.1), so a skill that is only
    /// `MissingInSeed` is NOT alert-worthy on its own. Significant drift is: a role whose functional
    /// Knowledge changed ([`DriftStatus::Changed`]) or that the live catalog dropped entirely
    /// ([`DriftStatus::MissingInLive`] — the seed would bootstrap a role prod retired), or a bound
    /// skill whose body/scopes changed ([`DriftStatus::Changed`]). The boot alert gates on this so a
    /// restart does not ring the bell for the intentional unbound-skill omission; the REST `/drift`
    /// report still surfaces EVERYTHING for inspection.
    pub fn is_significant(&self) -> bool {
        self.roles
            .iter()
            .any(|r| matches!(r.status, DriftStatus::Changed | DriftStatus::MissingInLive))
            || self.skills.iter().any(|s| s.status == DriftStatus::Changed)
    }

    /// The roles whose `prompt`/`model`/`scopes`/`skills` the live catalog has moved away from the
    /// shipped seed (status [`DriftStatus::Changed`]) — the set worth paging an operator about,
    /// because a greenfield reseed would bring these up stale.
    pub fn changed_roles(&self) -> Vec<&str> {
        self.roles
            .iter()
            .filter(|r| r.status == DriftStatus::Changed)
            .map(|r| r.role.as_str())
            .collect()
    }

    /// A one-line operator-facing summary ("3 role(s), 2 skill(s) drift; roles changed: mayor,
    /// polecat"). Empty-safe: `"no drift"` when the seed matches the live catalog.
    pub fn summary(&self) -> String {
        if self.is_empty() {
            return "no drift: embedded seed matches the live skills catalog".to_string();
        }
        let changed = self.changed_roles();
        let mut s = format!(
            "{} role(s), {} skill(s) drift between the embedded seed and the live catalog",
            self.roles.len(),
            self.skills.len()
        );
        if !changed.is_empty() {
            s.push_str(&format!("; roles changed: {}", changed.join(", ")));
        }
        s
    }
}

/// Compare the embedded `seed` catalog against the `live` catalog and report every divergence,
/// per role and per skill. Pure + deterministic: ids are walked in sorted order (a [`BTreeSet`]
/// union of both sides), so the report is stable across runs.
///
/// - **Skills**: id present only in `seed` ⇒ [`DriftStatus::MissingInLive`]; only in `live` ⇒
///   [`DriftStatus::MissingInSeed`]; in both with a differing `body`/`default_scopes`/`group`/
///   `label`/`description` ⇒ [`DriftStatus::Changed`] (with the differing field names).
/// - **Roles**: same presence logic over the bindings, and for a role in both we compare its
///   `prompt`, `model_config`, `scopes`, and `enabled_skills`.
pub fn compute_drift(seed: &SkillCatalog, live: &SkillCatalog) -> DriftReport {
    let mut report = DriftReport::default();

    // High-water mark of the live catalog (newest skill registration).
    report.newest_live_secs = live
        .skills()
        .iter()
        .map(|s| s.registered_at_secs)
        .max()
        .unwrap_or(0);

    // ---- skills ------------------------------------------------------------
    let skill_ids: BTreeSet<String> = seed
        .skills()
        .into_iter()
        .map(|s| s.id)
        .chain(live.skills().into_iter().map(|s| s.id))
        .collect();
    for id in &skill_ids {
        match (seed.get(id), live.get(id)) {
            (Some(_), None) => report.skills.push(SkillDrift {
                skill: id.clone(),
                status: DriftStatus::MissingInLive,
                fields: Vec::new(),
                live_registered_at_secs: 0,
            }),
            (None, Some(l)) => report.skills.push(SkillDrift {
                skill: id.clone(),
                status: DriftStatus::MissingInSeed,
                fields: Vec::new(),
                live_registered_at_secs: l.registered_at_secs,
            }),
            (Some(s), Some(l)) => {
                let mut fields = Vec::new();
                if s.body != l.body {
                    fields.push("body".to_string());
                }
                if s.default_scopes != l.default_scopes {
                    fields.push("scopes".to_string());
                }
                if s.group != l.group {
                    fields.push("group".to_string());
                }
                if s.label != l.label {
                    fields.push("label".to_string());
                }
                if s.description != l.description {
                    fields.push("description".to_string());
                }
                if !fields.is_empty() {
                    report.skills.push(SkillDrift {
                        skill: id.clone(),
                        status: DriftStatus::Changed,
                        fields,
                        live_registered_at_secs: l.registered_at_secs,
                    });
                }
            }
            (None, None) => unreachable!("id came from the union of both catalogs"),
        }
    }

    // ---- roles -------------------------------------------------------------
    let seed_roles: BTreeSet<String> = seed.bindings().into_iter().map(|b| b.role).collect();
    let live_roles: BTreeSet<String> = live.bindings().into_iter().map(|b| b.role).collect();
    let role_names: BTreeSet<String> = seed_roles.union(&live_roles).cloned().collect();
    for role in &role_names {
        let in_seed = seed_roles.contains(role);
        let in_live = live_roles.contains(role);
        // The newest live-skill registration this role enables — the "since when" proxy.
        let live_skill_age_secs = live
            .skills_for_role(role)
            .iter()
            .filter_map(|sk| live.get(sk))
            .map(|sk| sk.registered_at_secs)
            .max()
            .unwrap_or(0);
        match (in_seed, in_live) {
            (true, false) => report.roles.push(RoleDrift {
                role: role.clone(),
                status: DriftStatus::MissingInLive,
                fields: Vec::new(),
                live_skill_age_secs: 0,
            }),
            (false, true) => report.roles.push(RoleDrift {
                role: role.clone(),
                status: DriftStatus::MissingInSeed,
                fields: Vec::new(),
                live_skill_age_secs,
            }),
            (true, true) => {
                let mut fields = Vec::new();
                if seed.role_prompt(role) != live.role_prompt(role) {
                    fields.push("prompt".to_string());
                }
                if seed.role_model(role) != live.role_model(role) {
                    fields.push("model".to_string());
                }
                if seed.role_scopes(role) != live.role_scopes(role) {
                    fields.push("scopes".to_string());
                }
                if seed.skills_for_role(role) != live.skills_for_role(role) {
                    fields.push("skills".to_string());
                }
                if !fields.is_empty() {
                    report.roles.push(RoleDrift {
                        role: role.clone(),
                        status: DriftStatus::Changed,
                        fields,
                        live_skill_age_secs,
                    });
                }
            }
            (false, false) => unreachable!("role came from the union of both catalogs"),
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::SkillEvent;

    /// CI guard against silent drift (AC4): the embedded seed must replay to a catalog that has ZERO
    /// drift against itself — i.e. `seed_catalog()` is internally consistent and `compute_drift` is
    /// reflexive. A malformed `seeds/knowledge.json` (or a `workspace_seed_events` regression) trips
    /// this in CI rather than shipping a broken bootstrap.
    #[test]
    fn embedded_seed_has_no_self_drift() {
        let seed = seed_catalog();
        let report = compute_drift(&seed, &seed);
        assert!(
            report.is_empty(),
            "embedded seed drifts against itself: {}",
            report.summary()
        );
        assert_eq!(report.summary(), "no drift: embedded seed matches the live skills catalog");
    }

    /// CI guard (AC4): the embedded seed's role + skill ROSTER must stay stable. If an edit to
    /// `seeds/knowledge.json` drops a role, blanks a prompt, or empties a role's skills, this fails —
    /// the seed can never silently rot into an empty/partial bootstrap.
    #[test]
    fn embedded_seed_roster_is_complete() {
        let seed = seed_catalog();
        for role in [
            "mayor", "sheriff", "overseer", "refinery", "polecat", "dog", "witness", "deacon",
        ] {
            let prompt = seed.role_prompt(role);
            assert!(
                prompt.as_deref().map(|p| !p.trim().is_empty()).unwrap_or(false),
                "seed role `{role}` must carry a non-empty prompt"
            );
            assert!(
                !seed.skills_for_role(role).is_empty(),
                "seed role `{role}` must enable >=1 skill"
            );
        }
        // The role-functional skill subset is the 14 bound skills (docs/ops/greenfield-seeds.md §4.1).
        assert_eq!(seed.skills().len(), 14, "seed must carry the 14 role-bound skills");
    }

    fn live_from_seed_with<F: FnOnce(&mut SkillState)>(mutate: F) -> SkillCatalog {
        let mut s = SkillState::default();
        for ev in workspace_seed_events(0) {
            s.apply(&ev);
        }
        mutate(&mut s);
        s.catalog
    }

    #[test]
    fn detects_a_changed_role_prompt() {
        let seed = seed_catalog();
        let live = live_from_seed_with(|s| {
            s.apply(&SkillEvent::RolePromptSet {
                role: "polecat".into(),
                prompt: "a DIFFERENT prompt curated live".into(),
                now_secs: 9_000,
            });
        });
        let report = compute_drift(&seed, &live);
        assert!(!report.is_empty());
        let polecat = report
            .roles
            .iter()
            .find(|r| r.role == "polecat")
            .expect("polecat drift reported");
        assert_eq!(polecat.status, DriftStatus::Changed);
        assert!(polecat.fields.contains(&"prompt".to_string()));
        assert_eq!(report.changed_roles(), vec!["polecat"]);
    }

    #[test]
    fn detects_a_changed_skill_body_with_live_age() {
        let seed = seed_catalog();
        // Re-register an existing seed skill with a new body + a real epoch.
        let existing = seed.skills()[0].clone();
        let live = live_from_seed_with(|s| {
            s.apply(&SkillEvent::Registered {
                skill: existing.id.clone(),
                label: existing.label.clone(),
                description: existing.description.clone(),
                default_scopes: existing.default_scopes.clone(),
                body: format!("{}\n\nEXTRA live edit", existing.body),
                group: existing.group.clone(),
                now_secs: 1_700_000_000,
            });
        });
        let report = compute_drift(&seed, &live);
        let drift = report
            .skills
            .iter()
            .find(|d| d.skill == existing.id)
            .expect("skill body drift reported");
        assert_eq!(drift.status, DriftStatus::Changed);
        assert!(drift.fields.contains(&"body".to_string()));
        assert_eq!(drift.live_registered_at_secs, 1_700_000_000);
        assert_eq!(report.newest_live_secs, 1_700_000_000);
    }

    #[test]
    fn benign_unbound_skill_addition_is_not_significant() {
        // A populated catalog with an EXTRA skill bound to no role (the design-library case the seed
        // omits on purpose) is drift in the full report, but not worth paging an operator.
        let seed = seed_catalog();
        let live = live_from_seed_with(|s| {
            s.apply(&SkillEvent::Registered {
                skill: "design-taste-frontend".into(),
                label: "Design taste".into(),
                description: String::new(),
                default_scopes: vec![],
                body: "unbound design skill".into(),
                group: "design".into(),
                now_secs: 30,
            });
        });
        let report = compute_drift(&seed, &live);
        assert!(!report.is_empty(), "the extra skill IS reported");
        assert!(
            !report.is_significant(),
            "but an unbound MissingInSeed skill alone is not alert-worthy"
        );
    }

    #[test]
    fn a_changed_role_prompt_is_significant() {
        let seed = seed_catalog();
        let live = live_from_seed_with(|s| {
            s.apply(&SkillEvent::RolePromptSet {
                role: "mayor".into(),
                prompt: "curated-live mayor prompt that moved on".into(),
                now_secs: 9_000,
            });
        });
        let report = compute_drift(&seed, &live);
        assert!(report.is_significant(), "a changed role prompt must page");
    }

    #[test]
    fn detects_presence_drift_both_directions() {
        let seed = seed_catalog();
        // Live: retire a seed skill (MissingInLive) and register a brand-new one (MissingInSeed).
        let retired = seed.skills()[0].id.clone();
        let live = live_from_seed_with(|s| {
            s.apply(&SkillEvent::Retired {
                skill: retired.clone(),
                now_secs: 10,
            });
            s.apply(&SkillEvent::Registered {
                skill: "brand-new-live-skill".into(),
                label: "New".into(),
                description: String::new(),
                default_scopes: vec!["feed.read".into()],
                body: "live only".into(),
                group: String::new(),
                now_secs: 20,
            });
        });
        let report = compute_drift(&seed, &live);
        let gone = report.skills.iter().find(|d| d.skill == retired).unwrap();
        assert_eq!(gone.status, DriftStatus::MissingInLive);
        let added = report
            .skills
            .iter()
            .find(|d| d.skill == "brand-new-live-skill")
            .unwrap();
        assert_eq!(added.status, DriftStatus::MissingInSeed);
    }
}
