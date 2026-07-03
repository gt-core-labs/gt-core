//! Canonical least-privilege skill catalog for the autonomous agent roles
//! (`hq-agent-provisioning.4`).
//!
//! gt-skills' catalog is normally operator-driven (REST/MCP `skills.*`), persisted in the
//! [`SkillsRepository`](crate::SkillsRepository). But the orchestration daemon is a separate
//! process with no access to that store and no skills in the per-workspace event log, so it builds
//! its agent scope policy from THIS code preset: deterministic, versioned, reviewed in a PR — it
//! can never drift to `*`. The per-agent token minter (`hq-agent-provisioning.3`) resolves a slung
//! polecat/dog's scopes via [`SkillCatalog::scopes_for_roles`] against this catalog.
//!
//! Least-privilege per role. Caveat: RBAC scopes are `resource.verb`, not row-level, so a polecat's
//! `issues.write` authorizes the issues namespace, not literally "only its own bead" — bounding a
//! polecat to a single bead is a server-side claim check, not expressible in the JWT.

use serde::Deserialize;

#[cfg(feature = "axum")]
use gt_events::AppError;

#[cfg(feature = "axum")]
use crate::http::{SkillWriter, WorkspaceSkills};
use crate::{SkillCatalog, SkillEvent, SkillState};

/// The curated interactive-role Knowledge, extracted from a live workspace's `skills.*` log and
/// versioned here (`hq-greenfield-seeds.2`). On a greenfield deploy there is no prior state, so this
/// is what makes the seeded roles *functional* — real `SKILL.md` bodies, per-role system prompts and
/// model configs — instead of empty scope-carriers. See `docs/ops/greenfield-seeds.md` for how to
/// regenerate it from a running deploy (replay the workspace's `skills.*` events, role-functional
/// subset = skills bound to ≥1 role). Embedded as bytes so the seed travels with the binary
/// (orchestrator-agnostic — no external file under k8s).
const KNOWLEDGE_SEED_JSON: &str = include_str!("../seeds/knowledge.json");

/// A skill in the versioned seed: the catalog row plus its `SKILL.md` body and scope grant.
#[derive(Deserialize)]
struct SeedSkill {
    id: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    default_scopes: Vec<String>,
    #[serde(default)]
    body: String,
}

/// A role in the versioned seed: its system prompt, model config, and enabled-skill bindings. Scopes
/// are intentionally absent — they derive from the enabled skills' `default_scopes` via
/// [`SkillCatalog::role_scopes_migration`] at seed time, exactly as the live deploy resolved them.
#[derive(Deserialize)]
struct SeedRole {
    role: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    permission_mode: String,
    #[serde(default)]
    effort: String,
    #[serde(default)]
    enabled: Vec<String>,
}

#[derive(Deserialize)]
struct KnowledgeSeed {
    skills: Vec<SeedSkill>,
    roles: Vec<SeedRole>,
}

fn register(s: &mut SkillState, id: &str, scopes: &[&str]) {
    s.apply(&SkillEvent::Registered {
        skill: id.to_string(),
        label: id.to_string(),
        description: String::new(),
        default_scopes: scopes.iter().map(|x| x.to_string()).collect(),
        body: String::new(),
        group: String::new(),
        now_secs: 0,
    });
}

fn enable(s: &mut SkillState, role: &str, skill: &str) {
    s.apply(&SkillEvent::EnabledForRole {
        role: role.to_string(),
        skill: skill.to_string(),
        now_secs: 0,
    });
}

/// Build the canonical least-privilege catalog binding each automatic agent role to the minimal
/// scope set its work needs. No role is granted `*`; an unbound role (e.g. `overseer`) gets nothing.
///
/// - `polecat` → `issues.read`, `issues.write`, `merge.write`, `memory.read`, `memory.write`,
///   `a2a.read`, `a2a.write`, `notify.write` (work + claim + transition + submit merge + durable
///   memory + P2P + escalate to the operator — `gtcore-44fab7`).
/// - `mayor` → `issues.read`, `issues.write`, `agent.read`, `agent.write`, `merge.read`,
///   `merge.write`, `dispatch.read`, `dispatch.write`, `notify.write` (coordinate + the delegation
///   edge `dispatch.request`/`dispatch.probe` its prompt drives — gtcore-c54d88 — plus escalation
///   when a delegation fails).
/// - `sheriff` → `merge.read`, `merge.write`, `notify.write`, `memory.read`, `memory.write`
///   (drive the merge board, escalate to the operator, recall/persist durable memory).
/// - `refinery` → `merge.write` (submit MERGE_READY).
/// - `witness` → `issues.read`, `comments.write`, `notify.write`, `memory.read`, `memory.write`
///   (read a closed bead's AC, flag a bad closure with a comment + operator alert, durable memory).
/// - `deacon` → `issues.read`, `board.read`, `merge.read`, `notify.write`, `memory.read`,
///   `memory.write` (read-only flow scan across tracker/board/merge, escalate, durable memory).
///
/// The trigger-driven role agents (`gtcore-999795`) run sheriff/witness/deacon as *agents with
/// criterion*, and their kickoff prompts (`crate`-external, `gt_composition::role_agent`) tell each
/// to recall/persist memory, escalate via `notify.send`, and — for the witness — flag a bad closure
/// via `comments.create`. Those grants are enumerated GRANULARLY here (the security layer), never as
/// a wildcard: a missing scope would otherwise make the feature ship inert (the same class of bug
/// `gtcore-abf278` fixed for the polecat). NOTE the operator-notification grant is `notify.write`
/// (→ the `notify.*` MCP namespace `notify.send` lives in), NOT `notifications.write` (that folds to
/// the `notifications.*` read/dismiss namespace and would NOT authorize `notify.send`).
///
/// `memory.*` and `a2a.*` close two grants that shipped inert (`gtcore-abf278`): a slung polecat's
/// prompt tells it to recall/persist durable memory (`memory.recall`/`memory.save`, `gtcore-bad8d9`)
/// and to coordinate peer-to-peer over A2A (`a2a.inbox`/`a2a.delegate`/…, `gtcore-3a3557`), but the
/// daemon-minted token granted only `issues.*`+`merge.*`, so every such call returned `unauthorized`.
/// The MCP grant is per-namespace (any granular `<resource>.<verb>` folds to `<resource>.*` in
/// [`Scope::from_workspace_claim`](gt_rbac::Scope::from_workspace_claim)), and the a2a tool verbs
/// (`inbox`/`send`/`delegate`/…) are NOT in the closed `SCOPE_VERBS` vocabulary — so the grant rides
/// the known `read`/`write` verbs (`a2a.read`+`a2a.write` → the whole `a2a.*` namespace), exactly as
/// `issues.read`+`issues.write` authorizes `issues.*`. Still least-privilege: no role gets `*`.
pub fn agent_least_privilege_catalog() -> SkillCatalog {
    let mut s = SkillState::default();
    register(&mut s, "bead-work", &["issues.read", "issues.write"]);
    register(&mut s, "bead-coordinate", &["issues.read", "issues.write", "agent.read", "agent.write", "merge.read", "merge.write"]);
    register(&mut s, "merge-ops", &["merge.read", "merge.write"]);
    register(&mut s, "merge-submit", &["merge.write"]);
    register(&mut s, "observe", &["issues.read"]);
    // gtcore-abf278: durable memory (gtcore-bad8d9) and peer-to-peer A2A (gtcore-3a3557) were
    // mergeable features left inert because the polecat token never carried their scopes.
    register(&mut s, "memory-access", &["memory.read", "memory.write"]);
    register(&mut s, "a2a-coordinate", &["a2a.read", "a2a.write"]);
    // gtcore-999795: scopes the trigger-driven role agents' kickoffs actually exercise. `notify.write`
    // (NOT `notifications.write`) is what authorizes the `notify.send` MCP tool — `notify.send` lives
    // in the `notify.*` namespace, while `notifications.*` is the members' read/dismiss namespace.
    register(&mut s, "operator-notify", &["notify.write"]);
    // gtcore-c54d88: the mayor's delegation edge. `dispatch.request` (gtcore-d24661) is what
    // materializes a delegated bead; without this grant MAYOR mode ships inert — the first real
    // delegation (2026-07-02) returned unauthorized on every attempt. The tool verbs
    // (`request`/`probe`) are not in the closed SCOPE_VERBS vocabulary, so the grant rides
    // `dispatch.read`+`dispatch.write` folding to the whole `dispatch.*` namespace, exactly like
    // the a2a grant above.
    register(&mut s, "dispatch-request", &["dispatch.read", "dispatch.write"]);
    register(&mut s, "comments-write", &["comments.write"]);
    // The deacon's read-only flow scan reads the board (`board.list`) and merge board (`merge.list`)
    // on top of the tracker (`issues.read`, via `observe`).
    register(&mut s, "flow-read", &["board.read", "merge.read"]);

    enable(&mut s, "polecat", "bead-work");
    enable(&mut s, "polecat", "merge-submit");
    enable(&mut s, "polecat", "memory-access");
    enable(&mut s, "polecat", "a2a-coordinate");
    // gtcore-44fab7: the polecat is the role IN THE FIELD, yet it was the only one without a channel
    // to the operator — its `notify.send` calls returned `unauthorized` and the alert was lost (only
    // the audit trail kept it, which nobody reads in the heat of the moment). `operator-notify` grants
    // `notify.write`, which folds to the `notify.*` MCP namespace `notify.send` lives in — same
    // mechanism the sheriff/witness/deacon already use. The members' read/dismiss surface is the
    // SEPARATE `notifications.*` namespace, so this is emit-only: the polecat can escalate but not
    // list/ack/administer notifications.
    enable(&mut s, "polecat", "operator-notify");
    enable(&mut s, "mayor", "bead-coordinate");
    // gtcore-c54d88: the mayor delegates via dispatch.request and its prompt tells it to report a
    // failed delegation via notify.send — both were unauthorized, leaving MAYOR mode unable to
    // move the frontier (same inert-grant class as gtcore-abf278 / gtcore-44fab7).
    enable(&mut s, "mayor", "dispatch-request");
    enable(&mut s, "mayor", "operator-notify");
    // sheriff (gtcore-999795): drive the merge board + escalate + durable memory.
    enable(&mut s, "sheriff", "merge-ops");
    enable(&mut s, "sheriff", "operator-notify");
    enable(&mut s, "sheriff", "memory-access");
    enable(&mut s, "refinery", "merge-submit");
    // witness (gtcore-999795): read AC + flag a bad closure (comment + operator alert) + memory.
    enable(&mut s, "witness", "observe");
    enable(&mut s, "witness", "comments-write");
    enable(&mut s, "witness", "operator-notify");
    enable(&mut s, "witness", "memory-access");
    // deacon (gtcore-999795): read-only flow scan + escalate + memory.
    enable(&mut s, "deacon", "observe");
    enable(&mut s, "deacon", "flow-read");
    enable(&mut s, "deacon", "operator-notify");
    enable(&mut s, "deacon", "memory-access");

    // hq-role-scopes: a role's scopes are now read from `RoleBinding::scopes`, not derived from its
    // skills. Seed each role's scopes from its enabled-skill `default_scopes` via the one-shot
    // migration so the daemon's per-agent token minter (`scopes_for_roles`) resolves the same
    // least-privilege set it always did.
    for ev in s.catalog.role_scopes_migration(0) {
        s.apply(&ev);
    }

    s.catalog
}

/// The canonical INTERACTIVE-session role Knowledge, as `skills.*` events to seed a fresh workspace's
/// log at boot (`hq-role-mcp`, `hq-greenfield-seeds.2`). Unlike [`agent_least_privilege_catalog`]
/// (the daemon's in-memory polecat policy), this is event-sourced and operator-overridable via the
/// Knowledge REST surface — it just guarantees a clean deploy on a new machine comes up with
/// *functional* roles out of the box instead of empty tokens and blank prompts.
///
/// The content is the versioned extract in [`KNOWLEDGE_SEED_JSON`] (`seeds/knowledge.json`), pulled
/// from a live deploy — NOT invented here. Each role gets:
/// - its real `SKILL.md`-bearing skills ([`SkillEvent::Registered`] carries `body` + `default_scopes`),
/// - its enabled-skill bindings ([`SkillEvent::EnabledForRole`]),
/// - its system prompt ([`SkillEvent::RolePromptSet`] — the terminal writes it as `<workdir>/CLAUDE.md`),
/// - its model config ([`SkillEvent::RoleModelSet`] — model + permission-mode + effort the launch stamps).
///
/// Role scopes are NOT stored per-role: they derive from the enabled skills' `default_scopes` via
/// [`SkillCatalog::role_scopes_migration`], exactly as the live deploy resolved them (e.g. mayor's
/// `notify-ops` → `notifications.write`, `tracker-ops` → `workspace.member` + `graph.read`).
/// `workspace.member` expands to the operational namespace on MCP (`Scope::from_workspace_claim`) but
/// never to `workspace.create`, so no seeded role is an admin.
///
/// Seeded ONLY when the workspace catalog is empty, so it never clobbers a human-curated catalog
/// (the caller in `rest_modules.rs` gates on `catalog.is_empty()`).
pub fn workspace_seed_events(now: u64) -> Vec<SkillEvent> {
    let seed: KnowledgeSeed = serde_json::from_str(KNOWLEDGE_SEED_JSON)
        .expect("seeds/knowledge.json is a versioned, build-time-checked artifact");

    let mut events = Vec::new();
    for sk in &seed.skills {
        events.push(SkillEvent::Registered {
            skill: sk.id.clone(),
            label: sk.label.clone(),
            description: sk.description.clone(),
            default_scopes: sk.default_scopes.clone(),
            body: sk.body.clone(),
            group: sk.group.clone(),
            now_secs: now,
        });
    }
    for r in &seed.roles {
        for skill in &r.enabled {
            events.push(SkillEvent::EnabledForRole {
                role: r.role.clone(),
                skill: skill.clone(),
                now_secs: now,
            });
        }
        if !r.prompt.is_empty() {
            events.push(SkillEvent::RolePromptSet {
                role: r.role.clone(),
                prompt: r.prompt.clone(),
                now_secs: now,
            });
        }
        if !(r.model.is_empty() && r.permission_mode.is_empty() && r.effort.is_empty()) {
            events.push(SkillEvent::RoleModelSet {
                role: r.role.clone(),
                model: r.model.clone(),
                permission_mode: r.permission_mode.clone(),
                effort: r.effort.clone(),
                now_secs: now,
            });
        }
    }
    // hq-role-scopes: scopes are read from `RoleBinding::scopes`, not derived from skills on the live
    // read path — so a fresh workspace must also seed each role's scopes, else a seeded role would
    // have skills but an empty grant. Replay the register/enable events and append the one-shot
    // migration's `RoleScopesSet` per role (the union of its enabled skills' `default_scopes`), so the
    // seeded catalog resolves the same grants the live deploy did.
    let mut s = SkillState::default();
    for ev in &events {
        s.apply(ev);
    }
    events.extend(s.catalog.role_scopes_migration(now));
    events
}

/// Seed `workspace`'s `skills.*` stream with the canonical role catalog when — and only when — its
/// catalog is empty (gtcore-03baf7). Returns the number of events appended (`0` = the catalog was
/// already populated and was left untouched).
///
/// This is the one guard the boot path (`rest_modules.rs`) applies before seeding the boot
/// workspace, factored out so tenant provisioning (`workspace.create` / `POST /api/v1/workspace`)
/// can give a NEW workspace the same functional roles at creation. Every role launch replays
/// `skills.*` scoped to the session's workspace, so without this a fresh tenant's roles come up
/// with no skills, no Knowledge, no model and no permissions. Idempotent by the emptiness guard:
/// re-provisioning an existing tenant (or racing a curated catalog) never appends a second seed.
#[cfg(feature = "axum")]
pub async fn seed_workspace_if_empty<S>(skills: &S, workspace: &str, now: u64) -> Result<usize, AppError>
where
    S: WorkspaceSkills + SkillWriter + ?Sized,
{
    let catalog = skills.catalog(workspace).await?;
    if !catalog.is_empty() {
        return Ok(0);
    }
    let events = workspace_seed_events(now);
    let total = events.len();
    for ev in events {
        skills.append(workspace, ev).await?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scopes(c: &SkillCatalog, role: &str) -> Vec<String> {
        c.scopes_for_roles(&[role.to_string()])
    }

    #[test]
    fn each_role_gets_its_minimal_scopes_and_never_the_wildcard() {
        let c = agent_least_privilege_catalog();
        // gtcore-abf278: the polecat grant carries memory.* and a2a.* so the durable-memory
        // (gtcore-bad8d9) and P2P-delegation (gtcore-3a3557) features its prompt relies on are
        // actually authorized — not just issues.*+merge.*. gtcore-44fab7 adds notify.write so the
        // field role can escalate to the operator. Order is the legacy union's: enabled skills walked
        // alphabetically by id (a2a-coordinate, bead-work, memory-access, merge-submit, operator-notify),
        // each contributing its `default_scopes` in registered order.
        assert_eq!(
            scopes(&c, "polecat"),
            vec!["a2a.read", "a2a.write", "issues.read", "issues.write", "memory.read", "memory.write", "merge.write", "notify.write"]
        );
        // mayor: bead-coordinate, dispatch-request (gtcore-c54d88), operator-notify.
        assert_eq!(
            scopes(&c, "mayor"),
            vec!["issues.read", "issues.write", "agent.read", "agent.write", "merge.read", "merge.write", "dispatch.read", "dispatch.write", "notify.write"]
        );
        assert_eq!(scopes(&c, "refinery"), vec!["merge.write"]);
        // gtcore-999795: sheriff/witness/deacon carry the scopes their role-agent kickoffs exercise,
        // enumerated granularly. Order is the legacy union's: enabled skills walked alphabetically by
        // id, each contributing its `default_scopes` in registered order, dedup'd first-seen.
        // sheriff: memory-access, merge-ops, operator-notify.
        assert_eq!(
            scopes(&c, "sheriff"),
            vec!["memory.read", "memory.write", "merge.read", "merge.write", "notify.write"]
        );
        // witness: comments-write, memory-access, observe, operator-notify.
        assert_eq!(
            scopes(&c, "witness"),
            vec!["comments.write", "memory.read", "memory.write", "issues.read", "notify.write"]
        );
        // deacon: flow-read, memory-access, observe, operator-notify.
        assert_eq!(
            scopes(&c, "deacon"),
            vec!["board.read", "merge.read", "memory.read", "memory.write", "issues.read", "notify.write"]
        );
        // No automatic role is ever the operator wildcard.
        for role in [
            "polecat", "mayor", "sheriff", "refinery", "witness", "deacon", "overseer",
        ] {
            assert!(
                !scopes(&c, role).iter().any(|s| s == "*"),
                "{role} must not carry '*'"
            );
        }
    }

    /// gtcore-abf278 (AC): the polecat's daemon-minted grant, expanded the way the MCP boundary
    /// expands a JWT claim ([`gt_rbac::Scope::from_workspace_claim`]), must AUTHORIZE the memory and
    /// a2a tools its prompt drives — `memory.recall`/`memory.save` (gtcore-bad8d9) and
    /// `a2a.inbox`/`a2a.delegate`/… (gtcore-3a3557) — while still DENYING a namespace it was never
    /// granted, so the fix is a grant widening, not an enforcement weakening.
    #[test]
    fn polecat_grant_reaches_memory_and_a2a_but_not_ungranted_namespaces() {
        let c = agent_least_privilege_catalog();
        let scope = gt_rbac::Scope::from_workspace_claim("polecat", &scopes(&c, "polecat"));

        // The two formerly-inert capabilities are now authorized end-to-end.
        scope.check("memory.recall").expect("memory.recall authorized");
        scope.check("memory.save.execute").expect("memory.save authorized");
        scope.check("a2a.inbox").expect("a2a.inbox authorized");
        scope.check("a2a.discover").expect("a2a.discover authorized");
        scope.check("a2a.delegate.execute").expect("a2a.delegate authorized");
        scope.check("a2a.send.execute").expect("a2a.send authorized");

        // The original grant still works.
        scope.check("issues.read.execute").expect("issues still authorized");
        scope.check("merge.submit.execute").expect("merge still authorized");

        // gtcore-44fab7: the polecat can now escalate to the operator (notify.send authorized).
        scope.check("notify.send.execute").expect("notify.send authorized");

        // Enforcement is NOT weakened: a namespace the polecat was never granted stays denied.
        assert!(scope.check("workspace.create").is_err(), "no workspace admin");
        assert!(scope.check("agent.add.execute").is_err(), "no agent dispatch");
        assert!(scope.check("quota.set.execute").is_err(), "no quota control");

        // Still least-privilege: never the wildcard.
        assert!(!scope.allow.iter().any(|s| s == "*"), "polecat must not carry '*'");
    }

    /// gtcore-c54d88 (AC): the mayor's daemon-minted grant must AUTHORIZE the delegation edge its
    /// prompt drives — `dispatch.request` (+ `dispatch.probe`) and the failure escalation
    /// `notify.send` — while ungranted namespaces stay denied. The first real MAYOR-mode wake
    /// (2026-07-02) hit unauthorized on all three and the frontier could not move.
    #[test]
    fn mayor_grant_reaches_the_delegation_edge() {
        let c = agent_least_privilege_catalog();
        let scope = gt_rbac::Scope::from_workspace_claim("mayor", &scopes(&c, "mayor"));

        scope.check("dispatch.request.execute").expect("dispatch.request authorized");
        scope.check("dispatch.probe.execute").expect("dispatch.probe authorized");
        scope.check("notify.send.execute").expect("notify.send authorized");
        scope.check("issues.read.execute").expect("tracker still authorized");

        // Enforcement not weakened: ungranted namespaces stay denied.
        assert!(scope.check("workspace.create").is_err(), "no workspace admin");
        assert!(scope.check("quota.set.execute").is_err(), "no quota control");
        assert!(!scope.allow.iter().any(|s| s == "*"), "mayor must not carry '*'");
    }

    /// gtcore-999795 (AC): the trigger-driven role agents' daemon-minted grants, expanded the way the
    /// MCP boundary expands a JWT claim ([`gt_rbac::Scope::from_workspace_claim`]), must AUTHORIZE the
    /// tools each role's kickoff drives — otherwise the feature ships inert. The crucial subtlety is
    /// the operator-notification grant: `notify.write` authorizes `notify.send`, whereas the wrong-
    /// namespace `notifications.write` would NOT (it folds to `notifications.*`).
    #[test]
    fn role_agent_grants_authorize_their_kickoff_tools() {
        let c = agent_least_privilege_catalog();
        let scope = |role: &str| gt_rbac::Scope::from_workspace_claim(role, &scopes(&c, role));

        // sheriff: drive the merge board, escalate, recall/persist memory.
        let sheriff = scope("sheriff");
        sheriff.check("merge.list.execute").expect("sheriff reads the board");
        sheriff.check("merge.submit.execute").expect("sheriff recovers a slot");
        sheriff.check("notify.send.execute").expect("sheriff escalates");
        sheriff.check("memory.recall").expect("sheriff recalls memory");
        sheriff.check("memory.save.execute").expect("sheriff persists memory");
        assert!(sheriff.check("issues.transition.execute").is_err(), "sheriff has no tracker write");

        // witness: read the closed bead, flag a bad closure with a comment + operator alert, memory.
        let witness = scope("witness");
        witness.check("issues.read.execute").expect("witness reads the bead");
        witness.check("comments.create.execute").expect("witness flags a bad closure");
        witness.check("notify.send.execute").expect("witness alerts the operator");
        witness.check("memory.recall").expect("witness recalls memory");
        assert!(witness.check("merge.submit.execute").is_err(), "witness cannot merge");

        // deacon: read-only flow scan across tracker/board/merge, escalate, memory.
        let deacon = scope("deacon");
        deacon.check("issues.list.execute").expect("deacon scans the tracker");
        deacon.check("board.list.execute").expect("deacon reads the board");
        deacon.check("merge.list.execute").expect("deacon reads the merge board");
        deacon.check("notify.send.execute").expect("deacon escalates");
        deacon.check("memory.recall").expect("deacon recalls memory");

        // Still least-privilege: never the wildcard.
        for role in ["sheriff", "witness", "deacon"] {
            assert!(!scope(role).allow.iter().any(|s| s == "*"), "{role} must not carry '*'");
        }
    }

    /// gtcore-44fab7 (AC): the role→scope matrix for the operator-notification channel. Every role
    /// bound to `operator-notify` (the field polecat + the escalating role-agents) must AUTHORIZE
    /// `notify.send`; every role without it stays DENIED — the grant is enumerated per-role, never a
    /// blanket. And the grant is EMIT-ONLY: it must NOT reach the members' read/dismiss surface, which
    /// is the SEPARATE `notifications.*` namespace (list/ack/dismiss), so a polecat can escalate but
    /// cannot list or administer notifications.
    #[test]
    fn notify_send_scope_matrix_across_roles() {
        let c = agent_least_privilege_catalog();
        let scope = |role: &str| gt_rbac::Scope::from_workspace_claim(role, &scopes(&c, role));

        // Bound to operator-notify → can escalate to the operator (mayor since gtcore-c54d88:
        // its prompt reports a failed delegation via notify.send).
        for role in ["polecat", "sheriff", "witness", "deacon", "mayor"] {
            scope(role)
                .check("notify.send.execute")
                .unwrap_or_else(|e| panic!("{role} must be able to notify.send: {e}"));
        }

        // Not bound to operator-notify → notify.send denied (per-role grant, not a blanket).
        for role in ["refinery", "overseer", "nobody"] {
            assert!(
                scope(role).check("notify.send.execute").is_err(),
                "{role} must NOT be able to notify.send"
            );
        }

        // Emit-only: the notify.write grant folds to `notify.*` (where notify.send lives), NOT to the
        // members' `notifications.*` read/dismiss namespace — so list/ack/dismiss stay out of reach.
        let polecat = scope("polecat");
        assert!(
            polecat.check("notifications.list.execute").is_err(),
            "polecat must not reach the notifications read/dismiss namespace"
        );
        assert!(
            polecat.check("notifications.dismiss.execute").is_err(),
            "polecat must not dismiss notifications"
        );
    }

    #[test]
    fn an_unbound_role_gets_nothing() {
        let c = agent_least_privilege_catalog();
        assert!(scopes(&c, "overseer").is_empty());
        assert!(scopes(&c, "nobody").is_empty());
    }

    /// In-memory two-trait store for [`seed_workspace_if_empty`]: a per-workspace event vec whose
    /// catalog is the same replay the composition root's `EventLogSkills` performs.
    #[cfg(feature = "axum")]
    struct MemStore(std::sync::Mutex<std::collections::HashMap<String, Vec<SkillEvent>>>);

    #[cfg(feature = "axum")]
    #[async_trait::async_trait]
    impl WorkspaceSkills for MemStore {
        async fn catalog(&self, workspace: &str) -> Result<SkillCatalog, AppError> {
            let mut s = SkillState::default();
            if let Some(evs) = self.0.lock().unwrap().get(workspace) {
                for ev in evs {
                    s.apply(ev);
                }
            }
            Ok(s.catalog)
        }
    }

    #[cfg(feature = "axum")]
    #[async_trait::async_trait]
    impl SkillWriter for MemStore {
        async fn append(&self, workspace: &str, event: SkillEvent) -> Result<(), AppError> {
            self.0
                .lock()
                .unwrap()
                .entry(workspace.to_string())
                .or_default()
                .push(event);
            Ok(())
        }
    }

    #[cfg(feature = "axum")]
    #[tokio::test]
    async fn seed_workspace_if_empty_seeds_once_and_never_twice() {
        // gtcore-03baf7: a fresh tenant's empty catalog receives the full seed (equal to the
        // embedded seed catalog), and a re-provision is a no-op — the emptiness guard is what makes
        // tenant provisioning idempotent and curated-catalog-safe.
        let store = MemStore(std::sync::Mutex::new(std::collections::HashMap::new()));

        let seeded = seed_workspace_if_empty(&store, "acme", 0).await.unwrap();
        assert_eq!(seeded, workspace_seed_events(0).len());
        let catalog = store.catalog("acme").await.unwrap();
        assert!(!catalog.is_empty());
        let drift = crate::compute_drift(&crate::seed_catalog(), &catalog);
        assert!(
            drift.is_empty(),
            "seeded tenant catalog must match the embedded seed: {}",
            drift.summary()
        );

        // Second provision: guard sees the populated catalog, appends nothing.
        let again = seed_workspace_if_empty(&store, "acme", 1).await.unwrap();
        assert_eq!(again, 0);
        assert_eq!(
            store.0.lock().unwrap().get("acme").unwrap().len(),
            seeded,
            "re-provision must not duplicate seed events"
        );

        // Other tenants are independent streams: seeding `acme` leaves `beta` empty.
        assert!(store.catalog("beta").await.unwrap().is_empty());
    }

    #[test]
    fn workspace_seed_replays_the_curated_knowledge_extract() {
        // Replaying the versioned seed (`seeds/knowledge.json`) yields a catalog matching the live
        // deploy it was extracted from: real skill bodies, per-role prompts + models, and scopes that
        // derive from the bound skills — a greenfield deploy comes up with FUNCTIONAL roles
        // (hq-greenfield-seeds.2), not empty scope-carriers.
        let mut s = SkillState::default();
        for ev in workspace_seed_events(0) {
            s.apply(&ev);
        }
        let c = s.catalog;
        let binding = |role: &str| {
            c.bindings()
                .into_iter()
                .find(|b| b.role == role)
                .unwrap_or_else(|| panic!("seed must bind role `{role}`"))
        };

        // Scopes derive from the enabled skills' `default_scopes`, exactly as the live deploy resolved
        // them: bead-intake/tracker-pm → issues.{read,write}, tracker-ops → agent/merge/graph/workspace,
        // notify-ops → notifications.write, crew-commit → workspace.member, memory-access → memory.{read,write}.
        assert_eq!(
            scopes(&c, "mayor"),
            vec!["issues.read", "issues.write", "notifications.write", "agent.read", "agent.write", "graph.read", "merge.read", "merge.write", "workspace.member"],
        );
        assert_eq!(
            scopes(&c, "polecat"),
            vec!["workspace.member", "memory.read", "memory.write"],
        );
        assert_eq!(scopes(&c, "dog"), vec!["issues.read", "issues.write", "workspace.member"]);
        assert_eq!(scopes(&c, "refinery"), vec!["workspace.member"]);

        // Every role carries a non-empty system prompt + model config — the functional payload a
        // greenfield deploy was missing — and a real skill body travels with the catalog.
        for role in [
            "mayor", "sheriff", "overseer", "refinery", "polecat", "dog", "witness", "deacon",
        ] {
            let b = binding(role);
            assert!(!b.prompt.trim().is_empty(), "{role} must have a prompt");
            assert!(
                !b.model_config.is_unset(),
                "{role} must have a model config"
            );
            assert!(
                !b.enabled_skills.is_empty(),
                "{role} must have enabled skills"
            );
            assert!(
                !scopes(&c, role).iter().any(|s| s == "*"),
                "{role} must not carry '*'"
            );
        }
        // The bodies are the point of the seed: a known role-bound skill carries its `SKILL.md`.
        let mayor = binding("mayor");
        assert!(mayor.enabled_skills.contains("tracker-ops"));
        let tracker_ops = c.get("tracker-ops").expect("tracker-ops registered");
        assert!(
            !tracker_ops.body.trim().is_empty(),
            "skill bodies must be seeded, not blank"
        );
    }
}
