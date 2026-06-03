use std::path::Path;

use gt_events::AppError;

use crate::record::EventRecord;

/// Read-side compatibility adapter (hq-mod-events.2, extended for `agent.*` in hq-mcp-dispatch.11).
///
/// The domain event kinds (`rig.*`, `quota.*`, `merge.*`, and `agent.*`) were backfilled with an
/// explicit `.v1` version suffix so the log carries versioned kinds (docs/03 non-negotiable).
/// Logs written *before* that backfill hold the bare kind (`"quota.rotated"`, `"agent.spawned"`);
/// those `events.jsonl` files are **never rewritten**. So on read we upgrade a known bare kind to
/// its current form, making legacy and current records indistinguishable to every downstream
/// consumer (replay folds by struct shape, but consumers that key on the kind string — feeds,
/// kind tallies — need the uniform form).
///
/// The set is closed on purpose: only kinds whose owning domain has since been versioned are
/// upgraded. Kinds still emitted bare by their domain (`quota.login_*`, `scheduling.*`, …)
/// pass through untouched — blindly appending `.v1` to every bare kind would corrupt them. A
/// kind that already carries any `.vN` suffix is not in this set, so it is left as-is.
const LEGACY_BARE_KINDS_V1: &[&str] = &[
    // gt-rig
    "rig.added",
    "rig.adopted",
    "rig.removed",
    "rig.prefix_changed",
    "rig.default_branch_changed",
    // gt-quota
    "quota.tokens_sampled",
    "quota.usage_probed",
    "quota.window_reset",
    "quota.block_predicted",
    "quota.account_limited",
    "quota.rotated",
    "quota.blocked",
    // gt-merge
    "merge.ready",
    "merge.started",
    "merge.merged",
    "merge.failed",
    // gt-agent (hq-mcp-dispatch.11; `agent.session_end` is a rename, see LEGACY_RENAMED_KINDS)
    "agent.spawned",
    "agent.heartbeat",
    "agent.killed",
    // gt-skills (hq-mod-events.9; enabled/disabled-for-role are kebab renames, see LEGACY_RENAMED_KINDS)
    "skills.registered",
    "skills.retired",
];

/// Legacy convoy kinds that were not just version-suffixed but **re-namespaced**
/// (`hq-mod-events.8`). events.2 left `gt-orchestration` on the `orch.*` family prefix; the
/// convoy domain was later aligned to the `convoy.*.v1` leaf namespace its siblings use. A
/// bare suffix-append cannot express that (the module segment + noun both change), so these
/// are a full `legacy -> forward` remap. The convoy kinds are joined by `agent.session_end`
/// (hq-mcp-dispatch.11): its sibling agent kinds version with a plain `.v1` append, but the
/// `session_end` leaf was also kebab-normalized to `session-end` when gt-agent aligned to the
/// declared `agent.session-end.v1` shape, so a bare suffix-append cannot express it.
const LEGACY_RENAMED_KINDS: &[(&str, &str)] = &[
    ("orch.convoy_created", "convoy.created.v1"),
    ("orch.convoy_launched", "convoy.launched.v1"),
    ("orch.member_dispatched", "convoy.member_dispatched.v1"),
    ("orch.member_completed", "convoy.member_completed.v1"),
    ("orch.member_failed", "convoy.member_failed.v1"),
    ("orch.convoy_closed", "convoy.closed.v1"),
    ("orch.convoy_failed", "convoy.failed.v1"),
    // gt-agent: snake -> kebab leaf rename (siblings use LEGACY_BARE_KINDS_V1).
    ("agent.session_end", "agent.session-end.v1"),
    // gt-skills: snake -> kebab leaf rename (hq-mod-events.9; registered/retired use LEGACY_BARE_KINDS_V1).
    ("skills.enabled_for_role", "skills.enabled-for-role.v1"),
    ("skills.disabled_for_role", "skills.disabled-for-role.v1"),
];

/// Upgrade a legacy kind to its current form in place: a bare `.v1` suffix for the events.2
/// set, or a full re-namespace for the convoy set (`hq-mod-events.8`). No-op for already-current
/// kinds and for kinds outside both sets.
fn upgrade_legacy_kind(rec: &mut EventRecord) {
    if LEGACY_BARE_KINDS_V1.contains(&rec.kind.as_str()) {
        rec.kind.push_str(".v1");
    } else if let Some((_, forward)) =
        LEGACY_RENAMED_KINDS.iter().find(|(legacy, _)| *legacy == rec.kind)
    {
        rec.kind = (*forward).to_string();
    }
}

/// Lee todos los records del log en orden. Una línea jsonl vacía se ignora.
pub fn read_all(path: &Path) -> Result<Vec<EventRecord>, AppError> {
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(AppError::Other(format!("read log: {e}"))),
    };
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<EventRecord>(l)
                .map(|mut rec| {
                    upgrade_legacy_kind(&mut rec);
                    rec
                })
                .map_err(|e| AppError::Other(format!("decode record: {e}")))
        })
        .collect()
}

/// Últimos `n` records (tail). Útil para feeds / debugging.
pub fn tail(path: &Path, n: usize) -> Result<Vec<EventRecord>, AppError> {
    let all = read_all(path)?;
    let start = all.len().saturating_sub(n);
    Ok(all[start..].to_vec())
}

/// Records whose `ts` is strictly greater than `since` (RFC3339), capped at `limit` from the
/// tail end (most recent). When `since` is `None` returns the last `limit` records. Used by
/// `gt-web`'s `GET /api/feed?since=` historico (hq-fe-api-r.5) to seed the SSE consumer.
///
/// `ts` comparison is **string lexicographic** on the RFC3339 form; all writers in this
/// project emit timezone-`Z` records (`record::from_envelope` uses `Rfc3339`) so the lex
/// order matches chronological order. A malformed `since` is treated as "no filter" — the
/// caller decides whether to surface that as a 400; the reader stays infallible on its
/// query input so the gateway can short-circuit empty logs uniformly.
pub fn since(
    path: &Path,
    since: Option<&str>,
    limit: usize,
) -> Result<Vec<EventRecord>, AppError> {
    let all = read_all(path)?;
    let filtered: Vec<EventRecord> = match since {
        Some(s) if !s.is_empty() => all.into_iter().filter(|r| r.ts.as_str() > s).collect(),
        _ => all,
    };
    let start = filtered.len().saturating_sub(limit);
    Ok(filtered[start..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn rec_line(kind: &str) -> String {
        format!(
            r#"{{"event_id":"e1","correlation_id":"c1","causation_id":null,"ts":"2026-05-27T10:00:00Z","type":"{kind}","payload":{{}}}}"#
        )
    }

    #[test]
    fn legacy_bare_kind_is_upgraded_to_v1() {
        let dir = std::env::temp_dir().join(format!("gt-eventlog-reader-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        // A legacy bare kind, a current `.v1` kind, and an out-of-set kind (`scheduling.*` is
        // still emitted bare by its domain, so it must pass through untouched).
        writeln!(f, "{}", rec_line("quota.rotated")).unwrap();
        writeln!(f, "{}", rec_line("merge.merged.v1")).unwrap();
        writeln!(f, "{}", rec_line("scheduling.tick")).unwrap();
        drop(f);

        let recs = read_all(&path).unwrap();
        assert_eq!(recs[0].kind, "quota.rotated.v1", "legacy bare -> v1");
        assert_eq!(recs[1].kind, "merge.merged.v1", "already versioned untouched");
        assert_eq!(recs[2].kind, "scheduling.tick", "out-of-set kind untouched");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_agent_kinds_are_canonicalized() {
        let dir =
            std::env::temp_dir().join(format!("gt-eventlog-reader-agent-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        // Three bare agent kinds that version with a plain `.v1` append, the `session_end` leaf
        // that also kebab-renames, a current forward kind, and an out-of-set kind.
        writeln!(f, "{}", rec_line("agent.spawned")).unwrap();
        writeln!(f, "{}", rec_line("agent.heartbeat")).unwrap();
        writeln!(f, "{}", rec_line("agent.killed")).unwrap();
        writeln!(f, "{}", rec_line("agent.session_end")).unwrap();
        writeln!(f, "{}", rec_line("agent.spawned.v1")).unwrap();
        writeln!(f, "{}", rec_line("scheduling.tick")).unwrap();
        drop(f);

        let recs = read_all(&path).unwrap();
        assert_eq!(recs[0].kind, "agent.spawned.v1", "bare -> v1");
        assert_eq!(recs[1].kind, "agent.heartbeat.v1", "bare -> v1");
        assert_eq!(recs[2].kind, "agent.killed.v1", "bare -> v1");
        assert_eq!(recs[3].kind, "agent.session-end.v1", "snake -> kebab leaf rename");
        assert_eq!(recs[4].kind, "agent.spawned.v1", "already forward untouched");
        assert_eq!(recs[5].kind, "scheduling.tick", "out-of-set kind untouched");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_skills_kinds_are_canonicalized() {
        let dir =
            std::env::temp_dir().join(format!("gt-eventlog-reader-skills-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        // Two bare skills kinds that version with a plain `.v1` append, the two `*_for_role`
        // leaves that also kebab-rename, a current forward kind, and an out-of-set kind.
        writeln!(f, "{}", rec_line("skills.registered")).unwrap();
        writeln!(f, "{}", rec_line("skills.retired")).unwrap();
        writeln!(f, "{}", rec_line("skills.enabled_for_role")).unwrap();
        writeln!(f, "{}", rec_line("skills.disabled_for_role")).unwrap();
        writeln!(f, "{}", rec_line("skills.registered.v1")).unwrap();
        writeln!(f, "{}", rec_line("scheduling.tick")).unwrap();
        drop(f);

        let recs = read_all(&path).unwrap();
        assert_eq!(recs[0].kind, "skills.registered.v1", "bare -> v1");
        assert_eq!(recs[1].kind, "skills.retired.v1", "bare -> v1");
        assert_eq!(recs[2].kind, "skills.enabled-for-role.v1", "snake -> kebab leaf rename");
        assert_eq!(recs[3].kind, "skills.disabled-for-role.v1", "snake -> kebab leaf rename");
        assert_eq!(recs[4].kind, "skills.registered.v1", "already forward untouched");
        assert_eq!(recs[5].kind, "scheduling.tick", "out-of-set kind untouched");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_convoy_kind_is_renamed_to_leaf_v1() {
        let dir =
            std::env::temp_dir().join(format!("gt-eventlog-reader-convoy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        // Legacy orch.* convoy kinds (full re-namespace), a current forward kind, and an
        // out-of-set kind.
        writeln!(f, "{}", rec_line("orch.convoy_created")).unwrap();
        writeln!(f, "{}", rec_line("orch.member_dispatched")).unwrap();
        writeln!(f, "{}", rec_line("convoy.closed.v1")).unwrap();
        writeln!(f, "{}", rec_line("scheduling.tick")).unwrap();
        drop(f);

        let recs = read_all(&path).unwrap();
        assert_eq!(recs[0].kind, "convoy.created.v1", "legacy orch.* -> convoy.*.v1");
        assert_eq!(recs[1].kind, "convoy.member_dispatched.v1", "member re-namespaced");
        assert_eq!(recs[2].kind, "convoy.closed.v1", "already forward untouched");
        assert_eq!(recs[3].kind, "scheduling.tick", "out-of-set kind untouched");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
