//! Stored-conversation resolution for ended agent sessions (gtcore-c848dd).
//!
//! Every orchestrated session's `claude` writes its conversation as JSONL under the account dir
//! it ran with: `<accounts_root>/<account-ulid>/projects/<workdir-slug>/<conversation-uuid>.jsonl`,
//! where `<workdir-slug>` is the session's working directory with every `/` and `.` mapped to `-`
//! (a polecat's `/rig-wt/<session>` becomes `-rig-wt-<session>`; a role session's
//! `<channel_root>/role-wd/<session>` ends in `-role-wd-<session>`). The FE's Terminal on an
//! ENDED session has no process to attach — this module finds that stored file by scanning the
//! account dirs for a project slug ending in the (slug-sanitized) session id, and parses it into
//! the compact [`Transcript`] the `GET /api/v1/agent/:id/transcript` route serves.
//!
//! Parsing keeps the operator-facing substance and drops the plumbing: `user`/`assistant` records
//! contribute their text content and (for the assistant) the tool names invoked; sidechain
//! records (subagents), file-history snapshots, mode markers and thinking blocks are skipped.

use std::path::{Path, PathBuf};

use gt_agent::{Transcript, TranscriptSource, TranscriptTurn};

/// Filesystem-backed [`TranscriptSource`] over the shared accounts volume.
pub struct FsTranscripts {
    accounts_root: PathBuf,
}

impl FsTranscripts {
    pub fn new(accounts_root: PathBuf) -> Self {
        Self { accounts_root }
    }
}

impl TranscriptSource for FsTranscripts {
    fn transcript(&self, session: &str) -> Option<Transcript> {
        let file = resolve_transcript_file(&self.accounts_root, session)?;
        let raw = std::fs::read_to_string(&file).ok()?;
        let turns = parse_transcript_jsonl(&raw);
        if turns.is_empty() {
            return None;
        }
        Some(Transcript {
            session: session.to_string(),
            turns,
        })
    }
}

/// The workdir-slug component a session id contributes: the same `[^A-Za-z0-9-]` → `-` mapping
/// the `claude` CLI applies to the whole cwd path when naming its per-project dir.
fn slugify(part: &str) -> String {
    part.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' })
        .collect()
}

/// Find the newest conversation JSONL for `session`: scan `<root>/*/projects/*` for a project
/// dir whose slug ends in `-<session-slug>` (the workdir basename is the session id for every
/// orchestrated launch — polecat worktrees and role-wd dirs alike) and pick the most recently
/// modified `.jsonl` inside. Newest wins because a re-sling reuses the session id with a fresh
/// conversation file.
fn resolve_transcript_file(root: &Path, session: &str) -> Option<PathBuf> {
    let suffix = format!("-{}", slugify(session));
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for account in std::fs::read_dir(root).ok()? {
        let projects = account.ok()?.path().join("projects");
        let Ok(dirs) = std::fs::read_dir(&projects) else {
            continue;
        };
        for dir in dirs.flatten() {
            let name = dir.file_name().to_string_lossy().into_owned();
            if !name.ends_with(&suffix) {
                continue;
            }
            let Ok(files) = std::fs::read_dir(dir.path()) else {
                continue;
            };
            for f in files.flatten() {
                let p = f.path();
                if p.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                let modified = f
                    .metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                if best.as_ref().map(|(t, _)| modified > *t).unwrap_or(true) {
                    best = Some((modified, p));
                }
            }
        }
    }
    best.map(|(_, p)| p)
}

/// Fold the raw JSONL into ordered turns. Tolerant by design: an unparseable line is skipped —
/// the transcript is an observability read, never load-bearing.
fn parse_transcript_jsonl(raw: &str) -> Vec<TranscriptTurn> {
    let mut turns = Vec::new();
    for line in raw.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        // Subagent sidechains are their own conversations — keep the main thread only.
        if v.get("isSidechain").and_then(|s| s.as_bool()) == Some(true) {
            continue;
        }
        let role = match v.get("type").and_then(|t| t.as_str()) {
            Some("user") => "user",
            Some("assistant") => "assistant",
            _ => continue,
        };
        let at = v
            .get("timestamp")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let content = v.get("message").and_then(|m| m.get("content"));
        let mut text = String::new();
        let mut tools = Vec::new();
        match content {
            // A bare-string user prompt.
            Some(serde_json::Value::String(s)) => text.push_str(s),
            Some(serde_json::Value::Array(items)) => {
                for item in items {
                    match item.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                                if !text.is_empty() {
                                    text.push('\n');
                                }
                                text.push_str(t);
                            }
                        }
                        Some("tool_use") => {
                            if let Some(n) = item.get("name").and_then(|n| n.as_str()) {
                                tools.push(n.to_string());
                            }
                        }
                        // thinking / tool_result / images: plumbing, not conversation.
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        if text.is_empty() && tools.is_empty() {
            continue; // a turn with no operator-facing substance (e.g. tool_result carrier)
        }
        turns.push(TranscriptTurn {
            role: role.to_string(),
            text,
            tools,
            at,
        });
    }
    turns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_turns_and_tool_uses_skipping_plumbing() {
        let raw = concat!(
            r#"{"type":"mode","mode":"normal"}"#, "\n",
            r#"{"type":"user","timestamp":"2026-07-03T23:11:50Z","message":{"role":"user","content":"You are a gt polecat. Work bead X."}}"#, "\n",
            r#"{"type":"file-history-snapshot","messageId":"m1"}"#, "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"Claiming the bead."},{"type":"tool_use","name":"mcp__gt__issues_claim_execute","input":{}}]}}"#, "\n",
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"ok"}]}}"#, "\n",
            r#"{"type":"assistant","isSidechain":true,"message":{"role":"assistant","content":[{"type":"text","text":"subagent noise"}]}}"#, "\n",
            r#"not json at all"#, "\n",
        );
        let turns = parse_transcript_jsonl(raw);
        assert_eq!(turns.len(), 2, "prompt + assistant turn; plumbing skipped: {turns:?}");
        assert_eq!(turns[0].role, "user");
        assert!(turns[0].text.starts_with("You are a gt polecat"));
        assert_eq!(turns[0].at, "2026-07-03T23:11:50Z");
        assert_eq!(turns[1].role, "assistant");
        assert_eq!(turns[1].text, "Claiming the bead.");
        assert_eq!(turns[1].tools, vec!["mcp__gt__issues_claim_execute"]);
    }

    #[test]
    fn resolves_the_newest_jsonl_for_the_session_slug() {
        // Layout: two accounts; the session's project dir exists in one of them with two
        // conversations (a re-sling) — the newest file wins. Dots in the session id slugify.
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp
            .path()
            .join("01AAA")
            .join("projects")
            .join("-rig-wt-authapp-authapp-9238e5");
        std::fs::create_dir_all(&proj).unwrap();
        std::fs::write(proj.join("old.jsonl"), "{}").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(proj.join("new.jsonl"), "{}").unwrap();
        let other = tmp.path().join("01BBB").join("projects").join("-rig-wt-unrelated");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("x.jsonl"), "{}").unwrap();

        let hit = resolve_transcript_file(tmp.path(), "authapp-authapp-9238e5").unwrap();
        assert!(hit.ends_with("new.jsonl"), "newest conversation wins: {hit:?}");
        assert!(resolve_transcript_file(tmp.path(), "no-such-session").is_none());

        // FsTranscripts end-to-end over the same layout: empty parse ⇒ None (the '{}' stub has
        // no turns), a real turn ⇒ Some.
        std::fs::write(
            proj.join("new.jsonl"),
            r#"{"type":"user","message":{"role":"user","content":"hola"}}"#,
        )
        .unwrap();
        let src = FsTranscripts::new(tmp.path().to_path_buf());
        let t = src.transcript("authapp-authapp-9238e5").unwrap();
        assert_eq!(t.turns.len(), 1);
        assert_eq!(t.turns[0].text, "hola");
    }
}
