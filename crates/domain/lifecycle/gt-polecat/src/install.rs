//! Polecat hook installer (`hq-agent-provisioning.2`).
//!
//! A spawned `claude` polecat reports back to the daemon entirely through Claude Code hooks —
//! plain SHELL commands, no `gt` client dependency (gt-core's `gt` has no hook subcommands, and
//! the gastown Go template is bound to a different binary). This module owns the static settings
//! template and a safe installer that drops it into the rig checkout's `.claude/settings.json`,
//! which Claude auto-loads from its working directory (the polecat's `GT_RIG_PATH`).
//!
//! The hooks read the env the daemon exports at spawn (`hq-agent-provisioning.1`):
//! - `GT_HEARTBEAT_FILE` — `touch`ed on SessionStart / UserPromptSubmit / PostToolUse so the
//!   supervisor's staleness check (90s) sees an active polecat as alive.
//! - `GT_CHANNEL_ROOT` + `GT_HOOK_BEAD` + `GT_BRANCH` — on Stop the polecat drops a
//!   `{"bead","branch"}` message into `$GT_CHANNEL_ROOT/merge-ready/` (atomic tmp+rename, the
//!   gt-channel `*.event` convention) so the daemon's refinery submits the merge and frees the slot.
//! - `GT_CHANNEL_ROOT` + `GT_HOOK_ACCOUNT` — also on Stop, the polecat reports its turn's token
//!   usage (parsed from the Claude transcript) into `$GT_CHANNEL_ROOT/quota-feed/` so predictive
//!   account rotation has an INPUT to evaluate (`hq-agent-provisioning.8`).
//!
//! `bypassPermissions` + `hasCompletedOnboarding` keep an autonomous claude from blocking on the
//! interactive "trust this folder" / permission prompts (the `hq-orchd-deploy.2` gotcha).

use std::io;
use std::path::{Path, PathBuf};

use serde_json::json;

/// Marker key embedded in the managed settings file so the installer never clobbers a human's own
/// `.claude/settings.json` and can recognise its own file for an idempotent refresh.
pub const MANAGED_MARKER: &str = "_gt_managed";
const MANAGED_VALUE: &str = "polecat-hooks";

/// Best-effort heartbeat touch (a missing env var is a silent no-op, never a hook failure). Creates
/// the heartbeat directory first: `touch` does not create parent dirs, so without this the hook
/// silently failed when the dir was absent → no heartbeat → the supervisor re-slings forever
/// (hq-orchd-deploy.19).
const HEARTBEAT_CMD: &str = r#"[ -n "$GT_HEARTBEAT_FILE" ] && { mkdir -p "$(dirname "$GT_HEARTBEAT_FILE")" 2>/dev/null; touch "$GT_HEARTBEAT_FILE" 2>/dev/null; } || true"#;

/// Drop a `{bead,branch}` merge-ready message as an atomic `*.event` file in the channel dir,
/// matching the gt-channel `Channel::emit` convention (`.<id>.tmp` written then renamed to
/// `<id>.event`). No-op when the env isn't wired.
const MERGE_READY_CMD: &str = concat!(
    r#"if [ -n "$GT_CHANNEL_ROOT" ] && [ -n "$GT_HOOK_BEAD" ]; then "#,
    r#"d="$GT_CHANNEL_ROOT/merge-ready"; mkdir -p "$d"; "#,
    r#"id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N); "#,
    r#"printf '{"bead":"%s","branch":"%s"}' "$GT_HOOK_BEAD" "${GT_BRANCH:-$GT_HOOK_BEAD}" "#,
    r#"> "$d/.$id.tmp" && mv "$d/.$id.tmp" "$d/$id.event"; fi"#,
);

/// Report this polecat turn's token usage into the quota-feed so predictive rotation has an INPUT
/// (hq-agent-provisioning.8). On Stop the hook reads the event JSON on stdin (`transcript_path` +
/// `session_id`), sums the `message.usage` of the assistant messages WRITTEN SINCE the last report
/// (a per-session offset file keyed off `GT_HEARTBEAT_FILE` avoids double-counting the cached prompt
/// re-read every turn), and drops a `{account,session,sample}` [`gt_composition::quota_rotation::
/// QuotaFeedPayload`] as an atomic `*.event` into `$GT_CHANNEL_ROOT/quota-feed`. The daemon's feed
/// loop folds it via `quota.sample`, growing the account's consumption + burn-rate EWMA.
///
/// `$GT_HOOK_ACCOUNT` (the keychain account the sling resolved) labels the message — Claude Code
/// hooks never see the `anthropic-ratelimit-*` response headers, so the token sample (not the
/// authoritative window) is the only figure obtainable here. Best-effort: a missing env var, no
/// `jq`, or an absent transcript is a silent no-op, never a hook failure.
pub const COSTS_REPORT_CMD: &str = concat!(
    r#"if [ -n "$GT_CHANNEL_ROOT" ] && [ -n "$GT_HOOK_ACCOUNT" ] && command -v jq >/dev/null 2>&1; then "#,
    r#"ev=$(cat); "#,
    r#"tp=$(printf '%s' "$ev" | jq -r '.transcript_path // empty' 2>/dev/null); "#,
    r#"sid=$(printf '%s' "$ev" | jq -r '.session_id // empty' 2>/dev/null); "#,
    r#"if [ -n "$tp" ] && [ -f "$tp" ]; then "#,
    r#"off="${GT_HEARTBEAT_FILE:-/tmp/gt-qoff-$sid}.qoff"; "#,
    r#"start=$(cat "$off" 2>/dev/null || echo 0); "#,
    r#"end=$(wc -l < "$tp" 2>/dev/null || echo 0); "#,
    r#"if [ "$end" -gt "$start" ]; then "#,
    r#"s=$(tail -n +$((start+1)) "$tp" | jq -sc '[.[]|select(.type=="assistant" and .message.usage!=null)] as $a | if ($a|length)==0 then empty else {model:($a[-1].message.model//"unknown"),input:([$a[].message.usage.input_tokens//0]|add),output:([$a[].message.usage.output_tokens//0]|add),cache_read:([$a[].message.usage.cache_read_input_tokens//0]|add),cache_creation:([$a[].message.usage.cache_creation_input_tokens//0]|add)} end' 2>/dev/null); "#,
    r#"if [ -n "$s" ]; then "#,
    r#"d="$GT_CHANNEL_ROOT/quota-feed"; mkdir -p "$d"; "#,
    r#"id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N); "#,
    r#"printf '{"account":"%s","session":"%s","sample":%s}' "$GT_HOOK_ACCOUNT" "${sid:-$GT_HOOK_ACCOUNT}" "$s" "#,
    r#"> "$d/.$id.tmp" && mv "$d/.$id.tmp" "$d/$id.event"; fi; "#,
    r#"printf '%s' "$end" > "$off"; fi; fi; fi"#,
);

/// One Claude hook entry running `cmd` for every event of its kind (`matcher: ""`).
fn hook(cmd: &str) -> serde_json::Value {
    json!({ "matcher": "", "hooks": [ { "type": "command", "command": cmd } ] })
}

/// The `Stop` hook entry that reports a turn's token usage into the quota-feed ([`COSTS_REPORT_CMD`]).
/// Exported so an INTERACTIVE session (the terminal's role-session launch) can install the same
/// predictive-rotation feed the polecats emit (`hq-quota-feed`) — the command is identical (it reads
/// `GT_HOOK_ACCOUNT` + `GT_CHANNEL_ROOT` from the env), so the daemon's feed loop folds both the same way.
pub fn costs_report_hook_entry() -> serde_json::Value {
    hook(COSTS_REPORT_CMD)
}

/// The static polecat settings document (`.claude/settings.json`). Identical for every polecat —
/// the per-agent values live in the env the hooks read at execution, not in the file.
pub fn polecat_settings_json() -> String {
    let v = json!({
        MANAGED_MARKER: MANAGED_VALUE,
        "hasCompletedOnboarding": true,
        "permissions": { "defaultMode": "bypassPermissions" },
        "hooks": {
            "SessionStart": [ hook(HEARTBEAT_CMD) ],
            "UserPromptSubmit": [ hook(HEARTBEAT_CMD) ],
            "PostToolUse": [ hook(HEARTBEAT_CMD) ],
            "Stop": [ hook(MERGE_READY_CMD), hook(COSTS_REPORT_CMD) ],
        }
    });
    serde_json::to_string_pretty(&v).expect("static template serializes")
}

/// Install the polecat hook settings into `<rig_path>/.claude/settings.json`, returning the path.
///
/// Safe + idempotent: writes only when the file is absent or already gt-managed (carries
/// [`MANAGED_MARKER`]). A human-authored `settings.json` (no marker) is left untouched and an
/// [`io::ErrorKind::AlreadyExists`] is returned so the caller can log + skip rather than clobber it.
pub fn install_polecat_hooks(rig_path: &Path) -> io::Result<PathBuf> {
    let claude_dir = rig_path.join(".claude");
    let target = claude_dir.join("settings.json");
    if let Ok(existing) = std::fs::read_to_string(&target) {
        if !existing.contains(MANAGED_MARKER) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} exists and is not gt-managed; refusing to clobber",
                    target.display()
                ),
            ));
        }
        // gt-managed → safe to refresh to the current template.
    }
    std::fs::create_dir_all(&claude_dir)?;
    std::fs::write(&target, polecat_settings_json())?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_is_valid_json_with_marker_and_reporting_hooks() {
        let v: serde_json::Value = serde_json::from_str(&polecat_settings_json()).unwrap();
        assert_eq!(v[MANAGED_MARKER], json!("polecat-hooks"));
        assert_eq!(v["permissions"]["defaultMode"], json!("bypassPermissions"));
        assert_eq!(v["hasCompletedOnboarding"], json!(true));
        // Heartbeat touch on an activity event; merge-ready emit on Stop.
        let post = v["hooks"]["PostToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(post.contains("touch \"$GT_HEARTBEAT_FILE\""));
        let stop = v["hooks"]["Stop"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(stop.contains("$GT_CHANNEL_ROOT/merge-ready"));
        assert!(stop.contains(r#""bead":"%s""#) && stop.contains(r#""branch":"%s""#));
        assert!(stop.contains(".event"));
        // Second Stop hook reports token usage into the quota-feed (hq-agent-provisioning.8).
        let costs = v["hooks"]["Stop"][1]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(costs.contains("$GT_CHANNEL_ROOT/quota-feed"));
        assert!(costs.contains("$GT_HOOK_ACCOUNT"));
        assert!(costs.contains(r#""sample":%s"#));
        assert!(costs.contains("transcript_path"));
    }

    #[test]
    fn install_writes_then_refreshes_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let p = install_polecat_hooks(dir.path()).unwrap();
        assert!(p.exists());
        assert_eq!(p, dir.path().join(".claude").join("settings.json"));
        // Second install over our own managed file succeeds (refresh).
        let p2 = install_polecat_hooks(dir.path()).unwrap();
        assert_eq!(p, p2);
    }

    #[test]
    fn install_refuses_to_clobber_a_foreign_settings_file() {
        let dir = tempfile::tempdir().unwrap();
        let claude = dir.path().join(".claude");
        std::fs::create_dir_all(&claude).unwrap();
        std::fs::write(claude.join("settings.json"), r#"{"theme":"dark"}"#).unwrap();
        let err = install_polecat_hooks(dir.path()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        // The human's file is intact.
        let kept = std::fs::read_to_string(claude.join("settings.json")).unwrap();
        assert_eq!(kept, r#"{"theme":"dark"}"#);
    }
}
