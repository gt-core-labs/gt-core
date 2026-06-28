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
    // Attribute by model: group the turn's assistant messages by `message.model` and emit ONE
    // sample per model present (compact, one object per line). Summing every message but labelling
    // the lot with `$a[-1]`'s model mis-attributes a turn that ends on a sub-agent/compaction
    // message. The daemon's `quota.sample` collapses the raw id to its family, so the chart groups
    // cleanly. Empty turn -> `group_by([])[]` yields nothing -> no sample, as before.
    r#"s=$(tail -n +$((start+1)) "$tp" | jq -sc '[.[]|select(.type=="assistant" and .message.usage!=null)]|group_by(.message.model)[]|{model:(.[0].message.model//"unknown"),input:([.[].message.usage.input_tokens//0]|add),output:([.[].message.usage.output_tokens//0]|add),cache_read:([.[].message.usage.cache_read_input_tokens//0]|add),cache_creation:([.[].message.usage.cache_creation_input_tokens//0]|add)}' 2>/dev/null); "#,
    r#"if [ -n "$s" ]; then "#,
    r#"d="$GT_CHANNEL_ROOT/quota-feed"; mkdir -p "$d"; "#,
    r#"printf '%s\n' "$s" | while IFS= read -r sm; do "#,
    r#"[ -n "$sm" ] || continue; "#,
    r#"id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N); "#,
    r#"printf '{"account":"%s","session":"%s","sample":%s}' "$GT_HOOK_ACCOUNT" "${sid:-$GT_HOOK_ACCOUNT}" "$sm" "#,
    r#"> "$d/.$id.tmp" && mv "$d/.$id.tmp" "$d/$id.event"; "#,
    r#"done; fi; "#,
    r#"printf '%s' "$end" > "$off"; fi; fi; fi"#,
);

/// `PreToolUse` guard that DENIES a file write into the memory corpus, redirecting the agent to the
/// `mcp__gt__memory_save` tool (`hq-memory-mcp.6`).
///
/// A `gt` MCP server cannot force its client: the `memory.*` namespace exists, but nothing stops an
/// agent from `Write`ing a `…/memory/*.md` file the old human-assisted way. This hook closes that
/// hole at the HARNESS level — it is seeded into every polecat's settings (and, via
/// `seed_user_hooks`, into the interactive account settings too), so the barrier travels with the
/// config the rig hands each agent.
///
/// The matcher (`Write|Edit|MultiEdit`) scopes it to the file-mutating tools; the command then reads
/// the `PreToolUse` event JSON on stdin (`{tool_name, tool_input:{file_path|filePath}}`, the
/// `COSTS_REPORT_CMD` pattern) and, when the target path contains a `/memory/` segment and ends in
/// `.md`, exits 2 — claude treats a `PreToolUse` exit 2 as "deny the tool call" and surfaces the
/// stderr message to the agent. Anything else passes (`exit 0`). `jq`-free: the path is sniffed with
/// `grep` so the guard fires even on a minimal image. Best-effort *toward denial*: if `tool_input`
/// can't be parsed the guard does not block (it never wedges a legitimate write), the
/// permissions `deny` rule below being the declarative backstop.
const MEMORY_GUARD_CMD: &str = concat!(
    r#"ev=$(cat); "#,
    // Pull the target path from either `file_path` (Write/Edit) or `filePath`, tolerating both
    // jq-present and jq-absent images.
    r#"if command -v jq >/dev/null 2>&1; then "#,
    r#"p=$(printf '%s' "$ev" | jq -r '.tool_input.file_path // .tool_input.filePath // empty' 2>/dev/null); "#,
    r#"else "#,
    r#"p=$(printf '%s' "$ev" | grep -oE '"file_?[Pp]ath"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/'); "#,
    r#"fi; "#,
    // Deny only a memory-corpus markdown file: a `/memory/` path segment + `.md` suffix.
    r#"case "$p" in "#,
    r#"*/memory/*.md) "#,
    r#"echo "❌ BLOCKED: local memory files are read-only — write memories via the gt MCP tool mcp__gt__memory_save (memory.save), not by editing $p" >&2; "#,
    r#"exit 2 ;; "#,
    r#"esac; "#,
    r#"exit 0"#,
);

/// `SessionStart` hook that PUSHES relevant memory into a fresh agent's context so it is born
/// warm instead of cold (`hq-memory-autorecall.1`) — the READ mirror of the [`MEMORY_GUARD_CMD`]
/// write barrier (`hq-memory-mcp.6`). The context bottleneck is that every sling starts blank and
/// must re-derive the hard operating rules + project lore from scratch; this hook front-loads them.
///
/// It runs `gtmcp call memory.recall '{"query":"<bead>","limit":8}'` once at session start. The
/// `memory.recall` server tool ALWAYS returns every `feedback` rule (the hard rules a loop must
/// obey) in full, fused with the top-k semantic matches for the query — so even when the query
/// (the pinned `$GT_HOOK_BEAD`) is a weak signal, the unconditional feedback set still lands. The
/// recall result's `content[0].text` is an inner JSON `{count, memories:[{name,kind,body,…}]}`;
/// the hook renders it to a markdown block and emits it as the SessionStart
/// `additionalContext` (the documented Claude Code channel that prepends a hook's text to the
/// agent's context), so the agent reads its rules + lore before its first action.
///
/// Auth/transport: `gtmcp` (the rig MCP shell client) speaks the gt-core Streamable-HTTP handshake
/// and reads its access JWT from `~/.config/gt-mcp/session.json`. It is invoked only when it is on
/// PATH and a bead query is derivable. BEST-EFFORT: a missing `$GT_HOOK_BEAD`, an absent `gtmcp`, a
/// failed recall, or absent `jq` is a silent `exit 0` WITHOUT extra context — the hook must NEVER
/// break a polecat's launch (the same contract as every other hook here).
const MEMORY_RECALL_CMD: &str = concat!(
    // Knob (hq-memory-autorecall.3): SessionStart autorecall is ON by default; an explicit falsy
    // GT_MEMORY_AUTORECALL turns it off — an operator escape hatch / A-B measure of its value.
    r#"case "${GT_MEMORY_AUTORECALL:-on}" in 0|false|FALSE|off|OFF|no|NO) exit 0 ;; esac; "#,
    // Gate: need a bead to query, the gtmcp client, and jq to shape the output. Any miss → no-op.
    r#"q="${GT_HOOK_BEAD:-}"; "#,
    r#"[ -n "$q" ] || exit 0; "#,
    r#"command -v gtmcp >/dev/null 2>&1 || exit 0; "#,
    r#"command -v jq >/dev/null 2>&1 || exit 0; "#,
    // Top-k is tunable via GT_MEMORY_AUTORECALL_LIMIT (digits only; any non-numeric falls back to 8).
    r#"lim="${GT_MEMORY_AUTORECALL_LIMIT:-8}"; case "$lim" in ''|*[!0-9]*) lim=8 ;; esac; "#,
    // Recall (best-effort, short timeout so a slow/unreachable server never stalls the launch).
    r#"req=$(jq -nc --arg q "$q" --argjson l "$lim" '{query:$q,limit:$l}' 2>/dev/null) || exit 0; "#,
    // Capture stdout+stderr and the exit code instead of discarding them (hq-rbac-reachability.3):
    // a swallowed `|| exit 0` is exactly what kept a scope-denied recall (memory.* ungranted) looking
    // like "no memories" for weeks. Still best-effort — every failure path exits 0 and never breaks the
    // launch — but now it LOGS to stderr (the polecat log) so a dead autorecall is diagnosable.
    r#"raw=$(gtmcp --compact call memory.recall "$req" 2>&1); rc=$?; "#,
    r#"if [ "$rc" -ne 0 ]; then printf '[autorecall] SessionStart recall failed (rc=%s): %s\n' "$rc" "$(printf '%s' "$raw" | tr '\n' ' ' | cut -c1-200)" >&2; exit 0; fi; "#,
    r#"[ -n "$raw" ] || exit 0; "#,
    // A 0-exit JSON-RPC error envelope (e.g. `memory.recall not in scope`) carries no memories; surface
    // it loudly instead of silently rendering an empty block.
    r#"derr=$(printf '%s' "$raw" | jq -r '.error.message // empty' 2>/dev/null); "#,
    r#"if [ -n "$derr" ]; then printf '[autorecall] SessionStart recall denied: %s\n' "$derr" >&2; exit 0; fi; "#,
    // The MCP envelope wraps the tool payload as content[0].text (a JSON string); unwrap then parse.
    r#"inner=$(printf '%s' "$raw" | jq -r '(.content[0].text // empty)' 2>/dev/null); "#,
    r#"[ -n "$inner" ] || inner="$raw"; "#,
    // Render memories -> a text block (jq `+` concatenation, no string interpolation, so the raw
    // Rust literal stays free of escape hazards; plain `===` rules instead of markdown `#` headers
    // to keep the literal free of any `"#` raw-string-terminator collision). A header then one
    // section per memory; an empty memory list yields "" so the guard below injects nothing.
    r#"md=$(printf '%s' "$inner" | jq -r '([(.memories // [])[] | "=== " + .name + " [" + .kind + "] ===\n" + (.description // "") + "\n\n" + (.body // "")] | sort) as $m | if ($m | length) == 0 then "" else "RECALLED MEMORY (auto-injected from prior work). Durable team memories below; treat any [feedback] item as a HARD operating rule.\n\n" + ($m | join("\n\n")) end' 2>/dev/null); "#,
    // Nothing recalled (no memories / parse failed) → stay silent, never emit an empty block.
    r#"[ -n "$md" ] && printf '%s' "$md" | jq -R -s '{hookSpecificOutput:{hookEventName:"SessionStart",additionalContext:.}}' 2>/dev/null; "#,
    // Observability (hq-memory-autorecall.3): when something was injected, log the recalled memory
    // names to STDERR (the polecat log), so an operator can audit WHAT was recalled per sling
    // without it polluting the agent's context (stdout is the context channel; stderr is the log).
    r#"if [ -n "$md" ]; then names=$(printf '%s' "$inner" | jq -r '[(.memories // [])[].name] | join(", ")' 2>/dev/null); [ -n "$names" ] && printf '[autorecall] SessionStart injected: %s\n' "$names" >&2; fi; "#,
    r#"exit 0"#,
);

/// Per-turn variant of [`MEMORY_RECALL_CMD`]: a `UserPromptSubmit` hook that does a SEMANTIC recall
/// against THIS turn's user prompt (not the global pinned bead) and injects the result as the turn's
/// `additionalContext` (`hq-memory-autorecall.2`). Where the SessionStart recall warms the agent
/// once with bead-relevant lore, this refines per-step: a prompt like "fix the merge-queue race"
/// pulls the memories that prompt is actually about, even mid-session.
///
/// COST GOVERNANCE — this fires on EVERY turn, so it adds a recall round-trip (tokens + latency) to
/// every prompt. It is therefore gated behind [`MEMORY_AUTORECALL_TURN_ENV`]
/// (`GT_MEMORY_AUTORECALL_TURN`) and is OFF unless that env is set to a truthy value (`1`/`true`/
/// `on`/`yes`). The default is OFF because the SessionStart recall (.1) already delivers the bulk of
/// the benefit (the agent is born warm with its hard `feedback` rules + bead lore); the per-turn
/// refinement is opt-in so an operator pays the per-turn cost only where the workload warrants it.
/// When ON, the recall `limit` is a conservative [`MEMORY_RECALL_TURN_LIMIT`] (3) — a tight top-k so
/// the per-turn injection stays a nudge, not a context flood. The env NAME + default are shared with
/// the `.3` knobs bead (the `GT_MEMORY_AUTORECALL*` family).
///
/// The Claude Code `UserPromptSubmit` event arrives on stdin as JSON carrying the typed prompt in a
/// `prompt` field (e.g. `{"hook_event_name":"UserPromptSubmit","prompt":"<text>","session_id":…}`);
/// the hook extracts it the same jq-with-grep-fallback way [`MEMORY_GUARD_CMD`] reads `tool_input`,
/// uses it as the recall `query`, unwraps the MCP `content[0].text` envelope, renders the memories to
/// the same text block, and emits it under `hookSpecificOutput.hookEventName == "UserPromptSubmit"`
/// (the documented per-turn additionalContext channel). BEST-EFFORT: the env unset, an empty prompt,
/// an absent `gtmcp`/`jq`, or a failed recall is a silent `exit 0` WITHOUT extra context — the hook
/// must NEVER break the turn (the same contract as every other hook here).
const MEMORY_RECALL_TURN_CMD: &str = concat!(
    // Gate 1: opt-in env. Default OFF — only a truthy GT_MEMORY_AUTORECALL_TURN enables per-turn
    // recall, because it costs a round-trip every turn and the SessionStart recall already warms.
    r#"case "${GT_MEMORY_AUTORECALL_TURN:-}" in 1|true|TRUE|on|ON|yes|YES) ;; *) exit 0 ;; esac; "#,
    // Need the gtmcp client and jq to query + shape the output. Any miss → no-op.
    r#"command -v gtmcp >/dev/null 2>&1 || exit 0; "#,
    r#"command -v jq >/dev/null 2>&1 || exit 0; "#,
    // Read the UserPromptSubmit event and extract the typed prompt, tolerating jq-present and
    // jq-absent images (the MEMORY_GUARD_CMD pattern, here against `.prompt`).
    r#"ev=$(cat); "#,
    r#"if command -v jq >/dev/null 2>&1; then "#,
    r#"q=$(printf '%s' "$ev" | jq -r '.prompt // .user_input // empty' 2>/dev/null); "#,
    r#"else "#,
    r#"q=$(printf '%s' "$ev" | grep -oE '"prompt"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/'); "#,
    r#"fi; "#,
    // No prompt text to query with → stay silent.
    r#"[ -n "$q" ] || exit 0; "#,
    // Recall (best-effort, conservative limit so the per-turn injection stays a focused nudge).
    r#"req=$(jq -nc --arg q "$q" '{query:$q,limit:3}' 2>/dev/null) || exit 0; "#,
    // Same un-swallow as the SessionStart hook (hq-rbac-reachability.3): capture rc + output and log a
    // failure/denial to stderr instead of a silent `|| exit 0`. Best-effort preserved (always exit 0).
    r#"raw=$(gtmcp --compact call memory.recall "$req" 2>&1); rc=$?; "#,
    r#"if [ "$rc" -ne 0 ]; then printf '[autorecall] turn recall failed (rc=%s): %s\n' "$rc" "$(printf '%s' "$raw" | tr '\n' ' ' | cut -c1-200)" >&2; exit 0; fi; "#,
    r#"[ -n "$raw" ] || exit 0; "#,
    r#"derr=$(printf '%s' "$raw" | jq -r '.error.message // empty' 2>/dev/null); "#,
    r#"if [ -n "$derr" ]; then printf '[autorecall] turn recall denied: %s\n' "$derr" >&2; exit 0; fi; "#,
    // Unwrap the MCP envelope (content[0].text is the tool payload JSON string) then render.
    r#"inner=$(printf '%s' "$raw" | jq -r '(.content[0].text // empty)' 2>/dev/null); "#,
    r#"[ -n "$inner" ] || inner="$raw"; "#,
    r#"md=$(printf '%s' "$inner" | jq -r '([(.memories // [])[] | "=== " + .name + " [" + .kind + "] ===\n" + (.description // "") + "\n\n" + (.body // "")] | sort) as $m | if ($m | length) == 0 then "" else "RECALLED MEMORY (auto-injected for this turn). Durable team memories relevant to your prompt below; treat any [feedback] item as a HARD operating rule.\n\n" + ($m | join("\n\n")) end' 2>/dev/null); "#,
    // Nothing recalled → stay silent, never emit an empty block.
    r#"[ -n "$md" ] && printf '%s' "$md" | jq -R -s '{hookSpecificOutput:{hookEventName:"UserPromptSubmit",additionalContext:.}}' 2>/dev/null; "#,
    r#"exit 0"#,
);

/// One Claude hook entry running `cmd` for every event of its kind (`matcher: ""`).
fn hook(cmd: &str) -> serde_json::Value {
    json!({ "matcher": "", "hooks": [ { "type": "command", "command": cmd } ] })
}

/// One Claude hook entry running `cmd` only for tools matching `matcher` (a `|`-joined tool list).
fn hook_for(matcher: &str, cmd: &str) -> serde_json::Value {
    json!({ "matcher": matcher, "hooks": [ { "type": "command", "command": cmd } ] })
}

/// Decide whether a write to `path` must be DENIED because it targets the file-based memory corpus
/// (`…/memory/*.md`) — the source-of-truth the [`MEMORY_GUARD_CMD`] hook enforces, lifted into pure
/// Rust so the rule is unit-tested without spawning a shell.
///
/// A path is a memory write iff it has a `memory` path component AND a `.md` extension. This matches
/// the corpus convention (`gt-memory`: `…/memory/{feedback,project,reference,user}*.md` + the
/// `MEMORY.md` index) while leaving unrelated paths (a `src/memory_store.rs`, a `notes.md` outside a
/// `memory/` dir) free to be written.
pub fn is_memory_corpus_write(path: &str) -> bool {
    let p = std::path::Path::new(path);
    let is_md = p
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("md"))
        .unwrap_or(false);
    if !is_md {
        return false;
    }
    // A `memory` directory component anywhere ABOVE the file (not the file's own stem), so
    // `…/memory/x.md` denies but `…/memory.md` does not.
    p.parent()
        .map(|parent| {
            parent
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => s.to_str(),
                    _ => None,
                })
                .any(|s| s == "memory")
        })
        .unwrap_or(false)
}

/// Default cap on the relevance-ranked tail the SessionStart autorecall asks for
/// (`hq-memory-autorecall.1`). The `feedback` rules return in full regardless; this only bounds the
/// semantic top-k so the injected context stays a focused primer, not a full memory dump.
pub const MEMORY_RECALL_LIMIT: u32 = 8;

/// Decide whether SessionStart memory autorecall should fire for a given pinned bead, and if so
/// build the `memory.recall` arguments — lifted into pure Rust so the gate + query construction are
/// unit-tested without spawning the shell hook ([`MEMORY_RECALL_CMD`]).
///
/// Autorecall is active iff a non-blank bead is pinned (`$GT_HOOK_BEAD` at the edge): the bead id is
/// the only universally-available query signal in a SessionStart shell, and it seeds the semantic
/// tail. A blank/absent bead ⇒ `None` (the hook no-ops), mirroring the shell gate exactly so the two
/// can't drift. The hard `feedback` rules land regardless of the query — the server returns them
/// unconditionally — so even a weak query still warms the agent with its operating rules.
pub fn memory_recall_args(bead: Option<&str>) -> Option<serde_json::Value> {
    let bead = bead.map(str::trim).filter(|b| !b.is_empty())?;
    Some(json!({ "query": bead, "limit": MEMORY_RECALL_LIMIT }))
}

/// Env that toggles the SessionStart autorecall (`hq-memory-autorecall.3`). Unlike the per-turn
/// [`MEMORY_AUTORECALL_TURN_ENV`], SessionStart recall is ON by default (it is the primary
/// bottleneck-reliever); this env is the operator escape hatch / A-B switch.
pub const MEMORY_AUTORECALL_ENV: &str = "GT_MEMORY_AUTORECALL";

/// Env that overrides the SessionStart recall top-k (`hq-memory-autorecall.3`). Absent/non-numeric
/// falls back to [`MEMORY_RECALL_LIMIT`].
pub const MEMORY_AUTORECALL_LIMIT_ENV: &str = "GT_MEMORY_AUTORECALL_LIMIT";

/// Whether SessionStart autorecall is enabled given [`MEMORY_AUTORECALL_ENV`] — ON unless the value
/// is explicitly falsy (`0`/`false`/`off`/`no`, case-insensitive). Pure-Rust mirror of the shell
/// `case` gate in [`MEMORY_RECALL_CMD`] so the two cannot drift, unit-tested.
pub fn memory_autorecall_enabled(env_value: Option<&str>) -> bool {
    !matches!(
        env_value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("0" | "false" | "off" | "no")
    )
}

/// Resolve the SessionStart recall top-k from [`MEMORY_AUTORECALL_LIMIT_ENV`]: a positive integer,
/// else [`MEMORY_RECALL_LIMIT`]. Mirror of the shell digit-guard, unit-tested.
pub fn memory_recall_limit(env_value: Option<&str>) -> u32 {
    env_value
        .map(str::trim)
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(MEMORY_RECALL_LIMIT)
}

/// Env an operator sets to opt into per-turn memory recall (`hq-memory-autorecall.2`). The per-turn
/// hook ([`MEMORY_RECALL_TURN_CMD`]) fires on EVERY prompt, so it is OFF unless this env is truthy —
/// the SessionStart recall (.1) already warms the agent; the per-turn refinement is opt-in cost.
/// Shared name with the `.3` knobs bead (the `GT_MEMORY_AUTORECALL*` family).
pub const MEMORY_AUTORECALL_TURN_ENV: &str = "GT_MEMORY_AUTORECALL_TURN";

/// Conservative per-turn recall top-k (`hq-memory-autorecall.2`): tighter than the SessionStart
/// [`MEMORY_RECALL_LIMIT`] so a mid-turn injection stays a focused nudge, not a context flood.
pub const MEMORY_RECALL_TURN_LIMIT: u32 = 3;

/// Whether a value read from [`MEMORY_AUTORECALL_TURN_ENV`] enables per-turn recall — the truthy set
/// the shell `case` gate accepts (`1`/`true`/`on`/`yes`, case-insensitive). Lifted into pure Rust so
/// the Rust mirror and the shell gate cannot drift, and unit-tested.
pub fn memory_autorecall_turn_enabled(env_value: Option<&str>) -> bool {
    matches!(
        env_value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

/// Build the per-turn `memory.recall` arguments for a user prompt — the pure-Rust mirror of
/// [`MEMORY_RECALL_TURN_CMD`]'s gate + query. `None` when per-turn recall is disabled
/// ([`memory_autorecall_turn_enabled`]) or the prompt is blank (the hook no-ops); else the prompt
/// seeds the semantic query at the conservative [`MEMORY_RECALL_TURN_LIMIT`].
pub fn memory_recall_turn_args(env_value: Option<&str>, prompt: &str) -> Option<serde_json::Value> {
    if !memory_autorecall_turn_enabled(env_value) {
        return None;
    }
    let prompt = prompt.trim();
    if prompt.is_empty() {
        return None;
    }
    Some(json!({ "query": prompt, "limit": MEMORY_RECALL_TURN_LIMIT }))
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
///
/// `enabledMcpjsonServers` pre-trusts the project `.mcp.json` `gt` server so an autonomous polecat
/// never stalls on the interactive "New MCP server found … Use this MCP server?" prompt
/// (`hq-polecat-provisioning-20260608.1`): the `.mcp.json` the daemon seeds into the worktree is
/// project-scoped, and without this allowlist claude blocks on first launch waiting for a keypress.
/// The managed-agent `permissions` block — the SINGLE source of every gt role's claude permission
/// model (gtcore-a01791). Every launch path materialises this same value into a session's
/// `settings.json`: the polecat sling (via [`polecat_settings_json`]), the interactive/role + mayor
/// apparatus (via `gt_composition::build_settings`), and the account seed (`seed_user_hooks`). One
/// definition, every role — so "the permissions load from the agents model" holds uniformly.
///
/// - `defaultMode: bypassPermissions` — autonomous agents never stop at an interactive permission
///   prompt.
/// - `deny` — a declarative backstop to the PreToolUse memory guard (hq-memory-mcp.6): the agent
///   must save memories via `mcp__gt__memory_save`, never by writing the `*/memory/*.md` corpus.
///   `permissions.deny` tool names are VALIDATED by claude at startup — an entry naming a tool the
///   running claude doesn't know logs "deny rule … matches no known tool" (observed live in a mayor
///   session, gtcore-a01791). `MultiEdit` was folded into `Edit` in recent claude, so it is omitted
///   here to keep the rule set valid. (The PreToolUse hook matcher below still lists `MultiEdit`:
///   hook matchers are not name-validated, so keeping it is harmless + future-proof.)
pub fn managed_permissions() -> serde_json::Value {
    json!({
        "defaultMode": "bypassPermissions",
        "deny": [ "Write(**/memory/**.md)", "Edit(**/memory/**.md)" ]
    })
}

pub fn polecat_settings_json() -> String {
    let v = json!({
        MANAGED_MARKER: MANAGED_VALUE,
        "hasCompletedOnboarding": true,
        // Suppress the interactive "1. No / 2. Yes, I accept" bypass-permissions confirmation
        // dialog on startup. Without this, autonomous polecats block waiting for a keypress even
        // though `permissions.defaultMode` is already `bypassPermissions`.
        "dangerouslySkipPermissions": true,
        "enabledMcpjsonServers": ["gt"],
        // The shared managed-agent permission model (gtcore-a01791) — identical for every role.
        "permissions": managed_permissions(),
        "hooks": {
            // SessionStart: heartbeat touch + memory autorecall (hq-memory-autorecall.1) — the
            // recall hook PUSHES the team's feedback rules + bead-relevant lore into the fresh
            // agent's context so it is born warm, the read mirror of the PreToolUse memory guard.
            "SessionStart": [ hook(HEARTBEAT_CMD), hook(MEMORY_RECALL_CMD) ],
            // UserPromptSubmit: heartbeat touch + per-turn memory autorecall (hq-memory-autorecall.2).
            // The per-turn recall is OPT-IN (GT_MEMORY_AUTORECALL_TURN) and no-ops by default — it
            // refines context against THIS prompt, on top of the SessionStart warm-up.
            "UserPromptSubmit": [ hook(HEARTBEAT_CMD), hook(MEMORY_RECALL_TURN_CMD) ],
            "PostToolUse": [ hook(HEARTBEAT_CMD) ],
            // PreToolUse memory guard (hq-memory-mcp.6): DENY a write into the file-based memory
            // corpus, redirecting the agent to mcp__gt__memory_save. The deterministic harness-level
            // half of the enforcement; the `permissions.deny` above is its declarative twin.
            "PreToolUse": [ hook_for("Write|Edit|MultiEdit", MEMORY_GUARD_CMD) ],
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
        // Suppress the interactive bypass-permissions confirmation dialog on startup.
        assert_eq!(v["dangerouslySkipPermissions"], json!(true));
        // The project .mcp.json `gt` server is pre-trusted so the polecat never stalls on the
        // interactive "Use this MCP server?" prompt (hq-polecat-provisioning-20260608.1).
        assert_eq!(v["enabledMcpjsonServers"], json!(["gt"]));
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
        // PreToolUse memory guard (hq-memory-mcp.6): scoped to the file-mutating tools, denies a
        // `…/memory/*.md` write (exit 2) and redirects to mcp__gt__memory_save.
        let pre = &v["hooks"]["PreToolUse"][0];
        assert_eq!(pre["matcher"], json!("Write|Edit|MultiEdit"));
        let guard = pre["hooks"][0]["command"].as_str().unwrap();
        assert!(guard.contains("*/memory/*.md)"), "matches the memory corpus path");
        assert!(guard.contains("exit 2"), "denies the tool call");
        assert!(guard.contains("mcp__gt__memory_save"), "redirects to the MCP tool");
        // Declarative backstop: deny rules under the bypassPermissions mode.
        let deny = v["permissions"]["deny"].as_array().unwrap();
        assert!(deny.iter().any(|r| r == "Write(**/memory/**.md)"));
        assert!(deny.iter().any(|r| r == "Edit(**/memory/**.md)"));
        // SessionStart carries TWO hooks: the heartbeat touch and the memory autorecall
        // (hq-memory-autorecall.1) that injects recalled memory as additionalContext.
        let ss = v["hooks"]["SessionStart"].as_array().unwrap();
        assert_eq!(ss.len(), 2, "heartbeat + memory autorecall");
        assert!(ss[0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("touch \"$GT_HEARTBEAT_FILE\""));
        let recall = ss[1]["hooks"][0]["command"].as_str().unwrap();
        assert!(recall.contains("gtmcp"), "invokes the rig MCP client");
        assert!(recall.contains("memory.recall"), "calls the recall tool");
        assert!(recall.contains("GT_HOOK_BEAD"), "queries by the pinned bead");
        // Injects via the documented SessionStart additionalContext channel, and is best-effort:
        // a missing bead / gtmcp / jq is a silent exit 0, never a launch failure.
        assert!(recall.contains("additionalContext"));
        assert!(recall.contains("SessionStart"));
        assert!(recall.contains("exit 0"));
        // Knobs + observability (hq-memory-autorecall.3): on/off gate, tunable limit, and a
        // stderr log of what was injected.
        assert!(recall.contains("GT_MEMORY_AUTORECALL"), "on/off knob");
        assert!(recall.contains("GT_MEMORY_AUTORECALL_LIMIT"), "tunable top-k");
        assert!(recall.contains("[autorecall] SessionStart injected"), "observability log");
        // hq-rbac-reachability.3: a recall failure/denial is LOGGED to stderr, not swallowed by a
        // silent `|| exit 0` — the gap that hid the memory.* scope denial for weeks.
        assert!(recall.contains("recall failed"), "logs a transport failure");
        assert!(recall.contains("recall denied"), "logs a scope/JSON-RPC error");
        // UserPromptSubmit carries the heartbeat + the per-turn memory autorecall
        // (hq-memory-autorecall.2): opt-in, queries by the prompt, injects per-turn context.
        let ups = v["hooks"]["UserPromptSubmit"].as_array().unwrap();
        assert_eq!(ups.len(), 2, "heartbeat + per-turn autorecall");
        let turn = ups[1]["hooks"][0]["command"].as_str().unwrap();
        assert!(turn.contains("GT_MEMORY_AUTORECALL_TURN"), "opt-in env gate");
        assert!(turn.contains("memory.recall"), "calls the recall tool");
        assert!(turn.contains("UserPromptSubmit"), "per-turn additionalContext channel");
        assert!(turn.contains("exit 0"), "best-effort: never breaks the turn");
        assert!(turn.contains("recall denied"), "per-turn recall also logs a denial (hq-rbac-reachability.3)");
    }

    #[test]
    fn memory_recall_args_gate_and_query() {
        // hq-memory-autorecall.1: a pinned bead → recall args querying by that bead, limit capped.
        let a = memory_recall_args(Some("hq-abc.1")).unwrap();
        assert_eq!(a["query"], json!("hq-abc.1"));
        assert_eq!(a["limit"], json!(MEMORY_RECALL_LIMIT));
        // Whitespace is trimmed into the query.
        assert_eq!(
            memory_recall_args(Some("  hq-9  ")).unwrap()["query"],
            json!("hq-9")
        );
        // No bead pinned → autorecall is OFF (the hook no-ops), mirroring the shell gate.
        assert!(memory_recall_args(None).is_none());
        assert!(memory_recall_args(Some("")).is_none());
        assert!(memory_recall_args(Some("   ")).is_none());
    }

    #[test]
    fn memory_recall_turn_gate_and_query() {
        // hq-memory-autorecall.2: OFF by default — without the opt-in env, no per-turn recall.
        assert!(!memory_autorecall_turn_enabled(None));
        assert!(!memory_autorecall_turn_enabled(Some("0")));
        assert!(!memory_autorecall_turn_enabled(Some("off")));
        assert!(memory_recall_turn_args(None, "fix the merge race").is_none());
        // The truthy set (case-insensitive) enables it.
        for v in ["1", "true", "TRUE", "on", "ON", "yes", "  Yes  "] {
            assert!(memory_autorecall_turn_enabled(Some(v)), "{v} should enable");
        }
        // Enabled + a prompt → recall args querying by the prompt at the conservative turn limit.
        let a = memory_recall_turn_args(Some("on"), "  fix the merge-queue race  ").unwrap();
        assert_eq!(a["query"], json!("fix the merge-queue race"));
        assert_eq!(a["limit"], json!(MEMORY_RECALL_TURN_LIMIT));
        assert!(MEMORY_RECALL_TURN_LIMIT < MEMORY_RECALL_LIMIT, "turn top-k is tighter");
        // Enabled but a blank prompt → no-op, mirroring the shell gate.
        assert!(memory_recall_turn_args(Some("1"), "   ").is_none());
    }

    #[test]
    fn sessionstart_autorecall_knobs() {
        // hq-memory-autorecall.3: SessionStart recall is ON by default; only an explicit falsy
        // value disables it (the inverse default of the per-turn knob).
        assert!(memory_autorecall_enabled(None), "ON by default");
        assert!(memory_autorecall_enabled(Some("on")));
        assert!(memory_autorecall_enabled(Some("anything")));
        for off in ["0", "false", "FALSE", "off", "OFF", "no", "  No  "] {
            assert!(!memory_autorecall_enabled(Some(off)), "{off} disables");
        }
        // Limit override: a positive integer wins; absent / non-numeric / zero falls back.
        assert_eq!(memory_recall_limit(Some("3")), 3);
        assert_eq!(memory_recall_limit(Some("  20 ")), 20);
        assert_eq!(memory_recall_limit(None), MEMORY_RECALL_LIMIT);
        assert_eq!(memory_recall_limit(Some("abc")), MEMORY_RECALL_LIMIT);
        assert_eq!(memory_recall_limit(Some("0")), MEMORY_RECALL_LIMIT);
    }

    #[test]
    fn memory_corpus_writes_are_denied_others_pass() {
        // hq-memory-mcp.6: a markdown file under a `memory/` dir is a corpus write → DENY.
        assert!(is_memory_corpus_write("/home/nixos/gt-web/memory/feedback.md"));
        assert!(is_memory_corpus_write(
            "/home/nixos/.claude/projects/x/memory/MEMORY.md"
        ));
        assert!(is_memory_corpus_write("memory/project.md"));
        assert!(is_memory_corpus_write("/a/b/memory/c/d.md")); // nested under memory/
        // Not the corpus → ALLOW.
        assert!(!is_memory_corpus_write("/home/nixos/gt-web/src/memory_store.rs"));
        assert!(!is_memory_corpus_write("/home/nixos/notes.md")); // .md but no memory/ dir
        assert!(!is_memory_corpus_write("/home/nixos/memory.md")); // file named memory, not a dir
        assert!(!is_memory_corpus_write("/home/nixos/memory/data.json")); // memory/ but not .md
        assert!(!is_memory_corpus_write("")); // empty path
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
