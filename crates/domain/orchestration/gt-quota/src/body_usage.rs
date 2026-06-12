//! Pure extraction of API-reported token usage from `/v1/messages` response BODIES
//! (hq-284842) — the sample-side counterpart of `probe.rs` (headers).
//!
//! The passthrough proxy records `quota.sample` from what the provider itself reports in
//! each response, replacing edge estimates: local counters that never see the real
//! `input_tokens`/`output_tokens` drift from true spend. Two body shapes exist:
//!
//! - **Non-streaming JSON**: one object with `model` + a final `usage` block.
//! - **SSE stream**: `message_start` carries `model` + input/cache counts;
//!   the last `message_delta` carries the final cumulative `output_tokens`.
//!
//! Both parsers are pure and total over malformed input (`None`, never panic) so a body
//! the proxy couldn't parse degrades to "no sample recorded" while the response still
//! flows to the client untouched.

/// API-reported usage for one model response, in the exact units the provider billed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyUsage {
    pub model: String,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
}

/// Parse a NON-STREAMING `/v1/messages` response body.
pub fn parse_messages_json_usage(raw: &str) -> Option<BodyUsage> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    let usage = v.get("usage")?;
    Some(BodyUsage {
        model: v.get("model")?.as_str()?.to_string(),
        input: u(usage, "input_tokens"),
        output: u(usage, "output_tokens"),
        cache_read: u(usage, "cache_read_input_tokens"),
        cache_creation: u(usage, "cache_creation_input_tokens"),
    })
}

/// Parse a complete SSE stream transcript of a streaming `/v1/messages` response.
/// `message_start` seeds model + input/cache (and a provisional `output_tokens`);
/// every `message_delta.usage.output_tokens` OVERWRITES output (the provider sends the
/// cumulative total, the last one wins). `None` when no `message_start` was seen.
pub fn parse_messages_sse_usage(raw: &str) -> Option<BodyUsage> {
    let mut out: Option<BodyUsage> = None;
    for line in raw.lines() {
        let Some(data) = line.strip_prefix("data:").map(str::trim) else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        match v.get("type").and_then(|t| t.as_str()) {
            Some("message_start") => {
                let msg = v.get("message")?;
                let usage = msg.get("usage")?;
                out = Some(BodyUsage {
                    model: msg.get("model")?.as_str()?.to_string(),
                    input: u(usage, "input_tokens"),
                    output: u(usage, "output_tokens"),
                    cache_read: u(usage, "cache_read_input_tokens"),
                    cache_creation: u(usage, "cache_creation_input_tokens"),
                });
            }
            Some("message_delta") => {
                if let (Some(b), Some(tokens)) = (
                    out.as_mut(),
                    v.get("usage").and_then(|us| us.get("output_tokens")).and_then(|t| t.as_u64()),
                ) {
                    b.output = tokens;
                }
            }
            _ => {}
        }
    }
    out
}

fn u(v: &serde_json::Value, key: &str) -> u64 {
    v.get(key).and_then(|t| t.as_u64()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_usage_reads_all_token_families() {
        let raw = r#"{"id":"msg_1","model":"claude-opus-4-6","usage":{"input_tokens":120,"output_tokens":456,"cache_read_input_tokens":1000,"cache_creation_input_tokens":50}}"#;
        let b = parse_messages_json_usage(raw).unwrap();
        assert_eq!(b.model, "claude-opus-4-6");
        assert_eq!((b.input, b.output, b.cache_read, b.cache_creation), (120, 456, 1000, 50));
    }

    #[test]
    fn json_usage_missing_families_default_zero_and_malformed_is_none() {
        let b = parse_messages_json_usage(
            r#"{"model":"m","usage":{"input_tokens":1,"output_tokens":2}}"#,
        )
        .unwrap();
        assert_eq!((b.cache_read, b.cache_creation), (0, 0));
        assert!(parse_messages_json_usage("{}").is_none(), "no usage block");
        assert!(parse_messages_json_usage("not json").is_none());
    }

    #[test]
    fn sse_usage_combines_message_start_and_last_delta() {
        let raw = concat!(
            "event: message_start\n",
            r#"data: {"type":"message_start","message":{"model":"claude-opus-4-6","usage":{"input_tokens":25,"output_tokens":1,"cache_read_input_tokens":300,"cache_creation_input_tokens":7}}}"#,
            "\n\n",
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","delta":{"text":"hi"}}"#,
            "\n\n",
            "event: message_delta\n",
            r#"data: {"type":"message_delta","usage":{"output_tokens":90}}"#,
            "\n\n",
            "event: message_delta\n",
            r#"data: {"type":"message_delta","usage":{"output_tokens":142}}"#,
            "\n\n",
            "event: message_stop\n",
            r#"data: {"type":"message_stop"}"#,
            "\n",
        );
        let b = parse_messages_sse_usage(raw).unwrap();
        assert_eq!(b.model, "claude-opus-4-6");
        assert_eq!(b.input, 25);
        assert_eq!(b.output, 142, "last cumulative delta wins");
        assert_eq!((b.cache_read, b.cache_creation), (300, 7));
    }

    #[test]
    fn sse_usage_none_without_message_start_and_tolerates_garbage() {
        assert!(parse_messages_sse_usage("data: {\"type\":\"ping\"}\n").is_none());
        assert!(parse_messages_sse_usage("").is_none());
        // Garbage data lines are skipped, valid ones still parse.
        let raw = concat!(
            "data: not-json\n",
            r#"data: {"type":"message_start","message":{"model":"m","usage":{"input_tokens":5,"output_tokens":1}}}"#,
            "\n",
        );
        let b = parse_messages_sse_usage(raw).unwrap();
        assert_eq!(b.input, 5);
    }
}
