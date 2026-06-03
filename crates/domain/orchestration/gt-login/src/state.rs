//! Pure state machine + stream parser for the login flow. No I/O, no clocks: every
//! transition is data in / data out so the driver test can replay any sequence without
//! spawning a real PTY.

use std::sync::OnceLock;

use regex::Regex;

/// Driver lifecycle. The state moves forward only — once `Complete` or `Failed` it stays
/// there. The driver consults the current variant to decide what to do with the next chunk
/// or caller command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginState {
    /// `claude /login` spawned; we have not yet seen the URL in its output.
    AwaitingUrl,
    /// URL surfaced; waiting for the caller to forward a token from the user.
    UrlReady { url: String },
    /// Token has been written to stdin; waiting for the CLI to exit with success or fail.
    AwaitingExit,
    /// Terminal — CLI exited 0 after token acceptance.
    Complete,
    /// Terminal — anything that aborts the flow.
    Failed,
}

impl LoginState {
    /// `true` once no further input matters (driver loop must exit).
    pub fn is_terminal(&self) -> bool {
        matches!(self, LoginState::Complete | LoginState::Failed)
    }
}

/// Extract the first Anthropic OAuth URL from a chunk of CLI output.
///
/// Conservative pattern: anchored on the Anthropic-owned hosts the `claude /login`
/// command currently prints (`console.anthropic.com` and `claude.ai`), then any non-space
/// path. We deliberately keep the host set narrow so an unrelated link in the banner
/// (docs, status page) does not get misread as the OAuth URL.
///
/// Returns the matched URL stripped of trailing punctuation the terminal commonly
/// appends when wrapping a line (`.`, `,`, `)`, `]`, `>`). Hash-fragments are kept — the
/// OAuth `state` parameter lives there.
pub fn extract_url(chunk: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"https://(?:console\.anthropic\.com|claude\.ai)/[^\s]+")
            .expect("static regex compiles")
    });
    let mat = re.find(chunk)?;
    let url = mat.as_str().trim_end_matches(|c: char| {
        matches!(c, '.' | ',' | ')' | ']' | '>' | '"' | '\'')
    });
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_console_url() {
        let chunk = "Open the following URL in your browser:\n  \
            https://console.anthropic.com/oauth/authorize?client_id=abc&state=xyz\n";
        assert_eq!(
            extract_url(chunk).as_deref(),
            Some("https://console.anthropic.com/oauth/authorize?client_id=abc&state=xyz"),
        );
    }

    #[test]
    fn extract_claude_ai_url() {
        let chunk = "Visit https://claude.ai/oauth?code_challenge=abc to continue.";
        assert_eq!(
            extract_url(chunk).as_deref(),
            Some("https://claude.ai/oauth?code_challenge=abc"),
        );
    }

    #[test]
    fn trim_trailing_punct() {
        // Terminal often wraps with a period or paren; the regex grabs them as `[^\s]+`.
        let chunk = "(see https://console.anthropic.com/oauth?x=1).";
        assert_eq!(
            extract_url(chunk).as_deref(),
            Some("https://console.anthropic.com/oauth?x=1"),
        );
    }

    #[test]
    fn ignore_unrelated_hosts() {
        // Banner mentions docs/status — they must not be misread as the OAuth URL.
        let chunk = "Docs: https://docs.anthropic.com/  Status: https://status.anthropic.com/";
        assert_eq!(extract_url(chunk), None);
    }

    #[test]
    fn returns_first_match() {
        // Defensive: if the CLI ever prints two URLs, we lock onto the first (which is the
        // actual OAuth URL — banners come after).
        let chunk = "Open https://console.anthropic.com/oauth?a=1 then https://claude.ai/x?b=2";
        assert_eq!(
            extract_url(chunk).as_deref(),
            Some("https://console.anthropic.com/oauth?a=1"),
        );
    }

    #[test]
    fn terminal_state_check() {
        assert!(!LoginState::AwaitingUrl.is_terminal());
        assert!(!LoginState::UrlReady { url: "x".into() }.is_terminal());
        assert!(!LoginState::AwaitingExit.is_terminal());
        assert!(LoginState::Complete.is_terminal());
        assert!(LoginState::Failed.is_terminal());
    }
}
