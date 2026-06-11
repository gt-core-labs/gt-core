//! `InboundMail` — the inbound-email seam (hq-8a521a, epic hq-56b5ee).
//!
//! Mirror of the outbound [`EmailTransport`](crate::EmailTransport) discipline:
//! the IMAP/webhook server is PENDING, so the command mailbox polls THIS trait
//! and ships today with [`FileInbox`] — a directory of `*.json` messages (one
//! per file) that tests and operators can drop messages into. When the real
//! server lands, an Imap/Webhook impl replaces the file source and nothing in
//! the mailbox daemon changes.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// One inbound message, transport-agnostic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundEmail {
    /// Sender address (`From:`). The mailbox resolves this against the
    /// workspace member mirror — unknown senders are quarantined.
    pub from: String,
    /// Subject line.
    #[serde(default)]
    pub subject: String,
    /// Plain-text body.
    #[serde(default)]
    pub body: String,
    /// The transport's message id (threading anchor for the reply).
    #[serde(default)]
    pub message_id: String,
    /// `In-Reply-To:` — set when the sender replied to one of OUR emails (the
    /// outbox row id rides here), which routes the message through the
    /// email→comment bridge instead of the command parser.
    #[serde(default)]
    pub in_reply_to: Option<String>,
}

/// The inbound seam. Sync + blocking like the outbound port; the daemon owns
/// scheduling and never blocks an async worker on it directly.
pub trait InboundMail: Send + Sync {
    /// Drain the currently-available messages (each returned exactly once).
    fn poll(&self) -> Result<Vec<InboundEmail>, String>;
    /// Move one polled message to quarantine (unknown sender — kept for
    /// operator review, never deleted silently).
    fn quarantine(&self, msg: &InboundEmail, reason: &str);
    /// A short label for boot logs (`file`, `imap`).
    fn label(&self) -> &'static str;
}

/// The shipping default: a maildir-ish directory of `*.json` files
/// ([`InboundEmail`] shape). Polling consumes a file by renaming it `*.done`
/// (or `*.quarantine` with the reason alongside), so a crash between poll and
/// processing re-delivers rather than loses.
pub struct FileInbox {
    dir: PathBuf,
}

impl FileInbox {
    /// Watch `dir` (created if absent).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let _ = std::fs::create_dir_all(&dir);
        Self { dir }
    }

    fn done_path(&self, msg: &InboundEmail) -> PathBuf {
        self.dir.join(format!("{}.json.done", sanitize(&msg.message_id)))
    }
}

fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' }).collect()
}

impl InboundMail for FileInbox {
    fn poll(&self) -> Result<Vec<InboundEmail>, String> {
        let mut out = Vec::new();
        let entries = std::fs::read_dir(&self.dir).map_err(|e| e.to_string())?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let raw = match std::fs::read_to_string(&path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[inbound-mail] unreadable {path:?}: {e}");
                    continue;
                }
            };
            match serde_json::from_str::<InboundEmail>(&raw) {
                Ok(mut msg) => {
                    if msg.message_id.is_empty() {
                        msg.message_id =
                            path.file_stem().and_then(|s| s.to_str()).unwrap_or("msg").to_string();
                    }
                    // Consume: rename so a second poll never re-reads it.
                    let consumed = self.done_path(&msg);
                    if let Err(e) = std::fs::rename(&path, &consumed) {
                        eprintln!("[inbound-mail] consume rename failed {path:?}: {e}");
                        continue;
                    }
                    out.push(msg);
                }
                Err(e) => {
                    eprintln!("[inbound-mail] malformed {path:?}: {e}");
                    let _ = std::fs::rename(&path, path.with_extension("json.malformed"));
                }
            }
        }
        Ok(out)
    }

    fn quarantine(&self, msg: &InboundEmail, reason: &str) {
        let path = self.dir.join(format!("{}.json.quarantine", sanitize(&msg.message_id)));
        let body = serde_json::json!({ "reason": reason, "message": msg });
        if let Err(e) = std::fs::write(&path, body.to_string()) {
            eprintln!("[inbound-mail] quarantine write failed: {e}");
        }
    }

    fn label(&self) -> &'static str {
        "file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, from: &str) -> InboundEmail {
        InboundEmail {
            from: from.into(),
            subject: "move hq-1 to working".into(),
            body: String::new(),
            message_id: id.into(),
            in_reply_to: None,
        }
    }

    #[test]
    fn file_inbox_polls_each_message_exactly_once() {
        let dir = std::env::temp_dir().join(format!("gt-inbox-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let inbox = FileInbox::new(&dir);
        std::fs::write(dir.join("m1.json"), serde_json::to_string(&msg("m1", "ana@x.com")).unwrap())
            .unwrap();
        std::fs::write(dir.join("nota.txt"), "ignored").unwrap();

        let first = inbox.poll().expect("poll");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].from, "ana@x.com");
        // Consumed: a second poll sees nothing.
        assert!(inbox.poll().expect("poll 2").is_empty());

        // Quarantine writes the reviewable artifact.
        inbox.quarantine(&first[0], "unknown sender");
        assert!(dir.join("m1.json.quarantine").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_files_are_set_aside_not_looped() {
        let dir = std::env::temp_dir().join(format!("gt-inbox-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let inbox = FileInbox::new(&dir);
        std::fs::write(dir.join("bad.json"), "{not json").unwrap();
        assert!(inbox.poll().expect("poll").is_empty());
        assert!(dir.join("bad.json.malformed").exists());
        assert!(inbox.poll().expect("poll 2").is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
