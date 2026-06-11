//! `EmailTransport` — the outbound-email seam (hq-f24599, epic hq-56b5ee).
//!
//! The SMTP mail server is PENDING; the platform must not block on it (ADR
//! hq-423a4b D8). Producers never talk to a mail server: they enqueue rows into
//! the `email_outbox` (gt-store-pg) and the drain daemon pushes due rows through
//! THIS trait. Swapping engines is a config change, never a producer change:
//!
//! - [`LogTransport`] — the shipping default: records the send in memory and
//!   logs it, so the whole outbox pipeline (schedule → due → drain → status) is
//!   exercisable today.
//! - [`SmtpTransport`] — the documented, config-selected seam the real server
//!   plugs into. It parses its config now ([`SmtpConfig::from_env`]) and fails
//!   sends with an explicit "server pending" error until the SMTP
//!   implementation lands here (one `send` body; nothing else changes).
//!
//! Selection: [`transport_from_env`] reads `GT_EMAIL_TRANSPORT` (`log` default,
//! `smtp` once the server exists).

use std::sync::Mutex;

/// One outbound email, transport-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailMessage {
    /// Recipient address.
    pub to: String,
    /// Subject line.
    pub subject: String,
    /// Plain-text body (templating happens upstream of the transport).
    pub body: String,
}

/// The outbound transport seam. Sync + blocking by design: the drain daemon
/// owns concurrency (it can `spawn_blocking`), and the port stays free of any
/// async-runtime or mail-crate dependency — the same discipline as
/// [`Notifier`](crate::Notifier).
pub trait EmailTransport: Send + Sync {
    /// Deliver one message. `Err(reason)` marks the outbox row failed/retry.
    fn send(&self, msg: &EmailMessage) -> Result<(), String>;
    /// A short label for boot logs (`log`, `smtp`).
    fn label(&self) -> &'static str;
}

/// The shipping default: log + record. Keeps the pipeline fully exercisable
/// (and assertable in tests via [`sent`](Self::sent)) while the mail server is
/// pending.
#[derive(Default)]
pub struct LogTransport {
    sent: Mutex<Vec<EmailMessage>>,
}

impl LogTransport {
    /// A fresh recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every message "delivered" so far, in order.
    pub fn sent(&self) -> Vec<EmailMessage> {
        self.sent.lock().expect("log transport poisoned").clone()
    }
}

impl EmailTransport for LogTransport {
    fn send(&self, msg: &EmailMessage) -> Result<(), String> {
        eprintln!(
            "[email-outbox] LOG transport delivered: to={} subject={:?} ({} bytes)",
            msg.to,
            msg.subject,
            msg.body.len()
        );
        self.sent.lock().expect("log transport poisoned").push(msg.clone());
        Ok(())
    }

    fn label(&self) -> &'static str {
        "log"
    }
}

/// SMTP connection settings, parsed from env now so the seam is config-complete
/// before the server exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmtpConfig {
    /// `smtp://host:port` (or `smtps://`) — `GT_SMTP_URL`.
    pub url: String,
    /// The From: address — `GT_SMTP_FROM`.
    pub from: String,
    /// Optional `GT_SMTP_USER` / `GT_SMTP_PASS` credentials.
    pub user: Option<String>,
    pub pass: Option<String>,
}

impl SmtpConfig {
    /// Read the SMTP settings from the environment; `None` until both
    /// `GT_SMTP_URL` and `GT_SMTP_FROM` are set.
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("GT_SMTP_URL").ok()?;
        let from = std::env::var("GT_SMTP_FROM").ok()?;
        Some(Self {
            url,
            from,
            user: std::env::var("GT_SMTP_USER").ok(),
            pass: std::env::var("GT_SMTP_PASS").ok(),
        })
    }
}

/// The real-server seam. Wired and selectable TODAY (`GT_EMAIL_TRANSPORT=smtp`
/// + `GT_SMTP_URL`/`GT_SMTP_FROM`); the `send` body is the single place the
/// actual SMTP client (e.g. `lettre`) lands once the mail server exists. Until
/// then a send fails loud — the outbox marks the row retry/failed and nothing
/// upstream blocks or breaks.
pub struct SmtpTransport {
    config: SmtpConfig,
}

impl SmtpTransport {
    /// Wrap parsed SMTP settings.
    pub fn new(config: SmtpConfig) -> Self {
        Self { config }
    }

    /// The parsed settings (boot-log / diagnostics).
    pub fn config(&self) -> &SmtpConfig {
        &self.config
    }
}

impl EmailTransport for SmtpTransport {
    fn send(&self, msg: &EmailMessage) -> Result<(), String> {
        // SMTP server PENDING (hq-f24599): this is deliberately the only line to
        // replace with the real client call when it exists.
        Err(format!(
            "smtp transport configured ({} from {}) but the mail server is pending — \
             cannot deliver to {} yet",
            self.config.url, self.config.from, msg.to
        ))
    }

    fn label(&self) -> &'static str {
        "smtp"
    }
}

/// Select the transport from `GT_EMAIL_TRANSPORT` (`log` default): `smtp` picks
/// [`SmtpTransport`] when its config is complete, falling back (loudly) to the
/// log transport when it is not — a misconfigured deploy keeps draining instead
/// of wedging the outbox.
pub fn transport_from_env() -> std::sync::Arc<dyn EmailTransport> {
    match std::env::var("GT_EMAIL_TRANSPORT").as_deref() {
        Ok("smtp") => match SmtpConfig::from_env() {
            Some(cfg) => std::sync::Arc::new(SmtpTransport::new(cfg)),
            None => {
                eprintln!(
                    "[email-outbox] GT_EMAIL_TRANSPORT=smtp but GT_SMTP_URL/GT_SMTP_FROM \
                     incomplete — falling back to the log transport"
                );
                std::sync::Arc::new(LogTransport::new())
            }
        },
        _ => std::sync::Arc::new(LogTransport::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg() -> EmailMessage {
        EmailMessage {
            to: "ops@example.com".into(),
            subject: "reporte".into(),
            body: "hola".into(),
        }
    }

    #[test]
    fn log_transport_records_sends_in_order() {
        let t = LogTransport::new();
        assert!(t.send(&msg()).is_ok());
        let mut second = msg();
        second.subject = "otro".into();
        assert!(t.send(&second).is_ok());
        let sent = t.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].subject, "reporte");
        assert_eq!(sent[1].subject, "otro");
        assert_eq!(t.label(), "log");
    }

    #[test]
    fn smtp_transport_is_a_wired_seam_that_fails_loud_until_the_server_exists() {
        let t = SmtpTransport::new(SmtpConfig {
            url: "smtp://mail:587".into(),
            from: "gt@example.com".into(),
            user: None,
            pass: None,
        });
        let err = t.send(&msg()).expect_err("pending server must fail loud");
        assert!(err.contains("pending"), "{err}");
        assert_eq!(t.label(), "smtp");
        assert_eq!(t.config().from, "gt@example.com");
    }
}
