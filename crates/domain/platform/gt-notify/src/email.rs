//! `EmailTransport` — the outbound-email seam (hq-f24599, epic hq-56b5ee).
//!
//! Producers never talk to a mail server: they enqueue rows into the
//! `email_outbox` (gt-store-pg) and the drain daemon pushes due rows through
//! THIS trait (ADR hq-423a4b D8). Swapping engines is a config change, never a
//! producer change:
//!
//! - [`LogTransport`] — the default: records the send in memory and logs it,
//!   so the whole outbox pipeline (schedule → due → drain → status) is
//!   exercisable with no external dependency.
//! - [`SmtpTransport`] — real delivery via `lettre` (hq-8d848e, epic
//!   hq-9c199d): submission to the configured relay/provider
//!   ([`SmtpConfig::from_env`]), `smtps://` = implicit TLS (465), `smtp://` =
//!   STARTTLS (587).
//!
//! Selection: [`transport_from_env`] reads `GT_EMAIL_TRANSPORT` (`log`
//! default, `smtp` for real delivery).

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

/// Real SMTP delivery via `lettre` (hq-8d848e). Selected with
/// `GT_EMAIL_TRANSPORT=smtp` + `GT_SMTP_URL`/`GT_SMTP_FROM`. Blocking by
/// design (the drain daemon owns concurrency); any config or delivery error
/// returns `Err(reason)` so the outbox marks the row retry/failed — nothing
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

/// Split a `smtp://host[:port]` / `smtps://host[:port]` URL into
/// `(implicit_tls, host, port)`. Errors name the env var so a misconfigured
/// deploy is diagnosable straight from the outbox row.
fn parse_smtp_url(url: &str) -> Result<(bool, &str, Option<u16>), String> {
    let (smtps, rest) = if let Some(r) = url.strip_prefix("smtps://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("smtp://") {
        (false, r)
    } else {
        return Err(format!("GT_SMTP_URL must start with smtp:// or smtps://, got {url:?}"));
    };
    let rest = rest.trim_end_matches('/');
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => {
            let port =
                p.parse::<u16>().map_err(|e| format!("invalid port in GT_SMTP_URL {url:?}: {e}"))?;
            (h, Some(port))
        }
        None => (rest, None),
    };
    if host.is_empty() {
        return Err(format!("GT_SMTP_URL has no host: {url:?}"));
    }
    Ok((smtps, host, port))
}

impl EmailTransport for SmtpTransport {
    fn send(&self, msg: &EmailMessage) -> Result<(), String> {
        use lettre::transport::smtp::authentication::Credentials;
        use lettre::{Message, Transport};

        let email = Message::builder()
            .from(
                self.config
                    .from
                    .parse()
                    .map_err(|e| format!("invalid GT_SMTP_FROM {:?}: {e}", self.config.from))?,
            )
            .to(msg.to.parse().map_err(|e| format!("invalid recipient {:?}: {e}", msg.to))?)
            .subject(msg.subject.clone())
            .body(msg.body.clone())
            .map_err(|e| format!("message build failed: {e}"))?;

        let (smtps, host, port) = parse_smtp_url(&self.config.url)?;
        // smtps:// = implicit TLS (465); smtp:// = mandatory STARTTLS (587).
        // Plaintext submission is deliberately not offered.
        let mut builder = if smtps {
            lettre::SmtpTransport::relay(host)
        } else {
            lettre::SmtpTransport::starttls_relay(host)
        }
        .map_err(|e| format!("smtp relay setup failed for {host}: {e}"))?;
        if let Some(port) = port {
            builder = builder.port(port);
        }
        if let (Some(user), Some(pass)) = (&self.config.user, &self.config.pass) {
            builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
        }
        builder
            .build()
            .send(&email)
            .map(|_| ())
            .map_err(|e| format!("smtp delivery to {} via {host} failed: {e}", msg.to))
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
    fn smtp_url_parses_starttls_implicit_tls_and_ports() {
        assert_eq!(parse_smtp_url("smtp://mail.example.com:587").unwrap(), (
            false,
            "mail.example.com",
            Some(587)
        ));
        assert_eq!(parse_smtp_url("smtps://mail.example.com:465").unwrap(), (
            true,
            "mail.example.com",
            Some(465)
        ));
        // No port -> lettre's scheme default applies.
        assert_eq!(parse_smtp_url("smtps://mail.example.com").unwrap(), (
            true,
            "mail.example.com",
            None
        ));
    }

    #[test]
    fn smtp_url_misconfig_fails_loud_naming_the_env_var() {
        for bad in ["http://mail:25", "smtp://", "smtp://mail:notaport", "mail.example.com"] {
            let err = parse_smtp_url(bad).expect_err(bad);
            assert!(err.contains("GT_SMTP_URL"), "{bad} -> {err}");
        }
    }

    #[test]
    fn smtp_send_rejects_bad_addresses_and_urls_before_any_network_io() {
        let t = |url: &str, from: &str| {
            SmtpTransport::new(SmtpConfig {
                url: url.into(),
                from: from.into(),
                user: None,
                pass: None,
            })
        };
        assert_eq!(t("smtp://mail:587", "gt@example.com").label(), "smtp");
        assert_eq!(t("smtp://mail:587", "gt@example.com").config().from, "gt@example.com");

        let err = t("smtp://mail:587", "not-an-address").send(&msg()).expect_err("bad from");
        assert!(err.contains("GT_SMTP_FROM"), "{err}");

        let mut bad_to = msg();
        bad_to.to = "nope".into();
        let err = t("smtp://mail:587", "gt@example.com").send(&bad_to).expect_err("bad to");
        assert!(err.contains("recipient"), "{err}");

        let err = t("ftp://mail:21", "gt@example.com").send(&msg()).expect_err("bad scheme");
        assert!(err.contains("GT_SMTP_URL"), "{err}");
    }
}
