//! `gt-notify` — the Notifier PORT (hq-mysw, Paso 9.F).
//!
//! Operator signals (a stuck worker, a quota block, a stuck merge) reach a human through a
//! single port instead of every domain importing a mail client. This crate owns three things
//! and nothing else:
//!
//! 1. [`Notification`] — the transport-agnostic message (severity + subject + body + signal).
//! 2. [`Notifier`] — the port. One sync, fire-and-forget method, mirroring the edge-effect
//!    shape of `gt_root::Effects` (`sling`/`rotate`): the core decides, the edge delivers,
//!    the caller never blocks on transport.
//! 3. [`route`] — the policy deciding which [`Signal`] warrants mail vs feed-only.
//!
//! Adapters are NOT here. The real adapter (a bead-backed mail sender) lives in `bins/gt`;
//! the [`FakeNotifier`] for tests lives here only because it has no transport dependency.
//!
//! Why the port is sync + fire-and-forget: the composition root reacts to domain events on a
//! single async loop that is the *only* writer to the audit log (`docs/01-architecture.md`).
//! Blocking that loop on SMTP/webhook latency would stall the log. The status-bead side of an
//! escalation is the durable, awaited record; the notification is a best-effort heads-up on
//! top of it.

mod fake;
mod notification;
mod route;

pub use fake::FakeNotifier;
pub use notification::{Notification, Severity, Signal};
pub use route::{route, Channel};

/// The notification port. Implemented by the real bead-backed mail adapter (`bins/gt`) and by
/// [`FakeNotifier`] (tests). Object-safe so the composition root can hold it as
/// `Arc<dyn Notifier>` in `RootConfig`, exactly like `Arc<dyn Keychain>`.
///
/// `notify` is best-effort and returns nothing: delivery errors are the adapter's concern
/// (it logs them); the caller does not block and does not branch on success. Routing — should
/// this signal be delivered at all — is decided *before* the call via [`route`].
pub trait Notifier: Send + Sync {
    fn notify(&self, notification: &Notification);
}
