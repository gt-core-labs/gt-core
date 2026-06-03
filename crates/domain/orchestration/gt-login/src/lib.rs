//! `gt-login` — `hq-fe-auth.1` PTY driver for `claude /login`.
//!
//! Account-login bridge that runs the Anthropic CLI's OAuth flow under a pseudo-terminal so
//! the dashboard can drive it without a human terminal: spawn `claude /login`, capture the
//! authorization URL the CLI prints, hand it to the UI, then write the user-pasted token
//! back over stdin and wait for the CLI to confirm.
//!
//! Scope split (epic `hq-fe-auth`):
//!
//! - **`.1` (this crate)** — the driver: process spawn, URL regex, state machine, token
//!   writeback.
//! - `.2` — `POST /api/quota/accounts/:n/login` wiring on top of [`LoginDriver`].
//! - `.3` — SSE `quota.login_*` kinds mapped from [`LoginEvent`].
//! - `.4` — soft timeout (HTTP-side watchdog) + panic guard.
//! - `.5` — URL-phase HARD-kill via [`pty::PtyKiller`] +
//!   [`driver::LoginDriver::run_with_timeouts`] + zombie-reap on
//!   [`pty::PortablePty`]'s `Drop`.
//!
//! The driver is split into three layers so the state machine + URL extraction can be
//! tested without touching a real process:
//!
//! - [`pty`] — `Pty` port + adapters ([`pty::PortablePty`] for prod, [`pty::FakePty`] for
//!   tests).
//! - [`state`] — pure state machine ([`LoginState`]) and stream parser
//!   ([`state::extract_url`]).
//! - [`driver`] — the orchestrator that spawns the child, ingests its output through the
//!   `Pty` port, and emits [`LoginEvent`]s as state transitions happen.

pub mod driver;
pub mod events;
pub mod pty;
pub mod state;

pub use driver::{LoginDriver, LoginHandle, LoginOutcome, LoginTimeouts};
pub use events::{LoginEvent, LoginFailure};
pub use pty::{FakePty, PortablePty, Pty, PtyChild, PtyKiller};
pub use state::{extract_url, LoginState};
