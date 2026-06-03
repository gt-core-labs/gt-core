//! `gt-terminal` — terminal attach substrate for the dashboard dock terminal.
//!
//! Post-spike `hq-fe-term.0` chose **WebSocket on gt-web** as the transport (see
//! [`apps/web/docs/spike-hq-fe-term-0-transport.md`]). This crate ships the substrate the WS
//! route plugs into: an [`Attach`] port that hands the route a duplex byte-stream over a
//! live target (tmux session or fresh pty), with three adapters:
//!
//! - [`TmuxPipeAttach`] — primary. Streams pane bytes from `tmux pipe-pane -O` via a fifo
//!   and forwards keystrokes back via `tmux send-keys`. Tmux is already the multiplexer for
//!   polecat sessions, so we don't spawn a new pty per attach.
//! - [`PtyAttach`] — fallback for targets that have no tmux session (one-off command spawn,
//!   e.g. interactive scripts the dashboard wants to run ad-hoc). Wraps
//!   [`gt_login::pty::Pty`] so the pty adapter is shared with the `claude /login` driver.
//! - [`FakeAttach`] — in-memory adapter for tests: scripted reads + recorded writes.
//!
//! Scope split for the `hq-fe-term` epic:
//!
//! - **`.0`** (closed) — transport decision spike.
//! - **`.1` (this crate)** — Attach port + adapters.
//! - `.2` — WS route `GET /api/sessions/:id/term` in gt-web on top of `Attach`.
//! - `.3` — structured stream (kind tagging) layered over the raw byte stream.
//!
//! ## Ports
//!
//! [`Attach::open`] returns a boxed [`TerminalStream`]. The stream is *sync* — read/write
//! one chunk at a time — because the WS route owns the async loop (one task per attach,
//! `tokio::task::spawn_blocking` for the read side). Keeping the trait sync lets every
//! adapter forward to its underlying handle without dragging tokio into the domain crate.

pub mod fake;
pub mod module;
pub mod port;
pub mod pty_attach;
pub mod tmux_attach;

pub use fake::FakeAttach;
pub use module::TerminalModule;
pub use port::{Attach, AttachError, AttachHandle, TerminalReader, TerminalTarget, TerminalWriter};
pub use pty_attach::PtyAttach;
pub use tmux_attach::{CliTmuxAttachOps, FakeTmuxAttachOps, TmuxAttachOps, TmuxPipeAttach};
