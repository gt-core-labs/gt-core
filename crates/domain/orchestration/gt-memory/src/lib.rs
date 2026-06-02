//! `gt-memory` — MAPE-K **Knowledge**: machine-consume the memory corpus
//! (hq-auto.8).
//!
//! The repo keeps a file-based memory corpus (`…/memory/*.md`: `feedback` /
//! `project` / `reference` / `user` memories, plus a `MEMORY.md` index) — the
//! accumulated, hard-won lessons a human-assisted Claude reads each session. For
//! an *autonomous* loop those lessons must be machine-consumed instead of
//! human-read. This crate provides the four operations the corpus contract
//! implies:
//!
//! - **RECALL** — [`Corpus::recall`] loads the memories most relevant to a
//!   query (by description/name/body term overlap), not the whole corpus, so the
//!   planner (hq-auto.2) and workers pull only what bears on the task.
//! - **APPLY** — [`Corpus::operating_rules`] returns the `feedback` memories as
//!   *hard rules* the loop must obey; [`Corpus::by_kind`] exposes `project`
//!   state and `reference` pointers. See [`MemoryKind::is_operating_rule`].
//! - **STALENESS GUARD** — recall is point-in-time; memories reflect what was
//!   true when written. The consumer must verify a recalled memory against live
//!   code/graph before acting (documented on [`Corpus`]).
//! - **WRITE-BACK** — [`Corpus::upsert`] + [`Memory::to_markdown`] let the
//!   meta-loop (hq-auto.7) record newly-learned lessons, closing the
//!   self-improvement loop.
//!
//! Pure `std` — no YAML/serde dependency — so it links cheaply into the loop.

mod corpus;
mod memory;

pub use corpus::{Corpus, Hit};
pub use memory::{Memory, MemoryKind, ParseError};
