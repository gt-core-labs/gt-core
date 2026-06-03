//! `gt-wisp` — ephemeral memory with per-kind TTL + the reaper's compaction logic
//! (hq-t9vt / paso 9.C de `docs/11-cutover-roadmap.md`).
//!
//! Wisps are short-lived liveness/observability rows (heartbeat, ping, patrol, gc-report,
//! recovery, error, escalation). Each [`WispKind`] carries a default TTL; once an open wisp
//! ages past it the [`reaper`] closes it, unless it has accrued durable value (comments,
//! references, a `gt:keep` label) in which case it is preserved as a promote candidate.
//! Closed wisps age out and are purged.
//!
//! Isolation (same rule as `gt-merge`): this crate depends only on `gt-events`. It defines
//! the [`WispRepository`] port + an in-memory adapter; the Dolt adapter lives in
//! `gt-store-dolt::DoltWisp` and the scan/reap/purge job lives in `bins/gt-reaper`. Promotion
//! is classification only — the wisp→bead write is a composition-root concern, keeping this
//! crate free of a bead-store dependency.

pub mod kind;
pub mod promotion;
pub mod reaper;
mod repo;
mod wisp;

pub use kind::WispKind;
pub use promotion::{durable_value, should_promote, PromotionReason};
pub use reaper::{run, PromoteCandidate, ReapSummary};
pub use repo::{InMemoryWispRepo, WispRepository};
pub use wisp::{Wisp, WispStatus};
