//! `gt-quota` — Step 6.c of `docs/08-getting-started.md`. Per-session token tracking +
//! account-block prediction (predictive rotation).
//!
//! Aligned with `docs/features/token-tracking-prediction.md` and the kernel principles
//! (`docs/01-architecture.md`):
//!
//! - **Owned events** (`QuotaEvent`), an exhaustive `Serialize`/`Deserialize` enum.
//! - **Mutable state lives inside one actor** (`actor.rs`); everyone else asks for
//!   snapshots over a channel.
//! - **Pure, sync predictor** (`expectations.rs`): `now`, the EWMA rate and the threshold
//!   travel as event data or call arguments, never as `OffsetDateTime::now_utc()` or
//!   `rand`. Replaying the log rebuilds `QuotaState` byte-for-byte.
//! - **Inverted repository** (`repo.rs`): the domain defines `QuotaRepository`;
//!   `gt-store-pg` implements it against Postgres. The in-memory impl is the safety net for
//!   tests without a DB and the first half of the Step 6.c contract.

pub mod actor;
pub mod commands;
pub mod expectations;
/// The off-by-default `axum` REST adapter (`hq-fe-api-orch.4`): the orchestration sibling of the
/// issues/rig HTTP surfaces, mapping `quota.*` to REST routes that reuse the same commands as the
/// MCP tools.
#[cfg(feature = "axum")]
pub mod http;
pub mod keychain;
pub mod module;
pub mod probe;
pub mod repo;

mod cost;
mod events;
mod state;

pub use actor::QuotaHandle;
pub use commands::{ProbeWindow, QuotaCommand, RotateAccount, SampleTokens};
pub use cost::{cost_units, Cost, ModelWeights};
pub use events::QuotaEvent;
pub use expectations::{predict, Prediction};
#[cfg(feature = "axum")]
pub use http::{quota_router, ApiDoc, QuotaApiState, WorkspaceQuota};
pub use keychain::{CredentialRecord, InMemoryKeychain, Keychain};
#[cfg(feature = "axum")]
pub use module::QuotaHttpModule;
pub use module::QuotaModule;
pub use probe::{parse_anthropic_ratelimit, RatelimitHeaders};
pub use repo::{InMemoryQuota, QuotaRepository, UsageSample};
pub use state::{
    Account, AccountQuotaStatus, AccountRegistry, AccountWindow, Ewma, QuotaState, WindowKind,
};
