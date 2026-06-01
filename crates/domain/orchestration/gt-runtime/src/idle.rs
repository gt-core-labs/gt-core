//! Idle teardown for the [`RootRegistry`] (`hq-mt-routing.4`).
//!
//! A long-lived process accumulates tenants: every workspace ever resolved keeps
//! its composed [`RootHandle`](crate::RootHandle) — and that handle's actor stack
//! — resident. Most are cold most of the time. This module reaps them: a
//! background loop periodically calls
//! [`RootRegistry::reap_idle`](crate::RootRegistry::reap_idle), evicting the
//! handles no request has touched within the idle window so memory tracks active
//! tenants, not the catalog. A reaped workspace re-hydrates lazily on its next
//! request, so eviction is invisible to callers.
//!
//! The window is [`idle_from_env`] (`GT_WORKSPACE_IDLE_SECS`, default 30 min) and
//! the sweep cadence is chosen by the caller of
//! [`spawn_reaper`](crate::RootRegistry::spawn_reaper).

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::RootRegistry;

/// Default idle window before a workspace is eligible for teardown: 30 minutes.
pub const DEFAULT_IDLE_SECS: u64 = 1800;

/// The configured idle window, from `GT_WORKSPACE_IDLE_SECS` (seconds) or
/// [`DEFAULT_IDLE_SECS`] when unset or unparseable.
pub fn idle_from_env() -> Duration {
    let secs = std::env::var("GT_WORKSPACE_IDLE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDLE_SECS);
    Duration::from_secs(secs)
}

impl RootRegistry {
    /// Spawn the background reaper: every `tick`, evict workspaces idle for at
    /// least `idle`. Returns the task's [`JoinHandle`] so the host can abort it on
    /// shutdown; dropping the handle leaves the loop running for the process life.
    ///
    /// Spawning belongs here, not the kernel: `gt-runtime` is the orchestration
    /// tier, the only one allowed to `tokio::spawn` (docs/03).
    pub fn spawn_reaper(self: Arc<Self>, idle: Duration, tick: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick);
            // The immediate first tick fires a sweep at once; harmless on an empty
            // registry and keeps the cadence steady thereafter.
            loop {
                interval.tick().await;
                self.reap_idle(idle).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test for the env var: a process-global, so set/read/remove must not
    // interleave with a sibling test mutating the same name.
    #[test]
    fn idle_from_env_default_and_override() {
        std::env::remove_var("GT_WORKSPACE_IDLE_SECS");
        assert_eq!(idle_from_env(), Duration::from_secs(DEFAULT_IDLE_SECS));

        std::env::set_var("GT_WORKSPACE_IDLE_SECS", "5");
        assert_eq!(idle_from_env(), Duration::from_secs(5));

        std::env::set_var("GT_WORKSPACE_IDLE_SECS", "not-a-number");
        assert_eq!(
            idle_from_env(),
            Duration::from_secs(DEFAULT_IDLE_SECS),
            "unparseable value falls back to default"
        );

        std::env::remove_var("GT_WORKSPACE_IDLE_SECS");
    }
}
