//! Restart policy with exponential backoff and crash-loop detection.
//!
//! Port of `internal/daemon/restart_tracker.go`. The Go version reads `time.Now()` and
//! persists to `daemon/restart_state.json`; here the logic is **pure** — every method takes
//! `now` (unix seconds) from the caller, so the same edge-injects-time discipline the rest of
//! the kernel uses applies (see `RealEffects`/`SystemClock`). Persistence is a repo concern
//! left to the composition root and is not modelled here.

use std::collections::HashMap;

/// Tunables for [`RestartTracker`]. Zero-valued fields fall back to the Go defaults via
/// [`RestartConfig::with_defaults`].
#[derive(Debug, Clone, Copy)]
pub struct RestartConfig {
    /// Delay before the first retry (Go default 30s).
    pub initial_backoff_secs: u64,
    /// Maximum backoff delay (Go default 10m).
    pub max_backoff_secs: u64,
    /// Backoff growth factor per retry (Go default 2.0).
    pub backoff_multiplier: f64,
    /// Window over which restarts count toward a crash loop (Go default 15m).
    pub crash_loop_window_secs: u64,
    /// Restart count within the window that flips an agent into crash-loop (Go default 5).
    pub crash_loop_count: u32,
    /// How long an agent must run without restarting before its backoff resets (Go default 30m).
    pub stability_period_secs: u64,
    /// Fixed delay for a *paused* agent (transient external limit, not a crash). Does not
    /// escalate and does not count toward the crash-loop budget (Go default 60s).
    pub pause_backoff_secs: u64,
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            initial_backoff_secs: 30,
            max_backoff_secs: 10 * 60,
            backoff_multiplier: 2.0,
            crash_loop_window_secs: 15 * 60,
            crash_loop_count: 5,
            stability_period_secs: 30 * 60,
            pause_backoff_secs: 60,
        }
    }
}

impl RestartConfig {
    /// Fill any non-positive field from the defaults (mirrors Go `withDefaults`).
    pub fn with_defaults(mut self) -> Self {
        let d = RestartConfig::default();
        if self.initial_backoff_secs == 0 {
            self.initial_backoff_secs = d.initial_backoff_secs;
        }
        if self.max_backoff_secs == 0 {
            self.max_backoff_secs = d.max_backoff_secs;
        }
        if self.backoff_multiplier <= 0.0 {
            self.backoff_multiplier = d.backoff_multiplier;
        }
        if self.crash_loop_window_secs == 0 {
            self.crash_loop_window_secs = d.crash_loop_window_secs;
        }
        if self.crash_loop_count == 0 {
            self.crash_loop_count = d.crash_loop_count;
        }
        if self.stability_period_secs == 0 {
            self.stability_period_secs = d.stability_period_secs;
        }
        if self.pause_backoff_secs == 0 {
            self.pause_backoff_secs = d.pause_backoff_secs;
        }
        self
    }
}

/// Per-agent restart bookkeeping. `last_restart`/`crash_loop_since` are `Option` so "never"
/// is distinct from the unix epoch `0` (the Go version leans on `time.Time{}` zero values;
/// `Option` avoids the `now == 0` collision). `backoff_until` is a plain threshold where `0`
/// (the past) correctly means "no backoff".
#[derive(Debug, Clone, Default)]
struct AgentRestartInfo {
    last_restart: Option<u64>,
    restart_count: u32,
    backoff_until: u64,
    crash_loop_since: Option<u64>,
}

/// Tracks restart attempts per agent to throttle runaway restart loops.
#[derive(Debug, Clone)]
pub struct RestartTracker {
    config: RestartConfig,
    agents: HashMap<String, AgentRestartInfo>,
}

impl RestartTracker {
    pub fn new(config: RestartConfig) -> Self {
        Self {
            config: config.with_defaults(),
            agents: HashMap::new(),
        }
    }

    /// May `agent` be restarted right now? `false` while in a crash loop or inside its
    /// backoff window. Unknown agents may always restart.
    pub fn can_restart(&self, agent: &str, now: u64) -> bool {
        match self.agents.get(agent) {
            None => true,
            Some(info) => info.crash_loop_since.is_none() && now >= info.backoff_until,
        }
    }

    /// Record a (re)start attempt and compute the next backoff window. Resets the agent's
    /// counters first if it had been stable longer than `stability_period_secs`.
    pub fn record_restart(&mut self, agent: &str, now: u64) {
        let config = self.config;
        let info = self.agents.entry(agent.to_string()).or_default();

        // A previously stable agent (last restart long ago) starts fresh.
        if let Some(last) = info.last_restart {
            if now.saturating_sub(last) > config.stability_period_secs {
                info.restart_count = 0;
                info.crash_loop_since = None;
            }
        }

        info.last_restart = Some(now);
        info.restart_count += 1;

        // Exponential backoff: initial, doubled once per prior restart, capped at max.
        let mut backoff = config.initial_backoff_secs as f64;
        let mut i = 1;
        while i < info.restart_count && backoff < config.max_backoff_secs as f64 {
            backoff *= config.backoff_multiplier;
            i += 1;
        }
        let backoff = (backoff as u64).min(config.max_backoff_secs);
        info.backoff_until = now + backoff;

        // Crash loop: too many restarts inside the window (Go records `now` as last_restart
        // just above, so the window test is effectively the count threshold).
        if info.restart_count >= config.crash_loop_count {
            // `last_restart` was just set to `now`, so it always falls within the window.
            let window_start = now.saturating_sub(config.crash_loop_window_secs);
            if info.last_restart.is_some_and(|last| last >= window_start) {
                info.crash_loop_since = Some(now);
            }
        }
    }

    /// Record a *pause* (transient external limit, e.g. usage-limit). Applies the fixed
    /// `pause_backoff_secs` without escalating or counting toward the crash-loop budget.
    pub fn record_pause(&mut self, agent: &str, now: u64) {
        let pause = self.config.pause_backoff_secs;
        let info = self.agents.entry(agent.to_string()).or_default();
        info.last_restart = Some(now);
        info.backoff_until = now + pause;
    }

    /// Mark a healthy run: if the agent has been stable past `stability_period_secs`, clear
    /// its backoff and crash-loop state.
    pub fn record_success(&mut self, agent: &str, now: u64) {
        let stability = self.config.stability_period_secs;
        if let Some(info) = self.agents.get_mut(agent) {
            let stale = info
                .last_restart
                .map_or(true, |last| now.saturating_sub(last) > stability);
            if stale {
                info.restart_count = 0;
                info.crash_loop_since = None;
                info.backoff_until = 0;
            }
        }
    }

    pub fn is_in_crash_loop(&self, agent: &str) -> bool {
        self.agents
            .get(agent)
            .is_some_and(|info| info.crash_loop_since.is_some())
    }

    /// Seconds until `agent` leaves its backoff window (`0` if it may restart now).
    pub fn backoff_remaining(&self, agent: &str, now: u64) -> u64 {
        self.agents
            .get(agent)
            .map(|info| info.backoff_until.saturating_sub(now))
            .unwrap_or(0)
    }

    /// Manually clear an agent's crash-loop and backoff (the `gt daemon clear-backoff` path).
    pub fn clear_crash_loop(&mut self, agent: &str) {
        if let Some(info) = self.agents.get_mut(agent) {
            info.crash_loop_since = None;
            info.restart_count = 0;
            info.backoff_until = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_agent_can_restart() {
        let rt = RestartTracker::new(RestartConfig::default());
        assert!(rt.can_restart("p1", 100));
    }

    #[test]
    fn backoff_grows_exponentially_then_caps() {
        let cfg = RestartConfig {
            initial_backoff_secs: 10,
            max_backoff_secs: 80,
            backoff_multiplier: 2.0,
            crash_loop_count: 100, // disable crash-loop for this test
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_restart("p", 0);
        assert_eq!(rt.backoff_remaining("p", 0), 10); // 1st restart: initial
        rt.record_restart("p", 0);
        assert_eq!(rt.backoff_remaining("p", 0), 20); // 2nd: *2
        rt.record_restart("p", 0);
        assert_eq!(rt.backoff_remaining("p", 0), 40); // 3rd: *2
        rt.record_restart("p", 0);
        assert_eq!(rt.backoff_remaining("p", 0), 80); // 4th: capped at max
        rt.record_restart("p", 0);
        assert_eq!(rt.backoff_remaining("p", 0), 80); // 5th: still capped
    }

    #[test]
    fn cannot_restart_during_backoff() {
        let cfg = RestartConfig {
            initial_backoff_secs: 30,
            crash_loop_count: 100,
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_restart("p", 1000);
        assert!(!rt.can_restart("p", 1000));
        assert!(!rt.can_restart("p", 1029));
        assert!(rt.can_restart("p", 1030)); // backoff_until reached
    }

    #[test]
    fn crash_loop_blocks_restart() {
        let cfg = RestartConfig {
            initial_backoff_secs: 1,
            crash_loop_count: 3,
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_restart("p", 0);
        rt.record_restart("p", 0);
        assert!(!rt.is_in_crash_loop("p"));
        rt.record_restart("p", 0); // 3rd within window → crash loop
        assert!(rt.is_in_crash_loop("p"));
        assert!(!rt.can_restart("p", 1_000_000)); // crash loop ignores backoff window
    }

    #[test]
    fn stability_resets_count() {
        let cfg = RestartConfig {
            initial_backoff_secs: 10,
            max_backoff_secs: 1000,
            stability_period_secs: 100,
            crash_loop_count: 100,
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_restart("p", 0);
        rt.record_restart("p", 5);
        assert_eq!(rt.backoff_remaining("p", 5), 20); // count=2
        // A restart long after the last one resets the count → back to initial backoff.
        rt.record_restart("p", 1000);
        assert_eq!(rt.backoff_remaining("p", 1000), 10);
    }

    #[test]
    fn clear_crash_loop_unblocks() {
        let cfg = RestartConfig {
            crash_loop_count: 1,
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_restart("p", 0);
        assert!(rt.is_in_crash_loop("p"));
        rt.clear_crash_loop("p");
        assert!(!rt.is_in_crash_loop("p"));
        assert!(rt.can_restart("p", 0));
    }

    #[test]
    fn pause_does_not_count_toward_crash_loop() {
        let cfg = RestartConfig {
            pause_backoff_secs: 60,
            crash_loop_count: 1,
            ..RestartConfig::default()
        };
        let mut rt = RestartTracker::new(cfg);
        rt.record_pause("p", 0);
        assert!(!rt.is_in_crash_loop("p"));
        assert_eq!(rt.backoff_remaining("p", 0), 60);
    }
}
