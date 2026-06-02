//! Pure, sync account-block predictor.
//!
//! Every input is **data**: the clock, the window timestamps, the consumption and the EWMA.
//! The predictor never touches `OffsetDateTime::now_utc()` or `rand`. That makes it
//! replay-able (same log -> same `Prediction`, byte-for-byte) — the Step 3 gate.

use crate::state::AccountWindow;

/// The predictor's verdict.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prediction {
    /// Cost consumed up to `now_secs` (rounded up).
    pub consumed: u64,
    /// `limit - consumed` (clamped at zero).
    pub remaining: u64,
    /// Rate per minute (EWMA in cost units).
    pub rate_per_min: f64,
    /// If `Some`, how many seconds the predictor estimates until consumption exhausts the
    /// window **within** the live window.
    /// `None` if:
    ///   - the rate is 0 (no block is projected),
    ///   - the window resets before consumption runs out,
    ///   - it is already exhausted and only the reset is pending.
    pub eta_to_block_secs: Option<u64>,
    /// `true` if `eta_to_block_secs` falls under `threshold_secs` and there is still margin
    /// within the window → the rotation chain must fire.
    pub should_predict_block: bool,
}

/// Compute the verdict. `rate_per_min` comes from [`crate::Ewma`] (the actor aggregates it)
/// or any external estimator; here it is just data. `threshold_secs` defines when predictive
/// rotation fires.
pub fn predict(
    window: &AccountWindow,
    now_secs: u64,
    rate_per_min: f64,
    threshold_secs: u64,
) -> Prediction {
    let consumed_raw = window.consumed.ceil().max(0.0) as u64;
    let consumed = consumed_raw.min(window.limit);
    let remaining = window.limit.saturating_sub(consumed);

    let eta_to_block_secs = if remaining == 0 {
        // Already exhausted: not a prediction, a fact. Reactive rotation handles it.
        None
    } else if rate_per_min <= 0.0 {
        None
    } else {
        let minutes_to_block = remaining as f64 / rate_per_min;
        let secs = (minutes_to_block * 60.0).ceil();
        if secs.is_finite() && secs >= 0.0 && secs <= u64::MAX as f64 {
            Some(secs as u64)
        } else {
            None
        }
    };

    let should_predict_block = match eta_to_block_secs {
        Some(eta) => {
            // Only predict if the block falls *within* the live window; if the reset comes
            // first, there is nothing to rotate.
            let projected_block_at = now_secs.saturating_add(eta);
            eta <= threshold_secs && projected_block_at < window.resets_at_secs
        }
        None => false,
    };

    Prediction {
        consumed,
        remaining,
        rate_per_min,
        eta_to_block_secs,
        should_predict_block,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::WindowKind;

    fn window(limit: u64, consumed: f64, resets_at: u64) -> AccountWindow {
        AccountWindow {
            kind: WindowKind::Rolling5h,
            limit,
            started_at_secs: 0,
            resets_at_secs: resets_at,
            consumed,
        }
    }

    #[test]
    fn no_rate_means_no_eta() {
        let p = predict(&window(1000, 0.0, 18_000), 0, 0.0, 900);
        assert_eq!(p.eta_to_block_secs, None);
        assert!(!p.should_predict_block);
        assert_eq!(p.remaining, 1000);
    }

    #[test]
    fn eta_below_threshold_within_window_triggers() {
        // limit=1000, consumed=500, remaining=500. rate=100/min → 5 min to block.
        // resets in 1h, threshold 15min → must fire.
        let p = predict(&window(1000, 500.0, 3600), 0, 100.0, 900);
        assert_eq!(p.eta_to_block_secs, Some(300));
        assert!(p.should_predict_block);
    }

    #[test]
    fn eta_beyond_window_reset_does_not_trigger() {
        // limit=1000, consumed=500, rate=1/min → 500 min ~ 30 000 s. The window resets in
        // 1h: the projected block falls after the reset → no risk.
        let p = predict(&window(1000, 500.0, 3600), 0, 1.0, 900);
        assert_eq!(p.eta_to_block_secs, Some(30_000));
        assert!(!p.should_predict_block, "reset comes before the block");
    }

    #[test]
    fn already_exhausted_window_returns_none() {
        let p = predict(&window(1000, 1000.0, 3600), 0, 50.0, 900);
        assert_eq!(p.remaining, 0);
        assert_eq!(p.eta_to_block_secs, None);
        assert!(!p.should_predict_block, "already exhausted -> reactive, not predictive");
    }

    #[test]
    fn eta_above_threshold_does_not_trigger() {
        // 30 min to block, threshold 15 min → does not fire yet.
        let p = predict(&window(1000, 0.0, 7200), 0, 33.33, 900);
        let eta = p.eta_to_block_secs.unwrap();
        assert!(eta > 900);
        assert!(!p.should_predict_block);
    }

    #[test]
    fn predict_is_pure_same_inputs_same_output() {
        // Calling it twice in a row with the same data yields the same result.
        let w = window(1000, 250.0, 18_000);
        let p1 = predict(&w, 100, 50.0, 900);
        let p2 = predict(&w, 100, 50.0, 900);
        assert_eq!(p1, p2);
    }
}
