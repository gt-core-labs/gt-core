//! Normalize tokens into a **cost** unit comparable across models.
//!
//! The window budget (`window_limit`) is expressed in this unit. Summing raw tokens from
//! different models is invalid: input, output and cache weigh differently and the weights
//! vary by model. Every comparison against the limit goes through `cost_units`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Per-token-category weights, in cost units per token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelWeights {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_creation: f64,
}

impl ModelWeights {
    /// Identity: 1 token = 1 cost unit, ignoring category. Useful before there is a
    /// per-model calibration (the predictor stays correct, it just loses resolution across
    /// models).
    pub const IDENTITY: Self = Self {
        input: 1.0,
        output: 1.0,
        cache_read: 1.0,
        cache_creation: 1.0,
    };
}

/// Computed cost of a sample, in the common unit. `f64` because typical weights are
/// fractions (e.g. cache_read << 1.0); only the persisted aggregate is truncated to `u64`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost(pub f64);

/// Apply the `model` weights (or `IDENTITY` if uncalibrated) and return the cost.
pub fn cost_units(
    model: &str,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_creation: u64,
    weights: &HashMap<String, ModelWeights>,
) -> Cost {
    let w = weights.get(model).cloned().unwrap_or(ModelWeights::IDENTITY);
    Cost(
        input as f64 * w.input
            + output as f64 * w.output
            + cache_read as f64 * w.cache_read
            + cache_creation as f64 * w.cache_creation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_weights_sum_all_tokens() {
        let weights = HashMap::new(); // not calibrated -> IDENTITY fallback
        let c = cost_units("opus", 100, 50, 200, 10, &weights);
        assert_eq!(c.0, 360.0);
    }

    #[test]
    fn weighted_model_discounts_cache_reads() {
        let mut weights = HashMap::new();
        weights.insert(
            "opus".to_string(),
            ModelWeights {
                input: 1.0,
                output: 5.0,
                cache_read: 0.1,
                cache_creation: 1.25,
            },
        );
        // 100*1 + 50*5 + 200*0.1 + 10*1.25 = 100 + 250 + 20 + 12.5 = 382.5
        let c = cost_units("opus", 100, 50, 200, 10, &weights);
        assert!((c.0 - 382.5).abs() < 1e-9);
    }

    #[test]
    fn unknown_model_falls_back_to_identity() {
        let mut weights = HashMap::new();
        weights.insert("opus".to_string(), ModelWeights::IDENTITY);
        let c = cost_units("haiku", 10, 20, 0, 0, &weights);
        assert_eq!(c.0, 30.0);
    }
}
