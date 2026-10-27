//! Execution parameters of a run and the seeded random source.

use serde::{Deserialize, Serialize};

/// What a drop of the book level under a resting order means for its place in the queue, when
/// no fill explains the drop (the orders were cancelled).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueModel {
    /// The cancelled orders stood behind ours: our place does not move.
    #[default]
    Pessimistic,
    /// Cancellations are spread over the level: the queue ahead shrinks by the same share as the
    /// level.
    Proportional,
}

/// How orders are executed. Every field has a default; JSON with any subset is accepted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecParams {
    /// USDC on the account at the start.
    pub initial_usdc: f64,
    /// From the strategy's decision to the order reaching the book, ms.
    pub submit_latency_ms: u64,
    /// From the strategy's decision to the cancel taking effect, ms.
    pub cancel_latency_ms: u64,
    /// Extra random delay added to every order and cancel, uniform in 0..=jitter, ms.
    pub latency_jitter_ms: u64,
    /// How cancellations at our price move our place in the queue.
    pub queue_model: QueueModel,
    /// Trading fee on the notional of every own fill, basis points (0 now on Lucyd).
    pub fee_bps: u32,
    /// Fee on winning redemptions, basis points (the contract's `feeBps`, 0 now).
    pub redeem_fee_bps: u32,
    /// Chance that the on-chain settlement of an own fill fails: the fill is undone, USDC and
    /// tokens go back.
    pub revert_probability: f64,
    /// Seed of every random choice: jitter and reverts.
    pub seed: u64,
    /// Last share of the time range used as the out-of-sample check; 0 = no split.
    pub validation_fraction: f64,
    /// Step of the equity curve, ms.
    pub equity_interval_ms: u64,
    /// Fewer own fills than this get a warning in the report.
    pub min_fills_warning: u32,
    /// Step of the equity returns behind Sharpe and Sortino, ms (default one hour).
    pub risk_interval_ms: u64,
    /// Scale Sharpe and Sortino to a year: × √(intervals in 365 days). Off by default: the
    /// report gives them per `risk_interval_ms`.
    pub annualize: bool,
}

impl Default for ExecParams {
    fn default() -> Self {
        ExecParams {
            initial_usdc: 1000.0,
            submit_latency_ms: 100,
            cancel_latency_ms: 100,
            latency_jitter_ms: 0,
            queue_model: QueueModel::Pessimistic,
            fee_bps: 0,
            redeem_fee_bps: 0,
            revert_probability: 0.0,
            seed: 0,
            validation_fraction: 0.3,
            equity_interval_ms: 60_000,
            min_fills_warning: 30,
            risk_interval_ms: 3_600_000,
            annualize: false,
        }
    }
}

impl ExecParams {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        let bad = |m: &str| {
            Err(crate::Error::InvalidRequest(format!(
                "execution params: {m}"
            )))
        };
        if !(self.initial_usdc.is_finite() && self.initial_usdc >= 0.0) {
            return bad("initial_usdc must be 0 or more");
        }
        if !(0.0..=1.0).contains(&self.revert_probability) {
            return bad("revert_probability must be within 0..1");
        }
        if !(0.0..1.0).contains(&self.validation_fraction) {
            return bad("validation_fraction must be within 0..1 (0 = no split)");
        }
        if self.equity_interval_ms == 0 {
            return bad("equity_interval_ms must be above 0");
        }
        if self.risk_interval_ms == 0 {
            return bad("risk_interval_ms must be above 0");
        }
        if self.fee_bps > 10_000 || self.redeem_fee_bps > 10_000 {
            return bad("fees are basis points, at most 10000");
        }
        Ok(())
    }
}

/// SplitMix64: small, fast, and the same numbers on every platform and version.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in 0..=max.
    pub fn up_to(&mut self, max: u64) -> u64 {
        if max == 0 {
            0
        } else {
            self.next_u64() % (max + 1)
        }
    }

    /// Uniform in [0, 1).
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
