//! Late side: `seconds_before` the end of every market, buy the outcome on whose side BTC is
//! against the strike, with a market order capped at `max_price`.

use std::collections::BTreeSet;

use lucyd_backtest::Side;
use lucyd_backtest::engine::{BookFeed, Ctx, Strategy, Subscriptions};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LateSideConfig {
    /// How long before the end to buy.
    pub seconds_before: u64,
    /// USDC per market.
    pub usd: f64,
    /// Highest price to pay.
    pub max_price: f64,
}

impl Default for LateSideConfig {
    fn default() -> Self {
        LateSideConfig {
            seconds_before: 30,
            usd: 20.0,
            max_price: 0.95,
        }
    }
}

#[derive(Default)]
pub struct LateSide {
    pub cfg: LateSideConfig,
    done: BTreeSet<String>,
}

impl LateSide {
    pub fn new(cfg: LateSideConfig) -> Self {
        LateSide {
            cfg,
            done: BTreeSet::new(),
        }
    }
}

impl Strategy for LateSide {
    fn name(&self) -> String {
        "late_side".into()
    }

    fn subscriptions(&self) -> Subscriptions {
        // The book and prices are read from the context on the timer; no need for every change.
        Subscriptions {
            book: BookFeed::Off,
            fills: false,
            prices: false,
            timer_ms: Some(1000),
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx) {
        for m in ctx.open_markets() {
            if self.done.contains(&m.address)
                || m.time_to_end_ms > self.cfg.seconds_before as i64 * 1000
            {
                continue;
            }
            let (Some(strike), Some(price)) = (m.strike, ctx.market_price(&m.address)) else {
                continue;
            };
            if price.price == strike {
                continue;
            }
            let outcome = if price.price > strike { 0 } else { 1 };
            let Some(ask) = ctx.best_ask(&m.address, outcome) else {
                continue;
            };
            if ask.price > self.cfg.max_price {
                continue;
            }
            // whole tokens, at least $1 at the cap
            let tokens = ((self.cfg.usd / ask.price).floor() as u64)
                .max((1.0 / self.cfg.max_price).ceil() as u64);
            if ctx
                .market_order(
                    &m.address,
                    outcome,
                    Side::Buy,
                    tokens,
                    Some(self.cfg.max_price),
                )
                .is_ok()
            {
                self.done.insert(m.address);
            }
        }
    }
}
