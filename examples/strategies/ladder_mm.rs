//! Ladder market maker: around the book mid of up, `levels` bids (buys of up) and `levels` asks
//! (buys of down at 1 − price, as Lucyd's own market maker quotes) a few ticks apart. Requotes
//! when the mid moves, stops before the end, skips one side when the position is at its limit.

use std::collections::BTreeMap;

use lucyd_backtest::Side;
use lucyd_backtest::engine::{BookFeed, Ctx, Strategy, Subscriptions};
use serde::{Deserialize, Serialize};

const TICK: f64 = 0.0025;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LadderMmConfig {
    /// Levels on each side.
    pub levels: u32,
    /// Tokens per level (raised to $1 of notional where needed).
    pub tokens: u64,
    /// Distance of the first level from the mid, ticks.
    pub half_spread_ticks: u32,
    /// Between levels, ticks.
    pub step_ticks: u32,
    /// Requote when the mid moved this many ticks.
    pub requote_ticks: u32,
    /// Stop quoting and cancel this long before the end.
    pub stop_before_s: u64,
    /// Largest net position (up minus down tokens) per market.
    pub max_position: f64,
}

impl Default for LadderMmConfig {
    fn default() -> Self {
        LadderMmConfig {
            levels: 3,
            tokens: 20,
            half_spread_ticks: 4,
            step_ticks: 2,
            requote_ticks: 2,
            stop_before_s: 20,
            max_position: 200.0,
        }
    }
}

#[derive(Default)]
pub struct LadderMm {
    pub cfg: LadderMmConfig,
    /// Mid (ticks) each market was last quoted at.
    quoted: BTreeMap<String, u32>,
}

impl LadderMm {
    pub fn new(cfg: LadderMmConfig) -> Self {
        LadderMm {
            cfg,
            quoted: BTreeMap::new(),
        }
    }

    fn cancel_all(&mut self, ctx: &mut Ctx, market: &str) {
        for o in ctx.open_orders(Some(market)) {
            let _ = ctx.cancel(o.id);
        }
        self.quoted.remove(market);
    }

    fn quote(&mut self, ctx: &mut Ctx, market: &str) {
        let (Some(bid), Some(ask)) = (ctx.best_bid(market, 0), ctx.best_ask(market, 0)) else {
            return;
        };
        let mid = ((bid.price + ask.price) / 2.0 / TICK).round() as u32;
        if let Some(&last) = self.quoted.get(market)
            && last.abs_diff(mid) < self.cfg.requote_ticks
        {
            return;
        }
        for o in ctx.open_orders(Some(market)) {
            let _ = ctx.cancel(o.id);
        }
        self.quoted.insert(market.to_string(), mid);
        let net = ctx.position(market, 0) - ctx.position(market, 1);
        for i in 0..self.cfg.levels {
            let off = self.cfg.half_spread_ticks + i * self.cfg.step_ticks;
            // bid of up
            if net < self.cfg.max_position && mid > off {
                let t = mid - off;
                let _ = ctx.limit(market, 0, Side::Buy, t as f64 * TICK, self.size(t));
            }
            // ask of up = buy of down at 1 − price
            if net > -self.cfg.max_position && mid + off < 400 {
                let t = 400 - (mid + off);
                let _ = ctx.limit(market, 1, Side::Buy, t as f64 * TICK, self.size(t));
            }
        }
    }

    /// Tokens at `ticks`, at least $1 of notional.
    fn size(&self, ticks: u32) -> u64 {
        self.cfg.tokens.max(400u64.div_ceil(ticks as u64))
    }
}

impl Strategy for LadderMm {
    fn name(&self) -> String {
        "ladder_mm".into()
    }

    fn subscriptions(&self) -> Subscriptions {
        Subscriptions {
            book: BookFeed::Throttle(500),
            fills: false,
            prices: false,
            timer_ms: Some(1000),
        }
    }

    fn on_book(&mut self, ctx: &mut Ctx, market: &str) {
        let Some(m) = ctx.market(market) else {
            return;
        };
        if m.open && m.time_to_end_ms > self.cfg.stop_before_s as i64 * 1000 {
            self.quote(ctx, market);
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx) {
        for m in ctx.open_markets() {
            if m.time_to_end_ms <= self.cfg.stop_before_s as i64 * 1000 {
                if self.quoted.contains_key(&m.address) {
                    self.cancel_all(ctx, &m.address);
                }
            } else {
                self.quote(ctx, &m.address);
            }
        }
    }
}
