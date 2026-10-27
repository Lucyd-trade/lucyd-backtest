//! What a strategy implements and what it sees: the [`Strategy`] trait, its [`Ctx`], order
//! updates.

use serde::{Deserialize, Serialize};

use crate::book::{BookSide, OrderBook, TICK, price_ticks, tick_price};
use crate::fills::Fill;
use crate::request::Period;
use crate::trades::Side;

use super::world::{Order, OrderState, TICKS, UNIT, World, canonical, tokens, usd};

/// Id of an own order, unique within a run, from 1.
pub type OrderId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// How long an order lives.
pub enum TimeInForce {
    /// Rests in the book until filled, cancelled or the market ends.
    Gtc,
    /// Fill and kill: takes what it can on arrival, the rest is cancelled.
    Fak,
}

/// Whether an own fill took liquidity (the order crossed on arrival) or was taken (it rested).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Liquidity {
    /// Rested in the book and was taken.
    Maker,
    /// Took liquidity on arrival.
    Taker,
}

/// On-chain path of a match, when the other order is known (fills of resting orders against a
/// recorded taker). Fills against the recorded book do not know the other order's form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchType {
    /// Same outcome, opposite sides: tokens change hands.
    Direct,
    /// Two buys of opposite outcomes: a new pair of tokens is minted.
    Mint,
    /// Two sells of opposite outcomes: the pair is merged back into USDC.
    Merge,
}

/// Market result as the chain reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resolution {
    /// 0 = up, 1 = down; None when voided.
    pub winner: Option<u8>,
    /// The market got no oracle price in time: every token pays 0.5.
    pub voided: bool,
    /// Final price of the underlying, USD.
    pub resolved_price: Option<f64>,
}

/// A Pyth price as the strategy sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceView {
    /// USD.
    pub price: f64,
    /// Pyth confidence interval, USD.
    pub conf: f64,
    /// Pyth publish time, ms.
    pub publish_ms: i64,
    /// When it was received, ms (≤ now).
    pub ts_ms: i64,
}

/// News about an own order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderUpdate {
    /// Which order.
    pub order_id: OrderId,
    /// Market address.
    pub market: String,
    /// When it happened, unix ms.
    pub ts_ms: i64,
    #[serde(flatten)]
    /// What happened.
    pub kind: OrderUpdateKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
/// What happened to an own order; the JSON `status` field names the variant.
pub enum OrderUpdateKind {
    /// In the book (or processed, for FAK).
    Accepted,
    /// Not taken: `market_not_open`, `order_notional_below_minimum`, `insufficient_balance`,
    /// `insufficient_tokens`.
    Rejected {
        /// Why.
        reason: String,
    },
    /// Part or all of the order executed. `price` and `quantity` in the order's outcome.
    Filled {
        /// 0 = up, 1 = down.
        outcome: u8,
        /// Buy or sell.
        side: Side,
        /// Execution price.
        price: f64,
        /// Tokens filled now.
        quantity: f64,
        /// Tokens left on the order.
        remaining: f64,
        /// Maker or taker.
        liquidity: Liquidity,
        /// How the CLOB settled it, if known.
        match_type: Option<MatchType>,
        /// USDC.
        fee: f64,
    },
    /// The settlement of the fill just reported failed: USDC and tokens are back.
    SettlementFailed {
        /// Price of the undone fill.
        price: f64,
        /// Tokens of the undone fill.
        quantity: f64,
    },
    /// Left the book: `cancelled`, `fak_remainder`, `self_trade`, `market_closed`.
    Cancelled {
        /// Tokens left unfilled.
        remaining: f64,
        /// Why it left.
        reason: String,
    },
    /// The cancel missed: `unknown_order` (it reached the book before the order), `order_done`.
    CancelFailed {
        /// Why.
        reason: String,
    },
}

/// How the strategy wants its book updates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookFeed {
    /// No `on_book` calls; the book is still in the context.
    Off,
    /// On every recorded change.
    #[default]
    EveryChange,
    /// At most once per this many ms per market, with the latest book.
    Throttle(u64),
}

/// What the strategy receives besides own order updates, market opens, closes and resolutions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscriptions {
    /// How often `on_book` is called.
    pub book: BookFeed,
    /// Recorded fills of other participants (`on_fill`).
    pub fills: bool,
    /// Pyth ticks (`on_price`).
    pub prices: bool,
    /// `on_timer` every this many ms from the start of the run.
    pub timer_ms: Option<u64>,
}

impl Default for Subscriptions {
    fn default() -> Self {
        Subscriptions {
            book: BookFeed::EveryChange,
            fills: true,
            prices: true,
            timer_ms: None,
        }
    }
}

/// A trading strategy. Every handler has an empty default; implement the ones you need. Orders
/// and cancels placed through the context leave after the handler returns and reach the book
/// after the configured latency.
#[allow(unused_variables)]
pub trait Strategy {
    /// Name in the report.
    fn name(&self) -> String {
        "strategy".into()
    }
    /// What the engine sends; by default nothing but order updates.
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions::default()
    }
    /// Once, before the first event.
    fn on_start(&mut self, ctx: &mut Ctx) {}
    /// A market started trading.
    fn on_market_open(&mut self, ctx: &mut Ctx, market: &str) {}
    /// The book of `market` changed (see [`Subscriptions::book`]).
    fn on_book(&mut self, ctx: &mut Ctx, market: &str) {}
    /// A recorded fill of the order book (another participant's match).
    fn on_fill(&mut self, ctx: &mut Ctx, fill: &Fill) {}
    /// A Pyth tick of `feed` (needs [`Subscriptions::prices`]).
    fn on_price(&mut self, ctx: &mut Ctx, feed: &str, price: &PriceView) {}
    /// Every [`Subscriptions::timer_ms`].
    fn on_timer(&mut self, ctx: &mut Ctx) {}
    /// Trading ended (`end`); the result is not known yet.
    fn on_market_close(&mut self, ctx: &mut Ctx, market: &str) {}
    /// The result reached the chain and positions were paid out.
    fn on_resolution(&mut self, ctx: &mut Ctx, market: &str, result: &Resolution) {}
    /// News about an own order.
    fn on_order(&mut self, ctx: &mut Ctx, update: &OrderUpdate) {}
    /// Once, after the last event.
    fn on_finish(&mut self, ctx: &mut Ctx) {}
}

/// A market as the strategy may see it now: no result before resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketView {
    /// Market address.
    pub address: String,
    /// Series.
    pub period: Period,
    /// Underlying, e.g. `BTC`.
    pub instrument: String,
    /// Price feed, e.g. `btc-usd`.
    pub feed: Option<String>,
    /// Trading start, unix ms.
    pub start_ms: i64,
    /// Trading end, unix ms.
    pub end_ms: i64,
    /// USD; None before the market starts.
    pub strike: Option<f64>,
    /// Trading now.
    pub open: bool,
    /// ms until `end`, never below 0.
    pub time_to_end_ms: i64,
    /// Outcome names: `up`, `down`.
    pub outcomes: Vec<String>,
    /// Result, once resolved.
    pub resolution: Option<Resolution>,
}

/// A price level: USDC per token and size in tokens.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quote {
    /// USDC per token.
    pub price: f64,
    /// Tokens at that price.
    pub tokens: f64,
}

/// An own order that is on its way or in the book.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderView {
    /// Order id.
    pub id: OrderId,
    /// Market address.
    pub market: String,
    /// 0 = up, 1 = down.
    pub outcome: u8,
    /// Buy or sell.
    pub side: Side,
    /// Limit price.
    pub price: f64,
    /// GTC or FAK.
    pub tif: TimeInForce,
    /// Tokens.
    pub quantity: f64,
    /// Tokens filled.
    pub filled: f64,
    /// Tokens left.
    pub remaining: f64,
    /// Not in the book yet.
    pub pending: bool,
    /// Placed by the strategy, unix ms.
    pub submitted_ms: i64,
}

/// A local mistake in an order: it never leaves.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum OrderError {
    #[error("unknown market {0}")]
    /// No market with this address in the run.
    UnknownMarket(String),
    #[error("outcome {0}: 0 = up, 1 = down")]
    /// Outcome is not 0 or 1.
    BadOutcome(u8),
    #[error("price {0} is off the 0.0025 grid or outside (0, 1)")]
    /// Price outside 0.0025..0.9975 or off the 0.0025 grid.
    BadPrice(f64),
    #[error("quantity must be at least one whole token")]
    /// Zero tokens.
    ZeroQuantity,
    #[error("unknown order {0}")]
    /// No such order.
    UnknownOrder(OrderId),
}

pub(crate) enum Action {
    Submit(Box<Order>),
    Cancel(OrderId),
}

/// Everything a handler can see and do. All data is as of `now_ms`: nothing later.
pub struct Ctx<'a> {
    pub(crate) world: &'a World,
    pub(crate) actions: &'a mut Vec<Action>,
    pub(crate) next_id: &'a mut OrderId,
}

impl Ctx<'_> {
    /// Current time of the run, unix ms.
    pub fn now_ms(&self) -> i64 {
        self.world.now
    }

    fn view(&self, i: usize) -> MarketView {
        let m = &self.world.markets[i];
        let now = self.world.now;
        let start = m.info.start.timestamp_millis();
        let end = m.info.end.timestamp_millis();
        MarketView {
            address: m.info.address.clone(),
            period: m.info.period,
            instrument: m.info.instrument.clone(),
            feed: m.feed.clone(),
            start_ms: start,
            end_ms: end,
            strike: if now >= start { m.info.strike } else { None },
            open: m.open_at(now),
            time_to_end_ms: (end - now).max(0),
            outcomes: m.info.outcomes.clone(),
            resolution: m.resolution.clone(),
        }
    }

    fn index(&self, market: &str) -> Option<usize> {
        let index = &self.world.index;
        index
            .get(market)
            .or_else(|| index.get(&market.to_ascii_lowercase()))
            .copied()
    }

    /// Every market of the run, in catalog order.
    pub fn markets(&self) -> Vec<MarketView> {
        (0..self.world.markets.len())
            .map(|i| self.view(i))
            .collect()
    }

    /// Markets trading now.
    pub fn open_markets(&self) -> Vec<MarketView> {
        let mut open: Vec<usize> = self
            .world
            .active
            .iter()
            .copied()
            .filter(|&i| self.world.markets[i].open_at(self.world.now))
            .collect();
        open.sort_unstable();
        open.into_iter().map(|i| self.view(i)).collect()
    }

    /// A market by address, if it is in the run.
    pub fn market(&self, market: &str) -> Option<MarketView> {
        self.index(market).map(|i| self.view(i))
    }

    /// The book of both outcomes as the feed shows it now, without the liquidity own orders
    /// took. Up and down are mirrors: a bid of up at p is an ask of down at 1 − p. None before
    /// the first recorded book.
    pub fn book(&self, market: &str) -> Option<OrderBook> {
        let m = &self.world.markets[self.index(market)?];
        m.has_book.then(|| m.view(self.world.now))
    }

    /// Best bid of an outcome as the feed shows it now.
    pub fn best_bid(&self, market: &str, outcome: u8) -> Option<Quote> {
        self.top(market, outcome, BookSide::Bid)
    }

    /// Best ask of an outcome as the feed shows it now.
    pub fn best_ask(&self, market: &str, outcome: u8) -> Option<Quote> {
        self.top(market, outcome, BookSide::Ask)
    }

    /// The top of the book without building the book.
    fn top(&self, market: &str, outcome: u8, side: BookSide) -> Option<Quote> {
        if outcome > 1 {
            return None;
        }
        let m = &self.world.markets[self.index(market)?];
        if !m.has_book {
            return None;
        }
        let (t, s) = m.top(outcome, side)?;
        Some(Quote {
            price: tick_price(t),
            tokens: tokens(s),
        })
    }

    /// Last price of a feed (`btc-usd`).
    pub fn price(&self, feed: &str) -> Option<&PriceView> {
        self.world.prices.get(feed)
    }

    /// Last price of the market's underlying.
    pub fn market_price(&self, market: &str) -> Option<&PriceView> {
        let feed = self.world.markets[self.index(market)?].feed.as_deref()?;
        self.price(feed)
    }

    /// USDC not held by open buy orders.
    pub fn usdc(&self) -> f64 {
        usd(self.world.acct.free_usdc())
    }

    /// USDC on the account, including what open buy orders hold.
    pub fn usdc_total(&self) -> f64 {
        usd(self.world.acct.usdc)
    }

    /// Tokens of an outcome held, including those open sell orders hold.
    pub fn position(&self, market: &str, outcome: u8) -> f64 {
        self.index(market).map_or(0.0, |i| {
            self.world.acct.position(i, outcome) as f64 / UNIT as f64
        })
    }

    /// Own orders on their way or in the book; all markets when `market` is None.
    pub fn open_orders(&self, market: Option<&str>) -> Vec<OrderView> {
        let only = market.map(|m| self.index(m));
        self.world
            .live
            .iter()
            .map(|id| &self.world.orders[id])
            .filter(|o| only.is_none_or(|i| i == Some(o.market)))
            .map(|o| OrderView {
                id: o.id,
                market: self.world.markets[o.market].info.address.clone(),
                outcome: o.outcome,
                side: o.side,
                price: tick_price(o.ticks),
                tif: o.tif,
                quantity: tokens(o.qty),
                filled: tokens(o.filled),
                remaining: tokens(o.remaining()),
                pending: o.state == OrderState::Pending,
                submitted_ms: o.submitted_ms,
            })
            .collect()
    }

    fn order(
        &mut self,
        market: &str,
        outcome: u8,
        side: Side,
        price: f64,
        quantity: u64,
        tif: TimeInForce,
    ) -> Result<OrderId, OrderError> {
        let m = self
            .index(market)
            .ok_or_else(|| OrderError::UnknownMarket(market.into()))?;
        if outcome > 1 {
            return Err(OrderError::BadOutcome(outcome));
        }
        let ticks = price_ticks(price);
        if !(1..TICKS).contains(&ticks) || (tick_price(ticks) - price).abs() > TICK / 1000.0 {
            return Err(OrderError::BadPrice(price));
        }
        if quantity == 0 {
            return Err(OrderError::ZeroQuantity);
        }
        *self.next_id += 1;
        let id = *self.next_id;
        let (cside, cticks) = canonical(outcome, side, ticks);
        self.actions.push(Action::Submit(Box::new(Order {
            id,
            market: m,
            outcome,
            side,
            ticks,
            tif,
            cside,
            cticks,
            qty: quantity * UNIT as u64,
            filled: 0,
            state: OrderState::Pending,
            queue: 0,
            reserved_usdc: 0,
            reserved_tokens: 0,
            submitted_ms: self.world.now,
            arrived_ms: None,
            done_ms: None,
            end_reason: None,
            rejected: false,
            taker_best: None,
            taker_qty: 0,
            taker_notional: 0,
        })));
        Ok(id)
    }

    /// Limit order (GTC) for whole `tokens` of an outcome at `price` (0.0025 grid). What crosses
    /// the book on arrival executes at once; the rest rests.
    pub fn limit(
        &mut self,
        market: &str,
        outcome: u8,
        side: Side,
        price: f64,
        tokens: u64,
    ) -> Result<OrderId, OrderError> {
        self.order(market, outcome, side, price, tokens, TimeInForce::Gtc)
    }

    /// Market order (FAK) for whole `tokens`: walks the book on arrival up to `limit` (default
    /// the whole range: 0.9975 for a buy, 0.0025 for a sell); the rest is cancelled. A buy holds
    /// `limit` × tokens of USDC until it is processed, as on the exchange.
    pub fn market_order(
        &mut self,
        market: &str,
        outcome: u8,
        side: Side,
        tokens: u64,
        limit: Option<f64>,
    ) -> Result<OrderId, OrderError> {
        let price = limit.unwrap_or(match side {
            Side::Buy => tick_price(TICKS - 1),
            Side::Sell => TICK,
        });
        self.order(market, outcome, side, price, tokens, TimeInForce::Fak)
    }

    /// Cancel an order; it takes effect after the cancel latency, and the order may fill before.
    pub fn cancel(&mut self, order: OrderId) -> Result<(), OrderError> {
        // ids are issued in order; one placed in this same handler is not in the world yet
        if order == 0 || order > *self.next_id {
            return Err(OrderError::UnknownOrder(order));
        }
        self.actions.push(Action::Cancel(order));
        Ok(())
    }
}
