//! State of a run: the replayed book of every market in the order book's own terms, own orders,
//! the account, and the matching rules.
//!
//! The CLOB keeps one ladder per market. A buy of up at p and a sell of down at 1 − p are the
//! same bid of that ladder; a sell of up at p and a buy of down at 1 − p are the same ask. The
//! book of down that the feed shows is the mirror of up's. So the engine keeps the canonical
//! ladder (in up prices) and maps every order and every recorded fill onto it: a buy of up meets
//! a buy of down (mint) or a sell of up (direct) in the same queue, exactly as the CLOB does.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::book::{BookSide, Level, OrderBook, OutcomeBook, tick_price};
use crate::fills::Fill;
use crate::market::MarketInfo;
use crate::trades::Side;

use super::params::{ExecParams, QueueModel, Rng};
use super::strategy::{
    Liquidity, MatchType, OrderId, OrderUpdate, OrderUpdateKind, PriceView, Resolution, TimeInForce,
};

/// Ticks in one USDC (0.0025 step).
pub(crate) const TICKS: u32 = 400;
/// Raw units of one outcome token; also micro-USDC in one USDC.
pub(crate) const UNIT: i64 = 1_000_000;
/// Smallest order the CLOB accepts: 1 USDC of collateral.
pub(crate) const MIN_NOTIONAL: i64 = UNIT;
/// A recorded fill shows in the book within this time; older unexplained fill volume is dropped.
const FILL_TO_BOOK_MS: i64 = 2_000;

/// Micro-USDC of `raw` token units at `ticks`.
pub(crate) fn notional(raw: u64, ticks: u32) -> i64 {
    (raw as i128 * ticks as i128 * 2_500 / UNIT as i128) as i64
}

fn bps(amount: i64, bps: u32) -> i64 {
    (amount as i128 * bps as i128 / 10_000) as i64
}

pub(crate) fn usd(micro: i64) -> f64 {
    micro as f64 / UNIT as f64
}

pub(crate) fn tokens(raw: u64) -> f64 {
    raw as f64 / UNIT as f64
}

fn opposite(s: BookSide) -> BookSide {
    match s {
        BookSide::Bid => BookSide::Ask,
        BookSide::Ask => BookSide::Bid,
    }
}

fn idx(s: BookSide) -> usize {
    match s {
        BookSide::Bid => 0,
        BookSide::Ask => 1,
    }
}

/// Canonical side and price of an order on `outcome`.
pub(crate) fn canonical(outcome: u8, side: Side, ticks: u32) -> (BookSide, u32) {
    match (outcome, side) {
        (0, Side::Buy) => (BookSide::Bid, ticks),
        (0, Side::Sell) => (BookSide::Ask, ticks),
        (_, Side::Buy) => (BookSide::Ask, TICKS - ticks),
        (_, Side::Sell) => (BookSide::Bid, TICKS - ticks),
    }
}

/// Price of `outcome` for a canonical price.
fn outcome_ticks(outcome: u8, cticks: u32) -> u32 {
    if outcome == 0 { cticks } else { TICKS - cticks }
}

/// Does an order on `side` at `limit` reach a resting order of the other side at `t`?
fn crosses(side: BookSide, limit: u32, t: u32) -> bool {
    match side {
        BookSide::Bid => t <= limit,
        BookSide::Ask => t >= limit,
    }
}

/// Is `a` a better price than `b` for a resting order on `side`?
fn better(side: BookSide, a: u32, b: u32) -> bool {
    match side {
        BookSide::Bid => a > b,
        BookSide::Ask => a < b,
    }
}

/// One side of the recorded ladder: (tick, size) ascending by tick, no zero sizes. A book has
/// tens of levels, so a sorted vector beats a tree.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ladder(Vec<(u32, u64)>);

impl Ladder {
    fn get(&self, t: u32) -> u64 {
        match self.0.binary_search_by_key(&t, |l| l.0) {
            Ok(i) => self.0[i].1,
            Err(_) => 0,
        }
    }

    fn set(&mut self, t: u32, size: u64) {
        match (self.0.binary_search_by_key(&t, |l| l.0), size) {
            (Ok(i), 0) => {
                self.0.remove(i);
            }
            (Ok(i), s) => self.0[i].1 = s,
            (Err(_), 0) => {}
            (Err(i), s) => self.0.insert(i, (t, s)),
        }
    }

    fn levels(&self) -> &[(u32, u64)] {
        &self.0
    }

    fn low(&self) -> Option<u32> {
        self.0.first().map(|l| l.0)
    }

    fn high(&self) -> Option<u32> {
        self.0.last().map(|l| l.0)
    }
}

/// Own orders by id. Ids are issued from 1 in order and every one is submitted, so the order
/// with id `n` sits at `n - 1`.
#[derive(Debug, Clone, Default)]
pub(crate) struct Orders(Vec<Order>);

impl Orders {
    pub fn get(&self, id: &OrderId) -> Option<&Order> {
        id.checked_sub(1).and_then(|i| self.0.get(i as usize))
    }

    pub fn get_mut(&mut self, id: &OrderId) -> Option<&mut Order> {
        id.checked_sub(1).and_then(|i| self.0.get_mut(i as usize))
    }

    fn insert(&mut self, o: Order) {
        assert_eq!(
            o.id,
            self.0.len() as u64 + 1,
            "order ids are issued in order"
        );
        self.0.push(o);
    }

    /// All orders, by id.
    pub fn values(&self) -> std::slice::Iter<'_, Order> {
        self.0.iter()
    }
}

impl std::ops::Index<&OrderId> for Orders {
    type Output = Order;

    fn index(&self, id: &OrderId) -> &Order {
        self.get(id).expect("order exists")
    }
}

/// Liquidity the engine's own orders took from a recorded level, until the recording shows the
/// level anew.
#[derive(Debug, Clone, Copy)]
struct Eaten {
    amount: u64,
    /// Recorded size when it was taken.
    recorded: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderState {
    /// Sent, not in the book yet.
    Pending,
    Live,
    Done,
}

#[derive(Debug, Clone)]
pub(crate) struct Order {
    pub id: OrderId,
    pub market: usize,
    pub outcome: u8,
    pub side: Side,
    /// Limit price of `outcome`, ticks.
    pub ticks: u32,
    pub tif: TimeInForce,
    pub cside: BookSide,
    pub cticks: u32,
    /// Raw units.
    pub qty: u64,
    pub filled: u64,
    pub state: OrderState,
    /// Recorded volume ahead of the order at its price.
    pub queue: u64,
    pub reserved_usdc: i64,
    pub reserved_tokens: u64,
    pub submitted_ms: i64,
    pub arrived_ms: Option<i64>,
    pub done_ms: Option<i64>,
    /// Why it ended without a full fill: rejected or cancelled reason.
    pub end_reason: Option<String>,
    pub rejected: bool,
    /// Best opposite price when it arrived (price of `outcome`), for slippage.
    pub taker_best: Option<u32>,
    pub taker_qty: u64,
    pub taker_notional: i64,
}

impl Order {
    pub fn remaining(&self) -> u64 {
        self.qty - self.filled
    }
}

/// Money of one market, micro-USDC.
#[derive(Debug, Clone, Default)]
pub(crate) struct MarketAcc {
    /// Sells minus buys minus trading fees.
    pub cash: i64,
    pub fees: i64,
    pub payout: i64,
    pub turnover: i64,
    pub fills: u32,
    pub reverted: u32,
    pub orders: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Mkt {
    pub info: MarketInfo,
    pub feed: Option<String>,
    /// Canonical ladder as recorded: bids and asks by tick.
    book: [Ladder; 2],
    pub has_book: bool,
    eaten: BTreeMap<(BookSide, u32), Eaten>,
    /// Recorded fills not yet seen in the book: (time, size) per level.
    unseen_fills: BTreeMap<(BookSide, u32), VecDeque<(i64, u64)>>,
    /// Own live orders in arrival order.
    pub resting: Vec<OrderId>,
    pub closed: bool,
    pub resolution: Option<Resolution>,
    pub recorded_fills: u64,
    last_cticks: Option<u32>,
    pub acc: MarketAcc,
    /// Mark of up over time, micro-USDC: (unix ms, mark), at most every `mark_step` ms.
    pub marks: Vec<(i64, i64)>,
    mark_step: i64,
}

impl Mkt {
    fn new(info: MarketInfo) -> Self {
        let len = (info.end - info.start).num_milliseconds();
        Mkt {
            feed: info.feed_slug(),
            info,
            book: [Ladder::default(), Ladder::default()],
            has_book: false,
            eaten: BTreeMap::new(),
            unseen_fills: BTreeMap::new(),
            resting: Vec::new(),
            closed: false,
            resolution: None,
            recorded_fills: 0,
            last_cticks: None,
            acc: MarketAcc::default(),
            marks: Vec::new(),
            mark_step: (len / 150).max(1000),
        }
    }

    pub fn open_at(&self, now: i64) -> bool {
        !self.closed
            && self.info.start.timestamp_millis() <= now
            && now < self.info.end.timestamp_millis()
    }

    /// `recorded` at `t` minus what own orders took there.
    fn visible_of(&self, side: BookSide, t: u32, recorded: u64) -> u64 {
        if self.eaten.is_empty() {
            return recorded;
        }
        let eaten = self.eaten.get(&(side, t)).map_or(0, |e| e.amount);
        recorded.saturating_sub(eaten)
    }

    fn visible(&self, side: BookSide, t: u32) -> u64 {
        self.visible_of(side, t, self.book[idx(side)].get(t))
    }

    /// Best price of a canonical side with visible size: (ticks, size).
    fn best_visible_level(&self, side: BookSide) -> Option<(u32, u64)> {
        let levels = self.book[idx(side)].levels();
        let vis = |&(t, s): &(u32, u64)| {
            let v = self.visible_of(side, t, s);
            (v > 0).then_some((t, v))
        };
        match side {
            BookSide::Bid => levels.iter().rev().find_map(vis),
            BookSide::Ask => levels.iter().find_map(vis),
        }
    }

    fn best_visible(&self, side: BookSide) -> Option<u32> {
        self.best_visible_level(side).map(|l| l.0)
    }

    /// Best visible level of `outcome`'s `side` as the feed shows it: (ticks of the outcome,
    /// size). Down's bids are the mirrored canonical asks and its asks the mirrored bids.
    pub fn top(&self, outcome: u8, side: BookSide) -> Option<(u32, u64)> {
        let cside = if outcome == 0 { side } else { opposite(side) };
        let (t, v) = self.best_visible_level(cside)?;
        Some((outcome_ticks(outcome, t), v))
    }

    /// The book as the feed would show it now: recorded levels minus what own orders took, for
    /// both outcomes.
    pub fn view(&self, ts_ms: i64) -> OrderBook {
        let mut up = OutcomeBook::default();
        let mut down = OutcomeBook::default();
        for side in [BookSide::Bid, BookSide::Ask] {
            for &(t, s) in self.book[idx(side)].levels() {
                let v = self.visible_of(side, t, s);
                if v == 0 {
                    continue;
                }
                match side {
                    BookSide::Bid => {
                        up.bids.insert(t, v);
                        down.asks.insert(TICKS - t, v);
                    }
                    BookSide::Ask => {
                        up.asks.insert(t, v);
                        down.bids.insert(TICKS - t, v);
                    }
                }
            }
        }
        OrderBook {
            ts_ms,
            outcomes: vec![up, down],
        }
    }

    /// Notes the mark of up when it changed, at most every `mark_step` ms unless `force`.
    pub fn record_mark(&mut self, now: i64, force: bool) {
        let v = self.mark_up();
        match self.marks.last() {
            Some(&(_, last)) if last == v => {}
            Some(&(t, _)) if !force && now - t < self.mark_step => {}
            _ => self.marks.push((now, v)),
        }
    }

    /// Mark of up, micro-USDC per token: mid of the recorded book, one side of it, the last
    /// recorded fill, or 0.5.
    pub fn mark_up(&self) -> i64 {
        let bid = self.book[0].high();
        let ask = self.book[1].low();
        let t2 = match (bid, ask) {
            (Some(b), Some(a)) => b + a,
            (Some(x), None) | (None, Some(x)) => 2 * x,
            (None, None) => self.last_cticks.map_or(TICKS, |t| 2 * t),
        };
        t2 as i64 * 2_500 / 2
    }
}

/// Own balances, micro-USDC and raw token units.
#[derive(Debug, Clone, Default)]
pub(crate) struct Account {
    pub usdc: i64,
    pub reserved_usdc: i64,
    pub tokens: BTreeMap<(usize, u8), i64>,
    pub reserved_tokens: BTreeMap<(usize, u8), u64>,
}

impl Account {
    pub fn free_usdc(&self) -> i64 {
        self.usdc - self.reserved_usdc
    }

    pub fn position(&self, m: usize, o: u8) -> i64 {
        self.tokens.get(&(m, o)).copied().unwrap_or(0)
    }

    pub fn free_tokens(&self, m: usize, o: u8) -> i64 {
        self.position(m, o) - self.reserved_tokens.get(&(m, o)).copied().unwrap_or(0) as i64
    }

    /// Changes a balance; a balance back at 0 leaves the map, so walks over it stay short.
    fn add_tokens(&mut self, key: (usize, u8), delta: i64) {
        let v = self.tokens.entry(key).or_default();
        *v += delta;
        if *v == 0 {
            self.tokens.remove(&key);
        }
    }
}

/// One line of the own-order journal.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JournalRow {
    /// Unix ms.
    pub ts_ms: i64,
    /// `submitted`, `accepted`, `rejected`, `fill`, `settlement_failed`, `cancel_requested`,
    /// `cancelled`, `cancel_failed`, `payout`.
    pub event: String,
    /// Own order, for order events.
    pub order_id: Option<OrderId>,
    /// Market address.
    pub market: String,
    /// 0 = up, 1 = down.
    pub outcome: Option<u8>,
    /// Buy or sell.
    pub side: Option<Side>,
    /// GTC or FAK.
    pub tif: Option<TimeInForce>,
    /// Limit price for order events, execution price for fills, payout per token for payouts.
    pub price: Option<f64>,
    /// Tokens.
    pub quantity: Option<f64>,
    /// Tokens left on the order after the event.
    pub remaining: Option<f64>,
    /// Maker or taker, for fills.
    pub liquidity: Option<Liquidity>,
    /// How the CLOB settled a fill.
    pub match_type: Option<MatchType>,
    /// Why: rejection, cancel or end reason.
    pub reason: Option<String>,
    /// USDC paid in fees by this event.
    pub fee: Option<f64>,
    /// USDC on the account after the event.
    pub usdc: f64,
}

pub(crate) struct World {
    pub params: ExecParams,
    pub markets: Vec<Mkt>,
    pub index: HashMap<String, usize>,
    pub prices: BTreeMap<String, PriceView>,
    pub orders: Orders,
    /// Orders not done yet (pending or live), by id.
    pub live: BTreeSet<OrderId>,
    /// Markets by start time, and how many of them have started.
    by_start: Vec<usize>,
    started: usize,
    /// Started markets that had not ended at the last prune (a superset of the open ones).
    pub active: Vec<usize>,
    /// Earliest end among `active`: the next prune.
    prune_at: i64,
    pub acct: Account,
    pub journal: Vec<JournalRow>,
    pub rng: Rng,
    pub now: i64,
    /// Updates for the strategy, in the order they happened.
    pub updates: Vec<OrderUpdate>,
    /// Bumped on every change of own token balances.
    pub token_moves: u64,
}

impl World {
    pub fn new(infos: Vec<MarketInfo>, params: ExecParams) -> Self {
        let markets: Vec<Mkt> = infos.into_iter().map(Mkt::new).collect();
        let index = markets
            .iter()
            .enumerate()
            .map(|(i, m)| (m.info.address.clone(), i))
            .collect();
        let mut by_start: Vec<usize> = (0..markets.len()).collect();
        by_start.sort_by_key(|&i| markets[i].info.start);
        World {
            acct: Account {
                usdc: (params.initial_usdc * UNIT as f64).round() as i64,
                ..Account::default()
            },
            rng: Rng::new(params.seed),
            params,
            markets,
            index,
            prices: BTreeMap::new(),
            orders: Orders::default(),
            live: BTreeSet::new(),
            by_start,
            started: 0,
            active: Vec::new(),
            prune_at: i64::MAX,
            journal: Vec::new(),
            now: 0,
            updates: Vec::new(),
            token_moves: 0,
        }
    }

    /// Moves the clock (never back) and keeps `active` up to date.
    pub fn set_now(&mut self, t: i64) {
        self.now = self.now.max(t);
        let now = self.now;
        while let Some(&m) = self.by_start.get(self.started) {
            if self.markets[m].info.start.timestamp_millis() > now {
                break;
            }
            self.active.push(m);
            self.prune_at = self
                .prune_at
                .min(self.markets[m].info.end.timestamp_millis());
            self.started += 1;
        }
        if now >= self.prune_at {
            let markets = &self.markets;
            self.active
                .retain(|&m| now < markets[m].info.end.timestamp_millis());
            self.prune_at = self
                .active
                .iter()
                .map(|&m| markets[m].info.end.timestamp_millis())
                .min()
                .unwrap_or(i64::MAX);
        }
    }

    fn row(&self, event: &str, o: &Order) -> JournalRow {
        JournalRow {
            ts_ms: self.now,
            event: event.into(),
            order_id: Some(o.id),
            market: self.markets[o.market].info.address.clone(),
            outcome: Some(o.outcome),
            side: Some(o.side),
            tif: Some(o.tif),
            price: Some(tick_price(o.ticks)),
            quantity: Some(tokens(o.qty)),
            remaining: Some(tokens(o.remaining())),
            liquidity: None,
            match_type: None,
            reason: None,
            fee: None,
            usdc: usd(self.acct.usdc),
        }
    }

    /// Journals an event of order `id`; `edit` fills in the event's own fields.
    fn log(&mut self, event: &str, id: OrderId, edit: impl FnOnce(&mut JournalRow)) {
        let mut row = self.row(event, &self.orders[&id]);
        edit(&mut row);
        self.journal.push(row);
    }

    fn update(&mut self, id: OrderId, kind: OrderUpdateKind) {
        let m = self.orders[&id].market;
        self.updates.push(OrderUpdate {
            order_id: id,
            market: self.markets[m].info.address.clone(),
            ts_ms: self.now,
            kind,
        });
    }

    pub fn submit(&mut self, o: Order) {
        let row = self.row("submitted", &o);
        self.journal.push(row);
        self.markets[o.market].acc.orders += 1;
        self.live.insert(o.id);
        self.orders.insert(o);
    }

    /// The order reaches the book: checks, reservation, the crossing part, then the rest rests
    /// (GTC) or is cancelled (FAK).
    pub fn arrive(&mut self, id: OrderId) {
        let Some(o) = self.orders.get(&id) else {
            return;
        };
        if o.state != OrderState::Pending {
            return;
        }
        let m = o.market;
        let limit_notional = notional(o.qty, o.ticks);
        let reason = if !self.markets[m].open_at(self.now) {
            Some("market_not_open")
        } else if limit_notional < MIN_NOTIONAL
            && !(o.side == Side::Sell && self.acct.free_tokens(m, o.outcome) == o.qty as i64)
        {
            Some("order_notional_below_minimum")
        } else {
            match o.side {
                Side::Buy => {
                    let need = limit_notional + bps(limit_notional, self.params.fee_bps);
                    (self.acct.free_usdc() < need).then_some("insufficient_balance")
                }
                Side::Sell => (self.acct.free_tokens(m, o.outcome) < o.qty as i64)
                    .then_some("insufficient_tokens"),
            }
        };
        if let Some(reason) = reason {
            let o = self.orders.get_mut(&id).expect("order exists");
            o.state = OrderState::Done;
            o.rejected = true;
            o.done_ms = Some(self.now);
            o.end_reason = Some(reason.into());
            self.live.remove(&id);
            self.log("rejected", id, |r| r.reason = Some(reason.into()));
            self.update(
                id,
                OrderUpdateKind::Rejected {
                    reason: reason.into(),
                },
            );
            return;
        }

        {
            let o = self.orders.get_mut(&id).expect("order exists");
            match o.side {
                Side::Buy => {
                    let need = limit_notional + bps(limit_notional, self.params.fee_bps);
                    o.reserved_usdc = need;
                    self.acct.reserved_usdc += need;
                }
                Side::Sell => {
                    o.reserved_tokens = o.qty;
                    *self.acct.reserved_tokens.entry((m, o.outcome)).or_default() += o.qty;
                }
            }
            o.state = OrderState::Live;
            o.arrived_ms = Some(self.now);
        }
        self.log("accepted", id, |_| {});
        self.update(id, OrderUpdateKind::Accepted);

        self.take(id);

        let o = &self.orders[&id];
        if o.state == OrderState::Done {
            return;
        }
        match o.tif {
            TimeInForce::Fak => self.finish(id, Some("fak_remainder")),
            TimeInForce::Gtc => {
                let queue = self.markets[m].visible(o.cside, o.cticks);
                self.orders.get_mut(&id).expect("order exists").queue = queue;
                self.markets[m].resting.push(id);
            }
        }
    }

    /// The crossing part of an arriving order: walk the opposite side from the best price while
    /// it is within the limit, taking what is visible and remembering it as eaten.
    fn take(&mut self, id: OrderId) {
        let o = &self.orders[&id];
        let (m, cside, cticks, outcome) = (o.market, o.cside, o.cticks, o.outcome);
        let opp = opposite(cside);
        // The CLOB never matches two orders of one maker: own resting orders the incoming one
        // reaches are cancelled.
        let own: Vec<OrderId> = self.markets[m]
            .resting
            .iter()
            .copied()
            .filter(|r| {
                let r = &self.orders[r];
                r.cside == opp && crosses(cside, cticks, r.cticks)
            })
            .collect();
        for r in own {
            self.finish(r, Some("self_trade"));
        }
        let levels: Vec<(u32, u64)> = match cside {
            BookSide::Bid => self.markets[m].book[1]
                .levels()
                .iter()
                .take_while(|l| l.0 <= cticks)
                .copied()
                .collect(),
            BookSide::Ask => self.markets[m].book[0]
                .levels()
                .iter()
                .rev()
                .take_while(|l| l.0 >= cticks)
                .copied()
                .collect(),
        };
        for (t, size) in levels {
            let rem = self.orders[&id].remaining();
            if rem == 0 {
                break;
            }
            let vis = self.markets[m].visible(opp, t);
            if vis == 0 {
                continue;
            }
            let order = self.orders.get_mut(&id).expect("order exists");
            if order.taker_best.is_none() {
                order.taker_best = Some(outcome_ticks(outcome, t));
            }
            let qty = vis.min(rem);
            let e = self.markets[m].eaten.entry((opp, t)).or_insert(Eaten {
                amount: 0,
                recorded: size,
            });
            e.amount += qty;
            self.fill(id, qty, t, Liquidity::Taker, None);
        }
    }

    /// An own fill of `raw` units at canonical price `cticks`.
    fn fill(
        &mut self,
        id: OrderId,
        raw: u64,
        cticks: u32,
        liquidity: Liquidity,
        match_type: Option<MatchType>,
    ) {
        let fee_bps = self.params.fee_bps;
        let revert_p = self.params.revert_probability;
        let reverted = revert_p > 0.0 && self.rng.unit() < revert_p;
        let o = self.orders.get_mut(&id).expect("order exists");
        let raw = raw.min(o.remaining());
        if raw == 0 {
            return;
        }
        let m = o.market;
        let price = outcome_ticks(o.outcome, cticks);
        let value = notional(raw, price);
        let fee = bps(value, fee_bps);
        o.filled += raw;
        if liquidity == Liquidity::Taker {
            o.taker_qty += raw;
            o.taker_notional += value;
        }
        let key = (m, o.outcome);
        match o.side {
            Side::Buy => {
                let at_limit = notional(raw, o.ticks);
                let release = (at_limit + bps(at_limit, fee_bps)).min(o.reserved_usdc);
                o.reserved_usdc -= release;
                self.acct.reserved_usdc -= release;
                if !reverted {
                    self.acct.usdc -= value + fee;
                    self.acct.add_tokens(key, raw as i64);
                    self.token_moves += 1;
                }
            }
            Side::Sell => {
                let release = raw.min(o.reserved_tokens);
                o.reserved_tokens -= release;
                *self.acct.reserved_tokens.entry(key).or_default() -= release;
                if !reverted {
                    self.acct.add_tokens(key, -(raw as i64));
                    self.token_moves += 1;
                    self.acct.usdc += value - fee;
                }
            }
        }
        let (side, outcome) = (o.side, o.outcome);
        let remaining = tokens(o.remaining());
        let done = o.remaining() == 0;
        let acc = &mut self.markets[m].acc;
        if reverted {
            acc.reverted += 1;
        } else {
            acc.cash += match side {
                Side::Buy => -value,
                Side::Sell => value,
            } - fee;
            acc.fees += fee;
            acc.turnover += value;
            acc.fills += 1;
        }

        self.log("fill", id, |row| {
            row.price = Some(tick_price(price));
            row.quantity = Some(tokens(raw));
            row.liquidity = Some(liquidity);
            row.match_type = match_type;
            // a reverted fill and its undoing: balances never moved
            row.fee = Some(if reverted { 0.0 } else { usd(fee) });
        });
        self.update(
            id,
            OrderUpdateKind::Filled {
                outcome,
                side,
                price: tick_price(price),
                quantity: tokens(raw),
                remaining,
                liquidity,
                match_type,
                fee: if reverted { 0.0 } else { usd(fee) },
            },
        );
        if reverted {
            self.log("settlement_failed", id, |row| {
                row.price = Some(tick_price(price));
                row.quantity = Some(tokens(raw));
            });
            self.update(
                id,
                OrderUpdateKind::SettlementFailed {
                    price: tick_price(price),
                    quantity: tokens(raw),
                },
            );
        }
        if done {
            self.finish(id, None);
        }
    }

    /// Ends an order: releases what it still holds, leaves the book. `reason` = cancelled.
    fn finish(&mut self, id: OrderId, reason: Option<&str>) {
        let o = self.orders.get_mut(&id).expect("order exists");
        if o.state == OrderState::Done {
            return;
        }
        o.state = OrderState::Done;
        o.done_ms = Some(self.now);
        self.acct.reserved_usdc -= o.reserved_usdc;
        o.reserved_usdc = 0;
        if o.reserved_tokens > 0 {
            *self
                .acct
                .reserved_tokens
                .entry((o.market, o.outcome))
                .or_default() -= o.reserved_tokens;
            o.reserved_tokens = 0;
        }
        if let Some(r) = reason {
            o.end_reason = Some(r.into());
        }
        let m = o.market;
        let remaining = tokens(o.remaining());
        self.live.remove(&id);
        self.markets[m].resting.retain(|r| *r != id);
        if let Some(reason) = reason {
            self.log("cancelled", id, |row| row.reason = Some(reason.into()));
            self.update(
                id,
                OrderUpdateKind::Cancelled {
                    remaining,
                    reason: reason.into(),
                },
            );
        }
    }

    pub fn cancel_requested(&mut self, id: OrderId) {
        if self.orders.get(&id).is_some() {
            self.log("cancel_requested", id, |_| {});
        }
    }

    /// A cancel reaches the book.
    pub fn cancel(&mut self, id: OrderId) {
        let Some(o) = self.orders.get(&id) else {
            return;
        };
        let state = o.state;
        let failed = match state {
            OrderState::Live => {
                self.finish(id, Some("cancelled"));
                return;
            }
            // The book does not know the order yet: the cancel misses it.
            OrderState::Pending => "unknown_order",
            OrderState::Done => "order_done",
        };
        self.log("cancel_failed", id, |row| row.reason = Some(failed.into()));
        self.update(
            id,
            OrderUpdateKind::CancelFailed {
                reason: failed.into(),
            },
        );
    }

    /// A recorded book snapshot or delta. Both outcomes' levels map onto the canonical ladder;
    /// where both carry the same level, up's wins.
    pub fn apply_book(&mut self, m: usize, levels: &[Level], snapshot: bool) {
        // Canonical levels, the last one of a key winning; down first so that up wins.
        let mut new: Vec<((BookSide, u32), u64)> = Vec::with_capacity(levels.len());
        let ordered = levels
            .iter()
            .filter(|l| l.outcome != 0)
            .chain(levels.iter().filter(|l| l.outcome == 0));
        for l in ordered {
            let t = l.ticks();
            if !(1..TICKS).contains(&t) {
                continue;
            }
            let key = if l.outcome == 0 {
                (l.side, t)
            } else {
                (opposite(l.side), TICKS - t)
            };
            new.push((key, l.size));
        }
        new.reverse();
        new.sort_by_key(|l| l.0);
        new.dedup_by_key(|l| l.0);

        let mkt = &mut self.markets[m];
        mkt.has_book = true;
        let mut changes: Vec<((BookSide, u32), u64, u64)> = Vec::new();
        if snapshot {
            for side in [BookSide::Bid, BookSide::Ask] {
                for &(t, old) in mkt.book[idx(side)].levels() {
                    if new.binary_search_by_key(&(side, t), |l| l.0).is_err() {
                        changes.push(((side, t), old, 0));
                    }
                }
            }
        }
        for &(key, size) in &new {
            let old = mkt.book[idx(key.0)].get(key.1);
            changes.push((key, old, size));
        }
        for (key, old, size) in changes {
            self.set_level(m, key, old, size);
        }
        self.cross_resting(m);
    }

    fn set_level(&mut self, m: usize, key: (BookSide, u32), old: u64, new: u64) {
        let now = self.now;
        let model = self.params.queue_model;
        let mkt = &mut self.markets[m];
        mkt.book[idx(key.0)].set(key.1, new);
        // Eaten liquidity: new orders on the level (it grew) bring it back; when it shrinks, the
        // part we took stays taken as far as the level still holds it.
        if let Some(e) = mkt.eaten.get_mut(&key) {
            if new > e.recorded {
                mkt.eaten.remove(&key);
            } else if new < e.recorded {
                e.amount = e.amount.min(new);
                e.recorded = new;
                if e.amount == 0 {
                    mkt.eaten.remove(&key);
                }
            }
        }
        if new >= old {
            return;
        }
        // A drop: the part recorded fills explain already moved our queue; the rest are cancels.
        let mut drop = old - new;
        let mut explained = 0;
        if let Some(q) = mkt.unseen_fills.get_mut(&key) {
            while let Some(&(ts, size)) = q.front() {
                if ts < now - FILL_TO_BOOK_MS {
                    q.pop_front();
                    continue;
                }
                if drop == 0 {
                    break;
                }
                let used = size.min(drop);
                drop -= used;
                explained += used;
                if used == size {
                    q.pop_front();
                } else {
                    q.front_mut().expect("front exists").1 -= used;
                }
            }
            if q.is_empty() {
                mkt.unseen_fills.remove(&key);
            }
        }
        let cancels = drop;
        let base = old - explained;
        for id in &mkt.resting {
            let o = self.orders.get_mut(id).expect("resting order exists");
            if (o.cside, o.cticks) != key {
                continue;
            }
            if cancels > 0 && model == QueueModel::Proportional && base > 0 {
                let gone = (o.queue as u128 * cancels as u128 / base as u128) as u64;
                o.queue -= gone.min(o.queue);
            }
            // whatever the model, no more can stand ahead than the level holds
            o.queue = o.queue.min(new);
        }
    }

    /// Resting orders the book now trades through fill completely at their own price.
    fn cross_resting(&mut self, m: usize) {
        if self.markets[m].resting.is_empty() {
            return;
        }
        let best_bid = self.markets[m].best_visible(BookSide::Bid);
        let best_ask = self.markets[m].best_visible(BookSide::Ask);
        for id in self.markets[m].resting.clone() {
            let o = &self.orders[&id];
            let through = match o.cside {
                BookSide::Bid => best_ask.is_some_and(|a| a <= o.cticks),
                BookSide::Ask => best_bid.is_some_and(|b| b >= o.cticks),
            };
            if through && o.state == OrderState::Live {
                let (rem, t) = (o.remaining(), o.cticks);
                self.fill(id, rem, t, Liquidity::Maker, None);
            }
        }
    }

    /// A recorded fill: it took `size` from one side of the ladder. Own orders at a better price
    /// would have been taken first; at the same price it first eats the queue ahead of ours.
    pub fn recorded_fill(&mut self, m: usize, f: &Fill) {
        let Some(taker_outcome) = f.outcome_index else {
            return;
        };
        let t = crate::book::price_ticks(f.price);
        if !(1..TICKS).contains(&t) {
            return;
        }
        let (taker_side, ct) = canonical(taker_outcome, f.side, t);
        let side = opposite(taker_side);
        let now = self.now;
        let mkt = &mut self.markets[m];
        mkt.recorded_fills += 1;
        mkt.last_cticks = Some(ct);
        mkt.unseen_fills
            .entry((side, ct))
            .or_default()
            .push_back((now, f.size));

        let mut ours: Vec<(u32, usize, OrderId)> = mkt
            .resting
            .iter()
            .enumerate()
            .filter_map(|(i, id)| {
                let o = &self.orders[id];
                (o.cside == side && (o.cticks == ct || better(side, o.cticks, ct)))
                    .then_some((o.cticks, i, *id))
            })
            .collect();
        // price priority, then arrival
        ours.sort_by(|a, b| match side {
            BookSide::Bid => b.0.cmp(&a.0).then(a.1.cmp(&b.1)),
            BookSide::Ask => a.0.cmp(&b.0).then(a.1.cmp(&b.1)),
        });
        let mut left = f.size;
        for (price, _, id) in ours {
            if left == 0 {
                break;
            }
            let o = self.orders.get_mut(&id).expect("resting order exists");
            if price == ct {
                let ahead = o.queue.min(left);
                o.queue -= ahead;
                left -= ahead;
            }
            let qty = left.min(o.remaining());
            if qty == 0 {
                continue;
            }
            left -= qty;
            let mt = match_type(o.outcome, o.side, taker_outcome, f.side);
            self.fill(id, qty, price, Liquidity::Maker, mt);
        }
    }

    pub fn price_tick(&mut self, feed: &str, p: PriceView) {
        self.prices.insert(feed.to_string(), p);
    }

    /// The market expires: the CLOB drops its book with every resting order.
    pub fn close(&mut self, m: usize) {
        self.markets[m].closed = true;
        for id in self.markets[m].resting.clone() {
            self.finish(id, Some("market_closed"));
        }
    }

    /// The result is on chain: tokens are redeemed (winner 1 USDC, void 0.5 each).
    pub fn resolve(&mut self, m: usize, r: Resolution) {
        self.markets[m].resolution = Some(r.clone());
        for o in 0..2u8 {
            let held = self.acct.position(m, o);
            if held <= 0 {
                continue;
            }
            let (gross, fee) = if r.voided {
                (held / 2, 0)
            } else if r.winner == Some(o) {
                (held, bps(held, self.params.redeem_fee_bps))
            } else {
                (0, 0)
            };
            let pay = gross - fee;
            self.acct.tokens.remove(&(m, o));
            self.token_moves += 1;
            self.acct.usdc += pay;
            let acc = &mut self.markets[m].acc;
            acc.payout += pay;
            acc.fees += fee;
            self.journal.push(JournalRow {
                ts_ms: self.now,
                event: "payout".into(),
                order_id: None,
                market: self.markets[m].info.address.clone(),
                outcome: Some(o),
                side: None,
                tif: None,
                price: Some(gross as f64 / held as f64),
                quantity: Some(held as f64 / UNIT as f64),
                remaining: None,
                liquidity: None,
                match_type: None,
                reason: None,
                fee: Some(usd(fee)),
                usdc: usd(self.acct.usdc),
            });
        }
    }

    /// Value of open positions at the marks, micro-USDC.
    pub fn positions_value(&self) -> i64 {
        let mut v = 0i64;
        for (&(m, o), &raw) in &self.acct.tokens {
            if raw == 0 {
                continue;
            }
            let up = self.markets[m].mark_up();
            let p = if o == 0 { up } else { UNIT - up };
            v += (raw as i128 * p as i128 / UNIT as i128) as i64;
        }
        v
    }

    /// Whether any own tokens are held.
    pub fn holds_tokens(&self) -> bool {
        self.acct.tokens.values().any(|v| *v != 0)
    }
}

/// How the CLOB settles a match of two orders: same outcome = direct, two buys = mint, two
/// sells = merge.
fn match_type(o1: u8, s1: Side, o2: u8, s2: Side) -> Option<MatchType> {
    match (o1 == o2, s1, s2) {
        (true, a, b) if a != b => Some(MatchType::Direct),
        (false, Side::Buy, Side::Buy) => Some(MatchType::Mint),
        (false, Side::Sell, Side::Sell) => Some(MatchType::Merge),
        _ => None,
    }
}
