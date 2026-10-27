//! Execution engine: replays a [`Dataset`] to a [`Strategy`], executes its orders against the
//! recorded book and fills, keeps the account and builds a [`Report`].
//!
//! ```no_run
//! use lucyd_backtest::engine::{Ctx, ExecParams, Strategy, run};
//!
//! struct Idle;
//! impl Strategy for Idle {
//!     fn on_timer(&mut self, _ctx: &mut Ctx) {}
//! }
//!
//! # fn go(data: &lucyd_backtest::Dataset) -> lucyd_backtest::Result<()> {
//! let report = run(data, &ExecParams::default(), &mut Idle)?;
//! println!("{}", report.summary.pnl);
//! # Ok(()) }
//! ```
//!
//! The model is described in `docs/execution.md`.

mod html;
mod params;
mod report;
pub mod risk;
mod strategy;
mod world;

#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::rc::Rc;

use crate::error::Result;
use crate::event::{Ev, Stream};
use crate::loader::Dataset;

pub use params::{ExecParams, QueueModel};
pub use report::{
    EquityPoint, MarketReport, Metrics, OrderRecord, Periods, REPORT_VERSION, Report, write_journal,
};
pub use risk::MarketPnl;
pub use strategy::{
    BookFeed, Ctx, Liquidity, MarketView, MatchType, OrderError, OrderId, OrderUpdate,
    OrderUpdateKind, OrderView, PriceView, Quote, Resolution, Strategy, Subscriptions, TimeInForce,
};
pub use world::JournalRow;

use strategy::Action;
use world::World;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Job {
    Arrive(OrderId),
    Cancel(OrderId),
    Timer,
    Book(usize),
    Resolve(usize),
    Equity,
}

impl Job {
    /// Jobs that only make sense while the data runs.
    fn periodic(self) -> bool {
        matches!(self, Job::Timer | Job::Book(_) | Job::Equity)
    }
}

struct Engine {
    world: World,
    /// Market addresses, cheap to hand to handlers.
    addrs: Vec<Rc<str>>,
    /// Scheduled jobs by (time, order of scheduling), earliest first.
    jobs: BinaryHeap<Reverse<(i64, u64, Job)>>,
    job_seq: u64,
    /// Buffer for the actions of a handler.
    actions: Vec<Action>,
    next_id: OrderId,
    subs: Subscriptions,
    book_sent: Vec<Option<i64>>,
    book_due: Vec<bool>,
    equity: Vec<report::Sample>,
    /// Whether own tokens were held at the last check, and `token_moves` then.
    held: (bool, u64),
    start_ms: i64,
    end_ms: i64,
}

/// Runs `strategy` over `data` and returns the report. Same data, params and seed give the same
/// report, bit for bit.
pub fn run<S: Strategy + ?Sized>(
    data: &Dataset,
    params: &ExecParams,
    strategy: &mut S,
) -> Result<Report> {
    params.validate()?;
    let stream = Stream::new(data);
    let start_ms = data
        .request
        .from
        .map(|t| t.timestamp_millis())
        .or(stream.order.first().map(|e| e.0.ts_ms))
        .unwrap_or(0);
    let end_ms = data
        .request
        .to
        .map(|t| t.timestamp_millis())
        .or(stream.order.last().map(|e| e.0.ts_ms))
        .unwrap_or(start_ms);
    let infos = data.markets.iter().map(|m| m.info.clone()).collect();
    let n = data.markets.len();
    let mut e = Engine {
        world: World::new(infos, params.clone()),
        addrs: data
            .markets
            .iter()
            .map(|m| Rc::from(m.info.address.as_str()))
            .collect(),
        jobs: BinaryHeap::new(),
        job_seq: 0,
        actions: Vec::new(),
        next_id: 0,
        subs: strategy.subscriptions(),
        book_sent: vec![None; n],
        book_due: vec![false; n],
        equity: Vec::new(),
        held: (false, 0),
        start_ms,
        end_ms,
    };
    e.world.set_now(start_ms);
    e.call(strategy, |s, c| s.on_start(c));
    if let Some(t) = e.subs.timer_ms.filter(|t| *t > 0) {
        e.schedule(start_ms + t as i64, Job::Timer);
    }
    e.schedule(start_ms, Job::Equity);

    for &(key, src) in &stream.order {
        let ts_ms = key.ts_ms;
        e.drain(strategy, ts_ms, false);
        e.world.set_now(ts_ms);
        match stream.get(src) {
            Ev::MarketOpen(m) => {
                let addr = e.addrs[m].clone();
                e.call(strategy, |s, c| s.on_market_open(c, &addr));
            }
            Ev::PriceTick(feed, t) => {
                let p = PriceView {
                    price: t.price,
                    conf: t.conf,
                    publish_ms: t.publish_ms,
                    ts_ms,
                };
                if e.subs.prices {
                    e.world.price_tick(feed, p.clone());
                    e.call(strategy, |s, c| s.on_price(c, feed, &p));
                } else {
                    e.world.price_tick(feed, p);
                }
            }
            Ev::Book {
                market: m,
                levels,
                snapshot,
                ..
            } => {
                e.world.apply_book(m, levels, snapshot);
                e.world.markets[m].record_mark(ts_ms, false);
                e.flush(strategy);
                e.book_changed(strategy, m);
            }
            Ev::Fill(m, fill) => {
                e.world.recorded_fill(m, fill);
                e.world.markets[m].record_mark(ts_ms, false);
                e.flush(strategy);
                if e.subs.fills {
                    e.call(strategy, |s, c| s.on_fill(c, fill));
                }
            }
            Ev::MarketClose(m) => {
                e.world.markets[m].record_mark(ts_ms, true);
                e.world.close(m);
                e.flush(strategy);
                let addr = e.addrs[m].clone();
                e.call(strategy, |s, c| s.on_market_close(c, &addr));
                let info = &data.markets[m].info;
                if info.voided || info.winner.is_some() {
                    let at = info
                        .resolved_at
                        .map_or(ts_ms, |t| t.timestamp_millis())
                        .max(ts_ms);
                    e.schedule(at, Job::Resolve(m));
                }
            }
            // On-chain trades are a separate, late source; execution uses fills.
            Ev::Trade(..) => {}
        }
        e.check_held();
    }
    e.drain(strategy, i64::MAX, true);
    e.world.set_now(end_ms);
    e.call(strategy, |s, c| s.on_finish(c));
    e.flush(strategy);
    e.check_held();
    let last = e.sample();
    if e.equity.last().is_none_or(|p| p.0 < last.0) {
        e.equity.push(last);
    }
    let name = strategy.name();
    Ok(report::build(
        data, e.world, &e.equity, name, e.start_ms, e.end_ms,
    ))
}

impl Engine {
    fn schedule(&mut self, ts: i64, job: Job) {
        self.job_seq += 1;
        self.jobs.push(Reverse((ts, self.job_seq, job)));
    }

    /// Runs every job due at or before `until`. With `last`, periodic jobs past the end of the
    /// data are dropped.
    fn drain<S: Strategy + ?Sized>(&mut self, s: &mut S, until: i64, last: bool) {
        while let Some(&Reverse((ts, _, job))) = self.jobs.peek() {
            if ts > until {
                break;
            }
            self.jobs.pop();
            if last && job.periodic() && ts > self.end_ms {
                continue;
            }
            self.world.set_now(ts);
            self.job(s, job);
            self.check_held();
        }
    }

    fn sample(&self) -> report::Sample {
        let pos = self.world.positions_value();
        (self.world.now, self.world.acct.usdc + pos, pos)
    }

    /// Adds an equity point when the first position opens or the last one closes, so exposure
    /// is exact and not bound to the sampling step.
    fn check_held(&mut self) {
        if self.world.token_moves == self.held.1 {
            return;
        }
        let h = self.world.holds_tokens();
        self.held.1 = self.world.token_moves;
        if h != self.held.0 {
            self.held.0 = h;
            let p = self.sample();
            match self.equity.last_mut() {
                Some(last) if last.0 == p.0 => *last = p,
                _ => self.equity.push(p),
            }
        }
    }

    fn job<S: Strategy + ?Sized>(&mut self, s: &mut S, job: Job) {
        let now = self.world.now;
        match job {
            Job::Arrive(id) => {
                self.world.arrive(id);
                self.flush(s);
            }
            Job::Cancel(id) => {
                self.world.cancel(id);
                self.flush(s);
            }
            Job::Timer => {
                self.call(s, |s, c| s.on_timer(c));
                if let Some(t) = self.subs.timer_ms.filter(|t| *t > 0)
                    && now + (t as i64) <= self.end_ms
                {
                    self.schedule(now + t as i64, Job::Timer);
                }
            }
            Job::Book(m) => {
                self.book_due[m] = false;
                self.send_book(s, m);
            }
            Job::Resolve(m) => {
                let info = &self.world.markets[m].info;
                let r = Resolution {
                    winner: if info.voided { None } else { info.winner },
                    voided: info.voided,
                    resolved_price: info.resolved_price,
                };
                let addr = self.addrs[m].clone();
                self.world.resolve(m, r.clone());
                self.call(s, |s, c| s.on_resolution(c, &addr, &r));
            }
            Job::Equity => {
                let p = self.sample();
                match self.equity.last_mut() {
                    Some(last) if last.0 == p.0 => *last = p,
                    _ => self.equity.push(p),
                }
                let step = self.world.params.equity_interval_ms as i64;
                if now + step <= self.end_ms {
                    self.schedule(now + step, Job::Equity);
                }
            }
        }
    }

    fn book_changed<S: Strategy + ?Sized>(&mut self, s: &mut S, m: usize) {
        match self.subs.book {
            BookFeed::Off => {}
            BookFeed::EveryChange => self.send_book(s, m),
            BookFeed::Throttle(ms) => {
                let ms = ms as i64;
                match self.book_sent[m] {
                    Some(last) if self.world.now - last < ms => {
                        if !self.book_due[m] {
                            self.book_due[m] = true;
                            self.schedule(last + ms, Job::Book(m));
                        }
                    }
                    _ => self.send_book(s, m),
                }
            }
        }
    }

    fn send_book<S: Strategy + ?Sized>(&mut self, s: &mut S, m: usize) {
        self.book_sent[m] = Some(self.world.now);
        let addr = self.addrs[m].clone();
        self.call(s, |s, c| s.on_book(c, &addr));
    }

    /// Hands the world's order updates to the strategy.
    fn flush<S: Strategy + ?Sized>(&mut self, s: &mut S) {
        while !self.world.updates.is_empty() {
            for u in std::mem::take(&mut self.world.updates) {
                self.call(s, |s, c| s.on_order(c, &u));
            }
        }
    }

    /// Calls a handler, then sends what it decided.
    fn call<S: Strategy + ?Sized>(&mut self, s: &mut S, f: impl FnOnce(&mut S, &mut Ctx)) {
        let mut actions = std::mem::take(&mut self.actions);
        {
            let mut ctx = Ctx {
                world: &self.world,
                actions: &mut actions,
                next_id: &mut self.next_id,
            };
            f(s, &mut ctx);
        }
        let now = self.world.now;
        for a in actions.drain(..) {
            let jitter = self.world.params.latency_jitter_ms;
            match a {
                Action::Submit(o) => {
                    let id = o.id;
                    self.world.submit(*o);
                    let delay = self.world.params.submit_latency_ms + self.world.rng.up_to(jitter);
                    self.schedule(now + delay as i64, Job::Arrive(id));
                }
                Action::Cancel(id) => {
                    self.world.cancel_requested(id);
                    let delay = self.world.params.cancel_latency_ms + self.world.rng.up_to(jitter);
                    self.schedule(now + delay as i64, Job::Cancel(id));
                }
            }
        }
        self.actions = actions;
    }
}
