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

mod params;
mod report;
mod strategy;
mod world;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use crate::error::Result;
use crate::event::EventKind;
use crate::loader::Dataset;

pub use params::{ExecParams, QueueModel};
pub use report::{EquityPoint, MarketReport, Metrics, OrderRecord, Periods, Report, write_journal};
pub use strategy::{
    BookFeed, Ctx, Liquidity, MarketView, MatchType, OrderError, OrderId, OrderUpdate,
    OrderUpdateKind, OrderView, PriceView, Quote, Resolution, Strategy, Subscriptions, TimeInForce,
};
pub use world::JournalRow;

use strategy::Action;
use world::World;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Scheduled jobs by (time, order of scheduling).
    jobs: BTreeMap<(i64, u64), Job>,
    job_seq: u64,
    next_id: OrderId,
    subs: Subscriptions,
    book_sent: Vec<Option<i64>>,
    book_due: Vec<bool>,
    equity: Vec<(i64, i64)>,
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
    let events = data.events();
    let start_ms = data
        .request
        .from
        .map(|t| t.timestamp_millis())
        .or(events.first().map(|e| e.ts_ms))
        .unwrap_or(0);
    let end_ms = data
        .request
        .to
        .map(|t| t.timestamp_millis())
        .or(events.last().map(|e| e.ts_ms))
        .unwrap_or(start_ms);
    let infos = data.markets.iter().map(|m| m.info.clone()).collect();
    let n = data.markets.len();
    let mut e = Engine {
        world: World::new(infos, params.clone()),
        jobs: BTreeMap::new(),
        job_seq: 0,
        next_id: 0,
        subs: strategy.subscriptions(),
        book_sent: vec![None; n],
        book_due: vec![false; n],
        equity: Vec::new(),
        start_ms,
        end_ms,
    };
    e.world.now = start_ms;
    e.call(strategy, |s, c| s.on_start(c));
    if let Some(t) = e.subs.timer_ms.filter(|t| *t > 0) {
        e.schedule(start_ms + t as i64, Job::Timer);
    }
    e.schedule(start_ms, Job::Equity);

    for ev in &events {
        e.drain(strategy, ev.ts_ms, false);
        e.world.now = ev.ts_ms;
        let m = ev
            .market
            .as_deref()
            .and_then(|a| e.world.index.get(a).copied());
        match (&ev.kind, m) {
            (EventKind::MarketOpen { .. }, Some(m)) => {
                let addr = e.world.markets[m].info.address.clone();
                e.call(strategy, |s, c| s.on_market_open(c, &addr));
            }
            (
                EventKind::PriceTick {
                    feed,
                    price,
                    conf,
                    publish_ms,
                },
                _,
            ) => {
                let p = PriceView {
                    price: *price,
                    conf: *conf,
                    publish_ms: *publish_ms,
                    ts_ms: ev.ts_ms,
                };
                e.world.price_tick(feed, p.clone());
                if e.subs.prices {
                    e.call(strategy, |s, c| s.on_price(c, feed, &p));
                }
            }
            (EventKind::BookSnapshot { levels, .. }, Some(m)) => {
                e.world.apply_book(m, levels, true);
                e.flush(strategy);
                e.book_changed(strategy, m);
            }
            (EventKind::BookDelta { changes }, Some(m)) => {
                e.world.apply_book(m, changes, false);
                e.flush(strategy);
                e.book_changed(strategy, m);
            }
            (EventKind::Fill { fill }, Some(m)) => {
                e.world.recorded_fill(m, fill);
                e.flush(strategy);
                if e.subs.fills {
                    e.call(strategy, |s, c| s.on_fill(c, fill));
                }
            }
            (EventKind::MarketClose { .. }, Some(m)) => {
                e.world.close(m);
                e.flush(strategy);
                let addr = e.world.markets[m].info.address.clone();
                e.call(strategy, |s, c| s.on_market_close(c, &addr));
                let info = &data.markets[m].info;
                if info.voided || info.winner.is_some() {
                    let at = info
                        .resolved_at
                        .map_or(ev.ts_ms, |t| t.timestamp_millis())
                        .max(ev.ts_ms);
                    e.schedule(at, Job::Resolve(m));
                }
            }
            // On-chain trades are a separate, late source; execution uses fills.
            _ => {}
        }
    }
    e.drain(strategy, i64::MAX, true);
    e.world.now = e.world.now.max(end_ms);
    e.call(strategy, |s, c| s.on_finish(c));
    e.flush(strategy);
    let last = (e.world.now, e.world.equity());
    if e.equity.last().is_none_or(|p| p.0 < last.0) {
        e.equity.push(last);
    }
    Ok(report::build(
        data,
        &e.world,
        &e.equity,
        strategy.name(),
        e.start_ms,
        e.end_ms,
    ))
}

impl Engine {
    fn schedule(&mut self, ts: i64, job: Job) {
        self.job_seq += 1;
        self.jobs.insert((ts, self.job_seq), job);
    }

    /// Runs every job due at or before `until`. With `last`, periodic jobs past the end of the
    /// data are dropped.
    fn drain<S: Strategy + ?Sized>(&mut self, s: &mut S, until: i64, last: bool) {
        while let Some((&(ts, seq), &job)) = self.jobs.first_key_value() {
            if ts > until {
                break;
            }
            self.jobs.remove(&(ts, seq));
            if last && job.periodic() && ts > self.end_ms {
                continue;
            }
            self.world.now = self.world.now.max(ts);
            self.job(s, job);
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
                let addr = info.address.clone();
                self.world.resolve(m, r.clone());
                self.call(s, |s, c| s.on_resolution(c, &addr, &r));
            }
            Job::Equity => {
                self.equity.push((now, self.world.equity()));
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
        let addr = self.world.markets[m].info.address.clone();
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
        let mut actions = Vec::new();
        {
            let mut ctx = Ctx {
                world: &self.world,
                actions: &mut actions,
                next_id: &mut self.next_id,
            };
            f(s, &mut ctx);
        }
        let now = self.world.now;
        for a in actions {
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
    }
}
