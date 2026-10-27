//! The execution model on synthetic books and fills. Market `market()`: 0 s to 300 s, up wins.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use super::report::REPORT_VERSION;
use super::*;
use crate::book::tests::market;
use crate::book::{BookHistory, BookSide, BookUpdate, Level};
use crate::export::Format;
use crate::fills::{Fill, Settlement};
use crate::loader::MarketData;
use crate::market::MarketInfo;
use crate::prices::PriceTick;
use crate::request::{Request, parse_time};
use crate::trades::Side;

fn lvl(outcome: u8, side: BookSide, price: f64, tokens: u64) -> Level {
    Level {
        outcome,
        side,
        price,
        size: tokens * 1_000_000,
    }
}

/// Up levels and their down mirrors, as the feed records them.
fn up(bids: &[(f64, u64)], asks: &[(f64, u64)]) -> Vec<Level> {
    let mut out = Vec::new();
    for &(p, s) in bids {
        out.push(lvl(0, BookSide::Bid, p, s));
        out.push(lvl(1, BookSide::Ask, 1.0 - p, s));
    }
    for &(p, s) in asks {
        out.push(lvl(0, BookSide::Ask, p, s));
        out.push(lvl(1, BookSide::Bid, 1.0 - p, s));
    }
    out
}

fn snap(ts: i64, levels: Vec<Level>) -> BookUpdate {
    BookUpdate::Snapshot {
        ts_ms: ts,
        sequence: ts as u64,
        levels,
    }
}

fn delta(ts: i64, changes: Vec<Level>) -> BookUpdate {
    BookUpdate::Delta {
        ts_ms: ts,
        sequence: ts as u64,
        changes,
    }
}

/// A recorded fill of another participant: the taker's outcome, side and price.
fn rec(m: &MarketInfo, ts: i64, j: i64, outcome: u8, side: Side, price: f64, tokens: u64) -> Fill {
    Fill {
        trade_id: format!("t{j}"),
        market: m.address.clone(),
        ts_ms: ts,
        journal_id: j,
        taker: "0xother".into(),
        side,
        outcome_index: Some(outcome),
        taker_asset_id: m.outcome_ids[outcome as usize].clone(),
        maker: None,
        maker_side: None,
        maker_outcome_index: None,
        maker_asset_id: None,
        price,
        size: tokens * 1_000_000,
        quantity: tokens as f64,
        amount_usd: price * tokens as f64,
        match_type: None,
        settlement: Settlement::Confirmed,
        tx_hash: None,
        block_number: None,
    }
}

fn dataset_with(info: MarketInfo, updates: Vec<BookUpdate>, fills: Vec<Fill>) -> Dataset {
    let tick = |ms: i64| PriceTick {
        feed: "btc-usd".into(),
        publish_ms: ms - 300,
        receive_ms: ms,
        price: 80_000.0 + ms as f64,
        conf: 1.0,
        slot: None,
    };
    let mut prices = BTreeMap::new();
    prices.insert(
        "btc-usd".to_string(),
        (0..300).map(|s| tick(s * 1000 + 450)).collect(),
    );
    Dataset {
        request: Request::builder()
            .range(parse_time("0").unwrap(), parse_time("400").unwrap())
            .build()
            .unwrap(),
        markets: vec![MarketData {
            book: Some(BookHistory {
                market: info.address.clone(),
                updates,
            }),
            fills: Some(fills),
            trades: None,
            info,
        }],
        prices,
        warnings: vec![],
    }
}

fn dataset(updates: Vec<BookUpdate>, fills: Vec<Fill>) -> Dataset {
    dataset_with(market(), updates, fills)
}

type Act = Box<dyn FnMut(&mut Ctx)>;

/// Runs actions at given times (multiples of 100 ms) and records order updates.
#[derive(Default)]
struct Script {
    acts: Vec<(i64, Act)>,
    ups: Vec<OrderUpdate>,
    resolutions: Vec<Resolution>,
}

impl Script {
    fn at(mut self, t: i64, f: impl FnMut(&mut Ctx) + 'static) -> Self {
        self.acts.push((t, Box::new(f)));
        self
    }

    fn fills(&self, id: OrderId) -> Vec<(f64, f64, Liquidity, Option<MatchType>)> {
        self.ups
            .iter()
            .filter(|u| u.order_id == id)
            .filter_map(|u| match &u.kind {
                OrderUpdateKind::Filled {
                    price,
                    quantity,
                    liquidity,
                    match_type,
                    ..
                } => Some((*price, *quantity, *liquidity, *match_type)),
                _ => None,
            })
            .collect()
    }

    fn filled(&self, id: OrderId) -> f64 {
        self.fills(id).iter().map(|f| f.1).sum()
    }

    fn last(&self, id: OrderId) -> &OrderUpdateKind {
        &self
            .ups
            .iter()
            .rev()
            .find(|u| u.order_id == id)
            .unwrap()
            .kind
    }
}

impl Strategy for Script {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions {
            timer_ms: Some(100),
            ..Subscriptions::default()
        }
    }
    fn on_timer(&mut self, ctx: &mut Ctx) {
        let now = ctx.now_ms();
        for (t, f) in &mut self.acts {
            if *t == now {
                f(ctx);
            }
        }
    }
    fn on_order(&mut self, _ctx: &mut Ctx, u: &OrderUpdate) {
        self.ups.push(u.clone());
    }
    fn on_resolution(&mut self, _ctx: &mut Ctx, _m: &str, r: &Resolution) {
        self.resolutions.push(r.clone());
    }
}

fn addr() -> String {
    market().address
}

fn buy_up_fak(tokens: u64, limit: f64) -> impl FnMut(&mut Ctx) {
    move |c: &mut Ctx| {
        c.market_order(&addr(), 0, Side::Buy, tokens, Some(limit))
            .unwrap();
    }
}

fn limit(outcome: u8, side: Side, price: f64, tokens: u64) -> impl FnMut(&mut Ctx) {
    move |c: &mut Ctx| {
        c.limit(&addr(), outcome, side, price, tokens).unwrap();
    }
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn market_order_walks_levels_with_slippage() {
    let ds = dataset(
        vec![snap(
            1000,
            up(&[(0.45, 10)], &[(0.5, 10), (0.51, 10), (0.52, 10)]),
        )],
        vec![],
    );
    let mut s = Script::default().at(2000, buy_up_fak(25, 0.52));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    let f = s.fills(1);
    assert_eq!(
        f.iter().map(|x| (x.0, x.1)).collect::<Vec<_>>(),
        vec![(0.5, 10.0), (0.51, 10.0), (0.52, 5.0)]
    );
    assert!(f.iter().all(|x| x.2 == Liquidity::Taker));
    assert_eq!(s.ups[0].ts_ms, 2100, "arrives after the submit latency");
    let o = &r.orders[0];
    assert!(close(o.avg_price.unwrap(), 0.508));
    assert!(close(o.slippage.unwrap(), 0.008));
    assert!(close(r.summary.avg_slippage.unwrap(), 0.008));
    assert!(close(r.summary.turnover, 12.7));
    // 25 up tokens, up wins: +25 − 12.7
    assert!(close(r.summary.pnl, 12.3));
    assert!(close(r.summary.end_equity, 1012.3));
}

#[test]
fn eaten_liquidity_is_not_taken_twice() {
    let ds = dataset(
        vec![
            snap(1000, up(&[], &[(0.5, 10), (0.51, 10)])),
            // the same size again: what we took stays taken
            delta(3000, up(&[], &[(0.5, 10)])),
            // the level grew: new orders, the eaten part is back
            delta(4000, up(&[], &[(0.5, 12)])),
        ],
        vec![],
    );
    let mut s = Script::default()
        .at(2000, buy_up_fak(15, 0.51))
        .at(2000, buy_up_fak(15, 0.51))
        .at(3500, buy_up_fak(5, 0.5))
        .at(4500, buy_up_fak(5, 0.5));
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    assert_eq!(s.filled(1), 15.0);
    assert_eq!(s.filled(2), 5.0, "only the rest of 0.51 is left");
    assert!(
        matches!(s.last(2), OrderUpdateKind::Cancelled { reason, remaining } if reason == "fak_remainder" && *remaining == 10.0)
    );
    assert_eq!(s.filled(3), 0.0, "0.50 is still eaten");
    assert_eq!(s.filled(4), 5.0);
}

/// Book: bid 0.40 × 50 when we join (queue 50), others join behind (100), then 40 cancel (60),
/// then a taker sells 55 into 0.40.
fn queue_run(model: QueueModel) -> f64 {
    let m = market();
    let ds = dataset(
        vec![
            snap(1000, up(&[(0.4, 50)], &[(0.45, 50)])),
            delta(2500, up(&[(0.4, 100)], &[])),
            delta(3000, up(&[(0.4, 60)], &[])),
        ],
        vec![rec(&m, 4000, 1, 0, Side::Sell, 0.4, 55)],
    );
    let mut s = Script::default().at(2000, limit(0, Side::Buy, 0.4, 10));
    let p = ExecParams {
        queue_model: model,
        ..ExecParams::default()
    };
    run(&ds, &p, &mut s).unwrap();
    assert!(
        s.fills(1)
            .iter()
            .all(|x| x.0 == 0.4 && x.2 == Liquidity::Maker && x.3 == Some(MatchType::Direct))
    );
    s.filled(1)
}

#[test]
fn queue_pessimistic_keeps_place_on_cancels() {
    // queue stays 50: the fill of 55 leaves 5 for us
    assert_eq!(queue_run(QueueModel::Pessimistic), 5.0);
}

#[test]
fn queue_proportional_moves_up_on_cancels() {
    // 40 of 100 cancelled: queue 50 → 30, the fill of 55 fills all 10
    assert_eq!(queue_run(QueueModel::Proportional), 10.0);
}

#[test]
fn fills_explain_book_drops_and_do_not_count_twice() {
    let m = market();
    // queue 50; a taker sells 30 (queue 20), then the book shows that drop of 30
    let ds = dataset(
        vec![
            snap(1000, up(&[(0.4, 50)], &[(0.45, 50)])),
            delta(4100, up(&[(0.4, 20)], &[])),
        ],
        vec![
            rec(&m, 4000, 1, 0, Side::Sell, 0.4, 30),
            rec(&m, 5000, 2, 0, Side::Sell, 0.4, 25),
        ],
    );
    let mut s = Script::default().at(2000, limit(0, Side::Buy, 0.4, 10));
    let p = ExecParams {
        queue_model: QueueModel::Proportional,
        ..ExecParams::default()
    };
    run(&ds, &p, &mut s).unwrap();
    // the drop is the fill, not cancels: queue 20, the second fill of 25 gives us 5
    assert_eq!(s.filled(1), 5.0);
}

#[test]
fn book_trading_through_fills_at_our_price() {
    let ds = dataset(
        vec![
            snap(1000, up(&[(0.38, 50)], &[(0.45, 50)])),
            // asks come down through our bid
            delta(3000, up(&[], &[(0.39, 30)])),
        ],
        vec![],
    );
    let mut s = Script::default().at(2000, limit(0, Side::Buy, 0.4, 10));
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    let f = s.fills(1);
    assert_eq!(f.len(), 1);
    assert_eq!((f[0].0, f[0].1, f[0].2), (0.4, 10.0, Liquidity::Maker));
    let at = s
        .ups
        .iter()
        .find(|u| u.order_id == 1 && matches!(u.kind, OrderUpdateKind::Filled { .. }))
        .unwrap()
        .ts_ms;
    assert_eq!(at, 3000);
}

#[test]
fn order_fills_before_its_cancel_arrives() {
    let m = market();
    let ds = dataset(
        vec![snap(1000, up(&[(0.39, 50)], &[(0.45, 50)]))],
        // a taker sells into 0.39: our better bids are taken first, at our price
        vec![rec(&m, 3050, 1, 0, Side::Sell, 0.39, 20)],
    );
    let mut s = Script::default()
        .at(2000, limit(0, Side::Buy, 0.4, 10))
        .at(2000, limit(0, Side::Buy, 0.41, 5))
        .at(2500, |c: &mut Ctx| c.cancel(2).unwrap())
        .at(3000, |c: &mut Ctx| c.cancel(1).unwrap());
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    // order 2 was cancelled in time; order 1's cancel (3100) came after the fill (3050)
    assert!(
        matches!(s.last(2), OrderUpdateKind::Cancelled { reason, .. } if reason == "cancelled")
    );
    assert_eq!(s.filled(1), 10.0);
    assert_eq!(s.fills(1)[0].0, 0.4);
    assert!(
        matches!(s.last(1), OrderUpdateKind::CancelFailed { reason } if reason == "order_done")
    );
}

#[test]
fn mint_and_merge_through_the_other_outcome() {
    let m = market();
    let ds = dataset(
        vec![
            // only up's side is recorded: a buy of down meets up's bid
            snap(
                1000,
                vec![
                    lvl(0, BookSide::Bid, 0.6, 20),
                    lvl(0, BookSide::Ask, 0.75, 20),
                ],
            ),
        ],
        vec![
            // a taker buys up at 0.65: meets our resting buy of down at 0.35 (mint)
            rec(&m, 4000, 1, 0, Side::Buy, 0.65, 3),
            // a taker sells up at 0.70: meets our resting sell of down at 0.30 (merge)
            rec(&m, 6000, 2, 0, Side::Sell, 0.7, 10),
        ],
    );
    let mut s = Script::default()
        .at(2000, |c: &mut Ctx| {
            c.market_order(&addr(), 1, Side::Buy, 10, Some(0.4))
                .unwrap();
        })
        .at(3000, limit(1, Side::Buy, 0.35, 3))
        .at(5000, limit(1, Side::Sell, 0.3, 10));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    let f = s.fills(1);
    assert_eq!((f[0].0, f[0].1), (0.4, 10.0), "down bought at 1 − 0.60");
    assert_eq!(
        s.fills(2),
        vec![(0.35, 3.0, Liquidity::Maker, Some(MatchType::Mint))]
    );
    assert_eq!(
        s.fills(3),
        vec![(0.3, 10.0, Liquidity::Maker, Some(MatchType::Merge))]
    );
    // down: +10 +3 −10 = 3 tokens left, up wins and they pay nothing
    assert!(close(r.summary.pnl, -4.0 - 1.05 + 3.0));
}

#[test]
fn down_book_is_the_mirror_of_up() {
    struct Look(Vec<(Option<Quote>, Option<Quote>)>);
    impl Strategy for Look {
        fn on_book(&mut self, ctx: &mut Ctx, m: &str) {
            self.0.push((ctx.best_bid(m, 1), ctx.best_ask(m, 1)));
        }
    }
    let ds = dataset(
        vec![snap(
            1000,
            vec![
                lvl(0, BookSide::Bid, 0.6, 20),
                lvl(0, BookSide::Ask, 0.75, 7),
            ],
        )],
        vec![],
    );
    let mut s = Look(vec![]);
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    let (bid, ask) = s.0[0];
    assert!(close(bid.unwrap().price, 0.25) && bid.unwrap().tokens == 7.0);
    assert!(close(ask.unwrap().price, 0.4) && ask.unwrap().tokens == 20.0);
}

#[test]
fn payout_on_win_and_void() {
    let book = || vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))];
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&dataset(book(), vec![]), &ExecParams::default(), &mut s).unwrap();
    assert!(close(r.summary.pnl, 5.0));
    assert!(close(r.summary.end_equity, 1005.0));
    assert_eq!(r.markets[0].result, "up");
    assert!(close(r.markets[0].payout, 10.0));
    assert_eq!(s.resolutions[0].winner, Some(0));

    let mut voided = market();
    voided.winner = None;
    voided.voided = true;
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(
        &dataset_with(voided, book(), vec![]),
        &ExecParams::default(),
        &mut s,
    )
    .unwrap();
    assert!(close(r.markets[0].payout, 5.0), "0.5 per token");
    assert!(close(r.summary.pnl, 0.0));
    assert_eq!(r.markets[0].result, "void");

    let p = ExecParams {
        redeem_fee_bps: 200,
        fee_bps: 100,
        ..ExecParams::default()
    };
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&dataset(book(), vec![]), &p, &mut s).unwrap();
    // −5 − 0.05 trading fee + 10 − 0.2 redemption fee
    assert!(close(r.summary.pnl, 4.75));
    assert!(close(r.summary.fees, 0.25));
}

#[test]
fn unresolved_market_is_marked_at_the_book() {
    let mut open = market();
    open.winner = None;
    let ds = dataset_with(
        open,
        vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))],
        vec![],
    );
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    // the 0.50 ask is eaten from the view, but the mark uses the recorded mid 0.475
    assert!(close(r.markets[0].open_value, 4.75));
    assert!(close(r.summary.pnl, -0.25));
    assert!(r.warnings.iter().any(|w| w.contains("no result")));
}

#[test]
fn checks_on_arrival() {
    let ds = dataset(vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))], vec![]);
    let mut s = Script::default()
        .at(2000, |c: &mut Ctx| {
            // 1 token at 0.5 = $0.5 < $1
            c.limit(&addr(), 0, Side::Buy, 0.5, 1).unwrap();
            // no down tokens to sell
            c.limit(&addr(), 1, Side::Sell, 0.5, 10).unwrap();
            // more than the account holds
            c.limit(&addr(), 0, Side::Buy, 0.4, 5000).unwrap();
            assert!(c.limit(&addr(), 0, Side::Buy, 0.401, 5).is_err());
            assert!(c.limit("0x00", 0, Side::Buy, 0.4, 5).is_err());
            assert!(c.limit(&addr(), 0, Side::Buy, 0.4, 0).is_err());
        })
        // arrives after the end
        .at(299_900, buy_up_fak(10, 0.5));
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    let reason = |id| match s.last(id) {
        OrderUpdateKind::Rejected { reason } => reason.clone(),
        k => panic!("{k:?}"),
    };
    assert_eq!(reason(1), "order_notional_below_minimum");
    assert_eq!(reason(2), "insufficient_tokens");
    assert_eq!(reason(3), "insufficient_balance");
    assert_eq!(reason(4), "market_not_open");
}

#[test]
fn resting_orders_hold_usdc_and_leave_at_close() {
    let ds = dataset(vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))], vec![]);
    let free = Rc::new(Cell::new(0.0));
    let seen = free.clone();
    let mut s = Script::default()
        .at(2000, limit(0, Side::Buy, 0.4, 10))
        .at(3000, move |c: &mut Ctx| seen.set(c.usdc()));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    assert!(close(free.get(), 996.0));
    assert!(
        matches!(s.last(1), OrderUpdateKind::Cancelled { reason, .. } if reason == "market_closed")
    );
    assert!(close(r.summary.end_equity, 1000.0));
    assert_eq!(r.summary.limit_fill_rate, Some(0.0));
}

#[test]
fn own_resting_order_is_cancelled_on_self_trade() {
    let ds = dataset(vec![snap(1000, up(&[(0.3, 50)], &[(0.7, 50)]))], vec![]);
    let mut s = Script::default()
        .at(2000, buy_up_fak(10, 0.7))
        .at(3000, limit(0, Side::Sell, 0.6, 10))
        .at(4000, buy_up_fak(5, 0.65));
    run(&ds, &ExecParams::default(), &mut s).unwrap();
    assert!(
        matches!(s.last(2), OrderUpdateKind::Cancelled { reason, .. } if reason == "self_trade")
    );
    assert_eq!(s.filled(3), 0.0);
}

#[test]
fn reverted_settlement_undoes_the_fill() {
    let ds = dataset(vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))], vec![]);
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let p = ExecParams {
        revert_probability: 1.0,
        ..ExecParams::default()
    };
    let r = run(&ds, &p, &mut s).unwrap();
    assert!(
        s.ups
            .iter()
            .any(|u| matches!(u.kind, OrderUpdateKind::SettlementFailed { .. }))
    );
    assert!(close(r.summary.end_equity, 1000.0));
    assert_eq!((r.summary.fills, r.summary.reverted_fills), (0, 1));
}

fn busy_run(seed: u64) -> Report {
    let m = market();
    let mut updates = vec![snap(1000, up(&[(0.45, 500)], &[(0.5, 500)]))];
    let mut fills = vec![];
    for i in 0..50 {
        let ts = 2000 + i * 3000;
        let p = 0.40 + (i % 5) as f64 * 0.0025;
        updates.push(delta(ts, up(&[(p, 100 + i as u64)], &[(p + 0.05, 80)])));
        fills.push(rec(&m, ts + 1500, i, 0, Side::Sell, p, 30));
    }
    let ds = dataset(updates, fills);
    let mut s = Script::default();
    for i in 0..50 {
        let t = 2000 + i * 3000;
        let p = 0.40 + (i % 5) as f64 * 0.0025;
        s = s.at(t, move |c: &mut Ctx| {
            c.limit(&addr(), 0, Side::Buy, p, 5).unwrap();
            c.market_order(&addr(), 0, Side::Buy, 3, Some(0.6)).unwrap();
        });
    }
    let params = ExecParams {
        latency_jitter_ms: 400,
        revert_probability: 0.3,
        seed,
        ..ExecParams::default()
    };
    run(&ds, &params, &mut s).unwrap()
}

#[test]
fn same_seed_same_report() {
    let a = busy_run(7).to_json();
    assert_eq!(a, busy_run(7).to_json());
    assert_ne!(a, busy_run(8).to_json(), "the seed matters");
    let r = busy_run(7);
    assert!(r.summary.fills > 0 && r.summary.reverted_fills > 0);
    let back: Report = serde_json::from_str(&a).unwrap();
    assert_eq!(back.journal.len(), r.journal.len());
}

#[test]
fn strategy_sees_nothing_from_the_future() {
    struct Watch {
        history: BookHistory,
        checks: usize,
    }
    impl Watch {
        fn check(&mut self, ctx: &Ctx) {
            let now = ctx.now_ms();
            let m = ctx.markets().remove(0);
            if let Some(p) = ctx.price("btc-usd") {
                assert!(p.ts_ms <= now, "price from {} at {now}", p.ts_ms);
            }
            if now < m.start_ms {
                assert_eq!(m.strike, None);
            }
            if now < m.end_ms {
                assert!(m.resolution.is_none());
            }
            // records at `now` itself may come after this handler, never later ones
            let ok = |at: Option<crate::book::OrderBook>| match (ctx.book(&m.address), at) {
                (Some(seen), Some(rec)) => seen.outcomes == rec.outcomes,
                (None, None) => true,
                _ => false,
            };
            assert!(
                ok(self.history.at(now)) || ok(self.history.at(now - 1)),
                "at {now}"
            );
            self.checks += 1;
        }
    }
    impl Strategy for Watch {
        fn subscriptions(&self) -> Subscriptions {
            Subscriptions {
                timer_ms: Some(250),
                ..Subscriptions::default()
            }
        }
        fn on_book(&mut self, ctx: &mut Ctx, _m: &str) {
            self.check(ctx);
        }
        fn on_price(&mut self, ctx: &mut Ctx, _f: &str, p: &PriceView) {
            assert!(p.ts_ms <= ctx.now_ms());
            self.check(ctx);
        }
        fn on_timer(&mut self, ctx: &mut Ctx) {
            self.check(ctx);
        }
        fn on_market_close(&mut self, ctx: &mut Ctx, m: &str) {
            assert!(ctx.market(m).unwrap().resolution.is_none());
        }
    }
    let updates = vec![
        snap(1000, up(&[(0.45, 50)], &[(0.5, 50)])),
        delta(2000, up(&[(0.46, 10)], &[])),
        delta(2600, up(&[(0.45, 0)], &[(0.51, 3)])),
        snap(60_000, up(&[(0.3, 5)], &[(0.7, 5)])),
    ];
    let ds = dataset(updates.clone(), vec![]);
    let mut w = Watch {
        history: BookHistory {
            market: addr(),
            updates,
        },
        checks: 0,
    };
    run(&ds, &ExecParams::default(), &mut w).unwrap();
    assert!(w.checks > 1000);
}

#[test]
fn throttled_book_and_split_report() {
    struct Count(Vec<i64>);
    impl Strategy for Count {
        fn subscriptions(&self) -> Subscriptions {
            Subscriptions {
                book: BookFeed::Throttle(1000),
                ..Subscriptions::default()
            }
        }
        fn on_book(&mut self, ctx: &mut Ctx, _m: &str) {
            self.0.push(ctx.now_ms());
        }
    }
    let ds = dataset(
        (0..10)
            .map(|i| delta(1000 + i * 100, up(&[(0.4, 10 + i as u64)], &[])))
            .collect(),
        vec![],
    );
    let mut c = Count(vec![]);
    let r = run(&ds, &ExecParams::default(), &mut c).unwrap();
    assert_eq!(
        c.0,
        vec![1000, 2000],
        "first change, then the latest a second later"
    );
    let p = r.periods.unwrap();
    assert_eq!(p.split_ms, 280_000);
    // the market ends at 300 s, after the split
    assert_eq!(r.markets[0].sample, "validation");
    assert!(r.warnings.iter().any(|w| w.contains("do not change")));
    assert!(r.warnings.iter().any(|w| w.contains("too few")));
}

#[test]
fn journal_exports() {
    let ds = dataset(vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))], vec![]);
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    let events: Vec<&str> = r.journal.iter().map(|j| j.event.as_str()).collect();
    assert_eq!(events, vec!["submitted", "accepted", "fill", "payout"]);
    let csv = String::from_utf8(report::journal_bytes(&r.journal, Format::Csv)).unwrap();
    assert!(csv.starts_with("ts,event,order_id,market,"));
    assert!(csv.lines().nth(3).unwrap().contains(",fill,1,"));
    assert!(csv.contains(",buy,fak,0.5,10,0,taker,,"));
    let pq = report::journal_bytes(&r.journal, Format::Parquet);
    let batches = crate::pq::read_batches(bytes::Bytes::from(pq), "test").unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[test]
fn exposure_follows_the_position_exactly() {
    let ds = dataset(vec![snap(1000, up(&[(0.45, 50)], &[(0.5, 50)]))], vec![]);
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&ds, &ExecParams::default(), &mut s).unwrap();
    // a point where the position opens (order arrives at 2100) and where it is paid out
    let open = r.equity.iter().find(|p| p.ts_ms == 2100).unwrap();
    assert!(close(open.positions, 4.75), "10 up at the mid 0.475");
    let paid = r
        .journal
        .iter()
        .find(|j| j.event == "payout")
        .unwrap()
        .ts_ms;
    let closed = r.equity.iter().find(|p| p.ts_ms == paid).unwrap();
    assert_eq!(closed.positions, 0.0);
    let m = &r.summary;
    let span = (r.equity.last().unwrap().ts_ms - r.equity[0].ts_ms) as f64;
    assert!(close(m.exposure_time.unwrap(), (paid - 2100) as f64 / span));
    assert!(close(m.max_in_positions.unwrap(), 4.75));
    assert!(m.avg_in_positions.unwrap() > 0.0);
    // one winning market: no losses, so no profit factor
    assert_eq!(m.profit_factor, None);
    assert_eq!(m.best_market.as_ref().unwrap().market, addr());
    // the market with an order carries its price path
    assert!(!r.markets[0].marks.is_empty());
    assert!(close(r.markets[0].marks[0].1, 0.475));
    assert_eq!(r.version, REPORT_VERSION);
}

#[test]
fn risk_metrics_per_sample() {
    let r = busy_run(3);
    let p = r.periods.as_ref().unwrap();
    for m in [&r.summary, &p.fit, &p.validation] {
        assert_eq!(m.risk_interval_ms, 3_600_000);
        assert_eq!(m.annualization, 1.0);
    }
    // a run of 400 s has no full hour: Sharpe is undefined, not NaN
    assert_eq!(r.summary.sharpe, None);
    let json = r.to_json();
    assert!(!json.contains("NaN") && !json.contains("Infinity"));
    let short = ExecParams {
        risk_interval_ms: 10_000,
        equity_interval_ms: 1000,
        ..ExecParams::default()
    };
    let ds = dataset(
        (0..30)
            .map(|i| {
                let p = 0.40 + (i % 7) as f64 * 0.01;
                snap(1000 + i * 10_000, up(&[(p, 50)], &[(p + 0.02, 50)]))
            })
            .collect(),
        vec![],
    );
    let mut s = Script::default().at(2000, buy_up_fak(10, 0.5));
    let r = run(&ds, &short, &mut s).unwrap();
    assert!(r.summary.sharpe.is_some_and(f64::is_finite));
    assert!(r.summary.max_drawdown_duration_ms.is_some());
    let back = Report::from_json(&r.to_json()).unwrap();
    assert_eq!(back, r);
}

#[test]
fn html_report_is_self_contained() {
    let mut r = busy_run(5);
    r.strategy = "x</script><img src=x onerror=alert(1)>".into();
    r.warnings
        .push("warning with <b>tags</b> & </script>".into());
    let h = r.to_html();
    assert!(h.starts_with("<!doctype html>"));
    assert!(h.trim_end().ends_with("</html>"));
    for id in [
        "id=\"header\"",
        "id=\"warnings\"",
        "id=\"metrics\"",
        "id=\"equity\"",
        "id=\"drawdown\"",
        "id=\"market-pnl\"",
        "id=\"market-detail\"",
        "id=\"markets\"",
        "id=\"orders\"",
    ] {
        assert!(h.contains(id), "missing {id}");
    }
    // the warnings come before the charts
    assert!(h.find("id=\"warnings\"").unwrap() < h.find("id=\"equity\"").unwrap());
    // nothing loaded from outside
    for bad in [
        "<link",
        "src=\"http",
        "href=\"http",
        "url(",
        "@import",
        "<iframe",
        "<img",
    ] {
        assert!(!h.contains(bad), "found {bad}");
    }
    // exactly two script elements, the data one parses back to the report's numbers
    assert_eq!(h.matches("<script").count(), 2);
    assert_eq!(h.matches("</script>").count(), 2);
    let start = h
        .find("<script type=\"application/json\" id=\"data\">")
        .unwrap();
    let body = &h[start..];
    let body = &body[body.find('>').unwrap() + 1..body.find("</script>").unwrap()];
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["strategy"], serde_json::json!(r.strategy));
    assert_eq!(v["equity"].as_array().unwrap().len(), r.equity.len());
    assert_eq!(
        v["fills"].as_array().unwrap().len(),
        r.journal.iter().filter(|j| j.event == "fill").count()
    );
    assert!(v.get("journal").is_none());
    // every metric card shows fit and validation
    assert_eq!(
        h.matches("class=\"card\"").count(),
        h.matches("class=\"split\"").count()
    );
    // tags in text are escaped
    assert!(h.contains("warning with &lt;b&gt;tags&lt;/b&gt; &amp; &lt;/script&gt;"));
}
