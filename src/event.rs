//! The event stream: every market change in one time-ordered sequence. This is the contract
//! between the loader and the backtest engine; `docs/data-format.md` describes it.

use serde::{Deserialize, Serialize};

use crate::book::{BookUpdate, Level};
use crate::fills::Fill;
use crate::loader::Dataset;
use crate::prices::PriceTick;
use crate::trades::Trade;

/// One event. Events are sorted by `ts_ms`, then by kind (see [`EventKind::rank`]), then by
/// market or feed, then by their own order (book sequence, fill journal id, block and log index,
/// publish time).
/// `seq` is the position in the stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Position in the stream, from 0.
    pub seq: u64,
    /// Unix milliseconds, UTC.
    pub ts_ms: i64,
    /// Market address; None for price ticks (they belong to a feed, shared by markets).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market: Option<String>,
    #[serde(flatten)]
    /// What happened.
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Kinds of events in the stream.
pub enum EventKind {
    /// The market starts trading (at `start`); the strike is the price at that moment.
    MarketOpen {
        /// Strike, USD, once known.
        strike: Option<f64>,
    },
    /// A Pyth update, at the time the recorder received it.
    PriceTick {
        /// Feed name, e.g. `btc-usd`.
        feed: String,
        /// Price, USD.
        price: f64,
        /// Confidence interval, USD.
        conf: f64,
        /// Pyth publish time, unix ms.
        publish_ms: i64,
    },
    /// The complete book of both outcomes; replaces the previous book. `synthetic` = rebuilt
    /// from history at the range start, not recorded at this moment.
    BookSnapshot {
        /// Every level of both outcomes.
        levels: Vec<Level>,
        /// Rebuilt by the loader, not recorded at this moment.
        synthetic: bool,
    },
    /// Levels set to a new size; size 0 removes the level.
    BookDelta {
        /// Changed levels.
        changes: Vec<Level>,
    },
    /// A match of the order book, at the order book's time. Includes matches whose settlement
    /// failed: they took volume off the book all the same.
    Fill {
        /// The fill.
        fill: Fill,
    },
    /// An on-chain fill, at the time of the block that settled it. A separate source from
    /// `Fill`: the same match can appear as both.
    Trade {
        /// The trade.
        trade: Trade,
    },
    /// The market expires (at `end`) with its result.
    MarketClose {
        /// Winning outcome, if known and not voided.
        winner: Option<u8>,
        /// Final price of the underlying, USD.
        resolved_price: Option<f64>,
        /// Whether the market was voided.
        voided: bool,
    },
}

impl EventKind {
    /// Order of kinds at the same millisecond.
    pub fn rank(&self) -> u8 {
        match self {
            EventKind::MarketOpen { .. } => 0,
            EventKind::PriceTick { .. } => 1,
            EventKind::BookSnapshot {
                synthetic: true, ..
            } => 2,
            EventKind::BookSnapshot { .. } | EventKind::BookDelta { .. } => 3,
            EventKind::Fill { .. } => 4,
            EventKind::Trade { .. } => 5,
            EventKind::MarketClose { .. } => 6,
        }
    }

    /// Short name: `market_open`, `price_tick`, `book_snapshot` and so on.
    pub fn name(&self) -> &'static str {
        match self {
            EventKind::MarketOpen { .. } => "market_open",
            EventKind::PriceTick { .. } => "price_tick",
            EventKind::BookSnapshot { .. } => "book_snapshot",
            EventKind::BookDelta { .. } => "book_delta",
            EventKind::Fill { .. } => "fill",
            EventKind::Trade { .. } => "trade",
            EventKind::MarketClose { .. } => "market_close",
        }
    }
}

/// Sort key: time, kind, market or feed (by name), then order within the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub ts_ms: i64,
    rank: u8,
    source: u32,
    a: u64,
    b: u64,
}

/// Where an event's data lives in the dataset: market index, then the item's index.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Src {
    Open(usize),
    Close(usize),
    /// The book rebuilt at the range start (`Stream::synthetic`).
    Synthetic(usize),
    Book(usize, usize),
    Fill(usize, usize),
    Trade(usize, usize),
    /// Feed index in `Dataset::prices` order, tick index.
    Price(usize, usize),
}

/// An event borrowed from the dataset.
pub(crate) enum Ev<'a> {
    MarketOpen(usize),
    PriceTick(&'a str, &'a PriceTick),
    Book {
        market: usize,
        levels: &'a [Level],
        snapshot: bool,
        synthetic: bool,
    },
    Fill(usize, &'a Fill),
    Trade(usize, &'a Trade),
    MarketClose(usize),
}

/// The stream of a dataset in order, as positions in the dataset: nothing is copied.
pub(crate) struct Stream<'a> {
    ds: &'a Dataset,
    feeds: Vec<(&'a str, &'a [PriceTick])>,
    synthetic: Vec<Vec<Level>>,
    pub order: Vec<(Key, Src)>,
}

const RANK_OPEN: u8 = 0;
const RANK_PRICE: u8 = 1;
const RANK_SYNTHETIC: u8 = 2;
const RANK_BOOK: u8 = 3;
const RANK_FILL: u8 = 4;
const RANK_TRADE: u8 = 5;
const RANK_CLOSE: u8 = 6;

fn key(ts_ms: i64, rank: u8, source: u32, a: u64, b: u64) -> Key {
    Key {
        ts_ms,
        rank,
        source,
        a,
        b,
    }
}

impl<'a> Stream<'a> {
    /// The stream of a loaded dataset, clipped to the request range.
    pub fn new(ds: &'a Dataset) -> Self {
        let req = &ds.request;
        let from_ms = req.from.map(|t| t.timestamp_millis());
        let to_ms = req.to.map(|t| t.timestamp_millis());
        let in_range = |ms: i64| from_ms.is_none_or(|f| ms >= f) && to_ms.is_none_or(|t| ms < t);
        // Sources sort by name: markets by address, feeds by feed name.
        let mut names: Vec<&str> = ds
            .markets
            .iter()
            .map(|m| m.info.address.as_str())
            .chain(ds.prices.keys().map(String::as_str))
            .collect();
        names.sort_unstable();
        names.dedup();
        let source = |name: &str| names.binary_search(&name).expect("known source") as u32;

        let mut order: Vec<(Key, Src)> = Vec::new();
        let mut synthetic = vec![Vec::new(); ds.markets.len()];
        for (mi, m) in ds.markets.iter().enumerate() {
            let info = &m.info;
            let src = source(&info.address);
            let start = info.start.timestamp_millis();
            let end = info.end.timestamp_millis();
            if in_range(start) {
                order.push((key(start, RANK_OPEN, src, 0, 0), Src::Open(mi)));
            }
            if in_range(end) {
                order.push((key(end, RANK_CLOSE, src, 0, 0), Src::Close(mi)));
            }
            if let Some(h) = &m.book {
                // The range starts in the middle of the history: begin with the book as it was.
                if let Some(f) = from_ms
                    && h.first_ts_ms().is_some_and(|t| t < f)
                    && let Some(book) = h.at(f - 1)
                {
                    synthetic[mi] = book.levels();
                    order.push((key(f, RANK_SYNTHETIC, src, 0, 0), Src::Synthetic(mi)));
                }
                for (i, u) in h.updates.iter().enumerate() {
                    if in_range(u.ts_ms()) {
                        let k = key(u.ts_ms(), RANK_BOOK, src, u.sequence(), i as u64);
                        order.push((k, Src::Book(mi, i)));
                    }
                }
            }
            for (i, f) in m.fills.iter().flatten().enumerate() {
                if in_range(f.ts_ms) {
                    let k = key(f.ts_ms, RANK_FILL, src, f.journal_id.max(0) as u64, 0);
                    order.push((k, Src::Fill(mi, i)));
                }
            }
            for (i, t) in m.trades.iter().flatten().enumerate() {
                let ms = t.block_time.timestamp_millis();
                if in_range(ms) {
                    let k = key(ms, RANK_TRADE, src, t.block_number, t.log_index);
                    order.push((k, Src::Trade(mi, i)));
                }
            }
        }
        let mut feeds = Vec::new();
        for (fi, (feed, ticks)) in ds.prices.iter().enumerate() {
            feeds.push((feed.as_str(), ticks.as_slice()));
            // Files hold whole days: keep the ticks from the first start to the last end of the
            // markets on this feed.
            let lives = ds
                .markets
                .iter()
                .filter(|m| m.info.feed_slug().as_deref() == Some(feed));
            let (first, last) = lives.fold((i64::MAX, i64::MIN), |(a, b), m| {
                (
                    a.min(m.info.start.timestamp_millis()),
                    b.max(m.info.end.timestamp_millis()),
                )
            });
            let src = source(feed);
            for (i, t) in ticks.iter().enumerate() {
                if in_range(t.receive_ms) && (first..=last).contains(&t.receive_ms) {
                    let (a, b) = (t.publish_ms as u64, t.slot.unwrap_or(0));
                    order.push((key(t.receive_ms, RANK_PRICE, src, a, b), Src::Price(fi, i)));
                }
            }
        }
        // Stable; the sources come as already sorted runs, which the sort only merges.
        order.sort_by_key(|x| x.0);
        Stream {
            ds,
            feeds,
            synthetic,
            order,
        }
    }

    /// The event at `src`, borrowed.
    pub fn get(&self, src: Src) -> Ev<'_> {
        let ds = self.ds;
        match src {
            Src::Open(m) => Ev::MarketOpen(m),
            Src::Close(m) => Ev::MarketClose(m),
            Src::Synthetic(m) => Ev::Book {
                market: m,
                levels: &self.synthetic[m],
                snapshot: true,
                synthetic: true,
            },
            Src::Book(m, i) => {
                let u = &ds.markets[m].book.as_ref().expect("book").updates[i];
                let (levels, snapshot) = match u {
                    BookUpdate::Snapshot { levels, .. } => (levels, true),
                    BookUpdate::Delta { changes, .. } => (changes, false),
                };
                Ev::Book {
                    market: m,
                    levels,
                    snapshot,
                    synthetic: false,
                }
            }
            Src::Fill(m, i) => Ev::Fill(m, &ds.markets[m].fills.as_ref().expect("fills")[i]),
            Src::Trade(m, i) => Ev::Trade(m, &ds.markets[m].trades.as_ref().expect("trades")[i]),
            Src::Price(f, i) => Ev::PriceTick(self.feeds[f].0, &self.feeds[f].1[i]),
        }
    }

    /// The owned event of the public stream.
    fn event(&self, seq: u64, ts_ms: i64, src: Src) -> Event {
        let ds = self.ds;
        let addr = |m: usize| Some(ds.markets[m].info.address.clone());
        let (market, kind) = match self.get(src) {
            Ev::MarketOpen(m) => (
                addr(m),
                EventKind::MarketOpen {
                    strike: ds.markets[m].info.strike,
                },
            ),
            Ev::MarketClose(m) => {
                let info = &ds.markets[m].info;
                let kind = EventKind::MarketClose {
                    winner: info.winner,
                    resolved_price: info.resolved_price,
                    voided: info.voided,
                };
                (addr(m), kind)
            }
            Ev::Book {
                market,
                levels,
                snapshot,
                synthetic,
            } => {
                let kind = if snapshot {
                    EventKind::BookSnapshot {
                        levels: levels.to_vec(),
                        synthetic,
                    }
                } else {
                    EventKind::BookDelta {
                        changes: levels.to_vec(),
                    }
                };
                (addr(market), kind)
            }
            Ev::Fill(m, f) => (addr(m), EventKind::Fill { fill: f.clone() }),
            Ev::Trade(m, t) => (addr(m), EventKind::Trade { trade: t.clone() }),
            Ev::PriceTick(feed, t) => {
                let kind = EventKind::PriceTick {
                    feed: feed.to_string(),
                    price: t.price,
                    conf: t.conf,
                    publish_ms: t.publish_ms,
                };
                (None, kind)
            }
        };
        Event {
            seq,
            ts_ms,
            market,
            kind,
        }
    }
}

/// Builds the stream of a loaded dataset, clipped to the request range.
pub(crate) fn build(ds: &Dataset) -> Vec<Event> {
    let s = Stream::new(ds);
    s.order
        .iter()
        .enumerate()
        .map(|(i, (k, src))| s.event(i as u64, k.ts_ms, *src))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::book::tests::{history, market};
    use crate::fills::read_fills;
    use crate::fills::tests::{Row, fills_file, row as fill_row};
    use crate::loader::MarketData;
    use crate::market::MarketInfo;
    use crate::prices::PriceTick;
    use crate::request::{Request, parse_time};
    use crate::trades::Side;

    /// A taker against two makers at 3000 ms (journal ids out of order in the input), and a
    /// failed fill at 2000 ms.
    fn fills(info: &MarketInfo) -> Vec<Fill> {
        let rows = [
            fill_row(info, 3000, 9, "t:b"),
            fill_row(info, 3000, 8, "t:a"),
            Row {
                settlement: "failed",
                ..fill_row(info, 2000, 4, "x")
            },
        ];
        read_fills(info, bytes::Bytes::from(fills_file(&rows)))
            .unwrap()
            .0
    }

    fn dataset(from: &str, to: &str) -> Dataset {
        let info = market();
        let trade = Trade {
            trade_id: "t1".into(),
            market: info.address.clone(),
            tx_hash: "0x".into(),
            block_number: 7,
            log_index: 1,
            block_time: chrono::DateTime::from_timestamp(3, 0).unwrap(),
            match_type: "direct".into(),
            taker: "0x1".into(),
            side: Side::Buy,
            outcome_index: Some(0),
            price: 0.51,
            quantity: 1.0,
            amount_usd: 0.51,
        };
        let tick = |ms| PriceTick {
            feed: "btc-usd".into(),
            publish_ms: ms - 400,
            receive_ms: ms,
            price: 1.0,
            conf: 0.1,
            slot: None,
        };
        let mut prices = BTreeMap::new();
        prices.insert("btc-usd".to_string(), vec![tick(0), tick(1000), tick(3000)]);
        Dataset {
            request: Request::builder()
                .range(parse_time(from).unwrap(), parse_time(to).unwrap())
                .build()
                .unwrap(),
            markets: vec![MarketData {
                book: Some(history()),
                fills: Some(fills(&info)),
                trades: Some(vec![trade]),
                info,
            }],
            prices,
            warnings: vec![],
        }
    }

    fn names(ev: &[Event]) -> Vec<(i64, &'static str)> {
        ev.iter().map(|e| (e.ts_ms, e.kind.name())).collect()
    }

    #[test]
    fn merges_in_time_and_kind_order() {
        let ev = build(&dataset("0", "1000000"));
        assert_eq!(
            names(&ev),
            vec![
                (0, "market_open"),
                (0, "price_tick"),
                (1000, "price_tick"),
                (1000, "book_snapshot"),
                (2000, "book_delta"),
                (2000, "fill"),
                (3000, "price_tick"),
                (3000, "book_delta"),
                (3000, "fill"),
                (3000, "fill"),
                (3000, "trade"),
                (4000, "book_snapshot"),
                (5000, "book_delta"),
                (300_000, "market_close"),
            ]
        );
        assert!(ev.iter().enumerate().all(|(i, e)| e.seq == i as u64));
        assert_eq!(ev, build(&dataset("0", "1000000")), "deterministic");
    }

    #[test]
    fn clipped_range_starts_with_the_rebuilt_book() {
        let ev = build(&dataset("2", "4"));
        assert_eq!(
            names(&ev),
            vec![
                (2000, "book_snapshot"),
                (2000, "book_delta"),
                (2000, "fill"),
                (3000, "price_tick"),
                (3000, "book_delta"),
                (3000, "fill"),
                (3000, "fill"),
                (3000, "trade")
            ]
        );
        match &ev[0].kind {
            EventKind::BookSnapshot { levels, synthetic } => {
                assert!(synthetic);
                assert_eq!(levels.len(), 3, "book as of 1999 ms");
            }
            k => panic!("{k:?}"),
        }
    }

    #[test]
    fn fills_in_journal_order_next_to_trades() {
        let ev = build(&dataset("0", "1000000"));
        let ids: Vec<(&str, &str)> = ev
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::Fill { fill } => Some(("fill", fill.trade_id.as_str())),
                EventKind::Trade { trade } => Some(("trade", trade.trade_id.as_str())),
                _ => None,
            })
            .collect();
        assert_eq!(
            ids,
            vec![
                ("fill", "x"),
                ("fill", "t:a"),
                ("fill", "t:b"),
                ("trade", "t1")
            ]
        );
        let j = serde_json::to_value(ev.iter().find(|e| e.kind.name() == "fill").unwrap()).unwrap();
        assert_eq!(j["type"], "fill");
        assert_eq!(j["fill"]["settlement"], "failed");
        assert_eq!(j["fill"]["size"], 2_500_000);
    }

    #[test]
    fn json_shape() {
        let ev = build(&dataset("0", "1"));
        let j = serde_json::to_value(&ev[0]).unwrap();
        assert_eq!(j["type"], "market_open");
        assert_eq!(j["seq"], 0);
        let back: Event = serde_json::from_value(j).unwrap();
        assert_eq!(back, ev[0]);
    }
}
