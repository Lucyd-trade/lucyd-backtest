//! The event stream: every market change in one time-ordered sequence. This is the contract
//! between the loader and the backtest engine; `docs/data-format.md` describes it.

use serde::{Deserialize, Serialize};

use crate::book::{BookUpdate, Level};
use crate::loader::Dataset;
use crate::trades::Trade;

/// One event. Events are sorted by `ts_ms`, then by kind (see [`EventKind::rank`]), then by
/// market or feed, then by their own order (book sequence, block and log index, publish time).
/// `seq` is the position in the stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    /// Unix milliseconds, UTC.
    pub ts_ms: i64,
    /// Market address; None for price ticks (they belong to a feed, shared by markets).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market: Option<String>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// The market starts trading (at `start`); the strike is the price at that moment.
    MarketOpen { strike: Option<f64> },
    /// A Pyth update, at the time the recorder received it.
    PriceTick {
        feed: String,
        price: f64,
        conf: f64,
        publish_ms: i64,
    },
    /// The complete book of both outcomes; replaces the previous book. `synthetic` = rebuilt
    /// from history at the range start, not recorded at this moment.
    BookSnapshot { levels: Vec<Level>, synthetic: bool },
    /// Levels set to a new size; size 0 removes the level.
    BookDelta { changes: Vec<Level> },
    /// A fill, at the time of the block that settled it.
    Trade { trade: Trade },
    /// The market expires (at `end`) with its result.
    MarketClose {
        winner: Option<u8>,
        resolved_price: Option<f64>,
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
            EventKind::Trade { .. } => 4,
            EventKind::MarketClose { .. } => 5,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            EventKind::MarketOpen { .. } => "market_open",
            EventKind::PriceTick { .. } => "price_tick",
            EventKind::BookSnapshot { .. } => "book_snapshot",
            EventKind::BookDelta { .. } => "book_delta",
            EventKind::Trade { .. } => "trade",
            EventKind::MarketClose { .. } => "market_close",
        }
    }
}

/// Sort key: time, kind, market or feed, then order within the source.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    ts_ms: i64,
    rank: u8,
    source: String,
    a: u64,
    b: u64,
}

fn sort_events(mut items: Vec<(Key, Event)>) -> Vec<Event> {
    items.sort_by(|x, y| x.0.cmp(&y.0));
    items
        .into_iter()
        .enumerate()
        .map(|(i, (_, mut e))| {
            e.seq = i as u64;
            e
        })
        .collect()
}

/// Builds the stream of a loaded dataset, clipped to the request range.
pub(crate) fn build(ds: &Dataset) -> Vec<Event> {
    let req = &ds.request;
    let from_ms = req.from.map(|t| t.timestamp_millis());
    let in_range = |ms: i64| {
        from_ms.is_none_or(|f| ms >= f) && req.to.is_none_or(|t| ms < t.timestamp_millis())
    };
    let mut out: Vec<(Key, Event)> = Vec::new();
    let mut push =
        |ts_ms: i64, source: &str, a: u64, b: u64, market: Option<&str>, kind: EventKind| {
            let key = Key {
                ts_ms,
                rank: kind.rank(),
                source: source.to_string(),
                a,
                b,
            };
            out.push((
                key,
                Event {
                    seq: 0,
                    ts_ms,
                    market: market.map(str::to_string),
                    kind,
                },
            ));
        };

    for m in &ds.markets {
        let info = &m.info;
        let addr = info.address.as_str();
        let start = info.start.timestamp_millis();
        let end = info.end.timestamp_millis();
        if in_range(start) {
            push(
                start,
                addr,
                0,
                0,
                Some(addr),
                EventKind::MarketOpen {
                    strike: info.strike,
                },
            );
        }
        if in_range(end) {
            let close = EventKind::MarketClose {
                winner: info.winner,
                resolved_price: info.resolved_price,
                voided: info.voided,
            };
            push(end, addr, 0, 0, Some(addr), close);
        }
        if let Some(h) = &m.book {
            // The range starts in the middle of the history: begin with the book as it was.
            if let Some(f) = from_ms
                && h.first_ts_ms().is_some_and(|t| t < f)
                && let Some(book) = h.at(f - 1)
            {
                let kind = EventKind::BookSnapshot {
                    levels: book.levels(),
                    synthetic: true,
                };
                push(f, addr, 0, 0, Some(addr), kind);
            }
            for (i, u) in h.updates.iter().enumerate() {
                if !in_range(u.ts_ms()) {
                    continue;
                }
                let kind = match u {
                    BookUpdate::Snapshot { levels, .. } => EventKind::BookSnapshot {
                        levels: levels.clone(),
                        synthetic: false,
                    },
                    BookUpdate::Delta { changes, .. } => EventKind::BookDelta {
                        changes: changes.clone(),
                    },
                };
                push(u.ts_ms(), addr, u.sequence(), i as u64, Some(addr), kind);
            }
        }
        for t in m.trades.iter().flatten() {
            let ms = t.block_time.timestamp_millis();
            if in_range(ms) {
                push(
                    ms,
                    addr,
                    t.block_number,
                    t.log_index,
                    Some(addr),
                    EventKind::Trade { trade: t.clone() },
                );
            }
        }
    }
    for (feed, ticks) in &ds.prices {
        for t in ticks {
            if in_range(t.receive_ms) {
                let kind = EventKind::PriceTick {
                    feed: feed.clone(),
                    price: t.price,
                    conf: t.conf,
                    publish_ms: t.publish_ms,
                };
                push(
                    t.receive_ms,
                    feed,
                    t.publish_ms as u64,
                    t.slot.unwrap_or(0),
                    None,
                    kind,
                );
            }
        }
    }
    sort_events(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::book::tests::{history, market};
    use crate::loader::MarketData;
    use crate::prices::PriceTick;
    use crate::request::{Request, parse_time};
    use crate::trades::Side;

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
                info,
                book: Some(history()),
                trades: Some(vec![trade]),
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
                (3000, "price_tick"),
                (3000, "book_delta"),
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
                (3000, "price_tick"),
                (3000, "book_delta"),
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
    fn json_shape() {
        let ev = build(&dataset("0", "1"));
        let j = serde_json::to_value(&ev[0]).unwrap();
        assert_eq!(j["type"], "market_open");
        assert_eq!(j["seq"], 0);
        let back: Event = serde_json::from_value(j).unwrap();
        assert_eq!(back, ev[0]);
    }
}
