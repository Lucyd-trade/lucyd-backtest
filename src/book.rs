//! L2 order book history of a market: the snapshot + delta file of the data host, and the full
//! book at any moment.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, bad};
use crate::market::MarketInfo;
use crate::pq;

/// Price step of the order book, USDC per token.
pub const TICK: f64 = 0.0025;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    Bid,
    Ask,
}

/// One price level. In a delta, `size` 0 removes the level.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Level {
    /// 0 = up, 1 = down.
    pub outcome: u8,
    pub side: BookSide,
    /// USDC per token, a multiple of 0.0025.
    pub price: f64,
    /// Size in raw token units (1e6 per outcome token), as the public book shows it.
    pub size: u64,
}

impl Level {
    /// Size in outcome tokens.
    pub fn tokens(&self) -> f64 {
        self.size as f64 / crate::trades::TOKEN_SCALE
    }

    pub fn ticks(&self) -> u32 {
        price_ticks(self.price)
    }
}

/// Price of a tick count, without float noise (69 → 0.1725).
pub fn tick_price(ticks: u32) -> f64 {
    ticks as f64 * 25.0 / 10_000.0
}

pub fn price_ticks(price: f64) -> u32 {
    (price / TICK).round().max(0.0) as u32
}

/// A change of the book, as recorded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BookUpdate {
    /// The complete book of both outcomes; replaces everything before it.
    Snapshot {
        ts_ms: i64,
        sequence: u64,
        levels: Vec<Level>,
    },
    /// Levels set to a new size (0 = removed).
    Delta {
        ts_ms: i64,
        sequence: u64,
        changes: Vec<Level>,
    },
}

impl BookUpdate {
    pub fn ts_ms(&self) -> i64 {
        match self {
            BookUpdate::Snapshot { ts_ms, .. } | BookUpdate::Delta { ts_ms, .. } => *ts_ms,
        }
    }

    pub fn sequence(&self) -> u64 {
        match self {
            BookUpdate::Snapshot { sequence, .. } | BookUpdate::Delta { sequence, .. } => *sequence,
        }
    }
}

/// Bids and asks of one outcome, keyed by price in ticks.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OutcomeBook {
    pub bids: BTreeMap<u32, u64>,
    pub asks: BTreeMap<u32, u64>,
}

impl OutcomeBook {
    fn side_mut(&mut self, side: BookSide) -> &mut BTreeMap<u32, u64> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    /// Highest bid: (price, size).
    pub fn best_bid(&self) -> Option<(f64, u64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(t, s)| (tick_price(*t), *s))
    }

    /// Lowest ask: (price, size).
    pub fn best_ask(&self) -> Option<(f64, u64)> {
        self.asks.iter().next().map(|(t, s)| (tick_price(*t), *s))
    }

    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()?.0 + self.best_ask()?.0) / 2.0)
    }

    /// Bids, best first: (price, size).
    pub fn bid_levels(&self) -> impl Iterator<Item = (f64, u64)> + '_ {
        self.bids.iter().rev().map(|(t, s)| (tick_price(*t), *s))
    }

    /// Asks, best first: (price, size).
    pub fn ask_levels(&self) -> impl Iterator<Item = (f64, u64)> + '_ {
        self.asks.iter().map(|(t, s)| (tick_price(*t), *s))
    }
}

/// Full book of a market at a moment.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderBook {
    /// Time of the last update applied.
    pub ts_ms: i64,
    /// Index = outcome index.
    pub outcomes: Vec<OutcomeBook>,
}

impl OrderBook {
    pub fn outcome(&self, index: u8) -> Option<&OutcomeBook> {
        self.outcomes.get(index as usize)
    }

    pub fn apply(&mut self, u: &BookUpdate) {
        match u {
            BookUpdate::Snapshot { ts_ms, levels, .. } => {
                for o in &mut self.outcomes {
                    *o = OutcomeBook::default();
                }
                self.set(levels);
                self.ts_ms = *ts_ms;
            }
            BookUpdate::Delta { ts_ms, changes, .. } => {
                self.set(changes);
                self.ts_ms = *ts_ms;
            }
        }
    }

    fn set(&mut self, levels: &[Level]) {
        for l in levels {
            let i = l.outcome as usize;
            if self.outcomes.len() <= i {
                self.outcomes.resize(i + 1, OutcomeBook::default());
            }
            let side = self.outcomes[i].side_mut(l.side);
            if l.size == 0 {
                side.remove(&l.ticks());
            } else {
                side.insert(l.ticks(), l.size);
            }
        }
    }

    /// All levels, as a snapshot would list them.
    pub fn levels(&self) -> Vec<Level> {
        let mut out = Vec::new();
        for (i, o) in self.outcomes.iter().enumerate() {
            for (side, map) in [(BookSide::Bid, &o.bids), (BookSide::Ask, &o.asks)] {
                for (t, s) in map {
                    out.push(Level {
                        outcome: i as u8,
                        side,
                        price: tick_price(*t),
                        size: *s,
                    });
                }
            }
        }
        out
    }
}

/// Recorded book of one market, in time order.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BookHistory {
    pub market: String,
    pub updates: Vec<BookUpdate>,
}

/// One row of the book file.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BookRow {
    pub ts_ms: i64,
    pub sequence: u64,
    pub snapshot: bool,
    pub asset_id: Option<String>,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub size: Option<u64>,
}

pub(crate) fn read_rows(file: Bytes) -> Result<Vec<BookRow>> {
    let what = "book file";
    let mut rows = Vec::new();
    for b in pq::read_batches(file, what)? {
        let ts = pq::ts(&b, "timestamp", what)?;
        let seq = pq::u64s(&b, "state_sequence", what)?;
        let kind = pq::strs(&b, "kind", what)?;
        let asset = pq::strs(&b, "asset_id", what)?;
        let side = pq::strs(&b, "side", what)?;
        let price = pq::f64s(&b, "price", what)?;
        let size = pq::u64s(&b, "size", what)?;
        for i in 0..b.num_rows() {
            rows.push(BookRow {
                ts_ms: ts[i],
                sequence: seq[i].unwrap_or(0),
                snapshot: match kind[i].as_deref() {
                    Some("snapshot") => true,
                    Some("delta") => false,
                    k => return Err(bad(what, format!("unknown kind {k:?}"))),
                },
                asset_id: asset[i].clone(),
                side: side[i].clone(),
                price: price[i],
                size: size[i],
            });
        }
    }
    Ok(rows)
}

impl BookHistory {
    /// Groups rows into updates: rows of one snapshot share the timestamp; rows of one delta
    /// share the timestamp and the sequence. Rows of unknown tokens are skipped and counted.
    pub(crate) fn from_rows(market: &MarketInfo, mut rows: Vec<BookRow>) -> (Self, usize) {
        rows.sort_by_key(|r| (r.ts_ms, r.sequence));
        let mut updates: Vec<BookUpdate> = Vec::new();
        let mut skipped = 0;
        for r in rows {
            let level = match (&r.asset_id, r.side.as_deref(), r.price, r.size) {
                (Some(a), Some(side), Some(price), Some(size)) => {
                    let side = match side {
                        "bid" => Some(BookSide::Bid),
                        "ask" => Some(BookSide::Ask),
                        _ => None,
                    };
                    let level = side.zip(market.outcome_of(a)).map(|(side, outcome)| Level {
                        outcome,
                        side,
                        price: tick_price(price_ticks(price)),
                        size,
                    });
                    if level.is_none() {
                        skipped += 1;
                    }
                    level
                }
                _ => None,
            };
            let same = match updates.last_mut() {
                Some(BookUpdate::Snapshot { ts_ms, levels, .. })
                    if r.snapshot && *ts_ms == r.ts_ms =>
                {
                    levels.extend(level);
                    true
                }
                Some(BookUpdate::Delta {
                    ts_ms,
                    sequence,
                    changes,
                }) if !r.snapshot && *ts_ms == r.ts_ms && *sequence == r.sequence => {
                    changes.extend(level);
                    true
                }
                _ => false,
            };
            if !same {
                let levels: Vec<Level> = level.into_iter().collect();
                updates.push(if r.snapshot {
                    BookUpdate::Snapshot {
                        ts_ms: r.ts_ms,
                        sequence: r.sequence,
                        levels,
                    }
                } else {
                    BookUpdate::Delta {
                        ts_ms: r.ts_ms,
                        sequence: r.sequence,
                        changes: levels,
                    }
                });
            }
        }
        (
            BookHistory {
                market: market.address.clone(),
                updates,
            },
            skipped,
        )
    }

    /// Full book at `ts_ms` (updates at exactly that time included): the last snapshot at or
    /// before it plus the deltas after the snapshot. None before the first snapshot.
    pub fn at(&self, ts_ms: i64) -> Option<OrderBook> {
        let end = self.updates.partition_point(|u| u.ts_ms() <= ts_ms);
        let start = self.updates[..end]
            .iter()
            .rposition(|u| matches!(u, BookUpdate::Snapshot { .. }))?;
        let mut book = OrderBook {
            ts_ms,
            outcomes: vec![OutcomeBook::default(); 2],
        };
        for u in &self.updates[start..end] {
            book.apply(u);
        }
        Some(book)
    }

    pub fn first_ts_ms(&self) -> Option<i64> {
        self.updates.first().map(BookUpdate::ts_ms)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::market::outcome_id;
    use crate::request::{MarketType, Period};
    use chrono::DateTime;

    pub(crate) fn market() -> MarketInfo {
        let address = format!("0x{}", "ab".repeat(32));
        MarketInfo {
            outcome_ids: vec![
                outcome_id(&address, 0).unwrap(),
                outcome_id(&address, 1).unwrap(),
            ],
            address,
            market_type: MarketType::UpDown,
            period: Period::M5,
            instrument: "BTC".into(),
            feed_id: None,
            feed_name: Some("BTC/USD".into()),
            start: DateTime::from_timestamp(0, 0).unwrap(),
            end: DateTime::from_timestamp(300, 0).unwrap(),
            state: crate::market::MarketState::Settled,
            strike: None,
            resolved_price: None,
            resolved_at: None,
            winner: Some(0),
            voided: false,
            outcomes: vec!["up".into(), "down".into()],
            volume_usd: 0.0,
        }
    }

    pub(crate) fn row(
        m: &MarketInfo,
        ts: i64,
        seq: u64,
        snap: bool,
        lvl: Option<(u8, &str, f64, u64)>,
    ) -> BookRow {
        BookRow {
            ts_ms: ts,
            sequence: seq,
            snapshot: snap,
            asset_id: lvl.map(|l| m.outcome_ids[l.0 as usize].clone()),
            side: lvl.map(|l| l.1.to_string()),
            price: lvl.map(|l| l.2),
            size: lvl.map(|l| l.3),
        }
    }

    pub(crate) fn history() -> BookHistory {
        let m = market();
        let rows = vec![
            row(&m, 1000, 1, true, Some((0, "bid", 0.5, 10))),
            row(&m, 1000, 1, true, Some((0, "ask", 0.52, 7))),
            row(&m, 1000, 1, true, Some((1, "bid", 0.47, 3))),
            row(&m, 2000, 2, false, Some((0, "bid", 0.5, 0))),
            row(&m, 2000, 2, false, Some((0, "bid", 0.505, 4))),
            row(&m, 3000, 3, false, Some((0, "ask", 0.51, 1))),
            row(&m, 4000, 4, true, None),
            row(&m, 5000, 5, false, Some((1, "ask", 0.6, 2))),
        ];
        BookHistory::from_rows(&m, rows).0
    }

    #[test]
    fn groups_rows_into_updates() {
        let h = history();
        assert_eq!(h.updates.len(), 5);
        assert!(matches!(&h.updates[0], BookUpdate::Snapshot { levels, .. } if levels.len() == 3));
        assert!(matches!(&h.updates[1], BookUpdate::Delta { changes, .. } if changes.len() == 2));
        assert!(matches!(&h.updates[3], BookUpdate::Snapshot { levels, .. } if levels.is_empty()));
    }

    #[test]
    fn book_at_any_time() {
        let h = history();
        assert!(h.at(999).is_none());
        let b = h.at(1000).unwrap();
        assert_eq!(b.outcome(0).unwrap().best_bid(), Some((0.5, 10)));
        assert_eq!(b.outcome(1).unwrap().best_bid().unwrap().1, 3);

        let b = h.at(2500).unwrap();
        let up = b.outcome(0).unwrap();
        assert_eq!(up.best_bid(), Some((0.505, 4)));
        assert_eq!(up.bids.len(), 1, "0.5 removed");
        assert_eq!(up.best_ask(), Some((0.52, 7)));

        let b = h.at(3000).unwrap();
        assert_eq!(b.outcome(0).unwrap().best_ask(), Some((0.51, 1)));
        assert_eq!(b.outcome(0).unwrap().ask_levels().count(), 2);
        assert!((b.outcome(0).unwrap().mid().unwrap() - 0.5075).abs() < 1e-12);

        let b = h.at(4000).unwrap();
        assert!(b.levels().is_empty(), "empty snapshot clears");
        let b = h.at(9999).unwrap();
        assert_eq!(b.levels().len(), 1);
    }

    #[test]
    fn unknown_tokens_are_skipped() {
        let m = market();
        let mut r = row(&m, 1, 1, true, Some((0, "bid", 0.5, 1)));
        r.asset_id = Some("0xdead".into());
        let (h, skipped) = BookHistory::from_rows(&m, vec![r]);
        assert_eq!(skipped, 1);
        assert!(matches!(&h.updates[0], BookUpdate::Snapshot { levels, .. } if levels.is_empty()));
    }
}
