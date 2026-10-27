//! Trades from `GET /trades`: paging that never needs an offset above 10000, and grouping of
//! markets into few requests.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::market::MarketInfo;

/// Raw token units per outcome token.
pub const TOKEN_SCALE: f64 = 1e6;
pub(crate) const PAGE: u32 = 1000;
pub(crate) const MAX_OFFSET: u32 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

/// One fill, from the taker's side.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    /// `takerOrderId:makerOrderId`, unique per fill.
    pub trade_id: String,
    pub market: String,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
    /// Time of the block that settled the fill (whole seconds; the match happened up to a few
    /// blocks earlier).
    pub block_time: DateTime<Utc>,
    /// `direct`, `mint` or `merge`.
    pub match_type: String,
    /// Taker wallet, lowercase.
    pub taker: String,
    /// Taker side.
    pub side: Side,
    /// Taker outcome: 0 = up, 1 = down.
    pub outcome_index: Option<u8>,
    /// USDC per outcome token, 0–1.
    pub price: f64,
    /// Outcome tokens (raw units / 1e6).
    pub quantity: f64,
    pub amount_usd: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApiTrade {
    trade_id: String,
    tx_hash: String,
    log_index: u64,
    block_number: u64,
    block_time: DateTime<Utc>,
    market_id: String,
    match_type: String,
    trader: String,
    side: Side,
    outcome_index: Option<u8>,
    price: f64,
    quantity: u64,
    amount_usd: f64,
}

impl From<ApiTrade> for Trade {
    fn from(t: ApiTrade) -> Self {
        Trade {
            trade_id: t.trade_id,
            market: t.market_id.to_ascii_lowercase(),
            tx_hash: t.tx_hash,
            block_number: t.block_number,
            log_index: t.log_index,
            block_time: t.block_time,
            match_type: t.match_type,
            taker: t.trader.to_ascii_lowercase(),
            side: t.side,
            outcome_index: t.outcome_index,
            price: t.price,
            quantity: t.quantity as f64 / TOKEN_SCALE,
            amount_usd: t.amount_usd,
        }
    }
}

/// Chain order: block, then position in the block.
pub fn sort_trades(trades: &mut [Trade]) {
    trades.sort_by(|a, b| {
        (a.block_number, a.log_index, &a.trade_id).cmp(&(b.block_number, b.log_index, &b.trade_id))
    });
}

/// Pages through `[from, to)` oldest first. A page is 1000 rows; when the offset would pass
/// 10000 the window start moves to the time of the last row instead (rows of that second come
/// again and are dropped as duplicates).
#[derive(Debug)]
pub(crate) struct TradePager {
    from: i64,
    to: i64,
    offset: u32,
    seen: HashSet<String>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Step {
    More,
    Done,
}

impl TradePager {
    pub(crate) fn new(from: i64, to: i64) -> Self {
        TradePager {
            from,
            to,
            offset: 0,
            seen: HashSet::new(),
        }
    }

    /// `(from, to, offset)` of the next request.
    pub(crate) fn query(&self) -> (i64, i64, u32) {
        (self.from, self.to, self.offset)
    }

    /// Takes a page (oldest first) and returns its rows not seen before.
    pub(crate) fn take(&mut self, page: Vec<ApiTrade>) -> Result<(Vec<Trade>, Step)> {
        let full = page.len() as u32 >= PAGE;
        let last_time = page.last().map(|t| t.block_time.timestamp());
        let fresh: Vec<Trade> = page
            .into_iter()
            .filter(|t| self.seen.insert(t.trade_id.clone()))
            .map(Trade::from)
            .collect();
        if !full {
            return Ok((fresh, Step::Done));
        }
        if self.offset + PAGE < MAX_OFFSET {
            self.offset += PAGE;
        } else {
            let next = last_time.unwrap_or(self.from);
            if next <= self.from {
                return Err(Error::BadData {
                    what: "trades".into(),
                    detail: format!(
                        "more than {} trades in the second {}",
                        MAX_OFFSET + PAGE,
                        self.from
                    ),
                });
            }
            self.from = next;
            self.offset = 0;
        }
        Ok((fresh, Step::More))
    }
}

/// Time window of a market's trades: from a little before it opens until an hour after it ends
/// (fills settle on chain shortly after the match).
pub(crate) fn market_window(m: &MarketInfo) -> (DateTime<Utc>, DateTime<Utc>) {
    (m.start - Duration::minutes(5), m.end + Duration::hours(1))
}

/// A request for several markets at once.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Batch {
    pub markets: Vec<String>,
    pub from: i64,
    pub to: i64,
}

/// Groups `(market, from, to)` jobs into requests of at most `max_markets` addresses whose
/// window spans at most `max_span`: few requests, each with few rows.
pub(crate) fn plan_batches(
    mut jobs: Vec<(String, i64, i64)>,
    max_markets: usize,
    max_span: i64,
) -> Vec<Batch> {
    jobs.sort_by_key(|j| (j.1, j.2));
    let mut out: Vec<Batch> = Vec::new();
    for (market, from, to) in jobs {
        if let Some(b) = out.last_mut()
            && b.markets.len() < max_markets
            && to.max(b.to) - b.from <= max_span
        {
            b.markets.push(market);
            b.to = b.to.max(to);
            continue;
        }
        out.push(Batch {
            markets: vec![market],
            from,
            to,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(id: usize, secs: i64) -> ApiTrade {
        ApiTrade {
            trade_id: format!("t{id}"),
            tx_hash: "0x".into(),
            log_index: id as u64,
            block_number: 1,
            block_time: DateTime::from_timestamp(secs, 0).unwrap(),
            market_id: "0xAB".into(),
            match_type: "direct".into(),
            trader: "0xCD".into(),
            side: Side::Buy,
            outcome_index: Some(0),
            price: 0.5,
            quantity: 2_000_000,
            amount_usd: 1.0,
        }
    }

    #[test]
    fn short_page_ends() {
        let mut p = TradePager::new(0, 100);
        let (rows, step) = p.take(vec![api(1, 5), api(2, 6)]).unwrap();
        assert_eq!(step, Step::Done);
        assert_eq!(rows[0].quantity, 2.0);
        assert_eq!(rows[0].market, "0xab");
    }

    #[test]
    fn offset_then_moves_window_by_time() {
        let mut p = TradePager::new(0, 1_000_000);
        let mut n = 0;
        // ten full pages: offsets 0 … 9000
        for page in 0..10 {
            assert_eq!(p.query(), (0, 1_000_000, page * PAGE));
            let rows = (0..PAGE as usize)
                .map(|_| {
                    n += 1;
                    api(n, n as i64 / 10)
                })
                .collect();
            assert_eq!(p.take(rows).unwrap().1, Step::More);
        }
        // offset would reach 10000: restart from the last row's second
        let last_second = n as i64 / 10;
        assert_eq!(p.query(), (last_second, 1_000_000, 0));
        // the repeated rows of that second are dropped
        let again: Vec<_> = (n - 3..=n)
            .map(|i| api(i, i as i64 / 10))
            .chain([api(n + 1, last_second + 1)])
            .collect();
        let (rows, step) = p.take(again).unwrap();
        assert_eq!(step, Step::Done);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].trade_id, format!("t{}", n + 1));
    }

    #[test]
    fn too_many_trades_in_one_second_is_an_error() {
        let mut p = TradePager::new(50, 100);
        let mut n = 0;
        for _ in 0..10 {
            let rows = (0..PAGE as usize)
                .map(|_| {
                    n += 1;
                    api(n, 50)
                })
                .collect();
            if p.take(rows).is_err() {
                return;
            }
        }
        panic!("expected an error");
    }

    #[test]
    fn batches_by_count_and_span() {
        let jobs = (0..7)
            .map(|i| (format!("m{i}"), i * 100, i * 100 + 150))
            .collect();
        let b = plan_batches(jobs, 3, 1000);
        assert_eq!(
            b.iter().map(|b| b.markets.len()).collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
        assert_eq!((b[0].from, b[0].to), (0, 350));

        let jobs = vec![
            ("a".into(), 0, 10),
            ("b".into(), 5, 2000),
            ("c".into(), 3000, 3010),
        ];
        let b = plan_batches(jobs, 10, 1000);
        assert_eq!(b.len(), 3, "span limit splits");
    }

    #[test]
    fn chain_order() {
        let mut t: Vec<Trade> = vec![api(2, 1).into(), api(1, 1).into()];
        t[0].block_number = 2;
        sort_trades(&mut t);
        assert_eq!(t[0].trade_id, "t1");
    }
}
