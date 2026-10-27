//! CLOB fills of a market: the fills file of the data host, one row per match with the order
//! book's own millisecond time and the state of its on-chain settlement.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, bad};
use crate::market::MarketInfo;
use crate::pq;
use crate::trades::{Side, TOKEN_SCALE, Trade};

/// On-chain settlement of a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Settlement {
    /// Not settled when the file was written (published after an hour with fills still pending).
    Pending,
    /// Settled on chain.
    Confirmed,
    /// The settlement transaction failed. The match still happened in the order book and took
    /// the volume off it.
    Failed,
}

impl Settlement {
    /// `pending`, `confirmed` or `failed`.
    pub fn as_str(self) -> &'static str {
        match self {
            Settlement::Pending => "pending",
            Settlement::Confirmed => "confirmed",
            Settlement::Failed => "failed",
        }
    }
}

/// One match of the order book, from the taker's side. Field names shared with [`Trade`] mean
/// the same; both implement [`Execution`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    /// `takerOrderId:makerOrderId`, as in [`Trade::trade_id`].
    pub trade_id: String,
    /// Market address.
    pub market: String,
    /// Match time by the order book clock, unix ms. Same clock as the book files.
    pub ts_ms: i64,
    /// Order journal row id: the order of fills within one millisecond.
    pub journal_id: i64,
    /// Taker wallet, lowercase.
    pub taker: String,
    /// Taker side.
    pub side: Side,
    /// Taker outcome: 0 = up, 1 = down. None if the token is not one of the market's.
    pub outcome_index: Option<u8>,
    /// Taker outcome token, the same id as book `asset_id`.
    pub taker_asset_id: String,
    /// Maker wallet, lowercase; None if its order is unknown.
    pub maker: Option<String>,
    /// Side of the maker, if recorded.
    pub maker_side: Option<Side>,
    /// Outcome of the maker, if recorded.
    pub maker_outcome_index: Option<u8>,
    /// Token id of the maker, if recorded.
    pub maker_asset_id: Option<String>,
    /// USDC per token of the taker's outcome, 0–1.
    pub price: f64,
    /// Raw token units (1e6 per outcome token), the unit of book level sizes.
    pub size: u64,
    /// Outcome tokens (`size` / 1e6).
    pub quantity: f64,
    /// `price` × `quantity`.
    pub amount_usd: f64,
    /// `direct`, `mint` or `merge`.
    pub match_type: Option<String>,
    /// On-chain settlement of the match.
    pub settlement: Settlement,
    /// Settlement transaction.
    pub tx_hash: Option<String>,
    /// Block of the settlement, if any.
    pub block_number: Option<u64>,
}

/// What [`Trade`] (on-chain) and [`Fill`] (order book) have in common: an engine written
/// against this works with either.
pub trait Execution {
    /// Market address.
    fn market(&self) -> &str;
    /// Fill or trade id.
    fn trade_id(&self) -> &str;
    /// Unix ms: match time for a fill, block time for a trade.
    fn ts_ms(&self) -> i64;
    /// Taker address.
    fn taker(&self) -> &str;
    /// Side of the taker.
    fn side(&self) -> Side;
    /// Outcome of the taker.
    fn outcome_index(&self) -> Option<u8>;
    /// Price per token, USDC.
    fn price(&self) -> f64;
    /// Outcome tokens.
    fn quantity(&self) -> f64;
    /// USDC notional.
    fn amount_usd(&self) -> f64;
    /// `direct`, `mint` or `merge`, if known.
    fn match_type(&self) -> Option<&str>;
}

impl Execution for Fill {
    fn market(&self) -> &str {
        &self.market
    }
    fn trade_id(&self) -> &str {
        &self.trade_id
    }
    fn ts_ms(&self) -> i64 {
        self.ts_ms
    }
    fn taker(&self) -> &str {
        &self.taker
    }
    fn side(&self) -> Side {
        self.side
    }
    fn outcome_index(&self) -> Option<u8> {
        self.outcome_index
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn quantity(&self) -> f64 {
        self.quantity
    }
    fn amount_usd(&self) -> f64 {
        self.amount_usd
    }
    fn match_type(&self) -> Option<&str> {
        self.match_type.as_deref()
    }
}

impl Execution for Trade {
    fn market(&self) -> &str {
        &self.market
    }
    fn trade_id(&self) -> &str {
        &self.trade_id
    }
    fn ts_ms(&self) -> i64 {
        self.block_time.timestamp_millis()
    }
    fn taker(&self) -> &str {
        &self.taker
    }
    fn side(&self) -> Side {
        self.side
    }
    fn outcome_index(&self) -> Option<u8> {
        self.outcome_index
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn quantity(&self) -> f64 {
        self.quantity
    }
    fn amount_usd(&self) -> f64 {
        self.amount_usd
    }
    fn match_type(&self) -> Option<&str> {
        Some(&self.match_type)
    }
}

fn side(s: &str, what: &str) -> Result<Side> {
    match s {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(bad(what, format!("unknown side {s:?}"))),
    }
}

fn need<T>(v: Option<T>, col: &str, what: &str) -> Result<T> {
    v.ok_or_else(|| bad(what, format!("null in {col}")))
}

/// Fills of a market's file, in stream order (time, then journal id), and the number of rows
/// whose tokens are not the market's (kept, with no outcome index).
pub(crate) fn read_fills(market: &MarketInfo, file: Bytes) -> Result<(Vec<Fill>, usize)> {
    let what = "fills file";
    let mut out = Vec::new();
    let mut unknown = 0;
    for b in pq::read_batches(file, what)? {
        let ts = pq::ts(&b, "timestamp", what)?;
        let journal = pq::i64s(&b, "journal_id", what)?;
        let trade_id = pq::strs(&b, "trade_id", what)?;
        let price = pq::f64s(&b, "price", what)?;
        let size = pq::u64s(&b, "size", what)?;
        let taker = pq::strs(&b, "taker", what)?;
        let taker_side = pq::strs(&b, "taker_side", what)?;
        let taker_asset = pq::strs(&b, "taker_asset_id", what)?;
        let maker = pq::strs(&b, "maker", what)?;
        let maker_side = pq::strs(&b, "maker_side", what)?;
        let maker_asset = pq::strs(&b, "maker_asset_id", what)?;
        let match_type = pq::strs(&b, "match_type", what)?;
        let settlement = pq::strs(&b, "settlement", what)?;
        let tx_hash = pq::strs(&b, "tx_hash", what)?;
        let block = pq::i64s(&b, "block_number", what)?;
        for i in 0..b.num_rows() {
            let taker_asset_id = need(taker_asset[i].clone(), "taker_asset_id", what)?;
            let outcome_index = market.outcome_of(&taker_asset_id);
            if outcome_index.is_none() {
                unknown += 1;
            }
            let size = need(size[i], "size", what)?;
            let price = need(price[i], "price", what)?;
            let quantity = size as f64 / TOKEN_SCALE;
            out.push(Fill {
                trade_id: need(trade_id[i].clone(), "trade_id", what)?,
                market: market.address.clone(),
                ts_ms: ts[i],
                journal_id: need(journal[i], "journal_id", what)?,
                taker: need(taker[i].as_deref(), "taker", what)?.to_ascii_lowercase(),
                side: side(need(taker_side[i].as_deref(), "taker_side", what)?, what)?,
                outcome_index,
                maker_outcome_index: maker_asset[i].as_deref().and_then(|a| market.outcome_of(a)),
                taker_asset_id: taker_asset_id.to_ascii_lowercase(),
                maker: maker[i].as_deref().map(str::to_ascii_lowercase),
                maker_side: maker_side[i]
                    .as_deref()
                    .map(|s| side(s, what))
                    .transpose()?,
                maker_asset_id: maker_asset[i].as_deref().map(str::to_ascii_lowercase),
                price,
                size,
                quantity,
                amount_usd: price * quantity,
                match_type: match_type[i].clone(),
                settlement: match settlement[i].as_deref() {
                    Some("pending") => Settlement::Pending,
                    Some("confirmed") => Settlement::Confirmed,
                    Some("failed") => Settlement::Failed,
                    s => return Err(bad(what, format!("unknown settlement {s:?}"))),
                },
                tx_hash: tx_hash[i].clone(),
                block_number: block[i].and_then(|n| u64::try_from(n).ok()),
            });
        }
    }
    sort_fills(&mut out);
    Ok((out, unknown))
}

/// Stream order: match time, then journal id (a taker against several makers keeps its order).
pub fn sort_fills(fills: &mut [Fill]) {
    fills.sort_by_key(|f| (f.ts_ms, f.journal_id));
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::book::tests::market;

    /// One row of the fixture, in the column order of book-recorder's fills table.
    #[derive(Clone)]
    pub(crate) struct Row {
        pub ts: i64,
        pub journal_id: i64,
        pub trade_id: &'static str,
        pub price: f64,
        pub size: u64,
        pub taker: &'static str,
        pub taker_side: &'static str,
        pub taker_asset_id: String,
        pub maker: Option<&'static str>,
        pub maker_side: Option<&'static str>,
        pub maker_asset_id: Option<String>,
        pub match_type: Option<&'static str>,
        pub settlement: &'static str,
        pub tx_hash: Option<&'static str>,
        pub block_number: Option<i64>,
    }

    /// A direct match of a taker buying `up` from a maker selling it.
    pub(crate) fn row(m: &MarketInfo, ts: i64, journal_id: i64, trade_id: &'static str) -> Row {
        Row {
            ts,
            journal_id,
            trade_id,
            price: 0.55,
            size: 2_500_000,
            taker: "0xAA",
            taker_side: "buy",
            taker_asset_id: m.outcome_ids[0].clone(),
            maker: Some("0xBB"),
            maker_side: Some("sell"),
            maker_asset_id: Some(m.outcome_ids[0].clone()),
            match_type: Some("direct"),
            settlement: "confirmed",
            tx_hash: Some("0xtx"),
            block_number: Some(11_900_000),
        }
    }

    /// A fills file with the schema of book-recorder's `archive::fills_to_parquet`.
    pub(crate) fn fills_file(rows: &[Row]) -> Vec<u8> {
        let utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", utc, false),
            Field::new("journal_id", DataType::Int64, false),
            Field::new("trade_id", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("taker", DataType::Utf8, false),
            Field::new("taker_side", DataType::Utf8, false),
            Field::new("taker_asset_id", DataType::Utf8, false),
            Field::new("maker", DataType::Utf8, true),
            Field::new("maker_side", DataType::Utf8, true),
            Field::new("maker_asset_id", DataType::Utf8, true),
            Field::new("match_type", DataType::Utf8, true),
            Field::new("settlement", DataType::Utf8, false),
            Field::new("tx_hash", DataType::Utf8, true),
            Field::new("block_number", DataType::Int64, true),
        ]));
        let s = |f: &dyn Fn(&Row) -> Option<String>| -> ArrayRef {
            Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
        };
        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMillisecondArray::from_iter_values(rows.iter().map(|r| r.ts))
                    .with_timezone("UTC"),
            ),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.journal_id),
            )),
            s(&|r| Some(r.trade_id.into())),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.price))),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.size))),
            s(&|r| Some(r.taker.into())),
            s(&|r| Some(r.taker_side.into())),
            s(&|r| Some(r.taker_asset_id.clone())),
            s(&|r| r.maker.map(Into::into)),
            s(&|r| r.maker_side.map(Into::into)),
            s(&|r| r.maker_asset_id.clone()),
            s(&|r| r.match_type.map(Into::into)),
            s(&|r| Some(r.settlement.into())),
            s(&|r| r.tx_hash.map(Into::into)),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.block_number).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out
    }

    #[test]
    fn reads_every_column() {
        let m = market();
        let mut pending = row(&m, 2000, 7, "t2");
        pending.settlement = "pending";
        pending.tx_hash = None;
        pending.block_number = None;
        pending.taker_side = "sell";
        pending.taker_asset_id = m.outcome_ids[1].clone();
        let mut failed = row(&m, 1000, 5, "t1");
        failed.settlement = "failed";
        failed.maker = None;
        failed.maker_side = None;
        failed.maker_asset_id = None;
        failed.match_type = None;
        let file = fills_file(&[pending, failed]);

        let (fills, unknown) = read_fills(&m, Bytes::from(file)).unwrap();
        assert_eq!(unknown, 0);
        assert_eq!(fills.len(), 2, "failed fills are kept");
        let f = &fills[0];
        assert_eq!(
            (f.trade_id.as_str(), f.ts_ms, f.journal_id),
            ("t1", 1000, 5)
        );
        assert_eq!(f.settlement, Settlement::Failed);
        assert_eq!((f.size, f.quantity), (2_500_000, 2.5));
        assert!((f.amount_usd - 1.375).abs() < 1e-12);
        assert_eq!(f.taker, "0xaa");
        assert_eq!((f.side, f.outcome_index), (Side::Buy, Some(0)));
        assert_eq!((f.maker.as_deref(), f.maker_side), (None, None));
        assert_eq!(f.match_type, None);
        assert_eq!(
            (f.tx_hash.as_deref(), f.block_number),
            (Some("0xtx"), Some(11_900_000))
        );

        let f = &fills[1];
        assert_eq!(f.settlement, Settlement::Pending);
        assert_eq!((f.side, f.outcome_index), (Side::Sell, Some(1)));
        assert_eq!(f.maker.as_deref(), Some("0xbb"));
        assert_eq!(
            (f.maker_side, f.maker_outcome_index),
            (Some(Side::Sell), Some(0))
        );
        assert_eq!(f.taker_asset_id, m.outcome_ids[1]);
        assert_eq!((f.tx_hash.as_deref(), f.block_number), (None, None));
    }

    #[test]
    fn multi_maker_order_and_unknown_tokens() {
        let m = market();
        // One taker order against three makers: same millisecond, journal ids keep the order.
        let mut rows = vec![
            row(&m, 3000, 12, "t:c"),
            row(&m, 3000, 10, "t:a"),
            row(&m, 1000, 20, "early"),
            row(&m, 3000, 11, "t:b"),
        ];
        rows[2].taker_asset_id = "0xdead".into();
        let (fills, unknown) = read_fills(&m, Bytes::from(fills_file(&rows))).unwrap();
        assert_eq!(unknown, 1);
        assert_eq!(fills[0].outcome_index, None);
        let ids: Vec<&str> = fills.iter().map(|f| f.trade_id.as_str()).collect();
        assert_eq!(ids, vec!["early", "t:a", "t:b", "t:c"]);
    }

    #[test]
    fn bad_settlement_is_an_error() {
        let m = market();
        let mut r = row(&m, 1, 1, "t");
        r.settlement = "maybe";
        assert!(read_fills(&m, Bytes::from(fills_file(&[r]))).is_err());
    }
}
