//! The result of a run: metrics overall, per market and per sample period, the equity curve, the
//! own-order journal and warnings. The JSON form of [`Report`] is the contract for tools that
//! read runs.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::book::tick_price;
use crate::cache::write_atomic;
use crate::error::Result;
use crate::export::Format;
use crate::loader::Dataset;
use crate::network::Network;
use crate::request::{Period, Request};
use crate::trades::Side;

use super::params::ExecParams;
use super::strategy::{OrderId, TimeInForce};
use super::world::{JournalRow, Order, OrderState, World, tokens, usd};

/// Version of the report format.
pub const REPORT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub version: u32,
    pub strategy: String,
    pub request: Request,
    pub params: ExecParams,
    /// Time range of the run, unix ms.
    pub from_ms: i64,
    pub to_ms: i64,
    pub summary: Metrics,
    /// Fit and validation samples; None when `validation_fraction` is 0.
    pub periods: Option<Periods>,
    pub markets: Vec<MarketReport>,
    pub equity: Vec<EquityPoint>,
    pub orders: Vec<OrderRecord>,
    pub journal: Vec<JournalRow>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Periods {
    /// Markets ending before this time (unix ms) are the fit sample, the rest the validation one.
    pub split_ms: i64,
    pub fit: Metrics,
    pub validation: Metrics,
}

/// Money in USDC, prices in USDC per token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    pub start_equity: f64,
    pub end_equity: f64,
    /// Sum of the markets' P&L.
    pub pnl: f64,
    /// `pnl` / initial USDC × 100.
    pub return_pct: f64,
    /// Markets with at least one own fill.
    pub markets_traded: u32,
    /// Of those, markets with P&L above 0.
    pub markets_won: u32,
    pub win_rate: Option<f64>,
    /// Largest fall of equity from a previous high.
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    /// Notional of own fills.
    pub turnover: f64,
    pub fees: f64,
    pub fills: u32,
    /// Fills whose settlement failed (undone).
    pub reverted_fills: u32,
    pub orders: u32,
    pub rejected_orders: u32,
    /// Orders that took liquidity on arrival.
    pub taker_orders: u32,
    /// Average price of taker executions against the best price on arrival, per token,
    /// weighted by quantity; above 0 = worse.
    pub avg_slippage: Option<f64>,
    /// GTC orders that reached the book.
    pub limit_orders: u32,
    /// Of those, orders with any fill.
    pub limit_orders_filled: u32,
    pub limit_fill_rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketReport {
    pub market: String,
    pub period: Period,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// `fit` or `validation` (`all` without a split).
    pub sample: String,
    /// `up`, `down`, `void`, or `unresolved` (no result within the data).
    pub result: String,
    pub pnl: f64,
    /// Sells minus buys minus trading fees.
    pub cash: f64,
    /// Redemption at resolution, after its fee.
    pub payout: f64,
    /// Tokens still held at the end of an unresolved market, at the last mark.
    pub open_value: f64,
    pub fees: f64,
    pub turnover: f64,
    pub fills: u32,
    pub reverted_fills: u32,
    pub orders: u32,
    /// Fills of other participants in the data: without them the queue model only moves on book
    /// changes.
    pub recorded_fills: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub id: OrderId,
    pub market: String,
    pub outcome: u8,
    pub side: Side,
    pub tif: TimeInForce,
    pub price: f64,
    /// Tokens.
    pub quantity: f64,
    pub filled: f64,
    /// Average execution price, if anything filled.
    pub avg_price: Option<f64>,
    /// `filled`, `partially_filled`, `cancelled`, `rejected`, `open`.
    pub status: String,
    pub reason: Option<String>,
    pub submitted_ms: i64,
    pub arrived_ms: Option<i64>,
    pub done_ms: Option<i64>,
    /// Taker part: average price minus the best price on arrival (buys; reversed for sells).
    pub slippage: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EquityPoint {
    pub ts_ms: i64,
    /// USDC plus open positions at their marks.
    pub equity: f64,
}

/// Fill-weighted average execution price of an order, from the journal.
fn avg_prices(journal: &[JournalRow]) -> std::collections::BTreeMap<OrderId, (f64, f64)> {
    let mut out: std::collections::BTreeMap<OrderId, (f64, f64)> = Default::default();
    for r in journal.iter().filter(|r| r.event == "fill") {
        if let (Some(id), Some(p), Some(q)) = (r.order_id, r.price, r.quantity) {
            let e = out.entry(id).or_default();
            e.0 += p * q;
            e.1 += q;
        }
    }
    out
}

fn slippage(o: &Order) -> Option<(f64, f64)> {
    let best = tick_price(o.taker_best?);
    if o.taker_qty == 0 {
        return None;
    }
    let q = tokens(o.taker_qty);
    let avg = usd(o.taker_notional) / q;
    let s = match o.side {
        Side::Buy => avg - best,
        Side::Sell => best - avg,
    };
    Some((s, q))
}

fn record(o: &Order, w: &World, avg: Option<f64>) -> OrderRecord {
    let status = if o.rejected {
        "rejected"
    } else if o.state != OrderState::Done {
        "open"
    } else if o.filled == o.qty {
        "filled"
    } else if o.filled > 0 {
        "partially_filled"
    } else {
        "cancelled"
    };
    OrderRecord {
        id: o.id,
        market: w.markets[o.market].info.address.clone(),
        outcome: o.outcome,
        side: o.side,
        tif: o.tif,
        price: tick_price(o.ticks),
        quantity: tokens(o.qty),
        filled: tokens(o.filled),
        avg_price: avg,
        status: status.into(),
        reason: o.end_reason.clone(),
        submitted_ms: o.submitted_ms,
        arrived_ms: o.arrived_ms,
        done_ms: o.done_ms,
        slippage: slippage(o).map(|s| s.0),
    }
}

/// P&L parts of a market, micro-USDC: (pnl, open value).
fn market_pnl(w: &World, m: usize) -> (i64, i64) {
    let acc = &w.markets[m].acc;
    let up = w.markets[m].mark_up();
    let open: i64 = (0..2u8)
        .map(|o| {
            let raw = w.acct.position(m, o);
            let p = if o == 0 { up } else { 1_000_000 - up };
            (raw as i128 * p as i128 / 1_000_000) as i64
        })
        .sum();
    (acc.cash + acc.payout + open, open)
}

fn metrics(
    w: &World,
    markets: &[usize],
    equity: &[(i64, i64)],
    start_equity: i64,
    initial: i64,
) -> Metrics {
    let in_set = |m: usize| markets.contains(&m);
    let mut pnl = 0i64;
    let (mut traded, mut won, mut fills, mut reverted) = (0u32, 0u32, 0u32, 0u32);
    let (mut turnover, mut fees) = (0i64, 0i64);
    for &m in markets {
        let acc = &w.markets[m].acc;
        let p = market_pnl(w, m).0;
        pnl += p;
        turnover += acc.turnover;
        fees += acc.fees;
        fills += acc.fills;
        reverted += acc.reverted;
        if acc.fills > 0 {
            traded += 1;
            won += u32::from(p > 0);
        }
    }
    let (mut orders, mut rejected, mut takers, mut limits, mut limits_filled) = (0, 0, 0, 0, 0);
    let (mut slip_sum, mut slip_qty) = (0.0, 0.0);
    for o in w.orders.values().filter(|o| in_set(o.market)) {
        orders += 1;
        if o.rejected {
            rejected += 1;
            continue;
        }
        if let Some((s, q)) = slippage(o) {
            takers += 1;
            slip_sum += s * q;
            slip_qty += q;
        }
        if o.tif == TimeInForce::Gtc && o.arrived_ms.is_some() {
            limits += 1;
            limits_filled += u32::from(o.filled > 0);
        }
    }
    let (mut peak, mut dd, mut dd_pct) = (i64::MIN, 0i64, 0.0f64);
    for &(_, v) in equity {
        peak = peak.max(v);
        if peak - v > dd {
            dd = peak - v;
        }
        if peak > 0 {
            dd_pct = dd_pct.max((peak - v) as f64 / peak as f64 * 100.0);
        }
    }
    let end_equity = equity.last().map_or(start_equity, |p| p.1);
    Metrics {
        start_equity: usd(start_equity),
        end_equity: usd(end_equity),
        pnl: usd(pnl),
        return_pct: if initial > 0 {
            pnl as f64 / initial as f64 * 100.0
        } else {
            0.0
        },
        markets_traded: traded,
        markets_won: won,
        win_rate: (traded > 0).then(|| won as f64 / traded as f64),
        max_drawdown: usd(dd),
        max_drawdown_pct: dd_pct,
        turnover: usd(turnover),
        fees: usd(fees),
        fills,
        reverted_fills: reverted,
        orders,
        rejected_orders: rejected,
        taker_orders: takers,
        avg_slippage: (slip_qty > 0.0).then(|| slip_sum / slip_qty),
        limit_orders: limits,
        limit_orders_filled: limits_filled,
        limit_fill_rate: (limits > 0).then(|| limits_filled as f64 / limits as f64),
    }
}

pub(crate) fn build(
    data: &Dataset,
    w: &World,
    equity: &[(i64, i64)],
    strategy: String,
    from_ms: i64,
    to_ms: i64,
) -> Report {
    let initial = (w.params.initial_usdc * 1e6).round() as i64;
    let all: Vec<usize> = (0..w.markets.len()).collect();
    let f = w.params.validation_fraction;
    let split_ms =
        (f > 0.0).then(|| from_ms + ((to_ms - from_ms) as f64 * (1.0 - f)).round() as i64);
    let fit_of = |m: usize| split_ms.is_none_or(|s| w.markets[m].info.end.timestamp_millis() <= s);

    let summary = metrics(w, &all, equity, initial, initial);
    let periods = split_ms.map(|s| {
        let fit: Vec<usize> = all.iter().copied().filter(|&m| fit_of(m)).collect();
        let val: Vec<usize> = all.iter().copied().filter(|&m| !fit_of(m)).collect();
        let before: Vec<(i64, i64)> = equity.iter().copied().filter(|p| p.0 <= s).collect();
        let after: Vec<(i64, i64)> = equity.iter().copied().filter(|p| p.0 >= s).collect();
        let at_split = before.last().map_or(initial, |p| p.1);
        Periods {
            split_ms: s,
            fit: metrics(w, &fit, &before, initial, initial),
            validation: metrics(w, &val, &after, at_split, initial),
        }
    });

    let markets: Vec<MarketReport> = all
        .iter()
        .map(|&m| {
            let mk = &w.markets[m];
            let (pnl, open) = market_pnl(w, m);
            let result = match &mk.resolution {
                Some(r) if r.voided => "void".to_string(),
                Some(r) => r
                    .winner
                    .and_then(|i| mk.info.outcomes.get(i as usize).cloned())
                    .unwrap_or_else(|| "unresolved".into()),
                None => "unresolved".into(),
            };
            MarketReport {
                market: mk.info.address.clone(),
                period: mk.info.period,
                start: mk.info.start,
                end: mk.info.end,
                sample: match split_ms {
                    None => "all",
                    Some(_) if fit_of(m) => "fit",
                    Some(_) => "validation",
                }
                .into(),
                result,
                pnl: usd(pnl),
                cash: usd(mk.acc.cash),
                payout: usd(mk.acc.payout),
                open_value: usd(open),
                fees: usd(mk.acc.fees),
                turnover: usd(mk.acc.turnover),
                fills: mk.acc.fills,
                reverted_fills: mk.acc.reverted,
                orders: mk.acc.orders,
                recorded_fills: mk.recorded_fills,
            }
        })
        .collect();

    let avg = avg_prices(&w.journal);
    let orders = w
        .orders
        .values()
        .map(|o| {
            record(
                o,
                w,
                avg.get(&o.id).filter(|a| a.1 > 0.0).map(|a| a.0 / a.1),
            )
        })
        .collect();

    let mut warnings: Vec<String> = data.warnings.iter().map(|x| format!("data: {x}")).collect();
    if !data.request.data.book {
        warnings.push("the book was not loaded: orders find no liquidity".into());
    }
    if !data.request.data.fills {
        warnings.push(
            "fills were not loaded: resting orders fill only when the book trades through them"
                .into(),
        );
    }
    if summary.fills < w.params.min_fills_warning {
        warnings.push(format!(
            "only {} own fill(s), fewer than {}: too few to judge the strategy",
            summary.fills, w.params.min_fills_warning
        ));
    }
    let quiet: Vec<&str> = w
        .markets
        .iter()
        .filter(|m| m.acc.orders > 0 && m.recorded_fills == 0)
        .map(|m| m.info.address.as_str())
        .collect();
    if !quiet.is_empty() {
        warnings.push(format!(
            "{} market(s) with own orders had no recorded fills: queue positions there move only on book changes, fills of resting orders are a rough guess ({})",
            quiet.len(),
            short(&quiet)
        ));
    }
    let unresolved: Vec<&str> = w
        .markets
        .iter()
        .enumerate()
        .filter(|(m, mk)| mk.resolution.is_none() && market_pnl(w, *m).1 != 0)
        .map(|(_, mk)| mk.info.address.as_str())
        .collect();
    if !unresolved.is_empty() {
        warnings.push(format!(
            "{} market(s) with open positions have no result within the data: valued at the last book mid ({})",
            unresolved.len(),
            short(&unresolved)
        ));
    }
    if data.request.network == Network::Testnet {
        warnings.push(
            "testnet: most liquidity comes from the platform's own market maker; results say little about real-money returns"
                .into(),
        );
    }
    warnings.push(
        "own orders do not change what other participants did: the recorded book and fills are replayed as they were, so a large or frequent strategy looks better than it would be"
            .into(),
    );

    Report {
        version: REPORT_VERSION,
        strategy,
        request: data.request.clone(),
        params: w.params.clone(),
        from_ms,
        to_ms,
        summary,
        periods,
        markets,
        equity: equity
            .iter()
            .map(|&(ts_ms, v)| EquityPoint {
                ts_ms,
                equity: usd(v),
            })
            .collect(),
        orders,
        journal: w.journal.clone(),
        warnings,
    }
}

fn short(items: &[&str]) -> String {
    let mut s = items.iter().take(3).copied().collect::<Vec<_>>().join(", ");
    if items.len() > 3 {
        s.push_str(&format!(" and {} more", items.len() - 3));
    }
    s
}

impl Report {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report serializes")
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        write_atomic(path, self.to_json().as_bytes())
    }
}

const COLUMNS: [&str; 16] = [
    "ts",
    "event",
    "order_id",
    "market",
    "outcome",
    "side",
    "tif",
    "price",
    "quantity",
    "remaining",
    "liquidity",
    "match_type",
    "reason",
    "fee",
    "usdc",
    "ts_ms",
];

fn name<T: Serialize>(v: &Option<T>) -> Option<String> {
    v.as_ref()
        .and_then(|v| serde_json::to_value(v).ok())
        .and_then(|j| j.as_str().map(str::to_string))
}

fn iso(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

fn journal_csv(rows: &[JournalRow]) -> Result<Vec<u8>> {
    fn o<T: ToString>(v: &Option<T>) -> String {
        v.as_ref().map(|v| v.to_string()).unwrap_or_default()
    }
    let mut out = Vec::new();
    writeln!(out, "{}", COLUMNS.join(","))?;
    for r in rows {
        let fields = [
            iso(r.ts_ms),
            r.event.clone(),
            o(&r.order_id),
            r.market.clone(),
            o(&r.outcome),
            name(&r.side).unwrap_or_default(),
            name(&r.tif).unwrap_or_default(),
            o(&r.price),
            o(&r.quantity),
            o(&r.remaining),
            name(&r.liquidity).unwrap_or_default(),
            name(&r.match_type).unwrap_or_default(),
            o(&r.reason),
            o(&r.fee),
            r.usdc.to_string(),
            r.ts_ms.to_string(),
        ];
        let line: Vec<String> = fields
            .iter()
            .map(|s| {
                if s.contains([',', '"', '\n', '\r']) {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            })
            .collect();
        writeln!(out, "{}", line.join(","))?;
    }
    Ok(out)
}

fn journal_parquet(rows: &[JournalRow]) -> Result<Vec<u8>> {
    let utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let s = |n: &str| Field::new(n, DataType::Utf8, true);
    let f = |n: &str| Field::new(n, DataType::Float64, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", utc, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("order_id", DataType::UInt64, true),
        Field::new("market", DataType::Utf8, false),
        Field::new("outcome", DataType::Int64, true),
        s("side"),
        s("tif"),
        f("price"),
        f("quantity"),
        f("remaining"),
        s("liquidity"),
        s("match_type"),
        s("reason"),
        f("fee"),
        Field::new("usdc", DataType::Float64, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let strs = |g: &dyn Fn(&JournalRow) -> Option<String>| -> ArrayRef {
        let mut b = StringBuilder::new();
        rows.iter().for_each(|r| b.append_option(g(r)));
        Arc::new(b.finish())
    };
    let f64s = |g: &dyn Fn(&JournalRow) -> Option<f64>| -> ArrayRef {
        let mut b = Float64Builder::new();
        rows.iter().for_each(|r| b.append_option(g(r)));
        Arc::new(b.finish())
    };
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut id = UInt64Builder::new();
    let mut outcome = Int64Builder::new();
    let mut ms = Int64Builder::new();
    for r in rows {
        ts.append_value(r.ts_ms);
        id.append_option(r.order_id);
        outcome.append_option(r.outcome.map(i64::from));
        ms.append_value(r.ts_ms);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        strs(&|r| Some(r.event.clone())),
        Arc::new(id.finish()),
        strs(&|r| Some(r.market.clone())),
        Arc::new(outcome.finish()),
        strs(&|r| name(&r.side)),
        strs(&|r| name(&r.tif)),
        f64s(&|r| r.price),
        f64s(&|r| r.quantity),
        f64s(&|r| r.remaining),
        strs(&|r| name(&r.liquidity)),
        strs(&|r| name(&r.match_type)),
        strs(&|r| r.reason.clone()),
        f64s(&|r| r.fee),
        f64s(&|r| Some(r.usdc)),
        Arc::new(ms.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, schema, Some(props))?;
    w.write(&batch)?;
    w.close()?;
    Ok(out)
}

/// Writes the own-order journal to CSV or Parquet; returns the row count.
pub fn write_journal(rows: &[JournalRow], path: &Path, format: Format) -> Result<usize> {
    let data = match format {
        Format::Csv => journal_csv(rows)?,
        Format::Parquet => journal_parquet(rows)?,
    };
    write_atomic(path, &data)?;
    Ok(rows.len())
}

#[cfg(test)]
pub(crate) fn journal_bytes(rows: &[JournalRow], format: Format) -> Vec<u8> {
    match format {
        Format::Csv => journal_csv(rows).unwrap(),
        Format::Parquet => journal_parquet(rows).unwrap(),
    }
}
