//! Loads what a [`Request`] selects: the market catalog from the API, then order book, fills
//! and price files and on-chain trades, through the local cache.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::book::{BookHistory, OrderBook, read_rows};
use crate::cache::{Cache, default_cache_dir};
use crate::error::{Error, Result};
use crate::event::{self, Event};
use crate::fills::{Fill, read_fills};
use crate::http::Http;
use crate::market::{ApiMarket, MarketInfo};
use crate::network::Endpoints;
use crate::prices::{FIRST_PRICE_DAY, PriceTick, day_published, days, read_ticks};
use crate::request::{Period, Request};
use crate::trades::{
    ApiTrade, PAGE, Step, Trade, TradePager, market_window, plan_batches, sort_trades,
};

const MARKETS_PER_QUERY: usize = 50;
const TRADE_BATCH_MARKETS: usize = 40;
const TRADE_BATCH_SPAN: i64 = 2 * 86_400;
const FILE_DOWNLOADS: usize = 8;
/// Trades older than this are surely indexed by the API.
const TRADES_SETTLED: Duration = Duration::minutes(10);
/// A book file appears within ~26 h after a market ends; after this its absence is final.
const BOOK_FINAL_AFTER: Duration = Duration::days(2);
/// A fills file appears within ~27 h after a market ends (26 h without a row, then up to 1 h for
/// pending settlements), but the recorder's first run brings the last week of the order journal
/// at once: until 10 days after the end an absent file may still come.
const FILLS_FINAL_AFTER: Duration = Duration::days(10);

/// Markets selected by a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub markets: Vec<MarketInfo>,
    /// Markets left out by `fills_only` because they have no fills file.
    #[serde(default)]
    pub without_fills_excluded: usize,
    pub warnings: Vec<String>,
}

/// Loaded data of one market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub info: MarketInfo,
    /// None if the book was not requested or there is no file.
    pub book: Option<BookHistory>,
    /// CLOB fills; None if not requested, empty if the market has no fills file
    /// (`info.has_fills`). Order: time, then journal id.
    pub fills: Option<Vec<Fill>>,
    /// None if trades were not requested. Chain order.
    pub trades: Option<Vec<Trade>>,
}

/// Everything a request loaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub request: Request,
    pub markets: Vec<MarketData>,
    /// Ticks per feed (`btc-usd`), whole UTC days, sorted by receive time.
    pub prices: BTreeMap<String, Vec<PriceTick>>,
    /// Data that was expected but is not there (missing files, days without prices …).
    pub warnings: Vec<String>,
}

impl Dataset {
    /// All events inside the request range, in stream order.
    pub fn events(&self) -> Vec<Event> {
        event::build(self)
    }

    pub fn market(&self, address: &str) -> Option<&MarketData> {
        let a = address.to_ascii_lowercase();
        self.markets.iter().find(|m| m.info.address == a)
    }

    /// Full book of a market at `t`. None without book data or before the first snapshot.
    pub fn book_at(&self, address: &str, t: DateTime<Utc>) -> Option<OrderBook> {
        self.market(address)?
            .book
            .as_ref()?
            .at(t.timestamp_millis())
    }
}

/// Loader with its cache folder. One loader can serve many requests.
pub struct Loader {
    http: Http,
    cache_dir: PathBuf,
    /// Fixed "now" for tests.
    clock: Option<DateTime<Utc>>,
}

impl Loader {
    /// Cache in the system cache folder.
    pub fn new() -> Result<Self> {
        Self::with_cache_dir(default_cache_dir())
    }

    pub fn with_cache_dir(dir: impl Into<PathBuf>) -> Result<Self> {
        Ok(Loader {
            http: Http::new()?,
            cache_dir: dir.into(),
            clock: None,
        })
    }

    fn now(&self) -> DateTime<Utc> {
        self.clock.unwrap_or_else(Utc::now)
    }

    pub fn cache_dir(&self) -> &std::path::Path {
        &self.cache_dir
    }

    fn cache(&self, req: &Request, ep: &Endpoints) -> Cache {
        let profile = if req.api_url.is_some() || req.data_url.is_some() {
            ep.data_url.clone()
        } else {
            req.network.name().to_string()
        };
        Cache::new(&self.cache_dir, &profile)
    }

    /// Markets the request selects, oldest first. With fills requested or `fills_only`, each
    /// market's `has_fills` is filled in (the fills files are downloaded into the cache).
    pub async fn catalog(&self, req: &Request) -> Result<Catalog> {
        let ep = req.endpoints()?;
        let cache = self.cache(req, &ep);
        Ok(self.catalog_with_fills(req, &ep, &cache).await?.0)
    }

    /// The catalog and the fills files of its markets (empty unless fills are requested or
    /// filtered on).
    async fn catalog_with_fills(
        &self,
        req: &Request,
        ep: &Endpoints,
        cache: &Cache,
    ) -> Result<(Catalog, HashMap<String, bytes::Bytes>)> {
        let mut cat = self.select(req, ep).await?;
        if !req.data.fills && !req.fills_only {
            return Ok((cat, HashMap::new()));
        }
        let now = self.now();
        let files = self
            .fetch_fills(ep, cache, &cat.markets, now, &mut cat.warnings)
            .await?;
        for m in &mut cat.markets {
            m.has_fills = Some(files.contains_key(&m.address));
        }
        if req.fills_only {
            let before = cat.markets.len();
            cat.markets.retain(|m| m.has_fills == Some(true));
            cat.without_fills_excluded = before - cat.markets.len();
            if cat.without_fills_excluded > 0 {
                cat.warnings.push(format!(
                    "fills_only: {} market(s) without a fills file left out",
                    cat.without_fills_excluded
                ));
            }
        }
        Ok((cat, files))
    }

    async fn select(&self, req: &Request, ep: &Endpoints) -> Result<Catalog> {
        let mut warnings = Vec::new();
        let url = format!("{}/markets", ep.api_url);
        let mut markets = Vec::new();
        if !req.markets.is_empty() {
            for chunk in req.markets.chunks(MARKETS_PER_QUERY) {
                let q = vec![("address", chunk.join(",")), ("limit", "1000".to_string())];
                let page: Vec<ApiMarket> = self.http.api_json(&url, &q).await?;
                markets.extend(convert(page)?);
            }
            let found: HashSet<&str> = markets.iter().map(|m| m.address.as_str()).collect();
            for a in &req.markets {
                if !found.contains(a.as_str()) {
                    warnings.push(format!("market {a} not found"));
                }
            }
            let before = markets.len();
            markets.retain(|m| req.overlaps(m.start, m.end));
            if markets.len() < before {
                warnings.push(format!(
                    "{} market(s) outside the time range left out",
                    before - markets.len()
                ));
            }
        } else {
            let (from, to) = (req.from.expect("validated"), req.to.expect("validated"));
            let longest = req
                .periods
                .iter()
                .copied()
                .max()
                .unwrap_or(Period::D1)
                .duration();
            let mut base = vec![
                ("market_type", req.market_type.api_name().to_string()),
                ("order", "asc".to_string()),
            ];
            if let Some(i) = &req.instrument {
                base.push(("instrument", i.clone()));
            }
            if !req.periods.is_empty() {
                let p: Vec<&str> = req.periods.iter().map(|p| p.api_name()).collect();
                base.push(("period", p.join(",")));
            }
            let end_to = (to + longest).timestamp() + 1;
            let mut end_from = from.timestamp();
            let mut offset = 0u32;
            let mut seen = HashSet::new();
            loop {
                let mut q = base.clone();
                q.push(("end_from", end_from.to_string()));
                q.push(("end_to", end_to.to_string()));
                q.push(("limit", PAGE.to_string()));
                q.push(("offset", offset.to_string()));
                let page: Vec<ApiMarket> = self.http.api_json(&url, &q).await?;
                let full = page.len() as u32 >= PAGE;
                let page = convert(page)?;
                let last_end = page.last().map(|m| m.end.timestamp());
                markets.extend(page.into_iter().filter(|m| seen.insert(m.address.clone())));
                if !full {
                    break;
                }
                if offset + PAGE < crate::trades::MAX_OFFSET {
                    offset += PAGE;
                } else {
                    let next = last_end.unwrap_or(end_from);
                    if next <= end_from {
                        return Err(crate::error::bad(
                            "markets",
                            "more than 11000 markets end in one second",
                        ));
                    }
                    end_from = next;
                    offset = 0;
                }
            }
            markets.retain(|m| {
                m.market_type == req.market_type
                    && (req.periods.is_empty() || req.periods.contains(&m.period))
                    && req.instrument.as_ref().is_none_or(|i| *i == m.instrument)
                    && req.overlaps(m.start, m.end)
            });
            if markets.is_empty()
                && let Some(i) = &req.instrument
            {
                self.check_instrument(&url, req, i).await?;
            }
        }
        markets
            .sort_by(|a, b| (a.start, a.period, &a.address).cmp(&(b.start, b.period, &b.address)));
        Ok(Catalog {
            markets,
            without_fills_excluded: 0,
            warnings,
        })
    }

    /// An empty result for an instrument the API does not know is a typo, not "no data".
    async fn check_instrument(&self, url: &str, req: &Request, instrument: &str) -> Result<()> {
        let q = vec![
            ("market_type", req.market_type.api_name().to_string()),
            ("limit", "1000".to_string()),
        ];
        let page: Vec<ApiMarket> = self.http.api_json(url, &q).await?;
        let known: BTreeSet<String> = convert(page)?.into_iter().map(|m| m.instrument).collect();
        if known.is_empty() || known.contains(instrument) {
            return Ok(());
        }
        Err(Error::InvalidRequest(format!(
            "instrument {instrument} is not listed; recent markets have: {}",
            known.into_iter().collect::<Vec<_>>().join(", ")
        )))
    }

    /// Catalog plus the requested data. Cached files are reused; trades of markets that are not
    /// over yet are topped up.
    pub async fn load(&self, req: &Request) -> Result<Dataset> {
        let ep = req.endpoints()?;
        let cache = self.cache(req, &ep);
        let (catalog, mut fill_files) = self.catalog_with_fills(req, &ep, &cache).await?;
        let mut warnings = catalog.warnings;
        let now = self.now();

        let mut fills = HashMap::new();
        if req.data.fills {
            for m in &catalog.markets {
                let Some(file) = fill_files.remove(&m.address) else {
                    continue;
                };
                let (f, unknown) = read_fills(m, file)?;
                if unknown > 0 {
                    warnings.push(format!(
                        "fills {}: {unknown} fill(s) of unknown tokens (no outcome index)",
                        m.address
                    ));
                }
                fills.insert(m.address.clone(), f);
            }
        }

        let mut trades = if req.data.trades {
            self.load_trades(&ep, &cache, &catalog.markets, now).await?
        } else {
            HashMap::new()
        };
        let mut books = if req.data.book {
            self.load_books(&ep, &cache, &catalog.markets, now, &mut warnings)
                .await?
        } else {
            HashMap::new()
        };
        let prices = if req.data.prices {
            self.load_prices(&ep, &cache, req, &catalog.markets, now, &mut warnings)
                .await?
        } else {
            BTreeMap::new()
        };
        let markets = catalog
            .markets
            .into_iter()
            .map(|info| MarketData {
                book: books.remove(&info.address),
                fills: req
                    .data
                    .fills
                    .then(|| fills.remove(&info.address).unwrap_or_default()),
                trades: req
                    .data
                    .trades
                    .then(|| trades.remove(&info.address).unwrap_or_default()),
                info,
            })
            .collect();
        Ok(Dataset {
            request: req.clone(),
            markets,
            prices,
            warnings,
        })
    }

    async fn load_trades(
        &self,
        ep: &Endpoints,
        cache: &Cache,
        markets: &[MarketInfo],
        now: DateTime<Utc>,
    ) -> Result<HashMap<String, Vec<Trade>>> {
        let mut out = HashMap::new();
        let mut cached = HashMap::new();
        let mut jobs = Vec::new();
        for m in markets {
            let (start, end) = market_window(m);
            match cache.read_trades(&m.address)? {
                Some(f) if f.complete => {
                    out.insert(m.address.clone(), f.trades);
                }
                f => {
                    let from = f
                        .as_ref()
                        .map_or(start.timestamp(), |f| f.fetched_to.max(start.timestamp()));
                    jobs.push((m.address.clone(), from, end.timestamp()));
                    cached.insert(m.address.clone(), f.unwrap_or_default());
                }
            }
        }
        let by_addr: HashMap<&str, &MarketInfo> =
            markets.iter().map(|m| (m.address.as_str(), m)).collect();
        let url = format!("{}/trades", ep.api_url);
        for batch in plan_batches(jobs, TRADE_BATCH_MARKETS, TRADE_BATCH_SPAN) {
            let mut got: HashMap<String, Vec<Trade>> = HashMap::new();
            let mut pager = TradePager::new(batch.from, batch.to);
            loop {
                let (from, to, offset) = pager.query();
                let q = vec![
                    ("market", batch.markets.join(",")),
                    ("from", from.to_string()),
                    ("to", to.to_string()),
                    ("order", "asc".to_string()),
                    ("limit", PAGE.to_string()),
                    ("offset", offset.to_string()),
                ];
                let page: Vec<ApiTrade> = self.http.api_json(&url, &q).await?;
                let (rows, step) = pager.take(page)?;
                for t in rows {
                    got.entry(t.market.clone()).or_default().push(t);
                }
                if step == Step::Done {
                    break;
                }
            }
            for addr in &batch.markets {
                let m = by_addr[addr.as_str()];
                let mut file = cached.remove(addr).unwrap_or_default();
                let known: HashSet<String> =
                    file.trades.iter().map(|t| t.trade_id.clone()).collect();
                file.trades.extend(
                    got.remove(addr)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|t| !known.contains(&t.trade_id)),
                );
                sort_trades(&mut file.trades);
                let (_, window_end) = market_window(m);
                file.complete = m.state.is_final() && now - TRADES_SETTLED >= window_end;
                file.fetched_to = (now - TRADES_SETTLED).min(window_end).timestamp();
                cache.write_trades(addr, &file)?;
                out.insert(addr.clone(), file.trades);
            }
        }
        Ok(out)
    }

    async fn load_books(
        &self,
        ep: &Endpoints,
        cache: &Cache,
        markets: &[MarketInfo],
        now: DateTime<Utc>,
        warnings: &mut Vec<String>,
    ) -> Result<HashMap<String, BookHistory>> {
        let mut not_over = 0;
        let mut absent = Vec::new();
        let mut todo = Vec::new();
        for m in markets {
            if m.end > now {
                not_over += 1;
            } else {
                todo.push(m);
            }
        }
        let results: Vec<Result<(&MarketInfo, Option<bytes::Bytes>)>> = stream::iter(todo)
            .map(|m| async move {
                let path = cache.book_path(&m.book_path());
                if let Some(b) = cache.read(&path)? {
                    return Ok((m, Some(b)));
                }
                if cache.is_missing(&path) {
                    return Ok((m, None));
                }
                let url = format!("{}/books/{}", ep.data_url, m.book_path());
                match self.http.data_file(&url).await? {
                    Some(b) => {
                        cache.write(&path, &b)?;
                        Ok((m, Some(b)))
                    }
                    None => {
                        if now - m.end > BOOK_FINAL_AFTER {
                            cache.mark_missing(&path)?;
                        }
                        Ok((m, None))
                    }
                }
            })
            .buffer_unordered(FILE_DOWNLOADS)
            .collect()
            .await;
        let mut out = HashMap::new();
        for r in results {
            let (m, file) = r?;
            let Some(file) = file else {
                absent.push(m.address.clone());
                continue;
            };
            let (h, skipped) = BookHistory::from_rows(m, read_rows(file)?);
            if skipped > 0 {
                warnings.push(format!(
                    "book {}: {skipped} row(s) of unknown tokens skipped",
                    m.address
                ));
            }
            out.insert(m.address.clone(), h);
        }
        if not_over > 0 {
            warnings.push(format!(
                "book: {not_over} market(s) not over yet; a book file appears after the market ends and leaves the order book"
            ));
        }
        if !absent.is_empty() {
            absent.sort();
            warnings.push(format!(
                "book: no file for {} market(s) (never had an order, or not uploaded yet): {}",
                absent.len(),
                short_list(&absent)
            ));
        }
        Ok(out)
    }

    /// Fills files of the markets that have one. Markets not over yet are not asked for.
    async fn fetch_fills(
        &self,
        ep: &Endpoints,
        cache: &Cache,
        markets: &[MarketInfo],
        now: DateTime<Utc>,
        warnings: &mut Vec<String>,
    ) -> Result<HashMap<String, bytes::Bytes>> {
        let (todo, not_over): (Vec<&MarketInfo>, Vec<&MarketInfo>) =
            markets.iter().partition(|m| m.end <= now);
        let results: Vec<Result<(&MarketInfo, Option<bytes::Bytes>)>> = stream::iter(todo)
            .map(|m| async move {
                let path = cache.fills_path(&m.fills_path());
                if let Some(b) = cache.read(&path)? {
                    return Ok((m, Some(b)));
                }
                if cache.is_missing(&path) {
                    return Ok((m, None));
                }
                let url = format!("{}/fills/{}", ep.data_url, m.fills_path());
                match self.http.data_file(&url).await? {
                    Some(b) => {
                        cache.write(&path, &b)?;
                        Ok((m, Some(b)))
                    }
                    None => {
                        if now - m.end > FILLS_FINAL_AFTER {
                            cache.mark_missing(&path)?;
                        }
                        Ok((m, None))
                    }
                }
            })
            .buffer_unordered(FILE_DOWNLOADS)
            .collect()
            .await;
        let mut out = HashMap::new();
        let mut absent = Vec::new();
        for r in results {
            match r? {
                (m, Some(b)) => {
                    out.insert(m.address.clone(), b);
                }
                (m, None) => absent.push(m.address.clone()),
            }
        }
        if !not_over.is_empty() {
            warnings.push(format!(
                "fills: {} market(s) not over yet; a fills file appears after the market ends",
                not_over.len()
            ));
        }
        if !absent.is_empty() {
            absent.sort();
            warnings.push(format!(
                "fills: no file for {} market(s) (no match, before the fills history of about 2026-10-20, or not uploaded yet): {}",
                absent.len(),
                short_list(&absent)
            ));
        }
        Ok(out)
    }

    async fn load_prices(
        &self,
        ep: &Endpoints,
        cache: &Cache,
        req: &Request,
        markets: &[MarketInfo],
        now: DateTime<Utc>,
        warnings: &mut Vec<String>,
    ) -> Result<BTreeMap<String, Vec<PriceTick>>> {
        let mut wanted: BTreeSet<(String, NaiveDate)> = BTreeSet::new();
        for m in markets {
            let Some(feed) = m.feed_slug() else {
                warnings.push(format!("prices: market {} has no named feed", m.address));
                continue;
            };
            let from = req.from.map_or(m.start, |f| f.max(m.start));
            let to = req.to.map_or(m.end, |t| t.min(m.end));
            for d in days(from, to) {
                wanted.insert((feed.clone(), d));
            }
        }
        let mut before_start = BTreeSet::new();
        let mut unpublished = BTreeSet::new();
        let mut todo = Vec::new();
        for (feed, day) in wanted {
            if day < FIRST_PRICE_DAY {
                before_start.insert(day);
            } else if !day_published(day, now) {
                unpublished.insert(day);
            } else {
                todo.push((feed, day));
            }
        }
        let results: Vec<Result<(String, NaiveDate, Option<bytes::Bytes>)>> = stream::iter(todo)
            .map(|(feed, day)| async move {
                let day_s = day.format("%Y-%m-%d").to_string();
                let path = cache.price_path(&feed, &day_s);
                if let Some(b) = cache.read(&path)? {
                    return Ok((feed, day, Some(b)));
                }
                if cache.is_missing(&path) {
                    return Ok((feed, day, None));
                }
                let url = format!("{}/prices/pyth/{feed}/{day_s}.parquet", ep.data_url);
                let file = self.http.data_file(&url).await?;
                match &file {
                    Some(b) => cache.write(&path, b)?,
                    None if !day_published(day + Duration::days(1), now) => {}
                    None => cache.mark_missing(&path)?,
                }
                Ok((feed, day, file))
            })
            .buffer_unordered(FILE_DOWNLOADS)
            .collect()
            .await;
        let mut out: BTreeMap<String, Vec<PriceTick>> = BTreeMap::new();
        let mut absent = Vec::new();
        for r in results {
            let (feed, day, file) = r?;
            match file {
                Some(b) => out
                    .entry(feed.clone())
                    .or_default()
                    .extend(read_ticks(&feed, b)?),
                None => absent.push(format!("{feed} {day}")),
            }
        }
        for ticks in out.values_mut() {
            ticks.sort_by_key(|t| (t.receive_ms, t.publish_ms));
            ticks.dedup_by_key(|t| (t.receive_ms, t.publish_ms));
        }
        if !before_start.is_empty() {
            warnings.push(format!(
                "prices: price history starts {FIRST_PRICE_DAY}; no prices for {}",
                day_span(&before_start)
            ));
        }
        if !unpublished.is_empty() {
            let d: Vec<String> = unpublished.iter().map(|d| d.to_string()).collect();
            warnings.push(format!(
                "prices: a day's file appears after the UTC day ends; not yet available: {}",
                d.join(", ")
            ));
        }
        if !absent.is_empty() {
            absent.sort();
            warnings.push(format!("prices: no file for {}", short_list(&absent)));
        }
        Ok(out)
    }
}

fn convert(page: Vec<ApiMarket>) -> Result<Vec<MarketInfo>> {
    Ok(page
        .into_iter()
        .map(ApiMarket::into_info)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect())
}

fn day_span(days: &BTreeSet<NaiveDate>) -> String {
    match (days.first(), days.last()) {
        (Some(a), Some(b)) if a != b => format!("{a} … {b} ({} days)", days.len()),
        (Some(a), _) => a.to_string(),
        _ => String::new(),
    }
}

fn short_list(items: &[String]) -> String {
    const SHOW: usize = 5;
    if items.len() <= SHOW {
        items.join(", ")
    } else {
        format!(
            "{} and {} more",
            items[..SHOW].join(", "),
            items.len() - SHOW
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow_array::{
        ArrayRef, Float64Array, RecordBatch, TimestampMillisecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::event::EventKind;
    use crate::request::{DataKinds, parse_time};

    type Tick = (i64, i64, f64, f64, Option<u64>);

    /// A price file exactly as book-recorder writes it (`archive::prices_to_parquet`): sorted by
    /// `publish_time`, nullable `slot`, default writer settings.
    fn price_file(rows: &[Tick]) -> Vec<u8> {
        let utc = || DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("publish_time", utc(), false),
            Field::new("receive_time", utc(), false),
            Field::new("price", DataType::Float64, false),
            Field::new("conf", DataType::Float64, false),
            Field::new("slot", DataType::UInt64, true),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMillisecondArray::from_iter_values(rows.iter().map(|r| r.0))
                    .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(rows.iter().map(|r| r.1))
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.3))),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|r| r.4))),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out
    }

    /// Minimal HTTP server: path → body, anything else 404. Logs requested paths.
    async fn serve(routes: Vec<(&'static str, Vec<u8>)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let seen = log.clone();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let mut buf = vec![0u8; 16384];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let target = req.split_whitespace().nth(1).unwrap_or("");
                let path = target.split('?').next().unwrap_or("").to_string();
                seen.lock().unwrap().push(path.clone());
                let body = routes.iter().find(|r| r.0 == path).map(|r| r.1.clone());
                let head = match &body {
                    Some(b) => format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        b.len()
                    ),
                    None => {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                };
                let _ = s.write_all(head.as_bytes()).await;
                if let Some(b) = body {
                    let _ = s.write_all(&b).await;
                }
                let _ = s.shutdown().await;
            }
        });
        (format!("http://{addr}"), log)
    }

    const ADDR: &str = "0x72037b5314f64f03e899160a22c6097da6403f4d42f3684a9ac544ab944a7ae2";

    /// A 1h market across midnight UTC: prices come from two day files.
    const MARKET: &str = r#"[{"type":"market","address":"0x72037b5314f64f03e899160a22c6097da6403f4d42f3684a9ac544ab944a7ae2","instrument":"BTC","market_type":"UpDown","period":"1h","start_date":"2026-10-28T23:30:00Z","end_date":"2026-10-29T00:30:00Z","resolved_at":"2026-10-29T00:31:00Z","outcomes":["up","down"],"state":"settled","winner":"up","volume_traded":0,"strike":8000000000000,"resolved_value":8010000000000,"oracle":{"provider":"pyth","feeds":[{"id":"0xe62d","name":"BTC/USD","url":null}]}}]"#;

    fn request(base: &str) -> Request {
        Request::builder()
            .api_url(base)
            .data_url(base)
            .market(ADDR)
            .data(DataKinds {
                prices: true,
                ..DataKinds::NONE
            })
            .build()
            .unwrap()
    }

    fn ms(s: &str) -> i64 {
        parse_time(s).unwrap().timestamp_millis()
    }

    #[tokio::test]
    async fn prices_from_recorder_files() {
        let day1 = price_file(&[
            (
                ms("2026-10-28T23:00:00Z"),
                ms("2026-10-28T23:00:00Z") + 300,
                79000.0,
                50.0,
                Some(1),
            ),
            (
                ms("2026-10-28T23:45:00Z"),
                ms("2026-10-28T23:45:00Z") + 200,
                80050.5,
                40.0,
                Some(2),
            ),
            (
                ms("2026-10-28T23:45:01Z"),
                ms("2026-10-28T23:45:01Z") + 150,
                80051.0,
                41.0,
                None,
            ),
        ]);
        let day2 = price_file(&[
            (
                ms("2026-10-29T00:10:00Z"),
                ms("2026-10-29T00:10:00Z") + 100,
                80100.0,
                30.0,
                Some(9),
            ),
            (
                ms("2026-10-29T01:00:00Z"),
                ms("2026-10-29T01:00:00Z") + 100,
                80200.0,
                30.0,
                Some(10),
            ),
        ]);
        let (base, log) = serve(vec![
            ("/markets", MARKET.as_bytes().to_vec()),
            ("/prices/pyth/btc-usd/2026-10-28.parquet", day1),
            ("/prices/pyth/btc-usd/2026-10-29.parquet", day2),
        ])
        .await;
        let req = request(&base);
        let cache = tempfile::tempdir().unwrap();
        let mut loader = Loader::with_cache_dir(cache.path()).unwrap();
        loader.clock = Some(parse_time("2026-10-30T12:00:00Z").unwrap());

        let ds = loader.load(&req).await.unwrap();
        assert!(ds.warnings.is_empty(), "{:?}", ds.warnings);
        let ticks = &ds.prices["btc-usd"];
        assert_eq!(ticks.len(), 5, "whole days are kept");
        assert_eq!(ticks[2].slot, None);
        assert_eq!(ticks[1].receive_ms - ticks[1].publish_ms, 200);

        // The stream has only the ticks inside the market (no range in the request).
        let prices: Vec<f64> = ds
            .events()
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::PriceTick { price, feed, .. } if feed == "btc-usd" => Some(*price),
                _ => None,
            })
            .collect();
        assert_eq!(prices, vec![80050.5, 80051.0, 80100.0]);

        // Second load: both days from the cache.
        let files = |log: &Mutex<Vec<String>>| {
            log.lock()
                .unwrap()
                .iter()
                .filter(|p| p.starts_with("/prices"))
                .count()
        };
        assert_eq!(files(&log), 2);
        loader.load(&req).await.unwrap();
        assert_eq!(files(&log), 2);
    }

    #[tokio::test]
    async fn missing_and_unpublished_price_days_are_warnings() {
        let (base, _) = serve(vec![("/markets", MARKET.as_bytes().to_vec())]).await;
        let cache = tempfile::tempdir().unwrap();
        let mut loader = Loader::with_cache_dir(cache.path()).unwrap();
        // The 28th is over but has no file, the 29th is still running.
        loader.clock = Some(parse_time("2026-10-29T06:00:00Z").unwrap());
        let ds = loader.load(&request(&base)).await.unwrap();
        assert!(ds.prices.is_empty());
        let w = ds.warnings.join(" | ");
        assert!(w.contains("no file for btc-usd 2026-10-28"), "{w}");
        assert!(w.contains("not yet available: 2026-10-29"), "{w}");
    }

    // ---- fills ----

    const ADDR2: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

    /// MARKET, and the same market under ADDR2.
    fn two_markets() -> String {
        format!(
            "[{},{}]",
            &MARKET[1..MARKET.len() - 1],
            MARKET[1..MARKET.len() - 1].replace(ADDR, ADDR2)
        )
    }

    fn info(addr: &str) -> MarketInfo {
        let page: Vec<ApiMarket> = serde_json::from_str(&MARKET.replace(ADDR, addr)).unwrap();
        convert(page).unwrap().remove(0)
    }

    fn fills_route(addr: &str) -> &'static str {
        Box::leak(format!("/fills/updown/1h/btc/2026-10-28/{addr}.parquet").into_boxed_str())
    }

    /// A taker order against two makers (same ms), then a failed fill.
    fn fills_fixture(addr: &str) -> Vec<u8> {
        use crate::fills::tests::{Row, fills_file, row};
        let m = info(addr);
        let t = ms("2026-10-28T23:40:00Z");
        fills_file(&[
            row(&m, t, 101, "o1:m1"),
            row(&m, t, 102, "o1:m2"),
            Row {
                settlement: "failed",
                ..row(&m, t + 5_000, 103, "o2:m3")
            },
        ])
    }

    const TRADES: &str = r#"[{"trade_id":"o1:m1","tx_hash":"0xtx","log_index":3,"block_number":11900000,"block_time":"2026-10-28T23:40:12Z","market_id":"0x72037b5314f64f03e899160a22c6097da6403f4d42f3684a9ac544ab944a7ae2","match_type":"direct","trader":"0xaa","side":"buy","outcome_index":0,"price":0.55,"quantity":2500000,"amount_usd":1.375}]"#;

    fn loader_at(dir: &std::path::Path) -> Loader {
        let mut loader = Loader::with_cache_dir(dir).unwrap();
        loader.clock = Some(parse_time("2026-10-30T12:00:00Z").unwrap());
        loader
    }

    fn kinds(ds: &Dataset) -> Vec<(&'static str, String)> {
        ds.events()
            .into_iter()
            .filter_map(|e| match e.kind {
                EventKind::Fill { fill } => Some(("fill", fill.trade_id)),
                EventKind::Trade { trade } => Some(("trade", trade.trade_id)),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn default_kinds_load_fills_not_trades() {
        let (base, log) = serve(vec![
            ("/markets", MARKET.as_bytes().to_vec()),
            ("/trades", TRADES.as_bytes().to_vec()),
            (fills_route(ADDR), fills_fixture(ADDR)),
        ])
        .await;
        let req = Request::builder()
            .api_url(&base)
            .data_url(&base)
            .market(ADDR)
            .build()
            .unwrap();
        assert_eq!(req.data, DataKinds::DEFAULT);
        let cache = tempfile::tempdir().unwrap();
        let loader = loader_at(cache.path());
        let ds = loader.load(&req).await.unwrap();

        assert!(
            !log.lock().unwrap().iter().any(|p| p == "/trades"),
            "trades not asked for"
        );
        let m = &ds.markets[0];
        assert_eq!(m.info.has_fills, Some(true));
        assert!(m.trades.is_none());
        let fills = m.fills.as_ref().unwrap();
        assert_eq!(fills.len(), 3);
        assert_eq!(fills[2].settlement, crate::fills::Settlement::Failed);
        assert_eq!(
            kinds(&ds),
            vec![
                ("fill", "o1:m1".to_string()),
                ("fill", "o1:m2".to_string()),
                ("fill", "o2:m3".to_string())
            ]
        );
        // multi-maker fills are adjacent in the stream
        let ev = ds.events();
        let pos: Vec<usize> = ev
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e.kind, EventKind::Fill { .. }))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(pos[1], pos[0] + 1);

        // second load reads the fills file from the cache
        let fetched = |log: &Mutex<Vec<String>>| {
            log.lock()
                .unwrap()
                .iter()
                .filter(|p| p.starts_with("/fills"))
                .count()
        };
        assert_eq!(fetched(&log), 1);
        loader.load(&req).await.unwrap();
        assert_eq!(fetched(&log), 1);
    }

    #[tokio::test]
    async fn missing_fills_file_is_a_flag_not_an_error() {
        let (base, log) = serve(vec![("/markets", MARKET.as_bytes().to_vec())]).await;
        let req = Request::builder()
            .api_url(&base)
            .data_url(&base)
            .market(ADDR)
            .data(DataKinds {
                fills: true,
                ..DataKinds::NONE
            })
            .build()
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut loader = loader_at(cache.path());
        let ds = loader.load(&req).await.unwrap();
        let m = &ds.markets[0];
        assert_eq!(m.info.has_fills, Some(false));
        assert_eq!(m.fills.as_deref(), Some(&[][..]));
        let w = ds.warnings.join(" | ");
        assert!(w.contains("fills: no file for 1 market(s)"), "{w}");

        // Within ten days of the end the file may still come: asked for again. After that the
        // absence is final.
        let asked = |log: &Mutex<Vec<String>>| {
            log.lock()
                .unwrap()
                .iter()
                .filter(|p| p.starts_with("/fills"))
                .count()
        };
        assert_eq!(asked(&log), 1);
        loader.clock = Some(parse_time("2026-11-09T00:00:00Z").unwrap());
        loader.load(&req).await.unwrap();
        assert_eq!(asked(&log), 2);
        let ds = loader.load(&req).await.unwrap();
        assert_eq!(asked(&log), 2);
        assert_eq!(ds.markets[0].info.has_fills, Some(false));
    }

    #[tokio::test]
    async fn fills_and_trades_are_two_streams() {
        let (base, _) = serve(vec![
            ("/markets", MARKET.as_bytes().to_vec()),
            ("/trades", TRADES.as_bytes().to_vec()),
            (fills_route(ADDR), fills_fixture(ADDR)),
        ])
        .await;
        let req = Request::builder()
            .api_url(&base)
            .data_url(&base)
            .market(ADDR)
            .data("fills,trades".parse().unwrap())
            .build()
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let ds = loader_at(cache.path()).load(&req).await.unwrap();
        // the same match o1:m1 comes once from each source, nothing merged
        assert_eq!(
            kinds(&ds),
            vec![
                ("fill", "o1:m1".to_string()),
                ("fill", "o1:m2".to_string()),
                ("fill", "o2:m3".to_string()),
                ("trade", "o1:m1".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn fills_only_leaves_out_markets_without_a_file() {
        let (base, _) = serve(vec![
            ("/markets", two_markets().into_bytes()),
            (fills_route(ADDR2), fills_fixture(ADDR2)),
        ])
        .await;
        let req = Request::builder()
            .api_url(&base)
            .data_url(&base)
            .markets([ADDR, ADDR2])
            .data(DataKinds::NONE)
            .fills_only(true)
            .build()
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let loader = loader_at(cache.path());
        let cat = loader.catalog(&req).await.unwrap();
        assert_eq!(cat.without_fills_excluded, 1);
        assert_eq!(cat.markets.len(), 1);
        assert_eq!(cat.markets[0].address, ADDR2);
        assert!(
            cat.warnings
                .iter()
                .any(|w| w.contains("1 market(s) without a fills file"))
        );

        let ds = loader.load(&req).await.unwrap();
        assert_eq!(ds.markets.len(), 1);
        assert!(ds.markets[0].fills.is_none(), "filtered on, not requested");

        // without the option both stay, each with its flag
        let req = Request {
            fills_only: false,
            data: DataKinds::DEFAULT,
            ..req
        };
        let cat = loader.catalog(&req).await.unwrap();
        let flags: Vec<_> = cat
            .markets
            .iter()
            .map(|m| (m.address.as_str(), m.has_fills))
            .collect();
        assert_eq!(cat.without_fills_excluded, 0);
        assert!(flags.contains(&(ADDR, Some(false))) && flags.contains(&(ADDR2, Some(true))));
    }

    #[tokio::test]
    async fn market_not_over_has_no_fills_without_asking() {
        let (base, log) = serve(vec![("/markets", MARKET.as_bytes().to_vec())]).await;
        let req = Request::builder()
            .api_url(&base)
            .data_url(&base)
            .market(ADDR)
            .data(DataKinds {
                fills: true,
                ..DataKinds::NONE
            })
            .build()
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut loader = Loader::with_cache_dir(cache.path()).unwrap();
        loader.clock = Some(parse_time("2026-10-29T00:00:00Z").unwrap());
        let ds = loader.load(&req).await.unwrap();
        assert_eq!(ds.markets[0].info.has_fills, Some(false));
        assert!(ds.warnings.iter().any(|w| w.contains("not over yet")));
        assert!(!log.lock().unwrap().iter().any(|p| p.starts_with("/fills")));
    }
}
