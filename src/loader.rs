//! Loads what a [`Request`] selects: the market catalog from the API, then trades, order book
//! files and price files, through the local cache.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::book::{BookHistory, OrderBook, read_rows};
use crate::cache::{Cache, default_cache_dir};
use crate::error::{Error, Result};
use crate::event::{self, Event};
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

/// Markets selected by a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub markets: Vec<MarketInfo>,
    pub warnings: Vec<String>,
}

/// Loaded data of one market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub info: MarketInfo,
    /// None if the book was not requested or there is no file.
    pub book: Option<BookHistory>,
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
        })
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

    /// Markets the request selects, oldest first.
    pub async fn catalog(&self, req: &Request) -> Result<Catalog> {
        let ep = req.endpoints()?;
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
        Ok(Catalog { markets, warnings })
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
        let catalog = self.catalog(req).await?;
        let mut warnings = catalog.warnings;
        let now = Utc::now();

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
