//! What to load: one [`Request`], built with [`Request::builder`] or read from JSON.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::network::{Endpoints, Network, NetworkConfig};

/// Market type. Only `UpDown` exists today; more will be added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MarketType {
    /// Will the price be above the strike (price at open) at expiry.
    #[default]
    UpDown,
}

impl MarketType {
    /// Name in the API (`market_type`).
    pub fn api_name(self) -> &'static str {
        match self {
            MarketType::UpDown => "UpDown",
        }
    }

    /// Folder name on the data host.
    pub fn path_segment(self) -> &'static str {
        match self {
            MarketType::UpDown => "updown",
        }
    }
}

impl FromStr for MarketType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "updown" | "up-down" | "up_down" => Ok(MarketType::UpDown),
            _ => Err(Error::InvalidRequest(format!(
                "unknown market type {s:?}: UpDown"
            ))),
        }
    }
}

impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

/// Market series: how long a market lasts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Period {
    #[serde(rename = "5m")]
    /// 5 minutes.
    M5,
    #[serde(rename = "15m")]
    /// 15 minutes.
    M15,
    #[serde(rename = "30m")]
    /// 30 minutes.
    M30,
    #[serde(rename = "1h")]
    /// 1 hour.
    H1,
    #[serde(rename = "4h")]
    /// 4 hours.
    H4,
    #[serde(rename = "1d", alias = "daily", alias = "Daily")]
    /// 1 day.
    D1,
}

impl Period {
    /// Every series, shortest first.
    pub const ALL: [Period; 6] = [
        Period::M5,
        Period::M15,
        Period::M30,
        Period::H1,
        Period::H4,
        Period::D1,
    ];

    /// Length of one market.
    pub fn duration(self) -> Duration {
        match self {
            Period::M5 => Duration::minutes(5),
            Period::M15 => Duration::minutes(15),
            Period::M30 => Duration::minutes(30),
            Period::H1 => Duration::hours(1),
            Period::H4 => Duration::hours(4),
            Period::D1 => Duration::days(1),
        }
    }

    /// Short name used here and in JSON: `5m` … `1d`.
    pub fn as_str(self) -> &'static str {
        match self {
            Period::M5 => "5m",
            Period::M15 => "15m",
            Period::M30 => "30m",
            Period::H1 => "1h",
            Period::H4 => "4h",
            Period::D1 => "1d",
        }
    }

    /// Name in the API (`period`): the daily series is `Daily` there.
    pub fn api_name(self) -> &'static str {
        match self {
            Period::D1 => "Daily",
            p => p.as_str(),
        }
    }

    /// Folder name on the data host.
    pub fn path_segment(self) -> &'static str {
        match self {
            Period::D1 => "daily",
            p => p.as_str(),
        }
    }
}

impl FromStr for Period {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "5m" => Ok(Period::M5),
            "15m" => Ok(Period::M15),
            "30m" => Ok(Period::M30),
            "1h" | "60m" => Ok(Period::H1),
            "4h" => Ok(Period::H4),
            "1d" | "24h" | "daily" => Ok(Period::D1),
            _ => Err(Error::InvalidRequest(format!(
                "unknown period {s:?}: 5m, 15m, 30m, 1h, 4h or 1d"
            ))),
        }
    }
}

impl fmt::Display for Period {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which data to load besides the market catalog. `fills` (order book matches) and `trades`
/// (on-chain fills) are two separate sources; either, both or none can be loaded, and they are
/// never merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataKinds {
    #[serde(default)]
    /// L2 order book history.
    pub book: bool,
    /// CLOB fills from the data host.
    #[serde(default)]
    pub fills: bool,
    /// On-chain fills from `GET /trades`.
    #[serde(default)]
    pub trades: bool,
    #[serde(default)]
    /// Pyth price history.
    pub prices: bool,
}

impl DataKinds {
    /// Everything, on-chain trades included.
    pub const ALL: DataKinds = DataKinds {
        book: true,
        fills: true,
        trades: true,
        prices: true,
    };
    /// Nothing (catalog only).
    pub const NONE: DataKinds = DataKinds {
        book: false,
        fills: false,
        trades: false,
        prices: false,
    };
    /// `book,fills,prices`: on-chain trades only when asked for.
    pub const DEFAULT: DataKinds = DataKinds {
        book: true,
        fills: true,
        trades: false,
        prices: true,
    };
}

impl Default for DataKinds {
    fn default() -> Self {
        DataKinds::DEFAULT
    }
}

impl FromStr for DataKinds {
    type Err = Error;

    /// `book,fills,trades,prices` in any combination, `all` or `none`.
    fn from_str(s: &str) -> Result<Self> {
        let mut out = DataKinds::NONE;
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            match part.to_ascii_lowercase().as_str() {
                "all" => out = DataKinds::ALL,
                "none" => {}
                "book" | "books" | "orderbook" => out.book = true,
                "fills" | "fill" => out.fills = true,
                "trades" | "trade" => out.trades = true,
                "prices" | "price" => out.prices = true,
                _ => {
                    return Err(Error::InvalidRequest(format!(
                        "unknown data kind {part:?}: book, fills, trades, prices, all or none"
                    )));
                }
            }
        }
        Ok(out)
    }
}

/// What to load. A market is selected by address (`markets`) or by filters (`market_type`,
/// `instrument`, `periods` and the time range); a market matches the range if it overlaps it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default)]
    /// Which deployment.
    pub network: Network,
    /// Overrides the profile's API URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    /// Overrides the profile's data host URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_url: Option<String>,
    #[serde(default)]
    /// Market type.
    pub market_type: MarketType,
    /// Underlying asset as `/markets` returns it, e.g. `BTC`. None = any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument: Option<String>,
    /// Series. Empty = all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub periods: Vec<Period>,
    /// Market addresses, lowercase `0x` + 64 hex. An alternative to instrument and periods.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub markets: Vec<String>,
    /// Range start, UTC, inclusive. Required unless `markets` is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<DateTime<Utc>>,
    /// Range end, UTC, exclusive. Required unless `markets` is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    /// Which data to load.
    pub data: DataKinds,
    /// Leave out markets without a fills file (no match, not over yet, or older than the fills
    /// history); the catalog counts them.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fills_only: bool,
}

impl Request {
    /// A builder with defaults.
    pub fn builder() -> RequestBuilder {
        RequestBuilder::default()
    }

    /// Parses and validates a JSON request.
    pub fn from_json(json: &str) -> Result<Self> {
        let req: Request = serde_json::from_str(json)
            .map_err(|e| Error::InvalidRequest(format!("bad JSON: {e}")))?;
        req.validate()
    }

    /// Base URLs: the overrides, else the network profile.
    pub fn endpoints(&self) -> Result<Endpoints> {
        NetworkConfig {
            network: self.network,
            api_url: self.api_url.clone(),
            data_url: self.data_url.clone(),
        }
        .endpoints()
    }

    /// The request as pretty JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("request serializes")
    }

    /// Checks the request and normalizes it (lowercase addresses, sorted unique periods).
    pub fn validate(mut self) -> Result<Self> {
        let invalid = |m: String| Err(Error::InvalidRequest(m));
        let mut markets = Vec::with_capacity(self.markets.len());
        for m in &self.markets {
            let m = m.trim().to_ascii_lowercase();
            let hex = m.strip_prefix("0x").unwrap_or("");
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return invalid(format!(
                    "market {m:?} must be 0x followed by 64 hex characters"
                ));
            }
            if !markets.contains(&m) {
                markets.push(m);
            }
        }
        self.markets = markets;
        self.periods.sort();
        self.periods.dedup();
        if let Some(i) = &self.instrument {
            let i = i.trim();
            if i.is_empty() || !i.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return invalid(format!(
                    "instrument {i:?} must be letters and digits, e.g. BTC"
                ));
            }
            self.instrument = Some(i.to_ascii_uppercase());
        }
        if !self.markets.is_empty() && (self.instrument.is_some() || !self.periods.is_empty()) {
            return invalid(
                "markets selects by address: leave instrument and periods empty".into(),
            );
        }
        if self.markets.is_empty() && (self.from.is_none() || self.to.is_none()) {
            return invalid("from and to are required unless markets are given".into());
        }
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from >= to
        {
            return invalid(format!("from {from} must be before to {to}"));
        }
        Ok(self)
    }

    /// Whether a market lasting `[start, end)` overlaps the range.
    pub fn overlaps(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        self.from.is_none_or(|f| end > f) && self.to.is_none_or(|t| start < t)
    }

    /// Whether an event at `t` is inside the range.
    pub fn contains(&self, t: DateTime<Utc>) -> bool {
        self.from.is_none_or(|f| t >= f) && self.to.is_none_or(|to| t < to)
    }
}

/// Builder of [`Request`]; [`RequestBuilder::build`] validates.
#[derive(Debug, Clone, Default)]
pub struct RequestBuilder {
    req: RequestDraft,
}

#[derive(Debug, Clone, Default)]
struct RequestDraft {
    network: NetworkConfig,
    market_type: MarketType,
    instrument: Option<String>,
    periods: Vec<Period>,
    markets: Vec<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    data: DataKinds,
    fills_only: bool,
}

impl RequestBuilder {
    /// Network (default testnet).
    pub fn network(mut self, network: Network) -> Self {
        self.req.network.network = network;
        self
    }

    /// Overrides the profile's API URL.
    pub fn api_url(mut self, url: impl Into<String>) -> Self {
        self.req.network.api_url = Some(url.into());
        self
    }

    /// Overrides the profile's data host URL.
    pub fn data_url(mut self, url: impl Into<String>) -> Self {
        self.req.network.data_url = Some(url.into());
        self
    }

    /// Market type (default UpDown).
    pub fn market_type(mut self, t: MarketType) -> Self {
        self.req.market_type = t;
        self
    }

    /// Underlying, e.g. `BTC`.
    pub fn instrument(mut self, instrument: impl Into<String>) -> Self {
        self.req.instrument = Some(instrument.into());
        self
    }

    /// Adds a series.
    pub fn period(mut self, p: Period) -> Self {
        self.req.periods.push(p);
        self
    }

    /// Adds several series.
    pub fn periods(mut self, ps: impl IntoIterator<Item = Period>) -> Self {
        self.req.periods.extend(ps);
        self
    }

    /// Adds a market by address.
    pub fn market(mut self, address: impl Into<String>) -> Self {
        self.req.markets.push(address.into());
        self
    }

    /// Adds markets by address.
    pub fn markets<S: Into<String>>(mut self, addresses: impl IntoIterator<Item = S>) -> Self {
        self.req
            .markets
            .extend(addresses.into_iter().map(Into::into));
        self
    }

    /// Range `[from, to)`, UTC.
    pub fn range(mut self, from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        self.req.from = Some(from);
        self.req.to = Some(to);
        self
    }

    /// Range start, inclusive.
    pub fn from(mut self, from: DateTime<Utc>) -> Self {
        self.req.from = Some(from);
        self
    }

    /// Range end, exclusive.
    pub fn to(mut self, to: DateTime<Utc>) -> Self {
        self.req.to = Some(to);
        self
    }

    /// Whole UTC days, `to` inclusive.
    pub fn days(self, from: NaiveDate, to: NaiveDate) -> Self {
        let start = from.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
        let end = (to + Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .expect("midnight")
            .and_utc();
        self.range(start, end)
    }

    /// Which data to load.
    pub fn data(mut self, data: DataKinds) -> Self {
        self.req.data = data;
        self
    }

    /// Only markets that have a fills file.
    pub fn fills_only(mut self, on: bool) -> Self {
        self.req.fills_only = on;
        self
    }

    /// Checks and returns the request.
    pub fn build(self) -> Result<Request> {
        let d = self.req;
        Request {
            network: d.network.network,
            api_url: d.network.api_url,
            data_url: d.network.data_url,
            market_type: d.market_type,
            instrument: d.instrument,
            periods: d.periods,
            markets: d.markets,
            from: d.from,
            to: d.to,
            data: d.data,
            fills_only: d.fills_only,
        }
        .validate()
    }
}

/// Parses `2026-10-27`, `2026-10-27T12:00:00Z` or unix seconds.
pub fn parse_time(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).expect("midnight").and_utc());
    }
    if let Ok(t) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(t.and_utc());
    }
    if let Ok(secs) = s.parse::<i64>()
        && let Some(t) = DateTime::from_timestamp(secs, 0)
    {
        return Ok(t);
    }
    Err(Error::InvalidRequest(format!(
        "time {s:?}: use 2026-10-27, 2026-10-27T12:00:00Z or unix seconds"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "0x72037B5314f64f03e899160a22c6097da6403f4d42f3684a9ac544ab944a7ae2";

    fn t(s: &str) -> DateTime<Utc> {
        parse_time(s).unwrap()
    }

    #[test]
    fn builds_and_normalizes() {
        let r = Request::builder()
            .instrument("btc")
            .periods([Period::H1, Period::M5, Period::H1])
            .range(t("2026-10-20"), t("2026-10-21"))
            .data(DataKinds::ALL)
            .build()
            .unwrap();
        assert_eq!(r.instrument.as_deref(), Some("BTC"));
        assert_eq!(r.periods, vec![Period::M5, Period::H1]);
    }

    #[test]
    fn rejects_bad_requests() {
        assert!(Request::builder().build().is_err(), "no range, no markets");
        assert!(
            Request::builder()
                .range(t("2026-10-21"), t("2026-10-20"))
                .build()
                .is_err()
        );
        assert!(Request::builder().market("0x1234").build().is_err());
        assert!(
            Request::builder()
                .market(A)
                .period(Period::M5)
                .build()
                .is_err()
        );
        assert!(
            Request::builder()
                .instrument("BTC/USD")
                .range(t("2026-10-20"), t("2026-10-21"))
                .build()
                .is_err()
        );
    }

    #[test]
    fn markets_without_range() {
        let r = Request::builder()
            .markets([A, &A.to_ascii_lowercase()])
            .build()
            .unwrap();
        assert_eq!(r.markets, vec![A.to_ascii_lowercase()]);
    }

    #[test]
    fn json_roundtrip() {
        let r = Request::builder()
            .api_url("http://localhost:3000")
            .instrument("BTC")
            .period(Period::D1)
            .days("2026-10-01".parse().unwrap(), "2026-10-07".parse().unwrap())
            .data(DataKinds {
                book: true,
                fills: false,
                trades: false,
                prices: true,
            })
            .build()
            .unwrap();
        let json = r.to_json();
        assert!(json.contains("\"1d\""), "{json}");
        assert!(json.contains("\"network\": \"testnet\""), "{json}");
        assert_eq!(Request::from_json(&json).unwrap(), r);
        assert_eq!(r.to, Some(t("2026-10-08")));
    }

    #[test]
    fn json_minimal_and_typos() {
        let r = Request::from_json(
            r#"{"network":"testnet","periods":["15m","Daily"],"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z","data":{"trades":true}}"#,
        )
        .unwrap();
        assert_eq!(r.periods, vec![Period::M15, Period::D1]);
        assert!(r.data.trades && !r.data.book && !r.data.fills);
        assert!(Request::from_json(r#"{"perods":["5m"],"markets":[]}"#).is_err());
    }

    #[test]
    fn default_kinds_leave_out_trades() {
        let d = DataKinds::default();
        assert!(d.book && d.fills && d.prices && !d.trades);
        let r = Request::builder().market(A).build().unwrap();
        assert_eq!(r.data, DataKinds::DEFAULT);
        let r = Request::from_json(&format!(r#"{{"markets":["{A}"]}}"#)).unwrap();
        assert_eq!(r.data, DataKinds::DEFAULT, "no data field");
        assert!(!r.fills_only);
        let r = Request::builder()
            .market(A)
            .fills_only(true)
            .build()
            .unwrap();
        assert!(r.to_json().contains("fills_only"));
        assert_eq!(Request::from_json(&r.to_json()).unwrap(), r);
    }

    #[test]
    fn overlap() {
        let r = Request::builder()
            .range(t("2026-10-20T10:00:00Z"), t("2026-10-20T11:00:00Z"))
            .build()
            .unwrap();
        assert!(r.overlaps(t("2026-10-20T09:55:00Z"), t("2026-10-20T10:00:01Z")));
        assert!(
            !r.overlaps(t("2026-10-20T09:55:00Z"), t("2026-10-20T10:00:00Z")),
            "ends at from"
        );
        assert!(r.overlaps(t("2026-10-20T10:59:59Z"), t("2026-10-20T11:04:59Z")));
        assert!(
            !r.overlaps(t("2026-10-20T11:00:00Z"), t("2026-10-20T11:05:00Z")),
            "starts at to"
        );
        assert!(
            r.overlaps(t("2026-10-20T00:00:00Z"), t("2026-10-21T00:00:00Z")),
            "covers"
        );
    }

    #[test]
    fn parses_inputs() {
        assert_eq!("Daily".parse::<Period>().unwrap(), Period::D1);
        assert_eq!(
            "book,prices".parse::<DataKinds>().unwrap(),
            DataKinds {
                book: true,
                fills: false,
                trades: false,
                prices: true
            }
        );
        assert!("fills,trades".parse::<DataKinds>().unwrap().fills);
        assert!("bok".parse::<DataKinds>().is_err());
        assert_eq!(t("1761523200"), t("2025-10-27"));
    }
}
