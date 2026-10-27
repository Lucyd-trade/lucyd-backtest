//! Market catalog: what `/markets` returns, converted to plain numbers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tiny_keccak::{Hasher, Keccak};

use crate::error::{Result, bad};
use crate::request::{MarketType, Period};

/// Fixed-point scale of `strike` and `resolved_value` in the API.
pub const PRICE_SCALE: f64 = 1e8;

/// Market lifecycle as the API reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarketState {
    Upcoming,
    Open,
    Pending,
    Settled,
    Voided,
}

impl MarketState {
    /// Settled or voided: nothing about the market changes any more.
    pub fn is_final(self) -> bool {
        matches!(self, MarketState::Settled | MarketState::Voided)
    }
}

/// One market of the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketInfo {
    /// Market id: `0x` + 64 hex, lowercase. Same as the CLOB market id.
    pub address: String,
    pub market_type: MarketType,
    pub period: Period,
    pub instrument: String,
    /// Pyth feed id the market settles on, `0x…`.
    pub feed_id: Option<String>,
    /// Feed name as Pyth lists it, e.g. `BTC/USD`.
    pub feed_name: Option<String>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub state: MarketState,
    /// Price at open, USD (API value / 1e8). None until the market opens.
    pub strike: Option<f64>,
    /// Oracle price at expiry, USD (API value / 1e8). None until settled.
    pub resolved_price: Option<f64>,
    pub resolved_at: Option<DateTime<Utc>>,
    /// Index of the winning outcome: 0 = up, 1 = down. None if not settled or voided.
    pub winner: Option<u8>,
    /// Void: every outcome token pays 0.5.
    pub voided: bool,
    /// Outcome labels, index = outcome index: `["up", "down"]`.
    pub outcomes: Vec<String>,
    /// Outcome token ids (ERC-1155 / `asset_id` in the book files), index = outcome index.
    pub outcome_ids: Vec<String>,
    /// Total traded notional, USD, at load time.
    pub volume_usd: f64,
    /// Whether the data host has a fills file of the market. None = not checked (fills neither
    /// requested nor filtered on). False: no match, not over yet, or older than the fills history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_fills: Option<bool>,
}

impl MarketInfo {
    /// Outcome index of a token id, if it belongs to this market.
    pub fn outcome_of(&self, asset_id: &str) -> Option<u8> {
        let a = asset_id.to_ascii_lowercase();
        self.outcome_ids
            .iter()
            .position(|id| *id == a)
            .map(|i| i as u8)
    }

    /// Folder of the book file on the data host, without the `books/` prefix.
    pub fn book_path(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}.parquet",
            self.market_type.path_segment(),
            self.period.path_segment(),
            path_segment(&self.instrument),
            self.start.format("%Y-%m-%d"),
            self.address
        )
    }

    /// Path of the fills file on the data host, without the `fills/` prefix: the same as the
    /// book file's.
    pub fn fills_path(&self) -> String {
        self.book_path()
    }

    /// Price file folder name of the feed, e.g. `btc-usd`.
    pub fn feed_slug(&self) -> Option<String> {
        self.feed_name.as_deref().map(path_segment)
    }
}

/// Lowercase path segment as the data host names folders: anything but `[a-z0-9]` becomes `-`.
pub fn path_segment(s: &str) -> String {
    let out: String = s
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .collect();
    if out.is_empty() {
        "unknown".into()
    } else {
        out
    }
}

/// Outcome token id: keccak256(market id ++ uint8(index)).
pub fn outcome_id(market: &str, index: u8) -> Result<String> {
    let bytes = hex::decode(market.trim_start_matches("0x"))
        .map_err(|e| bad("market address", format!("{market}: {e}")))?;
    let mut k = Keccak::v256();
    k.update(&bytes);
    k.update(&[index]);
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    Ok(format!("0x{}", hex::encode(out)))
}

/// `/markets` item, only the fields used here.
#[derive(Debug, Deserialize)]
pub(crate) struct ApiMarket {
    address: String,
    instrument: Option<String>,
    market_type: String,
    period: String,
    state: MarketState,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
    winner: Option<String>,
    #[serde(default)]
    volume_traded: f64,
    strike: Option<i64>,
    resolved_value: Option<i64>,
    #[serde(default)]
    outcomes: Vec<String>,
    oracle: Option<ApiOracle>,
}

#[derive(Debug, Deserialize)]
struct ApiOracle {
    #[serde(default)]
    feeds: Vec<ApiFeed>,
}

#[derive(Debug, Deserialize)]
struct ApiFeed {
    id: String,
    name: Option<String>,
}

impl ApiMarket {
    /// None for market types or periods this version does not know.
    pub(crate) fn into_info(self) -> Result<Option<MarketInfo>> {
        let Ok(market_type) = self.market_type.parse::<MarketType>() else {
            return Ok(None);
        };
        let Ok(period) = self.period.parse::<Period>() else {
            return Ok(None);
        };
        let address = self.address.to_ascii_lowercase();
        let outcomes = if self.outcomes.is_empty() {
            vec!["up".to_string(), "down".to_string()]
        } else {
            self.outcomes
        };
        let outcome_ids = (0..outcomes.len() as u8)
            .map(|i| outcome_id(&address, i))
            .collect::<Result<Vec<_>>>()?;
        let winner = self.winner.as_deref().and_then(|w| {
            outcomes
                .iter()
                .position(|o| o.eq_ignore_ascii_case(w))
                .map(|i| i as u8)
        });
        let feed = self.oracle.and_then(|o| o.feeds.into_iter().next());
        Ok(Some(MarketInfo {
            address,
            market_type,
            period,
            instrument: self.instrument.unwrap_or_default().to_ascii_uppercase(),
            feed_id: feed.as_ref().map(|f| f.id.to_ascii_lowercase()),
            feed_name: feed.and_then(|f| f.name),
            start: self.start_date,
            end: self.end_date,
            voided: self.state == MarketState::Voided,
            state: self.state,
            strike: self.strike.map(|v| v as f64 / PRICE_SCALE),
            resolved_price: self.resolved_value.map(|v| v as f64 / PRICE_SCALE),
            resolved_at: self.resolved_at,
            winner,
            outcomes,
            outcome_ids,
            volume_usd: self.volume_traded,
            has_fills: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JSON: &str = r#"{"type":"market","address":"0x72037B5314f64f03e899160a22c6097da6403f4d42f3684a9ac544ab944a7ae2","instrument":"BTC","market_type":"UpDown","period":"Daily","start_date":"2026-10-27T00:00:00Z","end_date":"2026-10-28T00:00:00Z","resolved_at":"2026-10-28T00:01:00Z","outcomes":["up","down"],"state":"settled","winner":"down","volume_traded":12.5,"strike":8039586891868,"resolved_value":8038365090393,"oracle":{"provider":"pyth","feeds":[{"id":"0xE62D","name":"BTC/USD","url":null}]},"title":"t","category":"Crypto"}"#;

    #[test]
    fn converts_api_market() {
        let m: ApiMarket = serde_json::from_str(JSON).unwrap();
        let m = m.into_info().unwrap().unwrap();
        assert_eq!(m.period, Period::D1);
        assert_eq!(m.strike, Some(80395.86891868));
        assert_eq!(m.winner, Some(1));
        assert!(!m.voided);
        assert_eq!(m.feed_slug().as_deref(), Some("btc-usd"));
        assert_eq!(m.feed_id.as_deref(), Some("0xe62d"));
        assert!(
            m.book_path()
                .starts_with("updown/daily/btc/2026-10-27/0x72037b53")
        );
        assert_eq!(
            m.outcome_of(&m.outcome_ids[1].to_ascii_uppercase()),
            Some(1)
        );
    }

    #[test]
    fn outcome_id_is_keccak_of_market_and_index() {
        // keccak256(32 zero bytes ++ 0x00)
        let id = outcome_id(&format!("0x{}", "0".repeat(64)), 0).unwrap();
        assert_eq!(id.len(), 66);
        assert_ne!(id, outcome_id(&format!("0x{}", "0".repeat(64)), 1).unwrap());
    }
}
