//! Pyth price history: one file per feed and UTC day.

use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::pq;

/// First day with price files on the data host.
pub const FIRST_PRICE_DAY: NaiveDate = match NaiveDate::from_ymd_opt(2026, 10, 27) {
    Some(d) => d,
    None => panic!("valid date"),
};

/// One Pyth update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceTick {
    /// Feed folder name, e.g. `btc-usd`.
    pub feed: String,
    /// Price time by Pyth (whole seconds), ms.
    pub publish_ms: i64,
    /// When the recorder received it, ms. Events use this time.
    pub receive_ms: i64,
    /// USD, exponent applied.
    pub price: f64,
    /// Confidence interval, USD.
    pub conf: f64,
    /// Pythnet slot, when recorded.
    pub slot: Option<u64>,
}

pub(crate) fn read_ticks(feed: &str, file: Bytes) -> Result<Vec<PriceTick>> {
    let what = "price file";
    let mut out = Vec::new();
    let columns = ["publish_time", "receive_time", "price", "conf", "slot"];
    for b in pq::read_columns(file, what, &columns)? {
        let publish = pq::ts(&b, "publish_time", what)?;
        let receive = pq::ts(&b, "receive_time", what)?;
        let price = pq::f64s(&b, "price", what)?;
        let conf = pq::f64s(&b, "conf", what)?;
        let slot = if b.column_by_name("slot").is_some() {
            pq::u64s(&b, "slot", what)?
        } else {
            vec![None; b.num_rows()]
        };
        for i in 0..b.num_rows() {
            let (Some(price), Some(conf)) = (price[i], conf[i]) else {
                continue;
            };
            out.push(PriceTick {
                feed: feed.to_string(),
                publish_ms: publish[i],
                receive_ms: receive[i],
                price,
                conf,
                slot: slot[i],
            });
        }
    }
    if !out.is_sorted_by_key(|t| (t.receive_ms, t.publish_ms)) {
        out.sort_by_key(|t| (t.receive_ms, t.publish_ms));
    }
    Ok(out)
}

/// UTC days touched by `[from, to)`.
pub(crate) fn days(from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    if from >= to {
        return out;
    }
    let last = (to - Duration::milliseconds(1)).date_naive();
    let mut d = from.date_naive();
    while d <= last {
        out.push(d);
        d += Duration::days(1);
    }
    out
}

/// A day's file appears shortly after the day ends; until then its absence is expected.
pub(crate) fn day_published(day: NaiveDate, now: DateTime<Utc>) -> bool {
    let end = (day + Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .expect("midnight")
        .and_utc();
    now >= end + Duration::minutes(10)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::parse_time;

    #[test]
    fn days_of_range() {
        let d = days(
            parse_time("2026-10-27T23:00:00Z").unwrap(),
            parse_time("2026-10-29").unwrap(),
        );
        assert_eq!(d.len(), 2, "end exclusive: the 29th is not touched");
        assert_eq!(d[0].to_string(), "2026-10-27");
        assert!(
            days(
                parse_time("2026-10-29").unwrap(),
                parse_time("2026-10-29").unwrap()
            )
            .is_empty()
        );
    }

    #[test]
    fn publication_time() {
        let day = "2026-10-27".parse().unwrap();
        assert!(!day_published(
            day,
            parse_time("2026-10-28T00:05:00Z").unwrap()
        ));
        assert!(day_published(
            day,
            parse_time("2026-10-28T00:10:00Z").unwrap()
        ));
    }
}
