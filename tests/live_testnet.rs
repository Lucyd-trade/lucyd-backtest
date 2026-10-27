//! Against the live testnet. Off by default: `cargo test -- --ignored`.

use chrono::{Duration, Utc};
use lucyd_backtest::{DataKinds, EventKind, Loader, Period, Request};

#[tokio::test]
#[ignore = "needs network access to testnet"]
async fn loads_a_recent_hour() {
    // Two days back: book files of these markets are surely uploaded.
    let to = (Utc::now() - Duration::days(2))
        .date_naive()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc();
    let from = to - Duration::hours(1);
    let req = Request::builder()
        .instrument("BTC")
        .periods([Period::M5, Period::M15])
        .range(from, to)
        .data(DataKinds::ALL)
        .build()
        .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let loader = Loader::with_cache_dir(cache.path()).unwrap();

    let ds = loader.load(&req).await.unwrap();
    assert_eq!(ds.markets.len(), 12 + 4, "5m and 15m markets of one hour");
    for m in &ds.markets {
        assert!(
            m.info.state.is_final(),
            "{} is {:?}",
            m.info.address,
            m.info.state
        );
        assert!(m.info.strike.is_some_and(|s| s > 1000.0));
    }

    let events = ds.events();
    assert!(
        events
            .windows(2)
            .all(|w| w[0].ts_ms <= w[1].ts_ms && w[0].seq + 1 == w[1].seq)
    );
    let opens = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::MarketOpen { .. }))
        .count();
    assert_eq!(opens, 16);

    // A market with a book has a full book somewhere in the middle of its life.
    if let Some(m) = ds.markets.iter().find(|m| m.book.is_some()) {
        let mid = m.info.start + (m.info.end - m.info.start) / 2;
        let book = ds.book_at(&m.info.address, mid);
        assert!(book.is_some(), "book of {} at {mid}", m.info.address);
    }

    // Second run comes from the cache and gives the same result.
    let again = loader.load(&req).await.unwrap();
    assert_eq!(again.events(), events);
}

#[tokio::test]
#[ignore = "needs network access to testnet"]
async fn unknown_instrument_is_an_error() {
    let to = Utc::now() - Duration::days(1);
    let req = Request::builder()
        .instrument("NOPE")
        .range(to - Duration::hours(1), to)
        .build()
        .unwrap();
    let err = Loader::with_cache_dir(tempfile::tempdir().unwrap().path())
        .unwrap()
        .catalog(&req)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("BTC"), "{err}");
}
