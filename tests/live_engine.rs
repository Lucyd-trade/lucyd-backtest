//! The example strategies against the live testnet. Off by default: `cargo test -- --ignored`.

#[path = "../examples/strategies/ladder_mm.rs"]
mod ladder_mm;
#[path = "../examples/strategies/late_side.rs"]
mod late_side;

use chrono::{Duration, Utc};
use lucyd_backtest::engine::{ExecParams, Report, Strategy, run};
use lucyd_backtest::{Dataset, Loader, Period, Request};

async fn recent_data() -> Dataset {
    // Two days back: book, fills and prices of these markets are surely uploaded.
    let to = (Utc::now() - Duration::days(2))
        .date_naive()
        .and_hms_opt(16, 0, 0)
        .unwrap()
        .and_utc();
    let req = Request::builder()
        .instrument("BTC")
        .periods([Period::M5, Period::M15])
        .range(to - Duration::hours(4), to)
        .build()
        .unwrap();
    let cache = tempfile::tempdir().unwrap();
    Loader::with_cache_dir(cache.path())
        .unwrap()
        .load(&req)
        .await
        .unwrap()
}

fn check(data: &Dataset, make: impl Fn() -> Box<dyn Strategy>) -> Report {
    let params = ExecParams {
        latency_jitter_ms: 50,
        seed: 3,
        ..ExecParams::default()
    };
    let r = run(data, &params, make().as_mut()).unwrap();
    assert_eq!(r.markets.len(), data.markets.len());
    assert!(r.summary.end_equity.is_finite() && r.summary.end_equity >= 0.0);
    let fills = r.journal.iter().filter(|j| j.event == "fill").count() as u32;
    assert_eq!(fills, r.summary.fills + r.summary.reverted_fills);
    let pnl: f64 = r.markets.iter().map(|m| m.pnl).sum();
    assert!((pnl - r.summary.pnl).abs() < 1e-6);
    assert!(
        (r.summary.end_equity - r.summary.start_equity - r.summary.pnl).abs() < 1e-6,
        "equity moves only with market P&L"
    );
    let again = run(data, &params, make().as_mut()).unwrap();
    assert_eq!(again.to_json(), r.to_json(), "deterministic");
    r
}

#[tokio::test]
#[ignore = "needs network access to testnet"]
async fn example_strategies_on_recent_markets() {
    let data = recent_data().await;
    assert!(!data.markets.is_empty());

    let mm = check(&data, || {
        Box::new(ladder_mm::LadderMm::new(
            ladder_mm::LadderMmConfig::default(),
        ))
    });
    assert!(mm.summary.limit_orders > 0, "the market maker quotes");

    let late = check(&data, || {
        Box::new(late_side::LateSide::new(
            late_side::LateSideConfig::default(),
        ))
    });
    if !data.prices.is_empty() {
        assert!(late.summary.orders > 0, "with prices it buys near the end");
    }
}
