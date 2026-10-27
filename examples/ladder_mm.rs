//! Ladder market maker over a few hours of 5m BTC markets: `cargo run --example ladder_mm`.

#[path = "strategies/ladder_mm.rs"]
mod ladder_mm;

use lucyd_backtest::engine::{ExecParams, QueueModel, run};
use lucyd_backtest::{Loader, Period, Request, parse_time};

#[tokio::main]
async fn main() -> lucyd_backtest::Result<()> {
    let req = Request::builder()
        .instrument("BTC")
        .periods([Period::M5])
        .range(
            parse_time("2026-10-25T08:00:00Z")?,
            parse_time("2026-10-25T14:00:00Z")?,
        )
        .build()?;
    let data = Loader::new()?.load(&req).await?;

    let params = ExecParams {
        queue_model: QueueModel::Proportional,
        latency_jitter_ms: 50,
        seed: 1,
        ..ExecParams::default()
    };
    let mut strategy = ladder_mm::LadderMm::new(ladder_mm::LadderMmConfig::default());
    let report = run(&data, &params, &mut strategy)?;

    let s = &report.summary;
    println!(
        "P&L {:.2} USDC, {} fills, limit fill rate {:?}, turnover {:.2}",
        s.pnl, s.fills, s.limit_fill_rate, s.turnover
    );
    for w in &report.warnings {
        eprintln!("warning: {w}");
    }
    Ok(())
}
