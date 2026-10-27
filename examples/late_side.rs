//! Late side strategy over one day of 15m and 1h BTC markets: `cargo run --example late_side`.
//! It needs Pyth prices, recorded from 2026-10-27.

#[path = "strategies/late_side.rs"]
mod late_side;

use lucyd_backtest::engine::{ExecParams, run};
use lucyd_backtest::{Loader, Period, Request, parse_time};

#[tokio::main]
async fn main() -> lucyd_backtest::Result<()> {
    let req = Request::builder()
        .instrument("BTC")
        .periods([Period::M15, Period::H1])
        .range(parse_time("2026-10-27")?, parse_time("2026-10-28")?)
        .build()?;
    let data = Loader::new()?.load(&req).await?;

    let mut strategy = late_side::LateSide::new(late_side::LateSideConfig::default());
    let report = run(&data, &ExecParams::default(), &mut strategy)?;

    let s = &report.summary;
    println!(
        "P&L {:.2} USDC over {} market(s), win rate {:?}, max drawdown {:.2}",
        s.pnl, s.markets_traded, s.win_rate, s.max_drawdown
    );
    for w in &report.warnings {
        eprintln!("warning: {w}");
    }
    report.write_json(std::path::Path::new("late_side_report.json"))
}
