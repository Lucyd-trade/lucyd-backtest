//! The README example: one day of 15m and 1h BTC markets.

use lucyd_backtest::{DataKinds, EventKind, Loader, Period, Request, parse_time};

#[tokio::main]
async fn main() -> lucyd_backtest::Result<()> {
    let req = Request::builder()
        .instrument("BTC")
        .periods([Period::M15, Period::H1])
        .range(parse_time("2026-10-26")?, parse_time("2026-10-27")?)
        .data(DataKinds::DEFAULT) // book, fills, prices; add trades for on-chain fills
        .build()?;

    let data = Loader::new()?.load(&req).await?;
    for w in &data.warnings {
        eprintln!("warning: {w}");
    }

    for e in data.events() {
        match &e.kind {
            EventKind::Fill { fill } => {
                println!(
                    "{} fill {} @ {} ({:?})",
                    e.ts_ms, fill.quantity, fill.price, fill.settlement
                )
            }
            EventKind::MarketClose { winner, .. } => {
                println!("{} close, winner {winner:?}", e.ts_ms)
            }
            _ => {}
        }
    }

    // Full book of a market at any moment
    let Some(m) = data.markets.first() else {
        return Ok(());
    };
    let t = m.info.start + lucyd_backtest::chrono::Duration::minutes(5);
    if let Some(book) = data.book_at(&m.info.address, t) {
        println!(
            "up: bid {:?} ask {:?}",
            book.outcome(0).unwrap().best_bid(),
            book.outcome(0).unwrap().best_ask()
        );
    }
    Ok(())
}
