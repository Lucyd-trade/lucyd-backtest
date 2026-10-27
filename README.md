# lucyd-backtest

Backtesting toolkit for [Lucyd](https://testnet.lucyd.trade) prediction markets. This first part is the data loader: pick markets by series and time range, download their L2 order book history, order book fills, on-chain trades and Pyth prices, and get them as one time-ordered event stream, with the full order book at any moment. The execution engine and MCP tools for AI agents will be built on top of it.

> Lucyd currently runs on the Ethereum Sepolia testnet with test USDC. Most of the testnet liquidity comes from the platform's own market maker, plus a small stream of test trades, so books and fills are not those of a live market with independent traders. Use the data to build and test strategies and tooling, not to estimate real-money returns.

## What the markets are

Every market asks whether BTC will be higher at the end of its period than at the start. The price at the start is the strike; the market settles on the Pyth BTC/USD price at the end. Two outcome tokens, up and down, trade between 0.0025 and 0.9975 USDC in steps of 0.0025; the winning token pays 1 USDC. A market that gets no oracle price in time is voided and each token pays 0.5. Series: 5 min, 15 min, 30 min, 1 h, 4 h and 1 day, back to back.

## Install

With [Rust](https://rustup.rs) 1.88 or newer:

```bash
cargo install --git https://github.com/lucyd-trade/lucyd-backtest
```

As a library:

```toml
[dependencies]
lucyd-backtest = { git = "https://github.com/lucyd-trade/lucyd-backtest" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Use from code

```rust
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
            EventKind::Fill { fill } => println!("{} fill {} @ {} ({:?})", e.ts_ms, fill.quantity, fill.price, fill.settlement),
            EventKind::MarketClose { winner, .. } => println!("{} close, winner {winner:?}", e.ts_ms),
            _ => {}
        }
    }

    // Full book of a market at any moment
    let Some(m) = data.markets.first() else { return Ok(()) };
    let t = m.info.start + lucyd_backtest::chrono::Duration::minutes(5);
    if let Some(book) = data.book_at(&m.info.address, t) {
        println!("up: bid {:?} ask {:?}", book.outcome(0).unwrap().best_bid(), book.outcome(0).unwrap().best_ask());
    }
    Ok(())
}
```

`Loader::catalog` returns only the market list, without downloading data. The same program is in [examples/basic.rs](examples/basic.rs): `cargo run --example basic`.

## Use from the command line

```bash
# which markets a request selects
lucyd-backtest list --instrument BTC --period 15m,1h --from 2026-10-27 --to 2026-10-28

# download into the cache
lucyd-backtest fetch --period 5m --from 2026-10-27T10:00:00Z --to 2026-10-27T12:00:00Z --data book,fills

# only markets that have fills, with on-chain trades as well
lucyd-backtest export --period 15m --from 2026-10-25 --to 2026-10-27 --fills-only --data all -o events.parquet

# write the event stream to Parquet or CSV
lucyd-backtest export --period 1h --from 2026-10-27 --to 2026-10-28 -o events.parquet

# the same request as JSON
lucyd-backtest export --request request.json -o events.csv
```

`list` has a `fills` column (`yes` / `no`; `-` when fills are not asked for, e.g. `--data book`) and the count of markets with fills in the total line. `list --json` prints the catalog as JSON. Warnings (missing files, days without prices) go to stderr.

## Request

One structure, built with `Request::builder()` or read from JSON (`Request::from_json`, the `--request` flag). The same JSON is what agents will send through MCP later.

```json
{
  "network": "testnet",
  "market_type": "UpDown",
  "instrument": "BTC",
  "periods": ["15m", "1h"],
  "from": "2026-10-27T00:00:00Z",
  "to": "2026-10-28T00:00:00Z",
  "data": { "book": true, "fills": true, "trades": false, "prices": true },
  "fills_only": false
}
```

| Field | CLI flag | |
|---|---|---|
| `network` | `--network` | `testnet` (default) or `mainnet`. Mainnet is not launched yet and fails with a clear error unless both URLs are given. |
| `api_url`, `data_url` | `--api-url`, `--data-url` | Own URLs instead of the network's; either one or both. |
| `market_type` | `--market-type` | `UpDown`, the only type so far. |
| `instrument` | `--instrument` | Underlying asset as the API names it: `BTC`. Omit for any. An instrument the API does not list is an error naming the ones it has. |
| `periods` | `--period` | `5m`, `15m`, `30m`, `1h`, `4h`, `1d`; one or several. Omit for all. |
| `markets` | `--market` | Market addresses (`0x` + 64 hex), one or several. An alternative to `instrument` and `periods`. |
| `from`, `to` | `--from`, `--to` | UTC range `[from, to)`: `2026-10-27`, `2026-10-27T12:00:00Z` or unix seconds. A market is selected if it overlaps the range; events outside it are left out. Required unless `markets` is given. |
| `data` | `--data` | Any combination of `book`, `fills`, `trades`, `prices`, or `all`. Default `book,fills,prices`: on-chain trades are loaded only when asked for. Inside a JSON `data` object a missing field is false. |
| `fills_only` | `--fills-only` | Leave out markets without a fills file. The catalog counts them (`without_fills_excluded`) and says so in `warnings`. |

The CLI flag `--cache-dir` (or env `LUCYD_BACKTEST_CACHE`) moves the cache; in code use `Loader::with_cache_dir`.

## What you get

- **Catalog**: per market the address, type, period, instrument, Pyth feed id, start and end, strike, final price, winner and whether it was voided. Strike and prices are in USD (the API's 8-decimal values divided by 1e8).
- **Per market data**: the book history (snapshots and deltas), fills in match order, on-chain trades in chain order; plus Pyth ticks per feed. The catalog marks each market `has_fills` when fills are asked for.
- **Event stream**: `market_open`, `price_tick`, `book_snapshot`, `book_delta`, `fill`, `trade`, `market_close`, sorted by time with a fixed order for equal times. This is the input format of the coming engine.
- **Book at any moment**: `Dataset::book_at(market, time)` or `BookHistory::at(ms)` rebuilds both outcomes' bids and asks.

Full description of sources, fields, event order and timing caveats: [docs/data-format.md](docs/data-format.md).

## Fills or trades

Two separate sources of executions; load either or both (`data`), they are never merged or deduplicated.

| | `fills` (default) | `trades` |
|---|---|---|
| Source | order book's own record of every match (`data.testnet.lucyd.trade/fills/…`) | fills read from the exchange contract (`GET /trades`) |
| Time | the order book clock, in milliseconds, on the same clock as the book files | the block that settled the fill: up to ~12 s after the match, whole seconds |
| What is in it | every match, with its settlement: `pending`, `confirmed` or `failed`. A failed one still took volume off the book | only fills that settled on chain |
| History | from about 2026-10-20 (markets that ended after the fills recorder started) | from 2026-10-07, all markets |
| Use for | execution backtests and queue models: fills line up with the book deltas | checking against the chain, and periods before the fills history |

An engine can take either: `Fill` and `Trade` name their shared fields the same (`trade_id`, `market`, `taker`, `side`, `outcome_index`, `price`, `quantity`, `amount_usd`, `match_type`, `tx_hash`, `block_number`) and both implement the `Execution` trait.

## Data sources

| Data | Source | Since |
|---|---|---|
| Markets | public API `GET /markets` | all markets |
| Fills | `data.testnet.lucyd.trade/fills/…`, one Parquet file per market, from the order book's order journal | about 2026-10-20 |
| Trades | public API `GET /trades` (fills read from the exchange contract) | 2026-10-07 (current contracts) |
| Order book | `data.testnet.lucyd.trade/books/…`, one Parquet file per market, recorded from the public order book feed | early October 2026 |
| Pyth prices | `data.testnet.lucyd.trade/prices/pyth/…`, one Parquet file per feed and UTC day | 2026-10-27 |

The order book and fills files of a market appear after the market ends; a market without a single match has no fills file. A day of prices appears shortly after the UTC day ends. Missing data is reported in `warnings`, not as an error. The API allows 20 requests per second; the loader stays at 8 and retries rate limits and server errors.

## Cache

Downloads are kept in the system cache folder (`lucyd-backtest`). Finished markets and closed days are never downloaded twice; trades of markets still running are topped up on the next run. Files are written through a temporary name and renamed, so an interrupted run never corrupts the cache.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test -- --ignored   # also against the live testnet
```

## License

MIT
