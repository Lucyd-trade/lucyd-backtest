# lucyd-backtest

Backtesting toolkit for [Lucyd](https://testnet.lucyd.trade) prediction markets. Two parts: a data loader (pick markets by series and time range, download their L2 order book history, order book fills, on-chain trades and Pyth prices, and get them as one time-ordered event stream, with the full order book at any moment) and an execution engine that runs a strategy over that data with latency, slippage and queue position, keeps the account and reports P&L. MCP tools for AI agents will be built on top of it.

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

## Your own strategy in 5 minutes

```bash
cargo install --git https://github.com/lucyd-trade/lucyd-backtest
lucyd-backtest new my-strategy
cd my-strategy
cargo run --release
```

`new` creates a Cargo project with one strategy in `src/main.rs`: a working example (near the end of each market, buy the side BTC is on if it is not too expensive) with comments on every handler, how to read the book and prices, how to place and cancel orders, and where the request and execution parameters are set. `cargo run` downloads the data (cached after the first time), runs the backtest, prints the summary and writes `reports/report.json` and `reports/report.html`. Open the HTML in a browser, change the strategy, run again.

The engine comes in as a git dependency for now; once the crate is on crates.io, the one line in the project's `Cargo.toml` that says so switches it to `lucyd-backtest = "0.1"`. `--engine-path <dir>` points the project at a local checkout instead.

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

## Backtest a strategy

A strategy is a Rust type implementing `engine::Strategy`. Every handler has an empty default; implement the ones you need. Each handler gets a context (`Ctx`) with the state as of now (nothing later) and places orders through it.

```rust
use lucyd_backtest::Side;
use lucyd_backtest::engine::{Ctx, ExecParams, OrderUpdate, Strategy, Subscriptions, run};

struct BuyCheapUp;

impl Strategy for BuyCheapUp {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions { timer_ms: Some(1000), ..Subscriptions::default() }
    }

    fn on_timer(&mut self, ctx: &mut Ctx) {
        for m in ctx.open_markets() {
            let cheap = ctx.best_ask(&m.address, 0).is_some_and(|a| a.price <= 0.3);
            if cheap && ctx.position(&m.address, 0) == 0.0 && ctx.open_orders(Some(&m.address)).is_empty() {
                let _ = ctx.market_order(&m.address, 0, Side::Buy, 10, Some(0.3));
            }
        }
    }

    fn on_order(&mut self, _ctx: &mut Ctx, u: &OrderUpdate) {
        println!("{} {:?}", u.order_id, u.kind);
    }
}

// `data` is a Dataset from the loader, as above
let report = run(&data, &ExecParams::default(), &mut BuyCheapUp)?;
println!("P&L {:.2}", report.summary.pnl);
report.write_json(std::path::Path::new("report.json"))?;
```

**Handlers**: `on_start`, `on_market_open`, `on_book` (book changed), `on_fill` (a recorded fill of other participants), `on_price` (Pyth tick), `on_timer`, `on_market_close` (trading ended, result unknown), `on_resolution` (result on chain, positions paid out), `on_order` (own order accepted, filled partly or fully, cancelled, rejected with a reason, settlement failed), `on_finish`. `subscriptions()` picks what to receive: the book on every change, at most once per N ms (`BookFeed::Throttle(N)`) or not at all; fills; prices; a timer every N ms.

**Context**: `now_ms`, `markets` / `open_markets` / `market` (period, start, end, strike once started, time to end, result once resolved), `book` (both outcomes, full depth), `best_bid` / `best_ask`, `price` / `market_price` (last Pyth price), `usdc` (free) and `usdc_total`, `position`, `open_orders`. Orders: `limit` (GTC), `market_order` (FAK, with an optional worst price), `cancel`. A run can hold many markets; a strategy trades any market that is open.

Two examples in [examples/strategies](examples/strategies): `late_side` buys the outcome on BTC's side of the strike N seconds before the end; `ladder_mm` quotes a ladder on both sides of the mid. `cargo run --example late_side`, `cargo run --example ladder_mm`, or from the command line:

```bash
lucyd-backtest run --strategy ladder-mm --period 15m --from 2026-10-25T08:00:00Z --to 2026-10-25T14:00:00Z \
  --queue-model proportional --jitter-ms 50 --report report.json --journal journal.csv
lucyd-backtest run --strategy late-side --config '{"seconds_before": 60, "usd": 50}' --period 5m --from 2026-10-27 --to 2026-10-28
```

### Execution parameters

`ExecParams`, from code, from a JSON file (`--params`), or by flags. Every field has a default.

| Field | Flag | Default | |
|---|---|---|---|
| `initial_usdc` | `--initial-usdc` | 1000 | USDC at the start |
| `submit_latency_ms` | `--latency-ms` | 100 | decision to order in the book |
| `cancel_latency_ms` | `--cancel-latency-ms` | 100 | decision to cancel taking effect; the order can fill meanwhile |
| `latency_jitter_ms` | `--jitter-ms` | 0 | random extra delay up to this |
| `queue_model` | `--queue-model` | `pessimistic` | cancellations at our price stood behind us (`pessimistic`) or spread over the level (`proportional`) |
| `fee_bps` | `--fee-bps` | 0 | trading fee on own fills |
| `redeem_fee_bps` | | 0 | fee on winning redemptions (the contract's, 0 now) |
| `revert_probability` | `--revert-probability` | 0 | chance an own fill's settlement fails and is undone |
| `seed` | `--seed` | 0 | all randomness |
| `validation_fraction` | `--validation-fraction` | 0.3 | last share of the range reported separately as out-of-sample; 0 = none |
| `equity_interval_ms` | | 60000 | step of the equity curve |
| `min_fills_warning` | | 30 | fewer own fills give a warning |
| `risk_interval_ms` | `--risk-interval-ms` | 3600000 | step of the equity returns behind Sharpe and Sortino |
| `annualize` | `--annualize` | false | scale Sharpe and Sortino to a 365-day year |

### Report

`Report` (JSON via `to_json` / `write_json`, read back with `Report::read_json`): metrics for the whole run and separately for the fit and validation samples, per-market results with the price path of every market the strategy traded, the equity curve, every own order with its status, the journal of own events, and warnings. `engine::write_journal` writes the journal to CSV or Parquet; `report.summary_text()` is the terminal summary. Same data, parameters and seed give the same report bit for bit.

Metrics: P&L and return; max drawdown, in USDC and %, and how long it lasted (from the high to the return to it, or to the end); Sharpe and Sortino of the equity returns per `risk_interval_ms` (one hour by default, not annualized unless `annualize`); win rate, profit factor (won / lost, by market), average win and loss and their ratio, best and worst market, the longest run of losing markets; exposure (share of time with an open position, average and largest capital in positions); turnover, fees, fills, average taker slippage, limit order fill rate. A metric the sample cannot define (no losing market, too few points) is `null`, never infinite. Exact definitions: [docs/execution.md](docs/execution.md#report). The report format has a `version` (now 2); version 1 reports still read.

### HTML report

`report.to_html()` / `write_html`, `run --html` (next to `--report`, same name with `.html`) or `lucyd-backtest html report.json` turn a report into one self-contained page: no external scripts, styles or fonts, so it opens offline and can be sent as a file. On it: the request and parameters, the warnings, metric cards with the fit / validation split, the equity curve with the start of the validation period, drawdown, P&L by market (sortable bars; tap one to open the market), the chosen market's up or down price with the strategy's own buys and sells on it, and tables of markets and orders with filters. Light and dark theme, readable on a phone.

```bash
lucyd-backtest run --strategy late-side --period 5m --from 2026-10-27 --to 2026-10-28 --report out/late.json --html
lucyd-backtest html out/late.json -o late.html
```

### How orders execute, in short

The CLOB keeps one ladder per market: a buy of up at p and a buy of down at 1 − p meet each other (mint), as do two sells (merge); the engine works on that ladder, so liquidity is never counted twice. An order reaches the book after the latency; its crossing part walks the book from the best price (slippage), and what it took stays taken until the recording shows new orders there. A resting order queues behind what was recorded at its price; recorded fills eat that queue and then fill it; the book trading through its price fills it fully. Payout at resolution: 1 USDC for the winner, 0.5 for each token of a voided market.

Limits: other participants do not react to our orders; queue position is an estimate; markets without recorded fills move the queue only on book changes; on testnet most liquidity is the platform's own market maker. Full description: [docs/execution.md](docs/execution.md).

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
cargo test -- --ignored   # also against the live testnet, and builds a `new` project
```

## License

MIT
