# Notes for coding agents

How to work with this repo and with strategies built on it. Human docs: [README.md](README.md), [docs/execution.md](docs/execution.md), [docs/data-format.md](docs/data-format.md).

## What this is

A Rust crate and CLI: a loader that downloads historical data of Lucyd prediction markets (L2 order book, order book fills, on-chain trades, Pyth BTC/USD prices) and an engine that backtests a strategy on it. Markets: "will BTC be higher at the end of the period than at the start", periods 5m, 15m, 30m, 1h, 4h, 1d. Outcome 0 is up, 1 is down. Tokens trade from 0.0025 to 0.9975 USDC in steps of 0.0025; the winner pays 1, a voided market pays 0.5 per token. Testnet: liquidity is mostly the platform's own market maker, so results are not real-money estimates.

## Writing a strategy

Fastest path, outside this repo:

```bash
cargo install --git https://github.com/lucyd-trade/lucyd-backtest
lucyd-backtest new my-strategy && cd my-strategy
cargo run --release   # summary in the terminal, reports/report.json and reports/report.html
```

Edit `src/main.rs` of the new project. A strategy implements `engine::Strategy`; all handlers have empty defaults:

- `subscriptions()` picks what to receive: book (every change, `BookFeed::Throttle(ms)` or none), fills, prices, a timer.
- Handlers: `on_start`, `on_market_open`, `on_book`, `on_fill` (others' recorded fills), `on_price`, `on_timer`, `on_market_close` (result unknown yet), `on_resolution`, `on_order` (own order updates), `on_finish`.
- `Ctx` sees only the past: `now_ms`, `open_markets`, `market`, `book`, `best_bid` / `best_ask`, `market_price`, `usdc`, `position`, `open_orders`.
- Orders: `ctx.limit(market, outcome, side, price, tokens)` (GTC), `ctx.market_order(market, outcome, side, tokens, Some(worst_price))` (FAK), `ctx.cancel(id)`. Tokens are whole numbers; prices must be on the 0.0025 grid.

Worked examples: [examples/strategies/late_side.rs](examples/strategies/late_side.rs) (taker near the end of a market), [examples/strategies/ladder_mm.rs](examples/strategies/ladder_mm.rs) (maker ladder around the mid).

Without writing code, the CLI runs the built-in examples:

```bash
lucyd-backtest run --strategy late-side --period 5m --from 2026-10-27 --to 2026-10-28 --report out/r.json --html
```

## Reading results

`Report` JSON has `summary` (whole run) and the fit / validation split (`validation_fraction`, last 30% of the range by default). Judge a strategy on the validation sample, and look at `warnings`: too few own fills, missing data. Same data, params and seed give the same report, so compare runs by changing one thing at a time.

## Picking data

- Fills (default) exist from about 2026-10-20, Pyth prices from 2026-10-27, order books from early October 2026, on-chain trades from 2026-10-07.
- Start with one period and one day; a day of 5m markets is 288 markets.
- `lucyd-backtest list ...` shows what a request selects before downloading. `--fills-only` drops markets nobody traded.
- Data is cached; finished markets are never downloaded twice.

## Working on the crate itself

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test -- --ignored   # hits the live testnet and builds a `new` project; needs network
```

CI runs the first three on Linux, macOS and Windows. Layout: `src/loader.rs` + `request.rs` (what to load), `book.rs`, `fills.rs`, `trades.rs`, `prices.rs` (sources), `event.rs` (event stream), `engine/` (execution, report, risk, HTML), `scaffold.rs` + `templates/new/` (the `new` command), `main.rs` (CLI). If you change the public API or the report format, update README.md and the docs in the same change.

## Live data in an agent

For current markets, order books and trades (not history), connect the [Lucyd MCP server](https://github.com/lucyd-trade/mcp-server): hosted read-only at `https://mcp.lucyd.trade/mcp`, or locally with `npx -y @lucyd/mcp`.
