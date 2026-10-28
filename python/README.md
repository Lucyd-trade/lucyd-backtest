# lucyd-backtest for Python

Backtest trading strategies on [Lucyd](https://lucyd.trade) prediction markets from Python or Jupyter. Load the historical L2 order book, order book fills, on-chain trades and Pyth BTC/USD prices as pandas DataFrames. Then run a strategy written in Python on an execution engine that models latency, slippage on the recorded book and queue position. The engine is written in Rust and ships as a compiled wheel, so there is nothing to build.

> Lucyd currently runs on the Ethereum Sepolia testnet with test USDC. Most testnet liquidity comes from the platform's own market maker, so use the data to build and test strategies, not to estimate real-money returns.

```bash
pip install lucyd-backtest
```

Python 3.9 or newer on Linux, macOS or Windows. Dependencies: pandas and pyarrow.

## The markets

Every market asks one question: will BTC be higher at the end of its period than at the start? The price at the start is the strike. The market settles on the Pyth BTC/USD price at the end. There are two outcome tokens, up (0) and down (1). They trade from 0.0025 to 0.9975 USDC in steps of 0.0025, and the winning token pays 1 USDC. A market that gets no oracle price in time is voided, and each token then pays 0.5. Series run back to back: 5m, 15m, 30m, 1h, 4h and 1d.

## Data

```python
import lucyd_backtest as lb

data = lb.load(period="15m", start="2026-10-26", end="2026-10-28")

data.markets          # one row per market: start, end, strike, resolved price, winner, volume
data.fills()          # every match of the order book: price, size, side, taker, maker, settlement
data.prices()         # Pyth ticks: price, conf, publish time
data.book_updates()   # recorded book snapshots and deltas, one row per level
data.events()         # everything above as one time-ordered stream

book = data.book_at(data.markets.address[0], "2026-10-26T00:05:00Z")
book.best_bid("up"), book.asks("down")
```

`load` arguments:

- **`period`**: `"5m"`, `"15m"`, `"30m"`, `"1h"`, `"4h"` or `"1d"`, or a list of them. The default is all.
- **`start`, `end`**: `"2026-10-27"`, `"2026-10-27T12:00:00Z"`, unix seconds, a `date` or a `datetime`, all in UTC. A market is selected if it overlaps the range.
- **`data`**: any of `"book"`, `"fills"`, `"trades"` and `"prices"`, or `"all"`. The default is book, fills and prices.
- **`fills_only=True`**: leaves out markets nobody traded.
- **`market_ids`**: specific market addresses, used instead of a period and range.

`lb.markets(...)` takes the same arguments and returns only the market list, without downloading data.

Downloads are cached, and a finished market is never downloaded twice.

What is available from when:

| Data | Since |
|---|---|
| Fills | about 2026-10-20 |
| Order book | early October 2026 |
| On-chain trades | 2026-10-07 |
| Pyth prices | 2026-10-27 |

Gaps in the data show up as warnings in `data.warnings`; they are never errors.

## Backtest

```python
class LateSide(lb.Strategy):
    timer_ms = 1000

    def __init__(self, seconds_before=30, max_price=0.95):
        self.seconds_before, self.max_price, self.done = seconds_before, max_price, set()

    def on_timer(self, ctx):
        for m in ctx.open_markets():
            if m.address in self.done or m.time_to_end_ms > self.seconds_before * 1000:
                continue
            btc = ctx.market_price(m.address)
            if m.strike is None or btc is None:
                continue
            side = "up" if btc.price > m.strike else "down"
            ask = ctx.best_ask(m.address, side)
            if ask and ask.price <= self.max_price:
                ctx.market_order(m.address, side, "buy", 20, limit=self.max_price)
                self.done.add(m.address)

report = lb.backtest(data, LateSide(), initial_usdc=1000, submit_latency_ms=100)
report                       # in Jupyter: the full interactive report
report.summary["pnl"], report.validation
report.equity, report.orders, report.markets   # DataFrames
report.save_html("report.html")                # or report.show() outside Jupyter
```

**Handlers.** Implement only the ones you need; the engine calls only what you implement:

- `on_start`, `on_finish`;
- `on_market_open(ctx, market)`, `on_market_close(ctx, market)`;
- `on_resolution(ctx, market, result)`;
- `on_book(ctx, market)`: the book changed. Set `book_throttle_ms` to get it at most once per that many ms;
- `on_fill(ctx, fill)`: a recorded fill of another participant;
- `on_price(ctx, feed, price)`;
- `on_timer(ctx)`: needs `timer_ms`;
- `on_order(ctx, update)`: your order was accepted, filled, cancelled or rejected.

**Context.** `ctx` shows the state as of now and nothing later:

- markets: `ctx.open_markets()`, `ctx.market(address)`;
- book and prices: `ctx.book(address)`, `ctx.best_bid(address, outcome)`, `ctx.best_ask(...)`, `ctx.market_price(address)`;
- account: `ctx.usdc`, `ctx.position(address, outcome)`, `ctx.open_orders()`;
- orders: `ctx.limit(address, outcome, side, price, tokens)` places a GTC order, `ctx.market_order(address, outcome, side, tokens, limit=None)` a FAK order, and `ctx.cancel(order_id)` cancels.

An outcome is `"up"` / `0` or `"down"` / `1`. A side is `"buy"` or `"sell"`. Prices are on the 0.0025 grid, and tokens are whole numbers.

**Execution parameters.** Pass these as keywords to `backtest`:

| Parameter | Default |
|---|---|
| `initial_usdc` | 1000 |
| `submit_latency_ms`, `cancel_latency_ms` | 100 |
| `latency_jitter_ms` | 0 |
| `queue_model` | `"pessimistic"` (or `"proportional"`) |
| `fee_bps` | 0 |
| `revert_probability` | 0 |
| `seed` | 0 |
| `validation_fraction` | 0.3 |
| `risk_interval_ms` | 3600000 |
| `annualize` | False |

The same data, parameters and seed give the same report.

**Validation sample.** The last 30% of the range is reported separately as `report.validation`. Judge a strategy on that sample.

How orders execute, and the limits of the model: [docs/execution.md](https://github.com/lucyd-trade/lucyd-backtest/blob/main/docs/execution.md). Event columns: [docs/data-format.md](https://github.com/lucyd-trade/lucyd-backtest/blob/main/docs/data-format.md). A full example: [examples/late_side.py](https://github.com/lucyd-trade/lucyd-backtest/blob/main/python/examples/late_side.py).

## License

MIT
