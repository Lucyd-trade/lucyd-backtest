# Data format

What the loader reads, what it returns, and the event stream that the backtest engine will consume. Times are UTC. Prices of outcome tokens are USDC per token (0 to 1); BTC prices are USD.

## Sources

| Data | Where | Notes |
|---|---|---|
| Markets | `GET {api}/markets` | Filters `market_type`, `period`, `instrument`, `address`, `end_from` / `end_to`. Pages of 1000; past offset 10000 the loader moves the `end_from` window instead. |
| Trades | `GET {api}/trades` | `market` (several per request), `from` / `to` in unix seconds, `order=asc`. Long ranges are walked by time, never with an offset above 10000. |
| Order book | `{data}/books/<type>/<period>/<instrument>/<start date>/<market>.parquet` | One file per market, uploaded after the market ends and leaves the order book (usually within minutes, at most about 26 hours). A market that never had an order has no file. |
| Fills | `{data}/fills/<type>/<period>/<instrument>/<start date>/<market>.parquet` | Same path as the book file. One file per market, uploaded after the market ends (at most about 27 hours). No file: no match, the market is not over, or it is older than the fills history (about 2026-10-20). |
| Pyth prices | `{data}/prices/pyth/<feed>/<date>.parquet` | One file per feed and UTC day, published shortly after the day ends. Recorded from 2026-10-27; earlier days have no file. |

Testnet: `api` = `https://api.testnet.lucyd.trade`, `data` = `https://data.testnet.lucyd.trade`. Folder names are lowercase: type `updown`, periods `5m` `15m` `30m` `1h` `4h` `daily`, instrument `btc`, feed `btc-usd`.

The API allows 20 requests per second per IP. The loader sends at most 8 per second and retries 429, 408 and 5xx answers and network errors with exponential backoff (0.5 s doubling to 30 s, honouring `Retry-After`, 7 attempts).

### Book file

| Column | Type | |
|---|---|---|
| `timestamp` | timestamp ms UTC | when the recorder received the book |
| `state_sequence` | uint64 | order book publication order |
| `kind` | string | `snapshot` or `delta` |
| `asset_id` | string, null | outcome token id |
| `side` | string, null | `bid` / `ask` |
| `price` | double, null | multiple of 0.0025 |
| `size` | uint64, null | raw units, 1e6 per outcome token |

Rows with the same `timestamp` and kind `snapshot` are the complete book of both outcomes (an empty book is one row with null level fields). A `delta` row sets one level; size 0 removes it. The book at time T is the last snapshot at or before T plus the deltas after it up to T. A full snapshot is written at least once a minute while the book changes, and after every gap in recording. Changes within one ~100 ms window are merged by the order book before publication.

The outcome token id is `keccak256(market id ++ uint8(index))`, index 0 = up, 1 = down; the loader uses it to turn `asset_id` into an outcome index.

### Fills file

One row per match, sorted by `timestamp`, `journal_id`.

| Column | Type | |
|---|---|---|
| `timestamp` | timestamp ms UTC | match time by the order book clock |
| `journal_id` | int64 | order journal row id of the match |
| `trade_id` | string | `takerOrderId:makerOrderId`, as in `GET /trades` |
| `price` | double | price of the taker's outcome |
| `size` | uint64 | raw units, 1e6 per outcome token (the unit of book sizes) |
| `taker`, `maker` | string | wallets (`maker` null if its order is unknown) |
| `taker_side`, `maker_side` | string | `buy` / `sell` |
| `taker_asset_id`, `maker_asset_id` | string | outcome token, the same id as book `asset_id` |
| `match_type` | string, null | `direct` (same outcome), `mint` (both buy), `merge` (both sell) |
| `settlement` | string | `pending` / `confirmed` / `failed` |
| `tx_hash`, `block_number` | string / int64, null | settlement transaction |

`timestamp` is the order book's wall clock at the match; book file times are the receive time of the recorder on the same machine, so both are on one clock (the book lags by the ~100 ms publishing window plus delivery). A taker order against several makers is several rows with the same `timestamp`; `journal_id` keeps their order. `pending` means the settlement was still open when the file was written (at most an hour after the market's last fill).

### Price file

| Column | Type | |
|---|---|---|
| `publish_time` | timestamp ms UTC | price time by Pyth (whole seconds) |
| `receive_time` | timestamp ms UTC | when the recorder received the update |
| `price` | double | USD |
| `conf` | double | confidence interval, USD |
| `slot` | uint64, null | Pythnet slot |

About one update per second per feed.

## Catalog: `MarketInfo`

| Field | |
|---|---|
| `address` | market id, `0x` + 64 hex, lowercase (also the order book market id) |
| `market_type`, `period`, `instrument` | `UpDown`, `5m` … `1d`, `BTC` |
| `feed_id`, `feed_name` | Pyth feed the market settles on: `0xe62d…`, `BTC/USD` |
| `start`, `end` | trading window |
| `state` | `upcoming`, `open`, `pending` (ended, not yet resolved), `settled`, `voided` |
| `strike` | BTC price at `start`, USD (API value / 1e8) |
| `resolved_price` | BTC price at `end` used for settlement, USD |
| `resolved_at` | when the result was written on chain |
| `winner` | outcome index: 0 = up (`resolved_price > strike`), 1 = down |
| `voided` | no oracle price in time: every token pays 0.5 |
| `outcomes`, `outcome_ids` | labels and token ids, by outcome index |
| `has_fills` | the data host has a fills file of the market; absent when fills were neither asked for nor filtered on |

With `fills_only` the catalog keeps only markets with `has_fills`; `without_fills_excluded` counts the rest.

A market belongs to the request range `[from, to)` if `start < to` and `end > from`.

## Event stream

`Dataset::events()` returns one vector of events covering every selected market, clipped to the request range. It is the input contract of the backtest engine.

```json
{"seq": 0, "ts_ms": 1793008800000, "market": "0x0b30…", "type": "market_open", "strike": 80429.81}
```

Common fields:

| Field | |
|---|---|
| `seq` | position in the stream, from 0 |
| `ts_ms` | unix milliseconds |
| `market` | market address; absent on `price_tick` (a feed is shared by markets) |
| `type` | kind of event, below |

Kinds and their fields:

| `type` | Time | Fields |
|---|---|---|
| `market_open` | `start` | `strike` |
| `price_tick` | `receive_time` of the update | `feed` (`btc-usd`), `price`, `conf`, `publish_ms`. Only between the first start and the last end of the selected markets on that feed (and inside the range); `Dataset::prices` keeps the whole days. |
| `book_snapshot` | recorder time | `levels`, `synthetic` |
| `book_delta` | recorder time | `changes` |
| `fill` | match time (order book clock) | `fill` |
| `trade` | block time | `trade` |
| `market_close` | `end` | `winner`, `resolved_price`, `voided` |

A level (in `levels` and `changes`) is `{"outcome": 0, "side": "bid", "price": 0.5125, "size": 150000000}`; `size` is in raw units (1e6 per token), 0 in a change removes the level. A snapshot replaces the whole book of the market (both outcomes). When the range starts while a market's book already exists, the stream begins with a `synthetic: true` snapshot at `from`: the book rebuilt from history just before that moment.

A `fill` is one match from the taker's side: `trade_id`, `market`, `ts_ms`, `journal_id`, `taker`, `side`, `outcome_index`, `taker_asset_id`, `maker`, `maker_side`, `maker_outcome_index`, `maker_asset_id`, `price`, `size` (raw units, as book levels), `quantity` (tokens), `amount_usd` (`price` × `quantity`), `match_type`, `settlement` (`pending` / `confirmed` / `failed`), `tx_hash`, `block_number`. Failed fills are kept: in the order book the match happened and took the volume; whether to count it is the engine's decision.

A `trade` is one on-chain fill from the taker's side: `trade_id`, `market`, `tx_hash`, `block_number`, `log_index`, `block_time`, `match_type` (`direct`, `mint`, `merge`), `taker`, `side` (`buy` / `sell`), `outcome_index`, `price`, `quantity` (tokens), `amount_usd`.

### Order

Events are sorted by:

1. `ts_ms`;
2. kind at the same millisecond: `market_open`, `price_tick`, synthetic `book_snapshot`, recorded book events, `fill`, `trade`, `market_close`;
3. market address (feed name for prices);
4. own order: book `state_sequence` then row order; fill `journal_id`; trade block and log index; price `publish_ms`.

A taker order against several makers is several `fill` events in a row. With both `fills` and `trades` loaded the same match can appear once as each: two sources, not merged.

The same data always gives the same stream.

### Timing caveats

- Book times are when the recorder received the update, about 100 ms after the change.
- Fills carry the order book's own match time, in milliseconds. The book delta of the match comes about 100 ms later (publishing window).
- Trades carry the time of the block that settled them on chain, a few seconds to a few blocks after the match (up to ~12 s). The book shows the match earlier, as a delta.

### Fills or trades

- **fills**: order book time in milliseconds, in step with the book, with the settlement status; every match, including failed settlements. For execution backtests and queue models. From about 2026-10-20.
- **trades**: block time (up to ~12 s off), only fills that went through on chain. For checking against the chain and for periods before the fills history (from 2026-10-07).
- Price ticks use the receive time, so a strategy never sees a price before it could have.
- `market_close` carries the result at `end`. On chain the result arrives at `resolved_at`, usually within a minute.

## Flat export

`export` writes one row per event, and one row per level for book events (all with the event's `seq`). Columns: `seq`, `ts` (timestamp ms UTC), `type`, `market`, `feed`, `outcome`, `side` (`bid` / `ask` for book rows, `buy` / `sell` for fills and trades), `price`, `size` (tokens, for book levels, fills and trades), `amount_usd`, `conf`, `publish_ms`, `trade_id`, `journal_id` (fills), `taker`, `maker` (fills), `match_type`, `settlement` (fills), `tx_hash`, `block_number`, `strike`, `winner`, `resolved_price`, `voided`, `synthetic`. An empty book snapshot is one row with empty level columns.

## Cache

Default folder: the system cache directory + `lucyd-backtest` (`~/.cache/lucyd-backtest` on Linux, `~/Library/Caches/lucyd-backtest` on macOS, `%LOCALAPPDATA%\lucyd-backtest` on Windows), one subfolder per network (or per data URL when URLs are overridden). Layout mirrors the data host:

```
books/updown/15m/btc/2026-10-26/<market>.parquet
fills/updown/15m/btc/2026-10-26/<market>.parquet
prices/pyth/btc-usd/2026-10-27.parquet
trades/<market>.json
```

- Book, fills and price files never change once published, so a cached file is never downloaded again. A file still absent well after it was due (a day for prices, two days for books, ten days for fills: the fills recorder's first run uploads a week of history at once) gets a `.missing` marker and is not asked for again. Fills of a market that is not over are not asked for.
- Trades of a market are complete once the market is settled or voided and an hour has passed since its end. Until then each load fetches only trades newer than the last fetch (minus a 10 minute margin for indexing).
- Every file is written to a temporary name and renamed, so an interrupted run leaves no partial files. Deleting the folder is always safe.
- The market catalog is not cached: it is one or a few requests, and states change.
