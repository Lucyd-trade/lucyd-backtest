# Execution model

How the engine turns a strategy's orders into fills on recorded data, and where that differs from what would really have happened. Prices are USDC per outcome token, on the 0.0025 grid; quantities are whole tokens.

## The run

The engine replays the event stream of a [`Dataset`](data-format.md#event-stream) in time order and calls the strategy for each event it subscribed to. Between events it runs its own scheduled work: orders and cancels reaching the book, timers, throttled book updates, resolutions, equity samples. Everything the strategy sees through its context is the state after the events up to the current moment. Nothing from later is visible: no future book, fill or price, and no market result before the result reaches the chain.

At equal times scheduled work comes before the recorded event. A timer at 12:00:00.000 does not see a book delta recorded at the same millisecond; the next call will.

## One book per market

The order book keeps a single ladder per market, in prices of up:

| Order | Ladder side |
|---|---|
| buy up at p | bid at p |
| sell down at 1 − p | bid at p |
| sell up at p | ask at p |
| buy down at 1 − p | ask at p |

Orders on the same side and price share one queue, whatever outcome they were placed on. A buy of up meets a sell of up (a direct transfer of tokens) or a buy of down at the complementary price (mint: both pay, a new pair of tokens is split). A sell meets a buy (direct) or a sell of the other outcome (merge: the pair is redeemed for 1 USDC). The book of down that the feed shows is the mirror of up's: up's bid at 0.48 is down's ask at 0.52, the same orders. The recorded data confirms it: both outcomes carry the same levels mirrored, and a fill taken through the other outcome (`match_type = mint`) shows up as a change of that one ladder.

The engine works on this ladder. Every own order and every recorded fill is put onto it, so mint and merge are part of every match and the same liquidity is never counted twice. A strategy may still think in outcomes: `ctx.book()` shows both, `ctx.best_ask(market, 1)` is the best price to buy down.

Exchange rules the engine applies (from the order book code): price within 0.0025…0.9975 on the 0.0025 grid, whole tokens, at least 1 USDC of collateral per order (a sell of the whole position is exempt), execution at the resting order's price, and an order never matches another order of the same account: an incoming order that reaches an own resting order cancels the resting one (`self_trade`).

## Latency

Every order reaches the book `submit_latency_ms` after the strategy placed it (default 100), every cancel `cancel_latency_ms` after it was asked for (default 100). `latency_jitter_ms` adds a random delay up to that many ms to each. Until a cancel arrives the order works: it can still fill. A cancel that arrives before its order (possible with jitter) misses it (`cancel_failed: unknown_order`), as on the exchange.

Order updates reach the strategy at the moment they happen in the book; the delay back to the strategy is not modelled separately.

## On arrival

The engine checks the order the way the exchange does: the market is trading, the order has at least 1 USDC of collateral, and the account has the USDC (a buy holds limit price × quantity, plus the fee) or the tokens (a sell holds them). A failed check is `rejected` with the reason.

The crossing part executes at once. The order walks the opposite side of the recorded book from the best price, level by level, while the price is within its limit. On each level it takes what is there, at that level's price. The average price against the best price at arrival is the slippage, kept per order and in the report.

Whatever the order took is remembered as eaten: the engine keeps showing and offering the level minus that amount. The recording does not know about our order, so without this the same liquidity could be taken again and again. Eaten liquidity comes back when the recording shows the level grow (new orders arrived); when the level shrinks, the eaten part stays as far as the level still holds it.

The rest of a market order (FAK) is cancelled (`fak_remainder`). The rest of a limit order (GTC) rests in the book.

## A resting order

When a limit order starts resting, everything recorded at its price on its side stands ahead of it: that is its queue.

- A recorded fill on its side and price takes from the queue first. Once the queue is gone, the next recorded fills execute our order, partly or fully.
- A recorded fill at a worse price on our side means the taker reached past our price, so it would have met our better order first: our order fills at its own price, up to the fill's size.
- The level shrinking with no fill to explain it means orders there were cancelled. With `queue_model = pessimistic` (default) they stood behind us and the queue does not move. With `proportional` the queue shrinks by the same share as the level. In both cases no more than the level holds can stand ahead of us.
- The opposite side of the book coming to or through our price (the best ask at or below our bid, or the best bid at or above our ask) fills the whole order at our price.
- Fills whose on-chain settlement failed still count: in the book the match happened and the volume left.

Recorded fills reach the book file about 100 ms later as a delta. The engine matches such a drop against the fills it has just seen, so the same volume does not move the queue twice.

At the end of a market the order book drops it with all resting orders (`market_closed`).

## Account and resolution

USDC and tokens are kept per outcome and market, in exact integer units (micro-USDC and 1e-6 token). Buys hold USDC and sells hold tokens while the order is open; what is not used goes back when it ends.

`fee_bps` charges a fee on the notional of every own fill (Lucyd charges none now). `revert_probability` makes the on-chain settlement of an own fill fail with that chance: the strategy gets the fill and then `settlement_failed`, and USDC and tokens are as if it never happened. The order does not get the quantity back and the liquidity stays taken (a modelling choice).

When the result reaches the chain (`resolved_at`, or the market end if unknown) every token is redeemed: the winning outcome pays 1 USDC (minus `redeem_fee_bps`, the contract's redemption fee, 0 now), the other 0; a voided market pays 0.5 for every token of either outcome, with no fee. A market without a result inside the data keeps its tokens, valued at the last recorded mid (or the last fill price, or 0.5).

## Report

- **P&L**: per market, sells minus buys minus fees plus the payout (plus open tokens at the mark for unresolved markets); the run's P&L is their sum and equals the change of equity.
- **Win rate**: markets with P&L above 0 among markets with own fills.
- **Equity curve**: USDC plus open tokens at the marks (`equity`, with the positions part in `positions`), every `equity_interval_ms` (default one minute), when the first position opens or the last closes, and at the end; **max drawdown** is its largest fall from a previous high.
- **Price path**: for every market with own orders, `marks` holds up's mark (the value positions are marked at) on changes, at most every max(1 s, market length / 150); down is 1 − up. The HTML report draws own fills on it.
- **Turnover**, **fees**, **fills**, **reverted fills**.
- **Average slippage** of taker orders, weighted by quantity.
- **Limit fill rate**: limit orders that reached the book and got any fill.
- **Risk** (each also per fit and validation sample, from that sample's equity and markets):
  - **Sharpe**: the equity is taken at every full `risk_interval_ms` from the first point (the last point at or before each step; a trailing part shorter than a step is left out); simple returns between them; mean over the sample standard deviation (n − 1). **Sortino**: the same mean over the downside deviation, the root mean square of the returns below 0 taken over all n. Not annualized: both are per interval, and `annualization` in the metrics is 1. With `annualize` both are multiplied by √(intervals in 365 days), e.g. √8760 for one hour; `annualization` then holds that factor. Fewer than two returns, returns that never vary (Sharpe) or none below 0 (Sortino), or equity at 0 or below: `null`.
  - **Profit factor**: the sum of P&L of winning markets over the loss of losing ones, among markets with own fills; `null` without a losing market. **Average win / loss**: mean P&L of winning and of losing markets (the loss is below 0), and their ratio win / −loss. **Best and worst market** by P&L. **Longest losing streak**: most losing markets in a row by end time; a market at exactly 0 ends a streak.
  - **Drawdown length**: from the high before the largest drawdown to the first point back at that high; if equity never gets back, to the end of the sample, and `max_drawdown_recovered` is false.
  - **Exposure**: the equity curve also gets a point whenever the first position opens and the last one closes, so these are exact, not bound to the sampling step. Each point's value of open positions (at the marks) holds until the next point: the share of time with any position, the time-weighted average value of positions, and the largest value seen on the curve.
- **Fit and validation**: with `validation_fraction` f (default 0.3) markets ending in the last f of the time range form the validation sample, the rest the fit sample; each gets its own metrics, the validation drawdown starting from the equity at the split.
- **Orders** with status, average price and slippage, and the **journal** of every own event (submitted, accepted, rejected, fill, settlement_failed, cancel_requested, cancelled, cancel_failed, payout) with the USDC balance after it. `write_journal` exports the journal as CSV or Parquet.
- **Warnings**: too few own fills, markets where the strategy had orders but the data has no recorded fills, markets without a result, testnet liquidity, and that own orders do not change other participants.

The same data, parameters and seed give the same report, bit for bit: all randomness comes from one seeded generator, money is integer, and every collection is ordered.

## What the model cannot know

- **Other participants do not react to us.** The recording is replayed as it was. A real order would change prices, take liquidity others then miss, and invite reactions. The bigger and more frequent the orders, the more the backtest flatters them.
- **Queue position is estimated.** The book shows sizes per price, not the orders in them, so cancellations ahead of or behind us are a modelling choice (`queue_model`).
- **Trading through a resting order fills it fully.** In reality the crossing order might be smaller than ours.
- **Markets without recorded fills** move the queue only on book changes and through-trades: fills of resting orders there are rough.
- **Book timing.** The recorder sees changes about 100 ms after they happen; fills carry the order book's own clock. Within that window the order of events is approximate.
- **On-chain trades** (`trades` in the data) are not used for execution: they arrive up to a block later and miss failed matches.
- **Testnet.** Most liquidity comes from the platform's own market maker, with test USDC.
