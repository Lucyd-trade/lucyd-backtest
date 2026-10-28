"""Backtesting on Lucyd prediction markets from Python.

Load historical data as pandas DataFrames and backtest a strategy written in Python on the
same execution engine as the Rust crate (latency, slippage on the recorded book, queue position).

    import lucyd_backtest as lb

    data = lb.load(period="5m", start="2026-10-27", end="2026-10-28")
    data.markets           # one row per market
    data.fills()           # every match of the order book
    data.prices()          # Pyth BTC/USD ticks

    class BuyCheapUp(lb.Strategy):
        timer_ms = 1000

        def on_timer(self, ctx):
            for m in ctx.open_markets():
                ask = ctx.best_ask(m.address, "up")
                if ask and ask.price <= 0.3 and ctx.position(m.address, "up") == 0:
                    ctx.market_order(m.address, "up", "buy", 10, limit=0.3)

    report = lb.backtest(data, BuyCheapUp())
    report                 # in Jupyter: the full HTML report
    report.summary["pnl"]
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import io as _io
import json as _json
import os as _os
import tempfile as _tempfile
import warnings as _warnings
import webbrowser as _webbrowser
from typing import Any, Iterable, Optional, Sequence, Union

import pandas as pd

from . import _native
from ._native import Book, Ctx, LucydError

__version__ = "0.1.0"

__all__ = [
    "Book",
    "Ctx",
    "Data",
    "LucydError",
    "Report",
    "Strategy",
    "backtest",
    "load",
    "markets",
    "read_report",
]

PERIODS = ("5m", "15m", "30m", "1h", "4h", "1d")
DATA_KINDS = ("book", "fills", "trades", "prices")
HANDLERS = (
    "on_start",
    "on_market_open",
    "on_book",
    "on_fill",
    "on_price",
    "on_timer",
    "on_market_close",
    "on_resolution",
    "on_order",
    "on_finish",
)

TimeLike = Union[str, int, float, _dt.date, _dt.datetime, pd.Timestamp]


def _ms(t: TimeLike, name: str) -> int:
    """Unix ms of a time: `2026-10-27`, `2026-10-27T12:00:00Z`, unix seconds, date, datetime
    (naive means UTC) or pandas Timestamp."""
    if isinstance(t, bool):
        raise TypeError(f"{name}: expected a time, got {t!r}")
    if isinstance(t, _dt.datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=_dt.timezone.utc)
        return round(t.timestamp() * 1000)
    if isinstance(t, _dt.date):
        return round(_dt.datetime(t.year, t.month, t.day, tzinfo=_dt.timezone.utc).timestamp()) * 1000
    if isinstance(t, (int, float)):
        return round(t * 1000)
    if isinstance(t, str):
        return _native.parse_time_ms(t)
    raise TypeError(f"{name}: expected a date, datetime, string or unix seconds, got {type(t).__name__}")


def _iso(ms: int) -> str:
    t = _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc)
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _list(x: Union[None, str, Iterable[str]]) -> list:
    if x is None:
        return []
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return list(x)


def _request(
    period, start, end, instrument, markets, data, fills_only, network, api_url, data_url
) -> str:
    kinds = [k.lower() for k in _list(data)]
    if kinds == ["all"]:
        kinds = list(DATA_KINDS)
    bad = sorted(set(kinds) - set(DATA_KINDS))
    if bad:
        raise ValueError(f"data {bad}: any of {', '.join(DATA_KINDS)}, or 'all'")
    periods = _list(period)
    bad = [p for p in periods if p not in PERIODS]
    if bad:
        raise ValueError(f"period {bad}: any of {', '.join(PERIODS)}")
    req: dict[str, Any] = {"network": network, "data": {k: k in kinds for k in DATA_KINDS}}
    if instrument:
        req["instrument"] = instrument
    if periods:
        req["periods"] = periods
    if markets is not None:
        req["markets"] = [m.lower() for m in _list(markets)]
    if start is not None:
        req["from"] = _iso(_ms(start, "start"))
    if end is not None:
        req["to"] = _iso(_ms(end, "end"))
    if fills_only:
        req["fills_only"] = True
    if api_url:
        req["api_url"] = api_url
    if data_url:
        req["data_url"] = data_url
    return _json.dumps(req)


def _times(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


def _ms_times(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a UTC datetime column next to every `*_ms` timestamp column (`ts_ms` → `ts`)."""
    for c in [c for c in df.columns if c.endswith("_ms")]:
        name = c[:-3]
        if name not in df.columns:
            df.insert(df.columns.get_loc(c) + 1, name, pd.to_datetime(df[c], unit="ms", utc=True))
    return df


def _markets_frame(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    return _times(df, ["start", "end", "resolved_at"])


def markets(
    period: Union[None, str, Iterable[str]] = None,
    start: Optional[TimeLike] = None,
    end: Optional[TimeLike] = None,
    *,
    instrument: Optional[str] = None,
    market_ids: Union[None, str, Iterable[str]] = None,
    data: Union[str, Iterable[str]] = ("book", "fills", "prices"),
    fills_only: bool = False,
    network: str = "testnet",
    api_url: Optional[str] = None,
    data_url: Optional[str] = None,
    cache_dir: Optional[str] = None,
) -> pd.DataFrame:
    """Markets a request selects, without downloading their data. Same arguments as `load`.

    The `has_fills` column tells which markets have recorded fills (when fills are asked for).
    Warnings are in `df.attrs["warnings"]`.
    """
    req = _request(period, start, end, instrument, market_ids, data, fills_only, network, api_url, data_url)
    c = _json.loads(_native.catalog(req, cache_dir))
    df = _markets_frame(c["markets"])
    df.attrs["warnings"] = c.get("warnings", [])
    df.attrs["without_fills_excluded"] = c.get("without_fills_excluded", 0)
    return df


class Data:
    """Loaded data of the selected markets. Make one with `load`."""

    def __init__(self, native: Any):
        self._n = native
        self.request: dict = _json.loads(native.request_json())
        self.warnings: list = list(native.warnings)
        self._markets: Optional[pd.DataFrame] = None

    @property
    def markets(self) -> pd.DataFrame:
        """One row per market: address, period, start, end, strike, resolved price, winner
        (0 = up, 1 = down), voided, volume. Prices in USD."""
        if self._markets is None:
            self._markets = _markets_frame(_json.loads(self._n.markets_json()))
        return self._markets.copy()

    def events(self, kinds: Union[None, str, Iterable[str]] = None) -> pd.DataFrame:
        """The time-ordered event stream, one row per event (per level for book events).

        `kinds`: any of `market_open`, `price_tick`, `book_snapshot`, `book_delta`, `fill`,
        `trade`, `market_close`; all when None. Columns: docs/data-format.md.
        """
        k = _list(kinds) or None
        return pd.read_parquet(_io.BytesIO(self._n.events_parquet(k)))

    def _only(self, kind: str) -> pd.DataFrame:
        df = self.events([kind])
        return df.dropna(axis=1, how="all").reset_index(drop=True)

    def fills(self) -> pd.DataFrame:
        """Every match of the order book (needs `fills` in `data`), from the taker's side."""
        return self._only("fill")

    def trades(self) -> pd.DataFrame:
        """Fills settled on chain (needs `trades` in `data`)."""
        return self._only("trade")

    def prices(self) -> pd.DataFrame:
        """Pyth ticks (needs `prices` in `data`): price, conf, publish time."""
        return self._only("price_tick")

    def book_updates(self) -> pd.DataFrame:
        """Recorded book snapshots and deltas, one row per level."""
        df = self.events(["book_snapshot", "book_delta"])
        return df.dropna(axis=1, how="all").reset_index(drop=True)

    def book_at(self, market: str, time: TimeLike) -> Optional[Book]:
        """Full book of a market at a moment: `.best_bid("up")`, `.asks("down")`, ..."""
        return self._n.book_at(market.lower(), _ms(time, "time"))

    def __len__(self) -> int:
        return len(self._n)

    def __repr__(self) -> str:
        r = self.request
        span = f"{r.get('from', '?')} .. {r.get('to', '?')}"
        w = f", {len(self.warnings)} warnings" if self.warnings else ""
        return f"<lucyd_backtest.Data: {len(self)} markets, {span}{w}>"


def load(
    period: Union[None, str, Iterable[str]] = None,
    start: Optional[TimeLike] = None,
    end: Optional[TimeLike] = None,
    *,
    instrument: Optional[str] = None,
    market_ids: Union[None, str, Iterable[str]] = None,
    data: Union[str, Iterable[str]] = ("book", "fills", "prices"),
    fills_only: bool = False,
    network: str = "testnet",
    api_url: Optional[str] = None,
    data_url: Optional[str] = None,
    cache_dir: Optional[str] = None,
) -> Data:
    """Download (or read from the cache) the data of markets overlapping `[start, end)`.

    period: `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, a list of them, or None for all.
    start, end: `"2026-10-27"`, `"2026-10-27T12:00:00Z"`, unix seconds, date or datetime (UTC).
    market_ids: market addresses instead of period and range.
    data: any of `book`, `fills`, `trades`, `prices`, or `"all"`.
    fills_only: leave out markets nobody traded.

    Finished markets are cached and never downloaded twice.
    """
    req = _request(period, start, end, instrument, market_ids, data, fills_only, network, api_url, data_url)
    d = Data(_native.load(req, cache_dir))
    if d.warnings:
        _warnings.warn(f"{len(d.warnings)} data warnings, see .warnings", stacklevel=2)
    return d


class Strategy:
    """Base class of a strategy. Implement the handlers you need; the engine only calls those.

    Every handler gets `ctx` first: the state as of now (nothing later) and the way to trade.
    It is valid only during that call.

    Class attributes:
        timer_ms: how often `on_timer` runs, required with `on_timer`.
        book_throttle_ms: call `on_book` at most once per this many ms per market
            (default: on every change).
        name: name in the report (default: the class name).

    Context (`ctx`):
        ctx.now_ms, ctx.markets(), ctx.open_markets(), ctx.market(address)
            markets have .address .period .start_ms .end_ms .strike .open .time_to_end_ms
            .resolution
        ctx.book(address) -> Book, ctx.best_bid(address, outcome), ctx.best_ask(...)
            -> .price .tokens, or None
        ctx.market_price(address), ctx.price("btc-usd") -> .price .conf .publish_ms
        ctx.usdc, ctx.usdc_total, ctx.position(address, outcome), ctx.open_orders(address=None)
        ctx.limit(address, outcome, side, price, tokens) -> order id (GTC)
        ctx.market_order(address, outcome, side, tokens, limit=None) -> order id (FAK)
        ctx.cancel(order_id)

    outcome is 0 / "up" or 1 / "down"; side is "buy" or "sell"; prices on the 0.0025 grid;
    tokens whole.
    """

    timer_ms: Optional[int] = None
    book_throttle_ms: Optional[int] = None
    name: Optional[str] = None

    def on_start(self, ctx: Ctx) -> None:
        """Once, before the first event."""

    def on_market_open(self, ctx: Ctx, market: str) -> None:
        """A market started trading."""

    def on_book(self, ctx: Ctx, market: str) -> None:
        """The book of `market` changed."""

    def on_fill(self, ctx: Ctx, fill: Any) -> None:
        """A recorded fill of another participant: .price .quantity .side .outcome_index ..."""

    def on_price(self, ctx: Ctx, feed: str, price: Any) -> None:
        """A Pyth tick: .price .conf .publish_ms .ts_ms."""

    def on_timer(self, ctx: Ctx) -> None:
        """Every `timer_ms`."""

    def on_market_close(self, ctx: Ctx, market: str) -> None:
        """Trading ended; the result is not known yet."""

    def on_resolution(self, ctx: Ctx, market: str, result: Any) -> None:
        """The result is on chain and positions were paid out: .winner .voided .resolved_price."""

    def on_order(self, ctx: Ctx, update: Any) -> None:
        """News about an own order: .order_id .status (accepted, filled, cancelled, rejected,
        settlement_failed, cancel_failed) and, for fills, .price .quantity .remaining ..."""

    def on_finish(self, ctx: Ctx) -> None:
        """Once, after the last event."""


def _handlers(strategy: Any) -> list:
    out = []
    for h in HANDLERS:
        f = getattr(type(strategy), h, None)
        if callable(f) and f is not getattr(Strategy, h):
            out.append(h)
    return out


class Report:
    """Result of a backtest. In Jupyter it shows as the full HTML report."""

    def __init__(self, native: Any):
        self._n = native
        self._d: dict = _json.loads(native.to_json())

    @property
    def summary(self) -> dict:
        """Metrics of the whole run: pnl, return, max drawdown, Sharpe, win rate, fills, ..."""
        return self._d["summary"]

    @property
    def fit(self) -> Optional[dict]:
        """Metrics of the fit sample (the range before the validation part)."""
        return (self._d.get("periods") or {}).get("fit")

    @property
    def validation(self) -> Optional[dict]:
        """Metrics of the last `validation_fraction` of the range: judge a strategy on these."""
        return (self._d.get("periods") or {}).get("validation")

    @property
    def warnings(self) -> list:
        return self._d.get("warnings", [])

    @property
    def equity(self) -> pd.DataFrame:
        """Equity curve: ts, equity, positions."""
        return _ms_times(pd.DataFrame(self._d.get("equity", [])))

    @property
    def orders(self) -> pd.DataFrame:
        """Every own order with its status, fill and slippage."""
        rows = [{k: v for k, v in o.items() if not isinstance(v, (list, dict))} for o in self._d.get("orders", [])]
        return _ms_times(pd.DataFrame(rows))

    @property
    def markets(self) -> pd.DataFrame:
        """Result per traded market: pnl, payout, fees, turnover, fills."""
        rows = [{k: v for k, v in m.items() if not isinstance(v, (list, dict))} for m in self._d.get("markets", [])]
        return _times(pd.DataFrame(rows), ["start", "end"])

    @property
    def journal(self) -> pd.DataFrame:
        """Every own event: submits, fills, cancels, rejects, payouts."""
        return _ms_times(pd.DataFrame(self._d.get("journal", [])))

    def to_dict(self) -> dict:
        return _json.loads(self._n.to_json())

    def to_json(self) -> str:
        return self._n.to_json()

    def to_html(self) -> str:
        """The report as one self-contained HTML page."""
        return self._n.to_html()

    def save_json(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self._n.to_json())

    def save_html(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self._n.to_html())

    def show(self) -> None:
        """Open the HTML report in the browser (outside Jupyter)."""
        fd, path = _tempfile.mkstemp(prefix="lucyd-report-", suffix=".html")
        with _os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(self._n.to_html())
        _webbrowser.open("file://" + _os.path.abspath(path))

    def summary_text(self) -> str:
        return self._n.summary_text()

    def __repr__(self) -> str:
        return self._n.summary_text()

    def _repr_html_(self) -> str:
        page = _html.escape(self._n.to_html(), quote=True)
        return f'<iframe srcdoc="{page}" style="width:100%;height:900px;border:0"></iframe>'


def backtest(data: Data, strategy: Any, params: Optional[dict] = None, **kwargs: Any) -> Report:
    """Run `strategy` over `data` and return the report.

    Execution parameters, as keywords or a dict (all optional):
        initial_usdc=1000, submit_latency_ms=100, cancel_latency_ms=100, latency_jitter_ms=0,
        queue_model="pessimistic" | "proportional", fee_bps=0, redeem_fee_bps=0,
        revert_probability=0, seed=0, validation_fraction=0.3, equity_interval_ms=60000,
        min_fills_warning=30, risk_interval_ms=3600000, annualize=False

    Same data, parameters and seed give the same report. An exception in a handler stops the
    strategy and is raised here.
    """
    if not isinstance(data, Data):
        raise TypeError("data: expected the result of lucyd_backtest.load(...)")
    p = dict(params or {})
    p.update(kwargs)
    hs = _handlers(strategy)
    if not hs:
        raise ValueError("the strategy implements no handler (on_timer, on_book, on_price, ...)")
    timer = getattr(strategy, "timer_ms", None)
    if "on_timer" in hs and not timer:
        raise ValueError("on_timer needs timer_ms, e.g. timer_ms = 1000 on the class")
    throttle = getattr(strategy, "book_throttle_ms", None)
    name = getattr(strategy, "name", None) or type(strategy).__name__
    r = _native.run(
        data._n,
        strategy,
        _json.dumps(p),
        hs,
        str(name),
        timer_ms=int(timer) if timer else None,
        book_throttle_ms=int(throttle) if throttle else None,
    )
    return Report(r)


def read_report(path: str) -> Report:
    """A report saved with `save_json` (or the CLI's `--report`)."""
    with open(path, encoding="utf-8") as f:
        return Report(_native.read_report(f.read()))
