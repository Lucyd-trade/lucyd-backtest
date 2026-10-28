import json
import os

import pytest

import lucyd_backtest as lb
from lucyd_backtest import _handlers, _request


def req(**kw):
    args = dict(
        period=None, start=None, end=None, instrument=None, markets=None,
        data=("book", "fills", "prices"), fills_only=False, network="testnet",
        api_url=None, data_url=None,
    )
    args.update(kw)
    return json.loads(_request(**args))


def test_request():
    r = req(period="5m,1h", start="2026-10-27", end=1761609600, data="all")
    assert r["periods"] == ["5m", "1h"]
    assert r["from"] == "2026-10-27T00:00:00.000Z"
    assert r["to"] == "2025-10-28T00:00:00.000Z"
    assert r["data"] == {"book": True, "fills": True, "trades": True, "prices": True}


def test_request_errors():
    with pytest.raises(ValueError):
        req(period="2m")
    with pytest.raises(ValueError):
        req(data="books")
    with pytest.raises(ValueError):
        req(start="yesterday")


def test_handlers():
    class S(lb.Strategy):
        timer_ms = 1000

        def on_timer(self, ctx):
            pass

        def on_order(self, ctx, u):
            pass

    assert _handlers(S()) == ["on_timer", "on_order"]
    assert _handlers(lb.Strategy()) == []


@pytest.mark.skipif(not os.environ.get("LUCYD_LIVE"), reason="needs the testnet; set LUCYD_LIVE=1")
def test_live_backtest(tmp_path):
    # fills from ~2026-10-20, Pyth prices from 2026-10-27 16:41 UTC
    data = lb.load(
        period="5m", start="2026-10-27T17:00:00Z", end="2026-10-27T22:00:00Z",
        fills_only=True, cache_dir=str(tmp_path),
    )
    assert len(data) > 0
    assert {"address", "start", "end", "strike"} <= set(data.markets.columns)
    assert len(data.fills()) > 0
    assert len(data.prices()) > 0

    class Taker(lb.Strategy):
        timer_ms = 1000

        def __init__(self):
            self.done, self.updates = set(), []

        def on_timer(self, ctx):
            for m in ctx.open_markets():
                ask = ctx.best_ask(m.address, "up")
                if m.address not in self.done and ask is not None:
                    ctx.market_order(m.address, "up", "buy", 5, limit=0.9975)
                    self.done.add(m.address)

        def on_order(self, ctx, u):
            self.updates.append(u.status)

    s = Taker()
    r = lb.backtest(data, s, seed=1)
    assert "filled" in s.updates
    assert r.summary["fills"] > 0
    assert len(r.orders) == len(s.done)
    assert "<html" in r.to_html().lower()
    p = tmp_path / "r.json"
    r.save_json(str(p))
    assert lb.read_report(str(p)).summary == r.summary

    class Broken(lb.Strategy):
        timer_ms = 1000

        def on_timer(self, ctx):
            raise KeyError("boom")

    with pytest.raises(KeyError):
        lb.backtest(data, Broken())
