"""Late side: N seconds before the end of every market, buy the outcome on whose side BTC is
against the strike, with a market order capped at `max_price`.

    pip install lucyd-backtest
    python late_side.py
"""

import math

import lucyd_backtest as lb


class LateSide(lb.Strategy):
    timer_ms = 1000  # the book and prices are read from ctx on the timer

    def __init__(self, seconds_before=30, usd=20.0, max_price=0.95):
        self.seconds_before = seconds_before
        self.usd = usd
        self.max_price = max_price
        self.done = set()

    def on_timer(self, ctx):
        for m in ctx.open_markets():
            if m.address in self.done or m.time_to_end_ms > self.seconds_before * 1000:
                continue
            price = ctx.market_price(m.address)
            if m.strike is None or price is None or price.price == m.strike:
                continue
            outcome = "up" if price.price > m.strike else "down"
            ask = ctx.best_ask(m.address, outcome)
            if ask is None or ask.price > self.max_price:
                continue
            # whole tokens, at least $1 at the cap
            tokens = max(math.floor(self.usd / ask.price), math.ceil(1 / self.max_price))
            ctx.market_order(m.address, outcome, "buy", tokens, limit=self.max_price)
            self.done.add(m.address)


if __name__ == "__main__":
    data = lb.load(period="5m", start="2026-10-27", end="2026-10-28")
    print(data)
    report = lb.backtest(data, LateSide(seconds_before=60, usd=50))
    print(report)
    report.save_html("late_side.html")
