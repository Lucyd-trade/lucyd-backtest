//! The report as one self-contained HTML page: no external scripts, styles or fonts, so it opens
//! offline and can be mailed as a file. The header, warnings and metric cards are plain HTML;
//! charts (inline SVG) and tables are drawn by a small embedded script from the report data,
//! which sits in the page as JSON.

use std::fmt::Write;

use serde_json::{Value, json};

use super::report::{Metrics, Report, duration};

const STYLE: &str = include_str!("html/report.css");
const SCRIPT: &str = include_str!("html/report.js");

/// HTML-escapes text for element content and attribute values.
pub(crate) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// JSON safe inside a `<script>` element: no `<` at all, so neither `</script>` nor `<!--`.
fn script_json(v: &Value) -> String {
    serde_json::to_string(v)
        .expect("report data serializes")
        .replace('<', "\\u003c")
}

/// What the script needs: the report without the journal, plus own fills from it. Orders and
/// fills go as arrays with the market as an index into `markets` (a run can have many).
fn data(r: &Report) -> Value {
    let index: std::collections::HashMap<&str, usize> = r
        .markets
        .iter()
        .enumerate()
        .map(|(i, m)| (m.market.as_str(), i))
        .collect();
    let mi = |a: &str| index.get(a).copied();
    let mut v = serde_json::to_value(r).expect("report serializes");
    if let Some(o) = v.as_object_mut() {
        o.remove("journal");
        // [ts, market, outcome, side, price, quantity, liquidity, order id]
        let fills: Vec<Value> = r
            .journal
            .iter()
            .filter(|j| j.event == "fill")
            .map(|j| {
                json!([
                    j.ts_ms,
                    mi(&j.market),
                    j.outcome,
                    j.side,
                    j.price,
                    j.quantity,
                    j.liquidity,
                    j.order_id
                ])
            })
            .collect();
        o.insert("fills".into(), Value::Array(fills));
        // [id, market, outcome, side, tif, price, quantity, filled, avg price, status, reason,
        //  submitted ms, slippage]
        let orders: Vec<Value> = r
            .orders
            .iter()
            .map(|x| {
                json!([
                    x.id,
                    mi(&x.market),
                    x.outcome,
                    x.side,
                    x.tif,
                    x.price,
                    x.quantity,
                    x.filled,
                    x.avg_price,
                    x.status,
                    x.reason,
                    x.submitted_ms,
                    x.slippage
                ])
            })
            .collect();
        o.insert("orders".into(), Value::Array(orders));
    }
    v
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default()
}

fn f2(v: f64) -> String {
    format!("{v:.2}")
}

fn signed(v: f64) -> String {
    format!("{v:+.2}")
}

fn pct(v: f64) -> String {
    format!("{:.1}%", v * 100.0)
}

fn opt(v: Option<f64>, f: fn(f64) -> String) -> String {
    v.map(f).unwrap_or_else(|| "–".into())
}

/// One metric card: the whole run large, fit and validation below.
struct Card {
    label: &'static str,
    hint: &'static str,
    value: fn(&Metrics) -> String,
}

fn cards() -> Vec<Card> {
    vec![
        Card {
            label: "P&L, USDC",
            hint: "sum of the markets' P&L",
            value: |m| signed(m.pnl),
        },
        Card {
            label: "Return",
            hint: "P&L / initial USDC",
            value: |m| format!("{:+.2}%", m.return_pct),
        },
        Card {
            label: "Max drawdown",
            hint: "largest fall from a previous high",
            value: |m| format!("{} ({:.1}%)", f2(m.max_drawdown), m.max_drawdown_pct),
        },
        Card {
            label: "Drawdown length",
            hint: "from the high before the largest fall back to it",
            value: |m| match (m.max_drawdown_duration_ms, m.max_drawdown_recovered) {
                (Some(d), Some(false)) => format!("{} +", duration(d)),
                (Some(d), _) => duration(d),
                _ => "–".into(),
            },
        },
        Card {
            label: "Sharpe",
            hint: "mean / std of equity returns per risk interval",
            value: |m| opt(m.sharpe, f2),
        },
        Card {
            label: "Sortino",
            hint: "mean / downside deviation of the same returns",
            value: |m| opt(m.sortino, f2),
        },
        Card {
            label: "Win rate",
            hint: "markets with P&L above 0 among traded",
            value: |m| format!("{} of {}", opt(m.win_rate, pct), m.markets_traded),
        },
        Card {
            label: "Profit factor",
            hint: "won / lost, by market",
            value: |m| opt(m.profit_factor, f2),
        },
        Card {
            label: "Avg win / loss",
            hint: "average P&L of a winning and a losing market",
            value: |m| format!("{} / {}", opt(m.avg_win, signed), opt(m.avg_loss, signed)),
        },
        Card {
            label: "Losing streak",
            hint: "most losing markets in a row",
            value: |m| m.max_losing_streak.to_string(),
        },
        Card {
            label: "Exposure",
            hint: "time with an open position",
            value: |m| opt(m.exposure_time, pct),
        },
        Card {
            label: "In positions",
            hint: "average / largest value of open positions",
            value: |m| {
                format!(
                    "{} / {}",
                    opt(m.avg_in_positions, f2),
                    opt(m.max_in_positions, f2)
                )
            },
        },
        Card {
            label: "Fills",
            hint: "own fills (reverted)",
            value: |m| format!("{} ({})", m.fills, m.reverted_fills),
        },
        Card {
            label: "Turnover, USDC",
            hint: "notional of own fills",
            value: |m| f2(m.turnover),
        },
        Card {
            label: "Avg slippage",
            hint: "taker price vs best price on arrival, per token",
            value: |m| opt(m.avg_slippage, |v| format!("{v:.4}")),
        },
        Card {
            label: "Limit fill rate",
            hint: "limit orders with any fill",
            value: |m| opt(m.limit_fill_rate, pct),
        },
    ]
}

fn header(r: &Report, out: &mut String) {
    let q = &r.request;
    let mut what = Vec::new();
    if let Some(i) = &q.instrument {
        what.push(esc(i));
    }
    if !q.periods.is_empty() {
        what.push(
            q.periods
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if !q.markets.is_empty() {
        what.push(format!("{} market(s) by address", q.markets.len()));
    }
    let p = &r.params;
    let _ = write!(
        out,
        r#"<header id="header"><div class="top"><h1>{name}</h1><button id="theme" type="button" aria-label="Switch theme">◐</button></div>
<p class="range">{from} → {to} · {network} · {what}</p>
<dl class="params">
<div><dt>Initial USDC</dt><dd>{initial}</dd></div>
<div><dt>Latency</dt><dd>{lat} ms (cancel {clat}, jitter {jit})</dd></div>
<div><dt>Queue model</dt><dd>{queue}</dd></div>
<div><dt>Fees</dt><dd>{fee} bps, redeem {rfee} bps</dd></div>
<div><dt>Revert chance</dt><dd>{rev}</dd></div>
<div><dt>Validation</dt><dd>last {val}% of the range</dd></div>
<div><dt>Risk interval</dt><dd>{risk}{ann}</dd></div>
<div><dt>Seed</dt><dd>{seed}</dd></div>
<div><dt>Markets</dt><dd>{markets} ({traded} traded)</dd></div>
</dl>
<details><summary>Request and parameters as JSON</summary><pre>{json}</pre></details>
</header>"#,
        name = esc(&r.strategy),
        from = iso(r.from_ms),
        to = iso(r.to_ms),
        network = esc(q.network.name()),
        what = if what.is_empty() {
            "all markets".into()
        } else {
            what.join(" · ")
        },
        initial = f2(p.initial_usdc),
        lat = p.submit_latency_ms,
        clat = p.cancel_latency_ms,
        jit = p.latency_jitter_ms,
        queue = format!("{:?}", p.queue_model).to_lowercase(),
        fee = p.fee_bps,
        rfee = p.redeem_fee_bps,
        rev = p.revert_probability,
        val = (p.validation_fraction * 100.0).round(),
        risk = duration(p.risk_interval_ms as i64),
        ann = if p.annualize { ", annualized" } else { "" },
        seed = p.seed,
        markets = r.markets.len(),
        traded = r.summary.markets_traded,
        json = esc(
            &serde_json::to_string_pretty(&json!({"request": q, "params": p})).unwrap_or_default()
        ),
    );
}

fn warnings(r: &Report, out: &mut String) {
    out.push_str(r#"<section id="warnings" class="warnings" aria-label="Warnings">"#);
    let _ = write!(
        out,
        r#"<h2><span class="icon" aria-hidden="true">⚠</span> {} warning(s)</h2><ul>"#,
        r.warnings.len()
    );
    for w in &r.warnings {
        let _ = write!(out, "<li>{}</li>", esc(w));
    }
    out.push_str("</ul></section>");
}

fn metric_cards(r: &Report, out: &mut String) {
    out.push_str(r#"<section id="metrics"><h2>Metrics</h2>"#);
    if r.periods.is_some() {
        out.push_str(
            r#"<p class="note">Large: the whole run. Below: fit sample (markets ending before the split) and validation (after it, out of sample).</p>"#,
        );
    }
    out.push_str(r#"<div class="cards">"#);
    for c in cards() {
        let _ = write!(
            out,
            r#"<div class="card" title="{hint}"><div class="label">{label}</div><div class="value">{v}</div>"#,
            hint = esc(c.hint),
            label = esc(c.label),
            v = esc(&(c.value)(&r.summary)),
        );
        if let Some(p) = &r.periods {
            let _ = write!(
                out,
                r#"<div class="split"><span><b>fit</b> {}</span><span><b>val</b> {}</span></div>"#,
                esc(&(c.value)(&p.fit)),
                esc(&(c.value)(&p.validation)),
            );
        }
        out.push_str("</div>");
    }
    out.push_str("</div>");
    let mut extremes = String::new();
    if let (Some(b), Some(w)) = (&r.summary.best_market, &r.summary.worst_market) {
        let _ = write!(
            extremes,
            r##"<p class="note">Best market <a href="#market-detail" data-market="{ba}">{bs}</a> {bp} · worst <a href="#market-detail" data-market="{wa}">{ws}</a> {wp}</p>"##,
            ba = esc(&b.market),
            bs = esc(&short(&b.market)),
            bp = signed(b.pnl),
            wa = esc(&w.market),
            ws = esc(&short(&w.market)),
            wp = signed(w.pnl),
        );
    }
    out.push_str(&extremes);
    out.push_str("</section>");
}

fn short(a: &str) -> String {
    if a.len() > 14 {
        format!("{}…{}", &a[..8], &a[a.len() - 4..])
    } else {
        a.to_string()
    }
}

/// Renders the page.
pub(crate) fn render(r: &Report) -> String {
    let mut out = String::with_capacity(64 * 1024);
    let _ = write!(
        out,
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} · backtest report</title>
<style>{STYLE}</style></head><body><main>"#,
        title = esc(&r.strategy)
    );
    header(r, &mut out);
    warnings(r, &mut out);
    metric_cards(r, &mut out);
    out.push_str(
        r#"<section id="equity"><h2>Equity</h2><p class="note">USDC plus open positions at their marks. The dashed line is the start of the validation period.</p><div class="chart" id="equity-chart"></div></section>
<section id="drawdown"><h2>Drawdown</h2><p class="note">Equity below its previous high, USDC.</p><div class="chart" id="drawdown-chart"></div></section>
<section id="market-pnl"><h2>P&amp;L by market</h2><div class="controls"><label>Sort <select id="pnl-sort"><option value="time">by end time</option><option value="pnl">by P&amp;L</option></select></label></div><p class="note">Traded markets. Tap a bar to see the market below.</p><div class="chart" id="pnl-chart"></div></section>
<section id="market-detail"><h2>Market</h2><div class="controls"><label>Market <select id="detail-market"></select></label><span class="seg" role="group" aria-label="Outcome"><button type="button" data-outcome="0" aria-pressed="true">up</button><button type="button" data-outcome="1" aria-pressed="false">down</button></span></div><p class="note" id="detail-note"></p><div class="chart" id="detail-chart"></div></section>
<section id="markets"><h2>Markets</h2><div class="controls"><input type="search" id="markets-filter" placeholder="Filter: address, result, sample…" aria-label="Filter markets"><label><input type="checkbox" id="markets-traded" checked> with own orders</label></div><div class="table" id="markets-table"></div></section>
<section id="orders"><h2>Orders</h2><div class="controls"><input type="search" id="orders-filter" placeholder="Filter: market, reason…" aria-label="Filter orders"><select id="orders-status" aria-label="Status"><option value="">any status</option><option>filled</option><option>partially_filled</option><option>cancelled</option><option>rejected</option><option>open</option></select></div><div class="table" id="orders-table"></div></section>
<div id="tip" role="tooltip" hidden></div>
"#,
    );
    let _ = write!(
        out,
        r#"<script type="application/json" id="data">{}</script>
<script>{SCRIPT}</script>
<noscript><p class="note">Charts and tables need JavaScript; the metrics above do not.</p></noscript>
</main></body></html>
"#,
        script_json(&data(r))
    );
    out
}
