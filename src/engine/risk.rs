//! Risk metrics computed from what a report already holds: the equity curve and the per-market
//! P&L. Pure functions, so they can be checked on hand-made curves and recomputed from a saved
//! report.

use serde::{Deserialize, Serialize};

use super::report::{EquityPoint, MarketReport, Metrics};

/// A market and its P&L, for the best and worst market of a sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketPnl {
    /// Market address.
    pub market: String,
    /// P&L in USDC.
    pub pnl: f64,
}

/// Milliseconds in a 365-day year: markets trade around the clock.
pub const YEAR_MS: f64 = 365.0 * 24.0 * 3600.0 * 1000.0;

/// Equity at every full `step_ms` from the first point: the last point at or before each grid
/// time. A trailing part shorter than a step is left out.
pub fn resample(curve: &[EquityPoint], step_ms: u64) -> Vec<f64> {
    let (Some(first), Some(last)) = (curve.first(), curve.last()) else {
        return Vec::new();
    };
    if step_ms == 0 {
        return Vec::new();
    }
    let step = step_ms as i64;
    let mut out = Vec::new();
    let mut i = 0;
    let mut t = first.ts_ms;
    while t <= last.ts_ms {
        while i + 1 < curve.len() && curve[i + 1].ts_ms <= t {
            i += 1;
        }
        out.push(curve[i].equity);
        t += step;
    }
    out
}

/// Simple returns between consecutive values; None if a value is 0 or below (a return from it
/// has no meaning).
pub fn returns(values: &[f64]) -> Option<Vec<f64>> {
    values
        .windows(2)
        .map(|w| (w[0] > 0.0).then(|| w[1] / w[0] - 1.0))
        .collect()
}

/// Mean over the sample standard deviation (n − 1) of the returns, times `scale`. None with
/// fewer than 2 returns or when they never vary.
pub fn sharpe(r: &[f64], scale: f64) -> Option<f64> {
    if r.len() < 2 {
        return None;
    }
    let n = r.len() as f64;
    let mean = r.iter().sum::<f64>() / n;
    let var = r.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let sd = var.sqrt();
    (sd > 0.0)
        .then(|| mean / sd * scale)
        .filter(|v| v.is_finite())
}

/// Mean over the downside deviation (root mean square of the returns below 0, over all n), times
/// `scale`. None with fewer than 2 returns or without a single negative one.
pub fn sortino(r: &[f64], scale: f64) -> Option<f64> {
    if r.len() < 2 {
        return None;
    }
    let n = r.len() as f64;
    let mean = r.iter().sum::<f64>() / n;
    let dd = (r.iter().map(|x| x.min(0.0).powi(2)).sum::<f64>() / n).sqrt();
    (dd > 0.0)
        .then(|| mean / dd * scale)
        .filter(|v| v.is_finite())
}

/// Longest stretch of the deepest drawdown: from the high before the lowest point of the largest
/// fall to the first return to that high, or to the end of the curve. (duration ms, recovered);
/// None when equity never fell.
pub fn max_drawdown_duration(curve: &[EquityPoint]) -> Option<(i64, bool)> {
    let mut peak: Option<EquityPoint> = None;
    let (mut best_dd, mut best_peak) = (0.0f64, None::<EquityPoint>);
    for p in curve {
        match peak {
            Some(pk) if pk.equity >= p.equity => {
                if pk.equity - p.equity > best_dd {
                    best_dd = pk.equity - p.equity;
                    best_peak = Some(pk);
                }
            }
            _ => peak = Some(*p),
        }
    }
    let pk = best_peak?;
    let last = curve.last()?;
    match curve
        .iter()
        .find(|p| p.ts_ms > pk.ts_ms && p.equity >= pk.equity)
    {
        Some(r) => Some((r.ts_ms - pk.ts_ms, true)),
        None => Some((last.ts_ms - pk.ts_ms, false)),
    }
}

/// Exposure over the curve's span, holding each point's position value until the next point:
/// (share of time with any open position, time-weighted average value of positions, largest
/// value). None with fewer than 2 points or a span of 0.
pub fn exposure(curve: &[EquityPoint]) -> Option<(f64, f64, f64)> {
    let span = curve.last()?.ts_ms - curve.first()?.ts_ms;
    if curve.len() < 2 || span <= 0 {
        return None;
    }
    let (mut held, mut area) = (0i64, 0.0f64);
    for w in curve.windows(2) {
        let dt = w[1].ts_ms - w[0].ts_ms;
        if w[0].positions > 0.0 {
            held += dt;
        }
        area += w[0].positions * dt as f64;
    }
    let max = curve.iter().map(|p| p.positions).fold(0.0, f64::max);
    Some((held as f64 / span as f64, area / span as f64, max))
}

/// Fills the risk fields of `m` from the sample's equity curve and its markets.
/// `risk_interval_ms` is the step of the returns; with `annualize` Sharpe and Sortino are scaled
/// by √(intervals per 365-day year).
pub fn apply(
    m: &mut Metrics,
    curve: &[EquityPoint],
    markets: &[&MarketReport],
    risk_interval_ms: u64,
    annualize: bool,
) {
    let scale = if annualize && risk_interval_ms > 0 {
        (YEAR_MS / risk_interval_ms as f64).sqrt()
    } else {
        1.0
    };
    m.risk_interval_ms = risk_interval_ms;
    m.annualization = scale;
    let r = returns(&resample(curve, risk_interval_ms));
    m.sharpe = r.as_deref().and_then(|r| sharpe(r, scale));
    m.sortino = r.as_deref().and_then(|r| sortino(r, scale));

    let mut traded: Vec<&MarketReport> = markets.iter().copied().filter(|x| x.fills > 0).collect();
    traded.sort_by(|a, b| (a.end, a.start, &a.market).cmp(&(b.end, b.start, &b.market)));
    let wins: Vec<f64> = traded.iter().map(|x| x.pnl).filter(|p| *p > 0.0).collect();
    let losses: Vec<f64> = traded.iter().map(|x| x.pnl).filter(|p| *p < 0.0).collect();
    let gross_win: f64 = wins.iter().sum();
    let gross_loss: f64 = -losses.iter().sum::<f64>();
    m.profit_factor = (gross_loss > 0.0).then(|| gross_win / gross_loss);
    m.avg_win = (!wins.is_empty()).then(|| gross_win / wins.len() as f64);
    m.avg_loss = (!losses.is_empty()).then(|| -gross_loss / losses.len() as f64);
    m.win_loss_ratio = match (m.avg_win, m.avg_loss) {
        (Some(w), Some(l)) => Some(w / -l),
        _ => None,
    };
    let pick = |x: &&MarketReport| MarketPnl {
        market: x.market.clone(),
        pnl: x.pnl,
    };
    // first of equals, in end-time order
    m.best_market = traded
        .iter()
        .fold(None::<&MarketReport>, |b, x| match b {
            Some(b) if b.pnl >= x.pnl => Some(b),
            _ => Some(x),
        })
        .as_ref()
        .map(pick);
    m.worst_market = traded
        .iter()
        .fold(None::<&MarketReport>, |b, x| match b {
            Some(b) if b.pnl <= x.pnl => Some(b),
            _ => Some(x),
        })
        .as_ref()
        .map(pick);
    let (mut run, mut longest) = (0u32, 0u32);
    for x in &traded {
        run = if x.pnl < 0.0 { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    m.max_losing_streak = longest;

    let dd = max_drawdown_duration(curve);
    m.max_drawdown_duration_ms = dd.map(|d| d.0);
    m.max_drawdown_recovered = dd.map(|d| d.1);
    let ex = exposure(curve);
    m.exposure_time = ex.map(|e| e.0);
    m.avg_in_positions = ex.map(|e| e.1);
    m.max_in_positions = ex.map(|e| e.2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::Period;
    use chrono::DateTime;

    fn pt(ts_ms: i64, equity: f64, positions: f64) -> EquityPoint {
        EquityPoint {
            ts_ms,
            equity,
            positions,
        }
    }

    fn mkt(i: i64, pnl: f64, fills: u32) -> MarketReport {
        MarketReport {
            market: format!("0x{i:064x}"),
            period: Period::M5,
            start: DateTime::from_timestamp(i * 300, 0).unwrap(),
            end: DateTime::from_timestamp(i * 300 + 300, 0).unwrap(),
            sample: "all".into(),
            result: "up".into(),
            pnl,
            cash: pnl,
            payout: 0.0,
            open_value: 0.0,
            fees: 0.0,
            turnover: 10.0,
            fills,
            reverted_fills: 0,
            orders: fills,
            recorded_fills: 1,
            marks: vec![],
        }
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn resample_takes_the_last_point_of_every_full_step() {
        let h = 3_600_000;
        let c = [
            pt(0, 100.0, 0.0),
            pt(h / 2, 50.0, 0.0),
            pt(h, 110.0, 0.0),
            pt(h + 1, 120.0, 0.0),
            pt(2 * h + h / 2, 130.0, 0.0),
        ];
        // grid 0, 1h, 2h; the half step after 2h is left out
        assert_eq!(resample(&c, h as u64), vec![100.0, 110.0, 120.0]);
        assert_eq!(resample(&[], h as u64), Vec::<f64>::new());
    }

    #[test]
    fn sharpe_and_sortino_on_known_returns() {
        let s = sharpe(&[0.01, 0.03], 1.0).unwrap();
        assert!(close(s, 0.02 / (0.0002f64).sqrt()), "{s}");
        // no negative return: Sortino undefined
        assert_eq!(sortino(&[0.01, 0.03], 1.0), None);
        let r = [0.02, -0.01, 0.05];
        let so = sortino(&r, 1.0).unwrap();
        assert!(close(so, 0.02 / (0.0001f64 / 3.0).sqrt()), "{so}");
        // scaled
        assert!(close(sharpe(&[0.01, 0.03], 2.0).unwrap(), 2.0 * s));
        // too few or flat
        assert_eq!(sharpe(&[0.01], 1.0), None);
        assert_eq!(sharpe(&[0.01, 0.01, 0.01], 1.0), None);
        assert_eq!(sortino(&[], 1.0), None);
        // a value at 0 makes returns meaningless
        assert_eq!(returns(&[0.0, 1.0]), None);
        assert_eq!(returns(&[100.0, 110.0]), Some(vec![0.10000000000000009]));
    }

    #[test]
    fn drawdown_duration_runs_to_recovery_or_the_end() {
        let c = [
            pt(0, 100.0, 0.0),
            pt(10, 120.0, 0.0),
            pt(20, 90.0, 0.0),
            pt(30, 110.0, 0.0),
            pt(40, 125.0, 0.0),
            pt(50, 100.0, 0.0),
        ];
        // deepest fall 30 from the high at 10, back above it at 40
        assert_eq!(max_drawdown_duration(&c), Some((30, true)));
        let c = [pt(0, 100.0, 0.0), pt(5, 80.0, 0.0), pt(25, 90.0, 0.0)];
        assert_eq!(max_drawdown_duration(&c), Some((25, false)));
        let up = [pt(0, 100.0, 0.0), pt(5, 101.0, 0.0)];
        assert_eq!(max_drawdown_duration(&up), None);
        assert_eq!(max_drawdown_duration(&[pt(0, 1.0, 0.0)]), None);
    }

    #[test]
    fn exposure_is_time_weighted() {
        let c = [
            pt(0, 100.0, 0.0),
            pt(10, 100.0, 50.0),
            pt(30, 100.0, 0.0),
            pt(40, 100.0, 0.0),
        ];
        let (t, avg, max) = exposure(&c).unwrap();
        assert!(close(t, 0.5));
        assert!(close(avg, 25.0));
        assert!(close(max, 50.0));
        assert_eq!(exposure(&[pt(0, 1.0, 1.0)]), None);
        assert_eq!(exposure(&[pt(5, 1.0, 1.0), pt(5, 1.0, 1.0)]), None);
    }

    #[test]
    fn market_metrics_on_a_synthetic_report() {
        let ms: Vec<MarketReport> = [10.0, -5.0, -3.0, 2.0, -1.0, -4.0, -2.0]
            .iter()
            .enumerate()
            .map(|(i, &p)| mkt(i as i64, p, 1))
            .chain([mkt(20, -100.0, 0)]) // not traded: ignored
            .collect();
        // markets in reverse order: the streak still follows end time
        let refs: Vec<&MarketReport> = ms.iter().rev().collect();
        let mut m = Metrics::default();
        apply(&mut m, &[], &refs, 3_600_000, false);
        assert!(close(m.profit_factor.unwrap(), 12.0 / 15.0));
        assert!(close(m.avg_win.unwrap(), 6.0));
        assert!(close(m.avg_loss.unwrap(), -3.0));
        assert!(close(m.win_loss_ratio.unwrap(), 2.0));
        assert_eq!(m.best_market.as_ref().unwrap().pnl, 10.0);
        assert_eq!(m.worst_market.as_ref().unwrap().pnl, -5.0);
        assert_eq!(m.max_losing_streak, 3);
        assert_eq!(m.annualization, 1.0);
        // no curve: no Sharpe, drawdown or exposure
        assert_eq!((m.sharpe, m.sortino), (None, None));
        assert_eq!(m.max_drawdown_duration_ms, None);
        assert_eq!(m.exposure_time, None);
    }

    #[test]
    fn without_losses_the_loss_metrics_are_none() {
        let ms = [mkt(0, 3.0, 2), mkt(1, 0.0, 1), mkt(2, 1.0, 1)];
        let refs: Vec<&MarketReport> = ms.iter().collect();
        let mut m = Metrics::default();
        apply(&mut m, &[pt(0, 1000.0, 0.0)], &refs, 1000, true);
        assert_eq!(m.profit_factor, None);
        assert_eq!(m.avg_loss, None);
        assert_eq!(m.win_loss_ratio, None);
        assert!(close(m.avg_win.unwrap(), 2.0));
        assert_eq!(m.max_losing_streak, 0);
        assert_eq!(m.worst_market.as_ref().unwrap().pnl, 0.0);
        // one point: nothing to measure over time
        assert_eq!((m.sharpe, m.sortino), (None, None));
        assert_eq!(m.exposure_time, None);
        assert!(close(m.annualization, (YEAR_MS / 1000.0).sqrt()));
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("NaN") && !json.contains("inf"), "{json}");
    }

    #[test]
    fn sharpe_from_a_curve_per_interval_and_annualized() {
        let h = 3_600_000;
        let c: Vec<EquityPoint> = [100.0, 101.0, 100.0, 102.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| pt(i as i64 * h, v, 0.0))
            .collect();
        let r = returns(&resample(&c, h as u64)).unwrap();
        let mut m = Metrics::default();
        apply(&mut m, &c, &[], h as u64, false);
        assert!(close(m.sharpe.unwrap(), sharpe(&r, 1.0).unwrap()));
        assert!(close(m.sortino.unwrap(), sortino(&r, 1.0).unwrap()));
        apply(&mut m, &c, &[], h as u64, true);
        assert!(close(
            m.sharpe.unwrap(),
            sharpe(&r, 1.0).unwrap() * (24.0f64 * 365.0).sqrt()
        ));
    }

    #[test]
    fn version_1_metrics_still_read() {
        let v1 = r#"{"start_equity":1000.0,"end_equity":1001.0,"pnl":1.0,"return_pct":0.1,
            "markets_traded":1,"markets_won":1,"win_rate":1.0,"max_drawdown":0.0,
            "max_drawdown_pct":0.0,"turnover":5.0,"fees":0.0,"fills":1,"reverted_fills":0,
            "orders":1,"rejected_orders":0,"taker_orders":1,"avg_slippage":0.0,
            "limit_orders":0,"limit_orders_filled":0,"limit_fill_rate":null}"#;
        let m: Metrics = serde_json::from_str(v1).unwrap();
        assert_eq!(m.pnl, 1.0);
        assert_eq!(m.sharpe, None);
        let p: EquityPoint = serde_json::from_str(r#"{"ts_ms":1,"equity":2.0}"#).unwrap();
        assert_eq!(p.positions, 0.0);
    }
}
