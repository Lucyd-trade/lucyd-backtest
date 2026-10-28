//! Python bindings: `lucyd_backtest._native`. The Python package (`python/lucyd_backtest`) wraps
//! them with pandas and a `Strategy` base class; nothing here is meant to be called directly.

use std::cell::Cell;
use std::collections::HashSet;
use std::path::PathBuf;
use std::ptr;

use lucyd_backtest::engine::{
    self, BookFeed, Ctx, ExecParams, MarketView, OrderUpdate, OrderView, PriceView, Report,
    Resolution, Strategy, Subscriptions,
};
use lucyd_backtest::export::{Row, rows, to_parquet};
use lucyd_backtest::{
    Dataset, Fill, Loader, OrderBook, OutcomeBook, Request, Side, TOKEN_SCALE, parse_time,
};
use pyo3::IntoPyObjectExt;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple, PyType};
use serde::Serialize;
use serde_json::{Value, json};

create_exception!(
    _native,
    LucydError,
    PyException,
    "Loading or backtest failed."
);

fn lucyd_err(e: lucyd_backtest::Error) -> PyErr {
    match e {
        lucyd_backtest::Error::InvalidRequest(_) => PyValueError::new_err(e.to_string()),
        e => LucydError::new_err(e.to_string()),
    }
}

static NAMESPACE: GILOnceCell<Py<PyType>> = GILOnceCell::new();

/// JSON to Python: objects become `types.SimpleNamespace`, so fields read as attributes.
fn to_py(py: Python<'_>, v: &Value) -> PyResult<PyObject> {
    Ok(match v {
        Value::Null => py.None(),
        Value::Bool(b) => b.into_py_any(py)?,
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.into_py_any(py)?,
            (_, Some(u)) => u.into_py_any(py)?,
            _ => n.as_f64().unwrap_or(f64::NAN).into_py_any(py)?,
        },
        Value::String(s) => s.into_py_any(py)?,
        Value::Array(a) => {
            let l = PyList::empty(py);
            for x in a {
                l.append(to_py(py, x)?)?;
            }
            l.into_any().unbind()
        }
        Value::Object(o) => {
            let d = PyDict::new(py);
            for (k, x) in o {
                d.set_item(k, to_py(py, x)?)?;
            }
            NAMESPACE
                .import(py, "types", "SimpleNamespace")?
                .call((), Some(&d))?
                .unbind()
        }
    })
}

fn value<T: Serialize>(py: Python<'_>, x: &T) -> PyResult<PyObject> {
    let v = serde_json::to_value(x).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    to_py(py, &v)
}

fn parse_outcome(o: &Bound<'_, PyAny>) -> PyResult<u8> {
    if let Ok(s) = o.extract::<String>() {
        return match s.to_ascii_lowercase().as_str() {
            "up" => Ok(0),
            "down" => Ok(1),
            _ => Err(PyValueError::new_err(format!(
                "outcome {s:?}: 0 or \"up\", 1 or \"down\""
            ))),
        };
    }
    match o.extract::<i64>()? {
        i @ (0 | 1) => Ok(i as u8),
        i => Err(PyValueError::new_err(format!(
            "outcome {i}: 0 or \"up\", 1 or \"down\""
        ))),
    }
}

fn parse_side(s: &str) -> PyResult<Side> {
    match s.to_ascii_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(PyValueError::new_err(format!(
            "side {s:?}: \"buy\" or \"sell\""
        ))),
    }
}

fn whole_tokens(t: f64) -> PyResult<u64> {
    if t >= 1.0 && t.fract() == 0.0 && t < u64::MAX as f64 {
        Ok(t as u64)
    } else {
        Err(PyValueError::new_err(format!(
            "tokens {t}: a whole number, at least 1"
        )))
    }
}

fn market_json(m: &MarketView) -> Value {
    json!({
        "address": m.address,
        "period": m.period.as_str(),
        "instrument": m.instrument,
        "feed": m.feed,
        "start_ms": m.start_ms,
        "end_ms": m.end_ms,
        "strike": m.strike,
        "open": m.open,
        "time_to_end_ms": m.time_to_end_ms,
        "outcomes": m.outcomes,
        "resolution": m.resolution,
    })
}

fn order_json(o: &OrderView) -> Value {
    json!({
        "id": o.id,
        "market": o.market,
        "outcome": o.outcome,
        "side": o.side,
        "price": o.price,
        "tif": o.tif,
        "quantity": o.quantity,
        "filled": o.filled,
        "remaining": o.remaining,
        "pending": o.pending,
        "submitted_ms": o.submitted_ms,
    })
}

fn quote_json(price: f64, tokens: f64) -> Value {
    json!({ "price": price, "tokens": tokens })
}

/// What a handler sees and does: the engine's `Ctx`, valid only while the handler runs.
#[pyclass(unsendable, name = "Ctx", module = "lucyd_backtest")]
struct PyCtx {
    ptr: Cell<*mut Ctx<'static>>,
}

impl PyCtx {
    fn with<R>(&self, f: impl FnOnce(&mut Ctx<'static>) -> R) -> PyResult<R> {
        let p = self.ptr.get();
        if p.is_null() {
            return Err(PyRuntimeError::new_err(
                "ctx is only valid inside the handler it was passed to",
            ));
        }
        // SAFETY: set from the `&mut Ctx` of the running handler and cleared before the handler
        // returns to the engine; the engine does not touch the context meanwhile.
        Ok(f(unsafe { &mut *p }))
    }
}

#[pymethods]
impl PyCtx {
    /// Current time of the run, unix ms.
    #[getter]
    fn now_ms(&self) -> PyResult<i64> {
        self.with(|c| c.now_ms())
    }

    /// Every market of the run.
    fn markets(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let ms = self.with(|c| c.markets())?;
        ms.iter().map(|m| to_py(py, &market_json(m))).collect()
    }

    /// Markets trading now.
    fn open_markets(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let ms = self.with(|c| c.open_markets())?;
        ms.iter().map(|m| to_py(py, &market_json(m))).collect()
    }

    /// One market by address, or None.
    fn market(&self, py: Python<'_>, market: &str) -> PyResult<Option<PyObject>> {
        let m = self.with(|c| c.market(market))?;
        m.map(|m| to_py(py, &market_json(&m))).transpose()
    }

    /// Book of both outcomes now, or None before the first recorded book.
    fn book(&self, market: &str) -> PyResult<Option<PyBook>> {
        Ok(self.with(|c| c.book(market))?.map(|b| PyBook { b }))
    }

    /// Best bid of an outcome now: `.price`, `.tokens`; None if the side is empty.
    fn best_bid(
        &self,
        py: Python<'_>,
        market: &str,
        outcome: &Bound<'_, PyAny>,
    ) -> PyResult<Option<PyObject>> {
        let o = parse_outcome(outcome)?;
        let q = self.with(|c| c.best_bid(market, o))?;
        q.map(|q| to_py(py, &quote_json(q.price, q.tokens)))
            .transpose()
    }

    /// Best ask of an outcome now: `.price`, `.tokens`; None if the side is empty.
    fn best_ask(
        &self,
        py: Python<'_>,
        market: &str,
        outcome: &Bound<'_, PyAny>,
    ) -> PyResult<Option<PyObject>> {
        let o = parse_outcome(outcome)?;
        let q = self.with(|c| c.best_ask(market, o))?;
        q.map(|q| to_py(py, &quote_json(q.price, q.tokens)))
            .transpose()
    }

    /// Last Pyth price of a feed (`btc-usd`): `.price`, `.conf`, `.publish_ms`, `.ts_ms`.
    fn price(&self, py: Python<'_>, feed: &str) -> PyResult<Option<PyObject>> {
        let p = self.with(|c| c.price(feed).cloned())?;
        p.map(|p| value(py, &p)).transpose()
    }

    /// Last Pyth price of the market's underlying.
    fn market_price(&self, py: Python<'_>, market: &str) -> PyResult<Option<PyObject>> {
        let p = self.with(|c| c.market_price(market).cloned())?;
        p.map(|p| value(py, &p)).transpose()
    }

    /// USDC not held by open buy orders.
    #[getter]
    fn usdc(&self) -> PyResult<f64> {
        self.with(|c| c.usdc())
    }

    /// USDC including what open buy orders hold.
    #[getter]
    fn usdc_total(&self) -> PyResult<f64> {
        self.with(|c| c.usdc_total())
    }

    /// Tokens of an outcome held.
    fn position(&self, market: &str, outcome: &Bound<'_, PyAny>) -> PyResult<f64> {
        let o = parse_outcome(outcome)?;
        self.with(|c| c.position(market, o))
    }

    /// Own orders on their way or in the book; all markets when `market` is None.
    #[pyo3(signature = (market=None))]
    fn open_orders(&self, py: Python<'_>, market: Option<&str>) -> PyResult<Vec<PyObject>> {
        let os = self.with(|c| c.open_orders(market))?;
        os.iter().map(|o| to_py(py, &order_json(o))).collect()
    }

    /// Limit order (GTC). Returns the order id.
    fn limit(
        &self,
        market: &str,
        outcome: &Bound<'_, PyAny>,
        side: &str,
        price: f64,
        tokens: f64,
    ) -> PyResult<u64> {
        let (o, s, t) = (
            parse_outcome(outcome)?,
            parse_side(side)?,
            whole_tokens(tokens)?,
        );
        self.with(|c| c.limit(market, o, s, price, t))?
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Market order (FAK) up to `limit` (default: any price). Returns the order id.
    #[pyo3(signature = (market, outcome, side, tokens, limit=None))]
    fn market_order(
        &self,
        market: &str,
        outcome: &Bound<'_, PyAny>,
        side: &str,
        tokens: f64,
        limit: Option<f64>,
    ) -> PyResult<u64> {
        let (o, s, t) = (
            parse_outcome(outcome)?,
            parse_side(side)?,
            whole_tokens(tokens)?,
        );
        self.with(|c| c.market_order(market, o, s, t, limit))?
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Cancel an own order.
    fn cancel(&self, order_id: u64) -> PyResult<()> {
        self.with(|c| c.cancel(order_id))?
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

/// Book of both outcomes at a moment. Sizes in tokens.
#[pyclass(frozen, name = "Book", module = "lucyd_backtest")]
struct PyBook {
    b: OrderBook,
}

impl PyBook {
    fn side(&self, outcome: &Bound<'_, PyAny>) -> PyResult<Option<&OutcomeBook>> {
        Ok(self.b.outcome(parse_outcome(outcome)?))
    }
}

fn levels(it: impl Iterator<Item = (f64, u64)>) -> Vec<(f64, f64)> {
    it.map(|(p, s)| (p, s as f64 / TOKEN_SCALE)).collect()
}

#[pymethods]
impl PyBook {
    /// Time of the last update in the book, unix ms.
    #[getter]
    fn ts_ms(&self) -> i64 {
        self.b.ts_ms
    }

    /// Highest bid of an outcome: `.price`, `.tokens`.
    fn best_bid(&self, py: Python<'_>, outcome: &Bound<'_, PyAny>) -> PyResult<Option<PyObject>> {
        let q = self.side(outcome)?.and_then(|o| o.best_bid());
        q.map(|(p, s)| to_py(py, &quote_json(p, s as f64 / TOKEN_SCALE)))
            .transpose()
    }

    /// Lowest ask of an outcome: `.price`, `.tokens`.
    fn best_ask(&self, py: Python<'_>, outcome: &Bound<'_, PyAny>) -> PyResult<Option<PyObject>> {
        let q = self.side(outcome)?.and_then(|o| o.best_ask());
        q.map(|(p, s)| to_py(py, &quote_json(p, s as f64 / TOKEN_SCALE)))
            .transpose()
    }

    /// Midpoint of the best bid and ask of an outcome.
    fn mid(&self, outcome: &Bound<'_, PyAny>) -> PyResult<Option<f64>> {
        Ok(self.side(outcome)?.and_then(|o| o.mid()))
    }

    /// Bids of an outcome, best first: `[(price, tokens), ...]`.
    fn bids(&self, outcome: &Bound<'_, PyAny>) -> PyResult<Vec<(f64, f64)>> {
        Ok(self
            .side(outcome)?
            .map(|o| levels(o.bid_levels()))
            .unwrap_or_default())
    }

    /// Asks of an outcome, best first: `[(price, tokens), ...]`.
    fn asks(&self, outcome: &Bound<'_, PyAny>) -> PyResult<Vec<(f64, f64)>> {
        Ok(self
            .side(outcome)?
            .map(|o| levels(o.ask_levels()))
            .unwrap_or_default())
    }

    fn __repr__(&self) -> String {
        let top = |i: u8| {
            let o = self.b.outcome(i);
            let f = |q: Option<(f64, u64)>| q.map_or("-".to_string(), |(p, _)| p.to_string());
            (
                f(o.and_then(|o| o.best_bid())),
                f(o.and_then(|o| o.best_ask())),
            )
        };
        let (ub, ua) = top(0);
        let (db, da) = top(1);
        format!("<Book ts_ms={} up {ub}/{ua} down {db}/{da}>", self.b.ts_ms)
    }
}

/// Loaded data: markets, books, fills, trades, prices.
#[pyclass(frozen, name = "NativeDataset", module = "lucyd_backtest")]
struct PyDataset {
    d: Dataset,
}

#[pymethods]
impl PyDataset {
    fn request_json(&self) -> String {
        self.d.request.to_json()
    }

    fn markets_json(&self) -> PyResult<String> {
        let infos: Vec<_> = self.d.markets.iter().map(|m| &m.info).collect();
        serde_json::to_string(&infos).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[getter]
    fn warnings(&self) -> Vec<String> {
        self.d.warnings.clone()
    }

    /// The event stream as Parquet, only the given kinds when set.
    #[pyo3(signature = (kinds=None))]
    fn events_parquet<'py>(
        &self,
        py: Python<'py>,
        kinds: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .allow_threads(|| {
                let ev = self.d.events();
                let out: Vec<Row> = ev
                    .iter()
                    .filter(|e| {
                        kinds
                            .as_ref()
                            .is_none_or(|k| k.iter().any(|x| x == e.kind.name()))
                    })
                    .flat_map(rows)
                    .collect();
                to_parquet(&out)
            })
            .map_err(lucyd_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Book of a market at a moment, or None.
    fn book_at(&self, market: &str, ts_ms: i64) -> Option<PyBook> {
        let m = self.d.market(market)?;
        m.book.as_ref()?.at(ts_ms).map(|b| PyBook { b })
    }

    fn __len__(&self) -> usize {
        self.d.markets.len()
    }
}

/// A finished backtest.
#[pyclass(frozen, name = "NativeReport", module = "lucyd_backtest")]
struct PyReport {
    r: Report,
}

#[pymethods]
impl PyReport {
    fn to_json(&self) -> String {
        self.r.to_json()
    }

    fn to_html(&self) -> String {
        self.r.to_html()
    }

    fn summary_text(&self) -> String {
        self.r.summary_text()
    }
}

/// A Python strategy object behind the engine's `Strategy`.
struct Bridge<'py> {
    py: Python<'py>,
    obj: Bound<'py, PyAny>,
    ctx: Bound<'py, PyCtx>,
    on: HashSet<String>,
    subs: Subscriptions,
    name: String,
    /// First exception from a handler; later handlers are skipped.
    error: Option<PyErr>,
}

impl<'py> Bridge<'py> {
    fn call(
        &mut self,
        ctx: &mut Ctx<'_>,
        name: &str,
        args: impl FnOnce(Python<'py>) -> PyResult<Vec<PyObject>>,
    ) {
        if self.error.is_some() || !self.on.contains(name) {
            return;
        }
        let p: *mut Ctx<'static> = ptr::from_mut(ctx).cast();
        self.ctx.borrow().ptr.set(p);
        let r = args(self.py).and_then(|rest| {
            let mut all = Vec::with_capacity(rest.len() + 1);
            all.push(self.ctx.clone().into_any().unbind());
            all.extend(rest);
            self.obj
                .call_method1(name, PyTuple::new(self.py, all)?)
                .map(drop)
        });
        self.ctx.borrow().ptr.set(ptr::null_mut());
        if let Err(e) = r {
            self.error = Some(e);
        }
    }
}

impl Strategy for Bridge<'_> {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn subscriptions(&self) -> Subscriptions {
        self.subs.clone()
    }

    fn on_start(&mut self, ctx: &mut Ctx) {
        self.call(ctx, "on_start", |_| Ok(vec![]));
    }

    fn on_market_open(&mut self, ctx: &mut Ctx, market: &str) {
        self.call(ctx, "on_market_open", |py| {
            Ok(vec![market.into_py_any(py)?])
        });
    }

    fn on_book(&mut self, ctx: &mut Ctx, market: &str) {
        self.call(ctx, "on_book", |py| Ok(vec![market.into_py_any(py)?]));
    }

    fn on_fill(&mut self, ctx: &mut Ctx, fill: &Fill) {
        self.call(ctx, "on_fill", |py| Ok(vec![value(py, fill)?]));
    }

    fn on_price(&mut self, ctx: &mut Ctx, feed: &str, price: &PriceView) {
        self.call(ctx, "on_price", |py| {
            Ok(vec![feed.into_py_any(py)?, value(py, price)?])
        });
    }

    fn on_timer(&mut self, ctx: &mut Ctx) {
        self.call(ctx, "on_timer", |_| Ok(vec![]));
    }

    fn on_market_close(&mut self, ctx: &mut Ctx, market: &str) {
        self.call(ctx, "on_market_close", |py| {
            Ok(vec![market.into_py_any(py)?])
        });
    }

    fn on_resolution(&mut self, ctx: &mut Ctx, market: &str, result: &Resolution) {
        self.call(ctx, "on_resolution", |py| {
            Ok(vec![market.into_py_any(py)?, value(py, result)?])
        });
    }

    fn on_order(&mut self, ctx: &mut Ctx, update: &OrderUpdate) {
        self.call(ctx, "on_order", |py| Ok(vec![value(py, update)?]));
    }

    fn on_finish(&mut self, ctx: &mut Ctx) {
        self.call(ctx, "on_finish", |_| Ok(vec![]));
    }
}

fn loader(cache_dir: Option<PathBuf>) -> PyResult<Loader> {
    match cache_dir {
        Some(d) => Loader::with_cache_dir(d),
        None => Loader::new(),
    }
    .map_err(lucyd_err)
}

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Downloads (or reads from the cache) the data of a request given as JSON.
#[pyfunction]
#[pyo3(signature = (request_json, cache_dir=None))]
fn load(py: Python<'_>, request_json: &str, cache_dir: Option<PathBuf>) -> PyResult<PyDataset> {
    let req = Request::from_json(request_json).map_err(lucyd_err)?;
    let (loader, rt) = (loader(cache_dir)?, runtime()?);
    let d = py
        .allow_threads(|| rt.block_on(loader.load(&req)))
        .map_err(lucyd_err)?;
    Ok(PyDataset { d })
}

/// The market list of a request, as JSON, without downloading data.
#[pyfunction]
#[pyo3(signature = (request_json, cache_dir=None))]
fn catalog(py: Python<'_>, request_json: &str, cache_dir: Option<PathBuf>) -> PyResult<String> {
    let req = Request::from_json(request_json).map_err(lucyd_err)?;
    let (loader, rt) = (loader(cache_dir)?, runtime()?);
    let c = py
        .allow_threads(|| rt.block_on(loader.catalog(&req)))
        .map_err(lucyd_err)?;
    serde_json::to_string(&c).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Runs a Python strategy over a dataset. `handlers` are the methods it implements.
#[pyfunction]
#[pyo3(signature = (data, strategy, params_json, handlers, name, timer_ms=None, book_throttle_ms=None))]
#[allow(clippy::too_many_arguments)]
fn run(
    py: Python<'_>,
    data: &PyDataset,
    strategy: Bound<'_, PyAny>,
    params_json: &str,
    handlers: Vec<String>,
    name: String,
    timer_ms: Option<u64>,
    book_throttle_ms: Option<u64>,
) -> PyResult<PyReport> {
    let params: ExecParams = serde_json::from_str(params_json)
        .map_err(|e| PyValueError::new_err(format!("execution parameters: {e}")))?;
    let on: HashSet<String> = handlers.into_iter().collect();
    let subs = Subscriptions {
        book: match (on.contains("on_book"), book_throttle_ms) {
            (false, _) => BookFeed::Off,
            (true, Some(ms)) => BookFeed::Throttle(ms),
            (true, None) => BookFeed::EveryChange,
        },
        fills: on.contains("on_fill"),
        prices: on.contains("on_price"),
        timer_ms: timer_ms.filter(|_| on.contains("on_timer")),
    };
    let ctx = Bound::new(
        py,
        PyCtx {
            ptr: Cell::new(ptr::null_mut()),
        },
    )?;
    let mut bridge = Bridge {
        py,
        obj: strategy,
        ctx,
        on,
        subs,
        name,
        error: None,
    };
    let r = engine::run(&data.d, &params, &mut bridge).map_err(lucyd_err)?;
    if let Some(e) = bridge.error {
        return Err(e);
    }
    Ok(PyReport { r })
}

/// A report saved as JSON.
#[pyfunction]
fn read_report(json: &str) -> PyResult<PyReport> {
    Ok(PyReport {
        r: Report::from_json(json).map_err(lucyd_err)?,
    })
}

/// `2026-10-27`, `2026-10-27T12:00:00Z` or unix seconds, to unix ms.
#[pyfunction]
fn parse_time_ms(s: &str) -> PyResult<i64> {
    Ok(parse_time(s).map_err(lucyd_err)?.timestamp_millis())
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("LucydError", m.py().get_type::<LucydError>())?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyBook>()?;
    m.add_class::<PyDataset>()?;
    m.add_class::<PyReport>()?;
    m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_function(wrap_pyfunction!(catalog, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(read_report, m)?)?;
    m.add_function(wrap_pyfunction!(parse_time_ms, m)?)?;
    Ok(())
}
